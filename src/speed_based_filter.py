from datetime import datetime

import datazimmer as dz
import metazimmer.gpsping.ubermedia as um
import pandas as pd
from colassigner import Col

from .util import get_client, localize


class Arrival(dz.CompositeTypeBase):
    def __init__(self) -> None:
        self._arr_cols = ArrivalExtension.incoming
        self._start_coords = um.GpsPing.loc
        self._end_coords = um.GpsPing.loc

        self._start_time = um.GpsPing.datetime
        self._end_time = um.GpsPing.datetime

    def distance(self, df) -> Col[float]:  # meters
        start_points = localize(df, self._start_coords)
        end_points = localize(df.pipe(self._filter_df).shift(1), self._end_coords)
        return start_points.distance(end_points)

    def time(self, df) -> Col[float]:  # minutes
        end_times = df.pipe(self._filter_df).loc[:, self._end_time]
        return (df[self._start_time] - end_times.shift(1)).dt.total_seconds() / 60

    def speed(self, df) -> Col[float]:  # kph
        return (df[self._arr_cols.distance] / 1000) / (df[self._arr_cols.time] / 60)

    @staticmethod
    def _filter_df(df):
        return df


class ArrivalExtension(dz.AbstractEntity):

    incoming = Arrival


class DeviceGroupExtension(dz.AbstractEntity):
    def device_group(self, df) -> Col[str]:
        return df.loc[:, um.GpsPing.device_id].str[:2]


class PingWithArrival(DeviceGroupExtension, ArrivalExtension, um.GpsPing):
    pass


class DropStat(dz.AbstractEntity):

    day = dz.Index & datetime
    insufficient_pings = int
    high_speed = int


filtered_ping_table = dz.ScruTable(
    PingWithArrival, partitioning_cols=[PingWithArrival.device_group]
)
drop_stat_table = dz.ScruTable(DropStat)


@dz.register(
    dependencies=[um.ping_table], outputs=[filtered_ping_table, drop_stat_table]
)
def filter_pings(min_pings_by_device, max_speed):

    get_client()
    ddf = um.ping_table.get_full_ddf()

    drops = (
        ddf.groupby(um.GpsPing.device_id)
        .apply(
            proc_device,
            max_speed=max_speed,
            min_pings=min_pings_by_device,
            out_table=filtered_ping_table,
            meta=pd.DataFrame(columns=drop_stat_table.all_cols),
        )
        .compute()
        .groupby(DropStat.day)
        .sum()
    )

    drop_stat_table.replace_all(drops)


def proc_device(device_df, min_pings, max_speed, out_table):
    if device_df.shape[0] < min_pings:
        return device_df.pipe(_to_drop_stat, speed=False)
    _df = device_df.pipe(DeviceGroupExtension())
    misses = [pd.DataFrame(columns=drop_stat_table.all_cols)]
    while True:
        _df = _df.pipe(ArrivalExtension())
        speed_ser = _df[PingWithArrival.incoming.speed]
        speeding = (speed_ser > max_speed) | (speed_ser.shift(-1) > max_speed)
        if not speeding.sum():
            if _df.shape[0] < min_pings:
                misses.append(_df.pipe(_to_drop_stat, speed=False))
            else:
                out_table.extend(_df, try_dask=False)
            return pd.concat(misses).fillna(0)
        misses.append(_df.loc[speeding, :].pipe(_to_drop_stat, speed=True))
        _df = _df.loc[~speeding, :]


def _to_drop_stat(df, speed):
    return (
        df.rename(columns={um.GpsPing.datetime: DropStat.day})
        .assign(
            **{
                DropStat.high_speed: int(speed),
                DropStat.insufficient_pings: int(not speed),
            }
        )
        .set_index(DropStat.day)
        .resample("1D")[drop_stat_table.feature_cols]
        .sum()
        .reset_index()
    )
