from datetime import datetime
from itertools import groupby, islice
from multiprocessing import cpu_count
from pathlib import Path

import dask.dataframe as dd
import datazimmer as dz
import metazimmer.gpsping.ubermedia as um
import pandas as pd
from colassigner import Col
from distributed import as_completed

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

_DROP_COL = "_to_drop"
_COMBINED_META = pd.DataFrame(
    columns=sorted(
        [
            _DROP_COL,
            *set(drop_stat_table.feature_cols).union(set(filtered_ping_table.all_cols)),
        ]
    )
)
_drop_initer = {
    DropStat.high_speed: 0,
    DropStat.insufficient_pings: 0,
    _DROP_COL: False,
}


@dz.register(
    dependencies=[um.ping_table], outputs=[filtered_ping_table, drop_stat_table]
)
def filter_pings(min_pings_by_device, max_speed):

    batch_size = cpu_count()
    client = get_client()
    drop_dfs = []
    gb_iter = groupby(sorted(um.ping_table.paths, key=_getkey), _getkey)

    def _getfut(ddf):
        return client.submit(
            decorate_df, ddf, max_speed=max_speed, min_pings=min_pings_by_device
        )

    futures = []
    for _, fpaths in islice(gb_iter, batch_size):
        ddf = dd.read_parquet([*fpaths])
        futures.append(_getfut(ddf))

    seq = as_completed(futures)
    for decorated_future in seq:
        decorated_df = decorated_future.result()
        drop_dfs.append(
            decorated_df.loc[decorated_df[_DROP_COL], :].pipe(_to_drop_stat)
        )
        filtered_ping_table.extend(decorated_df.loc[~decorated_df[_DROP_COL], :])
        try:
            _, fpaths = next(gb_iter)
        except StopIteration:
            continue
        ddf = dd.read_parquet([*fpaths])
        new_future = _getfut(ddf)
        seq.add(new_future)
        del decorated_future


    drop_stat_table.replace_all(
        pd.concat(drop_dfs).fillna(0).groupby(DropStat.day).sum().reset_index()
    )


def _getkey(path):
    return Path(path).parts[-1]


def decorate_df(ddf, max_speed, min_pings):
    return (
        ddf.drop(um.ExtendedPing.year_month, axis=1)
        .compute()
        .groupby(um.ExtendedPing.device_id)
        .apply(
            proc_device,
            max_speed=max_speed,
            min_pings=min_pings,
            meta=_COMBINED_META,
        )
        .reset_index(drop=True)
    )


def proc_device(dev_df, max_speed, min_pings):
    misses = []
    remaining_df = dev_df.assign(**_drop_initer)
    while True:
        remaining_df = remaining_df.pipe(ArrivalExtension())
        if remaining_df.shape[0] < min_pings:
            misses.append(remaining_df.assign(**{DropStat.insufficient_pings: 1}))
            remaining_df = pd.DataFrame()
            break
        speed_ser = remaining_df[PingWithArrival.incoming.speed]
        speeding = (speed_ser > max_speed) | (speed_ser.shift(-1) > max_speed)
        if not speeding.sum():
            break
        misses.append(remaining_df.loc[speeding, :].assign(**{DropStat.high_speed: 1}))
        remaining_df = remaining_df.loc[~speeding, :]
    return pd.concat(misses + [remaining_df]).loc[:, _COMBINED_META.columns]


def _to_drop_stat(df):
    return (
        df.rename(columns={um.GpsPing.datetime: DropStat.day})
        .set_index(DropStat.day)
        .resample("1D")[drop_stat_table.feature_cols]
        .sum()
        .reset_index()
    )
