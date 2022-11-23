from datetime import datetime
from functools import partial

import datazimmer as dz
import metazimmer.gpsping.meta as meta
import pandas as pd
from atqo import parallel_map
from colassigner import Col
from metazimmer.gpsping.ubermedia.raw_proc import ping_table

from .util import localize


class Arrival(dz.CompositeTypeBase):
    def __init__(self) -> None:
        self._arr_cols = ArrivalExtension.incoming
        self._start_coords = meta.GpsPing.loc
        self._end_coords = meta.GpsPing.loc

        self._start_time = meta.GpsPing.datetime
        self._end_time = meta.GpsPing.datetime

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


class PingWithArrival(ArrivalExtension, meta.ExtendedPing):
    pass


class DropStat(dz.AbstractEntity):

    day = dz.Index & datetime
    insufficient_pings = int
    high_speed = int


filtered_ping_table = dz.ScruTable(
    PingWithArrival, partitioning_cols=[PingWithArrival.device_group]
)
drop_stat_table = dz.ScruTable(DropStat)


@dz.register(dependencies=[ping_table], outputs=[filtered_ping_table, drop_stat_table])
def filter_pings(min_pings_by_device, max_speed):

    proc_paths = partial(
        proc_device_groups, max_speed=max_speed, min_pings=min_pings_by_device
    )
    drop_dfs = list(
        parallel_map(
            proc_paths,
            ping_table.get_partition_paths(
                partition_col=meta.ExtendedPing.device_group
            ),
            pbar=True,
        )
    )
    drop_stat_table.replace_all(
        pd.concat(drop_dfs).fillna(0).groupby(DropStat.day).sum().reset_index()
    )


def proc_device_groups(gb_out, min_pings, max_speed):
    _, gpaths = gb_out
    dg_df = pd.concat(map(ping_table.trepo.read_df_from_path, gpaths))
    misses = [pd.DataFrame(columns=drop_stat_table.all_cols)]
    user_dfs = []
    for _, d_df in dg_df.groupby(meta.GpsPing.device_id):
        if d_df.shape[0] < min_pings:
            misses.append(_to_drop_stat(d_df, False))
            continue
        new_misses, proc_df = proc_device(d_df, max_speed)
        misses += new_misses
        if proc_df.shape[0] < min_pings:
            misses.append(proc_df.pipe(_to_drop_stat, speed=False))
        else:
            user_dfs.append(proc_df)
    filtered_ping_table.extend(pd.concat(user_dfs))
    return pd.concat(misses).fillna(0).groupby(DropStat.day).sum().reset_index()


def proc_device(device_df, max_speed):
    misses = []
    while True:
        device_df = device_df.pipe(ArrivalExtension())
        speed_ser = device_df[PingWithArrival.incoming.speed]
        speeding = (speed_ser > max_speed) | (speed_ser.shift(-1) > max_speed)
        if not speeding.sum():
            return misses, device_df
        misses.append(device_df.loc[speeding, :].pipe(_to_drop_stat, speed=True))
        device_df = device_df.loc[~speeding, :]


def _to_drop_stat(df, speed):
    return (
        df.rename(columns={meta.GpsPing.datetime: DropStat.day})
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
