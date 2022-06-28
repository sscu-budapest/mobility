from dataclasses import dataclass
from functools import partial

import datazimmer as dz
import numpy as np
import pandas as pd
from colassigner import Col

from .filtered_stops import filtered_stop_table
from .initial_stops import Stop
from .speed_based_filter import Arrival
from .util import get_client


@dataclass
class DaySetup:
    morning_end: int
    work_end: int
    home_arrive: int


class ArrivalFromPing(Arrival):
    def __init__(self) -> None:
        self._arr_cols = StopExtension.from_last_ping
        self._start_coords = Stop.first
        self._end_coords = Stop.last

        self._start_time = Stop.interval.start
        self._end_time = Stop.interval.end


class ArrivalFromStop(ArrivalFromPing):
    def __init__(self) -> None:
        super().__init__()
        self._arr_cols = StopExtension.from_last_stop

    @staticmethod
    def _filter_df(df):
        return (
            df.loc[~df[Stop.is_between_stops], :]
            .reindex(df.index)
            .fillna(method="ffill")
        )


class StopExtension(dz.AbstractEntity):
    def __init__(self, df, day: DaySetup) -> None:
        self._morning_end = self._get_hour(df, day.morning_end)
        self._work_end = self._get_hour(df, day.work_end)
        self._evening_start = self._get_hour(df, day.home_arrive)

    from_last_stop = ArrivalFromStop
    from_last_ping = ArrivalFromPing

    def duration(self, df) -> Col[float]:
        return (df[Stop.interval.end] - df[Stop.interval.start]).dt.total_seconds() / 60

    def morning_rate(self, df) -> Col[float]:
        morning_period = self._morning_end - df[Stop.interval.start]
        return self._get_rate(df, morning_period)

    def work_rate(self, df) -> Col[float]:
        start_time = np.maximum(df[Stop.interval.start], self._morning_end)
        end_time = np.minimum(df[Stop.interval.end], self._work_end)
        return self._get_rate(df, end_time - start_time)

    def evening_rate(self, df) -> Col[float]:
        return self._get_rate(df, df[Stop.interval.end] - self._evening_start)

    def _get_rate(self, df, spent_ext):
        spent_within = spent_ext.dt.total_seconds() / 60
        return np.clip(spent_within / df[StopExtension.duration], 0, 1)

    def _get_hour(self, df, hour):
        return df[Stop.interval.start].dt.normalize() + pd.Timedelta(hours=hour)


class SemanticStop(Stop, StopExtension):

    pass

    # is_home = bool
    # is_work = bool


def proc_device_group_partition(gdf, dayconf: DaySetup, out_table):
    out_table.replace_groups(
        gdf.groupby(Stop.device_id).apply(lambda df: StopExtension(df, dayconf)(df))
    )


semantic_stop_table = dz.ScruTable(SemanticStop, partitioning_cols=filtered_stop_table.partitioning_cols)


@dz.register(dependencies=[filtered_stop_table], outputs=[semantic_stop_table])
def step(morning_end: int, work_end: int, home_arrive: int):

    dayconf = DaySetup(morning_end, work_end, home_arrive)
    filtered_stop_table.map_partitions(
        fun=partial(proc_device_group_partition, dayconf=dayconf, out_table=semantic_stop_table),
        pbar=True,
    )
