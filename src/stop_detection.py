from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from colassigner import ColAssigner
from infostop import Infostop
from sscutils import CompositeTypeBase, ScruTable, TableFeaturesBase

from .create_filtered_dataset import FilteredPingFeatures, filtered_ping_table
from .imported_namespaces import um
from .pipereg import pipereg


class NoStops(Exception):
    pass


@dataclass
class DaySetup:
    work_start: int
    work_end: int
    home_arrive: int
    home_depart: int
    min_work_hours: float


class Duration(CompositeTypeBase):
    start = datetime
    end = datetime


class StopFeatures(TableFeaturesBase):
    device_id = str
    year_month = str
    dayofmonth = str
    place_label = int
    stop_number = int
    n_events = int
    interval = Duration
    center = um.Coordinates
    is_home = bool
    is_work = bool


class Labeler(ColAssigner):
    def __init__(self, model, day: DaySetup) -> None:
        self.model = model
        self.day = day

    def ts(self, df):
        return df[FilteredPingFeatures.datetime].view(int) / 10 ** 6

    def hour(self, df):
        return df[FilteredPingFeatures.datetime].dt.hour

    def place_label(self, df):
        arr = df.loc[:, [FilteredPingFeatures.loc.lon, FilteredPingFeatures.loc.lat, Labeler.ts]].values
        try:
            return self.model.fit_predict(arr)
        except Exception as e:
            assert "No stop events found" in str(e)
            raise NoStops("hopefully")

    def stop_number(self, df):
        return (df[Labeler.place_label] != df[Labeler.place_label].shift(1)).cumsum()

    def is_worktime(self, df):
        return (df[Labeler.hour] >= self.day.work_start) & (df[Labeler.hour] <= self.day.work_end)

    def is_hometime(self, df):
        return (df[Labeler.hour] >= self.day.home_arrive) | (df[Labeler.hour] <= self.day.home_depart)

    def is_top_home(self, df):
        hometime_events = df.groupby(Labeler.place_label)[Labeler.is_hometime].sum()
        top_label = hometime_events.idxmax()
        if hometime_events[top_label] < 1:
            return False
        return df[Labeler.place_label] == top_label

    def is_first_and_last(self, df):
        _col = df[Labeler.place_label]
        return (_col == _col.iloc[0]) & (_col == _col.iloc[-1])


base_groupers = [FilteredPingFeatures.device_id, FilteredPingFeatures.year_month, FilteredPingFeatures.dayofmonth]
stop_table = ScruTable(
    features=StopFeatures,
    partitioning_cols=base_groupers[1:],
)


def proc_device_day(day_df, model, day: DaySetup):
    try:
        labeled_df = day_df.sort_values(FilteredPingFeatures.datetime).pipe(Labeler(model, day))
    except NoStops:
        return pd.DataFrame()
    stop_df = (
        labeled_df.groupby([Labeler.stop_number, Labeler.place_label, *base_groupers])
        .apply(_by_stop, day.min_work_hours)
        .reset_index()
        .loc[lambda df: df[Labeler.place_label] > -1, :]
    )
    return stop_df


def proc_partition(partition_df, model, day):
    dfs = []
    for _, gdf in partition_df.groupby(base_groupers):
        dfs.append(proc_device_day(gdf, model, day))
    if dfs:
        pd.concat(dfs).pipe(stop_table.extend, verbose=False, try_dask=False)
    return 0


@pipereg.register(
    dependencies=[filtered_ping_table],
    outputs=[stop_table],
)
def step(
    work_start,
    work_end,
    home_arrive,
    home_depart,
    min_work_hours,
    r1,
    r2,
    min_staying_time,
    max_time_between,
    min_size,
    distance_metric,
):

    dayconf = DaySetup(work_start, work_end, home_arrive, home_depart, min_work_hours)

    model = Infostop(
        r1=r1,
        r2=r2,
        min_staying_time=min_staying_time,
        max_time_between=max_time_between,
        min_size=min_size,
        distance_metric=distance_metric,
    )

    filtered_ping_table.get_full_ddf().map_partitions(
        proc_partition, model, dayconf, enforce_metadata=False, meta={}
    ).compute()


def _by_stop(df, min_work_hours):
    start, end = df[FilteredPingFeatures.datetime].iloc[[0, -1]]
    dur = (end - start).total_seconds()
    return pd.Series(
        {
            StopFeatures.n_events: df.shape[0],
            StopFeatures.interval.start: start,
            StopFeatures.interval.end: end,
            StopFeatures.center.lon: df[FilteredPingFeatures.loc.lon].pipe(_center),
            StopFeatures.center.lat: df[FilteredPingFeatures.loc.lat].pipe(_center),
            StopFeatures.is_home: (df[Labeler.is_top_home] & df[Labeler.is_first_and_last]).all(),
            StopFeatures.is_work: (dur >= min_work_hours * 60 ** 2) & df[Labeler.is_worktime].any(),
        }
    )


def _center(s: pd.Series):
    return (s.max() + s.min()) / 2
