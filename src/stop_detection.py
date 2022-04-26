from dataclasses import dataclass
from datetime import datetime
from functools import partial

import datazimmer as dz
import geopandas
import pandas as pd
from atqo import parallel_map
from colassigner import ColAssigner
from infostop import Infostop
from metazimmer.gpsping import ubermedia as um
from structlog import get_logger

from .create_filtered_dataset import FilteredPingFeatures, filtered_ping_table

logger = get_logger()


class NoStops(Exception):
    pass


@dataclass
class DaySetup:
    work_start: int
    work_end: int
    home_arrive: int
    home_depart: int
    min_work_hours: float


class Duration(dz.CompositeTypeBase):
    start = datetime
    end = datetime


class Arrival(dz.CompositeTypeBase):
    speed = float  # kph
    distance = float  # mins
    time = float  # meters


class StopFeatures(dz.TableFeaturesBase):
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
    from_last_stop = Arrival
    from_last_ping = Arrival


class Labeler(ColAssigner):
    def __init__(self, model, day: DaySetup) -> None:
        self.model = model
        self.day = day

    def ts(self, df):
        return df[FilteredPingFeatures.datetime].view(int) / 10 ** 9

    def hour(self, df):
        return df[FilteredPingFeatures.datetime].dt.hour

    def place_label(self, df):
        arr = df.loc[
            :, [FilteredPingFeatures.loc.lon, FilteredPingFeatures.loc.lat, Labeler.ts]
        ].values
        try:
            return self.model.fit_predict(arr)
        except Exception as e:
            assert "No stop events found" in str(e)
            raise NoStops("hopefully")

    def stop_number(self, df):
        return (df[Labeler.place_label] != df[Labeler.place_label].shift(1)).cumsum()

    def is_worktime(self, df):
        return (df[Labeler.hour] >= self.day.work_start) & (
            df[Labeler.hour] <= self.day.work_end
        )

    def is_hometime(self, df):
        return (df[Labeler.hour] >= self.day.home_arrive) | (
            df[Labeler.hour] <= self.day.home_depart
        )

    def is_top_home(self, df):
        hometime_events = (
            df.groupby(Labeler.place_label)[Labeler.is_hometime]
            .sum()
            .drop(-1, errors="ignore")
        )
        if not len(hometime_events):
            return False
        top_label = hometime_events.idxmax()
        if hometime_events[top_label] < 1:
            return False
        return df[Labeler.place_label] == top_label

    def is_first_and_last(self, df):
        _col = df[Labeler.place_label]
        try:
            e1, el = _col.loc[_col != -1].iloc[[1, -1]]
        except IndexError:
            return False
        return (_col == e1) & (_col == el)


class LocalCoords(ColAssigner):
    def __init__(self, df) -> None:
        self.garr = geopandas.points_from_xy(
            df[StopFeatures.center.lon], df[StopFeatures.center.lat], crs="EPSG:4326"
        ).to_crs("EPSG:23700")

    def local_x(self, _):
        return self.garr.x

    def local_y(self, _):
        return self.garr.y


def pipe_assigner(df, assigner):
    return assigner(df)(df)


def add_arrivals(df, cols: Arrival):
    tcol, dcol, scol = cols.time, cols.distance, cols.speed
    return df.assign(
        **{
            tcol: (
                df[StopFeatures.interval.start] - df[StopFeatures.interval.end].shift(1)
            ).dt.total_seconds()
            / 60,
            dcol: df[[LocalCoords.local_x, LocalCoords.local_y]].pipe(
                lambda _df: ((_df - _df.shift(1)) ** 2).sum(axis=1) ** 0.5
            ),
            scol: lambda _df: (_df[dcol] / 1000) / (_df[tcol] / 60),
        }
    )


def add_speed_cols(df: pd.DataFrame):
    return (
        df.groupby(StopFeatures.device_id, as_index=False)
        .apply(
            lambda gdf: gdf.pipe(add_arrivals, StopFeatures.from_last_ping).pipe(
                lambda _df: add_arrivals(
                    _df.loc[lambda df: df[Labeler.place_label] > -1, :],
                    StopFeatures.from_last_stop,
                )
            )
        )
        .reset_index(drop=True)
    )


base_groupers = [
    FilteredPingFeatures.device_id,
    FilteredPingFeatures.year_month,
    FilteredPingFeatures.dayofmonth,
]
stop_table = dz.ScruTable(
    features=StopFeatures,
    partitioning_cols=base_groupers[1:],
)


def proc_partition(partition_path, model, day: DaySetup):
    dfs = []
    for _, day_df in pd.read_parquet(partition_path).groupby(base_groupers):
        try:
            day_df.sort_values(FilteredPingFeatures.datetime).pipe(
                Labeler(model, day)
            ).pipe(dfs.append)
        except Exception as e:
            # TODO log and handle this
            if not isinstance(e, NoStops):
                logger.exception(e)
            pass
    if not dfs:
        return
    (
        pd.concat(dfs)
        .pipe(_gb_stop, day.min_work_hours)
        .pipe(pipe_assigner, LocalCoords)
        .pipe(add_speed_cols)
        .pipe(stop_table.extend, verbose=False, try_dask=False)
    )


@dz.register(
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

    parallel_map(
        partial(proc_partition, model=model, day=dayconf),
        filtered_ping_table.paths,
        pbar=True,
        dist_api="mp",
        raise_errors=True,
    )


def _gb_stop(labeled_df, min_work_hours):
    dt_col = FilteredPingFeatures.datetime
    return (
        labeled_df.groupby([Labeler.stop_number, Labeler.place_label, *base_groupers])
        .agg(
            **{
                StopFeatures.n_events: pd.NamedAgg(dt_col, "count"),
                StopFeatures.interval.start: pd.NamedAgg(dt_col, "first"),
                StopFeatures.interval.end: pd.NamedAgg(dt_col, "last"),
                StopFeatures.center.lon: pd.NamedAgg(
                    FilteredPingFeatures.loc.lon, "mean"
                ),
                StopFeatures.center.lat: pd.NamedAgg(
                    FilteredPingFeatures.loc.lat, "mean"
                ),
                "top_home": pd.NamedAgg(Labeler.is_top_home, "max"),
                "f_and_last": pd.NamedAgg(Labeler.is_first_and_last, "max"),
                "some_worktime": pd.NamedAgg(Labeler.is_worktime, "max"),
            }
        )
        .assign(
            **{
                "duration": lambda df: (
                    df[StopFeatures.interval.end] - df[StopFeatures.interval.start]
                ).dt.total_seconds(),
                StopFeatures.is_home: lambda df: df[["top_home", "f_and_last"]].all(
                    axis=1
                ),
                StopFeatures.is_work: lambda df: df["some_worktime"]
                & (df["duration"] >= min_work_hours * 60 ** 2),
            }
        )
        .reset_index()
    )
