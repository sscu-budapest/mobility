import datetime as dt
from dataclasses import dataclass
from functools import partial

import datazimmer as dz
import pandas as pd
from colassigner import Col, get_all_cols
from metazimmer.gpsping.ubermedia import Coordinates

from .filtered_stops import filtered_stop_table
from .initial_stops import Stop
from .speed_based_filter import Arrival
from .util import localize


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

    from_last_stop = ArrivalFromStop
    from_last_ping = ArrivalFromPing

    def duration(self, df) -> Col[float]:
        return (df[Stop.interval.end] - df[Stop.interval.start]).dt.total_seconds() / 60


class SpecialPlace(dz.CompositeTypeBase):

    time = float  # minutes
    total_time_in_period = float  # minutes
    is_max_in_period = bool
    distance = float  # meters


class SemanticStop(Stop, StopExtension):

    time_bin = dt.datetime

    home = SpecialPlace
    work = SpecialPlace


def proc_device_group_partition(gdf, dayconf: DaySetup, period, out_table):
    out_table.replace_groups(
        gdf.groupby(Stop.device_id, as_index=False).apply(proc_device, dayconf, period)
    )


def proc_device(dedf, dayconf: DaySetup, period: str):

    extended_df = (
        _get_times_df(dedf, dayconf)
        .merge(dedf.pipe(StopExtension()))
        .assign(
            **{
                SemanticStop.time_bin: lambda df: df[Stop.interval.start]
                .dt.to_period(period)
                .dt.to_timestamp()
            }
        )
    )
    _gcols = [Stop.place_label, SemanticStop.time_bin]
    _places = [SemanticStop.work, SemanticStop.home]
    period_sums = (
        extended_df.groupby(_gcols)[[_p.time for _p in _places]]
        .transform("sum")
        .rename(columns={_p.time: _p.total_time_in_period for _p in _places})
    )
    period_maxes = (
        pd.concat([period_sums, extended_df[_gcols]], axis=1)
        .loc[~extended_df[Stop.is_between_stops], :]
        .groupby(_gcols)
        .transform("max")
        .rename(columns=lambda s: f"max_{s}")
    )

    return (
        pd.concat([extended_df, period_sums, period_maxes], axis=1)
        .fillna(0)
        .pipe(
            lambda df: df.assign(
                **{
                    _p.is_max_in_period: (
                        df[_p.total_time_in_period]
                        == df[f"max_{_p.total_time_in_period}"]
                    )
                    for _p in _places
                }
            )
        )
        .pipe(
            lambda df: df.assign(
                **{_p.distance: _get_dist_from_latest(df, _p) for _p in _places}
            )
        )
    )


semantic_stop_table = dz.ScruTable(
    SemanticStop, partitioning_cols=filtered_stop_table.partitioning_cols
)


@dz.register(dependencies=[filtered_stop_table], outputs=[semantic_stop_table])
def step(morning_end: int, work_end: int, home_arrive: int, time_unit: str):

    dayconf = DaySetup(morning_end, work_end, home_arrive)
    filtered_stop_table.map_partitions(
        fun=partial(
            proc_device_group_partition,
            dayconf=dayconf,
            period=time_unit,
            out_table=semantic_stop_table,
        ),
        pbar=True,
    )


def _get_times_df(dedf: pd.DataFrame, dayconf: DaySetup) -> pd.DataFrame:
    int_cols = [Stop.interval.start, Stop.interval.end]
    return (
        pd.concat([dedf.assign(_d=dedf[c]) for c in int_cols])
        .set_index("_d")
        .sort_index()
        .groupby(Stop.stop_number)
        .resample("1H")[int_cols]
        .first()
        .assign(
            _s=lambda df: df[int_cols[0]].fillna(method="ffill"),
            _e=lambda df: df[int_cols[1]].fillna(method="bfill"),
        )
        .reset_index()
        .assign(
            _d2=lambda df: df["_d"] + pd.Timedelta(hours=1),
            dur=lambda df: (
                df[["_e", "_d2"]].min(axis=1) - df[["_s", "_d"]].max(axis=1)
            ).dt.total_seconds()
            / 60,
            weekend=lambda df: df["_d"].dt.dayofweek.isin([5, 6]),
            hour=lambda df: df["_d"].dt.hour,
            **{
                SemanticStop.home.time: lambda df: (
                    (df["hour"] <= dayconf.morning_end)
                    | (df["hour"] >= dayconf.home_arrive)
                )
                * df["dur"],
                SemanticStop.work.time: lambda df: (
                    (df["hour"] > dayconf.morning_end)
                    & (df["hour"] <= dayconf.work_end)
                    & ~df["weekend"]
                )
                * df["dur"],
            },
        )
        .groupby(Stop.stop_number)[[SemanticStop.home.time, SemanticStop.work.time]]
        .sum()
        .reset_index()
    )


def _loc(df, coords: Coordinates, filter_col):
    return pd.DataFrame(
        {
            col: df[col]
            .where(df[filter_col])
            .fillna(method="ffill")
            .fillna(method="bfill")
            for col in get_all_cols(coords)
        },
        index=df.index,
    )


def _get_dist_from_latest(df, place: SpecialPlace):
    start_points = localize(df, Stop.center)
    end_points = localize(_loc(df, Stop.center, place.is_max_in_period), Stop.center)
    return start_points.distance(end_points)
