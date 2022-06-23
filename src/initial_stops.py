from datetime import datetime
from functools import partial
from multiprocessing import cpu_count

import datazimmer as dz
import pandas as pd
import psutil
from atqo import MULTI_GC_API
from colassigner import ColAssigner
from infostop import Infostop
from metazimmer.gpsping.ubermedia import Coordinates
from structlog import get_logger

from .speed_based_filter import PingWithArrival, filtered_ping_table

logger = get_logger()


class NoStops(Exception):
    pass


class TimeInterval(dz.CompositeTypeBase):
    start = datetime
    end = datetime


class Stop(dz.AbstractEntity):
    device_id = str
    device_group = str

    place_label = int
    stop_number = int
    n_events = int
    interval = TimeInterval
    center = Coordinates
    last = Coordinates
    first = Coordinates
    is_between_stops = bool


class Labeler(ColAssigner):
    def __init__(self, model_params) -> None:
        self.model = Infostop(**model_params)

    def ts(self, df):
        return df[PingWithArrival.datetime].view(int) / 10**9

    def place_label(self, df):

        arr = df.loc[
            :, [PingWithArrival.loc.lat, PingWithArrival.loc.lon, Labeler.ts]
        ].to_numpy()

        try:
            return self.model.fit_predict(arr)
        except Exception as e:
            # assert "No stop events found" in str(e)
            if not ("No stop events found" in str(e)):
                logger.warning(f"some infostop error {e}")
            raise NoStops("hopefully")

    def stop_number(self, df):
        return (df[Labeler.place_label] != df[Labeler.place_label].shift(1)).cumsum()


stop_table = dz.ScruTable(Stop, partitioning_cols=[Stop.device_group])


def proc_device(device_df, model_params: dict):
    try:
        labeled_df = device_df.sort_values(PingWithArrival.datetime).pipe(
            Labeler(model_params)
        )
    except Exception as e:
        # TODO log and handle this
        if not isinstance(e, NoStops):
            logger.exception(e)
        return pd.DataFrame(columns=stop_table.feature_cols)

    return labeled_df.pipe(_gb_stop)


def proc_partition(partition_df, params: dict, table: dz.ScruTable):
    table.extend(
        partition_df.groupby(PingWithArrival.device_id, as_index=False)
        .apply(proc_device, model_params=params)
        .reset_index(drop=True),
        try_dask=False,
    )


@dz.register(
    dependencies=[filtered_ping_table],
    outputs=[stop_table],
)
def step(
    r1,
    r2,
    min_staying_time,
    max_time_between,
    min_size,
    distance_metric,
):
    model_params = dict(
        r1=r1,
        r2=r2,
        min_staying_time=min_staying_time,
        max_time_between=max_time_between,
        min_size=min_size,
        distance_metric=distance_metric,
    )

    mem_gb = psutil.virtual_memory().total / 2**30
    workers = min(cpu_count(), int(mem_gb / 10) + 1)
    filtered_ping_table.map_partitions(
        fun=partial(proc_partition, params=model_params, table=stop_table),
        workers=workers,
        pbar=True,
        dist_api=MULTI_GC_API,
    )


def _gb_stop(labeled_df):
    dt_col = PingWithArrival.datetime
    loc_cols = PingWithArrival.loc
    firsts = [Stop.device_id, Stop.device_group, Stop.place_label]
    return (
        labeled_df.groupby([Labeler.stop_number])
        .agg(
            **{
                **{col: pd.NamedAgg(col, "first") for col in firsts},
                Stop.n_events: pd.NamedAgg(dt_col, "count"),
                Stop.interval.start: pd.NamedAgg(dt_col, "first"),
                Stop.interval.end: pd.NamedAgg(dt_col, "last"),
                Stop.center.lon: pd.NamedAgg(loc_cols.lon, "mean"),
                Stop.center.lat: pd.NamedAgg(loc_cols.lat, "mean"),
                Stop.first.lon: pd.NamedAgg(loc_cols.lon, "first"),
                Stop.first.lat: pd.NamedAgg(loc_cols.lat, "first"),
                Stop.last.lon: pd.NamedAgg(loc_cols.lon, "last"),
                Stop.last.lat: pd.NamedAgg(loc_cols.lat, "last"),
            }
        )
        .assign(**{Stop.is_between_stops: lambda df: df[Stop.place_label] == -1})
        .reset_index()
        .loc[:, stop_table.all_cols]
    )
