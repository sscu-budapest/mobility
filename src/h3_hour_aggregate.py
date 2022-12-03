import datetime as dt
from functools import partial, reduce
from multiprocessing import cpu_count

import datazimmer as dz
import pandas as pd
import psutil
from atqo import parallel_map
from metazimmer.gpsping import meta
from metazimmer.gpsping.ubermedia.raw_proc import ping_table

from .util import to_geo


class HashHour(dz.AbstractEntity):
    hid = dz.Index & str
    hour = dz.Index & dt.datetime

    year_month = str
    count = int


h3_table = dz.ScruTable(HashHour, partitioning_cols=[HashHour.year_month])


def get_h3_id(df):
    return df.pipe(to_geo).h3.geo_to_h3(10).index.to_numpy()


def proc_month_groups(paths_tup, min_count, min_duration):
    paths = paths_tup[1]
    fun = partial(proc_partition_path, min_count=min_count, min_duration=min_duration)
    red_df = reduce(_reducer, map(fun, paths))
    hour_col = red_df.index.get_level_values(HashHour.hour).astype(str).str[:7]
    h3_table.extend(red_df.assign(**{HashHour.year_month: hour_col}))


def proc_partition_path(path, min_count, min_duration):
    gdf = pd.read_parquet(path)
    agg_df = (
        gdf.assign(
            **{
                HashHour.hid: get_h3_id,
                HashHour.hour: gdf[meta.GpsPing.datetime].dt.floor("h"),
            }
        )
        .groupby([meta.GpsPing.device_id, *h3_table.index_cols])[meta.GpsPing.datetime]
        .agg(["min", "max", "count"])
        .assign(
            duration_minutes=lambda df: (df["max"] - df["min"]).dt.total_seconds() / 60
        )
    )
    if agg_df.empty:
        return pd.DataFrame(index=pd.MultiIndex(names=h3_table.index_cols))
    return (
        agg_df.loc[
            lambda df: (df["count"] >= min_count)
            & (df["duration_minutes"] >= min_duration)
        ]
        .groupby(h3_table.index_cols)[[HashHour.count]]
        .agg("count")
    )


@dz.register(dependencies=[ping_table], outputs=[h3_table])
def step(min_count, min_duration):
    mem_gb = psutil.virtual_memory().total / 2**30
    workers = min(cpu_count(), int(mem_gb / 10) + 1)

    proc_paths = partial(
        proc_month_groups, min_count=min_count, min_duration=min_duration
    )
    iterable = ping_table.get_partition_paths(
        partition_col=meta.ExtendedPing.year_month
    )
    list(parallel_map(proc_paths, iterable, pbar=True, workers=workers))


def _reducer(df1: pd.DataFrame, df2: pd.DataFrame):
    name = df1.columns[0]
    return (
        df1.join(df2, how="outer", rsuffix="r")
        .fillna(0)
        .sum(axis=1)
        .rename(name)
        .to_frame()
    )
