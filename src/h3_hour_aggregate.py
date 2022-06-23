import datetime as dt
from functools import partial

import datazimmer as dz
import pandas as pd
from atqo import parallel_map
from metazimmer.gpsping import ubermedia as um

from .util import to_geo


class HashHour(dz.AbstractEntity):
    hid = dz.Index & str
    hour = dz.Index & dt.datetime

    year_month = str
    count = int


h3_table = dz.ScruTable(HashHour, partitioning_cols=[HashHour.year_month])


def get_h3_id(df):
    return df.pipe(to_geo).h3.geo_to_h3(10).index.to_numpy()


def proc_month_paths(paths, min_count, min_duration, table):
    gdf = pd.concat(map(pd.read_parquet, paths))
    agg_df = (
        gdf.assign(
            **{
                HashHour.hid: get_h3_id,
                HashHour.hour: gdf[um.GpsPing.datetime].dt.floor("h"),
            }
        )
        .groupby([um.GpsPing.device_id, *h3_table.index_cols])[um.GpsPing.datetime]
        .agg(["min", "max", "count"])
        .assign(
            duration_minutes=lambda df: (df["max"] - df["min"]).dt.total_seconds() / 60
        )
    )
    if agg_df.empty:
        return
    h3_df = (
        agg_df.loc[
            lambda df: (df["count"] >= min_count)
            & (df["duration_minutes"] >= min_duration)
        ]
        .groupby(h3_table.index_cols)[[HashHour.count]]
        .agg("count")
    )
    hour_col = h3_df.index.get_level_values(HashHour.hour).astype(str).str[:7]
    table.extend(h3_df.assign(**{HashHour.year_month: hour_col}), try_dask=False)


@dz.register(dependencies=[um.ping_table], outputs=[h3_table])
def step(min_count, min_duration):
    proc_paths = partial(
        proc_month_paths,
        min_count=min_count,
        min_duration=min_duration,
        table=h3_table,
    )
    parallel_map(
        proc_paths,
        [*um.ping_table.get_partition_paths(partition_col=um.ExtendedPing.year_month)],
        pbar=True,
    )
