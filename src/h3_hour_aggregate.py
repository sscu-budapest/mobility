import datetime as dt

import datazimmer as dz
import pandas as pd
from metazimmer.gpsping import ubermedia as um

from .util import get_client, to_geo


class HashHour(dz.AbstractEntity):
    geohash = dz.Index & str
    hour = dz.Index & dt.datetime

    year_month = str
    count = int


h3_table = dz.ScruTable(HashHour, partitioning_cols=[HashHour.year_month])


def get_h3_id(df):
    return df.pipe(to_geo).h3.geo_to_h3(10).index.to_numpy()


def proc_gdf(gdf, min_count, min_duration, table):
    agg_df = (
        gdf.assign(
            **{
                HashHour.geohash: get_h3_id,
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

    table.extend(
        h3_df.assign(
            **{
                HashHour.year_month: h3_df.index.get_level_values(HashHour.hour)
                .astype(str)
                .str[:7]
            }
        ),
        try_dask=False,
    )
    return pd.DataFrame()


@dz.register(dependencies=[um.ping_table], outputs=[h3_table])
def step(min_count, min_duration):
    get_client()
    um.ping_table.get_full_ddf().groupby(um.ExtendedPing.year_month).apply(
        proc_gdf,
        min_count=min_count,
        min_duration=min_duration,
        table=h3_table,
        meta=pd.DataFrame(),
    ).compute()
