import datetime as dt
from functools import partial

import geopandas
import pandas as pd
from colassigner import get_all_cols
from sscutils import IndexBase, ScruTable, TableFeaturesBase

from .imported_namespaces import um
from .pipereg import pipereg


class RasterHourIndex(IndexBase):
    raster_id = str
    hour = dt.datetime


class RasterHourFeatures(TableFeaturesBase):
    month = str
    count = int


raster_table = ScruTable(RasterHourFeatures, index=RasterHourIndex, partitioning_cols=[RasterHourFeatures.month])


def get_raster_id(df):
    gser = geopandas.points_from_xy(df[um.PingFeatures.loc.lon], df[um.PingFeatures.loc.lat], crs="EPSG:4326").to_crs(
        "EPSG:23700"
    )
    _x, _y = [pd.Series(getattr(gser, coord) / 100).astype(int).astype(str) for coord in ["x", "y"]]
    return (_y + "-" + _x).values

def proc_gdf(gdf, min_count, min_duration, table):
    agg_df = (
        gdf.assign(
            **{
                RasterHourIndex.raster_id: get_raster_id,
                RasterHourIndex.hour: gdf[um.PingFeatures.datetime].dt.floor("h"),
            }
        )
        .groupby([um.PingFeatures.device_id, RasterHourIndex.raster_id, RasterHourIndex.hour])[
            um.PingFeatures.datetime
        ]
        .agg(["min", "max", "count"])
        .assign(duration_minutes=lambda df: (df["max"] - df["min"]).dt.total_seconds() / 60)
    )
    if agg_df.empty:
        return
    raster_df = (
        agg_df.loc[lambda df: (df["count"] >= min_count) & (df["duration_minutes"] >= min_duration)]
        .groupby(get_all_cols(RasterHourIndex))[[RasterHourFeatures.count]]
        .agg("count")
    )
    table.extend(
        raster_df.assign(
            **{RasterHourFeatures.month: raster_df.index.get_level_values(RasterHourIndex.hour).astype(str).str[:7]}
        ),
        try_dask=False,
    )


@pipereg.register(dependencies=[um.ping_table], outputs=[raster_table])
def step(min_count, min_duration):
    um.ping_table.trepo.map_partitions(
        partial(proc_gdf, min_count=min_count, min_duration=min_duration, table=raster_table), pbar=True
    )
