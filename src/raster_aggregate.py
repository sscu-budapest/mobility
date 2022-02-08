import datetime as dt
from functools import partial

import geopandas
import pandas as pd
from atqo import parallel_map
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
    gser = geopandas.points_from_xy(df[um.PingFeatures.loc.lon], df[um.PingFeatures.loc.lat], crs="EPSG:23700")
    _x, _y = [pd.Series(getattr(gser, coord) * 100).astype(int).astype(str) for coord in ["x", "y"]]
    return _x + "-" + _y


def duration_minutes(s):
    return (s.max() - s.min()).total_seconds() / 60


def proc_partition_path(part_path, min_count, min_duration):
    part_df = pd.read_parquet(part_path)
    raster_df = (
        part_df.assign(
            **{
                RasterHourIndex.raster_id: get_raster_id,
                RasterHourIndex.hour: part_df[um.PingFeatures.datetime].dt.floor("h"),
            }
        )
        .groupby([um.PingFeatures.device_id, RasterHourIndex.raster_id, RasterHourIndex.hour])[
            um.PingFeatures.datetime
        ]
        .agg([duration_minutes, "count"])
        .loc[lambda df: (df["count"] >= min_count) & (df["duration_minutes"] >= min_duration)]
        .groupby(get_all_cols(RasterHourIndex))[[RasterHourFeatures.count]]
        .agg("count")
    )
    raster_table.extend(
        raster_df.assign(
            **{RasterHourFeatures.month: raster_df.index.get_level_values(RasterHourIndex.hour).astype(str).str[:7]}
        ),
        try_dask=False,
    )


def regroup(dfp):
    # not pretty :()
    _df = pd.read_parquet(dfp)
    _df.groupby(_df.index.names).agg({RasterHourFeatures.month: "first", RasterHourFeatures.count: "sum"}).to_parquet(
        dfp
    )


@pipereg.register(dependencies=[um.ping_table], outputs=[raster_table])
def step(min_count, min_duration):
    parallel_map(
        partial(proc_partition_path, min_count=min_count, min_duration=min_duration),
        um.ping_table.trepo.paths,
        dist_api="mp",
        pbar=True,
        raise_errors=True
    )
    parallel_map(regroup, raster_table.trepo.paths, dist_api="mp")
