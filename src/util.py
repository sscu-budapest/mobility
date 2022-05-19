import shutil
from pathlib import Path

import geopandas as gpd
import h3pandas  # noqa: F401
from distributed.client import Client
from geopandas.array import GeometryArray
from metazimmer.gpsping import ubermedia as um


def to_geo(df, loc_cols: um.Coordinates = um.GpsPing.loc):
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[loc_cols.lon], df[loc_cols.lat]),
        crs="EPSG:4326",
    )


def get_client():
    shutil.make_archive("src", "zip", ".", base_dir="src")

    try:
        client = Client.current()
    except ValueError:
        client = Client()
    client.upload_file("src.zip")
    print("CLIENT", client)
    Path("src.zip").unlink()
    return client


def localize(df, cols: um.Coordinates, local_crs="EPSG:23700") -> GeometryArray:
    return gpd.points_from_xy(df[cols.lon], df[cols.lat], crs="EPSG:4326").to_crs(
        local_crs
    )
