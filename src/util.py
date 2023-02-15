import geopandas as gpd
import h3pandas  # noqa: F401
from geopandas.array import GeometryArray
from metazimmer.gpsping import meta
from metazimmer.gpsping.minor_report import dategroup


def to_geo(df, loc_cols: meta.Coordinates = meta.GpsPing.loc):
    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[loc_cols.lon], df[loc_cols.lat]),
        crs="EPSG:4326",
    )


def localize(df, cols: meta.Coordinates, local_crs="EPSG:23700") -> GeometryArray:
    return gpd.points_from_xy(df[cols.lon], df[cols.lat], crs="EPSG:4326").to_crs(
        local_crs
    )


def gdf_sjoin(_df, gdf: gpd.GeoDataFrame, loc_cols=meta.ExtendedPing.loc, how="inner"):
    return gdf.sjoin(to_geo(_df, loc_cols), how=how)


def colfilter(df, fset, col=meta.ExtendedPing.device_id):
    return df.loc[df[col].isin(fset), :]


def filtered_count(df, fset, col=meta.ExtendedPing.device_id):
    return colfilter(df, fset, col).pipe(dategroup)
