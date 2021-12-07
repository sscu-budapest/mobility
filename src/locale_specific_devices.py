import zipfile
from pathlib import Path

import geopandas
import pandas as pd
import requests
from sscutils import TableFeaturesBase, ScruTable

from .imported_namespaces import um
from .pipereg import pipereg


class LocalDeviceFeatures(TableFeaturesBase):
    county = str
    count = int
    rate = float


report_md_path = Path("reports", "local_user_distribution.md")

local_user_table = ScruTable(LocalDeviceFeatures)


def get_hungdf():
    gpath = Path("/tmp/gis_osm_places_a_free_1.shp")

    if not gpath.exists():
        resp = requests.get("https://download.geofabrik.de/europe/hungary-latest-free.shp.zip")
        hunpath = Path("/tmp/hun.zip")
        hunpath.write_bytes(resp.content)
        with zipfile.ZipFile(hunpath, "r") as zip_ref:
            zip_ref.extractall("/tmp")

    return (
        geopandas.read_file(gpath)
        .loc[
            lambda df: df["fclass"].isin(["county", "city"]) & (df["name"] != "Bratislava"),
            :,
        ]
        .loc[:, ["name", "geometry"]]
        .set_index("name")
    )


def to_geo(df):
    return geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df[um.PingFeatures.loc.lon], df[um.PingFeatures.loc.lat]),
        crs="EPSG:4326",
    )


def gpjoin(df, gdf):
    return geopandas.sjoin(to_geo(df), gdf, op="within", how="left").rename(columns={"index_right": LocalDeviceFeatures.county})


def ping_gb(df, gdf):
    return (
        df.pipe(gpjoin, gdf)
        .groupby([um.PingFeatures.device_id, LocalDeviceFeatures.county])[um.PingFeatures.datetime]
        .count()
        .reset_index()
        .rename(columns={um.PingFeatures.datetime: LocalDeviceFeatures.count})
    )


def get_report_table(df):
    return (
        df.groupby(LocalDeviceFeatures.county)[LocalDeviceFeatures.count]
        .agg(["sum", "count"])
        .rename(columns={"sum": "Ping Count", "count": "Device Count"})
        .pipe(
            lambda df: pd.concat(
                [
                    df,
                    (df / df.sum())
                    .rename(columns=lambda s: s.replace("Count", "Rate"))
                    .pipe(lambda df: df * 100)
                    .round(1)
                    .astype(str)
                    + "%",
                ],
                axis=1,
            )
        )
        .sort_values("Ping Count", ascending=False)
    )


@pipereg.register(dependencies=[um.ping_table], outputs=[local_user_table])
def get_local_users(min_rate):

    ddf = um.pings_table.get_full_ddf()

    device_locale_count_df = (
        ddf.map_partitions(
            ping_gb,
            gdf=get_hungdf(),
            meta=pd.DataFrame(
                {
                    um.PingFeatures.device_id: pd.Series([], dtype="str"),
                    LocalDeviceFeatures.county: pd.Series([], dtype="str"),
                    LocalDeviceFeatures.count: pd.Series([], dtype="int"),
                }
            ),
        )
        .groupby([um.PingFeatures.device_id, LocalDeviceFeatures.county])[LocalDeviceFeatures.count]
        .sum()
        .compute()
        .to_frame()
    )

    local_devices = device_locale_count_df.assign(
        **{
            LocalDeviceFeatures.rate: lambda df: df[LocalDeviceFeatures.count]
            / df.groupby(um.PingFeatures.device_id)[LocalDeviceFeatures.count].transform("sum")
        }
    ).loc[lambda df: df[LocalDeviceFeatures.rate] >= min_rate, :]

    local_devices.pipe(local_user_table.replace_all)

    report_table = get_report_table(local_devices)
    report_md_path.write_text(
        "\n\n".join(
            [
                "# Distribution of Local Users",
                f"Users with at least {min_rate} of their pings belonging to the same county",
                report_table.to_markdown(),
            ]
        )
    )
