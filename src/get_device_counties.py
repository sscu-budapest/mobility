import zipfile
from pathlib import Path

import datazimmer as dz
import geopandas
import pandas as pd
import requests
from colassigner import get_all_cols
from metazimmer.gpsping import ubermedia as um


class DeviceCountyIndex(dz.IndexBase):
    device_id = str  # TODO: wet - this is taken from ping table.
    county = str


class DeviceCountyFeatures(dz.TableFeaturesBase):
    count = int
    rate = float


report_md_path = Path("reports", "local_device_distribution.md")

device_county_table = dz.ScruTable(DeviceCountyFeatures, index=DeviceCountyIndex)


def get_hungdf():
    gpath = Path("/tmp/gis_osm_places_a_free_1.shp")

    if not gpath.exists():
        resp = requests.get(
            "https://download.geofabrik.de/europe/hungary-latest-free.shp.zip"
        )
        hunpath = Path("/tmp/hun.zip")
        hunpath.write_bytes(resp.content)
        with zipfile.ZipFile(hunpath, "r") as zip_ref:
            zip_ref.extractall("/tmp")

    return (
        geopandas.read_file(gpath)
        .loc[
            lambda df: df["fclass"].isin(["county", "city"])
            & (df["name"] != "Bratislava"),
            :,
        ]
        .loc[:, ["name", "geometry"]]
        .set_index("name")
    )


def to_geo(df):
    return geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(
            df[um.PingFeatures.loc.lon], df[um.PingFeatures.loc.lat]
        ),
        crs="EPSG:4326",
    )


def gpjoin(df, gdf):
    return geopandas.sjoin(to_geo(df), gdf, op="within", how="left").rename(
        columns={"index_right": DeviceCountyIndex.county}
    )


def ping_gb(df, gdf):
    return (
        df.pipe(gpjoin, gdf)
        .groupby([um.PingFeatures.device_id, DeviceCountyIndex.county])[
            um.PingFeatures.datetime
        ]
        .count()
        .reset_index()
        .rename(columns={um.PingFeatures.datetime: DeviceCountyFeatures.count})
    )


def get_report_table(df):
    return (
        df.groupby(DeviceCountyIndex.county)[DeviceCountyFeatures.count]
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


@dz.register(dependencies=[um.ping_table], outputs=[device_county_table])
def step(min_locale_rate):

    ddf = um.ping_table.get_full_ddf()

    device_locale_count_df = (
        ddf.map_partitions(
            ping_gb,
            gdf=get_hungdf(),
            meta=pd.DataFrame(
                {
                    DeviceCountyIndex.device_id: pd.Series([], dtype="str"),
                    DeviceCountyIndex.county: pd.Series([], dtype="str"),
                    DeviceCountyFeatures.count: pd.Series([], dtype="int"),
                }
            ),
        )
        .groupby(get_all_cols(DeviceCountyIndex))[DeviceCountyFeatures.count]
        .sum()
        .compute()
        .to_frame()
        .assign(
            **{
                DeviceCountyFeatures.rate: lambda df: df[DeviceCountyFeatures.count]
                / df.groupby(um.PingFeatures.device_id)[
                    DeviceCountyFeatures.count
                ].transform("sum")
            }
        )
    )

    device_locale_count_df.pipe(device_county_table.replace_all)

    local_devices = device_locale_count_df.loc[
        lambda df: df[DeviceCountyFeatures.rate] >= min_locale_rate, :
    ]
    report_table = get_report_table(local_devices)
    report_md_path.write_text(
        "\n\n".join(
            [
                "# Distribution of Local Devices",
                f"Devices with at least {min_locale_rate} "
                "of their pings belonging to the same county",
                report_table.to_markdown(),
            ]
        )
    )
