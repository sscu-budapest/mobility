import zipfile
from pathlib import Path

import datazimmer as dz
import geopandas as gpd
import pandas as pd
import requests

from .filtered_stops import Stop, filtered_stop_table
from .util import to_geo


class DeviceCounty(dz.AbstractEntity):
    device_id = dz.Index & str
    county = dz.Index & str
    count = int
    rate = float


device_dist_report = dz.ReportFile("local_device_distribution.md")
device_county_table = dz.ScruTable(DeviceCounty)


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
        gpd.read_file(gpath)
        .loc[
            lambda df: df["fclass"].isin(["county", "city"])
            & (df["name"] != "Bratislava"),
            :,
        ]
        .loc[:, ["name", "geometry"]]
        .set_index("name")
    )


def gpjoin(df, gdf):
    return gpd.sjoin(to_geo(df, Stop.center), gdf, op="within", how="left").rename(
        columns={"index_right": DeviceCounty.county}
    )


def stop_gb(df, gdf):
    return (
        df.pipe(gpjoin, gdf)
        .groupby([Stop.device_id, DeviceCounty.county])
        .agg(**{DeviceCounty.count: pd.NamedAgg(Stop.n_events, "sum")})
        .reset_index()
    )


def get_report_table(df):
    return (
        df.groupby(DeviceCounty.county)[DeviceCounty.count]
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


@dz.register(dependencies=[filtered_stop_table], outputs=[device_county_table])
def step(min_locale_rate):

    ddf = filtered_stop_table.get_full_ddf()

    device_locale_count_df = (
        ddf.map_partitions(
            stop_gb,
            gdf=get_hungdf(),
            meta=pd.DataFrame(
                {
                    DeviceCounty.device_id: pd.Series([], dtype="str"),
                    DeviceCounty.county: pd.Series([], dtype="str"),
                    DeviceCounty.count: pd.Series([], dtype="int"),
                }
            ),
        )
        .groupby(device_county_table.index_cols)[DeviceCounty.count]
        .sum()
        .compute()
        .to_frame()
        .assign(
            **{
                DeviceCounty.rate: lambda df: df[DeviceCounty.count]
                / df.groupby(Stop.device_id)[DeviceCounty.count].transform("sum")
            }
        )
    )

    device_locale_count_df.pipe(device_county_table.replace_all)

    local_devices = device_locale_count_df.loc[
        lambda df: df[DeviceCounty.rate] >= min_locale_rate, :
    ]
    report_table = get_report_table(local_devices)
    device_dist_report.write_text(
        "\n\n".join(
            [
                "# Distribution of Local Devices",
                f"Devices with at least {min_locale_rate} "
                "of their pings belonging to the same county",
                report_table.to_markdown(),
            ]
        )
    )
