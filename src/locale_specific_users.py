import zipfile
from pathlib import Path

import geopandas
import pandas as pd
import requests
from colassigner import ColAccessor
from sscutils import create_trepo_with_subsets

from .data_management import um_raw_cols as um_rc
from .data_management import um_trepos as um_t2
from .pipereg import pipereg


class LocaleCols(ColAccessor):
    county = "county"
    count = "count"
    rate = "rate"


report_md_path = Path("reports", "local_user_distribution.md")

local_users_table = create_trepo_with_subsets("local_users", prefix="locals", no_subsets=True)


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
        .loc[lambda df: df["fclass"].isin(["county", "city"]) & (df["name"] != "Bratislava"), :]
        .loc[:, ["name", "geometry"]]
        .set_index("name")
    )


def to_geo(df):
    return geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df[um_rc.PingCols.Location.lon], df[um_rc.PingCols.Location.lat]),
        crs="EPSG:4326",
    )


def gpjoin(df, gdf):
    return geopandas.sjoin(to_geo(df), gdf, op="within", how="left").rename(columns={"index_right": LocaleCols.county})


def ping_gb(df, gdf):
    return (
        df.pipe(gpjoin, gdf)
        .groupby([um_rc.PingCols.device_id, LocaleCols.county])[um_rc.PingCols.datetime]
        .count()
        .reset_index()
        .rename(columns={um_rc.PingCols.datetime: LocaleCols.count})
    )


def get_report_table(df):
    return (
        df.groupby(LocaleCols.county)[LocaleCols.count]
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


@pipereg.register(dependencies=[um_t2.pings_table], outputs=[local_users_table])
def get_local_users(min_rate):
    ddf = um_t2.pings_table.get_full_ddf()

    user_locale_count_df = (
        ddf.map_partitions(
            ping_gb,
            gdf=get_hungdf(),
            meta=pd.DataFrame(
                {
                    um_rc.PingCols.device_id: pd.Series([], dtype="str"),
                    LocaleCols.county: pd.Series([], dtype="str"),
                    LocaleCols.count: pd.Series([], dtype="int"),
                }
            ),
        )
        .groupby([um_rc.PingCols.device_id, LocaleCols.county])[LocaleCols.count]
        .sum()
        .compute()
        .to_frame()
    )

    local_users = user_locale_count_df.assign(
        **{
            LocaleCols.rate: lambda df: df[LocaleCols.count]
            / df.groupby(um_rc.PingCols.device_id)[LocaleCols.count].transform("sum")
        }
    ).loc[lambda df: df[LocaleCols.rate] >= min_rate, :]

    local_users.pipe(local_users_table.replace_all)

    report_table = get_report_table(local_users)
    report_md_path.write_text(
        "\n\n".join(
            [
                "# Distribution of Local Users",
                f"Users with at least {min_rate} of their pings belonging to the same county",
                report_table.to_markdown(),
            ]
        )
    )
