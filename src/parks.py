from metazimmer.gpsping.meta import ExtendedPing

from .util import gdf_sjoin


def write_part(_df, gdf, home_df, out_trepo):
    mdf = gdf_sjoin(_df, gdf.loc[:, ["OBJECTID", "geometry"]]).drop(
        ["geometry"], axis=1
    )
    if mdf.empty:
        return
    rel_homes = home_df.loc[
        (mdf[ExtendedPing.device_id].unique(), "home", slice(None)), :
    ]
    out_trepo.extend(
        mdf.drop("index_right", axis=1)
        .assign(
            time_bin=lambda df: df[ExtendedPing.datetime]
            .dt.to_period("W")
            .dt.to_timestamp()
        )
        .merge(
            rel_homes.rename(columns=lambda s: f"last_home__{s}").reset_index(),
            how="left",
        )
        .sort_values(ExtendedPing.datetime)
        .groupby(ExtendedPing.device_id, group_keys=False)
        .apply(lambda gdf: gdf.fillna(method="ffill"))
    )
