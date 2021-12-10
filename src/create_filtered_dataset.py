from colassigner import ColAssigner
from sscutils import ScruTable

from .get_device_counties import (
    DeviceCountyFeatures,
    DeviceCountyIndex,
    device_county_table,
)
from .get_device_days import DeviceDayFeatures, DeviceDayIndex, device_day_table
from .imported_namespaces import um
from .pipereg import pipereg


class FilteredPingFeatures(um.PingFeatures):
    pass


class ReliableCols(ColAssigner):
    def __init__(self, min_am, min_pm, min_sum):
        super().__init__()
        self.min_am = min_am
        self.min_pm = min_pm
        self.min_sum = min_sum

    def is_eventful(self, df):
        # this knows that cols sum up for the full day
        return df.sum(axis=1) >= self.min_sum

    def is_reliable(self, df):
        return (
            (df.loc[:, DeviceDayFeatures.am_count] >= self.min_am)
            & (df.loc[:, DeviceDayFeatures.pm_count] >= self.min_pm)
            & df.loc[:, ReliableCols.is_eventful]
        )


filtered_ping_table = ScruTable(FilteredPingFeatures, partitioning_cols=[um.PingFeatures.year_month])


@pipereg.register(
    dependencies=[um.ping_table, device_county_table, device_day_table],
    outputs=[filtered_ping_table],
)
def step(min_am, min_pm, min_sum, min_reliable_days, specific_to_locale, min_locale_rate):

    local_devices = (
        device_county_table.get_full_df()
        .loc[(specific_to_locale, slice(None)), :]
        .groupby(DeviceCountyIndex.device_id)
        .sum()
        .loc[
            lambda df: (df[DeviceCountyFeatures.rate] >= min_locale_rate)
            & (df[DeviceCountyFeatures.count] >= (min_sum * min_reliable_days * min_locale_rate)),
            :,
        ]
        .index
    )

    reliable_local_df = (
        device_day_table.get_full_df()
        .loc[(local_devices, slice(None), slice(None)), :]
        .pipe(ReliableCols(min_am, min_pm, min_sum))
        .loc[lambda df: df[ReliableCols.is_reliable], :]
    )

    good_devices = (
        reliable_local_df.groupby(DeviceDayIndex.device_id)[ReliableCols.is_reliable]
        .sum()
        .loc[lambda s: s >= min_reliable_days]
        .index
    )

    merge_cols = [um.PingFeatures.device_id, um.PingFeatures.year_month, um.PingFeatures.dayofmonth]

    (
        um.ping_table.get_full_ddf()
        .merge(
            reliable_local_df.reset_index().loc[
                lambda ddf: ddf[um.PingFeatures.device_id].isin(good_devices), merge_cols
            ],
            how="inner",
        )
        .compute()
        .pipe(filtered_ping_table.replace_all, parse=False)
        # TODO wrong metadata gets to bedrock
    )
