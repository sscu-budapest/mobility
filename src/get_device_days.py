from colassigner import Col, ColAssigner, get_all_cols
from sscutils import IndexBase, ScruTable, TableFeaturesBase

from .imported_namespaces import um
from .pipereg import pipereg


class DeviceDayFeatures(TableFeaturesBase):
    def __init__(self, am_start, am_end, pm_start, pm_end):
        self.am_start = am_start
        self.am_end = am_end
        self.pm_start = pm_start
        self.pm_end = pm_end

    def am_count(self, df) -> Col[int]:
        return (df[HourCol.hour] >= self.am_start) & (df[HourCol.hour] <= self.am_end)

    def pm_count(self, df) -> Col[int]:
        return (df[HourCol.hour] >= self.pm_start) & (df[HourCol.hour] <= self.pm_end)

    def other_count(self, df) -> Col[int]:
        return ~(df.loc[:, DeviceDayFeatures.am_count] | df.loc[:, DeviceDayFeatures.pm_count])


class DeviceDayIndex(IndexBase):
    device_id = str
    year_month = str
    dayofmonth = str


device_day_table = ScruTable(DeviceDayFeatures, partitioning_cols=[DeviceDayIndex.year_month])


class HourCol(ColAssigner):
    def hour(self, df):
        return df[um.PingFeatures.datetime].dt.hour


@pipereg.register(
    dependencies=[um.ping_table],
    outputs=[device_day_table],
)
def step(am_start, am_end, pm_start, pm_end):
    ping_ddf = um.ping_table.get_full_ddf()
    gb_cols = [um.PingFeatures.device_id, um.PingFeatures.year_month, um.PingFeatures.dayofmonth]
    (
        ping_ddf.pipe(HourCol())
        .pipe(DeviceDayFeatures(am_start, am_end, pm_start, pm_end))
        .groupby(gb_cols)[get_all_cols(DeviceDayFeatures)]
        .sum()
        .compute()
        .pipe(device_day_table.replace_all)
    )