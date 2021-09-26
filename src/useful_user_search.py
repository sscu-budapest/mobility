from colassigner import ColAssigner
from sscutils import create_trepo_with_subsets

from .data_management import um_raw_cols as um_rc
from .data_management import um_trepos as um_t2
from .locale_specific_users import local_users_table
from .pipereg import pipereg

prefix = "user_filtered"

all_local_pings_table = create_trepo_with_subsets(
    "local_pings", group_cols=[um_rc.PingCols.month, um_rc.PingCols.dayofmonth], prefix=prefix, no_subsets=True
)
good_local_user_pings_table = create_trepo_with_subsets(
    "good_local_user_pings",
    group_cols=[um_rc.PingCols.month, um_rc.PingCols.dayofmonth],
    prefix=prefix,
    no_subsets=True,
)
good_local_user_good_day_pings_table = create_trepo_with_subsets(
    "good_local_user_good_day_pings", group_cols=[um_rc.PingCols.month], prefix=prefix, no_subsets=True
)

class HourCol(ColAssigner):

    def hour(self, df):
        return df[um_rc.PingCols.datetime].dt.hour


class TimeOfDayCols(ColAssigner):
    def __init__(self, am_start, am_end, pm_start, pm_end):
        super().__init__()
        self.am_start = am_start
        self.am_end = am_end
        self.pm_start = pm_start
        self.pm_end = pm_end

    def am(self, df):
        return (df[HourCol.hour] >= self.am_start) & (df[HourCol.hour] <= self.am_end)

    def pm(self, df):
        return (df[HourCol.hour] >= self.pm_start) & (df[HourCol.hour] <= self.pm_end)

    def other(self, df):
        return ~(self.am(df) | self.pm(df))


class ReliableCols(ColAssigner):
    def __init__(self, min_am, min_pm, min_sum):
        super().__init__()
        self.min_am = min_am
        self.min_pm = min_pm
        self.min_sum = min_sum

    def is_reliable(self, df):
        return (df.loc[:, TimeOfDayCols.am] >= self.min_am) & (df.loc[:, TimeOfDayCols.pm] >= self.min_pm)

    def is_good(self, df):
        # this knows that cols sum up for the full day
        return df.sum(axis=1) >= self.min_sum


@pipereg.register(
    dependencies=[um_t2.pings_table, local_users_table],
    outputs=[all_local_pings_table, good_local_user_pings_table, good_local_user_good_day_pings_table],
)
def useful_user_extraction(
    am_start, am_end, pm_start, pm_end, min_am, min_pm, min_sum, min_reliable_days, specific_to_locale
):
    local_users = (
        local_users_table.get_full_df()
        .loc[(slice(None), specific_to_locale), :]
        .reset_index()[um_rc.PingCols.device_id]
    )
    ddf = um_t2.pings_table.get_full_ddf().loc[lambda df: df[um_rc.PingCols.device_id].isin(local_users), :]

    # all_local_pings_table.replace_all(ddf)

    gb_cols = [um_rc.PingCols.device_id, um_rc.PingCols.month, um_rc.PingCols.dayofmonth]

    user_days = (
        ddf.assign(**HourCol()).assign(**TimeOfDayCols(am_start, am_end, pm_start, pm_end))
        .groupby(gb_cols)[[TimeOfDayCols.am, TimeOfDayCols.pm, TimeOfDayCols.other]]
        .sum()
        .assign(**ReliableCols(min_am, min_pm, min_sum))
    )

    good_users = (
        user_days.groupby(um_rc.PingCols.device_id)[ReliableCols.is_reliable]
        .sum()
        .loc[lambda s: s >= min_reliable_days]
        .compute()
        .index
    )

    user_days.reset_index().loc[
        lambda df: df[ReliableCols.is_good] & df[um_rc.PingCols.device_id].isin(good_users), gb_cols
    ].merge(ddf, how="inner").pipe(good_local_user_good_day_pings_table.replace_all)
