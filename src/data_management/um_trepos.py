from sscutils import create_trepo_with_subsets

from .um_raw_cols import PingCols

pings_table = create_trepo_with_subsets(
    "pings", group_cols=[PingCols.month, PingCols.dayofmonth], max_records=2_000_000, prefix="um"
)
