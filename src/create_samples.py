from pathlib import Path

import dask.dataframe as dd
from parquetranger import TableRepo

from .data_dumps import ParsedCols, month_tables
from .pipeline_registry import pipereg
from .util import get_dask_client

DAYOFWEEK_COL = "dayofweek"


sample_dir = Path("data", "sample")

covid_sample, non_covid_sample = [
    TableRepo(sample_dir / name, group_cols=DAYOFWEEK_COL, max_records=300_000)
    for name in ["cov-week", "non-cov-week"]
]


@pipereg.register(outputs=[sample_dir])
def dump_samples():
    get_dask_client()
    for month, week_trepo in [("2019-11", non_covid_sample), ("2020-11", covid_sample)]:

        ddf = dd.read_parquet([p for p in month_tables.paths if month in p])
        weeks = ddf.loc[:, ParsedCols.dtime].dt.isocalendar().week
        wddf = ddf.loc[weeks == (weeks.min() + 1), :].assign(
            **{DAYOFWEEK_COL: lambda df: df.loc[:, ParsedCols.dtime].dt.dayofweek}
        )
        week_trepo.replace_all(wddf)
