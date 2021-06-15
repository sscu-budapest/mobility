import pandas as pd
from colassigner import ColAssigner, allcols
from parquetranger import TableRepo
from tqdm import tqdm

from .data_locs import raw_root


class ParsedCols(ColAssigner):
    def dtime(self, df):
        return pd.to_datetime(df["date"] + " " + df["time"])

    def lon(self, df):
        return df["lon"].astype(float)

    def lat(self, df):
        return df["lat"].astype(float)

    def user(self, df):
        return df["user"]

    def month(self, df):
        return df[ParsedCols.dtime].dt.date.astype(str).str[:7]


month_tables = TableRepo(
    raw_root / "months", group_cols=ParsedCols.month, max_records=2_500_000
)


def dump_months(size=2_500_000):
    pbar = tqdm()
    rs = []
    with open(raw_path, "r") as fp:
        for i, line in enumerate(fp):
            rs.append(line.strip().split("\t"))
            if ((i + 1) % (size * 2)) == 0:
                pd.DataFrame(
                    rs, columns=["country", "user", "lat", "lon", "ts", "date", "time"]
                ).assign(**ParsedCols()).loc[:, allcols(ParsedCols)].pipe(
                    month_tables.extend
                )
                rs = []
                pbar.update()
