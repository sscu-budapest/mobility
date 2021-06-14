import pandas as pd
from tqdm import tqdm

from .data_locs import parts_root, raw_path


def dump_parquet_parts(size=2_500_000):

    pbar = tqdm()
    rs = []
    with open(raw_path, "r") as fp:
        for i, line in enumerate(fp):
            rs.append(line.strip().split("\t"))
            if ((i + 1) % size) == 0:
                pd.DataFrame(
                    rs, columns=["country", "user", "lat", "lon", "ts", "date", "time"]
                ).assign(
                    dtime=lambda df: (df["date"] + " " + df["time"]).pipe(
                        pd.to_datetime
                    ),
                    lon=lambda df: df["lon"].astype(float),
                    lat=lambda df: df["lat"].astype(float),
                ).drop(
                    ["country", "ts", "date", "time"], axis=1
                ).to_parquet(
                    parts_root / f"{ i // size}.parquet"
                )
                rs = []
                pbar.update()


def dump_months():
    pass

def dump_users():
    pass
