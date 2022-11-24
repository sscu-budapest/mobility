from collections import defaultdict

import datazimmer as dz
import pandas as pd
from metazimmer.gpsping.ubermedia.raw_proc import ping_table

from .filtered_stops import filtered_stop_table
from .initial_stops import Stop, stop_table
from .speed_based_filter import filtered_ping_table

stop_report = dz.ReportFile("stop_look.md")


@dz.register(
    dependencies=[stop_table, ping_table, filtered_stop_table],
    outputs_nocache=[stop_report],
)
def results():
    report_df = (
        pd.DataFrame(
            {
                k: get_stats_of_table(tab)
                for k, tab in [
                    ("Filtered Stop Table", filtered_stop_table),
                    ("Stop Table", stop_table),
                    ("Filtered Ping Table", filtered_ping_table),
                    ("Ping Table", ping_table),
                ]
            }
        )
        .assign(
            StopRate=lambda df: df["Stop Table"] / df["Filtered Ping Table"],
            StopFilterRate=lambda df: df["Filtered Stop Table"] / df["Stop Table"],
            PingFilterRate=lambda df: df["Filtered Ping Table"] / df["Ping Table"],
        )
        .applymap(_stringify)
    )

    stop_report.write_text(report_df.to_markdown())


def get_stats_of_table(table: dz.ScruTable):
    dset = set()
    dic = defaultdict(lambda: 0)
    for odic in table.map_partitions(fun=stats_of_df):
        dset.update(odic.pop("device-set"))
        for k, v in odic.items():
            dic[k] += v
    return {"Unique Device": len(dset)} | dic


def stats_of_df(df: pd.DataFrame):
    if Stop.n_events in df.columns:
        ping_c = df[Stop.n_events].sum()
    else:
        ping_c = df.shape[0]
    return {
        "Record Count": df.shape[0],
        "device-set": set(df[Stop.device_id]),
        "Ping Count": ping_c,
    }


def _stringify(e):
    if e <= 1:
        return f"{100.0 * e:.2f}%"
    for exp, suff in [(9, "G"), (6, "M"), (3, "k")]:
        if e > 10**exp:
            return f"{e / 10 ** exp:.2f}{suff}"
    return int(e)
