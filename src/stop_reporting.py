import datazimmer as dz
import pandas as pd
from metazimmer.gpsping.ubermedia import ping_table

from .filtered_stops import filtered_stop_table
from .initial_stops import Stop, stop_table
from .speed_based_filter import filtered_ping_table
from .util import get_client

stop_report = dz.ReportFile("stop_look.md")


@dz.register(
    dependencies=[stop_table, ping_table, filtered_stop_table],
    outputs_nocache=[stop_report],
)
def results():
    def get_stats(table):
        ddf = table.get_full_ddf()
        if Stop.n_events in ddf.columns:
            ping_c = ddf[Stop.n_events].sum()
        else:
            ping_c = ddf.shape[0]
        return {
            "Record Count": ddf.shape[0],
            "Unique Device": ddf[Stop.device_id].nunique(),
            "Ping Count": ping_c,
        }

    client = get_client()

    report_df = (
        pd.DataFrame(
            client.compute(
                {
                    k: get_stats(tab)
                    for k, tab in [
                        ("Filtered Stop Table", filtered_stop_table),
                        ("Stop Table", stop_table),
                        ("Filtered Ping Table", filtered_ping_table),
                        ("Ping Table", ping_table),
                    ]
                }
            ).result()
        )
        .assign(
            StopRate=lambda df: df["Stop Table"] / df["Filtered Ping Table"],
            StopFilterRate=lambda df: df["Filtered Stop Table"] / df["Stop Table"],
            PingFilterRate=lambda df: df["Filtered Ping Table"] / df["Ping Table"],
        )
        .applymap(_stringify)
    )

    stop_report.write_text(report_df.to_markdown())


def _stringify(e):
    if e <= 1:
        return f"{100.0 * e:.2f}%"
    for exp, suff in [(9, "G"), (6, "M"), (3, "k")]:
        if e > 10**exp:
            return f"{e / 10 ** exp:.2f}{suff}"
    return int(e)
