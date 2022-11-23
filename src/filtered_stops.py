from functools import partial

import datazimmer as dz

from .initial_stops import Stop, stop_table

filtered_stop_table = dz.ScruTable(Stop, partitioning_cols=[Stop.device_group])


@dz.register(
    dependencies=[stop_table],
    outputs=[filtered_stop_table],
)
def step(min_stops_to_count, min_destinations):
    pfun = partial(
        _filter_to_devices, min_stops=min_stops_to_count, min_dests=min_destinations
    )
    list(stop_table.map_partitions(fun=pfun, pbar=True))


def _filter_to_devices(df, min_stops, min_dests):
    good_devices = (
        df.loc[~df[Stop.is_between_stops], :]
        .groupby([Stop.device_id, Stop.place_label])[Stop.n_events]
        .agg(["count", "sum"])
        .reset_index()
        .loc[lambda df: (df["count"] > min_stops), :]
        .groupby(Stop.device_id)[Stop.place_label]
        .nunique()
        .loc[lambda s: s >= min_dests]
        .index
    )
    filtered_stop_table.extend(df.loc[df[Stop.device_id].isin(good_devices), :])
