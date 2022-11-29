import datetime as dt

import datazimmer as dz
from colassigner import get_all_cols
from metazimmer.gpsping.meta import Coordinates

from .semantic_stops import SemanticStop, semantic_stop_table


class SemanticTemporalInfo(dz.AbstractEntity):

    info_name = dz.Index & str  # categorical home / work
    time_bin = dz.Index & dt.datetime
    device_id = dz.Index & str
    loc = Coordinates


semantic_info_table = dz.ScruTable(SemanticTemporalInfo)


@dz.register(dependencies=[semantic_stop_table], outputs=[semantic_info_table])
def step():

    renamer = dict(
        zip(*map(get_all_cols, [SemanticStop.center, SemanticTemporalInfo.loc]))
    )
    name_map = {"home": SemanticStop.home, "work": SemanticStop.work}

    for df in semantic_stop_table.dfs:
        for info_name, place in name_map.items():
            semantic_info_table.extend(
                df.loc[df[place.identified], :]
                .drop_duplicates(subset=[SemanticStop.time_bin, SemanticStop.device_id])
                .assign(**{SemanticTemporalInfo.info_name: info_name})
                .rename(columns=renamer)
            )
