from colassigner import ColAccessor


class Loc(ColAccessor):
    lon = "lon"
    lat = "lat"


class PingCols(ColAccessor):
    devide_id = "device_id"
    datetime = "datetime"
    month = "month"
    dayofmonth = "dayofmonth"

    class Location(Loc):
        pass
