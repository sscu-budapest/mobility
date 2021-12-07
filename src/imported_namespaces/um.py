from datetime import datetime  # noqa: F401

from sscutils import BaseEntity, CompositeTypeBase, TableFactory, TableFeaturesBase


class Ping(BaseEntity):
    pass


class Coordinates(CompositeTypeBase):
    lon = float
    lat = float


class PingFeatures(TableFeaturesBase):
    device_id = str
    datetime = datetime
    year_month = str
    dayofmonth = str
    loc = Coordinates


table_factory = TableFactory("um")
ping_table = table_factory.create(
    features=PingFeatures,
    subject_of_records=Ping,
    partitioning_cols=["year_month", "dayofmonth"],
    max_partition_size=2000000,
)
