from enum import Enum


class DataSource(Enum):
    """Available market _data sources."""
    ORACLE = "oracle"
    TIMESCALE = "timescale"
    BLOOMBERG = "bloomberg"
    MOCK = "mock"   # useful for local testing


