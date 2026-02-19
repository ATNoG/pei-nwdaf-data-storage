from .clickhouse_conf import ClickhouseConf
from .conf import Conf
from .influx_conf import InfluxConf
from .schema_conf import SchemaConf

__all__ = ["ClickhouseConf", "InfluxConf", "SchemaConf"]


def load_all() -> None:
    """Load all configs"""
    config: Conf
    for config in [ClickhouseConf, InfluxConf, SchemaConf]:
        config.load()
