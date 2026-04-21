from .clickhouse_conf import ClickhouseConf
from .conf import Conf
from .influx_conf import InfluxConf

__all__ = ["ClickhouseConf", "InfluxConf"]


def load_all() -> None:
    """Load all configs"""
    config: Conf
    for config in [ClickhouseConf, InfluxConf]:
        config.load()
