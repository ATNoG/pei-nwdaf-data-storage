from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse import ClickHouseService
from src.configs.influx_conf import InfluxConf
from src.services.influx import InfluxService


class ClickHouse:
    conf = ClickhouseConf()
    service = ClickHouseService()


class Influx:
    conf = InfluxConf()
    service = InfluxService()
