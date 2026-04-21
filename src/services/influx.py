from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from src.configs.influx_conf import InfluxConf
from src.models.raw import Raw, RAW_MEASUREMENT
from src.services.influx_query import QueryIF
import logging

logger = logging.getLogger(__name__)


class InfluxService:
    def __init__(self) -> None:
        self.conf = InfluxConf()

    def connect(self):
        self.client = InfluxDBClient(
            url=self.conf.url,
            token=self.conf.token,
            org=self.conf.org,
        )
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        self.query_api = self.client.query_api()

    def write_data(self, data: dict) -> None:
        raw = Raw(**data)
        self.write_api.write(bucket=self.conf.bucket, org=self.conf.org, record=raw.to_point())

    def write_batch(self, data_list: list[dict]) -> None:
        points = [Raw(**d).to_point() for d in data_list]
        self.write_api.write(bucket=self.conf.bucket, org=self.conf.org, record=points)

    def query_raw_data(self, start_time: int, end_time: int, tags: dict, batch_number: int):
        _LIMIT = 50
        offset = (batch_number - 1) * _LIMIT

        tag_filters = "".join(
            f' and r["{k}"] == "{v}"' for k, v in tags.items()
        )

        query = QueryIF.query_by_tags.format(
            bucket=self.conf.bucket,
            start_time=start_time,
            end_time=end_time,
            measurement=RAW_MEASUREMENT,
            tag_filters=tag_filters,
            limit=_LIMIT + 1,
            offset=offset,
        )

        result = self.query_api.query(query)

        rows = {}
        for table in result:
            for record in table.records:
                ts = record.get_time()
                if ts not in rows:
                    rows[ts] = {"timestamp": ts.isoformat()}
                    for k, v in record.values.items():
                        if not k.startswith("_") and k != "result" and k != "table":
                            rows[ts][k] = v
                rows[ts][record.get_field()] = record.get_value()

        rows = list(rows.values())
        return rows, len(rows) > _LIMIT

    def get_fields(self) -> list[str]:
        query = QueryIF.get_fields.format(
            bucket=self.conf.bucket,
            measurement=RAW_MEASUREMENT,
        )
        tables = self.query_api.query(query)
        return [record.get_value() for table in tables for record in table.records if record.get_value()]
