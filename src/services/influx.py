from posix import times
from influxdb_client.client.flux_table import FluxRecord, FluxTable
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS
from src.configs.influx_conf import InfluxConf
from src.models.raw import Raw, RAW_MEASUREMENT
from src.services.influx_query import QueryIF
import logging

logger = logging.getLogger(__name__)

class InfluxService:
    def __init__(self) -> None:
        self.conf=      InfluxConf()

    def connect(self):
        """Init client"""
        self.client = InfluxDBClient(
            url   = self.conf.url,
            token = self.conf.token,
            org   = self.conf.org,
        )

        # get iteration apis
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        self.query_api = self.client.query_api()

    def query_raw_data(self, start_time: int, end_time: int, cell_index: int, batch_number: int):
        _LIMIT = 50
        offset = (batch_number-1) * _LIMIT

        query = QueryIF.between.format(
            bucket=self.conf.bucket,
            start_time=start_time,
            end_time=end_time,
            cell_index=cell_index,
            limit=_LIMIT+1,
            offset=offset
        )
        print(query)
        result:list[FluxTable] = self.query_api.query(query)
        print(result)
        # Convert FluxTable to list of dicts
        rows = {}
        table:FluxTable
        record:FluxRecord
        for table in result:
            for record in table.records:
                timestamp = record.get_time()
                if timestamp not in rows:
                    rows[timestamp] = {k: v for k, v in record.values.items() if not k.startswith('_')}
                    rows[timestamp]["timestamp"] = timestamp
                    for col in record.values.keys():
                        if col.startswith('_'):
                            rows[timestamp][col] = None

                field = record.get_field()
                value = record.get_value()
                rows[timestamp][field] = value

        rows = list(rows.values())
        return rows, len(rows)>_LIMIT


    def write_data(self, data: dict) -> None :
        """Write a single record"""
        raw = Raw(**data)
        point = raw.to_point()
        self.write_api.write(bucket=self.conf.bucket, org = self.conf.org ,record = point)

    def write_batch(self, data_list:list[dict]) -> None :
        """Write a list of record"""
        raw_list = [Raw(**d) for d in data_list]
        points = [r.to_point() for r in raw_list]
        self.write_api.write(bucket=self.conf.bucket, org = self.conf.org ,record = points)

    def get_known_cells(self) -> list[int]:
        """Returns a list of known cell indexes"""

        query = QueryIF.get_known_cells.format(
            bucket=self.conf.bucket,
            measurement=RAW_MEASUREMENT
        )

        tables = self.query_api.query(query)

        cells = []
        for table in tables:
            for record in table.records:
                try:
                    cells.append(int(record["_value"]))
                except (TypeError, ValueError):
                    pass

        return set(cells)
