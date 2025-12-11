from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS, WriteApi
from influxdb_client.client.query_api import QueryApi
from src.configs.influx_conf import InfluxConf
from src.models.raw import Raw
from src.services.db_service import DBService
from src.services.influx_query import QueryIF

class InfluxService(DBService):
    def __init__(self) -> None:
        self.conf=      InfluxConf()
        self.connect()

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

    def get_data(self, batch_number:int = 1 ,batch_size:int = 50) -> list[Raw]:
        #TODO: add query
        return []

    def query_raw_data(self, start_time: int, end_time: int, cell_index: int, batch_number: int):
        _LIMIT = 50
        offset = (batch_number-1) * _LIMIT

        query = QueryIF.between.format(
            bucket=self.conf.bucket,
            start_time=start_time,
            end_time=end_time,
            cell_index=cell_index,
            limit=_LIMIT,
            offset=offset
        )

        result = self.query_api.query(query)

        # Convert FluxTable to list of dicts
        rows = []
        for table in result:
            for record in table.records:
                rows.append(record.values)

        return rows


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
