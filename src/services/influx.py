from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import ASYNCHRONOUS, WriteApi
from influxdb_client.client.query_api import QueryApi
from src.configs.influx_conf import InfluxConf
from src.models.raw import Raw
from src.services.db_service import DBService

class InfluxService(DBService):
    def __init__(self) -> None:
        self.conf=      InfluxConf()
        self.client:    InfluxDBClient
        self.write_api: WriteApi
        self.query_api: QueryApi

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
