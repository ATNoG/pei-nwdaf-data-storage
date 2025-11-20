from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
from influxdb_client.client.query_api import QueryApi
from src.configs.influx import InfluxConf
from src.models.raw import Raw

class InfluxService:
    def __init__(self) -> None:
        self.conf=      InfluxConf
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
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def get_data(self, batch_number:int = 1 ,batch_size:int = 50) -> list[Raw]:
        #TODO: add query
        return []

    def write_data(self, data:Raw) -> None :
        """Write a single record"""
        point = data.to_point()
        self.write_api.write(bucket=self.conf.bucket, org = self.conf.org ,record = point)

    def write_batch(self, data_list:list[Raw]) -> None :
        """Write a list of record"""
        points = [d.to_point() for d in data_list]
        self.write_api.write(bucket=self.conf.bucket, org = self.conf.org ,record = points)
