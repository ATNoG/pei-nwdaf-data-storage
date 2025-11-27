import clickhouse_connect
from clickhouse_connect.driver.client import Client
from src.configs.clickhouse_conf import ClickhouseConf
from src.services.db_service import DBService

class ClickHouseService(DBService):
    def __init__(self) -> None:
        self.conf =   ClickhouseConf()
        self.client:  Client

    def connect(self):
        self.client = clickhouse_connect.get_client(
            host     = self.conf.host,
            port     = self.conf.port,
            username = self.conf.user,
            password = self.conf.password,
        )

    def get_data(self, batch_number: int = 1, batch_size: int = 50) -> list:
        return []

    def write_data(self, data: dict) -> None:
        pass

    def write_batch(self, data_list: list[dict]) -> None:
        pass
