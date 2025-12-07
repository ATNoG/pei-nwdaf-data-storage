from datetime import datetime
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from src.configs.clickhouse_conf import ClickhouseConf
from src.models.processed_latency import ProcessedLatency
from src.services.db_service import DBService
from src.services.query import QueryCH

class ClickHouseService(DBService):
    def __init__(self) -> None:
        self.conf =   ClickhouseConf()
        self.client:  Client = None

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


    def query_processed_latency(self,
        start_time: datetime,
        end_time: datetime,
        cell_index: int,
        offset: int,
        limit: int
    ) -> list[ProcessedLatency]:
        """
        Query processed latency data from ClickHouse.

        Args:
            start_time: Filter by window_start_time >= start_time
            end_time: Filter by window_end_time <= end_time
            cell_index: Filter by specific cell index
            offset: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of ProcessedLatency objects
        """


        result = self.client.query(
            QueryCH.processed_latency,
            parameters={
                'start_time': start_time,
                'end_time': end_time,
                'cell_index': cell_index,
                'offset': offset,
                'limit': limit
            }
        )

        # Convert query results to ProcessedLatency objects
        processed_data = []
        for row in result.result_rows:
            data_dict = {
                'window_start_time': row[0],
                'window_end_time': row[1],
                'window_duration_seconds': row[2],
                'cell_index': row[3],
                'network': row[4],
                'rsrp_mean': row[5], 'rsrp_max': row[6], 'rsrp_min': row[7], 'rsrp_std': row[8],
                'sinr_mean': row[9], 'sinr_max': row[10], 'sinr_min': row[11], 'sinr_std': row[12],
                'rsrq_mean': row[13], 'rsrq_max': row[14], 'rsrq_min': row[15], 'rsrq_std': row[16],
                'latency_mean': row[17], 'latency_max': row[18], 'latency_min': row[19], 'latency_std': row[20],
                'cqi_mean': row[21], 'cqi_max': row[22], 'cqi_min': row[23], 'cqi_std': row[24],
                'primary_bandwidth': row[25],
                'ul_bandwidth': row[26],
                'sample_count': row[27]
            }
            processed_data.append(ProcessedLatency(**data_dict))

        return processed_data
