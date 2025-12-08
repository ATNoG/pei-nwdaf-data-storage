from datetime import datetime, timezone
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from src.configs.clickhouse_conf import ClickhouseConf
from src.models.processed_latency import ProcessedLatency
from src.services.db_service import DBService
from src.services.query import QueryCH


def transform_processor_output(data: dict) -> dict:
    """
    Transform processor's nested output format to flat storage format.

    Processor format:
    {
        "type": "latency",
        "cell_index": 123,
        "window_start": 1733684400,  // Unix timestamp in seconds
        "window_end": 1733684410,
        "rsrp": {"mean": -85.5, "max": -80, "min": -90, "std": 2.5, "samples": 100},
        "mean_latency": {...},
        ...
    }

    Storage format:
    {
        "window_start_time": datetime,
        "window_end_time": datetime,
        "window_duration_seconds": 10.0,
        "cell_index": 123,
        "rsrp_mean": -85.5,
        "rsrp_max": -80,
        ...
    }
    """
    # Extract timestamps and convert to datetime
    window_start = data.get("window_start")
    window_end = data.get("window_end")

    if window_start is None or window_end is None:
        raise ValueError("Missing window_start or window_end in processor output")

    window_start_dt = datetime.fromtimestamp(window_start, tz=timezone.utc)
    window_end_dt = datetime.fromtimestamp(window_end, tz=timezone.utc)
    window_duration = window_end - window_start

    # Skip records with no samples
    sample_count = data.get("sample_count", 0)
    if sample_count == 0:
        raise ValueError("Cannot store record with sample_count = 0")

    # Build flat structure
    transformed = {
        "window_start_time": window_start_dt,
        "window_end_time": window_end_dt,
        "window_duration_seconds": float(window_duration),
        "cell_index": data.get("cell_index"),
        "network": data.get("network") or "Unknown",
        "sample_count": sample_count,
        "primary_bandwidth": data.get("primary_bandwidth"),
        "ul_bandwidth": data.get("ul_bandwidth"),
    }

    # Flatten nested metric structures
    # Processor uses "mean_latency", storage uses "latency"
    metric_mapping = {
        "rsrp": "rsrp",
        "sinr": "sinr",
        "rsrq": "rsrq",
        "mean_latency": "latency",  # Map mean_latency -> latency
        "cqi": "cqi"
    }

    for processor_key, storage_key in metric_mapping.items():
        metric_data = data.get(processor_key)
        if metric_data and isinstance(metric_data, dict):
            transformed[f"{storage_key}_mean"] = metric_data.get("mean")
            transformed[f"{storage_key}_max"] = metric_data.get("max")
            transformed[f"{storage_key}_min"] = metric_data.get("min")
            transformed[f"{storage_key}_std"] = metric_data.get("std")

    return transformed

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
        """Write a single processed latency record to ClickHouse"""
        try:
            # Transform nested processor format to flat storage format
            transformed = transform_processor_output(data)
            processed = ProcessedLatency(**transformed)
            record_dict = processed.to_dict()

            # Convert dict to list in column order for ClickHouse
            record_row = [
                record_dict['window_start_time'],
                record_dict['window_end_time'],
                record_dict['window_duration_seconds'],
                record_dict['cell_index'],
                record_dict['network'],
                record_dict['rsrp_mean'], record_dict['rsrp_max'], record_dict['rsrp_min'], record_dict['rsrp_std'],
                record_dict['sinr_mean'], record_dict['sinr_max'], record_dict['sinr_min'], record_dict['sinr_std'],
                record_dict['rsrq_mean'], record_dict['rsrq_max'], record_dict['rsrq_min'], record_dict['rsrq_std'],
                record_dict['latency_mean'], record_dict['latency_max'], record_dict['latency_min'], record_dict['latency_std'],
                record_dict['cqi_mean'], record_dict['cqi_max'], record_dict['cqi_min'], record_dict['cqi_std'],
                record_dict['primary_bandwidth'],
                record_dict['ul_bandwidth'],
                record_dict['sample_count']
            ]

            # Use async_insert for better performance
            self.client.insert(
                'analytics.processed_latency',
                [record_row],
                settings={'async_insert': 1, 'wait_for_async_insert': 0}
            )
        except ValueError as e:
            # Skip records with no samples (sample_count = 0)
            if "sample_count = 0" in str(e):
                return
            raise Exception(f"Failed to write to ClickHouse: {e}")
        except Exception as e:
            raise Exception(f"Failed to write to ClickHouse: {e}")

    def write_batch(self, data_list: list[dict]) -> None:
        """Write multiple processed latency records to ClickHouse"""
        try:
            # Transform each record from nested processor format to flat storage format
            # Skip records with sample_count = 0
            transformed_list = []
            for d in data_list:
                try:
                    transformed = transform_processor_output(d)
                    transformed_list.append(transformed)
                except ValueError as e:
                    if "sample_count = 0" not in str(e):
                        raise

            # Skip batch if no valid records
            if not transformed_list:
                return

            processed_list = [ProcessedLatency(**d) for d in transformed_list]

            # Convert each dict to list in column order for ClickHouse
            record_rows = []
            for p in processed_list:
                record_dict = p.to_dict()
                record_row = [
                    record_dict['window_start_time'],
                    record_dict['window_end_time'],
                    record_dict['window_duration_seconds'],
                    record_dict['cell_index'],
                    record_dict['network'],
                    record_dict['rsrp_mean'], record_dict['rsrp_max'], record_dict['rsrp_min'], record_dict['rsrp_std'],
                    record_dict['sinr_mean'], record_dict['sinr_max'], record_dict['sinr_min'], record_dict['sinr_std'],
                    record_dict['rsrq_mean'], record_dict['rsrq_max'], record_dict['rsrq_min'], record_dict['rsrq_std'],
                    record_dict['latency_mean'], record_dict['latency_max'], record_dict['latency_min'], record_dict['latency_std'],
                    record_dict['cqi_mean'], record_dict['cqi_max'], record_dict['cqi_min'], record_dict['cqi_std'],
                    record_dict['primary_bandwidth'],
                    record_dict['ul_bandwidth'],
                    record_dict['sample_count']
                ]
                record_rows.append(record_row)

            # Use async_insert for better performance
            self.client.insert(
                'analytics.processed_latency',
                record_rows,
                settings={'async_insert': 1, 'wait_for_async_insert': 0}
            )
        except Exception as e:
            raise Exception(f"Failed to batch write to ClickHouse: {e}")


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
