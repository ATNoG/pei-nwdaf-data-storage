from datetime import datetime, timezone

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse_query import QueryCH

_FIELD_MAPPING = {"mean_latency": "latency"}


def apply_field_mapping(field: str) -> str:

    return _FIELD_MAPPING.get(field, field)


def transform_processor_output(data: dict, columns: set[str]) -> dict:
    """
    Transform processor's nested output format to flat storage format.
    """
    required = ["window_start", "window_end", "cell_index", "sample_count"]
    for field in required:
        if field not in data:
            raise ValueError(f"Missing required fields: {field}")

    # mandatory fields
    transformed = {
        "cell_index": data["cell_index"],
        "sample_count": data["sample_count"],
        "window_start_time": datetime.fromtimestamp(
            data["window_start"], tz=timezone.utc
        ),
        "window_end_time": datetime.fromtimestamp(data["window_end"], tz=timezone.utc),
        "window_duration_seconds": float(data["window_end"] - data["window_start"]),
    }

    # process flatten and nested metrics
    for key, value in data.items():
        if key in transformed:
            continue

        if isinstance(value, dict):
            base_name = apply_field_mapping(key)
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):
                    # NOT SUPPORTED
                    continue

                transformed[f"{base_name}_{sub_key}"] = sub_value

        else:
            # flat field
            transformed[key] = value

    return {k: v for k, v in transformed.items() if k in columns}


class ClickHouseService:
    def __init__(self) -> None:
        self.conf = ClickhouseConf()
        self.client: Client = None
        self._columns: set[str] = None

    def connect(self):
        self.client = clickhouse_connect.get_client(
            host=self.conf.host,
            port=self.conf.port,
            username=self.conf.user,
            password=self.conf.password,
        )
        result = self.client.query("DESCRIBE analytics.processed")
        self._columns = {row[0] for row in result.result_rows}

    def get_columns(self) -> set[str]:
        return self._columns

    def write_data(self, data: dict) -> None:
        """Write a single processed record to ClickHouse"""
        try:
            transformed = transform_processor_output(data, columns=self._columns)

            column_names = list(transformed.keys())
            values = [list(transformed.values())]

            self.client.insert(
                "analytics.processed",
                values,
                column_names=column_names,
                settings={"async_insert": 1, "wait_for_async_insert": 0},
            )
        except Exception as e:
            raise Exception(f"Failed to write to ClickHouse: {e}")

    def write_batch(self, data_list: list[dict]) -> None:
        """Write multiple processed records to ClickHouse"""
        try:
            transformed_list = [
                transform_processor_output(d, columns=self._columns) for d in data_list
            ]

            if not transformed_list:
                return

            column_names = list(transformed_list[0].keys())

            values = [list(d.values()) for d in transformed_list]

            self.client.insert(
                "analytics.processed",
                values,
                column_names=column_names,
                settings={"async_insert": 1, "wait_for_async_insert": 0},
            )
        except Exception as e:
            raise Exception(f"Failed to batch write to ClickHouse: {e}")

    def query_processed_latency(
        self,
        start_time: int,
        end_time: int,
        cell_index: int,
        window_duration_seconds: int,
        offset: int,
        limit: int,
    ) -> list[dict]:
        """
        Query processed latency data from ClickHouse.

        Args:
            start_time: Filter by window_start_time >= start_time (Unix timestamp in seconds)
            end_time: Filter by window_end_time <= end_time (Unix timestamp in seconds)
            cell_index: Filter by specific cell index
            offset: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of dicts
        """

        result = self.client.query(
            QueryCH.processed,
            parameters={
                "start_time": start_time,
                "end_time": end_time,
                "cell_index": cell_index,
                "offset": offset,
                "limit": limit,
                "window_duration_seconds": window_duration_seconds,
            },
        )

        column_names = result.column_names

        return [dict(zip(column_names, row)) for row in result.result_rows]
