from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Queue

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse_query import QueryCH

_FIELD_MAPPING = {"mean_latency": "latency"}

_SKIP_KEYS = {"window_start", "window_end", "cell_index", "ip_src", "sample_count", "network"}


def apply_field_mapping(field: str) -> str:
    return _FIELD_MAPPING.get(field, field)


def transform_processor_output(data: dict) -> dict:
    """
    Transform processor's nested output format to flat storage format.
    """
    required = ["window_start", "window_end", "cell_index", "sample_count"]
    for field in required:
        if field not in data:
            raise ValueError(f"Missing required fields: {field}")

    metrics: dict[str, float] = {}

    for key, value in data.items():
        if key in _SKIP_KEYS:
            continue
        if isinstance(value, dict):
            base_name = apply_field_mapping(key)
            for sub_key, sub_value in value.items():
                if not isinstance(sub_value, dict):
                    try:
                        metrics[f"{base_name}_{sub_key}"] = float(sub_value)
                    except (TypeError, ValueError):
                        pass  # skip non-numeric sub-fields
        else:
            try:
                metrics[key] = float(value)
            except (TypeError, ValueError):
                pass  # skip non-numeric flat fields (e.g. "type": "latency")

    return {
        "cell_index": data["cell_index"],
        "ip_src": data.get("ip_src"),
        "sample_count": data["sample_count"],
        "window_start_time": datetime.fromtimestamp(
            data["window_start"], tz=timezone.utc
        ),
        "window_end_time": datetime.fromtimestamp(data["window_end"], tz=timezone.utc),
        "window_duration_seconds": float(data["window_end"] - data["window_start"]),
        "network": data.get("network"),
        "metrics": metrics,
    }


class ClickHouseService:
    def __init__(self, pool_size: int = 4) -> None:
        self.conf = ClickhouseConf()
        self._pool: Queue[Client] = Queue(maxsize=pool_size)
        self._pool_size = pool_size

    def connect(self):
        """Create the connection pool."""
        for _ in range(self._pool_size):
            self._pool.put(self._create_client())

    def _create_client(self) -> Client:
        return clickhouse_connect.get_client(
            host=self.conf.host,
            port=self.conf.port,
            username=self.conf.user,
            password=self.conf.password,
        )

    @contextmanager
    def _get_client(self):
        """Borrow a client from the pool, return it when done."""
        client = self._pool.get()
        try:
            yield client
        finally:
            self._pool.put(client)

    def get_metric_keys(self) -> list[str]:
        with self._get_client() as client:
            result = client.query(QueryCH.metric_keys)
            return [row[0] for row in result.result_rows]

    def write_data(self, data: dict) -> None:
        """Write a single processed record to ClickHouse"""
        try:
            transformed = transform_processor_output(data)
            column_names = list(transformed.keys())
            values = [list(transformed.values())]
            with self._get_client() as client:
                client.insert(
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
            transformed_list = [transform_processor_output(d) for d in data_list]
            if not transformed_list:
                return
            column_names = list(transformed_list[0].keys())
            values = [list(d.values()) for d in transformed_list]
            with self._get_client() as client:
                client.insert(
                    "analytics.processed",
                    values,
                    column_names=column_names,
                    settings={"async_insert": 1, "wait_for_async_insert": 0},
                )
        except Exception as e:
            raise Exception(f"Failed to batch write to ClickHouse: {e}")

    def query_processed(
        self,
        start_time: int,
        end_time: int,
        cell_index: int,
        window_duration_seconds: int,
        offset: int,
        limit: int,
        ip_src: str | None = None,
    ) -> list[dict]:
        """
        Query processed latency data from ClickHouse.

        Args:
            start_time: Filter by window_start_time >= start_time (Unix timestamp in seconds)
            end_time: Filter by window_end_time <= end_time (Unix timestamp in seconds)
            cell_index: Filter by specific cell index
            offset: Number of records to skip
            limit: Maximum number of records to return
            ip_src: Optional source IP filter

        Returns:
            List of dicts with metrics flattened to top-level keys
        """
        params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "offset": offset,
            "limit": limit,
            "window_duration_seconds": window_duration_seconds,
        }

        if ip_src == "*":
            query = QueryCH.processed_all_ips
        elif ip_src is not None:
            query = QueryCH.processed_by_ip
            params["ip_src"] = ip_src
        else:
            query = QueryCH.processed

        with self._get_client() as client:
            result = client.query(query, parameters=params)

        column_names = result.column_names
        rows = [dict(zip(column_names, row)) for row in result.result_rows]
        for row in rows:
            metrics = row.pop("metrics", {})
            row.update(metrics)
        return rows
