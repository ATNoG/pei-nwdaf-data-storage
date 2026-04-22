from contextlib import contextmanager
from datetime import datetime, timezone
from queue import Empty, Queue

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse_query import QueryCH

_KNOWN_TAGS = {"snssai_sst", "snssai_sd", "dnn", "event"}
_REQUIRED_TAGS = {"snssai_sst", "dnn", "event"}


def transform_processor_output(data: dict) -> dict:
    """
    Transform Data-Processor window output to ClickHouse storage format.

    Input: {tags: {snssai_sst, snssai_sd, dnn, event, ...ue_tags},
            window_start, window_end, sample_count,
            metrics: {name: {mean, min, max, std, count}}}

    Metrics are flattened: thrputUl_mbps -> thrputUl_mbps_mean, thrputUl_mbps_min, ...
    None stat values (zero-fill windows) are omitted from the map.
    """
    for field in ("window_start", "window_end", "sample_count", "tags"):
        if field not in data:
            raise ValueError(f"Missing required field: {field}")

    if data["window_end"] < data["window_start"]:
        raise ValueError("window_end must be >= window_start")

    tags = data["tags"]
    for tag in _REQUIRED_TAGS:
        if not tags.get(tag):
            raise ValueError(f"Missing required tag: {tag}")

    ue_tags = {k: str(v) for k, v in tags.items() if k not in _KNOWN_TAGS}

    metrics: dict[str, float] = {}
    for metric_name, stats in data.get("metrics", {}).items():
        if isinstance(stats, dict):
            for stat_name, stat_value in stats.items():
                if stat_value is not None:
                    try:
                        metrics[f"{metric_name}_{stat_name}"] = float(stat_value)
                    except (TypeError, ValueError):
                        pass
        else:
            try:
                metrics[metric_name] = float(stats)
            except (TypeError, ValueError):
                pass

    return {
        "window_start": datetime.fromtimestamp(data["window_start"], tz=timezone.utc),
        "window_end": datetime.fromtimestamp(data["window_end"], tz=timezone.utc),
        "window_duration_seconds": int(data["window_end"] - data["window_start"]),
        "sample_count": int(data["sample_count"]),
        "snssai_sst": tags["snssai_sst"],
        "snssai_sd": tags.get("snssai_sd", ""),
        "dnn": tags["dnn"],
        "event": tags["event"],
        "ue_tags": ue_tags,
        "metrics": metrics,
    }


class ClickHouseService:
    def __init__(self, pool_size: int = 4) -> None:
        self.conf = ClickhouseConf()
        self._pool: Queue[Client] = Queue(maxsize=pool_size)
        self._pool_size = pool_size

    def connect(self):
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
        """Borrow a client from the pool; create a one-off if pool is empty (startup race)."""
        try:
            client = self._pool.get(timeout=5)
        except Empty:
            # Pool empty — create a single fresh client without touching the pool.
            # Concurrent threads that also hit Empty each get their own one-off client;
            # the pool naturally refills as normal borrowers return their clients.
            client = self._create_client()
            yield client
            return
        try:
            yield client
        finally:
            self._pool.put(client)

    def get_metric_keys(self) -> list[str]:
        with self._get_client() as client:
            result = client.query(QueryCH.metric_keys)
            return [row[0] for row in result.result_rows]

    def get_metric_event_map(self) -> dict[str, list[str]]:
        """Return {metric_key: [event, ...]} from the materialized view."""
        with self._get_client() as client:
            result = client.query(QueryCH.metric_event_map)
            return {row[0]: list(row[1]) for row in result.result_rows}

    def write_data(self, data: dict) -> None:
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
        snssai_sst: str,
        dnn: str,
        snssai_sd: str | None = None,
        event: str | None = None,
        window_duration_seconds: int | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict]:
        params: dict = {
            "start_time": start_time,
            "end_time": end_time,
            "snssai_sst": snssai_sst,
            "dnn": dnn,
            "offset": offset,
            "limit": limit,
        }

        query = (
            "SELECT * FROM analytics.processed"
            " WHERE snssai_sst = {snssai_sst:String}"
            " AND dnn = {dnn:String}"
            " AND toUnixTimestamp(window_start) >= {start_time:Int64}"
            " AND toUnixTimestamp(window_end) <= {end_time:Int64}"
        )

        if snssai_sd is not None:
            query += " AND snssai_sd = {snssai_sd:String}"
            params["snssai_sd"] = snssai_sd

        if event is not None:
            query += " AND event = {event:String}"
            params["event"] = event

        if window_duration_seconds is not None:
            # Stored as Int32; use exact integer match
            query += " AND window_duration_seconds = {window_duration_seconds:Int32}"
            params["window_duration_seconds"] = int(window_duration_seconds)

        # LIMIT 1 BY deduplicates rows with the same identity key
        # (data-storage can receive the same window more than once on restart).
        # Key excludes window_start so that LIMIT 1 BY actually collapses duplicates.
        query += (
            " ORDER BY window_end DESC"
            " LIMIT 1 BY snssai_sst, snssai_sd, dnn, event, window_start"
            " LIMIT {limit:Int32} OFFSET {offset:Int32}"
        )

        with self._get_client() as client:
            result = client.query(query, parameters=params)
            column_names = result.column_names
            rows = [dict(zip(column_names, row)) for row in result.result_rows]

        for row in rows:
            metrics = row.pop("metrics", {})
            row.update(metrics)
        return rows

    def query_decisions(
        self,
        start_time: int,
        end_time: int,
        cell_id: int | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict]:
        params = {
            "start_time": start_time,
            "end_time": end_time,
            "offset": offset,
            "limit": limit,
        }

        if cell_id is not None:
            query = QueryCH.decisions
            params["cell_id"] = cell_id
        else:
            query = QueryCH.decisions_all

        with self._get_client() as client:
            result = client.query(query, parameters=params)

        column_names = result.column_names
        return [dict(zip(column_names, row)) for row in result.result_rows]

    def write_decision(
        self,
        cell_id: int,
        timestamp: datetime,
        compression_method: str,
        compressed_data: str,
    ) -> None:
        try:
            with self._get_client() as client:
                count_query = "SELECT COUNT(*) FROM analytics.decisions WHERE cell_id = {cell_id:Int32}"
                result = client.query(count_query, parameters={"cell_id": cell_id})
                next_id = result.result_rows[0][0] + 1

                client.insert(
                    "analytics.decisions",
                    [[cell_id, next_id, timestamp, compression_method, compressed_data]],
                    column_names=["cell_id", "id", "timestamp", "compression_method", "compressed_data"],
                    settings={"async_insert": 1, "wait_for_async_insert": 0},
                )
        except Exception as e:
            raise Exception(f"Failed to write decision to ClickHouse: {e}")
