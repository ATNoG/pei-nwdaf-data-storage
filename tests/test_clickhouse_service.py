import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.services.clickhouse import ClickHouseService, transform_processor_output


@pytest.fixture
def mock_clickhouse_client():
    with patch("clickhouse_connect.get_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def clickhouse_service(mock_clickhouse_client):
    service = ClickHouseService()
    service.connect()
    return service


# ---------------------------------------------------------------------------
# transform_processor_output
# ---------------------------------------------------------------------------

VALID_INPUT = {
    "tags": {"snssai_sst": "1", "snssai_sd": "000001", "dnn": "internet", "event": "PERF_DATA"},
    "window_start": 1733684400,
    "window_end": 1733684460,
    "sample_count": 10,
    "metrics": {
        "thrputUl_mbps": {"mean": 11.5, "min": 5.0, "max": 20.0, "std": 3.2, "count": 10},
        "pdb_ms": {"mean": 25.0, "min": 10.0, "max": 50.0, "std": 8.0, "count": 10},
    },
}


class TestTransformProcessorOutput:
    def test_basic_transform(self):
        result = transform_processor_output(VALID_INPUT)

        assert result["window_start"] == datetime.fromtimestamp(1733684400, tz=timezone.utc)
        assert result["window_end"] == datetime.fromtimestamp(1733684460, tz=timezone.utc)
        assert result["window_duration_seconds"] == 60
        assert result["sample_count"] == 10
        assert result["snssai_sst"] == "1"
        assert result["snssai_sd"] == "000001"
        assert result["dnn"] == "internet"
        assert result["event"] == "PERF_DATA"

    def test_metrics_flattened(self):
        result = transform_processor_output(VALID_INPUT)
        metrics = result["metrics"]

        assert metrics["thrputUl_mbps_mean"] == 11.5
        assert metrics["thrputUl_mbps_min"] == 5.0
        assert metrics["thrputUl_mbps_max"] == 20.0
        assert metrics["thrputUl_mbps_std"] == 3.2
        assert metrics["thrputUl_mbps_count"] == 10.0
        assert metrics["pdb_ms_mean"] == 25.0

    def test_scalar_metric(self):
        data = {**VALID_INPUT, "metrics": {"thrputUl_mbps": 11.5}}
        result = transform_processor_output(data)
        assert result["metrics"]["thrputUl_mbps"] == 11.5

    def test_none_stat_values_omitted(self):
        data = {
            **VALID_INPUT,
            "metrics": {"thrputUl_mbps": {"mean": 11.5, "min": None, "max": None, "std": None, "count": 1}},
        }
        result = transform_processor_output(data)
        assert "thrputUl_mbps_mean" in result["metrics"]
        assert "thrputUl_mbps_min" not in result["metrics"]

    def test_ue_tags_extracted(self):
        data = {
            **VALID_INPUT,
            "tags": {**VALID_INPUT["tags"], "ueIpv4Addr": "10.0.0.1", "supi": "imsi-001"},
        }
        result = transform_processor_output(data)
        assert result["ue_tags"] == {"ueIpv4Addr": "10.0.0.1", "supi": "imsi-001"}

    def test_known_tags_not_in_ue_tags(self):
        result = transform_processor_output(VALID_INPUT)
        assert result["ue_tags"] == {}

    def test_snssai_sd_optional(self):
        data = {**VALID_INPUT, "tags": {"snssai_sst": "1", "dnn": "internet", "event": "PERF_DATA"}}
        result = transform_processor_output(data)
        assert result["snssai_sd"] == ""

    def test_missing_required_field_raises(self):
        for field in ("window_start", "window_end", "sample_count", "tags"):
            data = {k: v for k, v in VALID_INPUT.items() if k != field}
            with pytest.raises(ValueError, match=field):
                transform_processor_output(data)

    def test_missing_required_tag_raises(self):
        for tag in ("snssai_sst", "dnn", "event"):
            tags = {k: v for k, v in VALID_INPUT["tags"].items() if k != tag}
            with pytest.raises(ValueError, match=tag):
                transform_processor_output({**VALID_INPUT, "tags": tags})

    def test_window_end_before_start_raises(self):
        data = {**VALID_INPUT, "window_end": VALID_INPUT["window_start"] - 1}
        with pytest.raises(ValueError, match="window_end"):
            transform_processor_output(data)

    def test_empty_metrics(self):
        data = {**VALID_INPUT, "metrics": {}}
        result = transform_processor_output(data)
        assert result["metrics"] == {}


# ---------------------------------------------------------------------------
# ClickHouseService
# ---------------------------------------------------------------------------

class TestClickHouseService:
    def test_initialization(self):
        service = ClickHouseService()
        assert service.conf is not None

    def test_connect_fills_pool(self, mock_clickhouse_client):
        service = ClickHouseService(pool_size=2)
        service.connect()
        assert service._pool.qsize() == 2

    def test_get_metric_keys(self, clickhouse_service, mock_clickhouse_client):
        mock_result = MagicMock()
        mock_result.result_rows = [("thrputUl_mbps_mean",), ("pdb_ms_mean",)]
        mock_clickhouse_client.query.return_value = mock_result

        keys = clickhouse_service.get_metric_keys()
        assert keys == ["thrputUl_mbps_mean", "pdb_ms_mean"]

    def test_get_metric_event_map(self, clickhouse_service, mock_clickhouse_client):
        mock_result = MagicMock()
        mock_result.result_rows = [
            ("thrputUl_mbps_mean", ["PERF_DATA"]),
            ("pdb_ms_mean", ["PERF_DATA"]),
        ]
        mock_clickhouse_client.query.return_value = mock_result

        result = clickhouse_service.get_metric_event_map()
        assert result == {"thrputUl_mbps_mean": ["PERF_DATA"], "pdb_ms_mean": ["PERF_DATA"]}

    def test_write_data(self, clickhouse_service, mock_clickhouse_client):
        clickhouse_service.write_data(VALID_INPUT)

        mock_clickhouse_client.insert.assert_called_once()
        call_args = mock_clickhouse_client.insert.call_args
        assert call_args[0][0] == "analytics.processed"
        assert "metrics" in call_args[1]["column_names"]
        assert call_args[1]["settings"]["async_insert"] == 1

    def test_write_batch(self, clickhouse_service, mock_clickhouse_client):
        data = [VALID_INPUT, VALID_INPUT]
        clickhouse_service.write_batch(data)

        mock_clickhouse_client.insert.assert_called_once()
        assert len(mock_clickhouse_client.insert.call_args[0][1]) == 2

    def test_write_batch_empty(self, clickhouse_service, mock_clickhouse_client):
        clickhouse_service.write_batch([])
        mock_clickhouse_client.insert.assert_not_called()

    def test_query_processed_metrics_flattened(self, clickhouse_service, mock_clickhouse_client):
        mock_result = MagicMock()
        mock_result.column_names = [
            "window_start", "window_end", "window_duration_seconds", "sample_count",
            "snssai_sst", "snssai_sd", "dnn", "event", "ue_tags", "metrics",
        ]
        mock_result.result_rows = [(
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc),
            60, 10, "1", "000001", "internet", "PERF_DATA", {},
            {"thrputUl_mbps_mean": 11.5, "pdb_ms_mean": 25.0},
        )]
        mock_clickhouse_client.query.return_value = mock_result

        results = clickhouse_service.query_processed(
            start_time=0, end_time=9999999999,
            snssai_sst="1", dnn="internet",
        )

        assert len(results) == 1
        row = results[0]
        assert "metrics" not in row
        assert row["thrputUl_mbps_mean"] == 11.5
        assert row["snssai_sst"] == "1"

    def test_query_processed_empty(self, clickhouse_service, mock_clickhouse_client):
        mock_result = MagicMock()
        mock_result.column_names = []
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        results = clickhouse_service.query_processed(
            start_time=0, end_time=9999999999,
            snssai_sst="1", dnn="internet",
        )
        assert results == []

    def test_query_processed_pagination_params(self, clickhouse_service, mock_clickhouse_client):
        mock_result = MagicMock()
        mock_result.column_names = []
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        clickhouse_service.query_processed(
            start_time=0, end_time=9999999999,
            snssai_sst="1", dnn="internet",
            offset=50, limit=25,
        )

        params = mock_clickhouse_client.query.call_args[1]["parameters"]
        assert params["offset"] == 50
        assert params["limit"] == 25
