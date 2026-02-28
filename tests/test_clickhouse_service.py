import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from src.services.clickhouse import ClickHouseService, transform_processor_output


@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client for testing."""
    with patch("clickhouse_connect.get_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def clickhouse_service(mock_clickhouse_client):
    """Create a ClickHouseService instance with mocked client."""
    service = ClickHouseService()
    service.connect()
    return service


class TestTransformProcessorOutput:
    """Tests for transform_processor_output function."""

    def test_transform_complete_data(self):
        """Test transformation of complete processor output."""
        processor_output = {
            "type": "latency",
            "cell_index": 123,
            "network": "5G",
            "window_start": 1733684400,
            "window_end": 1733684410,
            "sample_count": 100,
            "primary_bandwidth": 100.0,
            "ul_bandwidth": 50.0,
            "rsrp": {
                "mean": -85.5,
                "max": -80.0,
                "min": -90.0,
                "std": 2.5,
                "samples": 100,
            },
            "sinr": {
                "mean": 15.0,
                "max": 20.0,
                "min": 10.0,
                "std": 3.0,
                "samples": 100,
            },
            "rsrq": {
                "mean": -10.0,
                "max": -8.0,
                "min": -12.0,
                "std": 1.5,
                "samples": 100,
            },
            "mean_latency": {
                "mean": 20.0,
                "max": 30.0,
                "min": 10.0,
                "std": 5.0,
                "samples": 100,
            },
            "cqi": {"mean": 12.0, "max": 15.0, "min": 10.0, "std": 2.0, "samples": 100},
        }

        result = transform_processor_output(processor_output)

        # Check timestamp conversion
        assert result["window_start_time"] == datetime.fromtimestamp(
            1733684400, tz=timezone.utc
        )
        assert result["window_end_time"] == datetime.fromtimestamp(
            1733684410, tz=timezone.utc
        )
        assert result["window_duration_seconds"] == 10.0

        # Check mandatory fields
        assert result["cell_index"] == 123
        assert result["network"] == "5G"
        assert result["sample_count"] == 100

        # Check flat numeric fields landed in metrics
        assert result["metrics"]["primary_bandwidth"] == 100.0
        assert result["metrics"]["ul_bandwidth"] == 50.0

        # Check flattened RSRP in metrics
        assert result["metrics"]["rsrp_mean"] == -85.5
        assert result["metrics"]["rsrp_max"] == -80.0
        assert result["metrics"]["rsrp_min"] == -90.0
        assert result["metrics"]["rsrp_std"] == 2.5

        # Check flattened latency (mean_latency -> latency)
        assert result["metrics"]["latency_mean"] == 20.0
        assert result["metrics"]["latency_max"] == 30.0
        assert result["metrics"]["latency_min"] == 10.0
        assert result["metrics"]["latency_std"] == 5.0

    def test_transform_with_partial_metrics(self):
        """Test transformation when some metrics are missing."""
        processor_output = {
            "cell_index": 123,
            "network": "5G",
            "window_start": 1733684400,
            "window_end": 1733684410,
            "sample_count": 50,
            "rsrp": {
                "mean": -85.5,
                "max": -80.0,
                "min": -90.0,
                "std": 2.5,
                "samples": 50,
            },
            # Missing sinr, rsrq, mean_latency, cqi
        }

        result = transform_processor_output(processor_output)

        assert result["metrics"]["rsrp_mean"] == -85.5
        assert "sinr_mean" not in result["metrics"]
        assert "latency_mean" not in result["metrics"]

    def test_transform_missing_required_fields(self):
        """Test that transformation raises error when required fields are missing."""
        processor_output = {
            "cell_index": 123,
            "network": "5G",
            "sample_count": 100,
            # Missing window_start and window_end
        }

        with pytest.raises(ValueError, match="Missing required fields"):
            transform_processor_output(processor_output)

    def test_transform_unknown_field_goes_to_metrics(self):
        """Test that unknown numeric fields are placed in the metrics map."""
        processor_output = {
            "cell_index": 1,
            "sample_count": 10,
            "window_start": 1733684400,
            "window_end": 1733684410,
            "throughput": {"mean": 1.5, "max": 2.0},
        }

        result = transform_processor_output(processor_output)

        assert result["metrics"]["throughput_mean"] == 1.5
        assert result["metrics"]["throughput_max"] == 2.0

    def test_transform_non_numeric_flat_field_is_skipped(self):
        """Test that non-numeric flat fields (e.g. 'type') are silently skipped."""
        processor_output = {
            "cell_index": 1,
            "sample_count": 10,
            "window_start": 1733684400,
            "window_end": 1733684410,
            "type": "latency",
        }

        result = transform_processor_output(processor_output)

        assert "type" not in result["metrics"]
        assert "type" not in result

    def test_transform_skip_keys_not_in_metrics(self):
        """Test that _SKIP_KEYS fields do not appear in metrics."""
        processor_output = {
            "cell_index": 1,
            "sample_count": 10,
            "window_start": 1733684400,
            "window_end": 1733684410,
            "network": "5G",
        }

        result = transform_processor_output(processor_output)

        assert "window_start" not in result["metrics"]
        assert "window_end" not in result["metrics"]
        assert "cell_index" not in result["metrics"]
        assert "sample_count" not in result["metrics"]
        assert "network" not in result["metrics"]


class TestClickHouseService:
    """Tests for ClickHouseService class."""

    def test_initialization(self):
        """Test ClickHouseService initialization."""
        service = ClickHouseService()
        assert service.conf is not None

    def test_connect(self, mock_clickhouse_client):
        """Test connecting to ClickHouse."""
        service = ClickHouseService()
        service.connect()
        assert service.client == mock_clickhouse_client

    def test_get_metric_keys(self, clickhouse_service, mock_clickhouse_client):
        """Test retrieving known metric keys from ClickHouse."""
        mock_result = MagicMock()
        mock_result.result_rows = [("rsrp_mean",), ("sinr_mean",), ("latency_mean",)]
        mock_clickhouse_client.query.return_value = mock_result

        keys = clickhouse_service.get_metric_keys()

        assert keys == ["rsrp_mean", "sinr_mean", "latency_mean"]
        mock_clickhouse_client.query.assert_called_once()

    def test_write_data(self, clickhouse_service, mock_clickhouse_client):
        """Test writing a single record to ClickHouse."""
        test_data = {
            "type": "latency",
            "cell_index": 1,
            "network": "5G",
            "window_start": 1704110400,
            "window_end": 1704110700,
            "sample_count": 100,
            "rsrp": {"mean": -80.0, "max": -70.0, "min": -90.0, "std": 5.0},
            "mean_latency": {"mean": 20.0, "max": 30.0, "min": 10.0, "std": 5.0},
        }

        clickhouse_service.write_data(test_data)

        mock_clickhouse_client.insert.assert_called_once()
        call_args = mock_clickhouse_client.insert.call_args

        assert call_args[0][0] == "analytics.processed"
        assert isinstance(call_args[0][1], list)
        assert len(call_args[0][1]) == 1
        assert "metrics" in call_args[1]["column_names"]
        assert call_args[1]["settings"]["async_insert"] == 1
        assert call_args[1]["settings"]["wait_for_async_insert"] == 0

    def test_write_batch(self, clickhouse_service, mock_clickhouse_client):
        """Test writing multiple records to ClickHouse."""
        test_data = [
            {
                "cell_index": 1,
                "network": "5G",
                "window_start": 1704110400,
                "window_end": 1704110700,
                "sample_count": 100,
                "rsrp": {"mean": -80.0},
            },
            {
                "cell_index": 2,
                "network": "4G",
                "window_start": 1704110400,
                "window_end": 1704110700,
                "sample_count": 50,
                "rsrp": {"mean": -90.0},
            },
        ]

        clickhouse_service.write_batch(test_data)

        mock_clickhouse_client.insert.assert_called_once()
        call_args = mock_clickhouse_client.insert.call_args
        assert call_args[0][0] == "analytics.processed"
        assert len(call_args[0][1]) == 2

    def test_write_batch_empty(self, clickhouse_service, mock_clickhouse_client):
        """Test that an empty batch does not call insert."""
        clickhouse_service.write_batch([])
        mock_clickhouse_client.insert.assert_not_called()

    def test_query_processed_latency_returns_flat_dicts(
        self, clickhouse_service, mock_clickhouse_client
    ):
        """Test that query results have metrics flattened to top-level keys."""
        mock_result = MagicMock()
        mock_result.column_names = [
            "cell_index",
            "sample_count",
            "window_start_time",
            "window_end_time",
            "window_duration_seconds",
            "network",
            "metrics",
        ]
        mock_result.result_rows = [
            (
                1,
                100,
                datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
                300.0,
                "5G",
                {"rsrp_mean": -80.0, "sinr_mean": 15.0},
            )
        ]
        mock_clickhouse_client.query.return_value = mock_result

        results = clickhouse_service.query_processed_latency(
            start_time=0,
            end_time=9999999999,
            cell_index=1,
            window_duration_seconds=300,
            offset=0,
            limit=100,
        )

        assert len(results) == 1
        row = results[0]
        # metrics map should be flattened
        assert "metrics" not in row
        assert row["rsrp_mean"] == -80.0
        assert row["sinr_mean"] == 15.0
        assert row["cell_index"] == 1
        assert row["network"] == "5G"

    def test_query_processed_latency_empty_result(
        self, clickhouse_service, mock_clickhouse_client
    ):
        """Test query with no results."""
        mock_result = MagicMock()
        mock_result.column_names = []
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        results = clickhouse_service.query_processed_latency(
            start_time=0,
            end_time=9999999999,
            cell_index=1,
            window_duration_seconds=300,
            offset=0,
            limit=100,
        )

        assert len(results) == 0

    def test_query_processed_latency_with_pagination(
        self, clickhouse_service, mock_clickhouse_client
    ):
        """Test that pagination parameters are passed correctly."""
        mock_result = MagicMock()
        mock_result.column_names = []
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        clickhouse_service.query_processed_latency(
            start_time=0,
            end_time=9999999999,
            cell_index=1,
            window_duration_seconds=300,
            offset=50,
            limit=25,
        )

        call_args = mock_clickhouse_client.query.call_args
        assert call_args[1]["parameters"]["offset"] == 50
        assert call_args[1]["parameters"]["limit"] == 25

    def test_query_processed_latency_multiple_rows(
        self, clickhouse_service, mock_clickhouse_client
    ):
        """Test query returning multiple rows all with flattened metrics."""
        mock_result = MagicMock()
        mock_result.column_names = [
            "cell_index", "sample_count", "window_start_time", "window_end_time",
            "window_duration_seconds", "network", "metrics",
        ]
        mock_result.result_rows = [
            (i, 100,
             datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
             datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
             300.0, "5G", {"rsrp_mean": -80.0})
            for i in range(3)
        ]
        mock_clickhouse_client.query.return_value = mock_result

        results = clickhouse_service.query_processed_latency(
            start_time=0, end_time=9999999999, cell_index=1,
            window_duration_seconds=300, offset=0, limit=100,
        )

        assert len(results) == 3
        for row in results:
            assert "metrics" not in row
            assert row["rsrp_mean"] == -80.0
