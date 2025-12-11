import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from src.services.clickhouse import ClickHouseService, transform_processor_output
from src.models.processed_latency import ProcessedLatency


@pytest.fixture
def mock_clickhouse_client():
    """Mock ClickHouse client for testing."""
    with patch('clickhouse_connect.get_client') as mock:
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
                "samples": 100
            },
            "sinr": {
                "mean": 15.0,
                "max": 20.0,
                "min": 10.0,
                "std": 3.0,
                "samples": 100
            },
            "rsrq": {
                "mean": -10.0,
                "max": -8.0,
                "min": -12.0,
                "std": 1.5,
                "samples": 100
            },
            "mean_latency": {
                "mean": 20.0,
                "max": 30.0,
                "min": 10.0,
                "std": 5.0,
                "samples": 100
            },
            "cqi": {
                "mean": 12.0,
                "max": 15.0,
                "min": 10.0,
                "std": 2.0,
                "samples": 100
            }
        }

        result = transform_processor_output(processor_output)

        # Check timestamp conversion
        assert result["window_start_time"] == datetime.fromtimestamp(1733684400, tz=timezone.utc)
        assert result["window_end_time"] == datetime.fromtimestamp(1733684410, tz=timezone.utc)
        assert result["window_duration_seconds"] == 10.0

        # Check metadata
        assert result["cell_index"] == 123
        assert result["network"] == "5G"
        assert result["sample_count"] == 100
        assert result["primary_bandwidth"] == 100.0
        assert result["ul_bandwidth"] == 50.0

        # Check flattened RSRP
        assert result["rsrp_mean"] == -85.5
        assert result["rsrp_max"] == -80.0
        assert result["rsrp_min"] == -90.0
        assert result["rsrp_std"] == 2.5

        # Check flattened latency (mean_latency -> latency)
        assert result["latency_mean"] == 20.0
        assert result["latency_max"] == 30.0
        assert result["latency_min"] == 10.0
        assert result["latency_std"] == 5.0

    def test_transform_with_null_metrics(self):
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
                "samples": 50
            }
            # Missing sinr, rsrq, mean_latency, cqi
        }

        result = transform_processor_output(processor_output)

        # Check that RSRP is present
        assert result["rsrp_mean"] == -85.5

        # Check that missing metrics result in None values
        assert "sinr_mean" not in result or result["sinr_mean"] is None
        assert "latency_mean" not in result or result["latency_mean"] is None

    def test_transform_missing_window_times(self):
        """Test that transformation raises error when window times are missing."""
        processor_output = {
            "cell_index": 123,
            "network": "5G",
            "sample_count": 100
            # Missing window_start and window_end
        }

        with pytest.raises(ValueError, match="Missing window_start or window_end"):
            transform_processor_output(processor_output)


class TestClickHouseService:
    """Tests for ClickHouseService class."""

    def test_initialization(self):
        """Test ClickHouseService initialization."""
        service = ClickHouseService()

        assert service.conf is not None
        # Client is type-annotated but not initialized until connect() is called

    def test_connect(self, mock_clickhouse_client):
        """Test connecting to ClickHouse."""
        service = ClickHouseService()
        service.connect()

        assert service.client == mock_clickhouse_client

    def test_write_data(self, clickhouse_service, mock_clickhouse_client):
        """Test writing a single ProcessedLatency record to ClickHouse using processor format."""
        # Prepare test data in processor format (nested structure)
        test_data = {
            "type": "latency",
            "cell_index": 1,
            "network": "5G",
            "window_start": 1704110400,  # 2024-01-01 12:00:00 UTC
            "window_end": 1704110700,    # 2024-01-01 12:05:00 UTC
            "sample_count": 100,
            "primary_bandwidth": 100.0,
            "ul_bandwidth": 50.0,
            "rsrp": {
                "mean": -80.0,
                "max": -70.0,
                "min": -90.0,
                "std": 5.0,
                "samples": 100
            },
            "sinr": {
                "mean": 15.0,
                "max": 20.0,
                "min": 10.0,
                "std": 3.0,
                "samples": 100
            },
            "rsrq": {
                "mean": -10.0,
                "max": -8.0,
                "min": -12.0,
                "std": 1.5,
                "samples": 100
            },
            "mean_latency": {
                "mean": 20.0,
                "max": 30.0,
                "min": 10.0,
                "std": 5.0,
                "samples": 100
            },
            "cqi": {
                "mean": 12.0,
                "max": 15.0,
                "min": 10.0,
                "std": 2.0,
                "samples": 100
            }
        }

        # Execute write
        clickhouse_service.write_data(test_data)

        # Verify insert was called with correct parameters
        mock_clickhouse_client.insert.assert_called_once()
        call_args = mock_clickhouse_client.insert.call_args

        # Check table name
        assert call_args[0][0] == 'analytics.processed_latency'

        # Check data format (should be a list with one dict)
        assert isinstance(call_args[0][1], list)
        assert len(call_args[0][1]) == 1

        # Check async insert settings
        assert call_args[1]['settings']['async_insert'] == 1
        assert call_args[1]['settings']['wait_for_async_insert'] == 0


    def test_query_processed_latency_success(self, clickhouse_service, mock_clickhouse_client):
        """Test successful query of processed latency data."""
        # Mock query result
        mock_result = MagicMock()
        mock_result.result_rows = [
            (
                # Window info
                datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
                300.0,
                # Cell info
                1,
                "5G",
                # RSRP stats
                -80.0, -70.0, -90.0, 5.0,
                # SINR stats
                15.0, 20.0, 10.0, 3.0,
                # RSRQ stats
                -10.0, -8.0, -12.0, 1.5,
                # Latency stats
                20.0, 30.0, 10.0, 5.0,
                # CQI stats
                12.0, 15.0, 10.0, 2.0,
                # Bandwidth
                100.0, 50.0,
                # Sample count
                100
            )
        ]
        mock_clickhouse_client.query.return_value = mock_result

        # Execute query
        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        results = clickhouse_service.query_processed_latency(
            start_time=start_time,
            end_time=end_time,
            cell_index=1,
            window_duration_seconds=10,
            offset=0,
            limit=100
        )

        # Verify query was called
        mock_clickhouse_client.query.assert_called_once()

        # Verify results
        assert len(results) == 1
        assert isinstance(results[0], ProcessedLatency)
        assert results[0].cell_index == 1
        assert results[0].network == "5G"
        assert results[0].rsrp_mean == -80.0
        assert results[0].sample_count == 100

    def test_query_processed_latency_empty_result(self, clickhouse_service, mock_clickhouse_client):
        """Test query with no results."""
        # Mock empty result
        mock_result = MagicMock()
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        results = clickhouse_service.query_processed_latency(
            start_time=start_time,
            end_time=end_time,
            cell_index=1,
            window_duration_seconds=10,
            offset=0,
            limit=100
        )

        assert len(results) == 0

    def test_query_processed_latency_with_pagination(self, clickhouse_service, mock_clickhouse_client):
        """Test query with pagination parameters."""
        mock_result = MagicMock()
        mock_result.result_rows = []
        mock_clickhouse_client.query.return_value = mock_result

        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        clickhouse_service.query_processed_latency(
            start_time=start_time,
            end_time=end_time,
            cell_index=1,
            window_duration_seconds=10,
            offset=50,
            limit=25
        )

        # Verify pagination parameters were passed
        call_args = mock_clickhouse_client.query.call_args
        assert call_args[1]['parameters']['offset'] == 50
        assert call_args[1]['parameters']['limit'] == 25

    def test_query_processed_latency_multiple_rows(self, clickhouse_service, mock_clickhouse_client):
        """Test query returning multiple rows."""
        # Create multiple mock rows
        base_row = [
            datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
            300.0, 1, "5G",
            -80.0, -70.0, -90.0, 5.0,  # RSRP
            15.0, 20.0, 10.0, 3.0,      # SINR
            -10.0, -8.0, -12.0, 1.5,    # RSRQ
            20.0, 30.0, 10.0, 5.0,      # Latency
            12.0, 15.0, 10.0, 2.0,      # CQI
            100.0, 50.0, 100
        ]

        mock_result = MagicMock()
        mock_result.result_rows = [tuple(base_row) for _ in range(3)]
        mock_clickhouse_client.query.return_value = mock_result

        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        results = clickhouse_service.query_processed_latency(
            start_time=start_time,
            end_time=end_time,
            cell_index=1,
            window_duration_seconds=10,
            offset=0,
            limit=100
        )

        assert len(results) == 3
        for result in results:
            assert isinstance(result, ProcessedLatency)

    def test_query_processed_latency_with_nullable_fields(self, clickhouse_service, mock_clickhouse_client):
        """Test query with NULL values in optional fields."""
        mock_result = MagicMock()
        mock_result.result_rows = [
            (
                datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
                300.0, 1, "5G",
                None, None, None, None,  # RSRP - all NULL
                15.0, 20.0, 10.0, 3.0,   # SINR
                None, None, None, None,  # RSRQ - all NULL
                20.0, 30.0, 10.0, 5.0,   # Latency
                None, None, None, None,  # CQI - all NULL
                None, None,              # Bandwidth - NULL
                50
            )
        ]
        mock_clickhouse_client.query.return_value = mock_result

        start_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        results = clickhouse_service.query_processed_latency(
            start_time=start_time,
            end_time=end_time,
            cell_index=1,
            window_duration_seconds=10,
            offset=0,
            limit=100
        )

        assert len(results) == 1
        assert results[0].rsrp_mean is None
        assert results[0].rsrp_max is None
        assert results[0].sinr_mean == 15.0  # Not null
        assert results[0].primary_bandwidth is None
        assert results[0].sample_count == 50
