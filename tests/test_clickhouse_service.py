import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from src.services.clickhouse import ClickHouseService
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
            offset=0,
            limit=100
        )

        assert len(results) == 1
        assert results[0].rsrp_mean is None
        assert results[0].rsrp_max is None
        assert results[0].sinr_mean == 15.0  # Not null
        assert results[0].primary_bandwidth is None
        assert results[0].sample_count == 50
