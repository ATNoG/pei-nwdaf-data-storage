import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from src.models.processed_latency import ProcessedLatency


@pytest.fixture
def mock_clickhouse_service():
    """Mock ClickHouseService for testing."""
    with patch('src.routers.v1.latency.ClickHouse') as mock:
        service_mock = MagicMock()
        mock.service = service_mock
        yield service_mock


@pytest.fixture
def test_client(mock_clickhouse_service):
    """Create a test client for the FastAPI app."""
    from fastapi import FastAPI
    from src.routers.v1 import v1_router

    app = FastAPI()
    app.include_router(v1_router, prefix="/api/v1", tags=["v1"])

    return TestClient(app)


@pytest.fixture
def sample_processed_latency():
    """Create a sample ProcessedLatency object for testing."""
    return ProcessedLatency(
        window_start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        window_end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
        window_duration_seconds=300.0,
        cell_index=1,
        network="5G",
        rsrp_mean=-80.0,
        rsrp_max=-70.0,
        rsrp_min=-90.0,
        rsrp_std=5.0,
        sinr_mean=15.0,
        sinr_max=20.0,
        sinr_min=10.0,
        sinr_std=3.0,
        rsrq_mean=-10.0,
        rsrq_max=-8.0,
        rsrq_min=-12.0,
        rsrq_std=1.5,
        latency_mean=20.0,
        latency_max=30.0,
        latency_min=10.0,
        latency_std=5.0,
        cqi_mean=12.0,
        cqi_max=15.0,
        cqi_min=10.0,
        cqi_std=2.0,
        primary_bandwidth=100.0,
        ul_bandwidth=50.0,
        sample_count=100
    )


class TestLatencyEndpoint:
    """Tests for the processed latency endpoint."""

    def test_get_processed_latency_success(self, test_client, mock_clickhouse_service, sample_processed_latency):
        """Test successful retrieval of processed latency data."""
        # Mock service response
        mock_clickhouse_service.query_processed_latency.return_value = [sample_processed_latency]

        # Make request
        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["cell_index"] == 1
        assert data[0]["network"] == "5G"
        assert data[0]["rsrp_mean"] == -80.0
        assert data[0]["sample_count"] == 100

        # Verify service was called correctly
        mock_clickhouse_service.query_processed_latency.assert_called_once()

    def test_get_processed_latency_empty_result(self, test_client, mock_clickhouse_service):
        """Test endpoint with no matching data."""
        mock_clickhouse_service.query_processed_latency.return_value = []

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 200
        assert response.json() == []

    def test_get_processed_latency_missing_required_params(self, test_client, mock_clickhouse_service):
        """Test endpoint with missing required parameters."""
        # Missing cell_index
        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z"
            }
        )

        assert response.status_code == 422  # Validation error

    def test_get_processed_latency_with_pagination(self, test_client, mock_clickhouse_service, sample_processed_latency):
        """Test endpoint with pagination parameters."""
        mock_clickhouse_service.query_processed_latency.return_value = [sample_processed_latency]

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1,
                "offset": 50,
                "limit": 25
            }
        )

        assert response.status_code == 200

        # Verify pagination was passed to service
        call_kwargs = mock_clickhouse_service.query_processed_latency.call_args[1]
        assert call_kwargs['offset'] == 50
        assert call_kwargs['limit'] == 25

    def test_get_processed_latency_invalid_limit(self, test_client, mock_clickhouse_service):
        """Test endpoint with invalid limit value."""
        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1,
                "limit": 5000  # Exceeds max of 1000
            }
        )

        assert response.status_code == 422  # Validation error

    def test_get_processed_latency_invalid_offset(self, test_client, mock_clickhouse_service):
        """Test endpoint with negative offset."""
        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1,
                "offset": -10
            }
        )

        assert response.status_code == 422  # Validation error

    def test_get_processed_latency_service_error(self, test_client, mock_clickhouse_service):
        """Test endpoint when service raises an exception."""
        mock_clickhouse_service.query_processed_latency.side_effect = Exception("Database error")

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]

    def test_get_processed_latency_multiple_results(self, test_client, mock_clickhouse_service, sample_processed_latency):
        """Test endpoint returning multiple results."""
        # Create multiple samples
        samples = [sample_processed_latency for _ in range(5)]
        mock_clickhouse_service.query_processed_latency.return_value = samples

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5

    def test_get_processed_latency_default_pagination(self, test_client, mock_clickhouse_service, sample_processed_latency):
        """Test endpoint uses default pagination values."""
        mock_clickhouse_service.query_processed_latency.return_value = [sample_processed_latency]

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 200

        # Verify default pagination values
        call_kwargs = mock_clickhouse_service.query_processed_latency.call_args[1]
        assert call_kwargs['offset'] == 0
        assert call_kwargs['limit'] == 100

    def test_get_processed_latency_with_null_fields(self, test_client, mock_clickhouse_service):
        """Test endpoint with data containing null/optional fields."""
        partial_data = ProcessedLatency(
            window_start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            window_end_time=datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
            window_duration_seconds=300.0,
            cell_index=1,
            network="5G",
            rsrp_mean=None,  # Null field
            rsrp_max=None,
            rsrp_min=None,
            rsrp_std=None,
            sample_count=50
        )

        mock_clickhouse_service.query_processed_latency.return_value = [partial_data]

        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "2024-01-01T12:00:00Z",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data[0]["rsrp_mean"] is None
        assert data[0]["sample_count"] == 50

    def test_get_processed_latency_invalid_datetime_format(self, test_client, mock_clickhouse_service):
        """Test endpoint with invalid datetime format."""
        response = test_client.get(
            "/api/v1/processed/latency/",
            params={
                "start_time": "invalid-date",
                "end_time": "2024-01-01T13:00:00Z",
                "cell_index": 1
            }
        )

        assert response.status_code == 422  # Validation error
