import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch, AsyncMock
import sys


@pytest.fixture
def mock_influx_service():
    """Mock the InfluxService for testing."""
    mock = MagicMock()
    mock.connect = MagicMock()
    mock.get_data = MagicMock(return_value=[
        {
            "timestamp": "2024-01-01T12:00:00Z",
            "datarate": 100.5,
            "mean_latency": 20.3,
            "rsrp": -80,
            "sinr": 15.5,
            "rsrq": -10,
            "direction": "downlink",
            "network": "5G",
            "cqi": 12,
            "cell_index": 1,
            "primary_bandwidth": 100,
            "ul_bandwidth": 50,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "altitude": 10.5,
            "velocity": 5.0
        }
    ])
    mock.client = MagicMock()
    mock.client.close = MagicMock()
    return mock


@pytest.fixture
def mock_clickhouse_service():
    """Mock the ClickHouseService for testing."""
    mock = MagicMock()
    mock.connect = MagicMock()
    mock.client = MagicMock()
    mock.client.close = MagicMock()
    return mock


@pytest.fixture
def mock_kafka_sink_manager():
    """Mock the KafkaSinkManager for testing."""
    mock = MagicMock()
    mock.start = AsyncMock()
    mock.stop = AsyncMock()
    return mock


@pytest.fixture
def client(mock_influx_service, mock_clickhouse_service, mock_kafka_sink_manager):
    """Create a test client with mocked services."""
    with patch('src.routers.query.InfluxService', return_value=mock_influx_service):
        with patch('src.routers.query.ClickHouseService', return_value=mock_clickhouse_service):
            with patch('src.sink.KafkaSinkManager', return_value=mock_kafka_sink_manager):
                # Override the service instances
                import src.routers.query as query_module
                query_module.Influx.service = mock_influx_service
                query_module.ClickHouse.service = mock_clickhouse_service

                from main import app
                with TestClient(app) as test_client:
                    yield test_client


class TestQueryEndpoint:
    """Tests for the /data/raw endpoint."""

    def test_get_raw_data_default_params(self, client, mock_influx_service):
        """Test getting raw data with default pagination parameters."""
        response = client.get("/data/raw/?param=test")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1

        # Verify the service was called with default parameters
        mock_influx_service.get_data.assert_called_once_with(
            batch_number=0,
            batch_size=100
        )

    def test_get_raw_data_custom_pagination(self, client, mock_influx_service):
        """Test getting raw data with custom pagination parameters."""
        mock_influx_service.get_data.reset_mock()

        response = client.get("/data/raw/?param=test&batch_number=2&batch_size=50")

        assert response.status_code == 200

        # Verify the service was called with custom parameters
        mock_influx_service.get_data.assert_called_once_with(
            batch_number=2,
            batch_size=50
        )

    def test_get_raw_data_empty_result(self, client, mock_influx_service):
        """Test getting raw data when no data is available."""
        mock_influx_service.get_data.return_value = []

        response = client.get("/data/raw/?param=test")

        assert response.status_code == 200
        data = response.json()
        assert data == []

    def test_get_raw_data_validates_response_model(self, client, mock_influx_service):
        """Test that response data matches the Raw model."""
        response = client.get("/data/raw/?param=test")

        assert response.status_code == 200
        data = response.json()

        # Verify first item has expected fields
        if len(data) > 0:
            item = data[0]
            assert "timestamp" in item
            assert "datarate" in item
            assert isinstance(item["datarate"], (int, float, type(None)))

    def test_get_raw_data_service_exception(self, client, mock_influx_service):
        """Test handling of service exceptions."""
        mock_influx_service.get_data.side_effect = Exception("Database connection error")

        with pytest.raises(Exception):
            response = client.get("/data/raw/?param=test")


class TestAppLifespan:
    """Tests for application lifespan events."""

    def test_app_startup_connects_databases(self, mock_influx_service, mock_clickhouse_service):
        """Test that databases are connected on startup."""
        with patch('src.routers.query.InfluxService', return_value=mock_influx_service):
            with patch('src.routers.query.ClickHouseService', return_value=mock_clickhouse_service):
                mock_sink_mgr = MagicMock()
                mock_sink_mgr.start = AsyncMock()
                mock_sink_mgr.stop = AsyncMock()
                with patch('main.KafkaSinkManager', return_value=mock_sink_mgr):
                    import src.routers.query as query_module
                    query_module.Influx.service = mock_influx_service
                    query_module.ClickHouse.service = mock_clickhouse_service

                    from main import app
                    with TestClient(app):
                        # Verify connections were established
                        mock_influx_service.connect.assert_called_once()
                        mock_clickhouse_service.connect.assert_called_once()
                        mock_sink_mgr.start.assert_called_once()

    def test_app_handles_kafka_startup_failure(self, mock_influx_service, mock_clickhouse_service, mock_kafka_sink_manager):
        """Test that app continues if Kafka fails to start."""
        mock_kafka_sink_manager.start.side_effect = Exception("Kafka connection failed")

        with patch('src.routers.query.InfluxService', return_value=mock_influx_service):
            with patch('src.routers.query.ClickHouseService', return_value=mock_clickhouse_service):
                with patch('src.sink.KafkaSinkManager', return_value=mock_kafka_sink_manager):
                    import src.routers.query as query_module
                    query_module.Influx.service = mock_influx_service
                    query_module.ClickHouse.service = mock_clickhouse_service

                    from main import app
                    # Should not raise an exception
                    with TestClient(app):
                        pass
