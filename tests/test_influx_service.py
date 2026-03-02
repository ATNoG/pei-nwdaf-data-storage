import pytest
from unittest.mock import MagicMock, patch
from src.services.influx import InfluxService


@pytest.fixture
def mock_influx_client():
    """Mock InfluxDB client for testing."""
    with patch("influxdb_client.client.influxdb_client.InfluxDBClient") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def influx_service(mock_influx_client):
    """Create an InfluxService instance with mocked client."""
    with patch("src.services.influx.InfluxDBClient", return_value=mock_influx_client):
        service = InfluxService()
        service.connect()
        return service


class TestGetFields:
    """Tests for InfluxService.get_fields()."""

    def test_get_fields_returns_list_of_strings(self, influx_service):
        """Test that get_fields returns a list of field key strings."""
        mock_record_1 = MagicMock()
        mock_record_1.get_value.return_value = "rsrp"
        mock_record_2 = MagicMock()
        mock_record_2.get_value.return_value = "sinr"
        mock_record_3 = MagicMock()
        mock_record_3.get_value.return_value = "mean_latency"

        mock_table = MagicMock()
        mock_table.records = [mock_record_1, mock_record_2, mock_record_3]

        influx_service.query_api.query.return_value = [mock_table]

        fields = influx_service.get_fields()

        assert fields == ["rsrp", "sinr", "mean_latency"]

    def test_get_fields_empty(self, influx_service):
        """Test that get_fields returns an empty list when no fields exist."""
        influx_service.query_api.query.return_value = []

        fields = influx_service.get_fields()

        assert fields == []

    def test_get_fields_skips_none_values(self, influx_service):
        """Test that get_fields skips records with None values."""
        mock_record_valid = MagicMock()
        mock_record_valid.get_value.return_value = "rsrp"
        mock_record_none = MagicMock()
        mock_record_none.get_value.return_value = None

        mock_table = MagicMock()
        mock_table.records = [mock_record_valid, mock_record_none]

        influx_service.query_api.query.return_value = [mock_table]

        fields = influx_service.get_fields()

        assert fields == ["rsrp"]
        assert None not in fields

    def test_get_fields_calls_correct_query(self, influx_service):
        """Test that get_fields issues a query referencing metric_keys."""
        influx_service.query_api.query.return_value = []

        influx_service.get_fields()

        influx_service.query_api.query.assert_called_once()
        query_str = influx_service.query_api.query.call_args[0][0]
        assert "fieldKeys" in query_str
