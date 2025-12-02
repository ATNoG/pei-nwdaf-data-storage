import pytest
from unittest.mock import MagicMock, patch, AsyncMock
import json
import logging


@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def mock_influx_sink(mock_logger):
    """Mock InfluxSink for testing."""
    with patch('src.sink.InfluxSink') as mock:
        instance = mock.return_value
        instance.write = MagicMock(return_value=True)
        yield instance


@pytest.fixture
def mock_clickhouse_sink(mock_logger):
    """Mock ClickHouseSink for testing."""
    with patch('src.sink.ClickHouseSink') as mock:
        instance = mock.return_value
        instance.write = MagicMock(return_value=True)
        yield instance


@pytest.fixture
def kafka_sink_manager(mock_influx_sink, mock_clickhouse_sink):
    """Create a KafkaSinkManager instance with mocked sinks."""
    from src.sink import KafkaSinkManager

    manager = KafkaSinkManager("localhost", "9092")
    manager.influx_sink = mock_influx_sink
    manager.clickhouse_sink = mock_clickhouse_sink

    return manager


class TestKafkaSinkManager:
    """Tests for KafkaSinkManager class."""

    def test_initialization(self, mock_influx_sink, mock_clickhouse_sink):
        """Test KafkaSinkManager initialization."""
        from src.sink import KafkaSinkManager

        manager = KafkaSinkManager("test-host", "9093")

        assert manager.kafka_host == "test-host"
        assert manager.kafka_port == "9093"
        assert manager.bridge is None
        assert not manager._running

    def test_route_message_raw_data_success(self, kafka_sink_manager, mock_influx_sink):
        """Test routing raw-data messages to InfluxDB."""
        test_data = {
            "topic": "raw-data",
            "content": json.dumps({
                "timestamp": "2024-01-01T12:00:00Z",
                "datarate": 100.5,
                "rsrp": -80
            })
        }

        result = kafka_sink_manager.route_message(test_data)

        assert result == test_data
        mock_influx_sink.write.assert_called_once()

        # Verify the correct data was passed
        call_args = mock_influx_sink.write.call_args[0][0]
        assert call_args["timestamp"] == "2024-01-01T12:00:00Z"
        assert call_args["datarate"] == 100.5

    def test_route_message_processed_data_success(self, kafka_sink_manager, mock_clickhouse_sink):
        """Test routing processed-data messages to ClickHouse."""
        test_data = {
            "topic": "processed-data",
            "content": json.dumps({
                "id": 1,
                "processed_value": 42.5
            })
        }

        result = kafka_sink_manager.route_message(test_data)

        assert result == test_data
        mock_clickhouse_sink.write.assert_called_once()

        # Verify the correct data was passed
        call_args = mock_clickhouse_sink.write.call_args[0][0]
        assert call_args["id"] == 1
        assert call_args["processed_value"] == 42.5

    def test_route_message_invalid_json(self, kafka_sink_manager, mock_influx_sink):
        """Test handling of invalid JSON in message content."""
        test_data = {
            "topic": "raw-data",
            "content": "invalid json {"
        }

        result = kafka_sink_manager.route_message(test_data)

        # Should return original data when JSON parsing fails
        assert result == test_data
        mock_influx_sink.write.assert_not_called()

    def test_route_message_unknown_topic(self, kafka_sink_manager, mock_influx_sink, mock_clickhouse_sink):
        """Test handling of unknown topic."""
        test_data = {
            "topic": "unknown-topic",
            "content": json.dumps({"data": "test"})
        }

        result = kafka_sink_manager.route_message(test_data)

        assert result == test_data
        mock_influx_sink.write.assert_not_called()
        mock_clickhouse_sink.write.assert_not_called()

    def test_route_message_influx_write_failure(self, kafka_sink_manager, mock_influx_sink):
        """Test handling of InfluxDB write failure."""
        mock_influx_sink.write.return_value = False

        test_data = {
            "topic": "raw-data",
            "content": json.dumps({"timestamp": "2024-01-01T12:00:00Z"})
        }

        result = kafka_sink_manager.route_message(test_data)

        assert result == test_data
        mock_influx_sink.write.assert_called_once()

    def test_route_message_clickhouse_write_failure(self, kafka_sink_manager, mock_clickhouse_sink):
        """Test handling of ClickHouse write failure."""
        mock_clickhouse_sink.write.return_value = False

        test_data = {
            "topic": "processed-data",
            "content": json.dumps({"id": 1})
        }

        result = kafka_sink_manager.route_message(test_data)

        assert result == test_data
        mock_clickhouse_sink.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_kafka_consumer(self, kafka_sink_manager):
        """Test starting the Kafka consumer."""
        topics = ["raw-data", "processed-data"]

        await kafka_sink_manager.start(*topics)

        assert kafka_sink_manager.bridge is not None
        assert kafka_sink_manager.bridge.topics == tuple(topics)
        assert kafka_sink_manager.bridge.hostname == "localhost"
        assert kafka_sink_manager.bridge.port == "9092"

    @pytest.mark.asyncio
    async def test_stop_kafka_consumer(self, kafka_sink_manager):
        """Test stopping the Kafka consumer."""
        # Start first
        await kafka_sink_manager.start("raw-data")

        # Then stop
        await kafka_sink_manager.stop()

        assert kafka_sink_manager.bridge._closed

    @pytest.mark.asyncio
    async def test_stop_without_bridge(self, kafka_sink_manager):
        """Test stopping when bridge was never started."""
        # Should not raise an exception
        await kafka_sink_manager.stop()

    def test_multiple_messages_same_topic(self, kafka_sink_manager, mock_influx_sink):
        """Test routing multiple messages to the same topic."""
        messages = [
            {
                "topic": "raw-data",
                "content": json.dumps({"timestamp": f"2024-01-01T12:00:0{i}Z", "datarate": 100 + i})
            }
            for i in range(3)
        ]

        for msg in messages:
            kafka_sink_manager.route_message(msg)

        assert mock_influx_sink.write.call_count == 3

    def test_message_with_all_fields(self, kafka_sink_manager, mock_influx_sink):
        """Test routing message with all expected fields."""
        complete_data = {
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

        test_data = {
            "topic": "raw-data",
            "content": json.dumps(complete_data)
        }

        kafka_sink_manager.route_message(test_data)

        mock_influx_sink.write.assert_called_once()
        call_args = mock_influx_sink.write.call_args[0][0]

        # Verify all fields are passed correctly
        for key, value in complete_data.items():
            assert call_args[key] == value
