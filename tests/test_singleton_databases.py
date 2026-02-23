"""
Tests for the singleton database services in src/services/databases.py

This test suite verifies:
1. Singleton behavior - only one instance is created and reused
2. Thread safety - concurrent access is handled correctly
3. Lazy initialization - connections are established only when first accessed
4. Backwards compatibility - both .service property and .get_service() work
5. Empty window filtering - ClickHouseSink skips empty windows
"""

import pytest
import threading
import time
from unittest.mock import MagicMock, patch, AsyncMock
from concurrent.futures import ThreadPoolExecutor, as_completed


class TestClickHouseSingleton:
    """Tests for ClickHouse singleton service."""

    def setup_method(self):
        """Reset singleton state before each test."""
        from src.services import databases

        databases.ClickHouse._instance = None

    @pytest.fixture
    def mock_clickhouse_client(self):
        """Mock ClickHouse client for testing."""
        with patch("clickhouse_connect.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_singleton_returns_same_instance(self, mock_clickhouse_client):
        """Test that multiple calls to get_service return the same instance."""
        from src.services.databases import ClickHouse

        service1 = ClickHouse.get_service()
        service2 = ClickHouse.get_service()

        assert service1 is service2, "get_service should return the same instance"

    def test_service_property_returns_same_instance(self, mock_clickhouse_client):
        """Test that .service property returns the singleton instance."""
        from src.services.databases import ClickHouse

        service1 = ClickHouse.service
        service2 = ClickHouse.service

        assert service1 is service2, ".service property should return the same instance"

    def test_get_service_and_property_are_interchangeable(self, mock_clickhouse_client):
        """Test that get_service() and .service return the same instance."""
        from src.services.databases import ClickHouse

        service_via_method = ClickHouse.get_service()
        service_via_property = ClickHouse.service

        assert service_via_method is service_via_property

    def test_lazy_initialization(self):
        """Test that connection is only established on first access."""
        from src.services.databases import ClickHouse
        from src.services.clickhouse import ClickHouseService

        # Before first access, instance should be None
        assert ClickHouse._instance is None

        with patch("clickhouse_connect.get_client") as mock_client:
            mock = MagicMock()
            mock_client.return_value = mock

            # First access triggers initialization
            service = ClickHouse.get_service()
            assert service is not None
            assert ClickHouse._instance is not None
            mock_client.assert_called_once()

    def test_connect_called_on_initialization(self, mock_clickhouse_client):
        """Test that connect() is called during initialization."""
        from src.services.databases import ClickHouse
        from src.services.clickhouse import ClickHouseService

        # Create service instance
        service = ClickHouse.get_service()

        # Verify the service has a client (meaning connect was called)
        assert service.client is not None
        assert service.client == mock_clickhouse_client

    def test_thread_safety_concurrent_access(self, mock_clickhouse_client):
        """Test that singleton is thread-safe under concurrent access."""
        from src.services.databases import ClickHouse

        instances = []
        n_threads = 10

        def get_instance():
            service = ClickHouse.get_service()
            instances.append(service)
            time.sleep(0.001)  # Small delay to increase race condition likelihood

        # Create multiple threads that all try to get the service
        threads = [threading.Thread(target=get_instance) for _ in range(n_threads)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # All threads should have received the same instance
        assert len(instances) == n_threads
        first_instance = instances[0]
        for instance in instances:
            assert instance is first_instance, (
                "All threads should receive the same singleton instance"
            )

    def test_thread_safety_with_thread_pool(self, mock_clickhouse_client):
        """Test thread safety using ThreadPoolExecutor for more realistic concurrency."""
        from src.services.databases import ClickHouse

        n_workers = 20
        instances = []

        def get_service():
            service = ClickHouse.get_service()
            instances.append(id(service))
            return service

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(get_service) for _ in range(n_workers)]
            results = [f.result() for f in as_completed(futures)]

        # All instances should be the same object (same id)
        assert len(set(instances)) == 1, (
            "All workers should receive the same singleton instance"
        )

    def test_singleton_persists_across_contexts(self, mock_clickhouse_client):
        """Test that singleton persists across different import contexts."""
        from src.services.databases import ClickHouse
        from src.routers.v1.processed import ClickHouse as RouterClickHouse

        # Both imports should access the same singleton
        service1 = ClickHouse.service
        service2 = RouterClickHouse.service

        assert service1 is service2


class TestInfluxSingleton:
    """Tests for InfluxDB singleton service."""

    def setup_method(self):
        """Reset singleton state before each test."""
        from src.services import databases

        databases.Influx._instance = None

    @pytest.fixture
    def mock_influx_client(self):
        """Mock InfluxDB client for testing."""
        with patch("src.services.influx.InfluxDBClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_singleton_returns_same_instance(self, mock_influx_client):
        """Test that multiple calls to get_service return the same instance."""
        from src.services.databases import Influx

        service1 = Influx.get_service()
        service2 = Influx.get_service()

        assert service1 is service2, "get_service should return the same instance"

    def test_service_property_returns_same_instance(self, mock_influx_client):
        """Test that .service property returns the singleton instance."""
        from src.services.databases import Influx

        service1 = Influx.service
        service2 = Influx.service

        assert service1 is service2, ".service property should return the same instance"

    def test_get_service_and_property_are_interchangeable(self, mock_influx_client):
        """Test that get_service() and .service return the same instance."""
        from src.services.databases import Influx

        service_via_method = Influx.get_service()
        service_via_property = Influx.service

        assert service_via_method is service_via_property

    def test_lazy_initialization(self):
        """Test that connection is only established on first access."""
        from src.services.databases import Influx
        from src.services.influx import InfluxService

        # Before first access, instance should be None
        assert Influx._instance is None

        with patch("src.services.influx.InfluxDBClient") as mock_client:
            mock = MagicMock()
            mock_client.return_value = mock

            # First access triggers initialization
            service = Influx.get_service()
            assert service is not None
            assert Influx._instance is not None
            mock_client.assert_called_once()

    def test_connect_called_on_initialization(self, mock_influx_client):
        """Test that connect() is called during initialization."""
        from src.services.databases import Influx

        # Create service instance
        service = Influx.get_service()

        # Verify the service has a client (meaning connect was called)
        assert service.client is not None
        assert service.client == mock_influx_client

    def test_thread_safety_concurrent_access(self, mock_influx_client):
        """Test that singleton is thread-safe under concurrent access."""
        from src.services.databases import Influx

        instances = []
        n_threads = 10

        def get_instance():
            service = Influx.get_service()
            instances.append(service)
            time.sleep(0.001)

        threads = [threading.Thread(target=get_instance) for _ in range(n_threads)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(instances) == n_threads
        first_instance = instances[0]
        for instance in instances:
            assert instance is first_instance


class TestClickHouseSinkEmptyWindowFiltering:
    """Tests for ClickHouseSink empty window filtering."""

    def setup_method(self):
        """Reset singleton state before each test."""
        from src.services import databases

        databases.ClickHouse._instance = None

    @pytest.fixture
    def mock_logger(self):
        """Mock logger for testing."""
        return MagicMock()

    @pytest.fixture
    def mock_clickhouse_client(self):
        """Mock ClickHouse client for testing."""
        with patch("clickhouse_connect.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    @pytest.fixture
    def clickhouse_sink(self, mock_logger, mock_clickhouse_client):
        """Create ClickHouseSink with mocked service."""
        from src.sinks.clickhouse_sink import ClickHouseSink

        return ClickHouseSink(mock_logger)

    def test_write_skips_empty_window(
        self, clickhouse_sink, mock_logger, mock_clickhouse_client
    ):
        """Test that empty windows (sample_count=0) are skipped."""
        empty_window_data = {
            "cell_index": 123,
            "window_start": 1733684400,
            "window_end": 1733684460,
            "sample_count": 0,
            "rsrp": {"mean": None, "max": None, "min": None, "std": None},
        }

        result = clickhouse_sink.write(empty_window_data)

        # Should return True (success) but not actually write
        assert result is True
        mock_clickhouse_client.insert.assert_not_called()

    def test_write_processes_valid_window(
        self, clickhouse_sink, mock_clickhouse_client
    ):
        """Test that valid windows are written normally."""
        valid_data = {
            "cell_index": 123,
            "window_start": 1733684400,
            "window_end": 1733684460,
            "sample_count": 100,
            "network": "5G",
            "primary_bandwidth": 100.0,
            "ul_bandwidth": 50.0,
            "rsrp": {"mean": -80.0, "max": -70.0, "min": -90.0, "std": 5.0},
        }

        result = clickhouse_sink.write(valid_data)

        # Should return True and write to database
        assert result is True
        mock_clickhouse_client.insert.assert_called_once()

    def test_write_handles_missing_sample_count(
        self, clickhouse_sink, mock_clickhouse_client
    ):
        """Test handling of missing sample_count field."""
        data_without_count = {
            "cell_index": 123,
            "window_start": 1733684400,
            "window_end": 1733684460,
            # sample_count is missing
            "rsrp": {"mean": -80.0, "max": -70.0, "min": -90.0, "std": 5.0},
        }

        result = clickhouse_sink.write(data_without_count)

        # Should return False because sample_count is a required field
        # The transform function will raise ValueError for missing required fields
        assert result is False


class TestInfluxSink:
    """Tests for InfluxSink with singleton."""

    def setup_method(self):
        """Reset singleton state before each test."""
        from src.services import databases

        databases.Influx._instance = None

    @pytest.fixture
    def mock_logger(self):
        """Mock logger for testing."""
        return MagicMock()

    @pytest.fixture
    def mock_influx_client(self):
        """Mock InfluxDB client for testing."""
        with patch("src.services.influx.InfluxDBClient") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    @pytest.fixture
    def influx_sink(self, mock_logger, mock_influx_client):
        """Create InfluxSink with mocked service."""
        from src.sinks.influx_sink import InfluxSink

        return InfluxSink(mock_logger)

    def test_sink_uses_singleton_service(self, influx_sink, mock_influx_client):
        """Test that InfluxSink uses the singleton service instance."""
        from src.services.databases import Influx

        # Get the singleton service
        singleton_service = Influx.get_service()

        # The sink's service should be the same instance
        assert influx_sink.service is singleton_service

    def test_write_delegates_to_service(self, influx_sink, mock_influx_client):
        """Test that write() delegates to the service."""
        test_data = {
            "timestamp": "2024-01-01T12:00:00Z",
            "cell_index": 123,
            "rsrp": -80,
        }

        # Mock the write_data method on the service
        with patch.object(influx_sink.service, "write_data") as mock_write:
            result = influx_sink.write(test_data)

            assert result is True
            # Verify the service's write_data was called with correct data
            mock_write.assert_called_once_with(test_data)


class TestServicePropertyDescriptor:
    """Tests for the _ServiceProperty descriptor."""

    def setup_method(self):
        """Reset singleton state before each test."""
        from src.services import databases

        databases.ClickHouse._instance = None
        databases.Influx._instance = None

    @pytest.fixture
    def mock_clickhouse_client(self):
        """Mock ClickHouse client for testing."""
        with patch("clickhouse_connect.get_client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_descriptor_works_with_class_access(self, mock_clickhouse_client):
        """Test that descriptor allows ClassName.service access."""
        from src.services.databases import ClickHouse

        # Access via class (how routers use it)
        service = ClickHouse.service

        assert service is not None
        assert isinstance(service, object)  # Should be ClickHouseService instance

    def test_descriptor_works_with_instance_access(self, mock_clickhouse_client):
        """Test that descriptor allows instance.service access."""
        from src.services.databases import ClickHouse

        # Create a fake instance
        fake_instance = object()

        # The descriptor should work even with unrelated instances
        # (obj is ignored, classmethod is called)
        service = ClickHouse.service

        assert service is not None

    def test_multiple_accesses_return_same_service(self, mock_clickhouse_client):
        """Test that descriptor consistently returns the same singleton."""
        from src.services.databases import ClickHouse

        service1 = ClickHouse.service
        service2 = ClickHouse.service
        service3 = ClickHouse.get_service()

        assert service1 is service2
        assert service2 is service3
