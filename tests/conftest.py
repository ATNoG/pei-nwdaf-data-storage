"""Pytest configuration and fixtures."""
import sys
from unittest.mock import MagicMock


# Mock the utils.kmw module before importing
class MockPyKafBridge:
    """Mock PyKafBridge for testing."""
    def __init__(self, *topics, hostname=None, port=None):
        self.topics = topics
        self.hostname = hostname
        self.port = port
        self._closed = False
        self._callbacks = {}
        self._consumer_task = None  # Added for consumer task tracking

    def add_n_topics(self, topics, bind=None):
        """Mock add_n_topics method."""
        for topic in topics:
            self._callbacks[topic] = bind

    async def start_consumer(self):
        """Mock start_consumer method."""
        pass

    async def close(self):
        """Mock close method."""
        self._closed = True


# Create a mock module
mock_kmw = MagicMock()
mock_kmw.PyKafBridge = MockPyKafBridge

# Inject the mock into sys.modules
sys.modules['utils'] = MagicMock()
sys.modules['utils.kmw'] = mock_kmw
