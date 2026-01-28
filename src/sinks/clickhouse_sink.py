from src.sinks.sinkI import Sink
from src.services.databases import ClickHouse


class ClickHouseSink(Sink):
    """Sink for writing processed data to ClickHouse using a singleton connection."""

    def __init__(self, logger):
        # Use the shared singleton from databases.py
        self.service = ClickHouse.get_service()
        self.logger = logger
        logger.info("ClickHouse sink initialized")

    def write(self, data: dict) -> bool:
        try:
            # Skip empty windows (no samples) - not useful for training/storage
            if data.get('sample_count') == 0:
                self.logger.debug("Skipping empty window with no samples")
                return True

            self.service.write_data(data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write to ClickHouse: {e}")
            return False

    def write_batch(self, data_list: list[dict]) -> bool:
        try:
            self.service.write_batch(data_list)
            return True
        except Exception as e:
            self.logger.error(f"Failed to batch write to ClickHouse: {e}")
            return False
