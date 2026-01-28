from src.sinks.sinkI import Sink
from src.services.databases import Influx


class InfluxSink(Sink):
    """Sink for writing raw data to InfluxDB using a singleton connection."""

    def __init__(self, logger):
        # Use the shared singleton from databases.py
        self.service = Influx.get_service()
        self.logger = logger
        logger.info("InfluxDB sink initialized")

    def write(self, data: dict) -> bool:
        try:
            self.service.write_data(data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write to InfluxDB: {e}")
            return False

    def write_batch(self, data_list: list[dict]) -> bool:
        try:
            self.service.write_batch(data_list)
            return True
        except Exception as e:
            self.logger.error(f"Failed to batch write to InfluxDB: {e}")
            return False
