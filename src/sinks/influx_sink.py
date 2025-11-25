from sinks.sinkI import Sink
from services.influx import InfluxService
class InfluxSink(Sink):
    def __init__(self, logger):
        self.service = InfluxService()
        self.service.connect()
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
