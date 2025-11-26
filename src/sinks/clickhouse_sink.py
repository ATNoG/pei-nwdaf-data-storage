from src.sinks.sinkI import Sink
from src.services.clickhouse import ClickHouseService

class ClickHouseSink(Sink):
    def __init__(self,logger):
        self.service = ClickHouseService()
        self.service.connect()
        self.logger = logger
        logger.info("ClickHouse sink initialized")

    def write(self, data: dict) -> bool:
        try:
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
