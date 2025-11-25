import asyncio
import logging
from typing import Optional

from communs.kmw import PyKafBridge
from sinks.clickhouse_sink import ClickHouseSink
from sinks.influx_sink import InfluxSink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaSinkManager:
    def __init__(self, kafka_host: str, kafka_port: str):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port

        self.influx_sink = InfluxSink(logger)
        self.clickhouse_sink = ClickHouseSink(logger)

        self.bridge: Optional[PyKafBridge] = None
        self._running = False

    def route_message(self, topic: str, message: dict) -> bool:
        if topic == "raw-data":
            return self.influx_sink.write(message)
        elif topic == "processed-data":
            return self.clickhouse_sink.write(message)
        else:
            logger.warning(f"Unknown topic: {topic}")
            return False

    async def process_kafka_messages(self):
        if not self.bridge or not self.bridge.consumer:
            logger.error("Kafka bridge not initialized")
            return

        loop = asyncio.get_event_loop()

        try:
            while self._running:
                messages = await loop.run_in_executor(
                    None, self.bridge.consumer.poll, 1000
                )

                # process message

                await asyncio.sleep(0)

        except asyncio.CancelledError:
            logger.info("Kafka sink consumer task cancelled")

    def start(self, *topics):
        self.bridge = PyKafBridge(self.kafka_host, self.kafka_port, *topics)
        self._running = True

        logger.info(f"Starting Kafka Sink Manager for topics: {topics}")

        self.bridge.consumer = self.bridge.consumer or self.bridge.start()

        asyncio.run(self.process_kafka_messages())

    def stop(self):
        self._running = False
        if self.bridge:
            self.bridge.stop()
        logger.info("Kafka Sink Manager stopped")
