import asyncio
import logging
from typing import Optional

from comms.kmw import PyKafBridge
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

    def route_message(self, data: dict) -> bool:
        topic: str = data['topic']
        message: dict = data['content']
        if topic == "raw-data":
            return self.influx_sink.write(message)
        elif topic == "processed-data":
            return self.clickhouse_sink.write(message)
        else:
            logger.warning(f"Unknown topic: {topic}")
            return False

    def start(self, *topics):
        self.bridge = PyKafBridge(*topics, hostname=self.kafka_host, port=self.kafka_port)

        logger.info(f"Starting Kafka Sink Manager for topics: {topics}")

        for topic in topics:
            self.bridge.bind_topic(topic, self.route_message)

        self.bridge.start()

    def stop(self):
        self.bridge.stop()
        logger.info("Kafka Sink Manager stopped")
