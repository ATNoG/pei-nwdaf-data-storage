import logging
from typing import Optional

import json

from utils.kmw import PyKafBridge
from src.sinks.clickhouse_sink import ClickHouseSink
from src.sinks.influx_sink import InfluxSink

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class KafkaSinkManager:
    def __init__(self, kafka_host: str, kafka_port: str):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port

        self.influx_sink = InfluxSink(logger)
        self.clickhouse_sink = ClickHouseSink(logger)

        self.bridge: Optional[PyKafBridge] = None
        self._running = False

    def route_message(self, data: dict) -> dict:
        topic: str = data['topic']
        message_str: str = data['content']

        try:
            message = json.loads(message_str)
            logger.info(f"Parsed message successfully from {topic}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            return data

        if topic == "raw-data":
            # Message is already the raw data we need
            logger.info(f"Attempting to write to InfluxDB: {list(message.keys())}")
            success = self.influx_sink.write(message)
            if success:
                logger.debug(f"wrote to InfluxDB")
            else:
                logger.error(f"Failed to write to InfluxDB: {message}")
        elif topic == "processed-data":
            # TODO
            success = self.clickhouse_sink.write(message)
            if not success:
                logger.error(f"Failed to write to ClickHouse: {message}")
        else:
            logger.warning(f"Unknown topic: {topic}")

        # Return the data for the callback chain
        return data

    async def start(self, *topics):
        self.bridge = PyKafBridge(*topics, hostname=self.kafka_host, port=self.kafka_port)

        logger.info(f"Starting Kafka Sink Manager for topics: {topics}")

        self.bridge.add_n_topics(topics, bind=self.route_message)

        await self.bridge.start_consumer()
        
        # Keep the event loop alive - wait for the consumer task to complete
        # This prevents asyncio.run() from exiting and cancelling the consumer task
        if self.bridge._consumer_task:
            await self.bridge._consumer_task

    async def stop(self):
        if self.bridge is not None:
            await self.bridge.close()
            logger.info("Kafka Sink Manager stopped")
        else:
            logger.info("Kafka Sink Manager was not running")
