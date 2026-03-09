import asyncio
import logging
from typing import Optional

import json

from utils.kmw import PyKafBridge
from src.sinks.clickhouse_sink import ClickHouseSink
from src.sinks.influx_sink import InfluxSink

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class KafkaSinkManager:
    def __init__(self, kafka_host: str, kafka_port: str, policy_client=None):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.policy_client = policy_client

        self.influx_sink = InfluxSink(logger)
        self.clickhouse_sink = ClickHouseSink(logger)

        self.bridge: Optional[PyKafBridge] = None
        self._running = False

    def route_message(self, data: dict) -> dict:
        topic: str = data['topic']
        message_str: str = data['content']

        try:
            message = json.loads(message_str)
            # logger.info(f"Parsed message successfully from {topic}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            return data

        sink_id = f"{self.policy_client.component_id}"
        if topic == "network.data.ingested":
            sink_id += ":influx"
            # Message is already the raw data we need
            logger.info(f"Attempting to write to InfluxDB: {list(message.keys())}")
            filtered_message = self._apply_policy(message, sink_id, topic)
            if not filtered_message:
                logger.warning(f"Message filtered by policy: sink={sink_id}")
            success = self.influx_sink.write(filtered_message)
            if success:
                logger.debug("wrote to InfluxDB")
            else:
                logger.error(f"Failed to write to InfluxDB: {filtered_message}")
        elif topic == "network.data.processed":
            sink_id += ":clickhouse"
            logger.info(f"Attempting to write to ClickHouse: {list(message.keys())}")
            filtered_message = self._apply_policy(message, sink_id, topic)
            if not filtered_message:
                logger.warning(f"Message filtered by policy: sink={sink_id}")
            # TODO
            success = self.clickhouse_sink.write(filtered_message)
            if not success:
                logger.error(f"Failed to write to ClickHouse: {filtered_message}")
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

    # NOTE: This function could in fact not exist, but creating a separate helper function
    # keeps things tidy and organized
    def _apply_policy(self, data: dict, sink_id: str, topic: str) -> dict:
        """
        Applies policy filtering, sink_id-specific, for differentiation upon configuration
        (This is so we don't mix raw and processed in Frontend)

        The naming standard should use ":" between the concrete component and the specified sink
        Example: sink_id = "data-storage:influx"
        """
        if self.policy_client is None or not self.policy_client.enable_policy:
            return data

        try:
            # Use sync wrapper for policy processing
            from policy_client import SyncPolicyClient
            sync_client = SyncPolicyClient(self.policy_client)

            result = sync_client.process_data(
                source_id="kafka",
                sink_id=sink_id,
                data=data,
                action="write"
            )

            if result.allowed:
                return result.data
            else:
                logger.warning(f"Policy blocked: sink={sink_id}, topic={topic}")
                return {}

        except Exception as e:
            if self.policy_client.fail_open:
                logger.warning(f"Policy failed for {sink_id}, allowing (fail_open): {e}")
                return data
            else:
                logger.error(f"Policy failed for {sink_id}, blocking (fail_closed): {e}")
                return {}

    async def stop(self):
        if self.bridge is not None:
            await self.bridge.close()
            logger.info("Kafka Sink Manager stopped")
        else:
            logger.info("Kafka Sink Manager was not running")
