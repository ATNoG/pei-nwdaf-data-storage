import base64
import gzip
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from utils.kmw import PyKafBridge

from src.sinks.clickhouse_sink import ClickHouseSink
from src.sinks.influx_sink import InfluxSink

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIMEOUT = float(os.getenv("BATCH_TIMEOUT", "1.0"))


class KafkaSinkManager:
    def __init__(self, kafka_host: str, kafka_port: str, policy_client=None):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.policy_client = policy_client

        self.influx_sink = InfluxSink(logger)
        self.clickhouse_sink = ClickHouseSink(logger)

        self.bridge: Optional[PyKafBridge] = None
        self._running = False

        self._influx_buffer: list[dict] = []
        self._buffer_lock = threading.Lock()
        self._last_flush = time.monotonic()

        self._flush_thread = threading.Thread(target=self._periodic_flush, daemon=True)
        self._flush_thread.start()

    def _periodic_flush(self):
        while True:
            time.sleep(BATCH_TIMEOUT)
            self._maybe_flush()

    def _flush_influx(self):
        with self._buffer_lock:
            batch = self._influx_buffer
            self._influx_buffer = []
            self._last_flush = time.monotonic()

        if batch:
            success = self.influx_sink.write_batch(batch)
            if success:
                logger.info(f"Flushed {len(batch)} records to InfluxDB")
            else:
                logger.error(f"Failed to flush {len(batch)} records to InfluxDB")

    def _maybe_flush(self):
        if (
            len(self._influx_buffer) >= BATCH_SIZE
            or (time.monotonic() - self._last_flush) >= BATCH_TIMEOUT
        ):
            self._flush_influx()

    def route_message(self, data: dict) -> dict:
        topic: str = data["topic"]
        message_str: str = data["content"]

        try:
            message = json.loads(message_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            return data

        # Policy is applied upstream by Ingestion/Processor before data reaches Kafka.
        # Data-Storage writes received data as-is to the appropriate database.
        if topic == "network.data.ingested":
            # Raw data -> InfluxDB
            records = message if isinstance(message, list) else [message]
            for record in records:
                with self._buffer_lock:
                    self._influx_buffer.append(record)
            self._maybe_flush()
        elif topic == "network.data.processed":
            tags = message.get("tags", {})
            logger.info(f"Writing to ClickHouse: event={tags.get('event')} "
                        f"sst={tags.get('snssai_sst')} dnn={tags.get('dnn')}")
            success = self.clickhouse_sink.write(message)
            if not success:
                logger.error(f"Failed to write to ClickHouse")
        elif topic == "network.decisions":
            try:
                # Message format: {"compression": "gzip", "data": "base64..."}
                compression_method = message.get("compression")
                compressed_data = message.get("data")

                if not compression_method or not compressed_data:
                    logger.error(f"Invalid decision message format: {message.keys()}")
                    return data

                # Decompress to extract cell_id and timestamp
                decoded = base64.b64decode(compressed_data)
                if compression_method == "gzip":
                    decompressed = gzip.decompress(decoded).decode("utf-8")
                else:
                    logger.error(f"Unsupported compression method: {compression_method}")
                    return data

                decision_data = json.loads(decompressed)
                cell_id = decision_data.get("cell_id")
                timestamp_str = decision_data.get("timestamp")

                if not cell_id or not timestamp_str:
                    logger.error(f"Missing cell_id or timestamp in decision: {decision_data.keys()}")
                    return data

                # Parse ISO timestamp to datetime
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                # Write to ClickHouse (stores compressed data)
                from src.services.databases import ClickHouse
                ClickHouse.get_service().write_decision(
                    cell_id=cell_id,
                    timestamp=timestamp,
                    compression_method=compression_method,
                    compressed_data=compressed_data,
                )
                logger.info(f"Stored decision for cell {cell_id} at {timestamp}")

            except Exception as e:
                logger.error(f"Failed to process decision message: {e}")
        else:
            logger.warning(f"Unknown topic: {topic}")

        # Return the data for the callback chain
        return data

    async def start(self, *topics):
        self.bridge = PyKafBridge(
            *topics, hostname=self.kafka_host, port=self.kafka_port
        )

        logger.info(f"Starting Kafka Sink Manager for topics: {topics}")

        self.bridge.add_n_topics(topics, bind=self.route_message)

        await self.bridge.start_consumer()

        # Keep the event loop alive - wait for the consumer task to complete
        # This prevents asyncio.run() from exiting and cancelling the consumer task
        if self.bridge._consumer_task:
            await self.bridge._consumer_task

    async def stop(self):
        self._flush_influx()
        if self.bridge is not None:
            await self.bridge.close()
            logger.info("Kafka Sink Manager stopped")
        else:
            logger.info("Kafka Sink Manager was not running")
