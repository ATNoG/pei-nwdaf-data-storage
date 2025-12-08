import os
import asyncio
from threading import Thread
from contextlib import asynccontextmanager
from fastapi import FastAPI

from src.routers.v1 import v1_router
from src.routers.v1.latency import ClickHouse
from src.sink import KafkaSinkManager

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = ["network.data.ingested", "network.data.processed"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    ClickHouse.service.connect()

    sink_manager = KafkaSinkManager(KAFKA_HOST, KAFKA_PORT)

    def kafka_worker():
        """
        Kafka runs in its own thread with its own event loop.
        This prevents rdkafka from blocking FastAPI startup.
        """
        try:
            asyncio.run(sink_manager.start(*KAFKA_TOPICS))
            print("Kafka consumer started successfully")
        except Exception as e:
            print(f"Kafka worker crashed: {e}")

    kafka_thread = Thread(
        target=kafka_worker,
        daemon=True,
        name="kafka-sink-thread"
    )
    kafka_thread.start()

    print(
        f"API started (Kafka connecting in background to "
        f"{KAFKA_HOST}:{KAFKA_PORT})"
    )

    yield

    try:
        await sink_manager.stop()
    except Exception as e:
        print(f"Warning: Error stopping Kafka sink: {e}")

    ClickHouse.service.client.close()


app = FastAPI(lifespan=lifespan)

app.include_router(v1_router, prefix="/api/v1", tags=["v1"])
