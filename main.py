import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.routers.v1 import v1_router
from src.routers.v1.latency import ClickHouse
from src.sink import KafkaSinkManager
import asyncio

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = ["raw-data","processed-data"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to databases
    ClickHouse.service.connect()

    # Initialize and start Kafka sink manager
    sink_manager = KafkaSinkManager(KAFKA_HOST, KAFKA_PORT)

    try:
        await asyncio.to_thread(sink_manager.start, *KAFKA_TOPICS)
    except Exception as e:
        print(f"Warning: Failed to start Kafka sink: {e}")
        print("API will continue running without Kafka sink")

    yield

    try:
        await sink_manager.stop()
    except Exception as e:
        print(f"Warning: Error stopping Kafka sink: {e}")

    ClickHouse.service.client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(v1_router, prefix="/api/v1", tags=["v1"])
