import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.routers.query import router, Influx, ClickHouse
from src.sink import KafkaSinkManager

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = ["raw-data"]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to databases
    Influx.service.connect()
    ClickHouse.service.connect()

    # Initialize and start Kafka sink manager
    sink_manager = KafkaSinkManager(KAFKA_HOST, KAFKA_PORT)

    try:
        await sink_manager.start(*KAFKA_TOPICS)
    except Exception as e:
        print(f"Warning: Failed to start Kafka sink: {e}")
        print("API will continue running without Kafka sink")

    yield

    try:
        await sink_manager.stop()
    except Exception as e:
        print(f"Warning: Error stopping Kafka sink: {e}")

    Influx.service.client.close()
    ClickHouse.service.client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/data", tags=["data"])
