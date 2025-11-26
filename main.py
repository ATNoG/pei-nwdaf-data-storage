import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.routers.query import router, Influx, ClickHouse
from src.sink import KafkaSinkManager

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "raw-data,processed-data").split(",")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect to databases
    Influx.service.connect()
    ClickHouse.service.connect()

    # Initialize and start Kafka sink manager
    sink_manager = KafkaSinkManager(KAFKA_HOST, KAFKA_PORT)

    # Run the sink in a background thread since it's blocking
    sink_task = asyncio.create_task(
        asyncio.to_thread(sink_manager.start, *KAFKA_TOPICS)
    )

    yield

    sink_manager.stop()
    try:
        await asyncio.wait_for(sink_task, timeout=5.0)
    except asyncio.TimeoutError:
        pass

    Influx.service.client.close()
    ClickHouse.service.client.close()

app = FastAPI(lifespan=lifespan)

app.include_router(router, prefix="/data", tags=["data"])
