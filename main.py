import asyncio
import os
from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI

from src.configs import load_all
from src.routers.v1 import v1_router
from src.services.databases import ClickHouse, Influx
from src.sink import KafkaSinkManager

from client import PolicyClient

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = ["network.data.ingested", "network.data.processed"]

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service:8000")
POLICY_COMPONENT_ID = os.getenv("POLICY_COMPONENT_ID", "data-storage")
POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"
POLICY_FAILOPEN = os.getenv("POLICY_FAILOPEN", "true").lower() == "true"

policy_client = PolicyClient(
    service_url=POLICY_SERVICE_URL,
    component_id=POLICY_COMPONENT_ID,
    enable_policy=POLICY_ENABLED,
    fail_open=POLICY_FAILOPEN
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database connections (singleton, handles connection internally)
    Influx.service  # Access triggers lazy initialization
    ClickHouse.service  # Access triggers lazy initialization
    load_all()

    # We may not want policy enabled, so we encase it in an if
    if POLICY_ENABLED:
        try:
            await Influx.service.connect()
            await ClickHouse.service.connect()

            await policy_client.register_component(
                component_type="storage",
                role="Storage",
                data_columns=[],
                auto_create_attributes=False,
                allowed_fields={
                    "data-storage:influx": Influx.service.get_fields(),
                    "data-storage:clickhouse": ClickHouse.service.get_metric_keys(),
                }
            )
            print("Registered with Policy Service")
        except Exception as e:
            print(f"Warning: Failed ot register with Policy Service: {e}")

    sink_manager = KafkaSinkManager(KAFKA_HOST, KAFKA_PORT, policy_client)

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

    kafka_thread = Thread(target=kafka_worker, daemon=True, name="kafka-sink-thread")
    kafka_thread.start()

    print(f"API started (Kafka connecting in background to {KAFKA_HOST}:{KAFKA_PORT})")

    yield

    try:
        await sink_manager.stop()
    except Exception as e:
        print(f"Warning: Error stopping Kafka sink: {e}")

    ClickHouse.service.client.close()


app = FastAPI(lifespan=lifespan)

app.include_router(v1_router, prefix="/api/v1", tags=["v1"])
