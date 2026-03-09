import asyncio
import os
from contextlib import asynccontextmanager
from threading import Thread

from fastapi import FastAPI

from src.configs import load_all
from src.routers.v1 import v1_router
from src.services.databases import ClickHouse, Influx
from src.sink import KafkaSinkManager

from policy_client import PolicyClient

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
KAFKA_TOPICS = ["network.data.ingested", "network.data.processed"]

POLICY_SERVICE_URL = os.getenv("POLICY_SERVICE_URL", "http://policy-service:8000")
POLICY_COMPONENT_ID = os.getenv("POLICY_COMPONENT_ID", "data-storage")
POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"
POLICY_FAILOPEN = os.getenv("POLICY_FAILOPEN", "true").lower() == "true"


def get_all_storage_fields() -> list[str]:
    """Get all field names from both InfluxDB and ClickHouse."""
    influx_fields = Influx.service.get_fields()
    clickhouse_fields = ClickHouse.service.get_metric_keys()

    # Combine and deduplicate while preserving order
    seen = set()
    all_fields = []
    for field_list in [influx_fields, clickhouse_fields]:
        for field in field_list:
            if field not in seen:
                seen.add(field)
                all_fields.append(field)
    return all_fields


policy_client = PolicyClient(
    service_url=POLICY_SERVICE_URL,
    component_id=POLICY_COMPONENT_ID,
    fields=get_all_storage_fields,
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
            print("Connecting to InfluxDB...")
            Influx.service.connect()
            print("InfluxDB connected")

            print("Connecting to ClickHouse...")
            ClickHouse.service.connect()
            print("ClickHouse connected")

            # Get fields after services are connected
            print("Getting fields from services...")
            influx_fields = Influx.service.get_fields()
            clickhouse_fields = ClickHouse.service.get_metric_keys()
            print(f"Got {len(influx_fields)} Influx fields, {len(clickhouse_fields)} ClickHouse fields")

            print("Registering with Policy Service...")
            result = await policy_client.register_component(
                component_type="storage",
                role=os.getenv("POLICY_ROLENAME", "Storage"),
                data_columns=get_all_storage_fields(),
                auto_create_attributes=False,
                allowed_fields={
                    "data-storage:influx": influx_fields,
                    "data-storage:clickhouse": clickhouse_fields,
                }
            )
            print(f"Registration result: {result}")
        except Exception as e:
            import traceback
            print(f"Warning: Failed to register with Policy Service: {e}")
            traceback.print_exc()

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
