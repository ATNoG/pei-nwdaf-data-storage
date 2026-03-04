"""
Endpoints for policy service queries
"""

from fastapi import APIRouter, HTTPException, Query

from src.services.databases import ClickHouse, Influx

router = APIRouter()


@app.get("/fields")
def get_available_fields():
    """
    Get available fields from InfluxDB and ClickHouse.

    Returns:
        Dictionary with 'influx' and 'clickhouse' field lists.
    """
    try:
        influx_fields = Influx.service.get_fields()
    except Exception as e:
        influx_fields = []
        print(f"Warning: Failed to get InfluxDB fields: {e}")

    try:
        clickhouse_metrics = ClickHouse.service.get_metric_keys()
    except Exception as e:
        clickhouse_metrics = []
        print(f"Warning: Failed to get ClickHouse metrics: {e}")

    return {
        "influx": influx_fields,
        "clickhouse": clickhouse_metrics
    }
