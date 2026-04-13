"""
Endpoints for query data
"""

import logging
import os

from fastapi import APIRouter, HTTPException, Query, Header, Request

from src.services.databases import ClickHouse

logger = logging.getLogger(__name__)

router = APIRouter()

POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"


@router.get("/example")
def get_latency_example():
    """
    Returns example response data for the processed endpoint.

    This endpoint provides a sample of what the actual data format looks like,
    useful for API documentation and client development.

    The example dynamically includes all known metric keys from ClickHouse.
    """
    example = {
        "cell_index": 0,
        "ip_src": None,
        "sample_count": 0,
        "window_start_time": 1733828400,
        "window_end_time": 1733828410,
        "window_duration_seconds": 0.0,
        "network": "",
    }
    for key in ClickHouse.service.get_metric_keys():
        example[key] = 0.0

    return [example]


@router.get("")
def get_processed_data(
    request: Request,
    start_time: int = Query(
        ..., description="Window start time (Unix timestamp in seconds)"
    ),
    end_time: int = Query(
        ..., description="Window end time (Unix timestamp in seconds)"
    ),
    cell_index: int = Query(..., description="Cell index (required)"),
    window_duration_seconds: int = Query(
        ..., description="Duration of the target windows"
    ),
    ip_src: str | None = Query(None, description="Source IP filter: omit for cell-level only, '*' for all per-IP rows, or a specific IP"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of records to return"
    ),
    x_component_id: str = Header(None, alias="X-Component-ID"),
):
    """
    Query processed data with various filters.

    Returns aggregated statistics over time windows including:
    - Signal quality metrics
    - Performance metrics
    - Network information
    - Statistical measures for each metric

    Policy: If X-Component-ID header is provided and POLICY_ENABLED=true,
    applies policy transformations for the source component reading from
    data-storage:clickhouse.
    """
    try:
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "offset": offset,
            "limit": limit,
            "window_duration_seconds": window_duration_seconds,
            "ip_src": ip_src,
        }

        results = ClickHouse.service.query_processed(**query_params)

        if POLICY_ENABLED and x_component_id:
            policy_client = getattr(request.app.state, 'policy_client', None) if hasattr(request, 'app') else None
            if policy_client:
                source_id = "data-storage:clickhouse"
                sink_id = x_component_id
                filtered_results = []

                for row in results:
                    try:
                        result = policy_client.process_data(
                            source_id=source_id,
                            sink_id=sink_id,
                            data=row,
                            action="read"
                        )

                        if result.allowed:
                            filtered_results.append(result.data)
                    except Exception as e:
                        if policy_client._async_client.fail_open:
                            filtered_results.append(row)
                            logger.warning(f"Policy failed for row, allowing (fail_open): {e}")
                        else:
                            logger.warning(f"Policy failed for row, blocking (fail_closed): {e}")

                results = filtered_results

        return results

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying processed latency: {str(e)}"
        )
