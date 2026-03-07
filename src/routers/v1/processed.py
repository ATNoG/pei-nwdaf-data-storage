"""
Endpoints for query data
"""

from fastapi import APIRouter, HTTPException, Query

from src.services.databases import ClickHouse

router = APIRouter()


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
):
    """
    Query processed data with various filters.

    Returns aggregated statistics over time windows including:
    - Signal quality metrics
    - Performance metrics
    - Network information
    - Statistical measures for each metric
    """
    try:
        # Build query parameters
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

        return results

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying processed latency: {str(e)}"
        )
