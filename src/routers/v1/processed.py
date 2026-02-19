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

    The example dynamically includes all columns from the ClickHouse table schema.
    """
    columns = ClickHouse.service.get_columns()

    example = {}
    for col in sorted(columns):
        if "time" in col.lower():
            example[col] = 1733828400
        elif col == "cell_index":
            example[col] = 0
        elif col == "sample_count":
            example[col] = 0
        elif col in ("network", "data_type"):
            example[col] = ""
        elif col.endswith("_seconds"):
            example[col] = 0.0
        else:
            example[col] = 0.0

    return [example]


@router.get("")
def get_processed_latency(
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
        }

        results = ClickHouse.service.query_processed_latency(**query_params)

        return results

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying processed latency: {str(e)}"
        )
