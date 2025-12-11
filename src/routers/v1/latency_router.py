"""
Endpoints for query data
"""
from fastapi import APIRouter, HTTPException, Query
from src.services.databases import ClickHouse
from src.models.processed_latency import ProcessedLatency

router = APIRouter()


@router.get("/latency/example", response_model=list[ProcessedLatency])
def get_latency_example():
    """
    Returns example response data for the processed latency endpoint.

    This endpoint provides a sample of what the actual data format looks like,
    useful for API documentation and client development.
    """
    return [
        {
            "window_start_time": 1733828400,
            "window_end_time": 1733828700,
            "window_duration_seconds": 0.0,
            "cell_index": 0,
            "network": "",
            "rsrp_mean": 0.0,
            "rsrp_max": 0.0,
            "rsrp_min": 0.0,
            "rsrp_std": 0.0,
            "sinr_mean": 0.0,
            "sinr_max": 0.0,
            "sinr_min": 0.0,
            "sinr_std": 0.0,
            "rsrq_mean": 0.0,
            "rsrq_max": 0.0,
            "rsrq_min": 0.0,
            "rsrq_std": 0.0,
            "latency_mean": 0.0,
            "latency_max": 0.0,
            "latency_min": 0.0,
            "latency_std": 0.0,
            "cqi_mean": 0.0,
            "cqi_max": 0.0,
            "cqi_min": 0.0,
            "cqi_std": 0.0,
            "primary_bandwidth": 0.0,
            "ul_bandwidth": 0.0,
            "sample_count": 0
        }
    ]


@router.get("/latency/", response_model=list[ProcessedLatency])
def get_processed_latency(
    start_time: int = Query(..., description="Window start time (Unix timestamp in seconds)"),
    end_time: int = Query(..., description="Window end time (Unix timestamp in seconds)"),

    cell_index: int = Query(..., description="Cell index (required)"),
    window_duration_seconds:int = Query(..., description="Duration of the target windows"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),

):
    """
    Query processed latency data with various filters.

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
            'window_duration_seconds':window_duration_seconds,
        }

        results = ClickHouse.service.query_processed_latency(**query_params)

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying processed latency: {str(e)}")
