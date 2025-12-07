"""
Endpoints for query data
"""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime
from src.configs.clickhouse_conf import ClickhouseConf
from src.services.clickhouse import ClickHouseService
from src.models.processed_latency import ProcessedLatency

router = APIRouter()

class ClickHouse():
    conf = ClickhouseConf()
    service = ClickHouseService()


@router.get("/latency/", response_model=list[ProcessedLatency])
def get_processed_latency(
    start_time: datetime = Query(..., description="Window start time (ISO format)"),
    end_time: datetime = Query(..., description="Window end time (ISO format)"),

    cell_index: int = Query(..., description="Cell index (required)"),

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
        }

        results = ClickHouse.service.query_processed_latency(**query_params)

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying processed latency: {str(e)}")
