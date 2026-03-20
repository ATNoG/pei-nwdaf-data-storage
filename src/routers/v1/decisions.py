"""
Endpoints for querying decision data
"""

from fastapi import APIRouter, HTTPException, Query

from src.services.databases import ClickHouse

router = APIRouter()


@router.get("")
def get_decisions(
    start_time: int = Query(..., description="Start time (Unix timestamp in seconds)"),
    end_time: int = Query(..., description="End time (Unix timestamp in seconds)"),
    cell_id: int | None = Query(None, description="Cell ID filter (optional)"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
):
    """
    Query decision data with filters.

    Returns compressed decision data including:
    - cell_id: Cell identifier
    - id: Decision record ID
    - timestamp: When the decision was made
    - compression_method: Compression algorithm used (e.g., "gzip")
    - compressed_data: Base64-encoded compressed decision JSON

    To decompress the data:
    1. Decode base64
    2. Decompress using the specified compression method
    3. Parse resulting JSON
    """
    try:
        results = ClickHouse.service.query_decisions(
            start_time=start_time,
            end_time=end_time,
            cell_id=cell_id,
            offset=offset,
            limit=limit,
        )
        return results

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying decisions: {str(e)}"
        )
