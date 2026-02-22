"""
Endpoints for query data
"""

from fastapi import APIRouter, HTTPException

from src.services.databases import Influx

router = APIRouter()


@router.get("", response_model=list[int])
def get_processed_data():
    """
    Return a list of know cell indexes
    """
    try:
        results = Influx.service.get_known_cells()
        return results

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error querying processed latency: {str(e)}"
        )
