from fastapi import APIRouter, HTTPException, Query
from src.configs.influx_conf import InfluxConf
from src.services.influx import InfluxService
from src.models.raw import RawResponse
from datetime import datetime


router = APIRouter()

class Influx():
    conf = InfluxConf()
    service = InfluxService()


@router.get("/", response_model=RawResponse)
def get_raw_data(
    start_time: int = Query(..., description="Start time (Unix timestamp in seconds)"),
    end_time: int = Query(..., description="End time (Unix timestamp in seconds)"),
    cell_index: int = Query(..., description="Cell index (required)"),

    batch_number: int = Query(1, ge=1, description="Batch number"),
):
    """
    Query raw data entries between a given start and end time for a specific cell.

    This endpoint returns raw entries directly from the database,
    limited to a maximum of 50 per request.
    """
    try:
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "batch_number": batch_number,
        }

        results,has_next = Influx.service.query_raw_data(**query_params)

        for row in results:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()

        return {"data": results, "has_next": has_next}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying raw data: {str(e)}")
