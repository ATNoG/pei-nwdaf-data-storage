from fastapi import APIRouter, HTTPException, Query
from src.models.raw import RawResponse
from datetime import datetime
from src.services.databases import Influx

router = APIRouter()



@router.get("", response_model=RawResponse)
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
        import sys
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "batch_number": batch_number,
        }
        print(f"API called with: start={start_time}, end={end_time}, cell={cell_index}"); sys.stdout.flush()

        results,has_next = Influx.service.query_raw_data(**query_params)
        print(f"Router got {len(results)} results"); sys.stdout.flush()

        with_data = [r for r in results if r.get('mean_latency')]
        print(f"Rows with data: {len(with_data)}"); sys.stdout.flush()

        for row in results:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()

        print(f"Returning {len(results)} results"); sys.stdout.flush()
        return {"data": results, "has_next": has_next}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying raw data: {str(e)}")
