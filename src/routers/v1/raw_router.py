"""
Endpoints for raw data queries
"""

import logging
import os

from fastapi import APIRouter, HTTPException, Query, Header, Request
from datetime import datetime

from src.services.databases import Influx

logger = logging.getLogger(__name__)

router = APIRouter()

POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"


@router.get("")
def get_raw_data(
    request: Request,
    start_time: int = Query(..., description="Start time (Unix timestamp in seconds)"),
    end_time: int = Query(..., description="End time (Unix timestamp in seconds)"),
    cell_index: int = Query(..., description="Cell index (required)"),
    batch_number: int = Query(1, ge=1, description="Batch number"),
    x_component_id: str = Header(None, alias="X-Component-ID"),
):
    """
    Query raw data entries between a given start and end time for a specific cell.

    This endpoint returns raw entries directly from the database,
    limited to a maximum of 50 per request.

    Policy: If X-Component-ID header is provided and POLICY_ENABLED=true,
    applies policy transformations for the source component reading from
    data-storage:influx.
    """
    try:
        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "batch_number": batch_number,
        }

        results, has_next = Influx.service.query_raw_data(**query_params)

        if POLICY_ENABLED and x_component_id:
            policy_client = getattr(request.app.state, 'policy_client', None) if hasattr(request, 'app') else None
            if policy_client:
                source_id = "data-storage:influx"
                sink_id = x_component_id
                filtered_results = []

                for row in results:
                    # Convert datetime objects to strings for policy processing
                    for k, v in row.items():
                        if isinstance(v, datetime):
                            row[k] = v.isoformat()

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
            else:
                # No policy client - just convert datetime objects
                for row in results:
                    for k, v in row.items():
                        if isinstance(v, datetime):
                            row[k] = v.isoformat()
        else:
            # No policy - just convert datetime objects
            for row in results:
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = v.isoformat()

        return {"data": results, "has_next": has_next}

    except Exception as e:
        logger.error(f"Error querying raw data: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error querying raw data: {str(e)}"
        )
