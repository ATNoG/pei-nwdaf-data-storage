import logging
import os
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Header, Request

from src.services.databases import Influx

logger = logging.getLogger(__name__)
router = APIRouter()

POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"


@router.get("")
def get_raw_data(
    request: Request,
    start_time: int = Query(..., description="Start time (Unix timestamp in seconds)"),
    end_time: int = Query(..., description="End time (Unix timestamp in seconds)"),
    batch_number: int = Query(1, ge=1, description="Batch number"),
    event: str = Query(None),
    ueIpv4Addr: str = Query(None),
    supi: str = Query(None),
    gpsi: str = Query(None),
    dnn: str = Query(None),
    snssai_sst: str = Query(None),
    snssai_sd: str = Query(None),
    x_component_id: str = Header(None, alias="X-Component-ID"),
):
    """
    Query raw data by time range and optional tag filters.
    At least one tag filter is recommended for meaningful results.
    """
    tags = {k: v for k, v in {
        "event": event,
        "ueIpv4Addr": ueIpv4Addr,
        "supi": supi,
        "gpsi": gpsi,
        "dnn": dnn,
        "snssai_sst": snssai_sst,
        "snssai_sd": snssai_sd,
    }.items() if v is not None}

    try:
        results, has_next = Influx.service.query_raw_data(
            start_time=start_time,
            end_time=end_time,
            tags=tags,
            batch_number=batch_number,
        )

        if POLICY_ENABLED and x_component_id:
            policy_client = getattr(request.app.state, "policy_client", None)
            if policy_client:
                filtered = []
                for row in results:
                    try:
                        result = policy_client.process_data(
                            source_id="data-storage:influx",
                            sink_id=x_component_id,
                            data=row,
                            action="read",
                        )
                        if result.allowed:
                            filtered.append(result.data)
                    except Exception as e:
                        if policy_client._async_client.fail_open:
                            filtered.append(row)
                results = filtered

        return {"data": results, "has_next": has_next}

    except Exception as e:
        logger.error(f"Error querying raw data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
