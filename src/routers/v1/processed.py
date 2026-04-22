import logging
import os

from fastapi import APIRouter, HTTPException, Query, Header, Request

from src.services.databases import ClickHouse

logger = logging.getLogger(__name__)

router = APIRouter()

POLICY_ENABLED = os.getenv("POLICY_ENABLED", "false").lower() == "true"


@router.get("/fields")
def get_processed_fields():
    """
    Returns all known metric keys grouped by the event type that produces them.

    Example: {"thrputUl_mbps_mean": ["PERF_DATA"], "speed_mean": ["UE_MOBILITY"]}
    """
    try:
        return ClickHouse.service.get_metric_event_map()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching metric fields: {str(e)}")


@router.get("")
def get_processed_data(
    request: Request,
    start_time: int = Query(..., description="Window start (Unix timestamp, seconds)"),
    end_time: int = Query(..., description="Window end (Unix timestamp, seconds)"),
    snssai_sst: str = Query(..., description="S-NSSAI SST (slice type)"),
    snssai_sd: str | None = Query(None, description="S-NSSAI SD (slice differentiator, 6 hex digits)"),
    dnn: str = Query(..., description="Data Network Name"),
    event: str | None = Query(None, description="Event type filter (e.g. PERF_DATA, UE_MOBILITY)"),
    window_duration_seconds: int | None = Query(None, description="Window duration filter (seconds)"),
    offset: int = Query(0, ge=0, description="Records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Max records to return"),
    x_component_id: str = Header(None, alias="X-Component-ID"),
):
    """
    Query processed analytics windows from ClickHouse.

    Groups are identified by snssai_sst + dnn + event. Additional UE-level tags
    (ueIpv4Addr, supi, etc.) are returned in the ue_tags field of each row.
    Metric stats are flattened: thrputUl_mbps_mean, thrputUl_mbps_min, etc.
    """
    try:
        results = ClickHouse.service.query_processed(
            start_time=start_time,
            end_time=end_time,
            snssai_sst=snssai_sst,
            snssai_sd=snssai_sd,
            dnn=dnn,
            event=event,
            window_duration_seconds=window_duration_seconds,
            offset=offset,
            limit=limit,
        )

        if POLICY_ENABLED and x_component_id:
            policy_client = getattr(request.app.state, "policy_client", None) if hasattr(request, "app") else None
            if policy_client:
                source_id = "data-storage:clickhouse"
                filtered = []
                for row in results:
                    try:
                        result = policy_client.process_data(
                            source_id=source_id,
                            sink_id=x_component_id,
                            data=row,
                            action="read",
                        )
                        if result.allowed:
                            filtered.append(result.data)
                    except Exception as e:
                        if policy_client._async_client.fail_open:
                            filtered.append(row)
                            logger.warning(f"Policy failed for row, allowing (fail_open): {e}")
                        else:
                            logger.warning(f"Policy failed for row, blocking (fail_closed): {e}")
                results = filtered

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying processed data: {str(e)}")
