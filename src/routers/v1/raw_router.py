from fastapi import APIRouter, HTTPException, Query, Header, Request
from datetime import datetime
from src.services.databases import Influx

router = APIRouter()


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

    Policy: If X-Component-ID header is provided, applies policy transformations
    for the source component reading from data-storage:influx.
    """
    try:
        import sys

        query_params = {
            "start_time": start_time,
            "end_time": end_time,
            "cell_index": cell_index,
            "batch_number": batch_number,
        }
        print(f"API called with: start={start_time}, end={end_time}, cell={cell_index}, source={x_component_id}")
        sys.stdout.flush()

        results, has_next = Influx.service.query_raw_data(**query_params)
        print(f"Router got {len(results)} results")
        sys.stdout.flush()

        with_data = [r for r in results if r.get("mean_latency")]
        print(f"Rows with data: {len(with_data)}")
        sys.stdout.flush()

        # Get policy client from app state
        policy_client = getattr(request.app.state, 'policy_client', None) if hasattr(request, 'app') else None

        # Apply policy transformations if source component is provided
        if x_component_id and policy_client and policy_client._async_client.enable_policy:
            # When data-processor requests data from data-storage:
            # - source_id is the data-processor (the one making the request)
            # - sink_id is data-storage:influx (the resource being read from)
            source_id = x_component_id
            sink_id = "data-storage:influx"
            print(f"POLICY CHECK: source_id={source_id}, sink_id={sink_id}, action=read")
            print(f"POLICY CHECK: enable_policy={policy_client._async_client.enable_policy}, fail_open={policy_client._async_client.fail_open}")
            sys.stdout.flush()
            filtered_results = []

            for row in results:
                # Convert datetime objects to strings for policy processing
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = v.isoformat()

                # Apply policy transformation with fail_open safety
                try:
                    result = policy_client.process_data(
                        source_id=source_id,
                        sink_id=sink_id,
                        data=row,
                        action="read"
                    )

                    print(f"POLICY RESULT: allowed={result.allowed}, reason={result.reason}")
                    sys.stdout.flush()

                    if result.allowed:
                        filtered_results.append(result.data)
                except Exception as e:
                    # Policy failed - apply fail_open behavior
                    if policy_client._async_client.fail_open:
                        # Allow data through on policy failure
                        filtered_results.append(row)
                        print(f"Policy failed for row, allowing (fail_open): {e}")
                    else:
                        # Block data on policy failure
                        print(f"Policy failed for row, blocking (fail_closed): {e}")

            results = filtered_results
            print(f"After policy filter: {len(results)} results")
            sys.stdout.flush()
        else:
            # No policy - just convert datetime objects
            for row in results:
                for k, v in row.items():
                    if isinstance(v, datetime):
                        row[k] = v.isoformat()

        print(f"Returning {len(results)} results")
        sys.stdout.flush()
        return {"data": results, "has_next": has_next}

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error querying raw data: {str(e)}"
        )
