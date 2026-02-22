class QueryCH:
    processed = """
    SELECT
        *
    FROM analytics.processed
    WHERE cell_index = {cell_index:Int32}
      AND window_duration_seconds = {window_duration_seconds:Int32}
      AND toUnixTimestamp(window_start_time) >= {start_time:Int64}
      AND toUnixTimestamp(window_end_time) <= {end_time:Int64}
    ORDER BY window_end_time DESC
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """
