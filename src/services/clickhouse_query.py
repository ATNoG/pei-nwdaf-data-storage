class QueryCH:
    processed = """
    SELECT
        *
    FROM analytics.processed
    WHERE cell_index = {cell_index:Int32}
      AND ip_src IS NULL
      AND window_duration_seconds = {window_duration_seconds:Int32}
      AND toUnixTimestamp(window_start_time) >= {start_time:Int64}
      AND toUnixTimestamp(window_end_time) <= {end_time:Int64}
    ORDER BY window_end_time DESC
    LIMIT 1 BY cell_index, window_start_time, window_duration_seconds
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """

    processed_by_ip = """
    SELECT
        *
    FROM analytics.processed
    WHERE cell_index = {cell_index:Int32}
      AND ip_src = {ip_src:String}
      AND window_duration_seconds = {window_duration_seconds:Int32}
      AND toUnixTimestamp(window_start_time) >= {start_time:Int64}
      AND toUnixTimestamp(window_end_time) <= {end_time:Int64}
    ORDER BY window_end_time DESC
    LIMIT 1 BY cell_index, ip_src, window_start_time, window_duration_seconds
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """

    processed_all_ips = """
    SELECT
        *
    FROM analytics.processed
    WHERE cell_index = {cell_index:Int32}
      AND ip_src IS NOT NULL
      AND window_duration_seconds = {window_duration_seconds:Int32}
      AND toUnixTimestamp(window_start_time) >= {start_time:Int64}
      AND toUnixTimestamp(window_end_time) <= {end_time:Int64}
    ORDER BY window_end_time DESC
    LIMIT 1 BY cell_index, ip_src, window_start_time, window_duration_seconds
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """

    metric_keys = """
    SELECT DISTINCT key
    FROM analytics.metric_keys
    """

    decisions = """
    SELECT
        cell_id,
        id,
        timestamp,
        compression_method,
        compressed_data
    FROM analytics.decisions
    WHERE cell_id = {cell_id:Int32}
      AND toUnixTimestamp(timestamp) >= {start_time:Int64}
      AND toUnixTimestamp(timestamp) <= {end_time:Int64}
    ORDER BY timestamp DESC
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """

    decisions_all = """
    SELECT
        cell_id,
        id,
        timestamp,
        compression_method,
        compressed_data
    FROM analytics.decisions
    WHERE toUnixTimestamp(timestamp) >= {start_time:Int64}
      AND toUnixTimestamp(timestamp) <= {end_time:Int64}
    ORDER BY timestamp DESC
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """
