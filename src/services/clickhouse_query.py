class QueryCH:
    metric_keys = """
    SELECT DISTINCT key
    FROM analytics.metric_keys
    """

    metric_event_map = """
    SELECT metric_key, groupUniqArray(event) AS events
    FROM analytics.metric_event_map FINAL
    GROUP BY metric_key
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
