class QueryCH:
    processed_latency = """
    SELECT
        window_start_time,
        window_end_time,
        window_duration_seconds,
        cell_index,
        network,
        rsrp_mean, rsrp_max, rsrp_min, rsrp_std,
        sinr_mean, sinr_max, sinr_min, sinr_std,
        rsrq_mean, rsrq_max, rsrq_min, rsrq_std,
        latency_mean, latency_max, latency_min, latency_std,
        cqi_mean, cqi_max, cqi_min, cqi_std,
        primary_bandwidth,
        ul_bandwidth,
        sample_count
    FROM analytics.processed_latency
    WHERE cell_index = {cell_index:Int32}
      AND window_duration_seconds = {window_duration_seconds:Int32}
      AND toUnixTimestamp(window_start_time) >= {start_time:Int64}
      AND toUnixTimestamp(window_end_time) <= {end_time:Int64}
    ORDER BY window_end_time DESC
    LIMIT {limit:Int32}
    OFFSET {offset:Int32}
    """
