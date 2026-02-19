CREATE DATABASE IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.processed
(
    timestamp DateTime64(3),
    cell_index Int32,

    network Nullable(String),
    data_type Nullable(String),  -- e.g., 'latency', 'anomaly', etc.

    rsrp_mean Nullable(Float64),
    rsrp_max Nullable(Float64),
    rsrp_min Nullable(Float64),
    rsrp_std Nullable(Float64),

    sinr_mean Nullable(Float64),
    sinr_max Nullable(Float64),
    sinr_min Nullable(Float64),
    sinr_std Nullable(Float64),

    rsrq_mean Nullable(Float64),
    rsrq_max Nullable(Float64),
    rsrq_min Nullable(Float64),
    rsrq_std Nullable(Float64),

    latency_mean Nullable(Float64),
    latency_max Nullable(Float64),
    latency_min Nullable(Float64),
    latency_std Nullable(Float64),

    cqi_mean Nullable(Float64),
    cqi_max Nullable(Float64),
    cqi_min Nullable(Float64),
    cqi_std Nullable(Float64),

    primary_bandwidth Nullable(Float64),
    ul_bandwidth Nullable(Float64),

    sample_count Int32,
    window_start_time DateTime64(3),
    window_end_time DateTime64(3),
    window_duration_seconds Float64
)
ENGINE = MergeTree
ORDER BY (cell_index, timestamp)
SETTINGS index_granularity = 8192;
