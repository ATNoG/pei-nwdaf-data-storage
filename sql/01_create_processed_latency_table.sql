-- Create processed_latency table for storing aggregated network metrics
CREATE TABLE IF NOT EXISTS processed_latency (
    -- Window temporal information
    window_start_time DateTime64(3),
    window_end_time DateTime64(3),
    window_duration_seconds Float64,

    -- Cell metadata
    cell_index Int32,
    network String,

    -- RSRP statistics (Signal power)
    rsrp_mean Nullable(Float64),
    rsrp_max Nullable(Float64),
    rsrp_min Nullable(Float64),
    rsrp_std Nullable(Float64),

    -- SINR statistics (Interference + noise)
    sinr_mean Nullable(Float64),
    sinr_max Nullable(Float64),
    sinr_min Nullable(Float64),
    sinr_std Nullable(Float64),

    -- RSRQ statistics (Signal quality)
    rsrq_mean Nullable(Float64),
    rsrq_max Nullable(Float64),
    rsrq_min Nullable(Float64),
    rsrq_std Nullable(Float64),

    -- Latency statistics
    latency_mean Nullable(Float64),
    latency_max Nullable(Float64),
    latency_min Nullable(Float64),
    latency_std Nullable(Float64),

    -- CQI statistics (Cell quality)
    cqi_mean Nullable(Float64),
    cqi_max Nullable(Float64),
    cqi_min Nullable(Float64),
    cqi_std Nullable(Float64),

    -- Bandwidth information
    primary_bandwidth Nullable(Float64),
    ul_bandwidth Nullable(Float64),

    -- Sample metadata (higher count = more confidence)
    sample_count Int32
)
ENGINE = MergeTree()
ORDER BY (cell_index, window_start_time)
SETTINGS index_granularity = 8192;
