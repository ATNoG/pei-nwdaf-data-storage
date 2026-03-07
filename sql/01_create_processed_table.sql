CREATE DATABASE IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.processed
(
    cell_index              Int32,
    src_ip                  Nullable(String),
    sample_count            Int32,
    window_start_time       DateTime64(3),
    window_end_time         DateTime64(3),
    window_duration_seconds Float64,
    network                 Nullable(String),
    metrics                 Map(String, Float64)
)
ENGINE = MergeTree
ORDER BY (cell_index, window_start_time)
SETTINGS index_granularity = 8192;
