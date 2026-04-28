CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.decisions (
    cell_id Int32,
    id UInt64,
    timestamp DateTime64(3),
    compression_method String,
    compressed_data String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (cell_id, timestamp)
TTL toDateTime(timestamp) + INTERVAL 1 YEAR;
