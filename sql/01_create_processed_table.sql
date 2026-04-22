CREATE DATABASE IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.processed
(
    window_start            DateTime64(3),
    window_end              DateTime64(3),
    window_duration_seconds UInt32,
    sample_count            UInt32,
    snssai_sst              String,
    snssai_sd               String,
    dnn                     String,
    event                   String,
    ue_tags                 Map(String, String),
    metrics                 Map(String, Float64)
)
ENGINE = MergeTree
ORDER BY (snssai_sst, snssai_sd, dnn, event, window_start)
SETTINGS index_granularity = 8192;
