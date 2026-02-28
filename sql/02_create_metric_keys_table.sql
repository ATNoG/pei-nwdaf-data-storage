CREATE TABLE IF NOT EXISTS analytics.metric_keys
(
    key String
)
ENGINE = ReplacingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.metric_keys_mv
TO analytics.metric_keys
AS
SELECT arrayJoin(mapKeys(metrics)) AS key
FROM analytics.processed;
