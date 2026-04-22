CREATE TABLE IF NOT EXISTS analytics.metric_event_map
(
    event      String,
    metric_key String
)
ENGINE = ReplacingMergeTree
ORDER BY (event, metric_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.metric_event_map_mv
TO analytics.metric_event_map
AS
SELECT
    event,
    arrayJoin(mapKeys(metrics)) AS metric_key
FROM analytics.processed;
