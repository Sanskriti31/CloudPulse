-- 1-minute rollups: one partition per server, metric and day (1,440 rows).
CREATE TABLE IF NOT EXISTS cloudpulse.metrics_rollup_1m (
    server_id    uuid,
    metric_type  text,
    bucket_date  date,
    window_start timestamp,
    min_value    double,
    max_value    double,
    avg_value    double,
    sample_count int,
    unit         text,
    PRIMARY KEY ((server_id, metric_type, bucket_date), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 2592000
  AND gc_grace_seconds = 86400
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  };

-- 1-hour rollups: one partition per server, metric and month (~744 rows).
CREATE TABLE IF NOT EXISTS cloudpulse.metrics_rollup_1h (
    server_id    uuid,
    metric_type  text,
    month_bucket text,
    window_start timestamp,
    min_value    double,
    max_value    double,
    avg_value    double,
    sample_count int,
    unit         text,
    PRIMARY KEY ((server_id, metric_type, month_bucket), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC)
  AND default_time_to_live = 31536000
  AND gc_grace_seconds = 86400
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 30
  };
