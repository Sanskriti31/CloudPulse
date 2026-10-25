CREATE KEYSPACE IF NOT EXISTS cloudpulse
WITH REPLICATION = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
};

CREATE TABLE IF NOT EXISTS cloudpulse.server_by_id (
    server_id uuid PRIMARY KEY,
    org_id text,
    hostname text,
    os_name text,
    registered_at timestamp,
    health_status text,
    last_seen timestamp
);

CREATE TABLE IF NOT EXISTS cloudpulse.servers_by_org (
    org_id text,
    server_id uuid,
    hostname text,
    os_name text,
    registered_at timestamp,
    health_status text,
    last_seen timestamp,
    PRIMARY KEY ((org_id), server_id)
);

-- Raw metrics: one partition per server per day.
-- 30-day TTL; TWCS with 1-day windows drops whole expired files.
CREATE TABLE IF NOT EXISTS cloudpulse.metrics_by_server_bucket (
    server_id uuid,
    bucket_date date,
    metric_time timestamp,
    metric_type text,
    metric_value double,
    unit text,
    PRIMARY KEY ((server_id, bucket_date), metric_time, metric_type)
) WITH CLUSTERING ORDER BY (metric_time DESC, metric_type ASC)
  AND default_time_to_live = 604800
  AND gc_grace_seconds = 86400
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  };

-- Current value per metric. No TTL: it always holds the newest reading.
CREATE TABLE IF NOT EXISTS cloudpulse.latest_metric_by_server (
    server_id uuid,
    metric_type text,
    metric_value double,
    unit text,
    recorded_at timestamp,
    PRIMARY KEY ((server_id), metric_type)
);