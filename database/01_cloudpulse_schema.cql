CREATE KEYSPACE IF NOT EXISTS cloudpulse
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
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

CREATE TABLE IF NOT EXISTS cloudpulse.metrics_by_server_bucket (
    server_id uuid,
    bucket_date date,
    metric_time timestamp,
    metric_type text,
    metric_value double,
    unit text,
    PRIMARY KEY ((server_id, bucket_date), metric_time, metric_type)
) WITH CLUSTERING ORDER BY (
    metric_time DESC,
    metric_type ASC
);

CREATE TABLE IF NOT EXISTS cloudpulse.latest_metric_by_server (
    server_id uuid,
    metric_type text,
    metric_value double,
    unit text,
    recorded_at timestamp,
    PRIMARY KEY ((server_id), metric_type)
);
