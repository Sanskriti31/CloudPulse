CREATE TABLE IF NOT EXISTS cloudpulse.alert_rules_by_org (
    org_id text, rule_id uuid, rule_name text, metric_type text,
    operator text, threshold double, duration_seconds int, severity text,
    server_id uuid, enabled boolean, created_by text,
    created_at timestamp, updated_at timestamp,
    PRIMARY KEY ((org_id), rule_id)
);

CREATE TABLE IF NOT EXISTS cloudpulse.alert_state_by_server (
    server_id uuid, rule_id uuid, state text, event_id timeuuid,
    org_id text, metric_type text, severity text, threshold double,
    pending_since timestamp, opened_at timestamp,
    last_value double, last_evaluated timestamp,
    PRIMARY KEY ((server_id), rule_id)
);

CREATE TABLE IF NOT EXISTS cloudpulse.open_alerts_by_org (
    org_id text, server_id uuid, rule_id uuid, event_id timeuuid,
    hostname text, rule_name text, metric_type text, severity text,
    threshold double, triggered_value double, opened_at timestamp,
    PRIMARY KEY ((org_id), server_id, rule_id)
);

CREATE TABLE IF NOT EXISTS cloudpulse.alert_events_by_server (
    server_id uuid, month_bucket text, event_id timeuuid, rule_id uuid,
    org_id text, rule_name text, metric_type text, severity text,
    threshold double, triggered_value double, status text,
    opened_at timestamp, resolved_at timestamp, resolved_by text,
    resolved_value double,
    PRIMARY KEY ((server_id, month_bucket), event_id)
) WITH CLUSTERING ORDER BY (event_id DESC)
  AND default_time_to_live = 7776000;

CREATE TABLE IF NOT EXISTS cloudpulse.alert_events_by_rule (
    rule_id uuid, month_bucket text, event_id timeuuid, server_id uuid,
    org_id text, hostname text, metric_type text, severity text,
    threshold double, triggered_value double, status text,
    opened_at timestamp, resolved_at timestamp, resolved_by text,
    resolved_value double,
    PRIMARY KEY ((rule_id, month_bucket), event_id)
) WITH CLUSTERING ORDER BY (event_id DESC)
  AND default_time_to_live = 7776000;

INSERT INTO cloudpulse.alert_rules_by_org
  (org_id, rule_id, rule_name, metric_type, operator, threshold,
   duration_seconds, severity, enabled, created_by, created_at, updated_at)
VALUES ('local', uuid(), 'High CPU', 'cpu_percent', '>', 90, 30,
        'CRITICAL', true, 'seed', toTimestamp(now()), toTimestamp(now()));

INSERT INTO cloudpulse.alert_rules_by_org
  (org_id, rule_id, rule_name, metric_type, operator, threshold,
   duration_seconds, severity, enabled, created_by, created_at, updated_at)
VALUES ('local', uuid(), 'High memory', 'memory_percent', '>', 85, 30,
        'WARNING', true, 'seed', toTimestamp(now()), toTimestamp(now()));

INSERT INTO cloudpulse.alert_rules_by_org
  (org_id, rule_id, rule_name, metric_type, operator, threshold,
   duration_seconds, severity, enabled, created_by, created_at, updated_at)
VALUES ('local', uuid(), 'Disk almost full', 'disk_percent', '>', 90, 0,
        'CRITICAL', true, 'seed', toTimestamp(now()), toTimestamp(now()));