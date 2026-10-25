-- Logs, newest first, one partition per server per day.
-- Lines with the same timestamp keep their file order via seq.
CREATE TABLE IF NOT EXISTS cloudpulse.logs_by_server_day (
    server_id   uuid,
    bucket_date date,
    log_time    timestamp,
    seq         int,
    log_id      timeuuid,
    level       text,
    source      text,
    message     text,
    PRIMARY KEY ((server_id, bucket_date), log_time, seq, log_id)
) WITH CLUSTERING ORDER BY (log_time DESC, seq DESC, log_id DESC)
  AND default_time_to_live = 1209600
  AND gc_grace_seconds = 86400
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  };

-- Same logs, partitioned by level as well.
-- Answers: "show only this server's ERROR logs for a day" without
-- reading every INFO line (no secondary index, no ALLOW FILTERING).
CREATE TABLE IF NOT EXISTS cloudpulse.logs_by_server_level_day (
    server_id   uuid,
    level       text,
    bucket_date date,
    log_time    timestamp,
    seq         int,
    log_id      timeuuid,
    source      text,
    message     text,
    PRIMARY KEY ((server_id, level, bucket_date), log_time, seq, log_id)
) WITH CLUSTERING ORDER BY (log_time DESC, seq DESC, log_id DESC)
  AND default_time_to_live = 1209600
  AND gc_grace_seconds = 86400
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  };