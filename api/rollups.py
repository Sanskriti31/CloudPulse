"""Builds 1-minute and 1-hour rollups from the raw metrics table.

Each run recomputes a recent window (default: the last 15 minutes) instead of
only the newest minute. The agent can deliver buffered samples up to
10 minutes late, and rollup writes are idempotent (same primary key is simply
overwritten), so recomputing the window picks up late data safely.
"""

import time
from collections import defaultdict
from datetime import timedelta

from cassandra.query import BatchStatement, BatchType

from api.views import CHART_METRICS, as_utc, cloud_session

MAX_BATCH = 100          # keep each unlogged batch well under Cassandra's size limit
DEFAULT_LOOKBACK_MINUTES = 15

SELECT_SERVER_IDS = cloud_session.prepare(
    "SELECT server_id FROM servers_by_org WHERE org_id = ?"
)
SELECT_RAW_RANGE = cloud_session.prepare("""
    SELECT metric_time, metric_type, metric_value, unit
    FROM metrics_by_server_bucket
    WHERE server_id = ? AND bucket_date = ?
      AND metric_time >= ? AND metric_time < ?
""")
INSERT_1M = cloud_session.prepare("""
    INSERT INTO metrics_rollup_1m
    (server_id, metric_type, bucket_date, window_start,
     min_value, max_value, avg_value, sample_count, unit)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
SELECT_1M_RANGE = cloud_session.prepare("""
    SELECT window_start, min_value, max_value, avg_value, sample_count, unit
    FROM metrics_rollup_1m
    WHERE server_id = ? AND metric_type = ? AND bucket_date = ?
      AND window_start >= ? AND window_start < ?
""")
INSERT_1H = cloud_session.prepare("""
    INSERT INTO metrics_rollup_1h
    (server_id, metric_type, month_bucket, window_start,
     min_value, max_value, avg_value, sample_count, unit)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")


def floor_minute(moment):
    return moment.replace(second=0, microsecond=0)


def floor_hour(moment):
    return moment.replace(minute=0, second=0, microsecond=0)


def days_between(start, end):
    """Every UTC date touched by the window [start, end)."""
    day = start.date()
    last = (end - timedelta(microseconds=1)).date()
    while day <= last:
        yield day
        day += timedelta(days=1)


def write_grouped(rows_by_partition, statement):
    """One unlogged batch per partition (split into chunks), sent in parallel."""
    futures = []
    for rows in rows_by_partition.values():
        for i in range(0, len(rows), MAX_BATCH):
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for params in rows[i:i + MAX_BATCH]:
                batch.add(statement, params)
            futures.append(cloud_session.execute_async(batch))
    for future in futures:
        future.result()


def rollup_minutes(server_id, start, end):
    """Rebuild 1-minute rollups for [start, end). Returns rows written."""
    values = defaultdict(list)          # (metric_type, minute) -> [values]
    units = {}

    for day in days_between(start, end):
        rows = cloud_session.execute(SELECT_RAW_RANGE, (server_id, day, start, end))
        for row in rows:
            if row.metric_type not in CHART_METRICS:
                continue
            minute = floor_minute(as_utc(row.metric_time))
            values[(row.metric_type, minute)].append(row.metric_value)
            units[row.metric_type] = row.unit

    rows_by_partition = defaultdict(list)   # (metric_type, date) -> rows
    for (metric_type, minute), samples in values.items():
        rows_by_partition[(metric_type, minute.date())].append((
            server_id, metric_type, minute.date(), minute,
            min(samples), max(samples), sum(samples) / len(samples),
            len(samples), units.get(metric_type),
        ))

    write_grouped(rows_by_partition, INSERT_1M)
    return len(values)


def rollup_hours(server_id, start, end):
    """Rebuild 1-hour rollups for [start, end) from the 1-minute rollups."""
    rows_by_partition = defaultdict(list)   # (metric_type, month) -> rows
    written = 0

    for metric_type in CHART_METRICS:
        hours = {}   # hour -> [min, max, weighted_sum, count, unit]

        for day in days_between(start, end):
            rows = cloud_session.execute(
                SELECT_1M_RANGE, (server_id, metric_type, day, start, end)
            )
            for row in rows:
                hour = floor_hour(as_utc(row.window_start))
                agg = hours.get(hour)
                if agg is None:
                    hours[hour] = [
                        row.min_value, row.max_value,
                        row.avg_value * row.sample_count, row.sample_count,
                        row.unit,
                    ]
                else:
                    agg[0] = min(agg[0], row.min_value)
                    agg[1] = max(agg[1], row.max_value)
                    agg[2] += row.avg_value * row.sample_count
                    agg[3] += row.sample_count

        for hour, (low, high, total, count, unit) in hours.items():
            month = hour.strftime("%Y-%m")
            rows_by_partition[(metric_type, month)].append((
                server_id, metric_type, month, hour,
                low, high, total / count, count, unit,
            ))
            written += 1

    write_grouped(rows_by_partition, INSERT_1H)
    return written


def run_rollups(org_ids, now, minutes_back=DEFAULT_LOOKBACK_MINUTES):
    """Roll up every server in the given orgs. Returns a summary dict."""
    started = time.perf_counter()

    minute_end = floor_minute(now)                    # completed minutes only
    minute_start = minute_end - timedelta(minutes=minutes_back)
    hour_start = floor_hour(minute_start)
    hour_end = floor_hour(now) + timedelta(hours=1)   # include the current hour

    servers = minute_rows = hour_rows = 0
    for org_id in org_ids:
        for row in cloud_session.execute(SELECT_SERVER_IDS, (org_id,)):
            servers += 1
            minute_rows += rollup_minutes(row.server_id, minute_start, minute_end)
            hour_rows += rollup_hours(row.server_id, hour_start, hour_end)

    return {
        "servers": servers,
        "minute_rows": minute_rows,
        "hour_rows": hour_rows,
        "window": f"{minute_start:%H:%M}-{minute_end:%H:%M} UTC",
        "ms": round((time.perf_counter() - started) * 1000, 1),
    }