import json
import math
import operator
import time
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import (require_GET, require_POST, require_http_methods,)
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType
from cassandra.util import uuid_from_time

# The cluster runs in Docker. Only cass1 is reachable from Windows (port 9042),
# so the driver is told to use only that node as its coordinator. cass1 then
# forwards each read/write to the replicas that own the partition.
CASSANDRA_HOSTS = getattr(settings, "CASSANDRA_HOSTS", ["127.0.0.1"])
CASSANDRA_CONSISTENCY = getattr(settings, "CASSANDRA_CONSISTENCY", "LOCAL_QUORUM")

default_profile = ExecutionProfile(
    load_balancing_policy=WhiteListRoundRobinPolicy(CASSANDRA_HOSTS),
    consistency_level=ConsistencyLevel.name_to_value[CASSANDRA_CONSISTENCY],
    serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,   # for IF NOT EXISTS
    request_timeout=10,
)
cluster = Cluster(
    CASSANDRA_HOSTS,
    port=getattr(settings, "CASSANDRA_PORT", 9042),
    execution_profiles={EXEC_PROFILE_DEFAULT: default_profile},
    protocol_version=5,
)
cloud_session = cluster.connect(getattr(settings, "CASSANDRA_KEYSPACE", "cloudpulse"))
print(f"CloudPulse: connected to Cassandra {CASSANDRA_HOSTS}, "
      f"consistency {CASSANDRA_CONSISTENCY}")

ALLOWED_METRICS = {
    "cpu_percent",
    "memory_percent",
    "disk_percent",
    "network_sent_bytes",      # old cumulative counters (kept for old data)
    "network_recv_bytes",
    "network_sent_bps",        # new: bytes per second
    "network_recv_bps",
}


# ======================================================================
# ALERT ENGINE
# ======================================================================

ALERT_TTL_SECONDS = 7776000          # 90 days, same as the table default
RULE_CACHE_SECONDS = 30              # re-read rules at most every 30 s
OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}
SEVERITIES = {"WARNING", "CRITICAL"}

_rule_cache = {}                     # org_id -> (loaded_at, [rules])


def as_utc(value):
    """The Cassandra driver returns naive UTC datetimes; make them aware."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def iso(value):
    value = as_utc(value)
    return value.isoformat() if value else None


def month_bucket(moment):
    return as_utc(moment).strftime("%Y-%m")


def recent_month_buckets(count):
    """Current month first, e.g. ['2026-10', '2026-09', ...]."""
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    buckets = []
    for _ in range(count):
        buckets.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month, year = 12, year - 1
    return buckets


def remaining_ttl(opened_at, now):
    """TTL for the resolve UPDATE so the whole row expires together."""
    age = int((as_utc(now) - as_utc(opened_at)).total_seconds())
    return max(ALERT_TTL_SECONDS - age, 60)


# ---- Prepared statements (parsed once by Cassandra, reused every time) ----

SELECT_RULES = cloud_session.prepare(
    "SELECT * FROM alert_rules_by_org WHERE org_id = ?"
)
INSERT_RULE = cloud_session.prepare("""
    INSERT INTO alert_rules_by_org
    (org_id, rule_id, rule_name, metric_type, operator, threshold,
     duration_seconds, severity, server_id, enabled, created_by,
     created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

SELECT_STATES = cloud_session.prepare(
    "SELECT * FROM alert_state_by_server WHERE server_id = ?"
)
SELECT_STATE = cloud_session.prepare(
    "SELECT * FROM alert_state_by_server WHERE server_id = ? AND rule_id = ?"
)
INSERT_PENDING_STATE = cloud_session.prepare("""
    INSERT INTO alert_state_by_server
    (server_id, rule_id, state, org_id, metric_type, severity,
     threshold, pending_since, last_value, last_evaluated)
    VALUES (?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?)
""")
INSERT_OPEN_STATE = cloud_session.prepare("""
    INSERT INTO alert_state_by_server
    (server_id, rule_id, state, event_id, org_id, metric_type, severity,
     threshold, pending_since, opened_at, last_value, last_evaluated)
    VALUES (?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
UPDATE_STATE_SEEN = cloud_session.prepare("""
    UPDATE alert_state_by_server
    SET last_value = ?, last_evaluated = ?
    WHERE server_id = ? AND rule_id = ?
""")
DELETE_STATE = cloud_session.prepare(
    "DELETE FROM alert_state_by_server WHERE server_id = ? AND rule_id = ?"
)

INSERT_OPEN_ORG = cloud_session.prepare("""
    INSERT INTO open_alerts_by_org
    (org_id, server_id, rule_id, event_id, hostname, rule_name,
     metric_type, severity, threshold, triggered_value, opened_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
DELETE_OPEN_ORG = cloud_session.prepare("""
    DELETE FROM open_alerts_by_org
    WHERE org_id = ? AND server_id = ? AND rule_id = ?
""")
SELECT_OPEN_ORG = cloud_session.prepare(
    "SELECT * FROM open_alerts_by_org WHERE org_id = ?"
)

INSERT_EVENT_BY_SERVER = cloud_session.prepare("""
    INSERT INTO alert_events_by_server
    (server_id, month_bucket, event_id, rule_id, org_id, rule_name,
     metric_type, severity, threshold, triggered_value, status, opened_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
""")
INSERT_EVENT_BY_RULE = cloud_session.prepare("""
    INSERT INTO alert_events_by_rule
    (rule_id, month_bucket, event_id, server_id, org_id, hostname,
     metric_type, severity, threshold, triggered_value, status, opened_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)
""")
RESOLVE_EVENT_BY_SERVER = cloud_session.prepare("""
    UPDATE alert_events_by_server USING TTL ?
    SET status = 'RESOLVED', resolved_at = ?, resolved_by = ?,
        resolved_value = ?
    WHERE server_id = ? AND month_bucket = ? AND event_id = ?
""")
RESOLVE_EVENT_BY_RULE = cloud_session.prepare("""
    UPDATE alert_events_by_rule USING TTL ?
    SET status = 'RESOLVED', resolved_at = ?, resolved_by = ?,
        resolved_value = ?
    WHERE rule_id = ? AND month_bucket = ? AND event_id = ?
""")
SELECT_EVENTS_BY_SERVER = cloud_session.prepare("""
    SELECT * FROM alert_events_by_server
    WHERE server_id = ? AND month_bucket = ? LIMIT ?
""")
SELECT_EVENTS_BY_RULE = cloud_session.prepare("""
    SELECT * FROM alert_events_by_rule
    WHERE rule_id = ? AND month_bucket = ? LIMIT ?
""")


def get_rules(org_id):
    """Rules for an org, cached for RULE_CACHE_SECONDS."""
    cached = _rule_cache.get(org_id)
    if cached and time.monotonic() - cached[0] < RULE_CACHE_SECONDS:
        return cached[1]
    rules = list(cloud_session.execute(SELECT_RULES, (org_id,)))
    _rule_cache[org_id] = (time.monotonic(), rules)
    return rules


def open_alert(rule, server_id, org_id, hostname, value, now, pending_since):
    """Open an alert: state + open list + both history tables, atomically."""
    event_id = uuid_from_time(now)
    bucket = month_bucket(now)

    batch = BatchStatement(batch_type=BatchType.LOGGED)
    batch.add(INSERT_OPEN_STATE, (
        server_id, rule.rule_id, event_id, org_id, rule.metric_type,
        rule.severity, rule.threshold, pending_since, now, value, now,
    ))
    batch.add(INSERT_OPEN_ORG, (
        org_id, server_id, rule.rule_id, event_id, hostname,
        rule.rule_name, rule.metric_type, rule.severity, rule.threshold,
        value, now,
    ))
    batch.add(INSERT_EVENT_BY_SERVER, (
        server_id, bucket, event_id, rule.rule_id, org_id, rule.rule_name,
        rule.metric_type, rule.severity, rule.threshold, value, now,
    ))
    batch.add(INSERT_EVENT_BY_RULE, (
        rule.rule_id, bucket, event_id, server_id, org_id, hostname,
        rule.metric_type, rule.severity, rule.threshold, value, now,
    ))
    cloud_session.execute(batch)
    return event_id


def resolve_alert(state, value, now, resolved_by):
    """Resolve an open alert in all four tables, atomically."""
    bucket = month_bucket(state.opened_at)     # partition it was opened in
    ttl = remaining_ttl(state.opened_at, now)

    batch = BatchStatement(batch_type=BatchType.LOGGED)
    batch.add(DELETE_STATE, (state.server_id, state.rule_id))
    batch.add(DELETE_OPEN_ORG, (state.org_id, state.server_id, state.rule_id))
    batch.add(RESOLVE_EVENT_BY_SERVER, (
        ttl, now, resolved_by, value,
        state.server_id, bucket, state.event_id,
    ))
    batch.add(RESOLVE_EVENT_BY_RULE, (
        ttl, now, resolved_by, value,
        state.rule_id, bucket, state.event_id,
    ))
    cloud_session.execute(batch)


def close_state(state, value, now, reason):
    """Resolve an OPEN alert, or just drop a PENDING state row."""
    if state.state == "OPEN":
        resolve_alert(state, value, now, reason)
        return 1
    cloud_session.execute(DELETE_STATE, (state.server_id, state.rule_id))
    return 0


def evaluate_alerts(server_id, org_id, hostname, metrics, now):
    """Run every rule against one ingested batch of metrics."""
    rules = get_rules(org_id)
    values = {metric_type: value for metric_type, value, _ in metrics}
    states = {
        row.rule_id: row
        for row in cloud_session.execute(SELECT_STATES, (server_id,))
    }
    opened = resolved = 0

    for rule in rules:
        if rule.server_id is not None and rule.server_id != server_id:
            continue

        state = states.get(rule.rule_id)

        # Rule switched off: close anything it had open on this server.
        if not rule.enabled:
            if state is not None:
                resolved += close_state(
                    state, values.get(rule.metric_type), now, "rule-disabled"
                )
            continue

        compare = OPERATORS.get(rule.operator)
        if (rule.metric_type not in values or compare is None
                or rule.threshold is None):
            continue

        value = values[rule.metric_type]
        breached = compare(value, rule.threshold)
        duration = rule.duration_seconds or 0

        if breached:
            if state is None:
                if duration <= 0:
                    open_alert(rule, server_id, org_id, hostname,
                               value, now, now)
                    opened += 1
                else:
                    cloud_session.execute(INSERT_PENDING_STATE, (
                        server_id, rule.rule_id, org_id, rule.metric_type,
                        rule.severity, rule.threshold, now, value, now,
                    ))
            elif state.state == "PENDING":
                pending_since = as_utc(state.pending_since) or now
                if (now - pending_since).total_seconds() >= duration:
                    open_alert(rule, server_id, org_id, hostname,
                               value, now, pending_since)
                    opened += 1
                else:
                    cloud_session.execute(
                        UPDATE_STATE_SEEN,
                        (value, now, server_id, rule.rule_id),
                    )
            else:  # already OPEN: just record the latest value
                cloud_session.execute(
                    UPDATE_STATE_SEEN,
                    (value, now, server_id, rule.rule_id),
                )

        elif state is not None:
            # Back to normal: resolve if open, forget if only pending.
            resolved += close_state(state, value, now, "auto")

    # Rule deleted: close any alert state left behind for it.
    known_rules = {rule.rule_id for rule in rules}
    for rule_id, state in states.items():
        if rule_id not in known_rules:
            resolved += close_state(
                state, values.get(state.metric_type), now, "rule-deleted"
            )

    return {"opened": opened, "resolved": resolved}


# ======================================================================
# METRICS: WRITE AND READ PATH
# ======================================================================

# Health is worked out from last_seen when it is read, not stored.
UP_SECONDS = 30            # 3 missed reports at a 10 s interval
DOWN_SECONDS = 120         # between the two the server is STALE
MAX_SAMPLE_AGE = timedelta(minutes=10)   # oldest buffered sample accepted
MAX_CLOCK_AHEAD = timedelta(minutes=1)   # agent clock may run this fast
FRESH_FOR_ALERTS = timedelta(seconds=60) # older samples skip the alert engine

_known_servers = set()     # servers already registered by this process

INSERT_SERVER_BY_ID_IF_NEW = cloud_session.prepare("""
    INSERT INTO server_by_id
    (server_id, org_id, hostname, os_name, registered_at,
     health_status, last_seen)
    VALUES (?, ?, ?, ?, ?, 'UP', ?)
    IF NOT EXISTS
""")
INSERT_SERVER_BY_ORG_IF_NEW = cloud_session.prepare("""
    INSERT INTO servers_by_org
    (org_id, server_id, hostname, os_name, registered_at,
     health_status, last_seen)
    VALUES (?, ?, ?, ?, ?, 'UP', ?)
    IF NOT EXISTS
""")
UPDATE_SERVER_BY_ID = cloud_session.prepare("""
    UPDATE server_by_id USING TIMESTAMP ?
    SET org_id = ?, hostname = ?, os_name = ?,
        health_status = 'UP', last_seen = ?
    WHERE server_id = ?
""")
UPDATE_SERVER_BY_ORG = cloud_session.prepare("""
    UPDATE servers_by_org USING TIMESTAMP ?
    SET hostname = ?, os_name = ?, health_status = 'UP', last_seen = ?
    WHERE org_id = ? AND server_id = ?
""")
INSERT_METRIC = cloud_session.prepare("""
    INSERT INTO metrics_by_server_bucket
    (server_id, bucket_date, metric_time, metric_type, metric_value, unit)
    VALUES (?, ?, ?, ?, ?, ?)
""")
INSERT_LATEST = cloud_session.prepare("""
    INSERT INTO latest_metric_by_server
    (server_id, metric_type, metric_value, unit, recorded_at)
    VALUES (?, ?, ?, ?, ?)
    USING TIMESTAMP ?
""")
SELECT_SERVERS_BY_ORG = cloud_session.prepare("""
    SELECT server_id, hostname, os_name, last_seen, registered_at
    FROM servers_by_org WHERE org_id = ?
""")
SELECT_LATEST = cloud_session.prepare("""
    SELECT metric_type, metric_value, recorded_at, unit
    FROM latest_metric_by_server WHERE server_id = ?
""")
SELECT_HISTORY_DAY = cloud_session.prepare("""
    SELECT metric_time, metric_type, metric_value, unit
    FROM metrics_by_server_bucket
    WHERE server_id = ? AND bucket_date = ?
""")

# ---- History resolutions (Phase 3) ----

# Metrics that are charted and rolled up.
CHART_METRICS = (
    "cpu_percent",
    "memory_percent",
    "disk_percent",
    "network_sent_bps",
    "network_recv_bps",
)

# Longest range each resolution may be asked for.
RESOLUTION_MAX_DAYS = {"raw": 7, "1m": 30, "1h": 365}

SELECT_ROLLUP_1M_DAY = cloud_session.prepare("""
    SELECT metric_type, window_start, min_value, max_value,
           avg_value, sample_count, unit
    FROM metrics_rollup_1m
    WHERE server_id = ? AND metric_type = ? AND bucket_date = ?
""")
SELECT_ROLLUP_1H_SINCE = cloud_session.prepare("""
    SELECT metric_type, window_start, min_value, max_value,
           avg_value, sample_count, unit
    FROM metrics_rollup_1h
    WHERE server_id = ? AND metric_type = ? AND month_bucket = ?
      AND window_start >= ?
""")


def pick_resolution(days):
    """Coarser data for longer ranges keeps every chart to a few hundred points."""
    if days <= 1:
        return "raw"
    if days <= 7:
        return "1m"
    return "1h"


def months_between(start_date, end_date):
    """'YYYY-MM' for every month from start_date to end_date inclusive."""
    year, month = start_date.year, start_date.month
    months = []
    while (year, month) <= (end_date.year, end_date.month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            month, year = 1, year + 1
    return months


def write_timestamp(moment):
    """Cassandra write time in microseconds. Newer samples always win."""
    return int(moment.timestamp() * 1_000_000)


def parse_collected_at(value, server_now):
    """Pick the time a sample is stored under.

    - Agent time, if it is sane (the normal case).
    - Server time, if the agent sent none, an unreadable one, or one that is
      in the future (agent clock running fast).
    - Rejected (ValueError) if older than MAX_SAMPLE_AGE: storing it under
      "now" would put old readings at the wrong place in the history.
    """
    if not value:
        return server_now, "server"
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return server_now, "server"
    if moment.tzinfo is None:
        return server_now, "server"

    moment = moment.astimezone(timezone.utc)
    if moment < server_now - MAX_SAMPLE_AGE:
        raise ValueError("Sample is older than 10 minutes")
    if moment > server_now + MAX_CLOCK_AHEAD:
        return server_now, "server"

    # Cassandra timestamps have millisecond precision.
    return moment.replace(microsecond=(moment.microsecond // 1000) * 1000), "agent"


def health_from_last_seen(last_seen, now):
    if last_seen is None:
        return "UNKNOWN", None
    age = (now - as_utc(last_seen)).total_seconds()
    if age <= UP_SECONDS:
        return "UP", age
    if age <= DOWN_SECONDS:
        return "STALE", age
    return "DOWN", age


def register_server_once(server_id, org_id, hostname, os_name, now):
    """LWT only the first time this process sees a server."""
    if server_id in _known_servers:
        return
    cloud_session.execute(
        INSERT_SERVER_BY_ID_IF_NEW,
        (server_id, org_id, hostname, os_name, now, now),
    )
    cloud_session.execute(
        INSERT_SERVER_BY_ORG_IF_NEW,
        (org_id, server_id, hostname, os_name, now, now),
    )
    _known_servers.add(server_id)


@csrf_exempt
@require_POST
def ingest_metrics(request):
    """Receive one batch of metrics from an agent and store it."""

    try:
        payload = json.loads(request.body)

        server_id = UUID(payload["server_id"])
        org_id = str(payload.get("org_id", "local"))
        hostname = str(payload["hostname"])
        os_name = str(payload["os_name"])
        metrics = payload["metrics"]

        if not org_id or len(org_id) > 100:
            raise ValueError("Invalid organization ID")
        if not hostname.strip() or not os_name.strip():
            raise ValueError("Hostname and OS are required")
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= 20:
            raise ValueError("Provide between 1 and 20 metrics")

        validated_metrics = []
        for item in metrics:
            metric_type = item["metric_type"]
            metric_value = float(item["metric_value"])
            unit = str(item["unit"])

            if metric_type not in ALLOWED_METRICS:
                raise ValueError("Unsupported metric type")
            if not math.isfinite(metric_value):
                raise ValueError("Metric value must be finite")

            validated_metrics.append((metric_type, metric_value, unit))

    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return JsonResponse(
            {"status": "error", "message": "Invalid metric payload"},
            status=400,
        )

    server_now = datetime.now(timezone.utc)
    try:
        recorded_at, time_source = parse_collected_at(
            payload.get("collected_at"), server_now
        )
    except ValueError as error:
        return JsonResponse(
            {"status": "error", "message": str(error)},
            status=400,
        )
    bucket_date = recorded_at.date()
    write_ts = write_timestamp(recorded_at)

    try:
        register_server_once(server_id, org_id, hostname, os_name, server_now)

        # 1) Heartbeat: both copies of the server row, atomically.
        #    last_seen is when the SERVER heard from the agent, so it uses the
        #    server clock (an agent with a slow clock still shows as UP).
        heartbeat_ts = write_timestamp(server_now)
        server_batch = BatchStatement(batch_type=BatchType.LOGGED)
        server_batch.add(UPDATE_SERVER_BY_ID, (
            heartbeat_ts, org_id, hostname, os_name, server_now, server_id,
        ))
        server_batch.add(UPDATE_SERVER_BY_ORG, (
            heartbeat_ts, hostname, os_name, server_now, org_id, server_id,
        ))

        # 2) History: every row is in ONE partition (server_id, bucket_date),
        #    so an unlogged batch is a single write to a single replica set.
        history_batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        # 3) Latest values: also one partition (server_id). USING TIMESTAMP
        #    means an older, retried sample can never overwrite a newer one.
        latest_batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        for metric_type, metric_value, unit in validated_metrics:
            history_batch.add(INSERT_METRIC, (
                server_id, bucket_date, recorded_at,
                metric_type, metric_value, unit,
            ))
            latest_batch.add(INSERT_LATEST, (
                server_id, metric_type, metric_value, unit,
                recorded_at, write_ts,
            ))

        # The three batches touch different partitions: send them together.
        futures = [
            cloud_session.execute_async(server_batch),
            cloud_session.execute_async(history_batch),
            cloud_session.execute_async(latest_batch),
        ]
        for future in futures:
            future.result()

    except Exception as exc:
        print(f"CloudPulse ingestion error: {exc}")
        return JsonResponse(
            {"status": "error", "message": "Database write failed."},
            status=500,
        )

    # Only fresh samples go through the alert engine; replayed old samples
    # would otherwise open or resolve alerts at the wrong time.
    alert_summary = {"opened": 0, "resolved": 0}
    if server_now - recorded_at <= FRESH_FOR_ALERTS:
        try:
            alert_summary = evaluate_alerts(
                server_id, org_id, hostname, validated_metrics, recorded_at
            )
        except Exception as exc:
            print(f"CloudPulse alert engine error: {exc}")

    return JsonResponse({
        "status": "success",
        "server_id": str(server_id),
        "stored_metrics": len(validated_metrics),
        "recorded_at": recorded_at.isoformat(),
        "time_source": time_source,
        "alerts": alert_summary,
    })


@require_GET
def list_servers(request):
    org_id = request.GET.get("org_id", "local")

    try:
        now = datetime.now(timezone.utc)
        servers = []

        for row in cloud_session.execute(SELECT_SERVERS_BY_ORG, (org_id,)):
            status, age = health_from_last_seen(row.last_seen, now)
            servers.append({
                "server_id": str(row.server_id),
                "hostname": row.hostname,
                "os_name": row.os_name,
                "health_status": status,
                "seconds_since_seen": round(age) if age is not None else None,
                "last_seen": iso(row.last_seen),
                "registered_at": iso(row.registered_at),
            })

        return JsonResponse({
            "status": "success",
            "count": len(servers),
            "servers": servers,
        })

    except Exception as error:
        print(f"CloudPulse list servers error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load servers."},
            status=500,
        )


@require_GET
def latest_metrics(request):
    server_id_text = request.GET.get("server_id")

    if not server_id_text:
        return JsonResponse({
            "status": "error",
            "message": "Please provide a server_id query parameter.",
        }, status=400)

    try:
        server_id = UUID(server_id_text)
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Invalid server_id. Please provide a valid UUID.",
        }, status=400)

    try:
        metrics = [{
            "metric_type": row.metric_type,
            "metric_value": row.metric_value,
            "recorded_at": iso(row.recorded_at),
            "unit": row.unit,
        } for row in cloud_session.execute(SELECT_LATEST, (server_id,))]

        return JsonResponse({
            "status": "success",
            "server_id": str(server_id),
            "count": len(metrics),
            "metrics": metrics,
        })

    except Exception as error:
        print(f"CloudPulse latest metrics error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load latest metrics."},
            status=500,
        )


@require_GET
def historical_metrics(request):
    """History at raw, 1-minute or 1-hour resolution.

    ?days=1..365  &resolution=auto|raw|1m|1h  (auto picks by range)
    """
    server_id_text = request.GET.get("server_id")

    if not server_id_text:
        return JsonResponse({
            "status": "error",
            "message": "Please provide a server_id parameter.",
        }, status=400)

    try:
        server_id = UUID(server_id_text)
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Invalid server_id UUID.",
        }, status=400)

    try:
        days = int(request.GET.get("days", "1"))
    except ValueError:
        days = 0

    resolution = request.GET.get("resolution", "auto")
    if resolution == "auto":
        resolution = pick_resolution(days)

    if resolution not in RESOLUTION_MAX_DAYS:
        return JsonResponse({
            "status": "error",
            "message": "resolution must be auto, raw, 1m or 1h.",
        }, status=400)

    max_days = RESOLUTION_MAX_DAYS[resolution]
    if not 1 <= days <= max_days:
        return JsonResponse({
            "status": "error",
            "message": f"days must be between 1 and {max_days} for {resolution}.",
        }, status=400)

    try:
        started = time.perf_counter()
        now = datetime.now(timezone.utc)
        today = now.date()
        dates = [today - timedelta(days=offset) for offset in range(days)]

        # Every query below names a full partition key; all are sent at once.
        if resolution == "raw":
            futures = [
                cloud_session.execute_async(SELECT_HISTORY_DAY, (server_id, day))
                for day in dates
            ]
        elif resolution == "1m":
            futures = [
                cloud_session.execute_async(
                    SELECT_ROLLUP_1M_DAY, (server_id, metric_type, day)
                )
                for metric_type in CHART_METRICS
                for day in dates
            ]
        else:
            since = now - timedelta(days=days)
            futures = [
                cloud_session.execute_async(
                    SELECT_ROLLUP_1H_SINCE, (server_id, metric_type, month, since)
                )
                for metric_type in CHART_METRICS
                for month in months_between(since.date(), today)
            ]

        collected = []
        for future in futures:
            for row in future.result():
                if resolution == "raw":
                    collected.append((row.metric_time, {
                        "recorded_at": iso(row.metric_time),
                        "metric_type": row.metric_type,
                        "metric_value": row.metric_value,
                        "unit": row.unit,
                    }))
                else:
                    collected.append((row.window_start, {
                        "recorded_at": iso(row.window_start),
                        "metric_type": row.metric_type,
                        "metric_value": round(row.avg_value, 2),
                        "min_value": row.min_value,
                        "max_value": row.max_value,
                        "sample_count": row.sample_count,
                        "unit": row.unit,
                    }))

        collected.sort(key=lambda item: item[0])     # oldest first
        history = [item[1] for item in collected]

        return JsonResponse({
            "status": "success",
            "server_id": str(server_id),
            "days_requested": days,
            "resolution": resolution,
            "partitions_read": len(futures),
            "query_ms": round((time.perf_counter() - started) * 1000, 1),
            "count": len(history),
            "metrics": history,
        })

    except Exception as error:
        print(f"CloudPulse history error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load history."},
            status=500,
        )


# ======================================================================
# LOGS (Phase 6)
# ======================================================================

import base64
import hashlib
from collections import defaultdict

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
LEVEL_ALIASES = {"WARN": "WARNING", "CRITICAL": "ERROR", "FATAL": "ERROR"}
MAX_LOG_LINES = 200          # lines per request
MAX_MESSAGE_CHARS = 2000     # longer messages are cut
MAX_BATCH_ROWS = 50
MAX_BATCH_BYTES = 40_000     # Cassandra rejects batches over 50 KB by default

INSERT_LOG = cloud_session.prepare("""
    INSERT INTO logs_by_server_day
    (server_id, bucket_date, log_time, seq, log_id, level, source, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
INSERT_LOG_BY_LEVEL = cloud_session.prepare("""
    INSERT INTO logs_by_server_level_day
    (server_id, level, bucket_date, log_time, seq, log_id, source, message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
SELECT_LOGS_DAY = cloud_session.prepare("""
    SELECT log_time, seq, log_id, level, source, message
    FROM logs_by_server_day
    WHERE server_id = ? AND bucket_date = ?
""")
SELECT_LOGS_LEVEL_DAY = cloud_session.prepare("""
    SELECT log_time, seq, log_id, level, source, message
    FROM logs_by_server_level_day
    WHERE server_id = ? AND level = ? AND bucket_date = ?
""")


def log_uuid(server_id, moment, seq, level, source, message):
    """A timeuuid that is the SAME every time the same line is sent.

    The time part is the log time (so it sorts correctly); the other bits come
    from a hash of the line. A retried request therefore overwrites the same
    rows instead of creating duplicates.
    """
    digest = hashlib.sha1(
        f"{server_id}|{moment.isoformat()}|{seq}|{level}|{source}|{message}".encode()
    ).digest()
    node = int.from_bytes(digest[:6], "big")
    clock_seq = int.from_bytes(digest[6:8], "big") & 0x3FFF
    return uuid_from_time(moment, node=node, clock_seq=clock_seq)


def write_log_batches(statement, groups):
    """One unlogged batch per partition, split by row count and size."""
    futures = []
    for rows in groups:
        batch, size = None, 0
        for row in rows:
            row_size = len(row[-1]) + 120          # message + fixed columns
            if batch is None or len(batch) >= MAX_BATCH_ROWS \
                    or size + row_size > MAX_BATCH_BYTES:
                if batch is not None:
                    futures.append(cloud_session.execute_async(batch))
                batch, size = BatchStatement(batch_type=BatchType.UNLOGGED), 0
            batch.add(statement, row)
            size += row_size
        if batch is not None:
            futures.append(cloud_session.execute_async(batch))
    for future in futures:
        future.result()


@csrf_exempt
@require_POST
def ingest_logs(request):
    """Receive log lines from an agent.

    {"server_id": "...", "logs": [{"ts": "...", "level": "ERROR",
      "source": "app.log", "message": "...", "seq": 17}, ...]}
    """
    try:
        payload = json.loads(request.body)
        server_id = UUID(payload["server_id"])
        entries = payload["logs"]
        if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_LOG_LINES:
            raise ValueError("Provide between 1 and 200 log lines")
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return JsonResponse(
            {"status": "error", "message": "Invalid log payload"},
            status=400,
        )

    server_now = datetime.now(timezone.utc)
    by_day = defaultdict(list)       # date -> rows for logs_by_server_day
    by_level = defaultdict(list)     # (level, date) -> rows for the level table
    accepted = rejected = 0

    for index, entry in enumerate(entries):
        try:
            level = str(entry.get("level", "INFO")).upper()
            level = LEVEL_ALIASES.get(level, level)
            if level not in LOG_LEVELS:
                level = "INFO"
            message = str(entry["message"]).strip()[:MAX_MESSAGE_CHARS]
            if not message:
                raise ValueError("Empty message")
            source = str(entry.get("source", "agent"))[:100]
            seq = int(entry.get("seq", index))
            # Same time rules as metrics: agent time if sane, rejected if
            # older than 10 minutes.
            moment, _ = parse_collected_at(entry.get("ts"), server_now)
        except (ValueError, TypeError, KeyError, AttributeError):
            rejected += 1
            continue

        log_id = log_uuid(server_id, moment, seq, level, source, message)
        day = moment.date()
        by_day[day].append(
            (server_id, day, moment, seq, log_id, level, source, message)
        )
        by_level[(level, day)].append(
            (server_id, level, day, moment, seq, log_id, source, message)
        )
        accepted += 1

    if accepted:
        try:
            write_log_batches(INSERT_LOG, by_day.values())
            write_log_batches(INSERT_LOG_BY_LEVEL, by_level.values())
        except Exception as exc:
            print(f"CloudPulse log ingestion error: {exc}")
            return JsonResponse(
                {"status": "error", "message": "Database write failed."},
                status=500,
            )

    return JsonResponse({
        "status": "success",
        "server_id": str(server_id),
        "accepted": accepted,
        "rejected": rejected,
    })


@require_GET
def list_logs(request):
    """Logs for one server and day, newest first, one page at a time.

    ?server_id=...  &date=YYYY-MM-DD (default today, UTC)
    &level=ERROR|WARNING|INFO|DEBUG  &limit=1..500 (default 100)
    &q=text (filters the page)  &page=<next_page from the previous response>
    """
    try:
        server_id = UUID(request.GET.get("server_id", ""))
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Provide a valid server_id.",
        }, status=400)

    level = request.GET.get("level", "").upper() or None
    if level is not None and level not in LOG_LEVELS:
        return JsonResponse({
            "status": "error",
            "message": "level must be DEBUG, INFO, WARNING or ERROR.",
        }, status=400)

    try:
        date_text = request.GET.get("date")
        day = (datetime.strptime(date_text, "%Y-%m-%d").date() if date_text
               else datetime.now(timezone.utc).date())
        limit = int(request.GET.get("limit", "100"))
        if not 1 <= limit <= 500:
            raise ValueError
        page = request.GET.get("page")
        paging_state = (base64.b64decode(page.encode(), altchars=b"-_", validate=True)
                        if page else None)
    except (ValueError, TypeError):
        return JsonResponse({
            "status": "error",
            "message": "Invalid date (YYYY-MM-DD), limit (1-500) or page.",
        }, status=400)

    search = request.GET.get("q", "").strip().lower()[:100]

    try:
        if level:
            bound = SELECT_LOGS_LEVEL_DAY.bind((server_id, level, day))
        else:
            bound = SELECT_LOGS_DAY.bind((server_id, day))
        bound.fetch_size = limit     # Cassandra returns at most `limit` rows

        # Native Cassandra paging: the driver hands back an opaque token
        # pointing just after the last row; the next request resumes there.
        result = cloud_session.execute(bound, paging_state=paging_state)
        rows = result.current_rows
        next_token = result.paging_state

        logs = [{
            "log_time": iso(row.log_time),
            "log_id": str(row.log_id),
            "level": row.level,
            "source": row.source,
            "message": row.message,
        } for row in rows if not search or search in row.message.lower()]

        return JsonResponse({
            "status": "success",
            "server_id": str(server_id),
            "date": day.isoformat(),
            "level": level,
            "rows_scanned": len(rows),
            "count": len(logs),
            "logs": logs,
            "next_page": (base64.urlsafe_b64encode(next_token).decode()
                          if next_token else None),
        })

    except Exception as error:
        print(f"CloudPulse list logs error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load logs."},
            status=500,
        )

# ======================================================================
# ALERT ENDPOINTS
# ======================================================================

def rule_to_dict(row):
    return {
        "rule_id": str(row.rule_id),
        "org_id": row.org_id,
        "rule_name": row.rule_name,
        "metric_type": row.metric_type,
        "operator": row.operator,
        "threshold": row.threshold,
        "duration_seconds": row.duration_seconds,
        "severity": row.severity,
        "server_id": str(row.server_id) if row.server_id else None,
        "enabled": row.enabled,
        "created_by": row.created_by,
        "created_at": iso(row.created_at),
        "updated_at": iso(row.updated_at),
    }


def event_to_dict(row):
    return {
        "event_id": str(row.event_id),
        "server_id": str(row.server_id),
        "rule_id": str(row.rule_id),
        "org_id": row.org_id,
        "metric_type": row.metric_type,
        "severity": row.severity,
        "threshold": row.threshold,
        "triggered_value": row.triggered_value,
        "status": row.status,
        "opened_at": iso(row.opened_at),
        "resolved_at": iso(row.resolved_at),
        "resolved_by": row.resolved_by,
        "resolved_value": row.resolved_value,
    }


@csrf_exempt
@require_http_methods(["GET", "POST"])
def alert_rules(request):
    """GET: list an org's rules.  POST: create a rule."""

    if request.method == "GET":
        org_id = request.GET.get("org_id", "local")
        try:
            rows = cloud_session.execute(SELECT_RULES, (org_id,))
            rules = sorted(
                (rule_to_dict(row) for row in rows),
                key=lambda r: r["rule_name"] or "",
            )
            return JsonResponse({
                "status": "success",
                "count": len(rules),
                "rules": rules,
            })
        except Exception as error:
            print(f"CloudPulse rules error: {error}")
            return JsonResponse(
                {"status": "error", "message": "Could not load rules."},
                status=500,
            )

    # POST: create a rule
    try:
        payload = json.loads(request.body)

        org_id = str(payload.get("org_id", "local")).strip()
        rule_name = str(payload["rule_name"]).strip()
        metric_type = payload["metric_type"]
        rule_operator = payload["operator"]
        threshold = float(payload["threshold"])
        duration_seconds = int(payload.get("duration_seconds", 0))
        severity = str(payload.get("severity", "WARNING")).upper()
        enabled = bool(payload.get("enabled", True))
        created_by = str(payload.get("created_by", "admin"))
        server_id = (
            UUID(payload["server_id"]) if payload.get("server_id") else None
        )

        if not org_id or len(org_id) > 100:
            raise ValueError("Invalid organization ID")
        if not rule_name or len(rule_name) > 100:
            raise ValueError("Rule name is required (max 100 characters)")
        if metric_type not in ALLOWED_METRICS:
            raise ValueError("Unsupported metric type")
        if rule_operator not in OPERATORS:
            raise ValueError("Operator must be one of >, >=, <, <=")
        if not math.isfinite(threshold):
            raise ValueError("Threshold must be a finite number")
        if not 0 <= duration_seconds <= 3600:
            raise ValueError("duration_seconds must be between 0 and 3600")
        if severity not in SEVERITIES:
            raise ValueError("Severity must be WARNING or CRITICAL")

    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as error:
        return JsonResponse(
            {"status": "error", "message": f"Invalid rule: {error}"},
            status=400,
        )

    # Cassandra has no UNIQUE constraint, so the API enforces
    # one rule name per organization.
    try:
        existing = cloud_session.execute(SELECT_RULES, (org_id,))
        if any((row.rule_name or "").strip().lower() == rule_name.lower()
               for row in existing):
            return JsonResponse({
                "status": "error",
                "message": f"A rule named '{rule_name}' already exists.",
            }, status=409)
    except Exception as error:
        print(f"CloudPulse rule check error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not check existing rules."},
            status=500,
        )

    now = datetime.now(timezone.utc)
    rule_id = uuid4()

    try:
        cloud_session.execute(INSERT_RULE, (
            org_id, rule_id, rule_name, metric_type, rule_operator,
            threshold, duration_seconds, severity, server_id, enabled,
            created_by, now, now,
        ))
    except Exception as error:
        print(f"CloudPulse rule insert error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not save rule."},
            status=500,
        )

    _rule_cache.pop(org_id, None)    # engine picks up the new rule now

    return JsonResponse(
        {"status": "success", "rule_id": str(rule_id)},
        status=201,
    )


@require_GET
def open_alerts(request):
    """All currently open alerts in an org, critical first, newest first."""
    org_id = request.GET.get("org_id", "local")

    try:
        rows = cloud_session.execute(SELECT_OPEN_ORG, (org_id,))
        alerts = [{
            "event_id": str(row.event_id),
            "server_id": str(row.server_id),
            "rule_id": str(row.rule_id),
            "hostname": row.hostname,
            "rule_name": row.rule_name,
            "metric_type": row.metric_type,
            "severity": row.severity,
            "threshold": row.threshold,
            "triggered_value": row.triggered_value,
            "opened_at": iso(row.opened_at),
        } for row in rows]

        alerts.sort(key=lambda a: a["opened_at"] or "", reverse=True)
        alerts.sort(key=lambda a: a["severity"] != "CRITICAL")

        return JsonResponse({
            "status": "success",
            "count": len(alerts),
            "alerts": alerts,
        })
    except Exception as error:
        print(f"CloudPulse open alerts error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load open alerts."},
            status=500,
        )


@require_GET
def alert_history(request):
    """History for one server (?server_id=) or one rule (?rule_id=)."""
    server_id_text = request.GET.get("server_id")
    rule_id_text = request.GET.get("rule_id")

    if bool(server_id_text) == bool(rule_id_text):
        return JsonResponse({
            "status": "error",
            "message": "Provide exactly one of server_id or rule_id.",
        }, status=400)

    try:
        key = UUID(server_id_text or rule_id_text)
        months = int(request.GET.get("months", "1"))
        limit = int(request.GET.get("limit", "100"))
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Invalid id, months or limit.",
        }, status=400)

    if not 1 <= months <= 6 or not 1 <= limit <= 500:
        return JsonResponse({
            "status": "error",
            "message": "months must be 1-6 and limit 1-500.",
        }, status=400)

    query = SELECT_EVENTS_BY_SERVER if server_id_text else SELECT_EVENTS_BY_RULE

    try:
        events = []
        # Newest month first; rows inside a month are already newest first.
        for bucket in recent_month_buckets(months):
            remaining = limit - len(events)
            if remaining <= 0:
                break
            rows = cloud_session.execute(query, (key, bucket, remaining))
            events.extend(event_to_dict(row) for row in rows)

        return JsonResponse({
            "status": "success",
            "count": len(events),
            "events": events,
        })
    except Exception as error:
        print(f"CloudPulse alert history error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not load alert history."},
            status=500,
        )


@csrf_exempt
@require_POST
def resolve_alert_view(request):
    """Manually resolve an open alert: {server_id, rule_id, resolved_by}."""
    try:
        payload = json.loads(request.body)
        server_id = UUID(payload["server_id"])
        rule_id = UUID(payload["rule_id"])
        resolved_by = str(payload.get("resolved_by", "admin"))[:100]
    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return JsonResponse({
            "status": "error",
            "message": "Provide valid server_id and rule_id.",
        }, status=400)

    try:
        state = cloud_session.execute(
            SELECT_STATE, (server_id, rule_id)
        ).one()

        if state is None or state.state != "OPEN":
            return JsonResponse({
                "status": "error",
                "message": "No open alert for this server and rule.",
            }, status=404)

        now = datetime.now(timezone.utc)
        resolve_alert(state, state.last_value, now, resolved_by)

        return JsonResponse({
            "status": "success",
            "event_id": str(state.event_id),
            "resolved_at": now.isoformat(),
        })
    except Exception as error:
        print(f"CloudPulse resolve error: {error}")
        return JsonResponse(
            {"status": "error", "message": "Could not resolve alert."},
            status=500,
        )