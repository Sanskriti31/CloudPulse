import json
import math
import operator
import time
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import (require_GET, require_POST, require_http_methods,)
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
from cassandra.util import uuid_from_time

cluster = Cluster(["127.0.0.1"])
cloud_session = cluster.connect("cloudpulse")

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


@csrf_exempt
@require_POST
def ingest_metrics(request):
    """Receive system metrics and store them in Cassandra."""

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

            validated_metrics.append(
                (metric_type, metric_value, unit)
            )

    except (ValueError, TypeError, KeyError, json.JSONDecodeError):
        return JsonResponse(
            {"status": "error", "message": "Invalid metric payload"},
            status=400,
        )

    recorded_at = datetime.now(timezone.utc)
    bucket_date = recorded_at.date()

    try:
        # Register each server once, preserving its registration time.
        cloud_session.execute(
            """
            INSERT INTO cloudpulse.server_by_id
            (server_id, org_id, hostname, os_name,
             registered_at, health_status, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (
                server_id, org_id, hostname, os_name,
                recorded_at, "UP", recorded_at,
            ),
        )

        cloud_session.execute(
            """
            INSERT INTO cloudpulse.servers_by_org
            (org_id, server_id, hostname, os_name,
             registered_at, health_status, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (
                org_id, server_id, hostname, os_name,
                recorded_at, "UP", recorded_at,
            ),
        )

        # Refresh the server's latest status and heartbeat.
        cloud_session.execute(
            """
            UPDATE cloudpulse.server_by_id
            SET org_id=%s, hostname=%s, os_name=%s,
                health_status=%s, last_seen=%s
            WHERE server_id=%s
            """,
            (org_id, hostname, os_name, "UP", recorded_at, server_id),
        )

        cloud_session.execute(
            """
            UPDATE cloudpulse.servers_by_org
            SET hostname=%s, os_name=%s,
                health_status=%s, last_seen=%s
            WHERE org_id=%s AND server_id=%s
            """,
            (hostname, os_name, "UP", recorded_at, org_id, server_id),
        )

        # Store every reading in the historical table and update
        # the latest reading for each metric type.
        for metric_type, metric_value, unit in validated_metrics:
            cloud_session.execute(
                """
                INSERT INTO cloudpulse.metrics_by_server_bucket
                (server_id, bucket_date, metric_time,
                 metric_type, metric_value, unit)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    server_id, bucket_date, recorded_at,
                    metric_type, metric_value, unit,
                ),
            )

            cloud_session.execute(
                """
                INSERT INTO cloudpulse.latest_metric_by_server
                (server_id, metric_type, metric_value, unit, recorded_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    server_id, metric_type, metric_value,
                    unit, recorded_at,
                ),
            )

    except Exception as exc:
        print(f"CloudPulse ingestion error: {exc}")
        return JsonResponse(
            {
                "status": "error",
                "message": "Database write failed; check Django terminal.",
            },
            status=500,
        )

    # Run the alert engine. A failure here must not lose the metrics,
    # which are already stored, so it is caught and logged.
    alert_summary = {"opened": 0, "resolved": 0}
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
        "alerts": alert_summary,
    })



@require_GET
def list_servers(request):
    org_id = request.GET.get("org_id", "local")

    try:
        rows = cloud_session.execute(
            """
            SELECT server_id, hostname, os_name, health_status,
                   last_seen, registered_at
            FROM servers_by_org
            WHERE org_id = %s
            """,
            (org_id,)
        )

        servers = []

        for row in rows:
            servers.append({
                "server_id": str(row.server_id),
                "hostname": row.hostname,
                "os_name": row.os_name,
                "health_status": row.health_status,
                "last_seen": (
                    row.last_seen.isoformat()
                    if row.last_seen else None
                ),
                "registered_at": (
                    row.registered_at.isoformat()
                    if row.registered_at else None
                ),
            })

        return JsonResponse({
            "status": "success",
            "count": len(servers),
            "servers": servers,
        })

    except Exception as error:
        return JsonResponse({
            "status": "error",
            "message": str(error),
        }, status=500)


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

        rows = cloud_session.execute(
            """
            SELECT metric_type, metric_value, recorded_at, unit
            FROM latest_metric_by_server
            WHERE server_id = %s
            """,
            (server_id,)
        )

        metrics = []

        for row in rows:
            metrics.append({
                "metric_type": row.metric_type,
                "metric_value": row.metric_value,
                "recorded_at": (
                    row.recorded_at.isoformat()
                    if row.recorded_at else None
                ),
                "unit": row.unit,
            })

        return JsonResponse({
            "status": "success",
            "server_id": str(server_id),
            "count": len(metrics),
            "metrics": metrics,
        })

    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Invalid server_id. Please provide a valid UUID.",
        }, status=400)

    except Exception as error:
        return JsonResponse({
            "status": "error",
            "message": str(error),
        }, status=500)


@require_GET
def historical_metrics(request):
    server_id_text = request.GET.get("server_id")

    if not server_id_text:
        return JsonResponse({
            "status": "error",
            "message": "Please provide a server_id parameter."
        }, status=400)

    try:
        server_id = UUID(server_id_text)
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "Invalid server_id UUID."
        }, status=400)

    try:
        days = int(request.GET.get("days", "7"))
    except ValueError:
        return JsonResponse({
            "status": "error",
            "message": "days must be an integer between 1 and 30."
        }, status=400)

    if not 1 <= days <= 30:
        return JsonResponse({
            "status": "error",
            "message": "days must be between 1 and 30."
        }, status=400)

    try:
        today = datetime.now(timezone.utc).date()
        collected = []

        query = """
            SELECT metric_time, metric_type, metric_value, unit
            FROM metrics_by_server_bucket
            WHERE server_id = %s AND bucket_date = %s
        """

        # Each Cassandra partition is queried using its full partition key.
        for offset in range(days):
            bucket_date = today - timedelta(days=offset)

            rows = cloud_session.execute(
                query,
                (server_id, bucket_date)
            )

            for row in rows:
                collected.append((
                    row.metric_time,
                    {
                        "recorded_at": (
                            row.metric_time.isoformat()
                            if row.metric_time else None
                        ),
                        "metric_type": row.metric_type,
                        "metric_value": row.metric_value,
                        "unit": row.unit,
                    }
                ))

        # Display readings from oldest to newest.
        collected.sort(key=lambda item: item[0])
        history = [item[1] for item in collected]

        return JsonResponse({
            "status": "success",
            "server_id": str(server_id),
            "days_requested": days,
            "count": len(history),
            "metrics": history,
        })

    except Exception as error:
        return JsonResponse({
            "status": "error",
            "message": str(error)
        }, status=500)


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