from uuid import UUID
from django.views.decorators.http import require_GET
import json
import math
from datetime import datetime, timezone, timedelta
from uuid import UUID

from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.http import JsonResponse
from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("dbms_test")

cloud_session = cluster.connect("cloudpulse")

def test_cassandra(request):
    rows = session.execute("SELECT * FROM users")

    data = []

    for row in rows:
        data.append({
            "user_id": str(row.user_id),
            "name": row.name,
            "email": row.email
        })

    return JsonResponse({
        "status": "success",
        "database": "Cassandra",
        "users": data
    })


ALLOWED_METRICS = {
    "cpu_percent",
    "memory_percent",
    "disk_percent",
    "network_sent_bytes",
    "network_recv_bytes",
}


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
        session.execute(
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

        session.execute(
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
        session.execute(
            """
            UPDATE cloudpulse.server_by_id
            SET org_id=%s, hostname=%s, os_name=%s,
                health_status=%s, last_seen=%s
            WHERE server_id=%s
            """,
            (org_id, hostname, os_name, "UP", recorded_at, server_id),
        )

        session.execute(
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
            session.execute(
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

            session.execute(
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

    return JsonResponse({
        "status": "success",
        "server_id": str(server_id),
        "stored_metrics": len(validated_metrics),
        "recorded_at": recorded_at.isoformat(),
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