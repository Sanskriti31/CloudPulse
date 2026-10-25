"""CloudPulse database benchmarks: talks to Cassandra directly (no Django).

Run from the CloudPulse folder with the venv active and the cluster running:

  python bench/db_bench.py seed          # synthetic history used by the read tests
  python bench/db_bench.py writes        # write-path strategies (Phase 0 vs Phase 1)
  python bench/db_bench.py consistency   # ONE vs LOCAL_QUORUM vs ALL
  python bench/db_bench.py reads         # raw vs 1-minute vs 1-hour history reads
  python bench/db_bench.py partition     # per-server vs per-metric raw partitions
  python bench/db_bench.py cleanup       # remove all benchmark data

Every result is printed and appended to bench/results/<test>.csv.
Benchmark data uses its own server IDs and org "bench", so the dashboard
(org "local") is not affected.
"""

import argparse
import csv
import math
import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType

HOST = "127.0.0.1"
KEYSPACE = "cloudpulse"
LEVELS = ("ONE", "LOCAL_QUORUM", "ALL")
METRICS = ("cpu_percent", "memory_percent", "disk_percent",
           "network_sent_bps", "network_recv_bps")
UNITS = {"cpu_percent": "%", "memory_percent": "%", "disk_percent": "%",
         "network_sent_bps": "B/s", "network_recv_bps": "B/s"}
BENCH_ORG = "bench"
HISTORY_SERVER = uuid.uuid5(uuid.NAMESPACE_DNS, "cloudpulse-bench-history")
WRITE_SERVER = uuid.uuid5(uuid.NAMESPACE_DNS, "cloudpulse-bench-writes")
RAW_DAYS = 2          # complete raw days seeded (yesterday and the day before)
ROLLUP_DAYS = 30      # days of 1-minute and 1-hour rollups seeded
RESULTS_DIR = Path(__file__).resolve().parent / "results"


# ---------------------------------------------------------------- helpers

def make_profile(level):
    return ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([HOST]),
        consistency_level=ConsistencyLevel.name_to_value[level],
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=60,
    )


def connect():
    profiles = {EXEC_PROFILE_DEFAULT: make_profile("LOCAL_QUORUM")}
    for level in LEVELS:
        profiles[level] = make_profile(level)
    cluster = Cluster([HOST], port=9042, execution_profiles=profiles,
                      protocol_version=5)
    return cluster, cluster.connect(KEYSPACE)


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[index]


def latency_stats(latencies_ms):
    values = sorted(latencies_ms)
    return {
        "n": len(values),
        "mean_ms": round(sum(values) / len(values), 2) if values else float("nan"),
        "p50_ms": round(percentile(values, 50), 2),
        "p95_ms": round(percentile(values, 95), 2),
        "p99_ms": round(percentile(values, 99), 2),
        "max_ms": round(values[-1], 2) if values else float("nan"),
    }


def report(test, rows):
    """Print rows as a table and append them to bench/results/<test>.csv."""
    if not rows:
        return
    columns = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in columns}
    print()
    print(" | ".join(c.ljust(widths[c]) for c in columns))
    print("-+-".join("-" * widths[c] for c in columns))
    for row in rows:
        print(" | ".join(str(row[c]).ljust(widths[c]) for c in columns))
    print()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"{test}.csv"
    new_file = not path.exists()
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_at"] + columns)
        if new_file:
            writer.writeheader()
        for row in rows:
            writer.writerow({"run_at": stamp, **row})
    print(f"Saved to {path}")


class Pipeline:
    """Keeps up to `limit` async requests in flight (for fast seeding)."""

    def __init__(self, session, limit=64):
        self.session = session
        self.limit = limit
        self.futures = deque()
        self.sent = 0

    def submit(self, statement, params=None):
        self.futures.append(self.session.execute_async(statement, params))
        self.sent += 1
        if len(self.futures) >= self.limit:
            self.drain(self.limit // 2)

    def drain(self, keep=0):
        while len(self.futures) > keep:
            self.futures.popleft().result()


def synthetic(metric, moment):
    """Smooth daily wave plus noise, so charts and rollups look realistic."""
    seconds = moment.timestamp()
    wave = math.sin(2 * math.pi * (seconds % 86400) / 86400)
    noise = random.uniform(-3, 3)
    if metric == "cpu_percent":
        return round(max(0.0, min(100.0, 35 + 20 * wave + noise)), 2)
    if metric == "memory_percent":
        return round(max(0.0, min(100.0, 70 + 10 * wave + noise)), 2)
    if metric == "disk_percent":
        return 78.8
    return round(max(0.0, 20000 + 15000 * wave + noise * 1000), 2)


def batches_of(statement, rows, size=100):
    for i in range(0, len(rows), size):
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for params in rows[i:i + size]:
            batch.add(statement, params)
        yield batch


def complete_raw_days(today):
    return [today - timedelta(days=offset) for offset in range(1, RAW_DAYS + 1)]


# ------------------------------------------------------------ statements

def prepare_all(session):
    p = session.prepare
    return {
        "raw": p("INSERT INTO metrics_by_server_bucket (server_id, bucket_date, "
                 "metric_time, metric_type, metric_value, unit) VALUES (?, ?, ?, ?, ?, ?)"),
        "raw_metric": p("INSERT INTO bench_metrics_by_server_metric_day (server_id, "
                        "metric_type, bucket_date, metric_time, metric_value) "
                        "VALUES (?, ?, ?, ?, ?)"),
        "latest": p("INSERT INTO latest_metric_by_server (server_id, metric_type, "
                    "metric_value, unit, recorded_at) VALUES (?, ?, ?, ?, ?) "
                    "USING TIMESTAMP ?"),
        "hb_id": p("UPDATE server_by_id USING TIMESTAMP ? SET org_id = ?, hostname = ?, "
                   "os_name = ?, health_status = 'UP', last_seen = ? WHERE server_id = ?"),
        "hb_org": p("UPDATE servers_by_org USING TIMESTAMP ? SET hostname = ?, "
                    "os_name = ?, health_status = 'UP', last_seen = ? "
                    "WHERE org_id = ? AND server_id = ?"),
        "r1m": p("INSERT INTO metrics_rollup_1m (server_id, metric_type, bucket_date, "
                 "window_start, min_value, max_value, avg_value, sample_count, unit) "
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
        "r1h": p("INSERT INTO metrics_rollup_1h (server_id, metric_type, month_bucket, "
                 "window_start, min_value, max_value, avg_value, sample_count, unit) "
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
        "sel_raw_day": p("SELECT metric_time, metric_type, metric_value "
                         "FROM metrics_by_server_bucket WHERE server_id = ? AND bucket_date = ?"),
        "sel_raw_metric_day": p("SELECT metric_time, metric_value "
                                "FROM bench_metrics_by_server_metric_day "
                                "WHERE server_id = ? AND metric_type = ? AND bucket_date = ?"),
        "sel_1m_day": p("SELECT window_start, avg_value FROM metrics_rollup_1m "
                        "WHERE server_id = ? AND metric_type = ? AND bucket_date = ?"),
        "sel_1h_since": p("SELECT window_start, avg_value FROM metrics_rollup_1h "
                          "WHERE server_id = ? AND metric_type = ? AND month_bucket = ? "
                          "AND window_start >= ?"),
        "sel_latest": p("SELECT metric_type, metric_value FROM latest_metric_by_server "
                        "WHERE server_id = ?"),
    }


def create_bench_table(session):
    """Raw data partitioned per metric: the alternative design being compared."""
    session.execute("""
        CREATE TABLE IF NOT EXISTS bench_metrics_by_server_metric_day (
            server_id uuid, metric_type text, bucket_date date,
            metric_time timestamp, metric_value double,
            PRIMARY KEY ((server_id, metric_type, bucket_date), metric_time)
        ) WITH CLUSTERING ORDER BY (metric_time DESC)
          AND default_time_to_live = 604800
    """)


# -------------------------------------------------------------- commands

def cmd_seed(session, args):
    create_bench_table(session)
    st = prepare_all(session)
    pipe = Pipeline(session, limit=args.concurrency)
    today = datetime.now(timezone.utc).date()
    started = time.perf_counter()

    # 1) Raw data: RAW_DAYS complete days, 10 s apart, in BOTH raw designs.
    raw_rows = 0
    for day in complete_raw_days(today):
        day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        per_metric = {m: [] for m in METRICS}
        for step in range(8640):
            moment = day_start + timedelta(seconds=10 * step)
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for metric in METRICS:
                value = synthetic(metric, moment)
                batch.add(st["raw"], (HISTORY_SERVER, day, moment, metric,
                                      value, UNITS[metric]))
                per_metric[metric].append((HISTORY_SERVER, metric, day, moment, value))
                raw_rows += 1
            pipe.submit(batch)
        for metric, rows in per_metric.items():
            for batch in batches_of(st["raw_metric"], rows):
                pipe.submit(batch)
        print(f"  raw day {day}: done")

    # 2) 1-minute and 1-hour rollups for ROLLUP_DAYS days (ending yesterday).
    minute_rows = hour_rows = 0
    for offset in range(1, ROLLUP_DAYS + 1):
        day = today - timedelta(days=offset)
        day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        for metric in METRICS:
            minutes, hours = [], {}
            for minute in range(1440):
                moment = day_start + timedelta(minutes=minute)
                avg = synthetic(metric, moment)
                low, high = round(avg * 0.9, 2), round(avg * 1.1, 2)
                minutes.append((HISTORY_SERVER, metric, day, moment,
                                low, high, avg, 6, UNITS[metric]))
                hour = moment.replace(minute=0)
                agg = hours.setdefault(hour, [low, high, 0.0, 0])
                agg[0], agg[1] = min(agg[0], low), max(agg[1], high)
                agg[2] += avg * 6
                agg[3] += 6
            for batch in batches_of(st["r1m"], minutes):
                pipe.submit(batch)
            minute_rows += len(minutes)
            hour_list = [
                (HISTORY_SERVER, metric, hour.strftime("%Y-%m"), hour,
                 low, high, total / count, count, UNITS[metric])
                for hour, (low, high, total, count) in hours.items()
            ]
            for batch in batches_of(st["r1h"], hour_list):
                pipe.submit(batch)
            hour_rows += len(hour_list)
    pipe.drain()

    seconds = time.perf_counter() - started
    report("seed", [{
        "raw_rows": raw_rows,
        "raw_per_metric_rows": raw_rows,
        "rollup_1m_rows": minute_rows,
        "rollup_1h_rows": hour_rows,
        "requests": pipe.sent,
        "seconds": round(seconds, 1),
        "rows_per_sec": round((raw_rows * 2 + minute_rows + hour_rows) / seconds),
    }])


def ingest_naive(session, moment, level):
    """Phase 0 style: plain string queries, 2 LWTs, 14 sequential round trips."""
    opts = {"execution_profile": level}
    session.execute(
        "INSERT INTO server_by_id (server_id, org_id, hostname, os_name, "
        "registered_at, health_status, last_seen) VALUES (%s, %s, %s, %s, %s, 'UP', %s) "
        "IF NOT EXISTS",
        (WRITE_SERVER, BENCH_ORG, "bench", "bench", moment, moment), **opts)
    session.execute(
        "INSERT INTO servers_by_org (org_id, server_id, hostname, os_name, "
        "registered_at, health_status, last_seen) VALUES (%s, %s, %s, %s, %s, 'UP', %s) "
        "IF NOT EXISTS",
        (BENCH_ORG, WRITE_SERVER, "bench", "bench", moment, moment), **opts)
    session.execute(
        "UPDATE server_by_id SET org_id = %s, hostname = %s, os_name = %s, "
        "health_status = 'UP', last_seen = %s WHERE server_id = %s",
        (BENCH_ORG, "bench", "bench", moment, WRITE_SERVER), **opts)
    session.execute(
        "UPDATE servers_by_org SET hostname = %s, os_name = %s, health_status = 'UP', "
        "last_seen = %s WHERE org_id = %s AND server_id = %s",
        ("bench", "bench", moment, BENCH_ORG, WRITE_SERVER), **opts)
    for metric in METRICS:
        value = synthetic(metric, moment)
        session.execute(
            "INSERT INTO metrics_by_server_bucket (server_id, bucket_date, metric_time, "
            "metric_type, metric_value, unit) VALUES (%s, %s, %s, %s, %s, %s)",
            (WRITE_SERVER, moment.date(), moment, metric, value, UNITS[metric]), **opts)
        session.execute(
            "INSERT INTO latest_metric_by_server (server_id, metric_type, metric_value, "
            "unit, recorded_at) VALUES (%s, %s, %s, %s, %s)",
            (WRITE_SERVER, metric, value, UNITS[metric], moment), **opts)


def ingest_prepared(session, st, moment, level):
    """Prepared statements, no LWT, still 12 sequential round trips."""
    ts = int(moment.timestamp() * 1_000_000)
    opts = {"execution_profile": level}
    session.execute(st["hb_id"], (ts, BENCH_ORG, "bench", "bench", moment, WRITE_SERVER), **opts)
    session.execute(st["hb_org"], (ts, "bench", "bench", moment, BENCH_ORG, WRITE_SERVER), **opts)
    for metric in METRICS:
        value = synthetic(metric, moment)
        session.execute(st["raw"], (WRITE_SERVER, moment.date(), moment, metric,
                                    value, UNITS[metric]), **opts)
        session.execute(st["latest"], (WRITE_SERVER, metric, value, UNITS[metric],
                                       moment, ts), **opts)


def build_batches(st, moment):
    """Phase 1 style: 3 batches (heartbeat LOGGED, history and latest UNLOGGED)."""
    ts = int(moment.timestamp() * 1_000_000)
    heartbeat = BatchStatement(batch_type=BatchType.LOGGED)
    heartbeat.add(st["hb_id"], (ts, BENCH_ORG, "bench", "bench", moment, WRITE_SERVER))
    heartbeat.add(st["hb_org"], (ts, "bench", "bench", moment, BENCH_ORG, WRITE_SERVER))
    history = BatchStatement(batch_type=BatchType.UNLOGGED)
    latest = BatchStatement(batch_type=BatchType.UNLOGGED)
    for metric in METRICS:
        value = synthetic(metric, moment)
        history.add(st["raw"], (WRITE_SERVER, moment.date(), moment, metric,
                                value, UNITS[metric]))
        latest.add(st["latest"], (WRITE_SERVER, metric, value, UNITS[metric], moment, ts))
    return heartbeat, history, latest


def ingest_batched(session, st, moment, level):
    futures = [session.execute_async(b, execution_profile=level)
               for b in build_batches(st, moment)]
    for future in futures:
        future.result()


def timed_ingests(n, warmup, func):
    base = datetime.now(timezone.utc) - timedelta(minutes=5)
    latencies = []
    started = None
    for i in range(warmup + n):
        moment = base + timedelta(milliseconds=i)
        if i == warmup:
            started = time.perf_counter()
        t0 = time.perf_counter()
        func(moment)
        if i >= warmup:
            latencies.append((time.perf_counter() - t0) * 1000)
    total = time.perf_counter() - started
    return latencies, n / total


def cmd_writes(session, args):
    st = prepare_all(session)
    level = "LOCAL_QUORUM"
    rows = []
    tests = [
        ("naive: strings + LWT, 14 sequential", 14,
         lambda m: ingest_naive(session, m, level)),
        ("prepared, 12 sequential", 12,
         lambda m: ingest_prepared(session, st, m, level)),
        ("prepared + 3 async batches (Phase 1)", 3,
         lambda m: ingest_batched(session, st, m, level)),
    ]
    for name, requests_per_ingest, func in tests:
        print(f"Running: {name} ...")
        latencies, per_sec = timed_ingests(args.n, args.warmup, func)
        rows.append({"strategy": name, "requests_per_ingest": requests_per_ingest,
                     **latency_stats(latencies), "ingests_per_sec": round(per_sec, 1)})

    # Throughput when many ingests are in flight at once (like many agents).
    print(f"Running: Phase 1 batches, {args.concurrency} ingests in flight ...")
    base = datetime.now(timezone.utc) - timedelta(minutes=4)
    pipe = Pipeline(session, limit=args.concurrency * 3)
    started = time.perf_counter()
    for i in range(args.n):
        for batch in build_batches(st, base + timedelta(milliseconds=i)):
            pipe.submit(batch)
    pipe.drain()
    per_sec = args.n / (time.perf_counter() - started)
    rows.append({"strategy": f"Phase 1 batches, {args.concurrency} in flight",
                 "requests_per_ingest": 3, "n": args.n, "mean_ms": "-", "p50_ms": "-",
                 "p95_ms": "-", "p99_ms": "-", "max_ms": "-",
                 "ingests_per_sec": round(per_sec, 1)})
    report("writes", rows)


def cmd_consistency(session, args):
    st = prepare_all(session)
    rows = []
    for level in LEVELS:
        print(f"Running: {level} ...")
        try:
            latencies, per_sec = timed_ingests(
                args.n, args.warmup, lambda m: ingest_batched(session, st, m, level))
            rows.append({"operation": "write (Phase 1 ingest)", "consistency": level,
                         **latency_stats(latencies), "ops_per_sec": round(per_sec, 1),
                         "result": "ok"})
        except Exception as exc:
            rows.append({"operation": "write (Phase 1 ingest)", "consistency": level,
                         "n": 0, "mean_ms": "-", "p50_ms": "-", "p95_ms": "-",
                         "p99_ms": "-", "max_ms": "-", "ops_per_sec": 0,
                         "result": type(exc).__name__})
        try:
            latencies = []
            for i in range(args.warmup + args.n):
                t0 = time.perf_counter()
                list(session.execute(st["sel_latest"], (WRITE_SERVER,),
                                     execution_profile=level))
                if i >= args.warmup:
                    latencies.append((time.perf_counter() - t0) * 1000)
            stats = latency_stats(latencies)
            rows.append({"operation": "read (latest values)", "consistency": level,
                         **stats, "ops_per_sec": round(1000 / stats["mean_ms"], 1),
                         "result": "ok"})
        except Exception as exc:
            rows.append({"operation": "read (latest values)", "consistency": level,
                         "n": 0, "mean_ms": "-", "p50_ms": "-", "p95_ms": "-",
                         "p99_ms": "-", "max_ms": "-", "ops_per_sec": 0,
                         "result": type(exc).__name__})
    report("consistency", rows)


def run_read(session, queries, repeat):
    """Runs a list of (statement, params) concurrently `repeat` times."""
    latencies, rows_read = [], 0
    for i in range(repeat + 1):              # first run is a warm-up
        t0 = time.perf_counter()
        futures = [session.execute_async(stmt, params) for stmt, params in queries]
        count = sum(len(list(f.result())) for f in futures)
        if i > 0:
            latencies.append((time.perf_counter() - t0) * 1000)
            rows_read = count
    return rows_read, latencies


def cmd_reads(session, args):
    st = prepare_all(session)
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    now = datetime.now(timezone.utc)

    def rollup_1m(days):
        return [(st["sel_1m_day"], (HISTORY_SERVER, m, today - timedelta(days=o)))
                for m in METRICS for o in range(1, days + 1)]

    since = now - timedelta(days=30)
    months = sorted({(since + timedelta(days=d)).strftime("%Y-%m") for d in range(31)})
    tests = [
        ("raw", "1 day", [(st["sel_raw_day"], (HISTORY_SERVER, yesterday))]),
        ("raw", "2 days", [(st["sel_raw_day"], (HISTORY_SERVER, d))
                           for d in complete_raw_days(today)]),
        ("1m", "1 day", rollup_1m(1)),
        ("1m", "7 days", rollup_1m(7)),
        ("1m", "30 days", rollup_1m(30)),
        ("1h", "30 days", [(st["sel_1h_since"], (HISTORY_SERVER, m, month, since))
                           for m in METRICS for month in months]),
    ]
    rows = []
    for resolution, span, queries in tests:
        print(f"Running: {resolution} {span} ...")
        rows_read, latencies = run_read(session, queries, args.repeat)
        stats = latency_stats(latencies)
        rows.append({"resolution": resolution, "range": span,
                     "partitions": len(queries), "rows_read": rows_read,
                     "p50_ms": stats["p50_ms"], "p95_ms": stats["p95_ms"],
                     "mean_ms": stats["mean_ms"]})
    report("reads", rows)


def cmd_partition(session, args):
    st = prepare_all(session)
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    tests = [
        ("CPU only, 1 day", "per server-day (current raw table)",
         [(st["sel_raw_day"], (HISTORY_SERVER, yesterday))]),
        ("CPU only, 1 day", "per server-metric-day (alternative)",
         [(st["sel_raw_metric_day"], (HISTORY_SERVER, "cpu_percent", yesterday))]),
        ("all 5 metrics, 1 day", "per server-day (current raw table)",
         [(st["sel_raw_day"], (HISTORY_SERVER, yesterday))]),
        ("all 5 metrics, 1 day", "per server-metric-day (alternative)",
         [(st["sel_raw_metric_day"], (HISTORY_SERVER, m, yesterday)) for m in METRICS]),
    ]
    rows = []
    for question, design, queries in tests:
        print(f"Running: {question} / {design} ...")
        rows_read, latencies = run_read(session, queries, args.repeat)
        stats = latency_stats(latencies)
        rows.append({"query": question, "design": design, "partitions": len(queries),
                     "rows_read": rows_read, "p50_ms": stats["p50_ms"],
                     "p95_ms": stats["p95_ms"]})
    report("partition", rows)


def cmd_cleanup(session, args):
    today = datetime.now(timezone.utc).date()
    pipe = Pipeline(session)
    for server in (HISTORY_SERVER, WRITE_SERVER):
        for offset in range(0, ROLLUP_DAYS + 2):
            day = today - timedelta(days=offset)
            pipe.submit("DELETE FROM metrics_by_server_bucket "
                        "WHERE server_id = %s AND bucket_date = %s", (server, day))
            for metric in METRICS:
                pipe.submit("DELETE FROM metrics_rollup_1m WHERE server_id = %s "
                            "AND metric_type = %s AND bucket_date = %s",
                            (server, metric, day))
        for metric in METRICS:
            for offset in range(0, 3):
                month = (today - timedelta(days=31 * offset)).strftime("%Y-%m")
                pipe.submit("DELETE FROM metrics_rollup_1h WHERE server_id = %s "
                            "AND metric_type = %s AND month_bucket = %s",
                            (server, metric, month))
        pipe.submit("DELETE FROM latest_metric_by_server WHERE server_id = %s", (server,))
        pipe.submit("DELETE FROM server_by_id WHERE server_id = %s", (server,))
    # Servers created by load_agents.py (load-agent-0 ... load-agent-499).
    for index in range(500):
        server = uuid.uuid5(uuid.NAMESPACE_DNS, f"cloudpulse-load-agent-{index}")
        for offset in range(0, 8):
            pipe.submit("DELETE FROM metrics_by_server_bucket "
                        "WHERE server_id = %s AND bucket_date = %s",
                        (server, today - timedelta(days=offset)))
        pipe.submit("DELETE FROM latest_metric_by_server WHERE server_id = %s", (server,))
        pipe.submit("DELETE FROM alert_state_by_server WHERE server_id = %s", (server,))
        pipe.submit("DELETE FROM server_by_id WHERE server_id = %s", (server,))
    pipe.submit("DELETE FROM servers_by_org WHERE org_id = %s", (BENCH_ORG,))
    pipe.drain()
    session.execute("DROP TABLE IF EXISTS bench_metrics_by_server_metric_day")
    print(f"Cleanup done: {pipe.sent} partition deletes, benchmark table dropped.")


def main():
    parser = argparse.ArgumentParser(description="CloudPulse Cassandra benchmarks")
    parser.add_argument("test", choices=["seed", "writes", "consistency", "reads",
                                         "partition", "cleanup"])
    parser.add_argument("--n", type=int, default=500, help="operations per test")
    parser.add_argument("--warmup", type=int, default=50, help="untimed warm-up ops")
    parser.add_argument("--repeat", type=int, default=10, help="runs per read test")
    parser.add_argument("--concurrency", type=int, default=32,
                        help="requests in flight for seed / concurrent writes")
    args = parser.parse_args()

    cluster, session = connect()
    try:
        {"seed": cmd_seed, "writes": cmd_writes, "consistency": cmd_consistency,
         "reads": cmd_reads, "partition": cmd_partition,
         "cleanup": cmd_cleanup}[args.test](session, args)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()