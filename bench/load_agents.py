"""Simulates many CloudPulse agents posting to the API at the same time.

Run from the CloudPulse folder with Django running:

  python bench/load_agents.py --agents 10  --duration 60
  python bench/load_agents.py --agents 50  --duration 60
  python bench/load_agents.py --agents 100 --duration 60 --interval 1

Each simulated agent has its own server ID and posts one 5-metric report every
--interval seconds (default 1 s, i.e. 10x faster than a real agent, so a few
agents create real load). They use org "bench", so the dashboard is unaffected.
Results are printed and appended to bench/results/load.csv.
"""

import argparse
import csv
import math
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def percentile(sorted_values, p):
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, int(round(p / 100 * (len(sorted_values) - 1))))
    return sorted_values[index]


def make_metrics():
    wave = math.sin(time.time() / 60)
    return [
        {"metric_type": "cpu_percent", "metric_value": round(40 + 20 * wave + random.uniform(-5, 5), 2), "unit": "%"},
        {"metric_type": "memory_percent", "metric_value": round(70 + random.uniform(-5, 5), 2), "unit": "%"},
        {"metric_type": "disk_percent", "metric_value": 78.8, "unit": "%"},
        {"metric_type": "network_sent_bps", "metric_value": round(random.uniform(1000, 50000), 2), "unit": "B/s"},
        {"metric_type": "network_recv_bps", "metric_value": round(random.uniform(1000, 90000), 2), "unit": "B/s"},
    ]


def run_agent(index, args, stop_at, results, lock):
    server_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"cloudpulse-load-agent-{index}"))
    session = requests.Session()
    next_run = time.monotonic() + random.uniform(0, args.interval)   # spread the starts

    while True:
        delay = next_run - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        if time.monotonic() >= stop_at:
            break
        next_run += args.interval

        payload = {
            "server_id": server_id,
            "org_id": args.org,
            "hostname": f"load-agent-{index:03d}",
            "os_name": "Simulated",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "metrics": make_metrics(),
        }
        started = time.perf_counter()
        try:
            status = session.post(args.url, json=payload, timeout=30).status_code
        except requests.RequestException:
            status = "error"
        elapsed_ms = (time.perf_counter() - started) * 1000

        with lock:
            results.append((elapsed_ms, status))


def main():
    parser = argparse.ArgumentParser(description="CloudPulse load generator")
    parser.add_argument("--agents", type=int, default=10)
    parser.add_argument("--duration", type=int, default=60, help="seconds")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between reports per agent")
    parser.add_argument("--url", default="http://127.0.0.1:8000/api/metrics/ingest/")
    parser.add_argument("--org", default="bench")
    parser.add_argument("--label", default="", help="note saved with the result, e.g. 'cass3 down'")
    args = parser.parse_args()

    offered = args.agents / args.interval
    print(f"{args.agents} agents x 1 report / {args.interval} s = {offered:.0f} requests/s offered, "
          f"for {args.duration} s ...")

    results, lock = [], threading.Lock()
    started = time.monotonic()
    stop_at = started + args.duration
    with ThreadPoolExecutor(max_workers=args.agents) as pool:
        for index in range(args.agents):
            pool.submit(run_agent, index, args, stop_at, results, lock)
    elapsed = time.monotonic() - started

    ok = sorted(ms for ms, status in results if status == 200)
    failed = len(results) - len(ok)
    row = {
        "agents": args.agents,
        "interval_s": args.interval,
        "offered_rps": round(offered, 1),
        "requests": len(results),
        "ok": len(ok),
        "errors": failed,
        "error_pct": round(100 * failed / len(results), 2) if results else 0,
        "achieved_rps": round(len(ok) / elapsed, 1),
        "metric_rows_per_s": round(5 * len(ok) / elapsed, 1),
        "p50_ms": round(percentile(ok, 50), 1),
        "p95_ms": round(percentile(ok, 95), 1),
        "p99_ms": round(percentile(ok, 99), 1),
        "max_ms": round(ok[-1], 1) if ok else float("nan"),
        "label": args.label,
    }

    print()
    for key, value in row.items():
        print(f"  {key:18} {value}")
    statuses = {}
    for _, status in results:
        statuses[status] = statuses.get(status, 0) + 1
    print(f"  {'status codes':18} {statuses}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / "load.csv"
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["run_at"] + list(row))
        if new_file:
            writer.writeheader()
        writer.writerow({"run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **row})
    print(f"\nSaved to {path}")


if __name__ == "__main__":
    main()