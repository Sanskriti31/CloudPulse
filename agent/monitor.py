"""CloudPulse monitoring agent.

Collects CPU, memory, disk and network metrics with psutil and posts them
to the CloudPulse API. Optionally follows a log file and sends new lines.
Each machine gets its own persistent server ID.
"""

import argparse
import os
import platform
import re
import socket
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psutil
import requests

DEFAULT_API_URL = "http://127.0.0.1:8000/api/metrics/ingest/"
DEFAULT_ORG_ID = "local"
DEFAULT_INTERVAL = 10
DEFAULT_ID_FILE = Path(__file__).resolve().parent / ".server_id"
BUFFER_SIZE = 120                        # reports (metrics + logs) kept while the API is down
MAX_BUFFER_AGE = timedelta(minutes=9)    # API rejects samples over 10 min old
MAX_LOG_LINES = 200                      # per request (API limit)
LEVEL_PATTERN = re.compile(r"\b(DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\b", re.IGNORECASE)


def parse_args():
    """Settings come from command-line flags, then env vars, then defaults."""
    parser = argparse.ArgumentParser(description="CloudPulse monitoring agent")
    parser.add_argument(
        "--api-url",
        default=os.environ.get("CLOUDPULSE_API_URL", DEFAULT_API_URL),
        help="Metrics ingest endpoint URL",
    )
    parser.add_argument(
        "--logs-url",
        default=os.environ.get("CLOUDPULSE_LOGS_URL"),
        help="Logs ingest endpoint URL (default: derived from --api-url)",
    )
    parser.add_argument(
        "--log-file",
        default=os.environ.get("CLOUDPULSE_LOG_FILE"),
        help="A text log file to follow; new lines are sent to CloudPulse",
    )
    parser.add_argument(
        "--org",
        default=os.environ.get("CLOUDPULSE_ORG_ID", DEFAULT_ORG_ID),
        help="Organization ID",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.environ.get("CLOUDPULSE_INTERVAL", DEFAULT_INTERVAL)),
        help="Seconds between reports (5-300)",
    )
    parser.add_argument(
        "--id-file",
        type=Path,
        default=Path(os.environ.get("CLOUDPULSE_ID_FILE", DEFAULT_ID_FILE)),
        help="File that stores this machine's server ID",
    )
    parser.add_argument(
        "--hostname",
        default=os.environ.get("CLOUDPULSE_HOSTNAME", socket.gethostname()),
        help="Name shown on the dashboard",
    )

    args = parser.parse_args()
    if not 5 <= args.interval <= 300:
        parser.error("--interval must be between 5 and 300 seconds")
    if not args.logs_url:
        args.logs_url = args.api_url.replace("/metrics/ingest/", "/logs/ingest/")
    return args


def load_or_create_server_id(id_file):
    """Return this machine's server ID, creating and saving it on first run."""
    try:
        return str(uuid.UUID(id_file.read_text(encoding="utf-8").strip()))
    except FileNotFoundError:
        pass
    except ValueError:
        print(f"Ignoring invalid server ID in {id_file}; generating a new one.")

    server_id = str(uuid.uuid4())
    id_file.parent.mkdir(parents=True, exist_ok=True)
    id_file.write_text(server_id + "\n", encoding="utf-8")
    print(f"Generated new server ID {server_id} (saved in {id_file})")
    return server_id


def system_drive():
    """Root of the system disk on Windows, macOS and Linux."""
    if os.name == "nt":
        return os.environ.get("SystemDrive", "C:") + "\\"
    return "/"


class NetworkRate:
    """Turns psutil's cumulative byte counters into bytes per second."""

    def __init__(self):
        self.previous = None  # (monotonic time, bytes_sent, bytes_recv)

    def sample(self):
        counters = psutil.net_io_counters()
        current = (time.monotonic(), counters.bytes_sent, counters.bytes_recv)
        previous, self.previous = self.previous, current

        if previous is None:
            return None

        elapsed = current[0] - previous[0]
        sent = current[1] - previous[1]
        recv = current[2] - previous[2]

        # Counters reset after a reboot or adapter change: skip this sample.
        if elapsed <= 0 or sent < 0 or recv < 0:
            return None

        return sent / elapsed, recv / elapsed


class LogTailer:
    """Follows a text file like `tail -f`: only lines added after start-up.

    Reads in binary so the saved position is an exact byte offset, and
    starts again from the top if the file is truncated or replaced.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.position = None
        self.seq = 0            # line counter, makes every line unique

    def read_new_lines(self, max_lines=MAX_LOG_LINES):
        try:
            size = self.path.stat().st_size
        except FileNotFoundError:
            return []

        if self.position is None:          # first look: skip existing content
            self.position = size
            return []
        if size < self.position:           # truncated or rotated
            self.position = 0
        if size == self.position:
            return []

        lines = []
        with self.path.open("rb") as handle:
            handle.seek(self.position)
            while len(lines) < max_lines:
                raw = handle.readline()
                if not raw.endswith(b"\n"):     # incomplete line: wait for the rest
                    break
                self.position += len(raw)
                text = raw.decode("utf-8", errors="replace").strip().lstrip("\ufeff")
                if text:
                    lines.append(text)
        return lines


def log_entry(message, source, seq, level=None):
    if level is None:
        match = LEVEL_PATTERN.search(message)
        level = match.group(1).upper() if match else "INFO"
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "source": source,
        "message": message,
        "seq": seq,
    }


def collect_metrics(network):
    metrics = [
        {
            "metric_type": "cpu_percent",
            # Usage since the previous call, i.e. over the whole interval.
            "metric_value": psutil.cpu_percent(interval=None),
            "unit": "%",
        },
        {
            "metric_type": "memory_percent",
            "metric_value": psutil.virtual_memory().percent,
            "unit": "%",
        },
        {
            "metric_type": "disk_percent",
            "metric_value": psutil.disk_usage(system_drive()).percent,
            "unit": "%",
        },
    ]

    rates = network.sample()
    if rates is not None:
        sent_rate, recv_rate = rates
        metrics.append({
            "metric_type": "network_sent_bps",
            "metric_value": round(sent_rate, 2),
            "unit": "B/s",
        })
        metrics.append({
            "metric_type": "network_recv_bps",
            "metric_value": round(recv_rate, 2),
            "unit": "B/s",
        })

    return metrics


def oldest_time(payload):
    """When the oldest sample in a report was taken."""
    if "logs" in payload:
        return min(datetime.fromisoformat(e["ts"]) for e in payload["logs"])
    return datetime.fromisoformat(payload["collected_at"])


def send_pending(session, pending):
    """Send buffered reports, oldest first. Stop at the first failure."""
    cutoff = datetime.now(timezone.utc) - MAX_BUFFER_AGE

    while pending:
        url, payload = pending[0]

        # The API rejects samples older than 10 minutes: drop them here.
        if oldest_time(payload) < cutoff:
            pending.popleft()
            print("Dropped a buffered report older than 9 minutes")
            continue

        try:
            response = session.post(url, json=payload, timeout=10)
        except requests.RequestException as exc:
            print(f"API unreachable ({type(exc).__name__}) "
                  f"| {len(pending)} report(s) buffered")
            return

        if response.status_code >= 500:
            print(f"API error {response.status_code} "
                  f"| {len(pending)} report(s) buffered")
            return

        pending.popleft()

        if response.status_code >= 400:
            # A bad report will never succeed, so retrying it is pointless.
            print(f"Report rejected ({response.status_code}): {response.text[:120]}")
            continue

        try:
            result = response.json()
            still = f" | {len(pending)} still buffered" if pending else ""
            if "logs" in payload:
                print(f"LOGS    | Stored {result['accepted']} line(s), "
                      f"rejected {result['rejected']}{still}")
            else:
                alerts = result.get("alerts", {})
                print(
                    f"SUCCESS | Stored {result['stored_metrics']} metrics "
                    f"| Alerts opened: {alerts.get('opened', 0)} "
                    f"resolved: {alerts.get('resolved', 0)} "
                    f"| Sample: {result['recorded_at']} ({result.get('time_source', '?')})"
                    + still
                )
        except (KeyError, ValueError) as exc:
            print(f"Unexpected API response: {exc}")


def main():
    args = parse_args()
    server_id = load_or_create_server_id(args.id_file)

    base_payload = {
        "server_id": server_id,
        "org_id": args.org,
        "hostname": args.hostname,
        "os_name": f"{platform.system()} {platform.release()}",
    }

    session = requests.Session()   # reuses one HTTP connection
    network = NetworkRate()
    pending = deque(maxlen=BUFFER_SIZE)   # (url, payload) waiting to be sent
    tailer = LogTailer(args.log_file) if args.log_file else None

    # Prime the counters so the first report covers a full interval.
    psutil.cpu_percent(interval=None)
    network.sample()
    if tailer:
        tailer.read_new_lines()        # remember the current end of the file

    print("CloudPulse monitoring agent started.")
    print(f"  Server ID : {server_id}")
    print(f"  Hostname  : {args.hostname}")
    print(f"  Org       : {args.org}")
    print(f"  API       : {args.api_url}")
    print(f"  Logs API  : {args.logs_url}")
    print(f"  Log file  : {args.log_file or '(none: only agent events are sent)'}")
    print(f"  Interval  : {args.interval} s (first report in {args.interval} s)")
    print(f"  Buffer    : up to {BUFFER_SIZE} reports while the API is down")
    print("Press Ctrl+C to stop.\n")

    # The agent reports its own start-up as a log line.
    pending.append((args.logs_url, {
        "server_id": server_id,
        "logs": [log_entry(
            f"CloudPulse agent started on {args.hostname} "
            f"(interval {args.interval} s, log file: {args.log_file or 'none'})",
            "cloudpulse-agent", 0, "INFO",
        )],
    }))

    next_run = time.monotonic() + args.interval

    while True:
        time.sleep(max(0.0, next_run - time.monotonic()))
        next_run += args.interval
        if next_run < time.monotonic():          # fell behind: don't burst
            next_run = time.monotonic() + args.interval

        try:
            pending.append((args.api_url, {
                **base_payload,
                # The moment the sample was taken. The API stores it under
                # this time, so a retried report overwrites the same row.
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "metrics": collect_metrics(network),
            }))
        except Exception as exc:
            print(f"Metric collection failed: {exc}")

        if tailer:
            try:
                lines = tailer.read_new_lines()
                if lines:
                    entries = []
                    for line in lines:
                        tailer.seq += 1
                        entries.append(log_entry(line, tailer.path.name, tailer.seq))
                    pending.append((args.logs_url, {"server_id": server_id, "logs": entries}))
            except OSError as exc:
                print(f"Could not read log file: {exc}")

        send_pending(session, pending)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCloudPulse monitoring agent stopped.")