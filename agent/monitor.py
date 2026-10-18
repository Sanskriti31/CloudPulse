"""CloudPulse monitoring agent.

Collects CPU, memory, disk and network metrics with psutil and posts them
to the CloudPulse API. Each machine gets its own persistent server ID.
"""

import argparse
import os
import platform
import socket
import time
import uuid
from pathlib import Path

import psutil
import requests

DEFAULT_API_URL = "http://127.0.0.1:8000/api/metrics/ingest/"
DEFAULT_ORG_ID = "local"
DEFAULT_INTERVAL = 10
DEFAULT_ID_FILE = Path(__file__).resolve().parent / ".server_id"


def parse_args():
    """Settings come from command-line flags, then env vars, then defaults."""
    parser = argparse.ArgumentParser(description="CloudPulse monitoring agent")
    parser.add_argument(
        "--api-url",
        default=os.environ.get("CLOUDPULSE_API_URL", DEFAULT_API_URL),
        help="Ingest endpoint URL",
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

    # Prime the counters so the first report covers a full interval.
    psutil.cpu_percent(interval=None)
    network.sample()

    print("CloudPulse monitoring agent started.")
    print(f"  Server ID : {server_id}")
    print(f"  Hostname  : {args.hostname}")
    print(f"  Org       : {args.org}")
    print(f"  API       : {args.api_url}")
    print(f"  Interval  : {args.interval} s (first report in {args.interval} s)")
    print("Press Ctrl+C to stop.\n")

    next_run = time.monotonic() + args.interval

    while True:
        time.sleep(max(0.0, next_run - time.monotonic()))
        next_run += args.interval
        if next_run < time.monotonic():          # fell behind: don't burst
            next_run = time.monotonic() + args.interval

        try:
            payload = {**base_payload, "metrics": collect_metrics(network)}

            response = session.post(args.api_url, json=payload, timeout=10)
            response.raise_for_status()
            result = response.json()

            alerts = result.get("alerts", {})
            print(
                f"SUCCESS | Stored {result['stored_metrics']} metrics "
                f"| Alerts opened: {alerts.get('opened', 0)} "
                f"resolved: {alerts.get('resolved', 0)} "
                f"| Time: {result['recorded_at']}"
            )

        except requests.RequestException as exc:
            print(f"API request failed: {exc}")
        except (KeyError, ValueError) as exc:
            print(f"Unexpected API response: {exc}")
        except Exception as exc:
            print(f"Metric collection failed: {exc}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCloudPulse monitoring agent stopped.")