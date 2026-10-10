
import os
import platform
import socket
import time

import psutil
import requests

API_URL = "http://127.0.0.1:8000/api/metrics/ingest/"
SERVER_ID = "22222222-2222-4222-8222-222222222222"
ORG_ID = "local"
INTERVAL_SECONDS = 10


def collect_metrics():
    cpu_percent = psutil.cpu_percent(interval=1)
    memory_percent = psutil.virtual_memory().percent

    drive = os.environ.get("SystemDrive", "C:") + "\\"
    disk_percent = psutil.disk_usage(drive).percent

    network = psutil.net_io_counters()

    metrics = [
        {
            "metric_type": "cpu_percent",
            "metric_value": cpu_percent,
            "unit": "%",
        },
        {
            "metric_type": "memory_percent",
            "metric_value": memory_percent,
            "unit": "%",
        },
        {
            "metric_type": "disk_percent",
            "metric_value": disk_percent,
            "unit": "%",
        },
        {
            "metric_type": "network_sent_bytes",
            "metric_value": network.bytes_sent,
            "unit": "bytes",
        },
        {
            "metric_type": "network_recv_bytes",
            "metric_value": network.bytes_recv,
            "unit": "bytes",
        },
    ]

    return {
        "server_id": SERVER_ID,
        "org_id": ORG_ID,
        "hostname": socket.gethostname(),
        "os_name": f"{platform.system()} {platform.release()}",
        "metrics": metrics,
    }


def main():
    print("CloudPulse monitoring agent started.")
    print(f"Sending metrics to {API_URL}")
    print("Collecting five system metrics every 10 seconds.")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            payload = collect_metrics()

            response = requests.post(
                API_URL,
                json=payload,
                timeout=10,
            )
            response.raise_for_status()

            result = response.json()

            print(
                f"SUCCESS | Stored {result['stored_metrics']} metrics "
                f"| Time: {result['recorded_at']}"
            )

        except requests.RequestException as exc:
            print(f"API request failed: {exc}")

        except (KeyError, ValueError) as exc:
            print(f"Unexpected API response: {exc}")

        except Exception as exc:
            print(f"Metric collection failed: {exc}")

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCloudPulse monitoring agent stopped.")
