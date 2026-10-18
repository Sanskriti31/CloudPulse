# CloudPulse

A cloud observability platform that collects infrastructure metrics from monitored machines, stores them as time-series data in **Apache Cassandra**, and shows them on a live **React** dashboard.

Built for the M.Tech Database Modelling and Design Lab (MACSE518), VIT.

**Team:** Navya Mongia, Sunidhi Raichur, Sanskriti Singh

---

## Architecture

```
+--------------+   POST every 10s   +-------------------+   CQL    +-----------+
| Agent        | -----------------> | Django REST API   | -------> | Cassandra |
| (psutil)     |   /api/metrics/    | (ingest + query)  | <------- | cloudpulse|
+--------------+       ingest/      +-------------------+          +-----------+
                                             ^
                                             | GET /api/servers, /metrics/latest, /metrics/history
                                     +---------------+
                                     | React + Vite  |
                                     | dashboard     |
                                     +---------------+
```

| Component | Folder | Tech |
|---|---|---|
| Monitoring agent | `agent/` | Python, psutil, requests |
| API | `api/`, `backend/` | Django, Django REST Framework, cassandra-driver |
| Database schema | `database/` | CQL (Cassandra) |
| Dashboard | `frontend/` | React, Vite, Recharts |

## Data model (Cassandra, query-driven)

One table per access pattern, with time-bucketed partitions so no partition grows without bound.

| Table | Primary key | Serves |
|---|---|---|
| `server_by_id` | `server_id` | Look up one server |
| `servers_by_org` | `(org_id), server_id` | List all servers of an organization |
| `metrics_by_server_bucket` | `((server_id, bucket_date), metric_time DESC, metric_type)` | Historical range queries (one partition per server per day) |
| `latest_metric_by_server` | `(server_id), metric_type` | Current value per metric for the overview cards |

## Prerequisites

- Python 3.12+
- Node.js 20+ and npm
- Docker (for Cassandra), or a local Cassandra install

## Setup and run

Run each step in its own terminal where noted.

### 1. Start Cassandra and load the schema

```bash
docker run --name cassandra -p 9042:9042 -d cassandra:4.1
```

Wait about a minute for it to finish starting, then load the schema:

```bash
docker cp database/01_cloudpulse_schema.cql cassandra:/schema.cql
docker exec -it cassandra cqlsh -f /schema.cql
```

Check that it worked:

```bash
docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE cloudpulse;"
```

### 2. Start the API

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
```

The API runs at `http://127.0.0.1:8000`.

### 3. Start the agent (on each machine to monitor)

In a new terminal with the same virtual environment active:

```bash
python agent/monitor.py
```

Edit the constants at the top of `agent/monitor.py` to change the API URL, server ID, organization, or interval.

### 4. Start the dashboard

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`. Servers appear once the agent has sent its first report.

## API reference

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/metrics/ingest/` | Agent sends a batch of metrics for one server |
| GET | `/api/servers/?org_id=` | List servers in an organization |
| GET | `/api/metrics/latest/?server_id=` | Latest value of every metric for a server |
| GET | `/api/metrics/history/?server_id=&days=` | Metric history, `days` from 1 to 30 (default 7) |

Example ingest payload:

```json
{
  "server_id": "22222222-2222-4222-8222-222222222222",
  "org_id": "local",
  "hostname": "my-laptop",
  "os_name": "Windows 11",
  "metrics": [
    { "metric_type": "cpu_percent", "metric_value": 42.5, "unit": "%" }
  ]
}
```

Collected metrics: `cpu_percent`, `memory_percent`, `disk_percent`, `network_sent_bytes`, `network_recv_bytes`.

## Current status

**Working**
- Agent collection and ingestion every 10 seconds
- Time-series storage with day-bucketed partitions
- Live overview cards and historical trend charts per server

**Known limitations**
- Agent currently assumes a Windows system drive for disk usage
- One fixed server ID in the agent, so run one agent per environment until IDs are generated per machine
- No authentication; the ingest endpoint is open (development only)
- Network metrics are cumulative byte counters, not rates
- Threshold alerts run in the browser only

## Roadmap

1. Full schema: organizations, users, alert rules and events, logs, audit trail
2. Retention (TTL) and `TimeWindowCompactionStrategy` on the metrics table
3. Rollup tables (1 min / 1 hour) for fast long-range queries
4. Server-side alert engine with open/resolved lifecycle
5. Centralized log ingestion and search
6. Multi-node Cassandra cluster with replication and consistency-level experiments
7. Benchmarks: ingest throughput and query latency under increasing load

## Repository layout

```
agent/        monitoring agent (psutil)
api/          Django app: ingest and query views
backend/      Django project settings and URLs
database/     CQL schema
frontend/     React dashboard
```
