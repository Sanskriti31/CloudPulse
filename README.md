# CloudPulse

A cloud observability platform that collects server metrics and logs, stores them as time-series data in a **3-node Apache Cassandra cluster**, raises alerts on the server side, and shows everything on a live **React** dashboard.

Built for the M.Tech Database Modelling and Design Lab (MACSE518), School of Computer Science and Engineering, VIT.

**Team:** Sanskriti Singh (26MAI0045), Navya Mongia (26MAI0054), Sunidhi Raichur (26MAI0061)

---

## Contents

1. [Architecture](#architecture)
2. [Data model](#data-model)
3. [Key design decisions](#key-design-decisions)
4. [Prerequisites](#prerequisites)
5. [Setup and run](#setup-and-run)
6. [Configuration](#configuration)
7. [API reference](#api-reference)
8. [Benchmarks and results](#benchmarks-and-results)
9. [Cluster experiments](#cluster-experiments)
10. [Known limitations](#known-limitations)
11. [Roadmap](#roadmap)
12. [Repository layout](#repository-layout)

---

## Architecture

```
+------------------+  POST metrics every 10 s   +----------------------+   CQL    +---------------------------+
| Agent (psutil)   | -------------------------> | Django API           | -------> | Cassandra cluster         |
| - CPU, memory,   |  POST new log lines        | - ingest + validate  | <------- | cass1  cass2  cass3       |
|   disk, network  | -------------------------> | - alert engine       |          | RF = 3, NetworkTopology   |
| - log file tail  |                            | - history / logs API |          | LOCAL_QUORUM by default   |
| - offline buffer |                            +----------------------+          +---------------------------+
+------------------+                                ^          ^                              ^
                                                    |          |                              |
                                     GET (every 10 s)          |  rollup_metrics --loop       |
                                                    |          +------------------------------+
                                           +-----------------+   builds 1-min / 1-hour rollups
                                           | React dashboard |
                                           +-----------------+
```

| Component | Folder | Technology |
|---|---|---|
| Monitoring agent | `agent/` | Python, psutil, requests |
| API and alert engine | `api/`, `backend/` | Django, cassandra-driver |
| Rollup job | `api/rollups.py`, `api/management/commands/` | Django management command |
| Database schema | `database/` | CQL |
| Cassandra cluster | `docker-compose.yml` | Apache Cassandra 4.1, 3 nodes |
| Dashboard | `frontend/` | React, Vite, Recharts |
| Benchmarks | `bench/` | Python |

---

## Data model

The schema is **query-first**: each table answers one question the application asks. Every read names a full partition key, so there are no `ALLOW FILTERING` queries and no secondary indexes. Time-series tables are **bucketed** (by day or month) so no partition grows without limit.

### Servers and metrics (`database/01_cloudpulse_schema.cql`)

| Table | Primary key | Answers | Retention |
|---|---|---|---|
| `server_by_id` | `(server_id)` | Look up one server | – |
| `servers_by_org` | `((org_id), server_id)` | List an organization's servers | – |
| `metrics_by_server_bucket` | `((server_id, bucket_date), metric_time DESC, metric_type)` | Raw readings for a server and day | 7 days, TWCS 1-day windows |
| `latest_metric_by_server` | `((server_id), metric_type)` | Current value of each metric | – |

### Alerts (`database/02_alerts_schema.cql`)

| Table | Primary key | Answers | Retention |
|---|---|---|---|
| `alert_rules_by_org` | `((org_id), rule_id)` | Rules an organization has defined | – |
| `alert_state_by_server` | `((server_id), rule_id)` | Is a rule PENDING or OPEN on this server? | Deleted on resolve |
| `open_alerts_by_org` | `((org_id), server_id, rule_id)` | All open alerts in an organization | Deleted on resolve |
| `alert_events_by_server` | `((server_id, month_bucket), event_id DESC)` | Alert history for a server | 90 days |
| `alert_events_by_rule` | `((rule_id, month_bucket), event_id DESC)` | Alert history for a rule | 90 days |

### Rollups (`database/03_rollups_schema.cql`)

| Table | Primary key | Answers | Retention |
|---|---|---|---|
| `metrics_rollup_1m` | `((server_id, metric_type, bucket_date), window_start DESC)` | 1-minute min/max/avg for one metric and day | 30 days, TWCS 1-day windows |
| `metrics_rollup_1h` | `((server_id, metric_type, month_bucket), window_start DESC)` | 1-hour min/max/avg for one metric and month | 365 days, TWCS 30-day windows |

### Logs (`database/04_logs_schema.cql`)

| Table | Primary key | Answers | Retention |
|---|---|---|---|
| `logs_by_server_day` | `((server_id, bucket_date), log_time DESC, seq DESC, log_id DESC)` | All logs for a server and day | 14 days, TWCS 1-day windows |
| `logs_by_server_level_day` | `((server_id, level, bucket_date), log_time DESC, seq DESC, log_id DESC)` | Only one level (e.g. ERROR) for a server and day | 14 days, TWCS 1-day windows |

**13 tables in total.** The keyspace uses `NetworkTopologyStrategy` with **replication factor 3**.

### Partition sizes

- Raw metrics: 5 metrics × 8,640 readings = **43,200 rows per server per day**, about 2–4 MB per partition, well under the ~100 MB guideline.
- 1-minute rollups: 1,440 rows per partition.
- 1-hour rollups: up to 744 rows per partition.

---

## Key design decisions

**Write path**
- **One single-partition batch per ingest.** All 5 metric rows share `(server_id, bucket_date)`, so an `UNLOGGED BATCH` is a single write. The two copies of the server row (`server_by_id`, `servers_by_org`) are written in a `LOGGED BATCH` so they never disagree.
- **Prepared statements** everywhere: Cassandra parses each query once.
- **LWT only once.** `IF NOT EXISTS` (a Paxos round) runs only the first time a server is seen, not on every report.
- **Idempotent retries.** The agent sends the time each sample was taken (`collected_at`), which becomes part of the primary key, so a retried report overwrites the same row instead of duplicating it.
- **Newest value always wins.** `latest_metric_by_server` is written `USING TIMESTAMP <sample time>`, so a late, retried sample can't overwrite a newer one.
- **Offline buffer.** The agent keeps up to 120 reports while the API is down and replays them, oldest first, when it's back.

**Reads**
- **Rollups chosen by range.** Today reads raw data, 7 days reads 1-minute rollups, and 30 days reads 1-hour rollups. A 30-day chart reads about 3,600 rows instead of about 1.3 million.
- **Write-optimised vs read-optimised partitions.** Raw data is partitioned per server-day (all metrics written together); rollups per server-metric (one metric read per chart). This trade-off is measured in `bench/results/partition.csv`.
- **Health worked out on read.** UP (report within 30 s), STALE (within 2 min) or DOWN, from `last_seen`. Nothing has to write "DOWN".
- **Native paging for logs.** Cassandra's paging state is returned as a `next_page` token. Pages stay fast however deep you go.

**Alerts**
- Rules support a duration ("CPU > 90% for 30 s") via a PENDING state, so short spikes don't fire.
- Opening and resolving an alert each update four tables in one `LOGGED BATCH`.
- The resolve `UPDATE` sets the **remaining** TTL, so the whole history row expires together.
- Disabled or deleted rules automatically close their open alerts.

**Logs**
- A second table partitioned by level answers "show only errors" from one partition.
- `log_id` is a deterministic `timeuuid` (log time + hash of the line), so retries don't duplicate lines. `seq` keeps lines with the same timestamp in file order.

**Retention and compaction**
- Tiered TTLs: 7 days raw, 30 days at 1 minute, 1 year at 1 hour, 14 days of logs, 90 days of alert history.
- `TimeWindowCompactionStrategy` on write-once time-series tables, so expired days are dropped as whole files. The alert history tables keep the default strategy, because their rows are updated when an alert resolves.

---

## Prerequisites

- **Windows, macOS or Linux** (developed on Windows 11)
- **Python 3.12+**
- **Node.js 20+** and npm
- **Docker Desktop** with at least **4 GB** of memory for Docker (3 Cassandra nodes with a 512 MB heap each)

On Windows with the WSL2 backend, set Docker's memory in `C:\Users\<you>\.wslconfig`:

```
[wsl2]
memory=6GB
```

Then run `wsl --shutdown` and restart Docker Desktop.

---

## Setup and run

Run each long-running step in its own terminal.

### 1. Start the Cassandra cluster

```powershell
docker compose up -d
docker exec cass1 nodetool status
```

The first start takes **4–6 minutes** because nodes join one at a time. Repeat `nodetool status` until all three lines start with `UN` (Up, Normal).

### 2. Load the schema (first time only)

```powershell
docker cp database/01_cloudpulse_schema.cql cass1:/01.cql
docker cp database/02_alerts_schema.cql cass1:/02.cql
docker cp database/03_rollups_schema.cql cass1:/03.cql
docker cp database/04_logs_schema.cql cass1:/04.cql
docker exec cass1 cqlsh -f /01.cql
docker exec cass1 cqlsh -f /02.cql
docker exec cass1 cqlsh -f /03.cql
docker exec cass1 cqlsh -f /04.cql
```

Run `02_alerts_schema.cql` only once: it also inserts three starter alert rules, and running it again would add duplicates.

### 3. Start the API

```powershell
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver
```

The API runs at `http://127.0.0.1:8000`. On start-up it prints:

```
CloudPulse: connected to Cassandra ['127.0.0.1'], consistency LOCAL_QUORUM
```

### 4. Start the rollup job

In a new terminal with the venv active:

```powershell
python manage.py rollup_metrics --backfill-hours 24   # optional, first time
python manage.py rollup_metrics --loop                # keeps running, once a minute
```

### 5. Start the agent (on each machine to monitor)

```powershell
python agent/monitor.py
```

To also send a log file:

```powershell
python agent/monitor.py --log-file path\to\app.log
```

The agent creates its server ID on first run and saves it in `agent/.server_id`. To send to an API on another machine:

```powershell
python agent/monitor.py --api-url http://<api-ip>:8000/api/metrics/ingest/ --hostname my-laptop
```

The API machine must then run `python manage.py runserver 0.0.0.0:8000` and list its IP in `ALLOWED_HOSTS`.

### 6. Start the dashboard

```powershell
cd frontend
npm install
npm run dev
```

Open **http://localhost:5173**. Use `localhost`, not `127.0.0.1`, because that's the origin allowed by CORS.

### Stopping and restarting

```powershell
docker compose stop      # stop the cluster, keep the data
docker compose start     # start it again (about 1-2 minutes)
docker compose down -v   # remove the cluster AND all data
```

---

## Configuration

### API (`backend/settings.py`)

| Setting | Default | Meaning |
|---|---|---|
| `CASSANDRA_HOSTS` | `["127.0.0.1"]` | Contact point (cass1, published on port 9042) |
| `CASSANDRA_PORT` | `9042` | Native protocol port |
| `CASSANDRA_KEYSPACE` | `cloudpulse` | Keyspace |
| `CASSANDRA_CONSISTENCY` | `LOCAL_QUORUM` | Read/write consistency level. Override per run with the environment variable of the same name. |

Example, running the API at consistency level ONE:

```powershell
$env:CASSANDRA_CONSISTENCY = "ONE"; python manage.py runserver
```

### Agent (`agent/monitor.py`)

| Option | Environment variable | Default |
|---|---|---|
| `--api-url` | `CLOUDPULSE_API_URL` | `http://127.0.0.1:8000/api/metrics/ingest/` |
| `--logs-url` | `CLOUDPULSE_LOGS_URL` | Derived from `--api-url` |
| `--log-file` | `CLOUDPULSE_LOG_FILE` | None (only agent events are sent) |
| `--org` | `CLOUDPULSE_ORG_ID` | `local` |
| `--interval` | `CLOUDPULSE_INTERVAL` | `10` seconds (5–300) |
| `--id-file` | `CLOUDPULSE_ID_FILE` | `agent/.server_id` |
| `--hostname` | `CLOUDPULSE_HOSTNAME` | The machine's hostname |

Collected metrics: `cpu_percent`, `memory_percent`, `disk_percent`, `network_sent_bps`, `network_recv_bps`.

---

## API reference

All responses are JSON with `"status": "success"` or `"status": "error"` and a `message`.

### Metrics and servers

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/metrics/ingest/` | Agent sends one batch of metrics |
| GET | `/api/servers/?org_id=local` | Servers in an organization, with `health_status` (UP / STALE / DOWN) and `seconds_since_seen` |
| GET | `/api/metrics/latest/?server_id=` | Latest value of every metric |
| GET | `/api/metrics/history/?server_id=&days=&resolution=` | History. `days` 1–365 (default 1). `resolution` = `auto` (default), `raw` (max 7 days), `1m` (max 30 days) or `1h` (max 365 days). Returns `partitions_read` and `query_ms`. |

Ingest payload:

```json
{
  "server_id": "22222222-2222-4222-8222-222222222222",
  "org_id": "local",
  "hostname": "my-laptop",
  "os_name": "Windows 11",
  "collected_at": "2026-10-25T11:07:58.379+00:00",
  "metrics": [
    { "metric_type": "cpu_percent", "metric_value": 42.5, "unit": "%" }
  ]
}
```

`collected_at` is optional. If it's missing, unreadable or in the future, the server's time is used. Samples older than 10 minutes are rejected with 400.

### Alerts

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/alerts/rules/?org_id=local` | List rules |
| POST | `/api/alerts/rules/` | Create a rule (rule names are unique per organization; a duplicate returns 409) |
| GET | `/api/alerts/open/?org_id=local` | Open alerts, critical first |
| GET | `/api/alerts/history/?server_id=` or `?rule_id=` | History, newest first. Optional `months` (1–6) and `limit` (1–500). |
| POST | `/api/alerts/resolve/` | Resolve an open alert by hand |

Create-rule payload:

```json
{
  "org_id": "local",
  "rule_name": "High CPU",
  "metric_type": "cpu_percent",
  "operator": ">",
  "threshold": 90,
  "duration_seconds": 30,
  "severity": "CRITICAL"
}
```

`operator` is `>`, `>=`, `<` or `<=`. `severity` is `WARNING` or `CRITICAL`. `duration_seconds` is 0–3600. An optional `server_id` limits the rule to one server.

Resolve payload: `{"server_id": "...", "rule_id": "...", "resolved_by": "admin"}`

### Logs

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/logs/ingest/` | Agent sends up to 200 log lines |
| GET | `/api/logs/?server_id=&date=&level=&q=&limit=&page=` | Logs for a server and day, newest first. `date` YYYY-MM-DD (default today, UTC). `level` DEBUG / INFO / WARNING / ERROR. `q` filters messages on the returned page. `limit` 1–500 (default 100). Pass the response's `next_page` as `page` for the next page. |

Log ingest payload:

```json
{
  "server_id": "22222222-2222-4222-8222-222222222222",
  "logs": [
    { "ts": "2026-10-25T14:38:57.480+00:00", "level": "ERROR",
      "source": "app.log", "message": "database connection refused", "seq": 17 }
  ]
}
```

`WARN` is stored as `WARNING`, and `CRITICAL` / `FATAL` as `ERROR`. Messages longer than 2,000 characters are cut.

---

## Benchmarks and results

Two scripts in `bench/` produce every number below. Results are appended to `bench/results/*.csv`. Benchmark data uses org `bench`, so the dashboard isn't affected.

```powershell
python bench/db_bench.py seed          # 2 raw days + 30 days of rollups (~390,000 rows)
python bench/db_bench.py writes        # write-path strategies
python bench/db_bench.py consistency   # ONE vs LOCAL_QUORUM vs ALL
python bench/db_bench.py reads         # raw vs 1-minute vs 1-hour
python bench/db_bench.py partition     # per server-day vs per server-metric-day
python bench/load_agents.py --agents 100 --duration 60
python bench/db_bench.py cleanup       # remove benchmark data
```

Measured on one Windows 11 laptop running all 3 nodes, at `LOCAL_QUORUM`.

### Write path

| Strategy | Round trips | p50 | Ingests/s |
|---|---|---|---|
| Original: string queries + 2 LWTs | 14 sequential | 219.7 ms | 4.6 |
| Prepared, no batching | 12 sequential | 188.3 ms | 5.4 |
| **Prepared + 3 async batches** | **3 parallel** | **15.6 ms** | **63.6** |
| Same, 32 ingests in flight | – | – | **851** |

### History reads

| Resolution | Range | Partitions | Rows read | p50 |
|---|---|---|---|---|
| raw | 1 day | 1 | 43,200 | 844 ms |
| raw | 2 days | 2 | 86,400 | 1,719 ms |
| 1-minute | 7 days | 35 | 50,400 | 220 ms |
| 1-minute | 30 days | 150 | 216,000 | 938 ms |
| **1-hour** | **30 days** | **10** | **3,530** | **29.8 ms** |

### Partition design

| Query | Design | Partitions | Rows read | p50 |
|---|---|---|---|---|
| CPU only, 1 day | per server-day (current raw table) | 1 | 43,200 | 352.9 ms |
| CPU only, 1 day | per server-metric-day | 1 | 8,640 | 64.6 ms |
| All 5 metrics, 1 day | per server-day | 1 | 43,200 | 394.5 ms |
| All 5 metrics, 1 day | per server-metric-day | 5 | 43,200 | 252.4 ms |

Raw data keeps the per server-day design because each ingest is then **one** single-partition write. Rollups, which are read one metric at a time, use per-metric partitions.

### Load through the API

| Simulated agents (1 report/s each) | Offered req/s | Achieved req/s | p50 | p95 | Errors |
|---|---|---|---|---|---|
| 10 | 10 | 9.9 | 30.3 ms | 47.9 ms | 0% |
| 50 | 50 | 49.2 | 28.4 ms | 49.8 ms | 0% |
| 100 | 100 | 98.4 | 24.3 ms | 39.3 ms | 0% |
| 200 | 200 | 196.7 | 33.6 ms | 209.8 ms | 0% |
| 300 | 300 | 188.3 | 1,278.8 ms | 2,541.1 ms | 0% |
| 400 | 400 | 288.7 | 1,297.0 ms | 2,104.9 ms | 0.02% |
| 100, **cass3 stopped** | 100 | 98.4 | 24.3 ms | 41.7 ms | **0%** |

- Each simulated agent reports every second, 10× faster than a real agent, so **200 simulated agents ≈ 2,000 real agents**.
- The API saturates at **about 190–290 requests per second**. The bottleneck is Django's single-process development server, not Cassandra, which handles 851 ingests per second directly.
- With one node stopped, there are **no errors and almost no latency change**.

### Fault tolerance (consistency levels with one node down)

| Consistency | 3 nodes up | 1 node down |
|---|---|---|
| ONE | ✅ | ✅ |
| LOCAL_QUORUM | ✅ | ✅ |
| ALL | ✅ | ❌ Unavailable (reported as `NoHostAvailable`) |

### Measurement note

On Windows, the Cassandra driver's network loop wakes up on the system timer tick of about **15.6 ms**, so each round trip from the laptop is rounded up to one tick. That's why the sequential strategies take almost exactly 14 × 15.6 ms and 12 × 15.6 ms, and why ONE, LOCAL_QUORUM and ALL all measured about 15.6 ms.

- **Still valid:** round-trip counts, throughput, rows read and fault-tolerance results.
- **Not conclusive at this resolution:** the latency differences **between consistency levels**. They should be re-measured from a Linux client on the cluster's network.

### Seeding

Bulk writes reached **10,367 rows per second** (390,000 rows in 37.9 s).

---

## Cluster experiments

```powershell
# Cluster state: 3 x UN expected
docker exec cass1 nodetool status

# Which nodes hold a partition (all 3 with RF = 3)
docker exec cass1 nodetool getendpoints cloudpulse latest_metric_by_server <server_id>

# Consistency levels and tracing
docker exec -it cass1 cqlsh
#   CONSISTENCY QUORUM;  TRACING ON;  SELECT ... ;

# One node down: QUORUM keeps working, ALL fails
docker stop cass3
docker start cass3

# Two nodes down: writes fail, the agent buffers, then replays with no data lost
docker stop cass2
docker stop cass3
docker start cass2
docker start cass3

# Re-sync replicas after a long outage
docker exec cass1 nodetool repair cloudpulse
```

**Consistency rule:** with replication factor RF, writing at W and reading at R gives strong consistency when **R + W > RF**. QUORUM + QUORUM = 2 + 2 > 3.

---

## Known limitations

- **No authentication.** The ingest, rule and resolve endpoints are open. Development settings are still in place (`DEBUG = True`, hardcoded `SECRET_KEY`).
- **One coordinator from Windows.** Docker Desktop doesn't expose the containers' internal IPs to the host, so the driver uses only `cass1` (`WhiteListRoundRobinPolicy`), and stopping `cass1` stops the app. In a real deployment, the app runs on the cluster's network, lists all nodes and uses `TokenAwarePolicy`.
- **Development web server.** `runserver` limits API throughput to about 200–300 requests per second.
- **Log search** filters one page at a time. Full-text search would need Cassandra 5's SAI indexes or a search engine.
- **Benchmark timing on Windows** is rounded to about 15.6 ms per round trip (see the measurement note).
- **Rule cache.** Alert rules are cached for 30 seconds in each Django process, so a rule change takes up to 30 seconds to apply.
- **No automated tests** in `api/tests.py`.

---

## Roadmap

1. ~~Server-side alert engine~~ (done)
2. ~~Retention (TTL) and TimeWindowCompactionStrategy~~ (done)
3. ~~1-minute and 1-hour rollups~~ (done)
4. ~~3-node cluster, replication and consistency experiments~~ (done)
5. ~~Benchmarks: ingest throughput and query latency under load~~ (done)
6. ~~Log ingestion and filtering~~ (done)
7. Per-agent API tokens, Admin/Viewer roles and an audit log
8. Re-run the consistency benchmarks from a Linux container on the cluster network
9. Production deployment: gunicorn/uvicorn behind a reverse proxy, settings from environment variables

---

## Repository layout

```
agent/
  monitor.py              monitoring agent (metrics, log tailing, offline buffer)
api/
  views.py                ingest, history, alert engine, alerts and logs API
  rollups.py              1-minute and 1-hour rollup builder
  management/commands/
    rollup_metrics.py     python manage.py rollup_metrics [--loop] [--backfill-hours N]
backend/
  settings.py             Django and Cassandra settings
  urls.py                 API routes
bench/
  db_bench.py             database benchmarks (direct to Cassandra)
  load_agents.py          load generator (simulated agents through the API)
  results/                benchmark results (CSV)
database/
  01_cloudpulse_schema.cql   keyspace, servers, raw and latest metrics
  02_alerts_schema.cql       alert tables and 3 starter rules
  03_rollups_schema.cql      1-minute and 1-hour rollups
  04_logs_schema.cql         log tables
frontend/
  src/App.jsx             dashboard
  src/App.css, index.css  styles
docker-compose.yml        3-node Cassandra cluster
requirements.txt          Python dependencies
```