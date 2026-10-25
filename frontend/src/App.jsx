import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";

const API_BASE = "http://127.0.0.1:8000/api";
const ORG_ID = "local";

const PERCENT_METRICS = [
  { key: "cpu_percent", label: "CPU", hint: "Average over the last 10 seconds" },
  { key: "memory_percent", label: "Memory", hint: "RAM in use" },
  { key: "disk_percent", label: "Disk", hint: "System drive used" },
];

const RATE_METRICS = [
  { key: "network_sent_bps", label: "Upload", hint: "Send rate, 10-second average" },
  { key: "network_recv_bps", label: "Download", hint: "Receive rate, 10-second average" },
];

const CHARTS = [
  { key: "cpu_percent", title: "CPU", color: "#1f5fd1" },
  { key: "memory_percent", title: "Memory", color: "#7a3fd1" },
  { key: "disk_percent", title: "Disk", color: "#0b7a75" },
];

// Plain names for metric types shown in alerts.
const METRIC_LABELS = {
  cpu_percent: "CPU",
  memory_percent: "Memory",
  disk_percent: "Disk",
  network_sent_bps: "Upload",
  network_recv_bps: "Download",
};

const RANGES = [
  { days: 1, label: "Today" },
  { days: 7, label: "7 days" },
  { days: 30, label: "30 days" },
];

const RESOLUTION_TEXT = {
  raw: "raw readings every 10 seconds",
  "1m": "1-minute averages",
  "1h": "1-hour averages",
};

// ---------------------------------------------------------------- helpers

function formatBytes(value) {
  if (value === undefined || value === null) return null;
  let size = Number(value);
  if (!Number.isFinite(size)) return null;
  if (size < 0) size = 0;

  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index++;
  }
  return { number: size.toFixed(index === 0 ? 0 : 1), unit: units[index] };
}

function secondsAgo(seconds) {
  if (seconds === null || seconds === undefined) return "never";
  if (seconds < 60) return `${seconds} s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min ago`;
  return `${Math.round(seconds / 3600)} h ago`;
}

async function getJson(url, options) {
  const response = await fetch(url, options);
  let data = {};
  try {
    data = await response.json();
  } catch {
    // Not JSON: fall through to the status check.
  }
  if (!response.ok || data.status !== "success") {
    throw new Error(data.message || `Request failed (${response.status})`);
  }
  return data;
}

// Runs `task(isActive)` now and every `intervalMs`, restarting when deps change.
function usePolling(task, intervalMs, deps) {
  useEffect(() => {
    let active = true;
    const run = () => task(() => active);
    run();
    const timer = setInterval(run, intervalMs);
    return () => {
      active = false;
      clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);
}

// ---------------------------------------------------------------- pieces

function BrandMark() {
  return (
    <svg className="brand__mark" viewBox="0 0 36 36" aria-hidden="true">
      <rect width="36" height="36" rx="8" fill="#1d3a66" />
      <path
        d="M5 20h6l3-8 5 14 4-10 2 4h6"
        fill="none"
        stroke="#7dd3fc"
        strokeWidth="2.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function Gauge({ value, limit }) {
  if (value === null) return <div className="gauge" />;
  const width = Math.max(0, Math.min(100, value));
  let tone = "";
  if (limit !== undefined) {
    if (value > limit) tone = "gauge__fill--over";
    else if (value >= limit - 10) tone = "gauge__fill--near";
  }
  return (
    <div
      className="gauge"
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(value)}
    >
      <div className={`gauge__fill ${tone}`} style={{ width: `${width}%` }} />
      {limit !== undefined && (
        <div className="gauge__limit" style={{ left: `calc(${limit}% - 1px)` }} />
      )}
    </div>
  );
}

// ---------------------------------------------------------------- app

function App() {
  // Servers
  const [servers, setServers] = useState([]);
  const [selectedServerId, setSelectedServerId] = useState("");
  const [serverError, setServerError] = useState("");

  // Live metrics
  const [metrics, setMetrics] = useState([]);
  const [metricsLoading, setMetricsLoading] = useState(true);
  const [metricsError, setMetricsError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);
  const [thresholds, setThresholds] = useState({});

  // History
  const [historyDays, setHistoryDays] = useState(1);
  const [historyMetrics, setHistoryMetrics] = useState([]);
  const [historyMeta, setHistoryMeta] = useState(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");

  // Alerts
  const [openAlerts, setOpenAlerts] = useState([]);
  const [alertHistory, setAlertHistory] = useState([]);
  const [alertError, setAlertError] = useState("");
  const [resolvingId, setResolvingId] = useState("");
  const [alertRefresh, setAlertRefresh] = useState(0);

  // Logs
  const [logs, setLogs] = useState([]);
  const [logLevel, setLogLevel] = useState("");
  const [logSearch, setLogSearch] = useState("");
  const [logSearchInput, setLogSearchInput] = useState("");
  const [logNextPage, setLogNextPage] = useState(null);
  const [logError, setLogError] = useState("");
  const [logLoadingOlder, setLogLoadingOlder] = useState(false);
  const [viewingOlder, setViewingOlder] = useState(false);
  const viewingOlderRef = useRef(false);

  // ---- servers (every 10 s)
  usePolling(async (isActive) => {
    try {
      const data = await getJson(`${API_BASE}/servers/?org_id=${ORG_ID}`);
      if (!isActive()) return;
      const list = data.servers || [];
      setServers(list);
      setSelectedServerId((current) =>
        list.some((item) => item.server_id === current)
          ? current
          : list[0]?.server_id || ""
      );
      setServerError("");
    } catch (err) {
      if (isActive()) setServerError(err.message);
    }
  }, 10000, []);

  // ---- alert thresholds for the gauges (every 60 s)
  usePolling(async (isActive) => {
    try {
      const data = await getJson(`${API_BASE}/alerts/rules/?org_id=${ORG_ID}`);
      if (!isActive()) return;
      const limits = {};
      for (const rule of data.rules || []) {
        if (!rule.enabled || ![">", ">="].includes(rule.operator)) continue;
        if (rule.server_id && rule.server_id !== selectedServerId) continue;
        const current = limits[rule.metric_type];
        if (current === undefined || rule.threshold < current) {
          limits[rule.metric_type] = rule.threshold;
        }
      }
      setThresholds(limits);
    } catch {
      // Thresholds only decorate the gauges; ignore failures.
    }
  }, 60000, [selectedServerId]);

  // ---- latest metrics (every 10 s)
  usePolling(async (isActive) => {
    if (!selectedServerId) {
      setMetrics([]);
      setMetricsLoading(false);
      return;
    }
    try {
      const data = await getJson(
        `${API_BASE}/metrics/latest/?server_id=${encodeURIComponent(selectedServerId)}`
      );
      if (!isActive()) return;
      setMetrics(data.metrics || []);
      setLastUpdated(new Date());
      setMetricsError("");
    } catch (err) {
      if (isActive()) setMetricsError(err.message);
    } finally {
      if (isActive()) setMetricsLoading(false);
    }
  }, 10000, [selectedServerId]);

  // ---- history (every 30 s)
  useEffect(() => {
    setHistoryMetrics([]);
    setHistoryMeta(null);
    setHistoryLoading(true);
  }, [selectedServerId, historyDays]);

  usePolling(async (isActive) => {
    if (!selectedServerId) {
      setHistoryLoading(false);
      return;
    }
    try {
      const data = await getJson(
        `${API_BASE}/metrics/history/?server_id=${encodeURIComponent(
          selectedServerId
        )}&days=${historyDays}`
      );
      if (!isActive()) return;
      setHistoryMetrics(data.metrics || []);
      setHistoryMeta({
        resolution: data.resolution,
        count: data.count,
        partitions: data.partitions_read,
        ms: data.query_ms,
      });
      setHistoryError("");
    } catch (err) {
      if (isActive()) setHistoryError(err.message);
    } finally {
      if (isActive()) setHistoryLoading(false);
    }
  }, 30000, [selectedServerId, historyDays]);

  // ---- alerts (every 10 s, and straight after a resolve)
  usePolling(async (isActive) => {
    if (!selectedServerId) return;
    try {
      const [openData, historyData] = await Promise.all([
        getJson(`${API_BASE}/alerts/open/?org_id=${ORG_ID}`),
        getJson(
          `${API_BASE}/alerts/history/?server_id=${encodeURIComponent(
            selectedServerId
          )}&limit=10`
        ),
      ]);
      if (!isActive()) return;
      setOpenAlerts(
        (openData.alerts || []).filter((alert) => alert.server_id === selectedServerId)
      );
      setAlertHistory(historyData.events || []);
      setAlertError("");
    } catch (err) {
      if (isActive()) setAlertError(err.message);
    }
  }, 10000, [selectedServerId, alertRefresh]);

  async function resolveAlert(alert) {
    setResolvingId(alert.event_id);
    try {
      await getJson(`${API_BASE}/alerts/resolve/`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          server_id: alert.server_id,
          rule_id: alert.rule_id,
          resolved_by: "dashboard",
        }),
      });
      setAlertRefresh((n) => n + 1);
    } catch (err) {
      setAlertError(err.message);
    } finally {
      setResolvingId("");
    }
  }

  // ---- logs (first page every 10 s; paused while reading older pages)
  function logsUrl(page) {
    const params = new URLSearchParams({ server_id: selectedServerId, limit: "50" });
    if (logLevel) params.set("level", logLevel);
    if (logSearch) params.set("q", logSearch);
    if (page) params.set("page", page);
    return `${API_BASE}/logs/?${params.toString()}`;
  }

  function setOlder(value) {
    viewingOlderRef.current = value;
    setViewingOlder(value);
  }

  useEffect(() => {
    setLogs([]);
    setLogNextPage(null);
    setOlder(false);
  }, [selectedServerId, logLevel, logSearch]);

  usePolling(async (isActive) => {
    if (!selectedServerId || viewingOlderRef.current) return;
    try {
      const data = await getJson(logsUrl(null));
      if (!isActive() || viewingOlderRef.current) return;
      setLogs(data.logs || []);
      setLogNextPage(data.next_page);
      setLogError("");
    } catch (err) {
      if (isActive()) setLogError(err.message);
    }
  }, 10000, [selectedServerId, logLevel, logSearch]);

  async function loadOlderLogs() {
    if (!logNextPage) return;
    setLogLoadingOlder(true);
    try {
      const data = await getJson(logsUrl(logNextPage));
      setOlder(true);
      setLogs((current) => [...current, ...(data.logs || [])]);
      setLogNextPage(data.next_page);
    } catch (err) {
      setLogError(err.message);
    } finally {
      setLogLoadingOlder(false);
    }
  }

  async function backToLatestLogs() {
    setOlder(false);
    try {
      const data = await getJson(logsUrl(null));
      setLogs(data.logs || []);
      setLogNextPage(data.next_page);
      setLogError("");
    } catch (err) {
      setLogError(err.message);
    }
  }

  // ---- derived values
  const metricMap = useMemo(
    () => Object.fromEntries(metrics.map((m) => [m.metric_type, m])),
    [metrics]
  );

  function numeric(key) {
    const value = metricMap[key]?.metric_value;
    return value === undefined || value === null ? null : Number(value);
  }

  const historyChartData = useMemo(() => {
    const byTimestamp = new Map();
    for (const item of historyMetrics) {
      if (!item.recorded_at) continue;
      if (!byTimestamp.has(item.recorded_at)) {
        const date = new Date(item.recorded_at);
        byTimestamp.set(item.recorded_at, {
          recorded_at: item.recorded_at,
          time:
            historyDays > 1
              ? date.toLocaleString([], {
                  month: "short",
                  day: "numeric",
                  hour: "2-digit",
                  minute: "2-digit",
                })
              : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        });
      }
      if (CHARTS.some((chart) => chart.key === item.metric_type)) {
        byTimestamp.get(item.recorded_at)[item.metric_type] = Number(item.metric_value);
      }
    }
    const sorted = [...byTimestamp.values()].sort((a, b) =>
      a.recorded_at.localeCompare(b.recorded_at)
    );
    const step = Math.max(1, Math.ceil(sorted.length / 150));   // keep charts light
    return sorted.filter((_, i) => i % step === 0 || i === sorted.length - 1);
  }, [historyMetrics, historyDays]);

  const server = servers.find((item) => item.server_id === selectedServerId);
  const health = (server?.health_status || "UNKNOWN").toLowerCase();
  const connectionError = serverError || metricsError;

  // ---------------------------------------------------------------- view
  return (
    <div className="app">
      <header className="topbar">
        <div className="wrap topbar__inner">
          <div className="brand">
            <BrandMark />
            <div>
              <div className="brand__name">CloudPulse</div>
              <div className="brand__tag">Live server monitoring on Apache Cassandra</div>
            </div>
          </div>

          <div className="server-picker">
            <label htmlFor="server-select">Server</label>
            <select
              id="server-select"
              value={selectedServerId}
              onChange={(event) => setSelectedServerId(event.target.value)}
            >
              {servers.length === 0 && <option value="">No servers reporting yet</option>}
              {servers.map((item) => (
                <option key={item.server_id} value={item.server_id}>
                  {item.hostname} ({item.health_status}) – {item.server_id.slice(0, 8)}
                </option>
              ))}
            </select>
          </div>
        </div>
      </header>

      {server && (
        <div className="status-strip">
          <div className="wrap status-strip__inner">
            <span className={`status status--${health}`}>
              {server.health_status || "UNKNOWN"}
            </span>
            <div className="fact">
              <span className="fact__label">Hostname</span>
              <span className="fact__value">{server.hostname || "Unknown"}</span>
            </div>
            <div className="fact">
              <span className="fact__label">Operating system</span>
              <span className="fact__value">{server.os_name || "Unknown"}</span>
            </div>
            <div className="fact">
              <span className="fact__label">Last report</span>
              <span className="fact__value">{secondsAgo(server.seconds_since_seen)}</span>
            </div>
            <span className="refreshed">
              {lastUpdated
                ? `Dashboard refreshed at ${lastUpdated.toLocaleTimeString()}`
                : "Waiting for the first reading"}
            </span>
          </div>
        </div>
      )}

      <main className="wrap">
        {connectionError && (
          <div className="banner" role="alert">
            <strong>Can't reach the CloudPulse API</strong>
            {connectionError}. Check that Django is running on port 8000 and the
            Cassandra cluster is up.
          </div>
        )}

        {!connectionError && servers.length === 0 && (
          <div className="panel section">
            <p className="empty">
              No servers are reporting yet. Start an agent with{" "}
              <strong>python agent/monitor.py</strong> and this page fills in within
              10 seconds.
            </p>
          </div>
        )}

        {/* ---------------- Live metrics ---------------- */}
        <section className="section" aria-labelledby="live-title">
          <div className="section__head">
            <div>
              <h2 className="section__title" id="live-title">Right now</h2>
              <p className="section__note">
                Updated every 10 seconds. The black mark on each bar is the alert
                threshold.
              </p>
            </div>
          </div>

          <div className="metrics">
            {PERCENT_METRICS.map((item) => {
              const value = numeric(item.key);
              const limit = thresholds[item.key];
              const over = value !== null && limit !== undefined && value > limit;
              return (
                <article key={item.key} className={`metric ${over ? "metric--over" : ""}`}>
                  <div className="metric__label">{item.label}</div>
                  <div className="metric__value">
                    {value === null ? (metricsLoading ? "…" : "–") : value.toFixed(1)}
                    {value !== null && <span className="metric__unit">%</span>}
                  </div>
                  <Gauge value={value} limit={limit} />
                  <div className="metric__hint">
                    {limit !== undefined
                      ? `${item.hint}. Alert above ${limit}%.`
                      : item.hint}
                  </div>
                </article>
              );
            })}

            {RATE_METRICS.map((item) => {
              const formatted = formatBytes(numeric(item.key));
              return (
                <article key={item.key} className="metric">
                  <div className="metric__label">{item.label}</div>
                  <div className="metric__value">
                    {formatted ? formatted.number : metricsLoading ? "…" : "–"}
                    {formatted && <span className="metric__unit">{formatted.unit}/s</span>}
                  </div>
                  <div className="metric__hint">{item.hint}</div>
                </article>
              );
            })}
          </div>
        </section>

        {/* ---------------- History ---------------- */}
        <section className="section" aria-labelledby="history-title">
          <div className="section__head">
            <div>
              <h2 className="section__title" id="history-title">History</h2>
              <p className="section__note">
                {historyMeta
                  ? `Showing ${RESOLUTION_TEXT[historyMeta.resolution] || historyMeta.resolution}.`
                  : "CPU, memory and disk over time."}
              </p>
            </div>
            <div className="segmented" role="group" aria-label="Time range">
              {RANGES.map((range) => (
                <button
                  key={range.days}
                  type="button"
                  aria-pressed={historyDays === range.days}
                  onClick={() => setHistoryDays(range.days)}
                >
                  {range.label}
                </button>
              ))}
            </div>
          </div>

          {historyError && <p className="error-text">Couldn't load history: {historyError}</p>}

          {historyChartData.length > 0 ? (
            <div className="charts">
              {CHARTS.map((chart) => (
                <article key={chart.key} className="chart">
                  <h3 className="chart__title">{chart.title}</h3>
                  <div className="chart__box">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart
                        data={historyChartData}
                        margin={{ top: 6, right: 8, bottom: 0, left: -8 }}
                      >
                        <CartesianGrid stroke="#e6ebf1" vertical={false} />
                        <XAxis
                          dataKey="time"
                          tick={{ fontSize: 12, fill: "#3f4a5c" }}
                          tickLine={false}
                          axisLine={{ stroke: "#d3dae4" }}
                          minTickGap={28}
                        />
                        <YAxis
                          domain={[0, 100]}
                          unit="%"
                          width={48}
                          tick={{ fontSize: 12, fill: "#3f4a5c" }}
                          tickLine={false}
                          axisLine={false}
                        />
                        <Tooltip
                          formatter={(value) => [`${Number(value).toFixed(1)}%`, chart.title]}
                          labelFormatter={(label) => label}
                          contentStyle={{
                            borderRadius: 8,
                            border: "1px solid #d3dae4",
                            color: "#172033",
                          }}
                        />
                        <Line
                          type="monotone"
                          dataKey={chart.key}
                          stroke={chart.color}
                          strokeWidth={2}
                          dot={false}
                          isAnimationActive={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="panel">
              <p className="empty">
                {historyLoading
                  ? "Loading history…"
                  : "No readings for this range yet. Keep the agent running and they appear here."}
              </p>
            </div>
          )}

          {historyMeta && (
            <p className="query-info">
              Read {historyMeta.count.toLocaleString()} rows from {historyMeta.partitions}{" "}
              Cassandra partition{historyMeta.partitions === 1 ? "" : "s"} in{" "}
              {historyMeta.ms} ms.
            </p>
          )}
        </section>

        {/* ---------------- Alerts and logs ---------------- */}
        <div className="section split">
          <section className="panel" aria-labelledby="alerts-title">
            <div className="panel__head">
              <h2 className="panel__title" id="alerts-title">Alerts</h2>
            </div>

            {alertError && <p className="error-text">{alertError}</p>}

            {openAlerts.length > 0 ? (
              openAlerts.map((alert) => {
                const critical = alert.severity === "CRITICAL";
                return (
                  <div
                    key={alert.event_id}
                    className={`alert ${critical ? "alert--critical" : "alert--warning"}`}
                  >
                    <div>
                      <div className="alert__name">
                        {alert.rule_name} ({alert.severity.toLowerCase()})
                      </div>
                      <div className="alert__detail">
                        {METRIC_LABELS[alert.metric_type] || alert.metric_type} reached{" "}
                        {Number(alert.triggered_value).toFixed(1)} (limit {alert.threshold}).
                        Open since{" "}
                        {new Date(alert.opened_at).toLocaleTimeString()}.
                      </div>
                    </div>
                    <button
                      type="button"
                      className="btn"
                      onClick={() => resolveAlert(alert)}
                      disabled={resolvingId === alert.event_id}
                    >
                      {resolvingId === alert.event_id ? "Resolving…" : "Resolve"}
                    </button>
                  </div>
                );
              })
            ) : (
              <p className="empty">No open alerts for this server.</p>
            )}

            <h3 className="subhead">Recent alerts</h3>
            {alertHistory.length > 0 ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Opened</th>
                      <th>Metric</th>
                      <th>Value</th>
                      <th>Status</th>
                      <th>Resolved by</th>
                    </tr>
                  </thead>
                  <tbody>
                    {alertHistory.map((event) => (
                      <tr key={event.event_id}>
                        <td className="nowrap">
                          {new Date(event.opened_at).toLocaleString([], {
                            month: "short",
                            day: "numeric",
                            hour: "2-digit",
                            minute: "2-digit",
                          })}
                        </td>
                        <td>{METRIC_LABELS[event.metric_type] || event.metric_type}</td>
                        <td className="num">{Number(event.triggered_value).toFixed(1)}</td>
                        <td className={event.status === "OPEN" ? "state-open" : "state-resolved"}>
                          {event.status === "OPEN" ? "Open" : "Resolved"}
                        </td>
                        <td>{event.resolved_by || "–"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="empty">No alerts this month.</p>
            )}
          </section>

          <section className="panel" aria-labelledby="logs-title">
            <div className="panel__head">
              <h2 className="panel__title" id="logs-title">Logs</h2>
              <div className="filters">
                <select
                  aria-label="Log level"
                  value={logLevel}
                  onChange={(event) => setLogLevel(event.target.value)}
                >
                  <option value="">All levels</option>
                  <option value="ERROR">Errors</option>
                  <option value="WARNING">Warnings</option>
                  <option value="INFO">Info</option>
                  <option value="DEBUG">Debug</option>
                </select>
                <form
                  className="filters"
                  onSubmit={(event) => {
                    event.preventDefault();
                    setLogSearch(logSearchInput.trim());
                  }}
                >
                  <input
                    aria-label="Search log messages"
                    value={logSearchInput}
                    onChange={(event) => setLogSearchInput(event.target.value)}
                    placeholder="Search messages"
                  />
                  <button type="submit" className="btn btn--primary">
                    Search
                  </button>
                </form>
              </div>
            </div>

            {logError && <p className="error-text">{logError}</p>}

            {logs.length > 0 ? (
              <div className="table-wrap table-wrap--tall">
                <table>
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Level</th>
                      <th className="col-source">Source</th>
                      <th>Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {logs.map((log) => (
                      <tr key={log.log_id}>
                        <td className="nowrap num">
                          {new Date(log.log_time).toLocaleTimeString()}
                        </td>
                        <td>
                          <span className={`badge badge--${log.level}`}>{log.level}</span>
                        </td>
                        <td className="nowrap col-source">{log.source}</td>
                        <td className="msg">{log.message}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="empty">
                No logs today{logLevel || logSearch ? " match these filters" : " for this server"}.
              </p>
            )}

            {(logNextPage || viewingOlder) && (
              <div className="log-actions">
                {logNextPage && (
                  <button
                    type="button"
                    className="btn"
                    onClick={loadOlderLogs}
                    disabled={logLoadingOlder}
                  >
                    {logLoadingOlder ? "Loading…" : "Load older"}
                  </button>
                )}
                {viewingOlder && (
                  <button type="button" className="btn" onClick={backToLatestLogs}>
                    Back to latest
                  </button>
                )}
              </div>
            )}
          </section>
        </div>

        <footer className="footer">
          CloudPulse, built with React, Django and a 3-node Apache Cassandra cluster.
        </footer>
      </main>
    </div>
  );
}

export default App;