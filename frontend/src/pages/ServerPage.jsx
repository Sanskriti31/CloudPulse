// Page 2: one server in detail (live values, history, its alerts).
import {
  ResponsiveContainer, LineChart, Line, CartesianGrid, XAxis, YAxis, Tooltip,
} from "recharts";
import { useEffect, useMemo, useState } from "react";
import { Gauge, Link, PageHead, ServerSelect, StatusPill, TablesUsed } from "../components.jsx";
import {
  API_BASE, METRIC_LABELS, ORG_ID, formatBytes, formatDateTime, formatValue,
  getJson, navigate, secondsAgo, thresholdsFor, usePolling,
} from "../lib.js";

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

const RANGES = [
  { days: 1, label: "Today" },
  { days: 7, label: "7 days" },
  { days: 30, label: "30 days" },
];

const RESOLUTION_TEXT = {
  raw: "raw readings every 10 seconds (metrics_by_server_bucket)",
  "1m": "1-minute averages (metrics_rollup_1m)",
  "1h": "1-hour averages (metrics_rollup_1h)",
};

export default function ServerPage({ servers, serverId }) {
  const server = servers.list.find((s) => s.server_id === serverId);

  // /servers with no id (or an unknown id): open the first server.
  useEffect(() => {
    if (servers.list.length > 0 && !server) {
      navigate(`/servers/${servers.list[0].server_id}`, { replace: true });
    }
  }, [servers.list, server]);

  const [metrics, setMetrics] = useState([]);
  const [metricsError, setMetricsError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);
  const [thresholds, setThresholds] = useState({});

  const [days, setDays] = useState(1);
  const [history, setHistory] = useState([]);
  const [historyMeta, setHistoryMeta] = useState(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");

  const [alertHistory, setAlertHistory] = useState([]);

  usePolling(async (isActive) => {
    try {
      const data = await getJson(`${API_BASE}/alerts/rules/?org_id=${ORG_ID}`);
      if (isActive()) setThresholds(thresholdsFor(data.rules || [], serverId));
    } catch {
      // Thresholds only decorate the gauges.
    }
  }, 60000, [serverId]);

  usePolling(async (isActive) => {
    if (!server) return;
    try {
      const data = await getJson(
        `${API_BASE}/metrics/latest/?server_id=${encodeURIComponent(serverId)}`
      );
      if (!isActive()) return;
      setMetrics(data.metrics || []);
      setLastUpdated(new Date());
      setMetricsError("");
    } catch (err) {
      if (isActive()) setMetricsError(err.message);
    }
  }, 10000, [serverId, Boolean(server)]);

  useEffect(() => {
    setHistory([]);
    setHistoryMeta(null);
    setHistoryLoading(true);
  }, [serverId, days]);

  usePolling(async (isActive) => {
    if (!server) return;
    try {
      const data = await getJson(
        `${API_BASE}/metrics/history/?server_id=${encodeURIComponent(serverId)}&days=${days}`
      );
      if (!isActive()) return;
      setHistory(data.metrics || []);
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
  }, 30000, [serverId, days, Boolean(server)]);

  usePolling(async (isActive) => {
    if (!server) return;
    try {
      const data = await getJson(
        `${API_BASE}/alerts/history/?server_id=${encodeURIComponent(serverId)}&limit=8`
      );
      if (isActive()) setAlertHistory(data.events || []);
    } catch {
      if (isActive()) setAlertHistory([]);
    }
  }, 15000, [serverId, Boolean(server)]);

  const metricMap = useMemo(
    () => Object.fromEntries(metrics.map((m) => [m.metric_type, Number(m.metric_value)])),
    [metrics]
  );

  const chartData = useMemo(() => {
    const byTime = new Map();
    for (const item of history) {
      if (!item.recorded_at) continue;
      if (!byTime.has(item.recorded_at)) {
        const date = new Date(item.recorded_at);
        byTime.set(item.recorded_at, {
          recorded_at: item.recorded_at,
          time:
            days > 1
              ? date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
              : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
        });
      }
      if (CHARTS.some((c) => c.key === item.metric_type)) {
        byTime.get(item.recorded_at)[item.metric_type] = Number(item.metric_value);
      }
    }
    const sorted = [...byTime.values()].sort((a, b) => a.recorded_at.localeCompare(b.recorded_at));
    const step = Math.max(1, Math.ceil(sorted.length / 150));
    return sorted.filter((_, i) => i % step === 0 || i === sorted.length - 1);
  }, [history, days]);

  if (!server) {
    return (
      <>
        <PageHead title="Servers" />
        <div className="panel">
          <p className="empty">
            {servers.loaded ? "No servers are reporting yet." : "Loading servers…"}
          </p>
        </div>
      </>
    );
  }

  return (
    <>
      <PageHead title={server.hostname || "Server"} note={`Server ID ${server.server_id}`}>
        <ServerSelect
          servers={servers.list}
          value={serverId}
          onChange={(id) => navigate(`/servers/${id}`)}
        />
      </PageHead>

      <div className="panel facts">
        <StatusPill status={server.health_status} />
        <div className="fact">
          <span className="fact__label">Operating system</span>
          <span className="fact__value">{server.os_name || "Unknown"}</span>
        </div>
        <div className="fact">
          <span className="fact__label">Last report</span>
          <span className="fact__value">{secondsAgo(server.seconds_since_seen)}</span>
        </div>
        <div className="fact">
          <span className="fact__label">Registered</span>
          <span className="fact__value">{formatDateTime(server.registered_at)}</span>
        </div>
        <span className="refreshed">
          {lastUpdated ? `Refreshed at ${lastUpdated.toLocaleTimeString()}` : "Waiting for data"}
        </span>
      </div>

      {metricsError && <p className="error-text">{metricsError}</p>}

      <section className="section">
        <div className="section__head">
          <div>
            <h2 className="section__title">Right now</h2>
            <p className="section__note">
              Updated every 10 seconds. The black mark on each bar is the alert threshold.
            </p>
          </div>
        </div>
        <div className="metrics">
          {PERCENT_METRICS.map((item) => {
            const value = metricMap[item.key] ?? null;
            const limit = thresholds[item.key];
            const over = value !== null && limit !== undefined && value > limit;
            return (
              <article key={item.key} className={`metric ${over ? "metric--over" : ""}`}>
                <div className="metric__label">{item.label}</div>
                <div className="metric__value">
                  {value === null ? "–" : value.toFixed(1)}
                  {value !== null && <span className="metric__unit">%</span>}
                </div>
                <Gauge value={value} limit={limit} />
                <div className="metric__hint">
                  {limit !== undefined ? `${item.hint}. Alert above ${limit}%.` : item.hint}
                </div>
              </article>
            );
          })}
          {RATE_METRICS.map((item) => {
            const formatted = formatBytes(metricMap[item.key]);
            return (
              <article key={item.key} className="metric">
                <div className="metric__label">{item.label}</div>
                <div className="metric__value">
                  {formatted ? formatted.number : "–"}
                  {formatted && <span className="metric__unit">{formatted.unit}/s</span>}
                </div>
                <div className="metric__hint">{item.hint}</div>
              </article>
            );
          })}
        </div>
      </section>

      <section className="section">
        <div className="section__head">
          <div>
            <h2 className="section__title">History</h2>
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
                aria-pressed={days === range.days}
                onClick={() => setDays(range.days)}
              >
                {range.label}
              </button>
            ))}
          </div>
        </div>

        {historyError && <p className="error-text">Couldn't load history: {historyError}</p>}

        {chartData.length > 0 ? (
          <div className="charts">
            {CHARTS.map((chart) => (
              <article key={chart.key} className="chart">
                <h3 className="chart__title">{chart.title}</h3>
                <div className="chart__box">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData} margin={{ top: 6, right: 8, bottom: 0, left: -8 }}>
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
                        contentStyle={{ borderRadius: 8, border: "1px solid #d3dae4", color: "#172033" }}
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
              {historyLoading ? "Loading history…" : "No readings for this range yet."}
            </p>
          </div>
        )}

        {historyMeta && (
          <p className="query-info">
            Read {historyMeta.count.toLocaleString()} rows from {historyMeta.partitions} Cassandra
            partition{historyMeta.partitions === 1 ? "" : "s"} in {historyMeta.ms} ms.
          </p>
        )}
      </section>

      <section className="section panel">
        <div className="panel__head">
          <h2 className="panel__title">Recent alerts on this server</h2>
          <Link to="/alerts" className="text-link">Manage alerts →</Link>
        </div>
        {alertHistory.length === 0 ? (
          <p className="empty">No alerts this month.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Opened</th>
                  <th>Rule</th>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Status</th>
                  <th>Resolved by</th>
                </tr>
              </thead>
              <tbody>
                {alertHistory.map((e) => (
                  <tr key={e.event_id}>
                    <td className="nowrap">{formatDateTime(e.opened_at)}</td>
                    <td>{e.rule_name || "–"}</td>
                    <td>{METRIC_LABELS[e.metric_type] || e.metric_type}</td>
                    <td className="num">{formatValue(e.metric_type, e.triggered_value)}</td>
                    <td className={e.status === "OPEN" ? "state-open" : "state-resolved"}>
                      {e.status === "OPEN" ? "Open" : "Resolved"}
                    </td>
                    <td>{e.resolved_by || "–"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <TablesUsed
        reads={["servers_by_org", "latest_metric_by_server", "metrics_by_server_bucket", "metrics_rollup_1m", "metrics_rollup_1h", "alert_events_by_server"]}
      />
    </>
  );
}