import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";
import { useEffect, useMemo, useState } from "react";

const API_BASE = "http://127.0.0.1:8000/api";

function formatBytes(value) {
  if (value === undefined || value === null) return "—";

  let size = Number(value);
  if (!Number.isFinite(size)) return "—";
  if (size < 0) return "0 B";

  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;

  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index++;
  }

  return `${size.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
}

function App() {
  const [historyMetrics, setHistoryMetrics] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const [servers, setServers] = useState([]);
  const [selectedServerId, setSelectedServerId] = useState("");
  const [metrics, setMetrics] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState(null);

  // Load the registered servers.
  useEffect(() => {
    let active = true;

    async function loadServers() {
      try {
        const response = await fetch(`${API_BASE}/servers/`);
        const data = await response.json();

        if (!response.ok || data.status !== "success") {
          throw new Error(data.message || "Could not load servers.");
        }

        if (active) {
          setServers(data.servers || []);
          setSelectedServerId((current) => {
            if (
              current &&
              (data.servers || []).some(
                (server) => server.server_id === current
              )
            ) {
              return current;
            }

            return data.servers?.[0]?.server_id || "";
          });
        }
      } catch (err) {
        if (active) setError(err.message);
      }
    }

    loadServers();
    const timer = setInterval(loadServers, 30000);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  // Load the latest metrics and refresh them every 10 seconds.
  useEffect(() => {
    if (!selectedServerId) {
      setMetrics([]);
      setLoading(false);
      return;
    }

    let active = true;

    async function loadMetrics() {
      try {
        const response = await fetch(
          `${API_BASE}/metrics/latest/?server_id=${encodeURIComponent(
            selectedServerId
          )}`
        );

        const data = await response.json();

        if (!response.ok || data.status !== "success") {
          throw new Error(data.message || "Could not load metrics.");
        }

        if (active) {
          setMetrics(data.metrics || []);
          setLastUpdated(new Date());
          setError("");
        }
      } catch (err) {
        if (active) setError(err.message);
      } finally {
        if (active) setLoading(false);
      }
    }

    setLoading(true);
    loadMetrics();

    const timer = setInterval(loadMetrics, 10000);

    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [selectedServerId]);

      useEffect(() => {
  if (!selectedServerId) {
    setHistoryMetrics([]);
    setHistoryLoading(false);
    return;
  }

  let active = true;

  async function loadHistoricalMetrics() {
    try {
      const response = await fetch(
        `${API_BASE}/metrics/history/?server_id=${encodeURIComponent(
          selectedServerId
        )}&days=1`
      );

      const data = await response.json();

      if (!response.ok || data.status !== "success") {
        throw new Error(
          data.message || "Could not load historical metrics."
        );
      }

      if (active) {
        setHistoryMetrics(data.metrics || []);
        setHistoryError("");
      }
    } catch (err) {
      if (active) {
        setHistoryError(err.message);
      }
    } finally {
      if (active) {
        setHistoryLoading(false);
      }
    }
  }

  setHistoryMetrics([]);
  setHistoryLoading(true);
  loadHistoricalMetrics();

  const timer = setInterval(loadHistoricalMetrics, 30000);

  return () => {
    active = false;
    clearInterval(timer);
  };
}, [selectedServerId]);

  const metricMap = useMemo(() => {
    return Object.fromEntries(
      metrics.map((metric) => [metric.metric_type, metric])
    );
  }, [metrics]);

  const historyChartData = useMemo(() => {
  const byTimestamp = new Map();

  for (const item of historyMetrics) {
    if (!item.recorded_at) continue;

    if (!byTimestamp.has(item.recorded_at)) {
      byTimestamp.set(item.recorded_at, {
        recorded_at: item.recorded_at,
        time: new Date(item.recorded_at).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        }),
      });
    }

    const point = byTimestamp.get(item.recorded_at);

    if (
      ["cpu_percent", "memory_percent", "disk_percent"].includes(
        item.metric_type
      )
    ) {
      point[item.metric_type] = Number(item.metric_value);
    }
  }

  const sorted = [...byTimestamp.values()].sort((a, b) =>
    a.recorded_at.localeCompare(b.recorded_at)
  );

  // Limit chart points to keep rendering responsive.
  const maxPoints = 150;
  const step = Math.max(1, Math.ceil(sorted.length / maxPoints));

  return sorted.filter(
    (_, index) =>
      index % step === 0 || index === sorted.length - 1
  );
}, [historyMetrics]);

  const server = servers.find(
    (item) => item.server_id === selectedServerId
  );

  function percentage(type) {
    const value = metricMap[type]?.metric_value;
    return value === undefined || value === null
      ? "—"
      : `${Number(value).toFixed(1)}%`;
  }

  function numericValue(type) {
    const value = metricMap[type]?.metric_value;
    return value === undefined || value === null ? null : Number(value);
  }

  const cpu = numericValue("cpu_percent");
  const memory = numericValue("memory_percent");
  const disk = numericValue("disk_percent");

  const alerts = [];

  if (cpu !== null && cpu > 90) {
    alerts.push("CPU usage is above 90%.");
  }

  if (memory !== null && memory > 85) {
    alerts.push("Memory usage is above 85%.");
  }

  if (disk !== null && disk > 90) {
    alerts.push("Disk usage is above 90%.");
  }

  const cards = [
    {
      title: "CPU Utilization",
      value: percentage("cpu_percent"),
      description: "Current processor usage",
      icon: "◉",
    },
    {
      title: "Memory Utilization",
      value: percentage("memory_percent"),
      description: "RAM currently in use",
      icon: "▤",
    },
    {
      title: "Disk Utilization",
      value: percentage("disk_percent"),
      description: "Usage of the monitored drive",
      icon: "▣",
    },
    {
      title: "Network Sent",
      value: formatBytes(metricMap.network_sent_bytes?.metric_value),
      description: "Cumulative bytes sent since boot",
      icon: "↑",
    },
    {
      title: "Network Received",
      value: formatBytes(metricMap.network_recv_bytes?.metric_value),
      description: "Cumulative bytes received since boot",
      icon: "↓",
    },
  ];

  const styles = {
    page: {
      minHeight: "100vh",
      background: "#f4f7fb",
      color: "#172033",
      fontFamily: "Inter, Segoe UI, Arial, sans-serif",
    },
    header: {
      background: "#14213d",
      color: "#ffffff",
      padding: "24px 5%",
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      flexWrap: "wrap",
      gap: "16px",
    },
    main: {
      maxWidth: "1250px",
      margin: "0 auto",
      padding: "32px 22px",
    },
    panel: {
      background: "#ffffff",
      border: "1px solid #e3e9f2",
      borderRadius: "14px",
      padding: "22px",
      marginBottom: "24px",
    },
    grid: {
      display: "grid",
      gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
      gap: "18px",
      marginBottom: "24px",
    },
    card: {
      background: "#ffffff",
      border: "1px solid #e3e9f2",
      borderRadius: "14px",
      padding: "22px",
      minWidth: 0,
    },
    muted: {
      color: "#6b7280",
      fontSize: "14px",
    },
  };

  return (
    <div style={styles.page}>
      <header style={styles.header}>
        <div>
          <div
            style={{
              fontSize: "13px",
              letterSpacing: "2px",
              color: "#a9c7ff",
              fontWeight: 700,
            }}
          >
            CLOUD OBSERVABILITY
          </div>
          <h1 style={{ margin: "8px 0", fontSize: "30px" }}>
            CloudPulse
          </h1>
          <div style={{ color: "#d4dced", fontSize: "14px" }}>
            Real-time infrastructure monitoring
          </div>
        </div>

        <div style={{ fontSize: "14px", color: "#d4dced" }}>
          <span style={{ color: "#72e3ac" }}>●</span>{" "}
          Dashboard connected
          <div style={{ marginTop: "8px" }}>
            Refresh interval: 10 seconds
          </div>
        </div>
      </header>

      <main style={styles.main}>
        <section style={styles.panel}>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              flexWrap: "wrap",
              gap: "18px",
            }}
          >
            <div>
              <h2 style={{ margin: "0 0 7px", fontSize: "20px" }}>
                Server Overview
              </h2>
              <p style={{ ...styles.muted, margin: 0 }}>
                Select a registered server to inspect its latest metrics.
              </p>
            </div>

            <div>
              <label
                htmlFor="server-select"
                style={{
                  display: "block",
                  fontSize: "13px",
                  marginBottom: "7px",
                  fontWeight: 600,
                }}
              >
                Monitored server
              </label>
              <select
                id="server-select"
                value={selectedServerId}
                onChange={(event) =>
                  setSelectedServerId(event.target.value)
                }
                style={{
                  padding: "10px 12px",
                  minWidth: "240px",
                  maxWidth: "100%",
                  border: "1px solid #ccd5e2",
                  borderRadius: "8px",
                  background: "#ffffff",
                  fontSize: "14px",
                }}
              >
                {servers.length === 0 && (
                  <option value="">No servers registered</option>
                )}

                {servers.map((item) => (
                  <option key={item.server_id} value={item.server_id}>
                    {item.hostname} — {item.server_id.slice(0, 8)}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {server && (
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: "12px 28px",
                marginTop: "24px",
                paddingTop: "20px",
                borderTop: "1px solid #edf0f5",
                fontSize: "14px",
              }}
            >
              <div>
                <div style={styles.muted}>Hostname</div>
                <strong>{server.hostname || "Unknown"}</strong>
              </div>
              <div>
                <div style={styles.muted}>Operating System</div>
                <strong>{server.os_name || "Unknown"}</strong>
              </div>
              <div>
                <div style={styles.muted}>Health Status</div>
                <strong
                  style={{
                    color:
                      server.health_status === "UP" ? "#14804a" : "#b45309",
                  }}
                >
                  {server.health_status || "Unknown"}
                </strong>
              </div>
              <div>
                <div style={styles.muted}>Last Seen</div>
                <strong>
                  {server.last_seen
                    ? new Date(server.last_seen).toLocaleString()
                    : "Not available"}
                </strong>
              </div>
            </div>
          )}
        </section>

        {error && (
          <div
            role="alert"
            style={{
              background: "#fff1f0",
              border: "1px solid #fecaca",
              color: "#991b1b",
              padding: "14px 18px",
              borderRadius: "10px",
              marginBottom: "22px",
            }}
          >
            <strong>Connection issue:</strong> {error}
            <div style={{ fontSize: "13px", marginTop: "5px" }}>
              Check that Cassandra and the Django server are running.
            </div>
          </div>
        )}

        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            flexWrap: "wrap",
            gap: "12px",
            marginBottom: "18px",
          }}
        >
          <h2 style={{ margin: 0, fontSize: "21px" }}>
            Live Metrics
          </h2>
          <span style={styles.muted}>
            {lastUpdated
              ? `Last refreshed: ${lastUpdated.toLocaleTimeString()}`
              : loading
                ? "Loading metrics…"
                : "Waiting for metric data"}
          </span>
        </div>

        <div style={styles.grid}>
          {cards.map((card) => (
            <article key={card.title} style={styles.card}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  gap: "10px",
                }}
              >
                <span style={{ ...styles.muted, fontWeight: 600 }}>
                  {card.title}
                </span>
                <span style={{ fontSize: "23px", color: "#3867d6" }}>
                  {card.icon}
                </span>
              </div>

              <div
                style={{
                  margin: "18px 0 10px",
                  fontSize: "30px",
                  fontWeight: 700,
                  overflowWrap: "anywhere",
                }}
              >
                {loading && metrics.length === 0 ? "Loading…" : card.value}
              </div>

              <div style={styles.muted}>{card.description}</div>
            </article>
          ))}
        </div>

<section style={styles.panel}>
  <h2 style={{ margin: "0 0 8px", fontSize: "20px" }}>
    Historical Trends
  </h2>

  <p style={styles.muted}>
    CPU, memory, and disk usage over the last 24 hours.
  </p>

  {historyError && (
    <p role="alert">
      Unable to load historical data: {historyError}
    </p>
  )}

  {historyLoading && historyChartData.length === 0 && (
    <p style={styles.muted}>Loading historical readings...</p>
  )}

  {!historyLoading &&
    !historyError &&
    historyChartData.length === 0 && (
      <p style={styles.muted}>
        No historical readings available yet. Keep the monitoring
        agent running and refresh this section after metrics arrive.
      </p>
    )}

  {historyChartData.length > 0 && (
    <div style={styles.grid}>
      {[
        {
          title: "CPU Usage",
          key: "cpu_percent",
          color: "#2563eb",
        },
        {
          title: "Memory Usage",
          key: "memory_percent",
          color: "#9333ea",
        },
        {
          title: "Disk Usage",
          key: "disk_percent",
          color: "#0d9488",
        },
      ].map((chart) => (
        <article key={chart.key} style={styles.card}>
          <h3 style={{ marginTop: 0 }}>{chart.title}</h3>

          <div style={{ width: "100%", height: 240 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={historyChartData}>
                <CartesianGrid strokeDasharray="3 3" />

                <XAxis
                  dataKey="time"
                  tick={{ fontSize: 11 }}
                  minTickGap={25}
                />

                <YAxis
                  domain={[0, 100]}
                  unit="%"
                  width={45}
                  tick={{ fontSize: 11 }}
                />

                <Tooltip
                  formatter={(value) => [
                    `${Number(value).toFixed(1)}%`,
                    chart.title,
                  ]}
                  labelFormatter={(label) => `Time: ${label}`}
                />

                <Line
                  type="monotone"
                  dataKey={chart.key}
                  name={chart.title}
                  stroke={chart.color}
                  strokeWidth={2}
                  dot={false}
                  connectNulls={false}
                  isAnimationActive={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </article>
      ))}
    </div>
  )}
</section>

        <section style={styles.panel}>
          <h2 style={{ margin: "0 0 8px", fontSize: "20px" }}>
            Threshold Alerts
          </h2>
          <p style={{ ...styles.muted, marginTop: 0 }}>
            CPU above 90%, memory above 85%, or disk above 90%.
          </p>

          {alerts.length > 0 ? (
            alerts.map((alert) => (
              <div
                key={alert}
                style={{
                  padding: "12px 14px",
                  borderRadius: "8px",
                  background: "#fff4e5",
                  color: "#92400e",
                  border: "1px solid #fed7aa",
                  marginTop: "10px",
                }}
              >
                ⚠ {alert}
              </div>
            ))
          ) : (
            <p
              style={{
                margin: "16px 0 0",
                color: "#536174",
                fontSize: "14px",
              }}
            >
              {metrics.length === 0
                ? "No metrics have been loaded yet."
                : "No threshold alerts based on the latest readings."}
            </p>
          )}
        </section>

        <div
          style={{
            textAlign: "center",
            color: "#7b8494",
            fontSize: "12px",
            padding: "12px 0 24px",
          }}
        >
          CloudPulse · React · Django · Apache Cassandra
        </div>
      </main>
    </div>
  );
}

export default App;