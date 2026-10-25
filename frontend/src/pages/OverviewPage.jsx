// Page 1: the whole fleet at a glance.
import { useState } from "react";
import { Gauge, Link, PageHead, SeverityBadge, StatusPill, TablesUsed } from "../components.jsx";
import {
  API_BASE, METRIC_LABELS, ORG_ID, PERCENT_KEYS, formatDateTime, formatValue,
  getJson, navigate, secondsAgo, thresholdsFor, usePolling,
} from "../lib.js";

export default function OverviewPage({ servers }) {
  const [latest, setLatest] = useState({});     // server_id -> {metric_type: value}
  const [openAlerts, setOpenAlerts] = useState([]);
  const [rules, setRules] = useState([]);
  const [error, setError] = useState("");

  const ids = servers.list.map((s) => s.server_id).join(",");

  // Latest values of every server, read in parallel (one partition each).
  usePolling(async (isActive) => {
    try {
      const results = await Promise.all(
        servers.list.map((s) =>
          getJson(`${API_BASE}/metrics/latest/?server_id=${encodeURIComponent(s.server_id)}`)
            .then((data) => [s.server_id, data.metrics || []])
            .catch(() => [s.server_id, []])
        )
      );
      if (!isActive()) return;
      const map = {};
      for (const [id, metrics] of results) {
        map[id] = Object.fromEntries(metrics.map((m) => [m.metric_type, Number(m.metric_value)]));
      }
      setLatest(map);
    } catch (err) {
      if (isActive()) setError(err.message);
    }
  }, 10000, [ids]);

  usePolling(async (isActive) => {
    try {
      const [alertData, ruleData] = await Promise.all([
        getJson(`${API_BASE}/alerts/open/?org_id=${ORG_ID}`),
        getJson(`${API_BASE}/alerts/rules/?org_id=${ORG_ID}`),
      ]);
      if (!isActive()) return;
      setOpenAlerts(alertData.alerts || []);
      setRules(ruleData.rules || []);
      setError("");
    } catch (err) {
      if (isActive()) setError(err.message);
    }
  }, 10000, []);

  const count = (status) => servers.list.filter((s) => s.health_status === status).length;
  const critical = openAlerts.filter((a) => a.severity === "CRITICAL").length;
  const alertsByServer = {};
  for (const alert of openAlerts) {
    alertsByServer[alert.server_id] = (alertsByServer[alert.server_id] || 0) + 1;
  }

  return (
    <>
      <PageHead
        title="Overview"
        note="Every server in the organisation, refreshed every 10 seconds."
      />

      {error && <p className="error-text">{error}</p>}

      <section className="stats" aria-label="Summary">
        <div className="stat">
          <div className="stat__label">Servers</div>
          <div className="stat__value">{servers.list.length}</div>
          <div className="stat__hint">reporting to this organisation</div>
        </div>
        <div className="stat stat--ok">
          <div className="stat__label">Up</div>
          <div className="stat__value">{count("UP")}</div>
          <div className="stat__hint">reported in the last 30 s</div>
        </div>
        <div className={`stat ${count("STALE") + count("DOWN") > 0 ? "stat--bad" : ""}`}>
          <div className="stat__label">Stale or down</div>
          <div className="stat__value">{count("STALE") + count("DOWN")}</div>
          <div className="stat__hint">{count("STALE")} stale, {count("DOWN")} down</div>
        </div>
        <div className={`stat ${critical > 0 ? "stat--bad" : openAlerts.length ? "stat--warn" : ""}`}>
          <div className="stat__label">Open alerts</div>
          <div className="stat__value">{openAlerts.length}</div>
          <div className="stat__hint">
            {critical} critical · {rules.filter((r) => r.enabled).length} rules enabled
          </div>
        </div>
      </section>

      <section className="section panel">
        <div className="panel__head">
          <h2 className="panel__title">Servers</h2>
          <span className="muted">Select a server to see its charts.</span>
        </div>
        {servers.list.length === 0 ? (
          <p className="empty">
            {servers.loaded
              ? "No servers are reporting yet. Start an agent with python agent/monitor.py."
              : "Loading servers…"}
          </p>
        ) : (
          <div className="table-wrap">
            <table className="table--rows">
              <thead>
                <tr>
                  <th>Server</th>
                  <th>Status</th>
                  <th>Operating system</th>
                  <th>Last report</th>
                  {PERCENT_KEYS.map((key) => (
                    <th key={key}>{METRIC_LABELS[key]}</th>
                  ))}
                  <th>Open alerts</th>
                </tr>
              </thead>
              <tbody>
                {servers.list.map((s) => {
                  const values = latest[s.server_id] || {};
                  const limits = thresholdsFor(rules, s.server_id);
                  return (
                    <tr
                      key={s.server_id}
                      className="row-link"
                      onClick={() => navigate(`/servers/${s.server_id}`)}
                    >
                      <td>
                        <Link to={`/servers/${s.server_id}`} className="strong-link">
                          {s.hostname || "Unknown"}
                        </Link>
                        <div className="muted mono">{s.server_id.slice(0, 8)}</div>
                      </td>
                      <td><StatusPill status={s.health_status} /></td>
                      <td>{s.os_name || "–"}</td>
                      <td className="nowrap">{secondsAgo(s.seconds_since_seen)}</td>
                      {PERCENT_KEYS.map((key) => (
                        <td key={key} className="cell-gauge">
                          <span className="num">{formatValue(key, values[key])}</span>
                          <Gauge value={values[key] ?? null} limit={limits[key]} small />
                        </td>
                      ))}
                      <td className="num">{alertsByServer[s.server_id] || 0}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="section panel">
        <div className="panel__head">
          <h2 className="panel__title">Open alerts</h2>
          <Link to="/alerts" className="text-link">View all alerts →</Link>
        </div>
        {openAlerts.length === 0 ? (
          <p className="empty">No open alerts. Everything is within its limits.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Rule</th>
                  <th>Server</th>
                  <th>Value</th>
                  <th>Opened</th>
                </tr>
              </thead>
              <tbody>
                {openAlerts.slice(0, 5).map((a) => (
                  <tr key={a.event_id}>
                    <td><SeverityBadge severity={a.severity} /></td>
                    <td>{a.rule_name}</td>
                    <td>{a.hostname}</td>
                    <td className="num">
                      {formatValue(a.metric_type, a.triggered_value)} (limit {a.threshold})
                    </td>
                    <td className="nowrap">{formatDateTime(a.opened_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <TablesUsed reads={["servers_by_org", "latest_metric_by_server", "open_alerts_by_org", "alert_rules_by_org"]} />
    </>
  );
}