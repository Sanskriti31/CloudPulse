// Page 3: open alerts across all servers, and alert history by server or rule.
import { useState } from "react";
import { Link, PageHead, SeverityBadge, TablesUsed } from "../components.jsx";
import {
  API_BASE, METRIC_LABELS, ORG_ID, formatDateTime, formatValue,
  getJson, sendJson, usePolling,
} from "../lib.js";

export default function AlertsPage({ servers }) {
  // Open alerts
  const [openAlerts, setOpenAlerts] = useState([]);
  const [severity, setSeverity] = useState("");
  const [openError, setOpenError] = useState("");
  const [resolvingId, setResolvingId] = useState("");
  const [refresh, setRefresh] = useState(0);

  // History
  const [rules, setRules] = useState([]);
  const [mode, setMode] = useState("server");          // "server" or "rule"
  const [pickedServer, setPickedServer] = useState("");
  const [pickedRule, setPickedRule] = useState("");
  const [months, setMonths] = useState(1);
  const [events, setEvents] = useState([]);
  const [historyError, setHistoryError] = useState("");

  const serverId = pickedServer || servers.list[0]?.server_id || "";
  const ruleId = pickedRule || rules[0]?.rule_id || "";
  const hostnames = Object.fromEntries(servers.list.map((s) => [s.server_id, s.hostname]));
  const ruleNames = Object.fromEntries(rules.map((r) => [r.rule_id, r.rule_name]));

  usePolling(async (isActive) => {
    try {
      const [alertData, ruleData] = await Promise.all([
        getJson(`${API_BASE}/alerts/open/?org_id=${ORG_ID}`),
        getJson(`${API_BASE}/alerts/rules/?org_id=${ORG_ID}`),
      ]);
      if (!isActive()) return;
      setOpenAlerts(alertData.alerts || []);
      setRules(ruleData.rules || []);
      setOpenError("");
    } catch (err) {
      if (isActive()) setOpenError(err.message);
    }
  }, 10000, [refresh]);

  const key = mode === "server" ? serverId : ruleId;
  usePolling(async (isActive) => {
    if (!key) {
      setEvents([]);
      return;
    }
    try {
      const param = mode === "server" ? "server_id" : "rule_id";
      const data = await getJson(
        `${API_BASE}/alerts/history/?${param}=${encodeURIComponent(key)}&months=${months}&limit=200`
      );
      if (!isActive()) return;
      setEvents(data.events || []);
      setHistoryError("");
    } catch (err) {
      if (isActive()) setHistoryError(err.message);
    }
  }, 15000, [mode, key, months, refresh]);

  async function resolve(alert) {
    setResolvingId(alert.event_id);
    try {
      await sendJson(`${API_BASE}/alerts/resolve/`, "POST", {
        server_id: alert.server_id,
        rule_id: alert.rule_id,
        resolved_by: "dashboard",
      });
      setRefresh((n) => n + 1);
    } catch (err) {
      setOpenError(err.message);
    } finally {
      setResolvingId("");
    }
  }

  const shown = openAlerts.filter((a) => !severity || a.severity === severity);

  return (
    <>
      <PageHead
        title="Alerts"
        note="Alerts are opened and resolved by the server-side engine on every ingest."
      >
        <Link to="/rules" className="btn">Manage rules</Link>
      </PageHead>

      <section className="panel">
        <div className="panel__head">
          <h2 className="panel__title">Open alerts ({openAlerts.length})</h2>
          <div className="segmented" role="group" aria-label="Severity">
            {[["", "All"], ["CRITICAL", "Critical"], ["WARNING", "Warning"]].map(([value, label]) => (
              <button
                key={label}
                type="button"
                aria-pressed={severity === value}
                onClick={() => setSeverity(value)}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        {openError && <p className="error-text">{openError}</p>}

        {shown.length === 0 ? (
          <p className="empty">No open alerts{severity ? ` with severity ${severity.toLowerCase()}` : ""}.</p>
        ) : (
          <div className="table-wrap">
            <table className="table--mid">
              <thead>
                <tr>
                  <th>Severity</th>
                  <th>Rule</th>
                  <th>Server</th>
                  <th>Metric</th>
                  <th>Value / limit</th>
                  <th>Opened</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {shown.map((a) => (
                  <tr key={a.event_id}>
                    <td><SeverityBadge severity={a.severity} /></td>
                    <td className="strong">{a.rule_name}</td>
                    <td>
                      <Link to={`/servers/${a.server_id}`} className="text-link">{a.hostname}</Link>
                    </td>
                    <td>{METRIC_LABELS[a.metric_type] || a.metric_type}</td>
                    <td className="num nowrap">
                      {formatValue(a.metric_type, a.triggered_value)} / {a.threshold}
                    </td>
                    <td className="nowrap">{formatDateTime(a.opened_at)}</td>
                    <td className="actions">
                      <button
                        type="button"
                        className="btn btn--small"
                        onClick={() => resolve(a)}
                        disabled={resolvingId === a.event_id}
                      >
                        {resolvingId === a.event_id ? "Resolving…" : "Resolve"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="section panel">
        <div className="panel__head">
          <div>
            <h2 className="panel__title">Alert history</h2>
            <p className="muted">
              The same events are stored twice: by server (<code>alert_events_by_server</code>)
              and by rule (<code>alert_events_by_rule</code>).
            </p>
          </div>
          <div className="filters">
            <div className="segmented" role="group" aria-label="History by">
              <button type="button" aria-pressed={mode === "server"} onClick={() => setMode("server")}>
                By server
              </button>
              <button type="button" aria-pressed={mode === "rule"} onClick={() => setMode("rule")}>
                By rule
              </button>
            </div>
            {mode === "server" ? (
              <select aria-label="Server" value={serverId} onChange={(e) => setPickedServer(e.target.value)}>
                {servers.list.length === 0 && <option value="">No servers</option>}
                {servers.list.map((s) => (
                  <option key={s.server_id} value={s.server_id}>{s.hostname} – {s.server_id.slice(0, 8)}</option>
                ))}
              </select>
            ) : (
              <select aria-label="Rule" value={ruleId} onChange={(e) => setPickedRule(e.target.value)}>
                {rules.length === 0 && <option value="">No rules</option>}
                {rules.map((r) => (
                  <option key={r.rule_id} value={r.rule_id}>{r.rule_name}</option>
                ))}
              </select>
            )}
            <select aria-label="Months" value={months} onChange={(e) => setMonths(Number(e.target.value))}>
              <option value={1}>This month</option>
              <option value={3}>Last 3 months</option>
              <option value={6}>Last 6 months</option>
            </select>
          </div>
        </div>

        {historyError && <p className="error-text">{historyError}</p>}

        {events.length === 0 ? (
          <p className="empty">No alerts in this period.</p>
        ) : (
          <div className="table-wrap table-wrap--tall">
            <table className="table--mid">
              <thead>
                <tr>
                  <th>Opened</th>
                  <th>{mode === "server" ? "Rule" : "Server"}</th>
                  <th>Severity</th>
                  <th>Metric</th>
                  <th>Value</th>
                  <th>Status</th>
                  <th>Resolved</th>
                  <th>Resolved by</th>
                </tr>
              </thead>
              <tbody>
                {events.map((e) => (
                  <tr key={e.event_id}>
                    <td className="nowrap">{formatDateTime(e.opened_at)}</td>
                    <td>
                      {mode === "server"
                        ? e.rule_name || ruleNames[e.rule_id] || "(deleted rule)"
                        : e.hostname || hostnames[e.server_id] || e.server_id.slice(0, 8)}
                    </td>
                    <td><SeverityBadge severity={e.severity} /></td>
                    <td>{METRIC_LABELS[e.metric_type] || e.metric_type}</td>
                    <td className="num">{formatValue(e.metric_type, e.triggered_value)}</td>
                    <td className={e.status === "OPEN" ? "state-open" : "state-resolved"}>
                      {e.status === "OPEN" ? "Open" : "Resolved"}
                    </td>
                    <td className="nowrap">{formatDateTime(e.resolved_at)}</td>
                    <td>{e.resolved_by || "–"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <p className="query-info">{events.length} events, newest first.</p>
      </section>

      <TablesUsed
        reads={["open_alerts_by_org", "alert_events_by_server", "alert_events_by_rule", "alert_rules_by_org"]}
        writes={["alert_state_by_server", "open_alerts_by_org", "alert_events_by_server", "alert_events_by_rule"]}
      />
    </>
  );
}