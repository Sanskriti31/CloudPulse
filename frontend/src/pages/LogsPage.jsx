// Page 5: log explorer (server, day, level, text search, paging).
import { useEffect, useRef, useState } from "react";
import { PageHead, ServerSelect, TablesUsed } from "../components.jsx";
import { API_BASE, getJson, navigate, todayUtc, usePolling } from "../lib.js";

const LEVELS = [
  ["", "All levels"],
  ["ERROR", "Errors"],
  ["WARNING", "Warnings"],
  ["INFO", "Info"],
  ["DEBUG", "Debug"],
];

export default function LogsPage({ servers, query }) {
  const serverId = query.get("server") || servers.list[0]?.server_id || "";
  const [day, setDay] = useState(todayUtc());
  const [level, setLevel] = useState("");
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");

  const [logs, setLogs] = useState([]);
  const [nextPage, setNextPage] = useState(null);
  const [scanned, setScanned] = useState(0);
  const [error, setError] = useState("");
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [viewingOlder, setViewingOlder] = useState(false);
  const viewingOlderRef = useRef(false);

  const isToday = day === todayUtc();

  function url(page) {
    const params = new URLSearchParams({ server_id: serverId, date: day, limit: "100" });
    if (level) params.set("level", level);
    if (search) params.set("q", search);
    if (page) params.set("page", page);
    return `${API_BASE}/logs/?${params.toString()}`;
  }

  function setOlder(value) {
    viewingOlderRef.current = value;
    setViewingOlder(value);
  }

  useEffect(() => {
    setLogs([]);
    setNextPage(null);
    setScanned(0);
    setOlder(false);
  }, [serverId, day, level, search]);

  // First page; refreshed every 10 s for today, paused while reading older pages.
  usePolling(async (isActive) => {
    if (!serverId || viewingOlderRef.current) return;
    try {
      const data = await getJson(url(null));
      if (!isActive() || viewingOlderRef.current) return;
      setLogs(data.logs || []);
      setNextPage(data.next_page);
      setScanned(data.rows_scanned ?? (data.logs || []).length);
      setError("");
    } catch (err) {
      if (isActive()) setError(err.message);
    }
  }, isToday ? 10000 : 3600000, [serverId, day, level, search]);

  async function loadOlder() {
    if (!nextPage) return;
    setLoadingOlder(true);
    try {
      const data = await getJson(url(nextPage));
      setOlder(true);
      setLogs((current) => [...current, ...(data.logs || [])]);
      setNextPage(data.next_page);
      setScanned((n) => n + (data.rows_scanned ?? 0));
    } catch (err) {
      setError(err.message);
    } finally {
      setLoadingOlder(false);
    }
  }

  async function backToLatest() {
    setOlder(false);
    try {
      const data = await getJson(url(null));
      setLogs(data.logs || []);
      setNextPage(data.next_page);
      setScanned(data.rows_scanned ?? 0);
    } catch (err) {
      setError(err.message);
    }
  }

  const counts = logs.reduce((acc, log) => ({ ...acc, [log.level]: (acc[log.level] || 0) + 1 }), {});
  const table = level ? "logs_by_server_level_day" : "logs_by_server_day";

  return (
    <>
      <PageHead
        title="Logs"
        note="Application log lines sent by the agent, newest first. Logs are kept for 14 days."
      >
        <ServerSelect
          id="logs-server"
          servers={servers.list}
          value={serverId}
          onChange={(id) => navigate(`/logs?server=${encodeURIComponent(id)}`)}
        />
      </PageHead>

      <section className="panel">
        <div className="panel__head">
          <div className="filters">
            <label className="field field--inline">
              <span className="field__label">Day (UTC)</span>
              <input
                type="date"
                value={day}
                max={todayUtc()}
                onChange={(e) => setDay(e.target.value || todayUtc())}
              />
            </label>
            <select aria-label="Log level" value={level} onChange={(e) => setLevel(e.target.value)}>
              {LEVELS.map(([value, label]) => (
                <option key={label} value={value}>{label}</option>
              ))}
            </select>
            <form
              className="filters"
              onSubmit={(e) => {
                e.preventDefault();
                setSearch(searchInput.trim());
              }}
            >
              <input
                aria-label="Search log messages"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                placeholder="Search messages"
              />
              <button type="submit" className="btn btn--primary">Search</button>
              {search && (
                <button
                  type="button"
                  className="btn"
                  onClick={() => {
                    setSearch("");
                    setSearchInput("");
                  }}
                >
                  Clear
                </button>
              )}
            </form>
          </div>
          <div className="chips">
            {["ERROR", "WARNING", "INFO", "DEBUG"].map((lv) => (
              <span key={lv} className={`badge badge--${lv}`}>{lv} {counts[lv] || 0}</span>
            ))}
          </div>
        </div>

        {error && <p className="error-text">{error}</p>}

        {logs.length > 0 ? (
          <div className="table-wrap table-wrap--tall table-wrap--logs">
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
                    <td className="nowrap num">{new Date(log.log_time).toLocaleTimeString()}</td>
                    <td><span className={`badge badge--${log.level}`}>{log.level}</span></td>
                    <td className="nowrap col-source">{log.source}</td>
                    <td className="msg">{log.message}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty">
            {serverId
              ? `No logs on ${day}${level || search ? " match these filters" : " for this server"}.`
              : "No servers are reporting yet."}
          </p>
        )}

        <div className="log-actions">
          {nextPage && (
            <button type="button" className="btn" onClick={loadOlder} disabled={loadingOlder}>
              {loadingOlder ? "Loading…" : "Load older"}
            </button>
          )}
          {viewingOlder && (
            <button type="button" className="btn" onClick={backToLatest}>Back to latest</button>
          )}
          <span className="query-info">
            Showing {logs.length} lines ({scanned} rows read from one <code>{table}</code> partition
            {nextPage ? ", more available" : ""}).
          </span>
        </div>
      </section>

      <TablesUsed reads={["logs_by_server_day", "logs_by_server_level_day"]} />
    </>
  );
}