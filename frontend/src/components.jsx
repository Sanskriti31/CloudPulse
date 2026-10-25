// Small building blocks shared by the pages.
import { navigate } from "./lib.js";

export function Link({ to, className, children, ...rest }) {
  return (
    <a
      href={to}
      className={className}
      onClick={(event) => {
        if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey) return;
        event.preventDefault();
        navigate(to);
      }}
      {...rest}
    >
      {children}
    </a>
  );
}

export function BrandMark() {
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

export function Gauge({ value, limit, small = false }) {
  if (value === null || value === undefined) {
    return <div className={`gauge ${small ? "gauge--small" : ""}`} />;
  }
  const width = Math.max(0, Math.min(100, value));
  let tone = "";
  if (limit !== undefined) {
    if (value > limit) tone = "gauge__fill--over";
    else if (value >= limit - 10) tone = "gauge__fill--near";
  }
  return (
    <div
      className={`gauge ${small ? "gauge--small" : ""}`}
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

export function StatusPill({ status }) {
  const text = status || "UNKNOWN";
  return <span className={`status status--${text.toLowerCase()}`}>{text}</span>;
}

export function SeverityBadge({ severity }) {
  return <span className={`badge badge--${severity}`}>{severity}</span>;
}

export function PageHead({ title, note, children }) {
  return (
    <div className="page-head">
      <div>
        <h1 className="page-head__title">{title}</h1>
        {note && <p className="page-head__note">{note}</p>}
      </div>
      {children && <div className="page-head__actions">{children}</div>}
    </div>
  );
}

export function ServerSelect({ servers, value, onChange, id = "server-select", allLabel }) {
  return (
    <label className="field field--inline" htmlFor={id}>
      <span className="field__label">Server</span>
      <select id={id} value={value} onChange={(event) => onChange(event.target.value)}>
        {allLabel && <option value="">{allLabel}</option>}
        {!allLabel && servers.length === 0 && <option value="">No servers yet</option>}
        {servers.map((item) => (
          <option key={item.server_id} value={item.server_id}>
            {item.hostname} ({item.health_status}) – {item.server_id.slice(0, 8)}
          </option>
        ))}
      </select>
    </label>
  );
}

// Lists the Cassandra tables a page reads or writes (useful in the viva).
export function TablesUsed({ reads = [], writes = [] }) {
  return (
    <p className="tables-used">
      <strong>Cassandra tables:</strong>{" "}
      {reads.length > 0 && <>reads {reads.map((t) => <code key={t}>{t}</code>)}</>}
      {writes.length > 0 && <> · writes {writes.map((t) => <code key={t}>{t}</code>)}</>}
    </p>
  );
}