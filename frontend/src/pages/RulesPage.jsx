// Page 4: alert rules with full CRUD (create, read, update, delete).
import { useState } from "react";
import { PageHead, SeverityBadge, TablesUsed } from "../components.jsx";
import {
  API_BASE, METRIC_LABELS, ORG_ID, PERCENT_KEYS, formatDateTime,
  getJson, sendJson, usePolling,
} from "../lib.js";

const EMPTY_FORM = {
  rule_name: "",
  metric_type: "cpu_percent",
  operator: ">",
  threshold: "90",
  duration_seconds: "30",
  severity: "WARNING",
  server_id: "",
  enabled: true,
};

function toForm(rule) {
  return {
    rule_name: rule.rule_name || "",
    metric_type: rule.metric_type,
    operator: rule.operator,
    threshold: String(rule.threshold),
    duration_seconds: String(rule.duration_seconds ?? 0),
    severity: rule.severity,
    server_id: rule.server_id || "",
    enabled: rule.enabled !== false,
  };
}

function toBody(form) {
  return {
    org_id: ORG_ID,
    rule_name: form.rule_name.trim(),
    metric_type: form.metric_type,
    operator: form.operator,
    threshold: Number(form.threshold),
    duration_seconds: Number(form.duration_seconds || 0),
    severity: form.severity,
    server_id: form.server_id || null,
    enabled: form.enabled,
    created_by: "dashboard",
  };
}

function unitFor(metricType) {
  return PERCENT_KEYS.includes(metricType) ? "%" : "B/s";
}

function describe(rule) {
  const metric = METRIC_LABELS[rule.metric_type] || rule.metric_type;
  const wait = rule.duration_seconds ? ` for ${rule.duration_seconds} s` : "";
  return `${metric} ${rule.operator} ${rule.threshold}${unitFor(rule.metric_type) === "%" ? "%" : " B/s"}${wait}`;
}

export default function RulesPage({ servers }) {
  const [rules, setRules] = useState([]);
  const [loadError, setLoadError] = useState("");
  const [refresh, setRefresh] = useState(0);

  const [form, setForm] = useState(EMPTY_FORM);
  const [editingId, setEditingId] = useState("");    // "" = creating a new rule
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState("");
  const [message, setMessage] = useState("");

  const [busyId, setBusyId] = useState("");
  const [confirmId, setConfirmId] = useState("");

  const hostnames = Object.fromEntries(servers.list.map((s) => [s.server_id, s.hostname]));

  usePolling(async (isActive) => {
    try {
      const data = await getJson(`${API_BASE}/alerts/rules/?org_id=${ORG_ID}`);
      if (!isActive()) return;
      setRules(data.rules || []);
      setLoadError("");
    } catch (err) {
      if (isActive()) setLoadError(err.message);
    }
  }, 30000, [refresh]);

  function update(field, value) {
    setForm((current) => ({ ...current, [field]: value }));
  }

  function startEdit(rule) {
    setEditingId(rule.rule_id);
    setForm(toForm(rule));
    setFormError("");
    setMessage("");
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function cancelEdit() {
    setEditingId("");
    setForm(EMPTY_FORM);
    setFormError("");
  }

  function closedText(data) {
    return data.alerts_closed ? ` ${data.alerts_closed} open alert(s) closed.` : "";
  }

  async function submit(event) {
    event.preventDefault();
    setFormError("");
    setMessage("");
    if (!form.rule_name.trim()) {
      setFormError("Give the rule a name.");
      return;
    }
    if (form.threshold === "" || !Number.isFinite(Number(form.threshold))) {
      setFormError("Threshold must be a number.");
      return;
    }
    setSaving(true);
    try {
      if (editingId) {
        const data = await sendJson(
          `${API_BASE}/alerts/rules/${editingId}/?org_id=${ORG_ID}`, "PUT", toBody(form)
        );
        setMessage(`Rule "${form.rule_name.trim()}" updated.${closedText(data)}`);
      } else {
        await sendJson(`${API_BASE}/alerts/rules/`, "POST", toBody(form));
        setMessage(`Rule "${form.rule_name.trim()}" created.`);
      }
      setEditingId("");
      setForm(EMPTY_FORM);
      setRefresh((n) => n + 1);
    } catch (err) {
      setFormError(err.message);
    } finally {
      setSaving(false);
    }
  }

  async function toggle(rule) {
    setBusyId(rule.rule_id);
    setMessage("");
    try {
      const body = { ...toBody(toForm(rule)), enabled: !rule.enabled };
      const data = await sendJson(
        `${API_BASE}/alerts/rules/${rule.rule_id}/?org_id=${ORG_ID}`, "PUT", body
      );
      setMessage(`Rule "${rule.rule_name}" ${rule.enabled ? "disabled" : "enabled"}.${closedText(data)}`);
      setRefresh((n) => n + 1);
    } catch (err) {
      setLoadError(err.message);
    } finally {
      setBusyId("");
    }
  }

  async function remove(rule) {
    setBusyId(rule.rule_id);
    setMessage("");
    try {
      const data = await sendJson(
        `${API_BASE}/alerts/rules/${rule.rule_id}/?org_id=${ORG_ID}`, "DELETE"
      );
      setMessage(`Rule "${rule.rule_name}" deleted.${closedText(data)}`);
      if (editingId === rule.rule_id) cancelEdit();
      setRefresh((n) => n + 1);
    } catch (err) {
      setLoadError(err.message);
    } finally {
      setBusyId("");
      setConfirmId("");
    }
  }

  return (
    <>
      <PageHead
        title="Alert rules"
        note="Create, edit, enable, disable and delete the thresholds the alert engine checks."
      />

      <section className="panel">
        <div className="panel__head">
          <h2 className="panel__title">{editingId ? "Edit rule" : "New rule"}</h2>
          {editingId && <span className="muted">Editing an existing rule (UPDATE … IF EXISTS)</span>}
        </div>

        <form className="form" onSubmit={submit}>
          <label className="field field--wide">
            <span className="field__label">Rule name</span>
            <input
              value={form.rule_name}
              onChange={(e) => update("rule_name", e.target.value)}
              placeholder="e.g. High CPU"
              maxLength={100}
            />
          </label>

          <label className="field">
            <span className="field__label">Metric</span>
            <select value={form.metric_type} onChange={(e) => update("metric_type", e.target.value)}>
              {Object.entries(METRIC_LABELS).map(([key, label]) => (
                <option key={key} value={key}>{label}</option>
              ))}
            </select>
          </label>

          <label className="field field--narrow">
            <span className="field__label">Condition</span>
            <select value={form.operator} onChange={(e) => update("operator", e.target.value)}>
              {[">", ">=", "<", "<="].map((op) => (
                <option key={op} value={op}>{op}</option>
              ))}
            </select>
          </label>

          <label className="field">
            <span className="field__label">Threshold ({unitFor(form.metric_type)})</span>
            <input
              type="number"
              step="any"
              value={form.threshold}
              onChange={(e) => update("threshold", e.target.value)}
            />
          </label>

          <label className="field">
            <span className="field__label">For at least (seconds)</span>
            <input
              type="number"
              min="0"
              max="3600"
              value={form.duration_seconds}
              onChange={(e) => update("duration_seconds", e.target.value)}
            />
          </label>

          <label className="field">
            <span className="field__label">Severity</span>
            <select value={form.severity} onChange={(e) => update("severity", e.target.value)}>
              <option value="WARNING">Warning</option>
              <option value="CRITICAL">Critical</option>
            </select>
          </label>

          <label className="field field--wide">
            <span className="field__label">Applies to</span>
            <select value={form.server_id} onChange={(e) => update("server_id", e.target.value)}>
              <option value="">All servers</option>
              {servers.list.map((s) => (
                <option key={s.server_id} value={s.server_id}>
                  {s.hostname} – {s.server_id.slice(0, 8)}
                </option>
              ))}
            </select>
          </label>

          <label className="check">
            <input
              type="checkbox"
              checked={form.enabled}
              onChange={(e) => update("enabled", e.target.checked)}
            />
            Enabled
          </label>

          <div className="form__actions">
            <button type="submit" className="btn btn--primary" disabled={saving}>
              {saving ? "Saving…" : editingId ? "Save changes" : "Create rule"}
            </button>
            {editingId && (
              <button type="button" className="btn" onClick={cancelEdit}>Cancel</button>
            )}
          </div>
        </form>

        {formError && <p className="error-text form__msg">{formError}</p>}
        {message && <p className="ok-text form__msg">{message}</p>}
      </section>

      <section className="section panel">
        <div className="panel__head">
          <h2 className="panel__title">Rules ({rules.length})</h2>
        </div>
        {loadError && <p className="error-text">{loadError}</p>}
        {rules.length === 0 ? (
          <p className="empty">No rules yet. Create one above.</p>
        ) : (
          <div className="table-wrap">
            <table className="table--mid">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Condition</th>
                  <th>Severity</th>
                  <th>Applies to</th>
                  <th>Status</th>
                  <th>Last changed</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {rules.map((rule) => (
                  <tr key={rule.rule_id} className={editingId === rule.rule_id ? "row--editing" : ""}>
                    <td className="strong">{rule.rule_name}</td>
                    <td className="nowrap">{describe(rule)}</td>
                    <td><SeverityBadge severity={rule.severity} /></td>
                    <td>{rule.server_id ? hostnames[rule.server_id] || rule.server_id.slice(0, 8) : "All servers"}</td>
                    <td>
                      <span className={rule.enabled ? "state-resolved" : "muted"}>
                        {rule.enabled ? "Enabled" : "Disabled"}
                      </span>
                    </td>
                    <td className="nowrap">{formatDateTime(rule.updated_at)}</td>
                    <td className="actions">
                      {confirmId === rule.rule_id ? (
                        <>
                          <span className="confirm-text">Delete this rule?</span>
                          <button
                            type="button"
                            className="btn btn--small btn--danger"
                            onClick={() => remove(rule)}
                            disabled={busyId === rule.rule_id}
                          >
                            Yes, delete
                          </button>
                          <button type="button" className="btn btn--small" onClick={() => setConfirmId("")}>
                            Keep
                          </button>
                        </>
                      ) : (
                        <>
                          <button type="button" className="btn btn--small" onClick={() => startEdit(rule)}>
                            Edit
                          </button>
                          <button
                            type="button"
                            className="btn btn--small"
                            onClick={() => toggle(rule)}
                            disabled={busyId === rule.rule_id}
                          >
                            {rule.enabled ? "Disable" : "Enable"}
                          </button>
                          <button
                            type="button"
                            className="btn btn--small btn--danger-outline"
                            onClick={() => setConfirmId(rule.rule_id)}
                          >
                            Delete
                          </button>
                        </>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <TablesUsed
        reads={["alert_rules_by_org"]}
        writes={["alert_rules_by_org (INSERT, UPDATE … IF EXISTS, DELETE … IF EXISTS)"]}
      />
    </>
  );
}