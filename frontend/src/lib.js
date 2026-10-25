// Shared settings, helpers, data hooks and a tiny router (no extra packages).
import { useEffect, useState, useSyncExternalStore } from "react";

export const API_BASE = "http://127.0.0.1:8000/api";
export const ORG_ID = "local";

export const METRIC_LABELS = {
  cpu_percent: "CPU",
  memory_percent: "Memory",
  disk_percent: "Disk",
  network_sent_bps: "Upload",
  network_recv_bps: "Download",
};

export const PERCENT_KEYS = ["cpu_percent", "memory_percent", "disk_percent"];

export function metricUnit(metricType) {
  return PERCENT_KEYS.includes(metricType) ? "%" : " B/s";
}

// ---------------------------------------------------------------- formatting

export function formatBytes(value) {
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

export function secondsAgo(seconds) {
  if (seconds === null || seconds === undefined) return "never";
  if (seconds < 60) return `${seconds} s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)} h ago`;
  return `${Math.round(seconds / 86400)} d ago`;
}

export function formatDateTime(iso) {
  if (!iso) return "–";
  return new Date(iso).toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function formatValue(metricType, value) {
  if (value === null || value === undefined) return "–";
  if (PERCENT_KEYS.includes(metricType)) return `${Number(value).toFixed(1)}%`;
  const bytes = formatBytes(value);
  return bytes ? `${bytes.number} ${bytes.unit}/s` : "–";
}

export function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------- API calls

export async function getJson(url, options) {
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

export function sendJson(url, method, body) {
  return getJson(url, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

// Runs `task(isActive)` now and every `intervalMs`, restarting when deps change.
export function usePolling(task, intervalMs, deps) {
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

// Server list for the whole app, refreshed every 10 s.
export function useServers() {
  const [state, setState] = useState({ list: [], error: "", loaded: false });
  usePolling(async (isActive) => {
    try {
      const data = await getJson(`${API_BASE}/servers/?org_id=${ORG_ID}`);
      if (!isActive()) return;
      const list = [...(data.servers || [])].sort((a, b) =>
        (a.hostname || "").localeCompare(b.hostname || "")
      );
      setState({ list, error: "", loaded: true });
    } catch (err) {
      if (isActive()) setState((s) => ({ ...s, error: err.message, loaded: true }));
    }
  }, 10000, []);
  return state;
}

// ---------------------------------------------------------------- router

function subscribe(callback) {
  window.addEventListener("popstate", callback);
  return () => window.removeEventListener("popstate", callback);
}

function currentUrl() {
  return window.location.pathname + window.location.search;
}

export function useLocation() {
  const url = useSyncExternalStore(subscribe, currentUrl);
  const parsed = new URL(url, window.location.origin);
  return { path: parsed.pathname, query: parsed.searchParams };
}

export function navigate(to, { replace = false } = {}) {
  if (to === currentUrl()) return;
  window.history[replace ? "replaceState" : "pushState"](null, "", to);
  window.dispatchEvent(new PopStateEvent("popstate"));
  if (!replace) window.scrollTo(0, 0);
}

// Lowest enabled "greater than" threshold per metric, for gauge marks.
export function thresholdsFor(rules, serverId) {
  const limits = {};
  for (const rule of rules) {
    if (!rule.enabled || ![">", ">="].includes(rule.operator)) continue;
    if (rule.server_id && rule.server_id !== serverId) continue;
    const current = limits[rule.metric_type];
    if (current === undefined || rule.threshold < current) {
      limits[rule.metric_type] = rule.threshold;
    }
  }
  return limits;
}