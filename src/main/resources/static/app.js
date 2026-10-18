/* FleetEventStream dashboard: dependency-free, polls the REST API every 2 seconds. */
"use strict";

const POLL_MS = 2000;
const numberFormat = new Intl.NumberFormat("en-US");
const state = {
  throughput: new Map(),
  dlqRows: [],
  polling: false,
  lastErrorKeys: "",
};

const $ = (id) => document.getElementById(id);

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "0";
  return numberFormat.format(Math.round(Number(value)));
}

function formatDecimal(value, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "0.0";
  return Number(value).toFixed(digits);
}

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleTimeString([], { hour12: false });
}

function formatTimestamp(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString([], { hour12: false });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  let body = null;
  try {
    body = await response.json();
  } catch (ignored) {
    // The error below includes the status code, which is enough for the UI.
  }
  if (!response.ok) {
    const detail = body && body.message ? body.message : `HTTP ${response.status}`;
    throw new Error(detail);
  }
  return body;
}

async function fetchHealth() {
  const response = await fetch("/api/health/pipeline", { cache: "no-store" });
  let body = null;
  try {
    body = await response.json();
  } catch (ignored) {
    // handled below
  }
  if (!body) throw new Error(`health endpoint returned HTTP ${response.status}`);
  return { httpStatus: response.status, body };
}

function setBadge(element, text, className) {
  element.className = "badge " + className;
  const label = element.querySelector("#healthText") || element.querySelector("span:last-child");
  if (label) label.textContent = text;
  else element.textContent = text;
}

function updateHealth(health) {
  const badge = $("healthBadge");
  if (!health) {
    setBadge(badge, "unreachable", "badge-down");
    return;
  }
  const body = health.body;
  const ok = health.httpStatus === 200 && body.healthy;
  setBadge(badge, body.status || (ok ? "UP" : "DEGRADED"), ok ? "badge-up" : "badge-down");
  if (body.lagThreshold) $("lagThreshold").textContent = formatNumber(body.lagThreshold);
  badge.title = [
    `kafka=${body.kafkaReachable}`,
    `database=${body.databaseReachable}`,
    `lag=${body.totalLag}`,
    body.details && Object.keys(body.details).length ? JSON.stringify(body.details) : "no details",
  ].join(" · ");
}

function mergeThroughput(response) {
  if (!response || !Array.isArray(response.points)) return;
  for (const point of response.points) {
    if (!point.bucket) continue;
    state.throughput.set(point.bucket, Number(point.eventCount) || 0);
  }
  const sortedKeys = [...state.throughput.keys()].sort();
  while (sortedKeys.length > 90) {
    deleteKey(sortedKeys.shift());
  }
}

function deleteKey(key) {
  state.throughput.delete(key);
}

function throughputSeries() {
  return [...state.throughput.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([bucket, count]) => ({ bucket, count }));
}

function updateThroughput(response) {
  mergeThroughput(response);
  const series = throughputSeries();
  const recent = series.slice(-10);
  const nowRate = recent.length
    ? recent.reduce((sum, point) => sum + point.count, 0) / recent.length
    : 0;
  $("throughputNow").textContent = formatDecimal(nowRate, 1);
  $("throughputTotal").textContent = formatNumber(response ? response.totalEvents : 0);
  $("throughputPeak").textContent = formatNumber(response ? response.peakEventsPerSecond : 0);
  $("throughputAvg").textContent = formatDecimal(response ? response.eventsPerSecond : 0, 1);
  drawChart(series);
}

function drawChart(series) {
  const canvas = $("throughputChart");
  const rect = canvas.getBoundingClientRect();
  if (!rect.width) return;
  const dpr = window.devicePixelRatio || 1;
  const width = rect.width;
  const height = 150;
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round(height * dpr);
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  // Grid
  ctx.strokeStyle = "rgba(125, 141, 163, 0.11)";
  ctx.lineWidth = 1;
  for (let i = 1; i <= 3; i++) {
    const y = (height / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  if (!series.length) {
    ctx.fillStyle = "#64748b";
    ctx.font = "12px ui-sans-serif, system-ui";
    ctx.fillText("Waiting for telemetry…", 10, height / 2);
    return;
  }

  const values = series.map((point) => point.count);
  const max = Math.max(...values, 1);
  const padding = 8;
  const usableHeight = height - padding * 2;
  const step = series.length > 1 ? width / (series.length - 1) : width;

  const coords = series.map((point, index) => ({
    x: index * step,
    y: padding + usableHeight - (point.count / max) * usableHeight,
  }));

  const gradient = ctx.createLinearGradient(0, 0, 0, height);
  gradient.addColorStop(0, "rgba(57, 213, 255, 0.30)");
  gradient.addColorStop(1, "rgba(57, 213, 255, 0.00)");
  ctx.beginPath();
  ctx.moveTo(coords[0].x, height);
  coords.forEach((point) => ctx.lineTo(point.x, point.y));
  ctx.lineTo(coords[coords.length - 1].x, height);
  ctx.closePath();
  ctx.fillStyle = gradient;
  ctx.fill();

  ctx.beginPath();
  coords.forEach((point, index) => {
    if (index === 0) ctx.moveTo(point.x, point.y);
    else ctx.lineTo(point.x, point.y);
  });
  ctx.strokeStyle = "#39d5ff";
  ctx.lineWidth = 2;
  ctx.shadowColor = "rgba(57, 213, 255, 0.55)";
  ctx.shadowBlur = 9;
  ctx.stroke();
  ctx.shadowBlur = 0;

  const last = coords[coords.length - 1];
  ctx.beginPath();
  ctx.arc(last.x, last.y, 3, 0, Math.PI * 2);
  ctx.fillStyle = "#8be9ff";
  ctx.fill();

  ctx.fillStyle = "#6f8097";
  ctx.font = "10px ui-monospace, SFMono-Regular, Menlo";
  ctx.fillText(formatNumber(max) + "/s peak", 6, 12);
}

function updateLag(lag) {
  if (!lag) return;
  $("lagGroup").textContent = lag.groupId || "fleet-event-consumers";
  $("lagTotal").textContent = formatNumber(lag.totalLag);
  $("lagMembers").textContent = formatNumber(lag.members);
  const groupBadge = $("groupState");
  groupBadge.textContent = lag.groupState || "UNKNOWN";
  groupBadge.className = "badge " + (lag.groupState === "Stable" || lag.groupState === "STABLE" ? "badge-up" : "badge-amber");

  const threshold = Number($("lagThreshold").textContent.replace(/,/g, "")) || 5000;
  const pct = Math.min(100, (Number(lag.totalLag) / threshold) * 100);
  $("lagMeter").style.width = pct + "%";

  const tbody = $("lagTable");
  tbody.replaceChildren();
  const partitions = lag.partitions || [];
  if (!partitions.length) {
    tbody.appendChild(emptyRow(5, "No partition data."));
    return;
  }
  const maxLag = Math.max(...partitions.map((p) => p.lag), 1);
  for (const partition of partitions) {
    const tr = document.createElement("tr");
    tr.appendChild(cell(String(partition.partition), "mono"));
    tr.appendChild(cell(formatNumber(partition.committedOffset), "num"));
    tr.appendChild(cell(formatNumber(partition.endOffset), "num"));
    tr.appendChild(cell(formatNumber(partition.lag), "num"));
    const barCell = document.createElement("td");
    const bar = document.createElement("div");
    bar.className = "lag-bar";
    const fill = document.createElement("span");
    fill.style.width = Math.max(2, (partition.lag / maxLag) * 100) + "%";
    bar.appendChild(fill);
    barCell.appendChild(bar);
    tr.appendChild(barCell);
    tbody.appendChild(tr);
  }
}

function updateDlq(stats, pipeline) {
  if (stats) {
    $("dlqTotal").textContent = formatNumber(stats.total);
    $("dlqLastHour").textContent = formatNumber(stats.lastHour);
    const reasons = $("topReasons");
    reasons.replaceChildren();
    for (const item of (stats.topFailureReasons || []).slice(0, 3)) {
      const row = document.createElement("div");
      row.className = "reason";
      const label = document.createElement("span");
      label.textContent = item.failureReason || "unknown";
      label.title = item.failureReason || "";
      const count = document.createElement("b");
      count.textContent = formatNumber(item.count);
      row.append(label, count);
      reasons.appendChild(row);
    }
  }
  if (pipeline) {
    $("dlqStored").textContent = formatNumber(pipeline.dlqStored);
    $("rowsPersisted").textContent = formatNumber(pipeline.persisted);
    $("rowsConsumed").textContent = formatNumber(pipeline.consumed);
    $("duplicates").textContent = formatNumber(pipeline.duplicatesSkipped);
    $("failed").textContent = formatNumber(pipeline.failed);
  }
}

function updateDlqRows(page) {
  if (!page) return;
  state.dlqRows = page.items || [];
  $("dlqPageInfo").textContent = `${formatNumber(page.total)} stored · showing ${state.dlqRows.length}`;
  $("replayBtn").disabled = state.dlqRows.length === 0;
  const tbody = $("dlqTable");
  tbody.replaceChildren();
  if (!state.dlqRows.length) {
    tbody.appendChild(emptyRow(6, "No dead letters recorded."));
    return;
  }
  for (const row of state.dlqRows) {
    const tr = document.createElement("tr");
    tr.appendChild(cell(String(row.id), "mono"));
    tr.appendChild(cell(`${row.sourceTopic}-${row.partitionId}@${row.sourceOffset}`, "mono"));
    tr.appendChild(cell(shortClass(row.errorClass), "err-class"));
    const reason = cell(row.failureReason || "unknown", "reason-cell");
    reason.title = row.failureReason || "";
    tr.appendChild(reason);
    tr.appendChild(cell(formatNumber(row.retryCount), "num"));
    tr.appendChild(cell(formatTimestamp(row.failedAt), "mono"));
    tbody.appendChild(tr);
  }
}

function updateSimulator(status) {
  if (!status) return;
  const running = Boolean(status.running);
  $("startBtn").disabled = running;
  $("stopBtn").disabled = !running;
  $("simStatus").textContent = running
    ? `running · ${formatDecimal(status.actualEventsPerSecond, 1)} ev/s · ${formatNumber(status.eventsPublished)} published`
    : `idle · ${formatNumber(status.eventsAttempted)} attempted in last run`;
}

function shortClass(value) {
  if (!value) return "unknown";
  const parts = String(value).split(".");
  return parts[parts.length - 1];
}

function cell(text, className) {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  return td;
}

function emptyRow(colspan, message) {
  const tr = document.createElement("tr");
  const td = document.createElement("td");
  td.colSpan = colspan;
  td.className = "empty";
  td.textContent = message;
  tr.appendChild(td);
  return tr;
}

function showErrors(errors) {
  const banner = $("connectionBanner");
  if (!errors.length) {
    banner.hidden = true;
    state.lastErrorKeys = "";
    return;
  }
  const key = errors.join("|");
  if (key === state.lastErrorKeys) return;
  state.lastErrorKeys = key;
  $("connectionMessage").textContent = errors.join(" · ");
  banner.hidden = false;
}

function toast(message, isError = false) {
  const element = $("toast");
  element.textContent = message;
  element.className = "banner " + (isError ? "banner-error" : "banner-info");
  element.hidden = false;
  window.clearTimeout(toast.timer);
  toast.timer = window.setTimeout(() => { element.hidden = true; }, 6000);
}

async function poll() {
  if (state.polling) return;
  state.polling = true;
  const endpoints = {
    throughput: "/api/stats/throughput?windowSeconds=60",
    lag: "/api/stats/lag",
    dlq: "/api/stats/dlq",
    health: "/api/health/pipeline",
    simulator: "/api/simulator/status",
    pipeline: "/api/stats/pipeline",
    recentDlq: "/api/admin/dlq?page=0&size=8",
  };
  const entries = Object.entries(endpoints);
  const results = await Promise.allSettled(entries.map(([key, url]) =>
    key === "health" ? fetchHealth() : fetchJson(url)));

  const errors = [];
  const values = {};
  results.forEach((result, index) => {
    const key = entries[index][0];
    if (result.status === "fulfilled") {
      values[key] = result.value;
    } else {
      errors.push(`${key}: ${result.reason && result.reason.message ? result.reason.message : "request failed"}`);
    }
  });

  try {
    updateThroughput(values.throughput);
    updateLag(values.lag);
    updateDlq(values.dlq, values.pipeline);
    updateDlqRows(values.recentDlq);
    updateHealth(values.health);
    updateSimulator(values.simulator);
    showErrors(errors);
    $("lastUpdated").textContent = "updated " + new Date().toLocaleTimeString([], { hour12: false });
  } catch (error) {
    showErrors([...errors, "render: " + error.message]);
  } finally {
    state.polling = false;
  }
}

async function startSimulator() {
  const rate = Number($("rate").value);
  const duration = Number($("duration").value);
  if (!Number.isFinite(rate) || rate < 1 || rate > 50000) {
    toast("Rate must be between 1 and 50000 events/sec.", true);
    return;
  }
  if (!Number.isFinite(duration) || duration < 0 || duration > 86400) {
    toast("Duration must be between 0 and 86400 seconds.", true);
    return;
  }
  $("startBtn").disabled = true;
  try {
    const result = await fetchJson("/api/simulator/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ eventsPerSecond: rate, vehicleCount: 20, durationSeconds: duration }),
    });
    toast(`Simulator started at ${result.targetEventsPerSecond} events/sec.`);
    await poll();
  } catch (error) {
    toast("Could not start simulator: " + error.message, true);
    $("startBtn").disabled = false;
  }
}

async function stopSimulator() {
  $("stopBtn").disabled = true;
  try {
    const result = await fetchJson("/api/simulator/stop", { method: "POST" });
    toast(`Simulator stopped after ${formatNumber(result.eventsAttempted)} events.`);
    await poll();
  } catch (error) {
    toast("Could not stop simulator: " + error.message, true);
    $("stopBtn").disabled = false;
  }
}

async function replayDlq() {
  if (!state.dlqRows.length) return;
  const ids = state.dlqRows.map((row) => row.id);
  $("replayBtn").disabled = true;
  try {
    const result = await fetchJson("/api/admin/replay/dlq", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    toast(`DLQ replay: ${result.republished} republished, ${result.skipped} skipped, ${result.missing} missing.`);
    await poll();
  } catch (error) {
    toast("DLQ replay failed: " + error.message, true);
    $("replayBtn").disabled = false;
  }
}

function updateClock() {
  $("clock").textContent = new Date().toLocaleTimeString([], { hour12: false });
}

$("startBtn").addEventListener("click", startSimulator);
$("stopBtn").addEventListener("click", stopSimulator);
$("replayBtn").addEventListener("click", replayDlq);
window.addEventListener("resize", () => drawChart(throughputSeries()));

updateClock();
window.setInterval(updateClock, 1000);
poll();
window.setInterval(poll, POLL_MS);
