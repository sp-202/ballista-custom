// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Ballista web UI: a single-page app over the scheduler's REST API
// (same origin, `/api/...`). No external dependencies.
//
// Tabs: Jobs, Stages, SQL (queries and their DataFusion metrics), Sessions
// (Spark Connect / gRPC clients), Executors, Cluster (resources and scheduler
// parameters), Environment.
"use strict";

const API = "/api";
const REFRESH_MS = 3000;
const STAGES_PAGE_JOBS = 20;
const SAMPLES = 200;

// ---------- helpers ----------
const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const num = (n) => (n == null ? "–" : Number(n).toLocaleString());
const compact = (n) => {
  if (n == null) return "–";
  const a = Math.abs(n);
  if (a >= 1e12) return `${(n / 1e12).toFixed(2)} T`;
  if (a >= 1e9) return `${(n / 1e9).toFixed(2)} B`;
  if (a >= 1e6) return `${(n / 1e6).toFixed(2)} M`;
  if (a >= 1e4) return `${(n / 1e3).toFixed(1)} K`;
  return Number(n).toLocaleString();
};
const bytes = (b) => {
  if (b == null) return "–";
  const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]; let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(i && b < 10 ? 2 : i ? 1 : 0)} ${u[i]}`;
};
const dur = (ms) => {
  if (ms == null || ms < 0) return "–";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)} min`;
  return `${(ms / 3600000).toFixed(1)} h`;
};
const nanos = (ns) => (ns == null ? "–" : dur(ns / 1e6));
const pad2 = (n) => String(n).padStart(2, "0");
// Spark's format, 2026-11-09 20:28:55, with a no-break space so it never wraps.
const time = (t) => {
  if (!t) return "–";
  const d = new Date(t);
  return `<span class="nw">${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}\u00a0${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}</span>`;
};
const ago = (t) => (t ? dur(Date.now() - t) + " ago" : "–");
const pct = (a, b) => (b ? `${((100 * a) / b).toFixed(b > 0 && a / b < 0.1 ? 1 : 0)}%` : "–");
const badge = (s) => `<span class="badge s-${esc(String(s).toLowerCase())}">${esc(s)}</span>`;
const isLive = (status) => ["running", "queued"].includes(String(status).toLowerCase());
const jobDuration = (j) => (j.end_time ? j.end_time - j.start_time : j.start_time ? Date.now() - j.start_time : null);
const metric = (e, type) => (e.metrics || []).find((m) => m.type === type)?.value;
const oneLine = (s, n = 200) => String(s ?? "").replace(/\s+/g, " ").trim().slice(0, n);
const shortId = (id) => (id && id.length > 13 ? id.slice(0, 8) + "…" : id || "–");
const lower = (s) => String(s).toLowerCase();

async function get(path) {
  const r = await fetch(API + path, { cache: "no-store" });
  const ct = r.headers.get("content-type") || "";
  const body = ct.includes("json") ? await r.json() : await r.text();
  if (!r.ok) throw new Error(body?.reason || body || r.statusText);
  return body;
}

// History server: an "application" is one client session; pages below are scoped to the chosen one.
let currentApp = null;
const appOf = (j) => j.app_id || j.session_id || "(unknown)";
const isHistory = () => document.body.classList.contains("history");
const scoped = () => (isHistory() && currentApp ? state.jobs.filter((j) => appOf(j) === currentApp) : state.jobs);
const appPath = (path) => (isHistory() && currentApp ? `#/app/${encodeURIComponent(currentApp)}/${path}` : `#/${path}`);
function appBanner() {
  if (!isHistory() || !currentApp) return "";
  const app = appsOf(state.jobs).find((a) => a.id === currentApp);
  return `<div class="appbar"><a href="#/apps">← Applications</a><span class="mono">${esc(currentApp)}</span>
    ${app ? `<span>${esc(app.name)}</span><span>${esc(app.user || "")}</span><span>${app.jobs.length} jobs</span>` : ""}</div>`;
}

// Re-render without collapsing open <details> sections or losing the focused filter box.
function render(html) {
  const view = $("#view");
  html = appBanner() + html;
  const open = new Set([...view.querySelectorAll("details[id][open]")].map((d) => d.id));
  const closed = new Set([...view.querySelectorAll("details[id]:not([open])")].map((d) => d.id));
  const focus = document.activeElement?.id;
  const caret = document.activeElement?.selectionStart;
  view.innerHTML = html;
  view.querySelectorAll("details[id]").forEach((d) => {
    if (open.has(d.id)) d.setAttribute("open", "");
    else if (closed.has(d.id)) d.removeAttribute("open");
  });
  if (focus) {
    const el = document.getElementById(focus);
    if (el) { el.focus(); if (caret != null && el.setSelectionRange) try { el.setSelectionRange(caret, caret); } catch (_) { /* not text */ } }
  }
  bindTables();
}

// ---------- sortable, filterable tables (state survives auto-refresh) ----------
const tableState = {}; // table id -> { col, dir, filter }

function dtable(id, headers, rows, empty = "Nothing to show") {
  if (!rows.length) return `<div class="empty">${esc(empty)}</div>`;
  const st = tableState[id] || (tableState[id] = { col: null, dir: 1 });
  let list = rows.map((r) => (Array.isArray(r) ? { cells: r } : r));
  if (st.col != null) {
    const key = (r) => { const c = r.cells[st.col]; return c.v ?? c.t ?? c; };
    list = [...list].sort((a, b) => {
      const x = key(a), y = key(b);
      return (typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y))) * st.dir;
    });
  }
  const size = st.size || 25, pages = Math.max(1, Math.ceil(list.length / size));
  st.page = Math.min(st.page || 0, pages - 1);
  const pager = list.length > 10 ? pagerHtml(id, st.page, pages, size, list.length) : "";
  if (list.length > size) list = list.slice(st.page * size, (st.page + 1) * size);
  const th = headers.map((h, i) => {
    const label = h.label ?? h;
    const arrow = st.col === i ? `<span class="arrow">${st.dir > 0 ? "▲" : "▼"}</span>` : "";
    return `<th class="sortable ${h.num ? "num" : ""}" data-table="${esc(id)}" data-col="${i}">${esc(label)}${arrow}</th>`;
  }).join("");
  const body = list.map((r) => `<tr>${r.cells.map((c, i) =>
    `<td class="${headers[i]?.num ? "num" : ""} ${c.cls || ""}">${c.h ?? c}</td>`).join("")}</tr>`).join("");
  return `${pager}<div class="scroll"><table><thead><tr>${th}</tr></thead><tbody>${body}</tbody></table></div>`;
}

// Spark-style pager: "Page: 1 2 >    N Pages. Show [25] items in a page".
function pagerHtml(id, page, pages, size, n) {
  const nums = [];
  for (let i = 0; i < pages; i++) if (i < 3 || i >= pages - 1 || Math.abs(i - page) <= 1) nums.push(i); else if (nums[nums.length - 1] !== -1) nums.push(-1);
  const btn = (i) => i === -1 ? `<span class="pgap">…</span>` : `<button class="pg ${i === page ? "on" : ""}" data-table="${esc(id)}" data-page="${i}">${i + 1}</button>`;
  return `<div class="pager"><span>Page:</span>${page > 0 ? `<button class="pg" data-table="${esc(id)}" data-page="${page - 1}">‹</button>` : ""}${nums.map(btn).join("")}
    ${page < pages - 1 ? `<button class="pg" data-table="${esc(id)}" data-page="${page + 1}">›</button>` : ""}
    <span class="right">${n} rows · ${pages} page${pages > 1 ? "s" : ""}. Show
    <select class="pgsize" data-table="${esc(id)}">${[10, 25, 50, 100, 500].map((v) => `<option ${v === size ? "selected" : ""}>${v}</option>`).join("")}</select> items in a page</span></div>`;
}
const cell = (h, v, cls) => ({ h, v, cls });

function bindTables() {
  document.querySelectorAll("button.pg").forEach((b) => {
    b.onclick = () => { (tableState[b.dataset.table] ||= { col: null, dir: 1 }).page = Number(b.dataset.page); route(); };
  });
  document.querySelectorAll("select.pgsize").forEach((sel) => {
    sel.onchange = () => { const st = (tableState[sel.dataset.table] ||= { col: null, dir: 1 }); st.size = Number(sel.value); st.page = 0; route(); };
  });
  document.querySelectorAll("th.sortable").forEach((th) => {
    th.onclick = () => {
      const st = tableState[th.dataset.table] || (tableState[th.dataset.table] = { col: null, dir: 1 });
      const col = Number(th.dataset.col);
      if (st.col === col) st.dir = -st.dir; else { st.col = col; st.dir = 1; }
      route();
    };
  });
}

// ---------- shared state ----------
const state = { jobs: [], executors: [], sched: null, schedConfig: null, ok: false, slots: { used: 0, total: 0 }, samples: [] };
const stageCache = new Map(); // job id -> stages of a finished job
const taskCache = new Map();  // job id -> { done, total } tasks over all stages

// Spark's "Tasks (for all stages): Succeeded/Total": fetched once for finished jobs.
async function warmTaskCounts(jobs) {
  await Promise.all(jobs.map(async (j) => {
    if (taskCache.has(j.job_id) && !isLive(j.status)) return;
    const { stages } = await get(`/job/${j.job_id}/stages`).catch(() => ({ stages: [] }));
    let done = 0, total = 0;
    for (const s of stages) for (const t of s.tasks || []) if (t) { total++; if (lower(t.status) === "successful") done++; }
    taskCache.set(j.job_id, { done, total });
  }));
}

async function refreshShared() {
  try {
    const [jobs, executors, sched] = await Promise.all([get("/jobs"), get("/executors"), get("/state")]);
    jobs.sort((a, b) => (b.start_time || 0) - (a.start_time || 0));
    Object.assign(state, { jobs, executors, sched, ok: true });
    if (executors.length || jobs.some((j) => isLive(j.status))) await updateSlots();
    sample();
  } catch (e) {
    state.ok = false;
  }
  $("#conn").className = "dot " + (state.ok ? "ok" : "bad");
  $("#conn-text").textContent = state.ok ? "connected" : "scheduler unreachable";
  if (state.sched) {
    // The history server answers /api/state as "history-server": no live executors or scheduler config.
    const history = state.sched.scheduler_id === "history-server";
    document.body.classList.toggle("history", history);
    const brand = document.querySelector(".brand .name");
    if (brand) brand.textContent = history ? "Ballista History Server" : "Ballista";
    const home = document.querySelector(".brand");
    if (home) home.href = history ? "#/apps" : "#/jobs";
    document.title = history ? "Ballista History" : "Ballista UI";
    $("#version").textContent = `v${state.sched.version}${history ? " · history" : ""}`;
    $("#df-version").textContent = `DataFusion ${state.sched.datafusion_version || ""}`.trim();
  }
}

// Task slots in use: the partitions of every running task of the running jobs.
async function updateSlots() {
  const total = state.executors.reduce((a, e) => a + (e.specification?.vcores || 0), 0);
  let used = 0;
  const live = state.jobs.filter((j) => lower(j.status) === "running");
  const lists = await Promise.all(live.map((j) => get(`/job/${j.job_id}/stages`).catch(() => ({ stages: [] }))));
  for (const { stages } of lists) {
    for (const s of stages) for (const t of s.tasks || []) if (t && lower(t.status) === "running") used += t.partition_id?.length || 1;
  }
  state.slots = { used: Math.min(used, total || used), total };
}

function sample() {
  const rss = state.executors.reduce((a, e) => a + (metric(e, "proc_physical_memory") || 0), 0);
  const last = state.samples[state.samples.length - 1];
  if (last && Date.now() - last.t < REFRESH_MS / 2) return;
  state.samples.push({ t: Date.now(), used: state.slots.used, total: state.slots.total, rss,
    active: state.jobs.filter((j) => isLive(j.status)).length });
  if (state.samples.length > SAMPLES) state.samples.shift();
}

function sparkline(values, max) {
  if (values.length < 2) return `<svg class="spark" viewBox="0 0 100 40" preserveAspectRatio="none"></svg>`;
  const hi = Math.max(max || 0, ...values, 1);
  const pts = values.map((v, i) => [(100 * i) / (values.length - 1), 38 - (36 * v) / hi]);
  const line = pts.map((p, i) => `${i ? "L" : "M"}${p[0].toFixed(2)},${p[1].toFixed(2)}`).join("");
  return `<svg class="spark" viewBox="0 0 100 40" preserveAspectRatio="none"><path class="fill" d="${line}L100,40L0,40Z"/><path class="line" d="${line}"/></svg>`;
}

// ---------- DataFusion metrics (parsed from the `plan_format=metrics` plan text) ----------
// A plan line looks like
//   DataSourceExec: file_groups={...}, file_type=parquet, metrics=[output_rows=20.00 M, bytes_scanned=115.3 M, ...]
function parseMetricPlan(text) {
  return (text || "").split("\n").filter((l) => l.trim()).map((line) => {
    const indent = line.match(/^ */)[0].length;
    const body = line.trim();
    const i = body.lastIndexOf(", metrics=[");
    let head = body, metrics = [];
    if (i >= 0 && body.endsWith("]")) {
      head = body.slice(0, i);
      metrics = body.slice(i + 11, -1).split(/, (?=[A-Za-z_][A-Za-z0-9_]*=)/).map((p) => {
        const k = p.indexOf("=");
        return { name: p.slice(0, k), raw: p.slice(k + 1) };
      });
    } else if (body.startsWith("metrics=[")) {
      head = "";
    }
    const colon = head.indexOf(":");
    const op = colon > 0 ? head.slice(0, colon) : head;
    const detail = colon > 0 ? head.slice(colon + 1).trim() : "";
    const m = new Map(metrics.map((x) => [x.name, x.raw]));
    return { indent, op, detail, metrics, m };
  });
}

// "20.00 M" / "547.9 MB" / "1.10s" / "131" -> a number (counts decimal, byte sizes binary, times in ns).
function parseValue(name, raw) {
  const s = String(raw).trim();
  let m = s.match(/^([\d.]+)\s*(ns|µs|us|ms|s)$/);
  if (m) return Number(m[1]) * { ns: 1, "µs": 1e3, us: 1e3, ms: 1e6, s: 1e9 }[m[2]];
  m = s.match(/^([\d.]+)\s*(KB|MB|GB|TB|PB)$/);
  if (m) return Number(m[1]) * 1024 ** ("KMGTP".indexOf(m[2][0]) + 1);
  m = s.match(/^([\d.]+)\s*([KMBT]?)$/);
  if (m) {
    if (m[2] === "B" && /bytes|mem/.test(name)) return Number(m[1]);
    return Number(m[1]) * ({ "": 1, K: 1e3, M: 1e6, B: 1e9, T: 1e12 }[m[2]]);
  }
  return null;
}
const isTime = (raw) => /^[\d.]+\s*(ns|µs|us|ms|s)$/.test(String(raw).trim());
const total = (raw) => { const m = String(raw).match(/(\d+)\s*total\s*→\s*(\d+)\s*matched/); return m ? { total: +m[1], matched: +m[2] } : null; };
const val = (op, name) => (op.m.has(name) ? parseValue(name, op.m.get(name)) : null);

// Totals over every operator of every stage: what a DataFusion user wants to see for a query.
function summarize(ops) {
  const s = { scanRows: 0, scanBytes: 0, scanFileBytes: 0, filesTotal: 0, filesMatched: 0, rgTotal: 0, rgMatched: 0,
    pagesPruned: 0, spillCount: 0, spillBytes: 0, shuffleRows: 0, shuffleWrite: 0, compute: 0, peakMem: 0, worstSkew: null,
    predErrors: 0, openErrors: 0 };
  for (const op of ops) {
    const name = op.op;
    if (/DataSourceExec|ParquetExec|CsvExec/.test(name)) {
      s.scanRows += val(op, "output_rows") || 0;
      s.scanBytes += val(op, "bytes_scanned") || 0;
      const eff = String(op.m.get("scan_efficiency_ratio") || "").match(/\(([\d.]+\s*[KMBT]?)\s*\/\s*([\d.]+\s*[KMBT]?)\)/);
      if (eff) s.scanFileBytes += parseValue("bytes", eff[2]) || 0;
      const f = total(op.m.get("files_ranges_pruned_statistics")); if (f) { s.filesTotal += f.total; s.filesMatched += f.matched; }
      const g = total(op.m.get("row_groups_pruned_statistics")); if (g) { s.rgTotal += g.total; s.rgMatched += g.matched; }
      s.pagesPruned += val(op, "page_index_pages_pruned") || 0;
      s.predErrors += val(op, "predicate_evaluation_errors") || 0;
      s.openErrors += (val(op, "file_open_errors") || 0) + (val(op, "file_scan_errors") || 0);
    }
    if (/ShuffleWriter/.test(name)) {
      s.shuffleRows += val(op, "output_rows") || 0;
      s.shuffleWrite += val(op, "write_time") || 0;
    }
    s.spillCount += val(op, "spill_count") || 0;
    s.spillBytes += val(op, "spilled_bytes") || val(op, "spill_bytes") || 0;
    s.compute += val(op, "elapsed_compute") || 0;
    s.peakMem = Math.max(s.peakMem, val(op, "peak_mem_used") || 0, val(op, "build_mem_used") || 0);
    const sk = String(op.m.get("output_rows_skew") || "").match(/^([\d.]+)%/);
    if (sk && (s.worstSkew == null || +sk[1] > s.worstSkew.pct)) s.worstSkew = { pct: +sk[1], op: name };
  }
  return s;
}

function chips(op, showZero) {
  const hot = (n, v) => (/spill/.test(n) && parseValue(n, v) > 0 ? "spill" : /pruned|matched|scan_efficiency|bytes_scanned/.test(n) ? "scan"
    : /errors/.test(n) && parseValue(n, v) > 0 ? "bad" : "");
  return `<div class="metricchips">${op.metrics.filter((x) => showZero || !/^(0|0 total → 0 matched|0\.00%?|0ns|0 B)$/.test(String(x.raw).trim()))
    .map((x) => `<span class="m ${hot(x.name, x.raw)}" title="${esc(x.name)}"><b>${esc(x.name)}</b>${esc(x.raw)}</span>`).join("")}</div>`;
}

function operatorTable(id, ops) {
  const rows = ops.map((op) => {
    const out = val(op, "output_rows"), t = val(op, "elapsed_compute");
    const spill = (val(op, "spill_count") || 0) > 0;
    return [
      cell(`<span class="opname" style="padding-left:${Math.min(op.indent, 30) * 4}px">${esc(op.op.replace(/Exec$/, ""))}</span>
        <div class="opdetail" style="padding-left:${Math.min(op.indent, 30) * 4}px" title="${esc(op.detail)}">${esc(oneLine(op.detail, 120))}</div>`, op.op),
      cell(out == null ? "–" : compact(out), out ?? -1, "num"),
      cell(t == null ? "–" : nanos(t), t ?? -1, "num"),
      cell(chips(op, false) + (spill ? "" : ""), 0),
    ];
  });
  return dtable(id, ["Operator", { label: "Output rows", num: true }, { label: "Compute", num: true }, "Metrics"], rows, "No operator metrics yet");
}

function summaryCards(s) {
  const rgPruned = s.rgTotal - s.rgMatched, filePruned = s.filesTotal - s.filesMatched;
  const card = (cls, k, v, n) => `<div class="card ${cls || ""}"><div class="k">${k}</div><div class="v">${v}</div><div class="n">${n || ""}</div></div>`;
  const cards = [];
  if (s.scanRows) cards.push(card("", "Rows scanned", compact(s.scanRows), s.scanBytes ? `${compact(s.scanBytes)} bytes decoded` : ""));
  if (s.scanFileBytes) cards.push(card("", "Scan efficiency", pct(s.scanBytes, s.scanFileBytes), `${compact(s.scanBytes)} of ${compact(s.scanFileBytes)} bytes`));
  if (s.rgTotal) cards.push(card("", "Row groups pruned", pct(rgPruned, s.rgTotal), `${num(rgPruned)} of ${num(s.rgTotal)} · ${num(filePruned)} file ranges`));
  cards.push(card(s.spillCount ? "warn" : "", "Spilled to disk", s.spillCount ? bytes(s.spillBytes) : "None", s.spillCount ? `${num(s.spillCount)} spills` : "all in memory"));
  if (s.shuffleRows) cards.push(card("", "Shuffled rows", compact(s.shuffleRows), s.shuffleWrite ? `${nanos(s.shuffleWrite)} writing` : ""));
  cards.push(card("", "Operator compute", nanos(s.compute), "summed over partitions"));
  if (s.peakMem) cards.push(card("", "Peak operator memory", bytes(s.peakMem), "join build / sort"));
  if (s.worstSkew && s.worstSkew.pct > 50) cards.push(card("warn", "Output skew", `${s.worstSkew.pct.toFixed(0)}%`, esc(s.worstSkew.op.replace(/Exec$/, ""))));
  if (s.predErrors || s.openErrors) cards.push(card("warn", "Scan errors", num(s.predErrors + s.openErrors), "predicate / file errors"));
  return `<div class="cards">${cards.join("")}</div>`;
}

// ---------- timelines ----------
function timeline(rows, t0, t1) {
  const span = Math.max(1, t1 - t0);
  const body = rows.map((r) => {
    const left = (100 * (r.start - t0)) / span;
    const width = Math.max(0.3, (100 * (r.end - r.start)) / span);
    return `<div class="tl-row"><div class="label">${r.label}</div>
      <div class="tl-track"><i class="${esc(r.cls || "")}" style="left:${left}%;width:${width}%" title="${esc(r.title || "")}"></i></div>
      <div class="dur">${dur(r.end - r.start)}</div></div>`;
  }).join("");
  return `<div class="timeline">${body}<div class="tl-axis"><div></div><div><span>0</span><span>${dur(span)}</span></div><div></div></div></div>`;
}

function stageWindow(stage) {
  const tasks = (stage.tasks || []).filter((t) => t && t.start_exec_time);
  if (!tasks.length) return null;
  return {
    start: Math.min(...tasks.map((t) => t.start_exec_time)),
    end: Math.max(...tasks.map((t) => t.end_exec_time || Date.now())),
  };
}

// ---------- DAG (parsed from the scheduler's DOT output) ----------
function parseDot(dot) {
  const stages = new Map();
  const nodeRe = /(stage_(\d+)(?:_\d+)*)\s*\[[^\]]*?label\s*=\s*"((?:[^"\\]|\\.)*)"/gs;
  for (const m of dot.matchAll(nodeRe)) {
    const id = Number(m[2]);
    const label = m[3];
    const op = label.split(/\\n|\n/)[0].trim();
    if (!stages.has(id)) stages.set(id, []);
    stages.get(id).push({ node: m[1], op, label });
  }
  for (const ops of stages.values()) ops.sort((a, b) => a.node.length - b.node.length);
  const edges = [];
  const add = (from, to) => {
    if (from !== to && stages.has(from) && !edges.some((e) => e.from === from && e.to === to)) edges.push({ from, to });
  };
  const edgeRe = /stage_(\d+)(?:_\d+)*\s*->\s*stage_(\d+)(?:_\d+)*/g;
  for (const m of dot.matchAll(edgeRe)) add(Number(m[1]), Number(m[2]));
  for (const [id, ops] of stages) {
    for (const o of ops) for (const m of o.label.matchAll(/upstream_stage:\s*(\d+)/g)) add(Number(m[1]), id);
  }
  return { stages, edges };
}

// info: Map stage id -> { status, rows (output), inRows, scan, spilled }
function dagSvg(dag, info, jobId) {
  const ids = [...dag.stages.keys()].sort((a, b) => a - b);
  if (!ids.length) return `<div class="empty">No stage graph available.</div>`;
  const preds = new Map(ids.map((id) => [id, []]));
  dag.edges.forEach((e) => preds.get(e.to)?.push(e.from));
  const layer = new Map();
  const depth = (id, seen = new Set()) => {
    if (layer.has(id)) return layer.get(id);
    if (seen.has(id)) return 0;
    seen.add(id);
    const d = Math.max(-1, ...preds.get(id).map((p) => depth(p, seen))) + 1;
    layer.set(id, d);
    return d;
  };
  ids.forEach((id) => depth(id));
  const layers = [];
  ids.forEach((id) => (layers[layer.get(id)] ||= []).push(id));
  const rank = new Map();
  layers.forEach((col) => {
    const key = (id) => {
      const ps = preds.get(id).filter((p) => rank.has(p));
      return ps.length ? ps.reduce((a, p) => a + rank.get(p), 0) / ps.length : id;
    };
    col.sort((a, b) => key(a) - key(b) || a - b);
    col.forEach((id, i) => rank.set(id, i));
  });

  const W = 196, OPS = 4, LINE = 15, PAD = 22, GAPX = 104, GAPY = 22;
  const H = (id) => 30 + Math.min(OPS, dag.stages.get(id).length) * LINE + PAD;
  const pos = new Map();
  let maxY = 0;
  layers.forEach((col, x) => {
    let y = 0;
    col.forEach((id) => { pos.set(id, { x: x * (W + GAPX), y, h: H(id) }); y += H(id) + GAPY; });
    maxY = Math.max(maxY, y);
  });
  const width = layers.length * (W + GAPX) - GAPX;

  const edges = dag.edges.map(({ from, to }) => {
    const a = pos.get(from), b = pos.get(to);
    if (!a || !b) return "";
    const x1 = a.x + W, y1 = a.y + a.h / 2, x2 = b.x - 6, y2 = b.y + b.h / 2, mx = (x1 + x2) / 2;
    const rows = info.get(String(from))?.rows;
    const label = rows == null ? "" : `<text class="edgerows" x="${mx}" y="${(y1 + y2) / 2 - 5}">${esc(compact(rows))} rows</text>`;
    return `<path class="edge" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}"/>
      <path class="arrow" d="M${x2},${y2 - 4} L${x2 + 6},${y2} L${x2},${y2 + 4} Z"/>${label}`;
  }).join("");
  const boxes = ids.map((id) => {
    const p = pos.get(id), ops = dag.stages.get(id), st = info.get(String(id)) || {};
    const status = lower(st.status || "");
    const cls = ["stage", status, st.scan ? "scan" : "", st.spilled ? "spilled" : ""].join(" ");
    const lines = ops.slice(0, OPS).map((o, i) =>
      `<text class="op" x="${p.x + 10}" y="${p.y + 44 + i * LINE}">${esc(o.op.replace(/Exec\b.*/, "").slice(0, 26))}</text>`).join("");
    const more = `<title>${esc(ops.map((o) => o.op).join("\n"))}</title>`;
    const rows = st.rows != null ? `${st.inRows != null ? compact(st.inRows) + " → " : ""}${compact(st.rows)} rows` : "";
    return `<a href="#/jobs/${esc(jobId)}/stages/${id}"><g class="${esc(cls)}">${more}
      <rect class="box" x="${p.x}" y="${p.y}" width="${W}" height="${p.h}" rx="7"/>
      <rect class="hdr" x="${p.x + 0.65}" y="${p.y + 0.65}" width="${W - 1.3}" height="26" rx="6"/>
      <text class="title" x="${p.x + 10}" y="${p.y + 18}">Stage ${id}${status ? " · " + esc(st.status) : ""}</text>${lines}
      <text class="rows" x="${p.x + 10}" y="${p.y + p.h - 8}">${esc(rows)}${st.spilled ? "  ⚠ spilled" : ""}</text></g></a>`;
  }).join("");
  return `<svg width="${width + 2}" height="${maxY}" viewBox="-1 -1 ${width + 2} ${maxY + 2}" role="img" aria-label="Stage DAG">${edges}${boxes}</svg>
    <div class="legend"><span><i style="background:var(--panel);border:1.5px solid var(--ok)"></i>succeeded</span>
    <span><i style="background:var(--panel);border:1.5px solid var(--run)"></i>running</span>
    <span><i style="background:var(--panel);border:1.5px dashed var(--warn)"></i>spilled</span>
    <span>edge label: rows shuffled</span></div>`;
}

async function jobDetails(jobId, finishedOk = true) {
  const [job, stagesResp, dot] = await Promise.all([
    get(`/job/${jobId}`),
    get(`/job/${jobId}/stages?plan_format=metrics`),
    get(`/job/${jobId}/dot`).catch(() => ""),
  ]);
  const stages = stagesResp.stages.sort((a, b) => Number(a.stage_id) - Number(b.stage_id));
  const parsed = new Map(stages.map((s) => [String(s.stage_id), parseMetricPlan(s.stage_plan)]));
  const info = new Map(stages.map((s) => {
    const ops = parsed.get(String(s.stage_id)) || [];
    return [String(s.stage_id), { status: s.stage_status, rows: s.output_rows, inRows: s.input_rows,
      scan: ops.some((o) => /DataSourceExec/.test(o.op)), spilled: ops.some((o) => (val(o, "spill_count") || 0) > 0) }];
  }));
  return { job, stages, parsed, info, dag: parseDot(dot), allOps: [...parsed.values()].flat() };
}

// ---------- shared row builders ----------
function sqlOf(j) { return j.job_name || ""; }

function jobRow(j) {
  const live = isLive(j.status);
  return [
    cell(`<a class="mono" href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a>`, j.job_id),
    cell(`<a class="sqlcell" href="#/sql/${esc(j.job_id)}" title="${esc(sqlOf(j) || j.job_status)}">${esc(oneLine(sqlOf(j)) || j.job_status)}</a>`, sqlOf(j)),
    cell(time(j.start_time), j.start_time || 0),
    cell(dur(jobDuration(j)), jobDuration(j) ?? -1, "num"),
    cell(`<div class="progress"><i class="${live ? "run" : ""}" style="width:${j.percent_complete}%"></i><span>${j.completed_stages}/${j.num_stages}</span></div>`, j.completed_stages),
    (() => { const t = taskCache.get(j.job_id); return t ? cell(`<div class="progress"><i class="${live ? "run" : ""}" style="width:${t.total ? (100 * t.done) / t.total : 0}%"></i><span>${t.done}/${t.total}</span></div>`, t.done) : cell("–", 0); })(),
    cell(badge(j.status), j.status),
  ];
}
const jobHeaders = ["Job Id", "Description", "Submitted", { label: "Duration", num: true }, "Stages: Succeeded/Total", "Tasks: Succeeded/Total", "Status"];

function clusterCards() {
  const ex = state.executors, rss = ex.reduce((a, e) => a + (metric(e, "proc_physical_memory") || 0), 0);
  const slots = state.slots, jobs = state.jobs;
  const act = jobs.filter((j) => isLive(j.status)).length;
  const card = (k, v, n) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div><div class="n">${n}</div></div>`;
  return `<div class="cards">
    ${card("Executors", ex.length, `${ex.reduce((a, e) => a + (e.specification?.vcores || 0), 0)} task slots`)}
    ${card("Slots in use", `${slots.used}<small> / ${slots.total}</small>`, `<div class="bar"><i class="${slots.used / (slots.total || 1) > 0.85 ? "hot" : ""}" style="width:${pct(slots.used, slots.total)}"></i></div>`)}
    ${card("Executor memory", bytes(rss), `peak ${bytes(ex.reduce((a, e) => a + (metric(e, "peak_physical_memory") || 0), 0))}`)}
    ${card("Running jobs", act, `${jobs.length} total`)}
  </div>`;
}

// ---------- pages ----------
async function jobsPage() {
  const jobs = scoped();
  const active = jobs.filter((j) => isLive(j.status));
  const done = jobs.filter((j) => lower(j.status) === "completed");
  const failed = jobs.filter((j) => ["failed", "cancelled"].includes(lower(j.status)));
  await warmTaskCounts([...active, ...done.slice(0, 100), ...failed.slice(0, 50)]);
  const recent = jobs.slice(0, 30).filter((j) => j.start_time).reverse();
  const t0 = Math.min(...recent.map((j) => j.start_time)), t1 = Math.max(...recent.map((j) => j.end_time || Date.now()));
  const tl = recent.length ? timeline(recent.map((j) => ({
    label: `<a href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a>`,
    start: j.start_time, end: j.end_time || Date.now(), cls: lower(j.status), title: oneLine(j.job_name) || j.job_status,
  })), t0, t1) : `<div class="empty">No jobs yet.</div>`;
  render(`
    <h1>Ballista Jobs</h1>
    <ul class="summary">
      <li><span>Scheduler:</span>${esc(state.sched?.scheduler_id || "–")}</li>
      ${state.sched?.started ? `<li><span>Total Uptime:</span>${dur(Date.now() - state.sched.started)}</li>` : ""}
      <li><span>Scheduling Mode:</span>${esc(state.sched?.scheduling_policy || "–")}</li>
      <li><span>Completed Jobs:</span><a href="#completed-jobs">${done.length}</a></li><li><span>Failed Jobs:</span>${failed.length}</li>
    </ul>
    ${document.body.classList.contains("history") ? "" : clusterCards()}
    <details class="section" id="jobs-timeline"><summary>Event Timeline</summary><div class="panel">${tl}</div></details>
    <h2>Active Jobs <span class="count">(${active.length})</span></h2><div class="panel">${dtable("jobs-active", jobHeaders, active.map(jobRow), "No active jobs")}</div>
    <h2 id="completed-jobs">Completed Jobs <span class="count">(${done.length})</span></h2><div class="panel">${dtable("jobs-done", jobHeaders, done.map(jobRow), "No completed jobs")}</div>
    <h2>Failed Jobs <span class="count">(${failed.length})</span></h2><div class="panel">${dtable("jobs-failed", jobHeaders, failed.map(jobRow), "No failed jobs")}</div>`);
}

function stageRows(jobId, stages) {
  return stages.map((s) => {
    const w = stageWindow(s);
    const tasks = (s.tasks || []).filter(Boolean);
    const done = tasks.filter((t) => lower(t.status) === "successful").length;
    const op = (s.stage_plan || "").split("\n")[0].replace(/:.*/, "").trim();
    return [
      cell(`<a href="#/jobs/${esc(jobId)}/stages/${esc(s.stage_id)}">${esc(s.stage_id)}</a>`, Number(s.stage_id)),
      cell(`<span class="mono">${esc(op)}</span>`, op),
      cell(badge(s.stage_status), s.stage_status),
      cell(w ? dur(w.end - w.start) : "–", w ? w.end - w.start : -1, "num"),
      cell(`<div class="progress"><i style="width:${tasks.length ? (100 * done) / tasks.length : 0}%"></i><span>${done}/${tasks.length}</span></div>`, done),
      cell(num(s.input_rows), s.input_rows, "num"), cell(num(s.output_rows), s.output_rows, "num"),
      cell(esc(s.elapsed_compute || "–"), 0, "num"),
    ];
  });
}
const stageHeaders = ["Stage", "Root operator", "Status", { label: "Duration", num: true }, "Tasks: Succeeded/Total",
  { label: "Input rows", num: true }, { label: "Output rows", num: true }, { label: "Compute", num: true }];

async function jobPage(jobId) {
  const { job, stages, parsed, info, dag, allOps } = await jobDetails(jobId);
  const windows = stages.map((s) => [s, stageWindow(s)]).filter(([, w]) => w);
  const t0 = job.start_time || Math.min(...windows.map(([, w]) => w.start));
  const t1 = job.end_time || Date.now();
  const tl = windows.length ? timeline(windows.map(([s, w]) => ({
    label: `<a href="#/jobs/${esc(jobId)}/stages/${esc(s.stage_id)}">Stage ${esc(s.stage_id)}</a>`,
    start: w.start, end: w.end, cls: lower(s.stage_status),
    title: `${s.stage_status}, ${(s.tasks || []).filter(Boolean).length} tasks`,
  })), t0, t1) : `<div class="empty">No task timing yet.</div>`;
  const plans = [["Logical plan", job.logical_plan], ["Physical plan", job.physical_plan], ["Final stage plan", job.stage_plan]]
    .filter(([, p]) => p)
    .map(([t, p], i) => `<details class="section" id="plan-${i}"><summary>${t}</summary><div class="panel"><pre class="plan">${esc(p)}</pre></div></details>`).join("");
  render(`
    <div class="crumbs"><a href="#/jobs">Jobs</a> / ${esc(jobId)}</div>
    <h1>Details for Job ${esc(jobId)}</h1>
    <ul class="summary">
      <li><span>Status:</span>${badge(job.status)}</li>
      <li><span>Submitted:</span>${time(job.start_time)}</li>
      <li><span>Duration:</span>${dur(jobDuration(job))}</li>
      <li><span>Stages:</span>${job.completed_stages}/${job.num_stages}</li>
      ${job.session_id ? `<li><span>Session:</span><a href="#/sessions/${esc(job.session_id)}">${esc(shortId(job.session_id))}</a></li>` : ""}
      <li><a href="#/sql/${esc(jobId)}">SQL details</a></li>
      <li><a href="#/environment/${esc(jobId)}">Configuration</a></li>
    </ul>
    <div class="sub">${esc(job.job_status)}</div>
    ${job.job_name ? `<pre class="sql">${esc(job.job_name)}</pre>` : ""}
    ${isLive(job.status) ? `<div class="toolbar"><button class="btn danger" id="cancel">Cancel job</button></div>` : ""}
    ${summaryCards(summarize(allOps))}
    <details class="section" id="job-timeline"><summary>Event Timeline</summary><div class="panel">${tl}</div></details>
    <details class="section" id="job-dag" open><summary>DAG Visualization</summary><div class="panel dag">${dagSvg(dag, info, jobId)}</div></details>
    <h2>Stages <span class="count">(${stages.length})</span></h2><div class="panel">${dtable("job-stages", stageHeaders, stageRows(jobId, stages), "No stages")}</div>
    ${plans}`);
  $("#cancel")?.addEventListener("click", async () => {
    await fetch(`${API}/job/${jobId}`, { method: "PATCH" });
    route();
  });
}

function quantileRow(label, p, fmt) {
  if (!p) return `<tr><td>${label}</td><td colspan="5" class="num">–</td></tr>`;
  return `<tr><td>${label}</td>${[p.min, p.p25, p.median, p.p75, p.max].map((v) => `<td class="num">${fmt(v)}</td>`).join("")}</tr>`;
}

async function stagePage(jobId, stageId) {
  const { stages } = await get(`/job/${jobId}/stages?plan_format=metrics`);
  const stage = stages.find((s) => String(s.stage_id) === String(stageId));
  if (!stage) throw new Error(`stage ${stageId} not found in job ${jobId}`);
  const tasks = (stage.tasks || []).filter(Boolean).sort((a, b) => a.id - b.id);
  const w = stageWindow(stage);
  const ops = parseMetricPlan(stage.stage_plan);
  const taskTl = w ? timeline(tasks.filter((t) => t.start_exec_time).map((t) => ({
    label: `Task ${t.id}`, start: t.start_exec_time, end: t.end_exec_time || Date.now(),
    cls: lower(t.status), title: `partitions ${t.partition_id.join(",")}`,
  })), w.start, w.end) : `<div class="empty">No task timing yet.</div>`;
  const taskRows = tasks.map((t) => [
    cell(t.id, t.id, "num"), cell(badge(t.status), t.status),
    cell(`<span class="mono">${esc(t.partition_id.length > 8 ? t.partition_id.slice(0, 8).join(",") + "…" : t.partition_id.join(","))}</span>`, t.partition_id.length),
    cell(time(t.launch_time), t.launch_time), cell(dur(t.exec_duration), t.exec_duration, "num"),
    cell(num(t.input_rows), t.input_rows, "num"), cell(num(t.output_rows), t.output_rows, "num"),
  ]);
  const med = stage.task_duration_percentiles?.median, max = stage.task_duration_percentiles?.max;
  const straggler = med && max && max > 3 * med && max > 1000;
  render(`
    <div class="crumbs"><a href="#/jobs">Jobs</a> / <a href="#/jobs/${esc(jobId)}">${esc(jobId)}</a> / Stage ${esc(stageId)}</div>
    <h1>Details for Stage ${esc(stageId)}</h1>
    <ul class="summary">
      <li><span>Status:</span>${badge(stage.stage_status)}</li>
      <li><span>Duration:</span>${w ? dur(w.end - w.start) : "–"}</li>
      <li><span>Tasks:</span>${tasks.length}</li>
      <li><span>Input rows:</span>${num(stage.input_rows)}</li>
      <li><span>Output rows:</span>${num(stage.output_rows)}</li>
      <li><span>Compute:</span>${esc(stage.elapsed_compute || "–")}</li>
    </ul>
    ${straggler ? `<div class="card warn"><div class="k">Straggler tasks</div><div class="n">slowest task ${dur(max)} is ${(max / med).toFixed(1)}× the median ${dur(med)} — likely a skewed partition</div></div>` : ""}
    ${summaryCards(summarize(ops))}
    <h2>Operators <span class="count">(DataFusion metrics, summed over partitions)</span></h2>
    <div class="panel">${operatorTable("stage-ops", ops)}</div>
    <h2>Summary Metrics for ${tasks.length} Tasks</h2>
    <div class="panel"><table><thead><tr><th>Metric</th><th class="num">Min</th><th class="num">25th percentile</th><th class="num">Median</th><th class="num">75th percentile</th><th class="num">Max</th></tr></thead>
      <tbody>${quantileRow("Duration", stage.task_duration_percentiles, dur)}${quantileRow("Input rows", stage.task_input_percentiles, num)}</tbody></table></div>
    <details class="section" id="stage-plan"><summary>Stage plan with metrics</summary><div class="panel"><pre class="plan">${esc(stage.stage_plan || "")}</pre></div></details>
    <details class="section" id="task-timeline"><summary>Event Timeline</summary><div class="panel">${taskTl}</div></details>
    <h2>Tasks <span class="count">(${tasks.length})</span></h2>
    <div class="panel">${dtable("stage-tasks", ["Index", "Status", "Partitions", "Launch time", { label: "Duration", num: true }, { label: "Input rows", num: true }, { label: "Output rows", num: true }], taskRows, "No tasks")}</div>`);
}

async function stagesPage() {
  const jobs = scoped().slice(0, STAGES_PAGE_JOBS);
  const lists = await Promise.all(jobs.map(async (j) => {
    if (stageCache.has(j.job_id)) return [j, stageCache.get(j.job_id)];
    const { stages } = await get(`/job/${j.job_id}/stages`).catch(() => ({ stages: [] }));
    if (!isLive(j.status)) stageCache.set(j.job_id, stages);
    return [j, stages];
  }));
  const rows = lists.flatMap(([j, stages]) => [...stages]
    .sort((a, b) => Number(a.stage_id) - Number(b.stage_id))
    .map((s) => [cell(`<a class="mono" href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a>`, j.job_id), ...stageRows(j.job_id, [s])[0]]));
  render(`
    <h1>Stages for the last ${jobs.length} jobs</h1>
    <div class="panel">${dtable("stages-all", ["Job", ...stageHeaders], rows, "No stages")}</div>`);
}

// ---- SQL tab: every query the cluster ran, with its text and DataFusion metrics ----
async function sqlPage() {
  const q = (tableState["sql-filter"]?.filter || "").toLowerCase();
  const status = tableState["sql-status"]?.filter || "all";
  const jobs = scoped().filter((j) => (status === "all" || lower(j.status) === status) &&
    (!q || sqlOf(j).toLowerCase().includes(q) || j.job_id.toLowerCase().includes(q) || (j.session_id || "").includes(q)));
  const rows = jobs.map((j) => [
    cell(`<a class="mono" href="#/sql/${esc(j.job_id)}">${esc(j.job_id)}</a>`, j.job_id),
    cell(`<a class="sqlcell" href="#/sql/${esc(j.job_id)}" title="${esc(sqlOf(j))}">${esc(oneLine(sqlOf(j), 240)) || '<span class="faint">(no description)</span>'}</a>`, sqlOf(j)),
    cell(time(j.start_time), j.start_time || 0),
    cell(dur(jobDuration(j)), jobDuration(j) ?? -1, "num"),
    cell(j.session_id ? `<a class="mono" href="#/sessions/${esc(j.session_id)}">${esc(shortId(j.session_id))}</a>` : "–", j.session_id || ""),
    cell(`${j.completed_stages}/${j.num_stages}`, j.completed_stages, "num"),
    cell(badge(j.status), j.status),
  ]);
  render(`
    <h1>SQL / DataFrame queries</h1>
    <div class="sub">One entry per Ballista job. Spark Connect queries show their SQL text; DataFrame operations show the operation kind.</div>
    <div class="toolbar">
      <input class="filter" id="sql-filter" placeholder="Filter by SQL text, job id or session…" value="${esc(tableState["sql-filter"]?.filter || "")}">
      <select class="filter" id="sql-status" style="min-width:140px">${["all", "running", "completed", "failed", "cancelled", "queued"]
        .map((s) => `<option value="${s}" ${s === status ? "selected" : ""}>${s}</option>`).join("")}</select>
      <span class="muted">${jobs.length} of ${scoped().length} queries</span>
    </div>
    <div class="panel">${dtable("sql-list", ["ID", "Description", "Submitted", { label: "Duration", num: true }, "Session", { label: "Stages", num: true }, "Status"], rows, "No queries")}</div>`);
  $("#sql-filter").addEventListener("input", (e) => { (tableState["sql-filter"] ||= {}).filter = e.target.value; route(); });
  $("#sql-status").addEventListener("change", (e) => { (tableState["sql-status"] ||= {}).filter = e.target.value; route(); });
}

async function sqlDetailPage(jobId) {
  const { job, stages, parsed, info, dag, allOps } = await jobDetails(jobId);
  const s = summarize(allOps);
  const showZero = tableState["sql-zero"]?.on;
  const perStage = stages.map((st) => {
    const ops = parsed.get(String(st.stage_id)) || [];
    const sm = summarize(ops);
    const tag = [sm.scanRows ? `scan ${compact(sm.scanRows)} rows` : "", sm.spillCount ? `⚠ spilled ${bytes(sm.spillBytes)}` : ""].filter(Boolean).join(" · ");
    return `<details class="section" id="sqlst-${esc(st.stage_id)}"><summary>Stage ${esc(st.stage_id)} · ${esc(st.stage_status)} · ${compact(st.output_rows)} rows out
      <span class="faint">${esc(tag)}</span> <a href="#/jobs/${esc(jobId)}/stages/${esc(st.stage_id)}">details →</a></summary>
      <div class="panel">${operatorTable("sqlops-" + st.stage_id, ops)}</div></details>`;
  }).join("");
  const plans = [["Logical plan", job.logical_plan], ["Physical plan", job.physical_plan]].filter(([, p]) => p)
    .map(([t, p], i) => `<details class="section" id="sqlplan-${i}"><summary>${t}</summary><div class="panel"><pre class="plan">${esc(p)}</pre></div></details>`).join("");
  render(`
    <div class="crumbs"><a href="#/sql">SQL</a> / ${esc(jobId)}</div>
    <h1>Query ${esc(jobId)}</h1>
    <ul class="summary">
      <li><span>Status:</span>${badge(job.status)}</li>
      <li><span>Submitted:</span>${time(job.start_time)}</li>
      <li><span>Duration:</span>${dur(jobDuration(job))}</li>
      <li><span>Stages:</span>${job.completed_stages}/${job.num_stages}</li>
      ${job.session_id ? `<li><span>Session:</span><a href="#/sessions/${esc(job.session_id)}">${esc(job.session_id)}</a></li>` : ""}
      <li><a href="#/jobs/${esc(jobId)}">Job page</a></li>
    </ul>
    <div class="sub">${esc(job.job_status)}</div>
    ${job.job_name ? `<div class="toolbar"><button class="btn" id="copy">Copy SQL</button></div><pre class="sql" id="sqltext">${esc(job.job_name)}</pre>`
      : `<div class="sub faint">No SQL text was sent with this job (it was not submitted through Spark Connect).</div>`}
    ${summaryCards(s)}
    <h2>Query plan <span class="count">(stage graph · rows between stages)</span></h2>
    <div class="panel dag">${dagSvg(dag, info, jobId)}</div>
    <h2>DataFusion operator metrics <label class="muted" style="font-weight:400;font-size:12px"><input type="checkbox" id="zero" ${showZero ? "checked" : ""}> include zero metrics</label></h2>
    ${perStage || '<div class="empty">No stages yet.</div>'}
    ${plans}`);
  $("#copy")?.addEventListener("click", () => navigator.clipboard?.writeText(job.job_name));
  $("#zero")?.addEventListener("change", (e) => { (tableState["sql-zero"] ||= {}).on = e.target.checked; route(); });
}

// ---- History server home: one row per application (client session), like Spark's ----
function appsOf(jobs) {
  const by = new Map();
  for (const j of jobs) {
    const id = appOf(j);
    const a = by.get(id) || { id, jobs: [], started: Infinity, ended: 0, user: "", failed: 0, appName: "" };
    a.appName = a.appName || j.app_name || "";
    a.jobs.push(j);
    a.started = Math.min(a.started, j.start_time || Infinity);
    a.ended = Math.max(a.ended, j.end_time || j.start_time || 0);
    a.user = a.user || j.user || "";
    if (["failed", "cancelled"].includes(lower(j.status))) a.failed++;
    by.set(id, a);
  }
  for (const a of by.values()) {
    const first = [...a.jobs].sort((x, y) => (x.start_time || 0) - (y.start_time || 0))[0];
    a.name = a.appName || oneLine(first?.job_name, 70) || "Spark Connect session";
  }
  return [...by.values()].sort((a, b) => b.ended - a.ended);
}

async function appsPage() {
  currentApp = null;
  document.body.classList.remove("in-app");
  const info = await get("/history").catch(() => ({}));
  const q = (tableState["apps-filter"]?.filter || "").toLowerCase();
  const apps = appsOf(state.jobs).filter((a) => !q || `${a.id} ${a.name} ${a.user}`.toLowerCase().includes(q));
  const rows = apps.map((a) => [
    cell(esc(state.sched?.version || ""), 0),
    cell(`<a class="mono nw" href="#/app/${esc(a.id)}">${esc(a.id)}</a>
      <div class="faint" style="font-size:12px">${a.jobs.length} job${a.jobs.length === 1 ? "" : "s"}${a.failed ? ` · <span style="color:var(--fail)">${a.failed} failed</span>` : ""}</div>`, a.id),
    cell(`<span class="trunc" title="${esc(a.name)}">${esc(a.name)}</span>`, a.name),
    cell(time(a.started === Infinity ? 0 : a.started), a.started === Infinity ? 0 : a.started),
    cell(time(a.ended), a.ended),
    cell(dur(a.ended - (a.started === Infinity ? a.ended : a.started)), a.ended - a.started, "num"),
    cell(esc(a.user || "–"), a.user),
    cell(time(a.ended), a.ended),
    cell(`<a href="${API}/history/app/${encodeURIComponent(a.id)}/log" download>Download</a>`, a.jobs.length),
  ]);
  render(`
    <h1>Ballista ${esc(state.sched?.version || "")} History Server</h1>
    <div class="histinfo"><div><b>Event log directory:</b> <span class="mono">${esc(info.event_log_dir || "–")}</span></div>
      <div><b>Last updated:</b> ${time(info.last_updated)}</div>
      <div><b>Client local time zone:</b> ${esc(Intl.DateTimeFormat().resolvedOptions().timeZone)}</div></div>
    <div class="toolbar"><input class="filter" id="apps-filter" placeholder="Search applications…" value="${esc(tableState["apps-filter"]?.filter || "")}">
      <span class="muted">${apps.length} application${apps.length === 1 ? "" : "s"}</span></div>
    <div class="panel apps-table">${dtable("apps", ["Version", "App ID", "App Name", "Started", "Completed", { label: "Duration", num: true }, "User", "Last Updated", "Event Log"],
      rows, "No completed applications found in the event log directory")}</div>`);
  $("#apps-filter").addEventListener("input", (e) => { (tableState["apps-filter"] ||= {}).filter = e.target.value; route(); });
}

// ---- Sessions tab: clients connected through Spark Connect (gRPC) or Flight SQL ----
function sessionsOf(jobs) {
  const by = new Map();
  for (const j of jobs) {
    const id = j.session_id || "(unknown)";
    const s = by.get(id) || { id, appId: "", jobs: [], first: Infinity, last: 0, busy: 0, failed: 0, running: 0, done: 0 };
    s.appId = s.appId || j.app_id || "";
    s.jobs.push(j);
    s.first = Math.min(s.first, j.start_time || Infinity);
    s.last = Math.max(s.last, j.end_time || j.start_time || 0, isLive(j.status) ? Date.now() : 0);
    s.busy += jobDuration(j) || 0;
    if (isLive(j.status)) s.running++; else if (lower(j.status) === "completed") s.done++; else s.failed++;
    by.set(id, s);
  }
  return [...by.values()].sort((a, b) => b.last - a.last);
}

async function sessionsPage() {
  const list = sessionsOf(state.jobs);
  const rows = list.map((s) => [
    cell(`<a class="mono nw" href="#/sessions/${esc(s.id)}">${esc(s.appId || s.id)}</a>`, s.appId || s.id),
    cell(`<span class="mono faint">${esc(shortId(s.id))}</span>`, s.id),
    cell(time(s.first === Infinity ? 0 : s.first), s.first === Infinity ? 0 : s.first),
    cell(s.running ? badge("Running") : `<span class="muted">${ago(s.last)}</span>`, s.last),
    cell(num(s.jobs.length), s.jobs.length, "num"), cell(num(s.running), s.running, "num"),
    cell(num(s.done), s.done, "num"), cell(num(s.failed), s.failed, "num"),
    cell(dur(s.busy), s.busy, "num"),
  ]);
  render(`
    <h1>Sessions</h1>
    <div class="sub">A session is one client connection (a Spark Connect session over gRPC, or a Flight SQL session). Jobs are grouped by the session that submitted them.</div>
    <ul class="summary"><li><span>Sessions:</span>${list.length}</li>
      <li><span>With running queries:</span>${list.filter((s) => s.running).length}</li><li><span>Queries:</span>${state.jobs.length}</li></ul>
    <div class="panel">${dtable("sessions", ["App ID", "Session", "First query", "Last activity", { label: "Queries", num: true }, { label: "Running", num: true },
      { label: "Succeeded", num: true }, { label: "Failed", num: true }, { label: "Query time", num: true }], rows, "No sessions yet — submit a query")}</div>`);
}

async function sessionPage(id) {
  const s = sessionsOf(state.jobs).find((x) => x.id === id);
  if (!s) throw new Error(`session ${id} has no jobs`);
  const newest = s.jobs[0];
  const cfg = await get(`/job/${newest.job_id}/config`).catch(() => ({}));
  const keys = Object.keys(cfg).filter((k) => !/^datafusion\.(sql_parser|explain|catalog)/.test(k)).sort();
  const filter = (tableState["session-cfg"]?.filter || "").toLowerCase();
  const shown = keys.filter((k) => !filter || k.includes(filter) || String(cfg[k]).toLowerCase().includes(filter));
  render(`
    <div class="crumbs"><a href="#/sessions">Sessions</a> / ${esc(shortId(id))}</div>
    <h1>Session <span class="mono" style="font-size:16px">${esc(id)}</span></h1>
    <ul class="summary">
      <li><span>First query:</span>${time(s.first)}</li><li><span>Last activity:</span>${time(s.last)}</li>
      <li><span>Queries:</span>${s.jobs.length}</li><li><span>Running:</span>${s.running}</li>
      <li><span>Succeeded:</span>${s.done}</li><li><span>Failed:</span>${s.failed}</li><li><span>Query time:</span>${dur(s.busy)}</li>
    </ul>
    <h2>Queries <span class="count">(${s.jobs.length})</span></h2>
    <div class="panel">${dtable("session-jobs", jobHeaders, s.jobs.map(jobRow), "No queries")}</div>
    <h2>Session configuration <span class="count">(as of its newest job; ${shown.length} of ${keys.length})</span></h2>
    <input class="filter" id="session-cfg" placeholder="Filter properties…" value="${esc(tableState["session-cfg"]?.filter || "")}">
    <div class="panel" style="margin-top:8px"><table class="kv"><tbody>${shown.map((k) => `<tr><td>${esc(k)}</td><td>${esc(cfg[k])}</td></tr>`).join("") ||
      `<tr><td colspan="2" class="empty">No properties</td></tr>`}</tbody></table></div>`);
  $("#session-cfg").addEventListener("input", (e) => { (tableState["session-cfg"] ||= {}).filter = e.target.value; route(); });
}

// ---- Executors and Cluster ----
function gauge(label, used, totalVal, fmt, hot = 0.85) {
  const p = totalVal ? Math.min(100, (100 * used) / totalVal) : 0;
  return `<div><div class="barlabel"><span>${label}</span><span>${fmt(used)}${totalVal ? " / " + fmt(totalVal) : ""}</span></div>
    <div class="bar"><i class="${p / 100 > hot ? "hot" : ""}" style="width:${p}%"></i></div></div>`;
}

async function executorsPage() {
  const rows = state.executors.map((e) => {
    const alive = Date.now() - e.last_seen < 180000;
    const rss = metric(e, "proc_physical_memory"), peak = metric(e, "peak_physical_memory");
    const pool = metric(e, "memory_pool_size"), reserved = metric(e, "memory_pool_reserved");
    const diskTotal = e.os_info?.total_disk_space, diskFree = e.os_info?.total_available_disk_space;
    return [
      cell(`<span class="mono">${esc(e.id)}</span>`, e.id), cell(`${esc(e.host)}:${e.port}`, e.host),
      cell(badge(alive ? "Alive" : "Dead"), alive ? 1 : 0),
      cell(num(e.specification?.vcores), e.specification?.vcores ?? 0, "num"),
      cell(gauge("RSS", rss || 0, pool || peak || 0, bytes), rss || 0),
      cell(pool ? gauge("Memory pool", reserved || 0, pool, bytes) : '<span class="faint">not reported</span>', reserved || 0),
      cell(bytes(peak), peak ?? 0, "num"),
      cell(diskTotal ? gauge("Disk used", diskTotal - diskFree, diskTotal, bytes, 0.9) : "–", diskFree ?? 0),
      cell(`${esc(e.os_info?.os_ver_long || "")}<div class="faint">${e.os_info?.physical_cores ?? "?"} cores · ${esc(e.os_info?.kernel_ver || "")}</div>`, e.os_info?.os_ver_long || ""),
      cell(ago(e.last_seen), e.last_seen),
    ];
  });
  render(`
    <h1>Executors</h1>
    <ul class="summary"><li><span>Active:</span>${state.executors.length}</li>
      <li><span>Total task slots:</span>${state.slots.total}</li><li><span>Slots in use:</span>${state.slots.used}</li></ul>
    <div class="panel">${dtable("executors", ["Executor ID", "Address", "Status", { label: "Task slots", num: true }, "Process memory (RSS)", "Memory pool",
      { label: "Peak RSS", num: true }, "Disk", "Host", "Heartbeat"], rows, "No executors registered")}</div>`);
}

async function clusterPage() {
  if (!state.schedConfig) state.schedConfig = await get("/scheduler/config").catch(() => ({}));
  const ex = state.executors, S = state.samples;
  const rss = ex.reduce((a, e) => a + (metric(e, "proc_physical_memory") || 0), 0);
  const peak = ex.reduce((a, e) => a + (metric(e, "peak_physical_memory") || 0), 0);
  const virt = ex.reduce((a, e) => a + (metric(e, "proc_virtual_memory") || 0), 0);
  const diskT = ex.reduce((a, e) => a + (e.os_info?.total_disk_space || 0), 0);
  const diskF = ex.reduce((a, e) => a + (e.os_info?.total_available_disk_space || 0), 0);
  const cfg = state.schedConfig, st = state.sched || {};
  const features = [["Graphviz", st.graphviz_support], ["Prometheus metrics", st.prometheus_support], ["KEDA autoscaling", st.keda_support], ["Substrait", st.substrait_support]];
  render(`
    <h1>Cluster</h1>
    <ul class="summary"><li><span>Ballista:</span>${esc(st.version || "–")}</li><li><span>DataFusion:</span>${esc(st.datafusion_version || "–")}</li>
      <li><span>Scheduler:</span>${esc(st.scheduler_id || "–")}</li><li><span>Up since:</span>${time(st.started)}</li></ul>
    <h2>Resources</h2>
    <div class="grid2">
      <div class="panel pad"><div class="barlabel"><span><b>Task slots in use</b></span><span>${state.slots.used} / ${state.slots.total}</span></div>
        <div class="bar"><i class="${state.slots.used / (state.slots.total || 1) > 0.85 ? "hot" : ""}" style="width:${pct(state.slots.used, state.slots.total)}"></i></div>
        ${sparkline(S.map((x) => x.used), state.slots.total)}<div class="faint">last ${dur(S.length ? Date.now() - S[0].t : 0)}</div></div>
      <div class="panel pad"><div class="barlabel"><span><b>Executor memory (RSS)</b></span><span>${bytes(rss)} · peak ${bytes(peak)}</span></div>
        <div class="bar"><i style="width:${peak ? Math.min(100, (100 * rss) / peak) : 0}%"></i></div>
        ${sparkline(S.map((x) => x.rss))}<div class="faint">virtual ${bytes(virt)}</div></div>
      <div class="panel pad"><div class="barlabel"><span><b>Running jobs</b></span><span>${state.jobs.filter((j) => isLive(j.status)).length} active</span></div>
        ${sparkline(S.map((x) => x.active))}</div>
      <div class="panel pad">${gauge("<b>Disk used on executor hosts</b>", diskT - diskF, diskT, bytes, 0.9)}<div class="faint" style="margin-top:6px">${ex.length} executors reporting</div></div>
    </div>
    <h2>Executors</h2>
    <div class="panel">${dtable("cluster-ex", ["Executor", { label: "Task slots", num: true }, "RSS", { label: "Peak", num: true }, "Heartbeat"],
      ex.map((e) => [cell(`${esc(e.host)}:${e.port}`, e.host), cell(num(e.specification?.vcores), e.specification?.vcores ?? 0, "num"),
        cell(gauge("", metric(e, "proc_physical_memory") || 0, metric(e, "peak_physical_memory") || 0, bytes), metric(e, "proc_physical_memory") || 0),
        cell(bytes(metric(e, "peak_physical_memory")), metric(e, "peak_physical_memory") ?? 0, "num"), cell(ago(e.last_seen), e.last_seen)]), "No executors")}</div>
    <h2>Scheduler parameters <span class="count">(${Object.keys(cfg).length})</span></h2>
    <input class="filter" id="cluster-filter" placeholder="Filter parameters…" value="${esc(tableState["cluster-filter"]?.filter || "")}">
    <div class="panel" style="margin-top:8px"><table class="kv"><tbody>${Object.keys(cfg).sort()
      .filter((k) => !tableState["cluster-filter"]?.filter || k.includes(tableState["cluster-filter"].filter.toLowerCase()))
      .map((k) => `<tr><td>${esc(k)}</td><td>${esc(cfg[k])}</td></tr>`).join("") || `<tr><td colspan="2" class="empty">Not available</td></tr>`}</tbody></table></div>
    <h2>Features</h2>
    <div class="metricchips">${features.map(([n, on]) => `<span class="m ${on ? "scan" : ""}"><b>${n}</b>${on ? "on" : "off"}</span>`).join("")}</div>`);
  $("#cluster-filter").addEventListener("input", (e) => { (tableState["cluster-filter"] ||= {}).filter = e.target.value; route(); });
}

async function environmentPage(jobId) {
  const job = jobId || state.jobs[0]?.job_id;
  const config = job ? await get(`/job/${job}/config`).catch(() => ({})) : {};
  const sched = Object.entries(state.sched || {}).map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(v)}</td></tr>`);
  const keys = Object.keys(config).sort();
  render(`
    <h1>Environment</h1>
    <h2>Scheduler</h2><div class="panel"><table class="kv"><tbody>${sched.join("")}</tbody></table></div>
    <h2>Session configuration ${job ? `of job <a href="#/jobs/${esc(job)}">${esc(job)}</a>` : ""}</h2>
    <input class="filter" id="cfg-filter" placeholder="Filter ${keys.length} properties…">
    <div class="panel" style="margin-top:8px"><table class="kv"><thead><tr><th>Name</th><th>Value</th></tr></thead><tbody id="cfg-body"></tbody></table></div>`);
  const fill = (q) => {
    $("#cfg-body").innerHTML = keys.filter((k) => !q || k.includes(q) || String(config[k]).toLowerCase().includes(q))
      .map((k) => `<tr><td>${esc(k)}</td><td>${esc(config[k])}</td></tr>`).join("") || `<tr><td colspan="2" class="empty">No properties</td></tr>`;
  };
  $("#cfg-filter").addEventListener("input", (e) => fill(e.target.value.toLowerCase()));
  fill("");
}

// ---------- theme: light by default, switchable, remembered ----------
function applyTheme(theme) {
  if (document.documentElement) document.documentElement.dataset.theme = theme;
  try { localStorage.setItem("ballista-theme", theme); } catch (e) { /* storage unavailable */ }
  const b = $("#theme");
  if (b) { b.textContent = theme === "dark" ? "☀" : "☾"; b.title = theme === "dark" ? "Switch to light theme" : "Switch to dark theme"; }
}
function initTheme() {
  let theme = "light";
  try { theme = localStorage.getItem("ballista-theme") === "dark" ? "dark" : "light"; } catch (e) { /* default */ }
  applyTheme(theme);
  $("#theme")?.addEventListener("click", () => applyTheme(document.documentElement?.dataset.theme === "dark" ? "light" : "dark"));
}

// ---------- router ----------
let routing = false;
async function route() {
  if (routing) return;
  routing = true;
  try {
    let parts = (location.hash.replace(/^#\/?/, "") || "jobs").split("/").map(decodeURIComponent);
    // History server: #/app/<app id>/<page> is a page inside one application, like Spark's
    // /history/<app id>/jobs, so the URL alone says which application it shows.
    const inApp = parts[0] === "app" && parts[1];
    if (inApp) { currentApp = parts[1]; parts = parts.slice(2); if (!parts.length) parts = ["jobs"]; }
    let [tab, id, sub, subId] = parts;
    await refreshShared();
    if (isHistory()) {
      // A deep link to one job (or its SQL page) opens inside that job's application.
      if (!currentApp && id && (tab === "jobs" || tab === "sql")) {
        const job = state.jobs.find((x) => x.job_id === id);
        if (job) currentApp = appOf(job);
      }
      // Applications are the history server's home: the job lists only exist inside one.
      if (!currentApp && !id && ["", "jobs", "stages", "sql", undefined].includes(tab)) {
        tab = "apps";
        if (location.hash !== "#/apps") history.replaceState(null, "", "#/apps");
      }
    } else {
      currentApp = null;
    }
    // Inside an application the tabs stay inside it, so reloading or sharing a page keeps its application.
    document.querySelectorAll("#nav a.app").forEach((a) => {
      a.href = currentApp ? `#/app/${encodeURIComponent(currentApp)}/${a.dataset.tab}` : `#/${a.dataset.tab}`;
    });
    document.body.classList.toggle("in-app", !!currentApp);
    document.querySelectorAll("#nav a").forEach((a) => a.classList.toggle("active", a.dataset.tab === tab));
    try {
      if (tab === "apps") await appsPage();
      else if (tab === "jobs" && id && sub === "stages" && subId) await stagePage(id, subId);
      else if (tab === "jobs" && id) await jobPage(id);
      else if (tab === "stages") await stagesPage();
      else if (tab === "sql" && id) await sqlDetailPage(id);
      else if (tab === "sql") await sqlPage();
      else if (tab === "sessions" && id) await sessionPage(id);
      else if (tab === "sessions") await sessionsPage();
      else if (tab === "executors") await executorsPage();
      else if (tab === "cluster") await clusterPage();
      else if (tab === "environment") await environmentPage(id);
      else await jobsPage();
    } catch (e) {
      render(`<div class="error">${esc(e.message || e)}</div>`);
    }
  } finally {
    routing = false;
  }
}

initTheme();
window.addEventListener("hashchange", route);
route();
setInterval(() => {
  // Pages holding a filter box keep it while the data refreshes; the environment page is static.
  if ($("#auto").checked && !location.hash.startsWith("#/environment")) route();
}, REFRESH_MS);
