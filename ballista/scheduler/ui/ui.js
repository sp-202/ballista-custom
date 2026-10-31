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
"use strict";

const API = "/api";
const REFRESH_MS = 3000;
const STAGES_PAGE_JOBS = 20;

// ---------- helpers ----------
const $ = (sel) => document.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const num = (n) => (n == null ? "–" : Number(n).toLocaleString());
const bytes = (b) => {
  if (b == null) return "–";
  const u = ["B", "KiB", "MiB", "GiB", "TiB"]; let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(i && b < 10 ? 1 : 0)} ${u[i]}`;
};
const dur = (ms) => {
  if (ms == null || ms < 0) return "–";
  if (ms < 1000) return `${ms} ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)} min`;
  return `${(ms / 3600000).toFixed(1)} h`;
};
const time = (t) => (t ? new Date(t).toLocaleString() : "–");
const badge = (s) => `<span class="badge s-${esc(String(s).toLowerCase())}">${esc(s)}</span>`;
const isLive = (status) => ["running", "queued"].includes(String(status).toLowerCase());
const jobDuration = (j) => (j.end_time ? j.end_time - j.start_time : j.start_time ? Date.now() - j.start_time : null);
const metric = (e, type) => (e.metrics || []).find((m) => m.type === type)?.value;

async function get(path) {
  const r = await fetch(API + path, { cache: "no-store" });
  const ct = r.headers.get("content-type") || "";
  const body = ct.includes("json") ? await r.json() : await r.text();
  if (!r.ok) throw new Error(body?.reason || body || r.statusText);
  return body;
}

function table(headers, rows, empty = "Nothing to show") {
  if (!rows.length) return `<div class="empty">${esc(empty)}</div>`;
  const th = headers.map((h) => `<th class="${h.num ? "num" : ""}">${esc(h.label ?? h)}</th>`).join("");
  return `<div class="scroll"><table><thead><tr>${th}</tr></thead><tbody>${rows.join("")}</tbody></table></div>`;
}

// Re-render without collapsing open <details> sections.
function render(html) {
  const view = $("#view");
  const open = new Set([...view.querySelectorAll("details[id][open]")].map((d) => d.id));
  view.innerHTML = html;
  open.forEach((id) => document.getElementById(id)?.setAttribute("open", ""));
}

// ---------- shared state ----------
const state = { jobs: [], executors: [], sched: null, ok: false };

async function refreshShared() {
  try {
    const [jobs, executors, sched] = await Promise.all([get("/jobs"), get("/executors"), get("/state")]);
    jobs.sort((a, b) => (b.start_time || 0) - (a.start_time || 0));
    Object.assign(state, { jobs, executors, sched, ok: true });
  } catch (e) {
    state.ok = false;
  }
  $("#conn").className = "dot " + (state.ok ? "ok" : "bad");
  $("#conn-text").textContent = state.ok ? "connected" : "scheduler unreachable";
  if (state.sched) $("#version").textContent = `v${state.sched.version}`;
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
  // Explicit edges between stages...
  const edgeRe = /stage_(\d+)(?:_\d+)*\s*->\s*stage_(\d+)(?:_\d+)*/g;
  for (const m of dot.matchAll(edgeRe)) add(Number(m[1]), Number(m[2]));
  // ...and every shuffle reader's input (range shuffle readers have no edge).
  for (const [id, ops] of stages) {
    for (const o of ops) for (const m of o.label.matchAll(/upstream_stage:\s*(\d+)/g)) add(Number(m[1]), id);
  }
  return { stages, edges };
}

function dagSvg(dag, statusById, jobId) {
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
  // Order each column by the mean position of its inputs to reduce crossings.
  const rank = new Map();
  layers.forEach((col) => {
    const key = (id) => {
      const ps = preds.get(id).filter((p) => rank.has(p));
      return ps.length ? ps.reduce((a, p) => a + rank.get(p), 0) / ps.length : id;
    };
    col.sort((a, b) => key(a) - key(b) || a - b);
    col.forEach((id, i) => rank.set(id, i));
  });

  const W = 176, OPS = 4, LINE = 15, PAD = 10, GAPX = 44, GAPY = 20;
  const H = (id) => 26 + Math.min(OPS, dag.stages.get(id).length) * LINE + PAD;
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
    return `<path class="edge" d="M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}"/>
      <path class="arrow" d="M${x2},${y2 - 4} L${x2 + 6},${y2} L${x2},${y2 + 4} Z"/>`;
  }).join("");
  const boxes = ids.map((id) => {
    const p = pos.get(id), ops = dag.stages.get(id);
    const status = String(statusById.get(String(id)) || "").toLowerCase();
    const lines = ops.slice(0, OPS).map((o, i) =>
      `<text class="op" x="${p.x + 10}" y="${p.y + 40 + i * LINE}">${esc(o.op.slice(0, 24))}</text>`).join("");
    const more = ops.length > OPS ? `<title>${esc(ops.map((o) => o.op).join("\n"))}</title>` : "";
    return `<a href="#/jobs/${esc(jobId)}/stages/${id}"><g class="stage ${esc(status)}">${more}
      <rect x="${p.x}" y="${p.y}" width="${W}" height="${p.h}" rx="6"/>
      <text class="title" x="${p.x + 10}" y="${p.y + 18}">Stage ${id}${status ? " · " + esc(status) : ""}</text>${lines}</g></a>`;
  }).join("");
  return `<svg width="${width + 2}" height="${maxY}" viewBox="-1 -1 ${width + 2} ${maxY + 2}" role="img" aria-label="Stage DAG">${edges}${boxes}</svg>`;
}

// ---------- pages ----------
function jobRow(j) {
  const live = isLive(j.status);
  return `<tr>
    <td class="mono"><a href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a></td>
    <td class="desc" title="${esc(j.job_status)}">${esc(j.job_name || j.job_status)}</td>
    <td>${time(j.start_time)}</td>
    <td class="num">${dur(jobDuration(j))}</td>
    <td><div class="progress"><i class="${live ? "run" : ""}" style="width:${j.percent_complete}%"></i>
      <span>${j.completed_stages}/${j.num_stages}</span></div></td>
    <td>${badge(j.status)}</td></tr>`;
}

async function jobsPage() {
  const jobs = state.jobs;
  const active = jobs.filter((j) => isLive(j.status));
  const done = jobs.filter((j) => String(j.status).toLowerCase() === "completed");
  const failed = jobs.filter((j) => ["failed", "cancelled"].includes(String(j.status).toLowerCase()));
  const headers = ["Job Id", "Description", "Submitted", { label: "Duration", num: true }, "Stages: Succeeded/Total", "Status"];
  const recent = jobs.slice(0, 30).filter((j) => j.start_time).reverse();
  const t0 = Math.min(...recent.map((j) => j.start_time)), t1 = Math.max(...recent.map((j) => j.end_time || Date.now()));
  const tl = recent.length ? timeline(recent.map((j) => ({
    label: `<a href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a>`,
    start: j.start_time, end: j.end_time || Date.now(),
    cls: String(j.status).toLowerCase(), title: j.job_status,
  })), t0, t1) : `<div class="empty">No jobs yet.</div>`;
  render(`
    <h1>Ballista Jobs</h1>
    <ul class="summary">
      <li><span>Scheduler:</span>${esc(state.sched?.scheduler_id || "–")}</li>
      <li><span>Started:</span>${time(state.sched?.started)}</li>
      <li><span>Scheduling:</span>${esc(state.sched?.scheduling_policy || "–")}</li>
      <li><span>Executors:</span>${state.executors.length}</li>
      <li><span>Active jobs:</span>${active.length}</li>
      <li><span>Completed jobs:</span>${done.length}</li>
      <li><span>Failed jobs:</span>${failed.length}</li>
    </ul>
    <details class="section" id="jobs-timeline" open><summary>Event Timeline</summary><div class="panel">${tl}</div></details>
    <h2>Active Jobs (${active.length})</h2><div class="panel">${table(headers, active.map(jobRow), "No active jobs")}</div>
    <h2>Completed Jobs (${done.length})</h2><div class="panel">${table(headers, done.map(jobRow), "No completed jobs")}</div>
    <h2>Failed Jobs (${failed.length})</h2><div class="panel">${table(headers, failed.map(jobRow), "No failed jobs")}</div>`);
}

function stageRows(jobId, stages) {
  return stages.map((s) => {
    const w = stageWindow(s);
    const tasks = (s.tasks || []).filter(Boolean);
    const done = tasks.filter((t) => String(t.status).toLowerCase() === "successful").length;
    const op = (s.stage_plan || "").split("\n")[0].replace(/:.*/, "").trim();
    return `<tr>
      <td><a href="#/jobs/${esc(jobId)}/stages/${esc(s.stage_id)}">${esc(s.stage_id)}</a></td>
      <td class="mono">${esc(op)}</td>
      <td>${badge(s.stage_status)}</td>
      <td class="num">${w ? dur(w.end - w.start) : "–"}</td>
      <td><div class="progress"><i style="width:${tasks.length ? (100 * done) / tasks.length : 0}%"></i><span>${done}/${tasks.length}</span></div></td>
      <td class="num">${num(s.input_rows)}</td><td class="num">${num(s.output_rows)}</td>
      <td class="num">${esc(s.elapsed_compute || "–")}</td></tr>`;
  });
}
const stageHeaders = ["Stage", "Root operator", "Status", { label: "Duration", num: true }, "Tasks: Succeeded/Total",
  { label: "Input rows", num: true }, { label: "Output rows", num: true }, { label: "Compute", num: true }];

async function jobPage(jobId) {
  const [job, stagesResp, dot] = await Promise.all([
    get(`/job/${jobId}`), get(`/job/${jobId}/stages`), get(`/job/${jobId}/dot`).catch(() => ""),
  ]);
  const stages = stagesResp.stages.sort((a, b) => Number(a.stage_id) - Number(b.stage_id));
  const statusById = new Map(stages.map((s) => [String(s.stage_id), s.stage_status]));
  const windows = stages.map((s) => [s, stageWindow(s)]).filter(([, w]) => w);
  const t0 = job.start_time || Math.min(...windows.map(([, w]) => w.start));
  const t1 = job.end_time || Date.now();
  const tl = windows.length ? timeline(windows.map(([s, w]) => ({
    label: `<a href="#/jobs/${esc(jobId)}/stages/${esc(s.stage_id)}">Stage ${esc(s.stage_id)}</a>`,
    start: w.start, end: w.end, cls: String(s.stage_status).toLowerCase(),
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
      <li><a href="#/environment/${esc(jobId)}">Configuration</a></li>
    </ul>
    <div class="sub">${esc(job.job_status)}</div>
    ${isLive(job.status) ? `<div class="toolbar"><button class="btn" id="cancel">Cancel job</button></div>` : ""}
    <details class="section" id="job-timeline" open><summary>Event Timeline</summary><div class="panel">${tl}</div></details>
    <details class="section" id="job-dag" open><summary>DAG Visualization</summary><div class="panel dag">${dagSvg(parseDot(dot), statusById, jobId)}</div></details>
    <h2>Stages (${stages.length})</h2><div class="panel">${table(stageHeaders, stageRows(jobId, stages), "No stages")}</div>
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
  const { stages } = await get(`/job/${jobId}/stages`);
  const stage = stages.find((s) => String(s.stage_id) === String(stageId));
  if (!stage) throw new Error(`stage ${stageId} not found in job ${jobId}`);
  const tasks = (stage.tasks || []).filter(Boolean).sort((a, b) => a.id - b.id);
  const w = stageWindow(stage);
  const taskTl = w ? timeline(tasks.filter((t) => t.start_exec_time).map((t) => ({
    label: `Task ${t.id}`, start: t.start_exec_time, end: t.end_exec_time || Date.now(),
    cls: String(t.status).toLowerCase(), title: `partitions ${t.partition_id.join(",")}`,
  })), w.start, w.end) : `<div class="empty">No task timing yet.</div>`;
  const taskRows = tasks.map((t) => `<tr>
    <td class="num">${t.id}</td><td>${badge(t.status)}</td>
    <td class="mono">${esc(t.partition_id.length > 8 ? t.partition_id.slice(0, 8).join(",") + "…" : t.partition_id.join(","))}</td>
    <td>${time(t.launch_time)}</td><td class="num">${dur(t.exec_duration)}</td>
    <td class="num">${num(t.input_rows)}</td><td class="num">${num(t.output_rows)}</td></tr>`);
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
    <h2>Summary Metrics for ${tasks.length} Tasks</h2>
    <div class="panel"><table class="quantiles"><thead><tr><th>Metric</th><th class="num">Min</th><th class="num">25th percentile</th><th class="num">Median</th><th class="num">75th percentile</th><th class="num">Max</th></tr></thead>
      <tbody>${quantileRow("Duration", stage.task_duration_percentiles, dur)}${quantileRow("Input rows", stage.task_input_percentiles, num)}</tbody></table></div>
    <details class="section" id="stage-plan" open><summary>Stage plan</summary><div class="panel"><pre class="plan">${esc(stage.stage_plan || "")}</pre></div></details>
    <details class="section" id="task-timeline" open><summary>Event Timeline</summary><div class="panel">${taskTl}</div></details>
    <h2>Tasks (${tasks.length})</h2>
    <div class="panel">${table(["Index", "Status", "Partitions", "Launch time", { label: "Duration", num: true }, { label: "Input rows", num: true }, { label: "Output rows", num: true }], taskRows, "No tasks")}</div>`);
}

const stageCache = new Map(); // job id -> stages of a finished job

async function stagesPage() {
  const jobs = state.jobs.slice(0, STAGES_PAGE_JOBS);
  const lists = await Promise.all(jobs.map(async (j) => {
    if (stageCache.has(j.job_id)) return [j, stageCache.get(j.job_id)];
    const { stages } = await get(`/job/${j.job_id}/stages`).catch(() => ({ stages: [] }));
    if (!isLive(j.status)) stageCache.set(j.job_id, stages);
    return [j, stages];
  }));
  const rows = lists.flatMap(([j, stages]) => stages
    .sort((a, b) => Number(a.stage_id) - Number(b.stage_id))
    .map((s) => stageRows(j.job_id, [s])[0].replace("<tr>", `<tr><td class="mono"><a href="#/jobs/${esc(j.job_id)}">${esc(j.job_id)}</a></td>`)));
  render(`
    <h1>Stages for the last ${jobs.length} jobs</h1>
    <div class="panel">${table(["Job", ...stageHeaders], rows, "No stages")}</div>`);
}

async function executorsPage() {
  const rows = state.executors.map((e) => {
    const pool = metric(e, "memory_pool_size"), used = metric(e, "memory_pool_reserved") || 0;
    const alive = Date.now() - e.last_seen < 180000;
    return `<tr>
      <td class="mono">${esc(e.id)}</td><td>${esc(e.host)}:${e.port}</td><td>${badge(alive ? "Alive" : "Dead")}</td>
      <td class="num">${e.specification?.vcores ?? "–"}</td>
      <td class="num">${bytes(used)} / ${bytes(pool)}</td>
      <td class="num">${bytes(metric(e, "proc_physical_memory"))}</td>
      <td class="num">${bytes(metric(e, "peak_physical_memory"))}</td>
      <td class="num">${bytes(e.os_info?.total_available_disk_space)}</td>
      <td>${time(e.last_seen)}</td></tr>`;
  });
  render(`
    <h1>Executors</h1>
    <ul class="summary"><li><span>Active:</span>${state.executors.length}</li>
      <li><span>Total task slots:</span>${state.executors.reduce((a, e) => a + (e.specification?.vcores || 0), 0)}</li></ul>
    <div class="panel">${table(["Executor ID", "Address", "Status", { label: "Task slots", num: true }, { label: "Memory pool used", num: true },
      { label: "RSS", num: true }, { label: "Peak RSS", num: true }, { label: "Disk free", num: true }, "Last heartbeat"], rows, "No executors registered")}</div>`);
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
    <div class="panel"><table class="kv"><thead><tr><th>Name</th><th>Value</th></tr></thead><tbody id="cfg-body"></tbody></table></div>`);
  const fill = (q) => {
    $("#cfg-body").innerHTML = keys.filter((k) => !q || k.includes(q) || String(config[k]).toLowerCase().includes(q))
      .map((k) => `<tr><td>${esc(k)}</td><td>${esc(config[k])}</td></tr>`).join("") || `<tr><td colspan="2" class="empty">No properties</td></tr>`;
  };
  $("#cfg-filter").addEventListener("input", (e) => fill(e.target.value.toLowerCase()));
  fill("");
}

// ---------- router ----------
let current = "";
async function route() {
  const parts = (location.hash.replace(/^#\/?/, "") || "jobs").split("/");
  const [tab, id, sub, subId] = parts;
  document.querySelectorAll("#nav a").forEach((a) => a.classList.toggle("active", a.dataset.tab === tab));
  current = location.hash;
  await refreshShared();
  try {
    if (tab === "jobs" && id && sub === "stages" && subId) await stagePage(id, subId);
    else if (tab === "jobs" && id) await jobPage(id);
    else if (tab === "stages") await stagesPage();
    else if (tab === "executors") await executorsPage();
    else if (tab === "environment") await environmentPage(id);
    else await jobsPage();
  } catch (e) {
    render(`<div class="error">${esc(e.message || e)}</div>`);
  }
}

window.addEventListener("hashchange", route);
route();
setInterval(() => {
  // The environment page holds a filter input; leave it alone while it is open.
  if ($("#auto").checked && !location.hash.startsWith("#/environment")) route();
}, REFRESH_MS);
