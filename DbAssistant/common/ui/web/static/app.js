"use strict";

// ---- API helper -----------------------------------------------------------
const api = {
  key: localStorage.getItem("dbtool_api_key") || "",
  async call(method, path, body, signal) {
    const headers = { "Content-Type": "application/json" };
    if (this.key) headers["X-API-Key"] = this.key;
    const opts = { method, headers };
    if (body !== undefined) opts.body = JSON.stringify(body);
    if (signal) opts.signal = signal;
    const res = await fetch(path, opts);
    let data = null;
    try { data = await res.json(); } catch (_) { data = null; }
    if (!res.ok) {
      const detail = (data && (data.detail || data.message)) || res.statusText;
      throw new Error(detail);
    }
    return data;
  },
  get(p) { return this.call("GET", p); },
  post(p, b) { return this.call("POST", p, b); },
  del(p) { return this.call("DELETE", p); },
};

// ---- DOM helpers -----------------------------------------------------------
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function setStatus(id, msg, kind) {
  const el = $("#" + id);
  if (!el) return;
  el.textContent = msg || "";
  el.className = "status" + (kind ? " " + kind : "");
}

function fillTable(table, columns, rows) {
  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");
  thead.innerHTML = "<tr>" + columns.map((c) => `<th>${esc(c)}</th>`).join("") + "</tr>";
  tbody.innerHTML = rows.map((r) =>
    "<tr>" + r.map((v) => `<td>${esc(v)}</td>`).join("") + "</tr>"
  ).join("");
}

function esc(v) {
  if (v === null || v === undefined) return "";
  return String(v)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

const enc = (v) => encodeURIComponent(v);

// ---- Modal dialog (mirrors Tk Toplevel dialogs) ----------------------------
function openModal(title, bodyHtml) {
  $("#modal-title").textContent = title;
  $("#modal-body").innerHTML = bodyHtml;
  $("#modal-overlay").hidden = false;
}
function closeModal() {
  $("#modal-overlay").hidden = true;
  $("#modal-body").innerHTML = "";
}
$("#modal-close").addEventListener("click", closeModal);
$("#modal-overlay").addEventListener("click", (e) => {
  if (e.target === $("#modal-overlay")) closeModal();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && !$("#modal-overlay").hidden) closeModal();
});

// ---- Tab switching ---------------------------------------------------------
$$(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    $$(".tab").forEach((t) => t.classList.remove("active"));
    $$(".panel").forEach((p) => p.classList.remove("active"));
    tab.classList.add("active");
    $("#panel-" + tab.dataset.tab).classList.add("active");
    onTabShown(tab.dataset.tab);
  });
});

function activateTab(name) {
  const tab = $(`.tab[data-tab="${name}"]`);
  if (tab) tab.click();
}

function onTabShown(name) {
  if (name === "connections") loadActiveConnections();
  if (name === "sql") { populateConnSelect("#sql-conn"); refreshAutocommit(); renderHistory(); }
  if (name === "objects") { populateConnSelect("#obj-conn"); setTimeout(populateObjTypes, 0); }
  if (name === "ai") { populateConnSelect("#ai-conn"); loadAiBackends(); loadAiSessions(); renderAiHistory(); }
  if (name === "migration") { populateConnSelect("#mig-source"); populateConnSelect("#mig-target"); }
  if (name === "monitor") populateMonTargets();
  if (name === "dashboard") loadDashboard();
  if (name === "settings") { $("#settings-api-key").value = api.key; loadSettings(); loadApiKeys(); }
}

// ---- Shared UI config (title, tab labels/order, theme) --------------------
// Read from the Web UI's own /ui/config endpoint, which is sourced from
// common/ui/shared (the same source of truth the Tk + Textual UIs use). This
// is how a label/theme change in Tk propagates here automatically.
const SHARED_TAB_TO_WEB = {
  welcome: "welcome",
  connections: "connections",
  dashboard: "dashboard",
  objects: "objects",
  sql_editor: "sql",
  migrator: "migration",
  ai: "ai",
  monitor: "monitor",
  settings: "settings",
};

async function applyUiConfig() {
  let cfg;
  try { cfg = await api.get("/ui/config"); } catch (_) { return; }
  if (cfg.title) {
    document.title = cfg.title;
    const brand = document.querySelector(".brand");
    if (brand) brand.textContent = cfg.title;
  }
  if (cfg.theme) applySharedTheme(cfg.theme);
  if (Array.isArray(cfg.tabs)) applySharedTabs(cfg.tabs);
  const specs = cfg.specs || {};
  if (specs.connection && Array.isArray(specs.connection.sections)) {
    applyConnectionLayout(specs.connection.sections);
  }
  if (specs.welcome) renderWelcome(specs.welcome);
  if (specs.objects) applyObjectsLabels(specs.objects);
  if (specs.sqlEditor) applySqlLabels(specs.sqlEditor);
  if (specs.ai) applyAiLabels(specs.ai);
  if (specs.monitoring) applyMonitoringLabels(specs.monitoring);
  const advanced = cfg.advancedModules !== false && specs.ai?.advancedModules !== false;
  if (!advanced) {
    for (const sel of ["#ai-train-llm", "#ai-build-app", "#ai-rag-manage", "[data-tab='app-builder']", "#panel-app-builder"]) {
      document.querySelectorAll(sel).forEach((el) => {
        el.hidden = true;
        el.style.display = "none";
      });
    }
  }
}

// Monitoring section titles + button labels are single-sourced from the shared
// spec (common/ui/shared/specs.py -> monitoring_payload). The static markup keeps
// stable element IDs; the three sections (server/database/cloud), their target
// actions and the shared view toolbar are stamped here so Tk, Textual and Web
// stay in sync.
const SHARED_MON_TOP_TO_DOM = {
  settings: "mon-settings", thresholds_settings: "mon-threshold-settings",
};
const SHARED_MON_SECTION_TO_FIELDSET = {
  server: "mon-sec-server", database: "mon-sec-database", cloud: "mon-sec-cloud",
};
const SHARED_MON_TARGET_TO_DOM = {
  server: { add: "mon-add-ssh", select: "mon-server-select", remove: "mon-server-remove" },
  database: { add: "mon-add-db", select: "mon-database-select", remove: "mon-database-remove" },
  cloud: { add: "mon-add-cloud", select: "mon-cloud-select", remove: "mon-cloud-remove" },
};
const SHARED_MON_METRICS_TO_DOM = {
  server: "mon-server-metrics-title",
  database: "mon-database-metrics-title",
  cloud: "mon-cloud-metrics-title",
};
const SHARED_MON_VIEW_TO_DOM = {
  alerts: "mon-alerts",
  show_graphs: ["mon-server-show-graphs", "mon-database-show-graphs", "mon-cloud-show-graphs"],
  show_text: ["mon-server-show-text", "mon-database-show-text", "mon-cloud-show-text"],
  clear_graphs: ["mon-server-clear-graphs", "mon-database-clear-graphs", "mon-cloud-clear-graphs"],
};
const SHARED_MON_THRESHOLD_TO_DOM = {
  load: "mon-thresholds", edit: "mon-thr-edit", check: "mon-thr-check",
  clear_alerts: "mon-alerts-clear",
};

function setLabel(domId, label) {
  const el = domId && document.getElementById(domId);
  if (el && label) el.textContent = label;
}

function applyMonitoringLabels(spec) {
  if (!spec) return;
  for (const a of spec.topActions || []) setLabel(SHARED_MON_TOP_TO_DOM[a.id], a.label);
  for (const sec of spec.sections || []) {
    const fs = document.getElementById(SHARED_MON_SECTION_TO_FIELDSET[sec.id]);
    const legend = fs && fs.querySelector("legend");
    if (legend && sec.title) legend.textContent = sec.title;
    setLabel(SHARED_MON_METRICS_TO_DOM[sec.id], sec.metricsTitle);
    const map = SHARED_MON_TARGET_TO_DOM[sec.id] || {};
    for (const act of sec.targetActions || []) setLabel(map[act.id], act.label);
  }
  for (const a of spec.viewActions || []) {
    const ids = SHARED_MON_VIEW_TO_DOM[a.id];
    if (Array.isArray(ids)) ids.forEach((domId) => setLabel(domId, a.label));
    else setLabel(ids, a.label);
  }
  for (const a of spec.thresholdActions || []) setLabel(SHARED_MON_THRESHOLD_TO_DOM[a.id], a.label);
}

// AI Query labels + result-tab set are single-sourced from the shared spec
// (common/ui/shared/specs.py -> ai_payload). Stable element IDs live in the
// static markup; labels/tabs are stamped here so Tk, Textual and Web stay in
// sync. The five result tabs map to fixed pane ids in spec order.
const SHARED_AI_ACTION_TO_DOM = {
  generate: "ai-ask", execute: "ai-exec", stop: "ai-stop", explain: "ai-explain",
  optimize: "ai-optimize", review: "ai-review", clear: "ai-clear",
  copy: "ai-copy-sql", edit: "ai-edit-sql", send_editor: "ai-send-editor",
  exec_rules: "ai-exec-rules", send_followup: "ai-followup-send", clear_chat: "ai-chat-clear",
  flag_query: "ai-flag-query", flag_interpretation: "ai-flag-interp",
};
const AI_RESULT_TAB_IDS = ["results", "explanation", "optimization", "rag", "chat", "review"];

const SHARED_AI_QUESTION_TOOL_TO_DOM = {
  questions_file: "ai-questions-file", index_rag: "ai-rag-index", train_llm: "ai-train-current",
};
function applyAiLabels(spec) {
  if (!spec) return;
  for (const grp of [spec.actions, spec.sqlActions, spec.chatActions]) {
    for (const action of grp || []) {
      const domId = SHARED_AI_ACTION_TO_DOM[action.id];
      const el = domId && document.getElementById(domId);
      if (el && action.label) el.textContent = action.label;
    }
  }
  for (const tool of spec.questionTools || []) {
    const el = document.getElementById(SHARED_AI_QUESTION_TOOL_TO_DOM[tool.id]);
    if (el && tool.label) el.textContent = tool.label;
  }
  // Use-RAG checkbox label, fallback backend label + hint (single-sourced).
  const useRag = document.getElementById("ai-use-rag");
  if (useRag && useRag.parentElement && spec.useRagLabel) {
    useRag.parentElement.lastChild.textContent = " " + spec.useRagLabel;
  }
  const fbLabel = document.getElementById("ai-fallback-label");
  if (fbLabel && spec.fallbackLabel) fbLabel.childNodes[0].textContent = spec.fallbackLabel + " ";
  const fbHint = document.getElementById("ai-fallback-hint");
  if (fbHint && spec.fallbackHint) fbHint.textContent = spec.fallbackHint;
  buildAiResultTabs(spec.resultTabs);
}

function buildAiResultTabs(labels) {
  const bar = document.getElementById("ai-result-tabs");
  if (!bar) return;
  bar.innerHTML = "";
  (labels || []).forEach((label, i) => {
    const id = AI_RESULT_TAB_IDS[i];
    if (!id) return;
    const btn = document.createElement("button");
    btn.className = "ai-result-tab" + (i === 0 ? " active" : "");
    btn.dataset.tab = id;
    btn.textContent = label;
    btn.addEventListener("click", () => showAiResultTab(id));
    bar.appendChild(btn);
  });
}

function showAiResultTab(id) {
  document.querySelectorAll("#ai-result-tabs .ai-result-tab").forEach((b) => {
    b.classList.toggle("active", b.dataset.tab === id);
  });
  document.querySelectorAll("#panel-ai .ai-result-pane").forEach((p) => {
    p.hidden = p.dataset.tab !== id;
  });
}

function renderAiRagContext(result) {
  const el = document.getElementById("ai-rag-context");
  if (!el) return;
  const items = (result && (result.rag_context || result.context || result.retrieved)) || [];
  const lines = [];
  if (Array.isArray(items)) {
    items.forEach((it, i) => {
      if (it && typeof it === "object") {
        const score = it.score || it.relevance || "";
        const text = it.text || it.content || it.document || JSON.stringify(it);
        lines.push(`[${i + 1}] ${score ? "(" + score + ") " : ""}${text}`);
      } else {
        lines.push(`[${i + 1}] ${it}`);
      }
    });
  } else if (items) {
    lines.push(String(items));
  }
  el.textContent = lines.join("\n\n") || "(no RAG context returned)";
}

function aiChatAppend(role, text) {
  const log = document.getElementById("ai-chat-log");
  if (!log) return;
  const div = document.createElement("div");
  div.className = role === "user" ? "chat-user" : role === "assistant" ? "chat-assistant" : "chat-system";
  const who = role === "user" ? "You" : role === "assistant" ? "AI" : "·";
  div.textContent = `${who}: ${text}`;
  log.appendChild(div);
  log.scrollTop = log.scrollHeight;
}

// SQL Editor action labels are single-sourced from the shared spec
// (common/ui/shared/specs.py -> sql_editor_payload). Stable element IDs stay in
// the static markup; labels are stamped here so Tk, Textual and Web stay in sync.
// Export is intentionally omitted: the Web splits it into CSV/JSON buttons.
const SHARED_SQL_ACTION_TO_DOM = {
  refresh: "sql-refresh",
  run_cursor: "sql-run-cursor",
  run_selected: "sql-run-sel",
  run_all: "sql-run-all",
  stop: "sql-stop",
  clear: "sql-clear",
  load: "sql-load",
  save: "sql-save",
  format: "sql-format",
  autocomplete: "sql-autocomplete-toggle",
  commit: "sql-commit",
  rollback: "sql-rollback",
  copy_all: "sql-result-copy",
  sort_asc: "sql-result-sort-asc",
  sort_desc: "sql-result-sort-desc",
  filter: "sql-result-filter",
  clear_filter: "sql-result-clear-filter",
  clear_results: "sql-clear-results",
};

function applySqlLabels(spec) {
  const groups = [spec.connectionActions, spec.editorActions, spec.resultActions];
  for (const grp of groups) {
    for (const action of grp || []) {
      const domId = SHARED_SQL_ACTION_TO_DOM[action.id];
      const el = domId && document.getElementById(domId);
      if (el && action.label) el.textContent = action.label;
    }
  }
}

// Database Objects action labels are single-sourced from the shared spec
// (common/ui/shared/specs.py -> objects_payload). The static markup keeps the
// stable element IDs; labels are stamped here so Tk, Textual and Web stay in sync.
const SHARED_OBJ_ACTION_TO_DOM = {
  refresh: "obj-refresh",
  import_jump: "obj-import-jump",
  clear_results: "obj-clear-results",
};
const SHARED_OBJ_ACTION_TO_CLASS = {
  schema: "obj-card-schema",
  sample: "obj-card-sample",
  count: "obj-card-count",
  export_selected: "obj-card-export",
};

function applyObjectsLabels(spec) {
  const groups = [spec.toolbarActions, spec.listActions, spec.detailActions];
  for (const grp of groups) {
    for (const action of grp || []) {
      const domId = SHARED_OBJ_ACTION_TO_DOM[action.id];
      const el = domId && document.getElementById(domId);
      if (el && action.label) el.textContent = action.label;
      const cls = SHARED_OBJ_ACTION_TO_CLASS[action.id];
      if (cls && action.label) {
        document.querySelectorAll(`.${cls}`).forEach((n) => { n.textContent = action.label; });
      }
    }
  }
}

// Welcome tab content is rendered entirely from the shared spec served by
// /ui/config (common/ui/shared/specs.py -> welcome_payload), so the Tk, Textual
// and Web Welcome screens stay in sync from a single source.
function renderWelcome(w) {
  const root = document.getElementById("welcome-content");
  if (!root || !w) return;
  const list = (items) =>
    `<ul class="welcome-list">${(items || [])
      .map((i) => `<li>${esc(i)}</li>`).join("")}</ul>`;
  const guide = (w.tabGuide || []).map((g) =>
    `<div class="welcome-tab-card"><h4>${esc(g.title)}</h4>` +
    `<ul>${(g.lines || []).map((l) => `<li>${esc(l)}</li>`).join("")}</ul></div>`
  ).join("");
  const shortcuts = (w.shortcuts || []).map((s) =>
    `<div class="welcome-row"><span class="welcome-key">${esc(s.keys)}</span>` +
    `<span>${esc(s.action)}</span></div>`).join("");
  const platforms = (w.platforms || []).map((p) =>
    `<div class="welcome-row"><span class="welcome-key">${esc(p.name)}</span>` +
    `<span>${esc(p.versions)}</span></div>`).join("");
  root.innerHTML =
    `<section class="welcome-hero"><h1>${esc(document.title)}</h1>` +
    `<p>${esc(w.tagline || "")}</p></section>` +
    `<h3>Quick Overview</h3>${list(w.overview)}` +
    `<h3>Tab Descriptions &amp; Usage Guide</h3>` +
    `<div class="welcome-guide">${guide}</div>` +
    `<h3>CLI, REST API &amp; modular builds</h3>${list(w.access)}` +
    `<div class="welcome-cols">` +
    `<div><h3>Keyboard Shortcuts</h3>${shortcuts}</div>` +
    `<div><h3>Platforms</h3>${platforms}</div></div>` +
    `<h3>Tips</h3>${list(w.tips)}` +
    `<p class="welcome-footer">${esc(w.footer || "")}</p>`;
}

// Connections-tab section ORDER + COLLAPSED-by-default state come from the
// shared spec (common/ui/shared/specs.py -> /ui/config). The static markup only
// supplies each section's native content; here we reorder the <details> blocks
// and set their open state to match the spec, so a layout change made once in
// the shared spec propagates to the Tk, Textual and Web UIs alike.
const SHARED_CONN_SECTION_TO_DOM = {
  active: "sec-active",
  saved: "sec-saved",
  direct: "sec-add",
  remote: "sec-remote",
  cloud: "sec-cloud",
};

function applyConnectionLayout(sections) {
  const panel = document.getElementById("panel-connections");
  if (!panel) return;
  sections.forEach((sec) => {
    const domId = SHARED_CONN_SECTION_TO_DOM[sec.id];
    if (!domId) return;
    const el = document.getElementById(domId);
    if (!el) return;
    el.open = !sec.collapsed;
    panel.appendChild(el);
  });
}

function applySharedTheme(t) {
  const r = document.documentElement.style;
  const set = (k, v) => { if (v) r.setProperty(k, v); };
  // Map the Tk palette onto the web CSS variables for visual parity.
  set("--bg", t.bgMain);
  set("--panel", t.bgSecondary);
  set("--panel-2", t.bgMain);
  set("--border", t.border);
  set("--text", t.textPrimary);
  set("--muted", t.textSecondary);
  set("--accent", t.primary);
  set("--accent-2", t.success);
  set("--danger", t.error);
  set("--row", t.bgSecondary);
}

function applySharedTabs(tabs) {
  const nav = document.getElementById("tabs");
  if (!nav) return;
  const byWeb = {};
  nav.querySelectorAll(".tab").forEach((el) => { byWeb[el.dataset.tab] = el; });
  tabs.forEach((t) => {
    const webId = SHARED_TAB_TO_WEB[t.id];
    if (!webId) return;
    const el = byWeb[webId];
    if (!el) return;
    el.textContent = t.label;        // propagate Tk's label
    if (t.module) el.hidden = false;  // installed module -> visible
    nav.appendChild(el);              // reorder to match the Tk tab order
  });
}

const monTargetSource = {};   // name -> source ("db" | "monitor" | "monitor-db" | "cloud")
const monTargetRows = {};     // name -> full row from /api/monitor/connections
// Saved targets + active (monitored) sets, per Tk-style category.
const MON_CAT_SOURCES = { server: ["monitor"], database: ["monitor-db", "db"], cloud: ["cloud"] };
const monSaved = { server: [], database: [], cloud: [] };
const monActive = { server: new Set(), database: new Set(), cloud: new Set() };
const monMetricsCache = {};   // `${cat}|${name}` -> metrics result
const MON_LOCAL_OS = "(local OS)";

function monRowLabel(cat, row) {
  const name = row.name || "";
  const kind = row.kind || "";
  const host = row.host || "";
  if (cat === "server") return host ? `${name}  [${kind || "vm"}@${host}]` : `${name}  [${kind || "vm"}]`;
  if (cat === "cloud") return `${name}  [${kind || "cloud"}]`;
  return `${name}  [${kind || (row.source === "monitor-db" ? "monitor" : "db")}]`;
}

function populateMonSection(cat) {
  const sel = $(`#mon-${cat}-list`);
  if (!sel) return;
  const prev = sel.value;
  const opts = monSaved[cat].map((row) => {
    const name = row.name || "";
    const marker = monActive[cat].has(name) ? "● " : "  ";
    return `<option value="${esc(name)}">${marker}${esc(monRowLabel(cat, row))}</option>`;
  });
  if (cat === "server" && monActive.server.has(MON_LOCAL_OS)) {
    opts.push(`<option value="${esc(MON_LOCAL_OS)}">● ${esc(MON_LOCAL_OS)}</option>`);
  }
  sel.innerHTML = opts.join("");
  if (prev) sel.value = prev;
}

// Load the three saved-target lists, split by category source.
async function loadMonSaved() {
  let rows = [];
  try {
    const r = await api.get("/api/monitor/connections?source=all");
    rows = r.connections || [];
  } catch (_) {
    rows = (connectionsCache || []).map((c) => ({ name: c.name, source: "db", kind: c.db_type, host: c.host }));
  }
  Object.keys(monTargetSource).forEach((k) => delete monTargetSource[k]);
  Object.keys(monTargetRows).forEach((k) => delete monTargetRows[k]);
  monSaved.server = []; monSaved.database = []; monSaved.cloud = [];
  const srcToCat = {};
  Object.keys(MON_CAT_SOURCES).forEach((cat) => MON_CAT_SOURCES[cat].forEach((s) => { srcToCat[s] = cat; }));
  rows.forEach((c) => {
    const n = c.name || c;
    if (!n) return;
    const src = c.source || c.kind || "db";
    monTargetSource[n] = src;
    monTargetRows[n] = c;
    const cat = srcToCat[src];
    if (cat) monSaved[cat].push(c);
  });
  ["server", "database", "cloud"].forEach(populateMonSection);
}
// Back-compat alias used by the tab activation hook.
const populateMonTargets = loadMonSaved;

// ---- Health + modules ------------------------------------------------------
async function refreshHealth() {
  const el = $("#health");
  try {
    await api.get("/api/health");
    el.textContent = "API: connected";
    el.className = "health ok";
  } catch (e) {
    el.textContent = "API: " + e.message;
    el.className = "health bad";
  }
}

async function detectModules() {
  try {
    const mods = await api.get("/api/modules");
    $$(".module-tab").forEach((tab) => {
      const key = tab.dataset.module;
      const info = mods[key];
      if (info && info.installed) tab.hidden = false;
    });
  } catch (_) { /* modules tabs stay hidden */ }
}

// ---- Connections -----------------------------------------------------------
let connectionsCache = [];
let connMeta = { db_types: [], engines: {} };

// -- metadata-driven form (mirrors the desktop connection form) --------------
async function loadConnMetadata() {
  try {
    connMeta = await api.get("/api/connections/metadata");
  } catch (_) {
    connMeta = { db_types: [], engines: {} };
  }
  const opts = (connMeta.db_types || [])
    .map((t) => `<option value="${esc(t)}">${esc(t)}</option>`).join("");
  const typeSel = $("#conn-dbtype");
  if (typeSel) { typeSel.innerHTML = opts; applyEngineToForm(); }
  const rTypeSel = $("#r-dbtype");
  if (rTypeSel) { rTypeSel.innerHTML = opts; applyRemoteEngine(); }
}

function currentEngine() {
  const t = $("#conn-dbtype") && $("#conn-dbtype").value;
  return (connMeta.engines && connMeta.engines[t]) || {};
}

function applyEngineToForm() {
  const eng = currentEngine();
  // Default port + service/db label
  const port = $("#conn-port");
  if (port && !port.dataset.touched) port.value = eng.default_port ?? "";
  const svcLabel = $("#conn-service-label");
  if (svcLabel) svcLabel.textContent = eng.service_label || "Database name";

  // SSL vs TLS groups
  const sslGroup = $("#ssl-group");
  const tlsGroup = $("#tls-group");
  if (eng.is_document) {
    sslGroup.hidden = true;
    tlsGroup.hidden = false;
    $("#conn-tls").checked = !!eng.tls_default;
  } else if (eng.supports_ssl) {
    tlsGroup.hidden = true;
    sslGroup.hidden = false;
    const modeSel = $("#conn-ssl-mode");
    const modes = eng.ssl_mode_options || [];
    modeSel.innerHTML = modes.map((m) => `<option>${esc(m)}</option>`).join("");
    const fields = new Set(eng.ssl_fields || []);
    $("#ssl-ca-row").hidden = !fields.has("ca");
    $("#ssl-cert-row").hidden = !fields.has("cert");
    $("#ssl-key-row").hidden = !fields.has("key");
    $("#ssl-wallet-row").hidden = !fields.has("wallet");
  } else {
    sslGroup.hidden = true;
    tlsGroup.hidden = true;
  }
}

// -- remote (SSH tunnel) form helpers ---------------------------------------
function remoteEngine() {
  const t = $("#r-dbtype") && $("#r-dbtype").value;
  return (connMeta.engines && connMeta.engines[t]) || {};
}

function applyRemoteEngine() {
  const eng = remoteEngine();
  const port = $("#r-port");
  if (port && !port.dataset.touched) port.value = eng.default_port ?? "";
  const svcLabel = $("#r-service-label");
  if (svcLabel) svcLabel.textContent = eng.service_label || "Database / Service";
}

function applyRemoteSshAuth() {
  const useKey = $("#r-ssh-auth").value === "key";
  $("#r-ssh-key-row").hidden = !useKey;
  $("#r-ssh-pw-row").hidden = useKey;
}

// -- saved connections list --------------------------------------------------
async function loadConnections() {
  try {
    connectionsCache = await api.get("/api/connections");
    const tbody = $("#conn-grid tbody");
    tbody.innerHTML = connectionsCache.map((c) => {
      const name = c.name;
      const ssh = c.ssh_tunnel ? `<span class="badge on">SSH</span>` : "";
      return `
      <tr>
        <td>${esc(name)}</td>
        <td>${esc(c.db_type || c.type)}</td>
        <td>${esc(c.host)}</td>
        <td>${esc(c.port)}</td>
        <td>${esc(c.service_or_db || c.database || c.db)}</td>
        <td>${esc(c.username || c.user)}</td>
        <td>${ssh}</td>
        <td>
          <button class="small conn-load" data-name="${esc(name)}">Load</button>
          <button class="small conn-connect" data-name="${esc(name)}">Connect</button>
          <button class="small conn-test" data-name="${esc(name)}">Test</button>
          <button class="small del conn-del" data-name="${esc(name)}">Remove</button>
        </td>
      </tr>`;
    }).join("");
    bindConnRowActions();
    setStatus("conn-status", `${connectionsCache.length} saved connection(s).`);
  } catch (e) {
    setStatus("conn-status", e.message, "bad");
  }
}

function bindConnRowActions() {
  $$(".conn-load").forEach((b) => b.addEventListener("click", () => {
    const c = connectionsCache.find((x) => x.name === b.dataset.name);
    if (!c) return;
    if (c.ssh_tunnel) {
      loadRemoteIntoForm(c);
      setStatus("r-status", "Loaded '" + c.name + "' into the remote form.", "ok");
    } else {
      loadConnIntoForm(c);
      setStatus("conn-status", "Loaded '" + c.name + "' into the form.", "ok");
    }
  }));
  $$(".conn-connect").forEach((b) => b.addEventListener("click", async () => {
    setStatus("conn-status", "Connecting " + b.dataset.name + "…");
    try {
      const r = await api.post(`/api/connections/${encodeURIComponent(b.dataset.name)}/open`);
      setStatus("conn-status", r.message || ("Connected " + b.dataset.name), r.ok === false ? "bad" : "ok");
      loadActiveConnections();
    } catch (e) { setStatus("conn-status", e.message, "bad"); }
  }));
  $$(".conn-test").forEach((b) => b.addEventListener("click", async () => {
    setStatus("conn-status", "Testing " + b.dataset.name + "…");
    try {
      const r = await api.post(`/api/connections/${encodeURIComponent(b.dataset.name)}/test`);
      setStatus("conn-status", r.message || "OK", r.ok ? "ok" : "bad");
      loadActiveConnections();
    } catch (e) { setStatus("conn-status", e.message, "bad"); }
  }));
  $$(".conn-del").forEach((b) => b.addEventListener("click", async () => {
    if (!confirm("Remove connection " + b.dataset.name + "?")) return;
    try {
      await api.del(`/api/connections/${encodeURIComponent(b.dataset.name)}`);
      loadConnections();
      loadActiveConnections();
    } catch (e) { setStatus("conn-status", e.message, "bad"); }
  }));
}

// -- active connections ------------------------------------------------------
let activeSelected = "";

function formatMigPreview(result) {
  const tables = result.tables || [];
  const lines = [
    "=".repeat(80),
    `SCHEMA CONVERSION PREVIEW (${tables.length} table(s))`,
    "=".repeat(80),
    "",
  ];
  if (result.error) lines.push(`ERROR: ${result.error}\n`);
  tables.forEach((row) => {
    lines.push(`Table: ${row.table || "?"}  ->  ${row.target_table || row.table || "?"}`);
    lines.push("-".repeat(80));
    if (row.error) { lines.push(`  ERROR: ${row.error}\n`); return; }
    (row.issues || []).forEach((issue) => lines.push(`  - ${issue}`));
    if (row.ddl) lines.push("", "GENERATED DDL:", row.ddl, "");
    (row.indexes_ddl || []).forEach((ddl) => { if (ddl) lines.push(ddl); });
    lines.push("");
  });
  return lines.join("\n");
}

function formatMigSample(result) {
  const tables = result.tables || [];
  const lines = [
    "=".repeat(80),
    "SAMPLE DATA (first row from each table)",
    `Checking ${tables.length} table(s)`,
    "=".repeat(80),
    "",
  ];
  if (result.error) lines.push(`ERROR: ${result.error}\n`);
  tables.forEach((row) => {
    lines.push(`Table: ${row.table || "?"}`, "-".repeat(80));
    if (row.error) { lines.push(`  ERROR: ${row.error}\n`); return; }
    if (row.columns && row.columns.length) lines.push("  Columns: " + row.columns.join(", "));
    (row.rows || []).forEach((sample, i) => {
      lines.push(`  Row ${i + 1}: ${typeof sample === "object" ? JSON.stringify(sample) : sample}`);
    });
    lines.push("");
  });
  return lines.join("\n");
}

let migOpCancel = false;

async function loadActiveConnections() {
  try {
    const rows = await api.get("/api/connections/active");
    const tbody = $("#active-grid tbody");
    tbody.innerHTML = (rows || []).map((c) => `
      <tr data-name="${esc(c.name)}" class="${activeSelected === c.name ? "sel" : ""}">
        <td>${esc(c.name)}</td>
        <td>${esc(c.db_type)}</td>
        <td>${esc(c.host)}</td>
        <td>${esc(c.port)}</td>
        <td>${esc(c.service_or_db)}</td>
        <td>${esc(c.username)}</td>
        <td><span class="badge ${c.connected ? "on" : ""}">${c.connected ? "connected" : "idle"}</span></td>
        <td><button class="small del active-disc" data-name="${esc(c.name)}">Disconnect</button></td>
      </tr>`).join("");
    $$("#active-grid tbody tr").forEach((tr) => tr.addEventListener("click", () => {
      $$("#active-grid tbody tr").forEach((r) => r.classList.remove("sel"));
      tr.classList.add("sel");
      activeSelected = tr.dataset.name || "";
    }));
    $$(".active-disc").forEach((b) => b.addEventListener("click", async () => {
      try {
        await api.post(`/api/connections/${encodeURIComponent(b.dataset.name)}/close`);
        loadActiveConnections();
      } catch (e) { setStatus("active-status", e.message, "bad"); }
    }));
    setStatus("active-status", `${(rows || []).length} active connection(s).`);
  } catch (e) {
    setStatus("active-status", e.message, "bad");
  }
}

$("#active-refresh").addEventListener("click", loadActiveConnections);
$("#active-disconnect-selected").addEventListener("click", async () => {
  if (!activeSelected) return setStatus("active-status", "Select an active connection row first.", "bad");
  try {
    await api.post(`/api/connections/${encodeURIComponent(activeSelected)}/close`);
    activeSelected = "";
    loadActiveConnections();
    setStatus("active-status", "Disconnected selected connection.", "ok");
  } catch (e) { setStatus("active-status", e.message, "bad"); }
});
$("#active-disconnect-all").addEventListener("click", async () => {
  if (!confirm("Disconnect ALL active connections?")) return;
  try {
    await api.post("/api/connections/close-all");
    loadActiveConnections();
  } catch (e) { setStatus("active-status", e.message, "bad"); }
});

// -- form wiring (direct) ----------------------------------------------------
$("#conn-dbtype").addEventListener("change", applyEngineToForm);
$("#conn-port").addEventListener("input", () => { $("#conn-port").dataset.touched = "1"; });
$("#conn-form").addEventListener("reset", () => {
  setTimeout(() => {
    delete $("#conn-port").dataset.touched;
    $("#conn-edit-name").value = "";
    applyEngineToForm();
  }, 0);
});

// -- form wiring (remote / SSH) ----------------------------------------------
$("#r-dbtype").addEventListener("change", applyRemoteEngine);
$("#r-ssh-auth").addEventListener("change", applyRemoteSshAuth);
$("#r-port").addEventListener("input", () => { $("#r-port").dataset.touched = "1"; });
$("#rconn-form").addEventListener("reset", () => {
  setTimeout(() => {
    delete $("#r-port").dataset.touched;
    $("#r-edit-name").value = "";
    applyRemoteEngine(); applyRemoteSshAuth();
  }, 0);
});

// Load a saved profile into the form (mirrors Tk "Load Connection"). The
// password is never returned by the API; leaving it blank on save preserves
// the previously-stored password.
function loadConnIntoForm(c) {
  const form = $("#conn-form");
  const set = (n, v) => { if (form.elements[n]) form.elements[n].value = (v === null || v === undefined) ? "" : v; };
  set("name", c.name);
  const dbType = c.db_type || c.type || "";
  if (dbType) { $("#conn-dbtype").value = dbType; applyEngineToForm(); }
  set("host", c.host);
  $("#conn-port").dataset.touched = "1";
  set("port", c.port);
  set("service", c.service_or_db || c.database || c.db || "");
  set("user", c.username || c.user || "");
  set("password", "");
  $("#conn-save-pw").checked = c.save_password !== false;
  if (form.elements["ssl_mode"] && c.ssl_mode) form.elements["ssl_mode"].value = c.ssl_mode;
  ["ssl_ca", "ssl_cert", "ssl_key", "wallet_location", "tls_ca_file"].forEach((k) => set(k, c[k]));
  if (form.elements["tls"]) $("#conn-tls").checked = !!c.tls;
  $("#conn-edit-name").value = c.name;   // entering edit mode for this profile
  $("#sec-add").open = true;
  $("#sec-add").scrollIntoView({ behavior: "smooth", block: "start" });
}

// Load an SSH-tunnel profile into the remote form.
function loadRemoteIntoForm(c) {
  const set = (id, v) => { const el = $("#" + id); if (el) el.value = (v === null || v === undefined) ? "" : v; };
  set("r-name", c.name);
  const dbType = c.db_type || c.type || "";
  if (dbType) { $("#r-dbtype").value = dbType; applyRemoteEngine(); }
  set("r-host", c.host);
  $("#r-port").dataset.touched = "1";
  set("r-port", c.port);
  set("r-service", c.service_or_db || c.database || c.db || "");
  set("r-user", c.username || c.user || "");
  set("r-password", "");
  $("#r-save-pw").checked = c.save_password !== false;
  const ssh = c.ssh_tunnel || {};
  set("r-ssh-host", ssh.ssh_host);
  set("r-ssh-port", ssh.ssh_port || 22);
  set("r-ssh-user", ssh.ssh_user);
  if (ssh.ssh_key_file) { $("#r-ssh-auth").value = "key"; set("r-ssh-key", ssh.ssh_key_file); }
  else { $("#r-ssh-auth").value = "password"; }
  applyRemoteSshAuth();
  $("#r-edit-name").value = c.name;
  $("#sec-remote").open = true;
  $("#sec-remote").scrollIntoView({ behavior: "smooth", block: "start" });
}

// Generic "Load Saved" dialog (mirrors Tk's "Saved Connections" Toplevel).
// `opts.filter` narrows the list (direct vs SSH); `opts.onLoad` populates the
// matching form; `opts.statusId` is the status line to report into.
async function openLoadSavedDialog(opts) {
  opts = opts || {};
  const filter = opts.filter || (() => true);
  const onLoad = opts.onLoad || loadConnIntoForm;
  const statusId = opts.statusId || "conn-status";
  try { await loadConnections(); } catch (_) {}
  const list = connectionsCache.filter(filter);
  const rows = list.map((c, i) => `
    <tr class="selectable" data-i="${i}">
      <td>${esc(c.name)}</td>
      <td>${esc(c.db_type || c.type)}</td>
      <td>${esc(c.host)}</td>
      <td>${esc(c.port)}</td>
      <td>${esc(c.service_or_db || c.database || c.db)}</td>
      <td>${esc(c.username || c.user)}</td>
      <td>${c.ssh_tunnel ? "SSH" : ""}</td>
    </tr>`).join("");
  openModal(opts.title || "Saved Connections", `
    ${list.length ? "" : '<p class="hint">No saved connections yet. Fill the form and click Save.</p>'}
    <div class="results-wrap"><table class="grid">
      <thead><tr><th>Name</th><th>DB Type</th><th>Host</th><th>Port</th><th>Database/Service</th><th>Username</th><th>SSH</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>
    <div class="modal-actions">
      <button id="ls-load" class="primary">Load Connection</button>
      <button id="ls-delete" class="del">Delete Connection</button>
      <button id="ls-close">Close</button>
    </div>`);
  let selected = -1;
  $$("#modal-body tr.selectable").forEach((tr) => tr.addEventListener("click", () => {
    $$("#modal-body tr.selectable").forEach((x) => x.classList.remove("sel"));
    tr.classList.add("sel"); selected = +tr.dataset.i;
  }));
  $("#ls-load").addEventListener("click", () => {
    if (selected < 0) return setStatus(statusId, "Select a saved connection first.", "bad");
    const c = list[selected];
    closeModal();
    onLoad(c);
    setStatus(statusId, "Loaded '" + c.name + "' into the form.", "ok");
  });
  $("#ls-delete").addEventListener("click", async () => {
    if (selected < 0) return;
    const name = list[selected].name;
    if (!confirm("Delete connection '" + name + "'?")) return;
    try {
      await api.del(`/api/connections/${enc(name)}`);
      closeModal();
      loadConnections();
      loadActiveConnections();
      setStatus(statusId, "Deleted '" + name + "'.", "ok");
    } catch (e) { alert(e.message); }
  });
  $("#ls-close").addEventListener("click", closeModal);
}

function buildConnBody() {
  const form = $("#conn-form");
  const eng = currentEngine();
  const val = (n) => { const el = form.elements[n]; return el ? el.value.trim() : ""; };
  const body = {
    name: val("name"),
    db_type: $("#conn-dbtype").value,
    host: val("host"),
    port: val("port"),
    user: val("user"),
    password: form.elements["password"].value,
    service: val("service"),
    save_password: $("#conn-save-pw").checked,
  };
  if (eng.is_document) {
    body.tls = $("#conn-tls").checked;
    if (val("tls_ca_file")) body.tls_ca_file = val("tls_ca_file");
  } else if (eng.supports_ssl) {
    if (val("ssl_mode")) body.ssl_mode = val("ssl_mode");
    ["ssl_ca", "ssl_cert", "ssl_key", "wallet_location"].forEach((k) => {
      if (val(k)) body[k] = val(k);
    });
  }
  return body;
}

// Build the request body for the remote (SSH tunnel) form.
function buildRemoteBody() {
  const v = (id) => { const el = $("#" + id); return el ? el.value.trim() : ""; };
  const useKey = $("#r-ssh-auth").value === "key";
  return {
    name: v("r-name"),
    db_type: $("#r-dbtype").value,
    host: v("r-host"),
    port: v("r-port"),
    user: v("r-user"),
    password: $("#r-password").value,
    service: v("r-service"),
    save_password: $("#r-save-pw").checked,
    ssh_tunnel: {
      ssh_host: v("r-ssh-host"),
      ssh_user: v("r-ssh-user"),
      ssh_port: parseInt(v("r-ssh-port") || "22", 10),
      ssh_password: useKey ? "" : $("#r-ssh-password").value,
      ssh_key_file: useKey ? v("r-ssh-key") : "",
    },
  };
}

// Upsert a profile from either form. `cfg` carries the body, the edit-name
// field id and the status line; create with POST, update existing with PUT.
async function upsertConnection(body, editNameId, statusId) {
  if (!body.name) { setStatus(statusId, "Connection name is required.", "bad"); return null; }
  if (!body.host) { setStatus(statusId, "Host is required.", "bad"); return null; }
  if (body.ssh_tunnel && !body.ssh_tunnel.ssh_host) {
    setStatus(statusId, "SSH host is required for a remote connection.", "bad"); return null;
  }
  const editName = $("#" + editNameId).value;
  const exists = connectionsCache.some((c) => c.name === body.name);
  if (editName) {
    await api.call("PUT", `/api/connections/${enc(editName)}`, { ...body, old_name: editName });
  } else if (exists) {
    await api.call("PUT", `/api/connections/${enc(body.name)}`, { ...body, old_name: body.name });
  } else {
    await api.post("/api/connections", body);
  }
  $("#" + editNameId).value = body.name;
  await loadConnections();
  return body.name;
}

async function saveConnection() {
  return upsertConnection(buildConnBody(), "conn-edit-name", "conn-status");
}

async function saveRemoteConnection() {
  return upsertConnection(buildRemoteBody(), "r-edit-name", "r-status");
}

// Connect/Test helpers — mirror Tk: Test uses form values directly; Connect
// saves the profile then opens with inline credentials (so save_password=false
// still works when the user typed a password).
async function connectFrom(saveFn, buildFn, statusId) {
  try {
    const body = buildFn();
    if (!body.name) { setStatus(statusId, "Connection name is required.", "bad"); return; }
    const name = await saveFn();
    if (!name) return;
    setStatus(statusId, "Connecting '" + name + "'…");
    const r = await api.post(`/api/connections/${enc(name)}/open-form`, body);
    setStatus(statusId, r.message || ("Connected " + name), r.ok === false ? "bad" : "ok");
    loadActiveConnections();
  } catch (e) { setStatus(statusId, e.message, "bad"); }
}
function formatTestResult(r) {
  const ok = !!r.ok;
  const msg = r.message || (ok ? "Connection succeeded." : "Connection failed.");
  const parts = [msg];
  const version = r.version;
  if (ok && version && !msg.includes(String(version))) {
    parts.push("Server: " + version + ".");
  }
  const latency = r.latency_ms;
  if (latency != null && !msg.includes("ms")) {
    parts.push(latency + " ms.");
  }
  return parts.join(" ");
}
async function testFrom(buildFn, statusId) {
  try {
    const body = buildFn();
    if (!body.name) { setStatus(statusId, "Connection name is required.", "bad"); return; }
    if (!body.host) { setStatus(statusId, "Host is required.", "bad"); return; }
    delete body.save_password;
    setStatus(statusId, "Testing '" + body.name + "'…");
    const r = await api.post("/api/connections/test-inline", body);
    setStatus(statusId, formatTestResult(r), r.ok ? "ok" : "bad");
  } catch (e) { setStatus(statusId, e.message, "bad"); }
}

// Direct form actions
$("#conn-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  try {
    const name = await saveConnection();
    if (name) setStatus("conn-status", "Saved connection '" + name + "'.", "ok");
  } catch (e) { setStatus("conn-status", e.message, "bad"); }
});
$("#conn-connect-form").addEventListener("click", () => connectFrom(saveConnection, buildConnBody, "conn-status"));
$("#conn-test-form").addEventListener("click", () => testFrom(buildConnBody, "conn-status"));
$("#conn-load-saved").addEventListener("click", () => openLoadSavedDialog({
  title: "Saved Connections (direct)",
  filter: (c) => !c.ssh_tunnel,
  onLoad: loadConnIntoForm,
  statusId: "conn-status",
}));

// Remote form actions
$("#rconn-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  try {
    const name = await saveRemoteConnection();
    if (name) setStatus("r-status", "Saved connection '" + name + "'.", "ok");
  } catch (e) { setStatus("r-status", e.message, "bad"); }
});
$("#r-connect").addEventListener("click", () => connectFrom(saveRemoteConnection, buildRemoteBody, "r-status"));
$("#r-test").addEventListener("click", () => testFrom(buildRemoteBody, "r-status"));
$("#r-load-saved").addEventListener("click", () => openLoadSavedDialog({
  title: "Saved Connections (remote / SSH)",
  filter: (c) => !!c.ssh_tunnel,
  onLoad: loadRemoteIntoForm,
  statusId: "r-status",
}));

$("#conn-refresh").addEventListener("click", loadConnections);

// ===========================================================================
// Cloud DB connection section (AWS / Azure / GCP / Other)
// Dynamic form built from /api/cloud/schemas (the same provider schemas the
// Tk and Textual cloud sections render). No values are hard-coded here.
// ===========================================================================
let cloudSchemas = null;

const CLOUD_AUTH_MODES = [
  { value: "keys", label: "Access Keys", src: "keysAuth" },
  { value: "pwd", label: "Username / Password", src: "pwdAuth" },
  { value: "sso", label: "SSO / OIDC", src: "ssoAuth" },
  { value: "env", label: "Environment / Instance Role", src: "envAuth" },
];

function cloudProvider() { return $("#cloud-provider") && $("#cloud-provider").value; }
function cloudAuthMode() { return $("#cloud-auth-mode") && $("#cloud-auth-mode").value; }
function cloudSchema() {
  return cloudSchemas && cloudSchemas.providers[cloudProvider()] || null;
}

function cloudFieldHtml(f) {
  const help = f.help ? ` title="${esc(f.help)}"` : "";
  const label = esc(f.label);
  if (f.choices) {
    const opts = f.choices.map((c) => `<option value="${esc(c)}">${esc(c) || "—"}</option>`).join("");
    return `<label>${label} <select name="${esc(f.key)}"${help}>${opts}</select></label>`;
  }
  const type = f.secret ? "password" : "text";
  const auto = f.secret ? ' autocomplete="new-password"' : ' autocomplete="off"';
  return `<label>${label} <input type="${type}" name="${esc(f.key)}"${auto}${help} /></label>`;
}

async function loadCloudSchemas() {
  try {
    cloudSchemas = await api.get("/api/cloud/schemas");
  } catch (e) {
    setStatus("cloud-status", "Cloud connections unavailable: " + e.message, "bad");
    return;
  }
  const provSel = $("#cloud-provider");
  provSel.innerHTML = (cloudSchemas.providerOrder || [])
    .map((p) => `<option value="${esc(p)}">${esc(cloudSchemas.providers[p].label || p)}</option>`).join("");
  renderCloudProvider();
}

function renderCloudAuthModes() {
  const schema = cloudSchema();
  if (!schema) return;
  const sel = $("#cloud-auth-mode");
  const prev = sel.value;
  sel.innerHTML = CLOUD_AUTH_MODES.map((m) => {
    let label = m.label;
    if (m.value === "sso" && schema.ssoAuth) label = schema.ssoAuth.tabLabel || label;
    if (m.value === "env" && schema.envAuth) label = schema.envAuth.tabLabel || label;
    return `<option value="${m.value}">${esc(label)}</option>`;
  }).join("");
  if (CLOUD_AUTH_MODES.some((m) => m.value === prev)) sel.value = prev;
}

function renderCloudResource() {
  const schema = cloudSchema();
  $("#cloud-resource-fields").innerHTML =
    (schema ? schema.resource : []).map(cloudFieldHtml).join("");
}

function renderCloudSql() {
  $("#cloud-sql-fields").innerHTML =
    ((cloudSchemas && cloudSchemas.sqlFields) || []).map(cloudFieldHtml).join("");
}

function renderCloudAuthFields() {
  const schema = cloudSchema();
  if (!schema) return;
  const mode = cloudAuthMode();
  let fields = [];
  let help = "";
  if (mode === "keys") fields = schema.keysAuth || [];
  else if (mode === "pwd") fields = schema.pwdAuth || [];
  else if (mode === "sso") { fields = (schema.ssoAuth || {}).fields || []; }
  else if (mode === "env") { fields = (schema.envAuth || {}).fields || []; help = (schema.envAuth || {}).help || ""; }
  $("#cloud-auth-fields").innerHTML = fields.map(cloudFieldHtml).join("");
  const helpEl = $("#cloud-auth-help");
  helpEl.textContent = help;
  helpEl.hidden = !help;
}

function renderCloudProvider() {
  renderCloudAuthModes();
  renderCloudResource();
  renderCloudAuthFields();
  renderCloudSql();
}

// Read every rendered field into a provider profile + sql_connection.
function collectCloudProfile() {
  const form = $("#cloud-form");
  const val = (n) => { const el = form.elements[n]; return el ? el.value : ""; };
  const profile = { provider: cloudProvider(), auth_mode: cloudAuthMode() };
  const schema = cloudSchema() || {};
  const collect = (fields) => (fields || []).forEach((f) => { profile[f.key] = val(f.key); });
  collect(schema.resource);
  const mode = cloudAuthMode();
  if (mode === "keys") collect(schema.keysAuth);
  else if (mode === "pwd") collect(schema.pwdAuth);
  else if (mode === "sso") collect((schema.ssoAuth || {}).fields);
  else if (mode === "env") collect((schema.envAuth || {}).fields);
  profile.sql_connection = {
    db_type: val("sql_db_type"),
    host: val("sql_host"),
    port: val("sql_port"),
    service_or_db: val("sql_database"),
    username: val("sql_username"),
    password: val("sql_password"),
  };
  return profile;
}

function loadCloudIntoForm(profile) {
  if (!cloudSchemas) return;
  const prov = profile.provider || cloudSchemas.providerOrder[0];
  $("#cloud-provider").value = prov;
  $("#cloud-auth-mode").value = profile.auth_mode || "keys";
  renderCloudProvider();
  const form = $("#cloud-form");
  Object.entries(profile).forEach(([k, v]) => {
    if (k === "sql_connection") return;
    if (form.elements[k] && v != null) form.elements[k].value = v;
  });
  const sql = profile.sql_connection || {};
  const setSql = (n, v) => { if (form.elements[n]) form.elements[n].value = v == null ? "" : v; };
  setSql("sql_db_type", sql.db_type);
  setSql("sql_host", sql.host);
  setSql("sql_port", sql.port);
  setSql("sql_database", sql.service_or_db || sql.database);
  setSql("sql_username", sql.username);
  setSql("sql_password", "");
  $("#cloud-edit-name").value = profile.display_name || "";
  $("#sec-cloud").open = true;
  $("#sec-cloud").scrollIntoView({ behavior: "smooth", block: "start" });
}

async function saveCloud() {
  const profile = collectCloudProfile();
  if (!profile.display_name) { setStatus("cloud-status", "Display Name is required.", "bad"); return null; }
  const editName = $("#cloud-edit-name").value;
  try {
    if (editName) {
      await api.call("PUT", `/api/cloud/connections/${enc(editName)}`, { ...profile, old_name: editName });
    } else {
      await api.post("/api/cloud/connections", profile);
    }
    $("#cloud-edit-name").value = profile.display_name;
    return profile.display_name;
  } catch (e) { setStatus("cloud-status", e.message, "bad"); return null; }
}

async function openCloudLoadSaved() {
  let rows = [];
  try { rows = await api.get("/api/cloud/connections"); } catch (_) {}
  const body = rows.map((c, i) => `
    <tr class="selectable" data-i="${i}">
      <td>${esc(c.display_name || c.name)}</td><td>${esc(c.provider)}</td>
      <td>${esc(c.db_type)}</td><td>${esc(c.host)}</td><td>${esc(c.port)}</td>
      <td>${esc(c.database)}</td><td>${esc(c.username)}</td>
    </tr>`).join("");
  openModal("Saved Cloud Connections", `
    ${rows.length ? "" : '<p class="hint">No saved cloud connections yet.</p>'}
    <div class="results-wrap"><table class="grid">
      <thead><tr><th>Name</th><th>Provider</th><th>DB Type</th><th>Host</th><th>Port</th><th>Database</th><th>Username</th></tr></thead>
      <tbody>${body}</tbody>
    </table></div>
    <div class="modal-actions">
      <button id="cl-load" class="primary">Load into form</button>
      <button id="cl-delete" class="del">Delete</button>
      <button id="cl-close">Close</button>
    </div>`);
  let selected = -1;
  $$("#modal-body tr.selectable").forEach((tr) => tr.addEventListener("click", () => {
    $$("#modal-body tr.selectable").forEach((x) => x.classList.remove("sel"));
    tr.classList.add("sel"); selected = +tr.dataset.i;
  }));
  $("#cl-load").addEventListener("click", async () => {
    if (selected < 0) return setStatus("cloud-status", "Select a profile first.", "bad");
    const name = rows[selected].name;
    try {
      const full = await api.get(`/api/cloud/connections/${enc(name)}`);
      closeModal();
      loadCloudIntoForm(full);
      setStatus("cloud-status", "Loaded '" + name + "' into the form.", "ok");
    } catch (e) { alert(e.message); }
  });
  $("#cl-delete").addEventListener("click", async () => {
    if (selected < 0) return;
    const name = rows[selected].name;
    if (!confirm("Delete cloud profile '" + name + "'?")) return;
    try {
      await api.del(`/api/cloud/connections/${enc(name)}`);
      closeModal();
      loadConnections();
      setStatus("cloud-status", "Deleted '" + name + "'.", "ok");
    } catch (e) { alert(e.message); }
  });
  $("#cl-close").addEventListener("click", closeModal);
}

// -- cloud form wiring -------------------------------------------------------
function bindCloudSection() {
  if (!$("#cloud-form")) return;
  $("#cloud-provider").addEventListener("change", renderCloudProvider);
  $("#cloud-auth-mode").addEventListener("change", renderCloudAuthFields);
  $("#cloud-form").addEventListener("reset", () => setTimeout(() => {
    $("#cloud-edit-name").value = "";
    renderCloudProvider();
    setStatus("cloud-status", "Cleared.");
  }, 0));
  $("#cloud-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const name = await saveCloud();
    if (name) { setStatus("cloud-status", "Saved cloud profile '" + name + "'.", "ok"); loadConnections(); }
  });
  $("#cloud-connect").addEventListener("click", async () => {
    const profile = collectCloudProfile();
    const editName = $("#cloud-edit-name").value;
    setStatus("cloud-status", "Connecting cloud DB…");
    try {
      const r = await api.post("/api/cloud/connect", editName ? { ...profile, old_name: editName } : profile);
      setStatus("cloud-status", r.message || "Connected.", r.ok === false ? "bad" : "ok");
      $("#cloud-edit-name").value = profile.display_name || "";
      loadConnections(); loadActiveConnections();
    } catch (e) { setStatus("cloud-status", e.message, "bad"); }
  });
  $("#cloud-test-login").addEventListener("click", async () => {
    setStatus("cloud-status", "Testing cloud login…");
    try {
      const r = await api.post("/api/cloud/test-login", collectCloudProfile());
      setStatus("cloud-status", r.message || "OK", r.ok ? "ok" : "bad");
    } catch (e) { setStatus("cloud-status", e.message, "bad"); }
  });
  $("#cloud-test-db").addEventListener("click", async () => {
    setStatus("cloud-status", "Testing DB connection…");
    try {
      const r = await api.post("/api/cloud/test-db", collectCloudProfile());
      setStatus("cloud-status", r.message || "OK", r.ok ? "ok" : "bad");
    } catch (e) { setStatus("cloud-status", e.message, "bad"); }
  });
  $("#cloud-resolve").addEventListener("click", async () => {
    setStatus("cloud-status", "Resolving SQL endpoint…");
    try {
      const r = await api.post("/api/cloud/resolve", collectCloudProfile());
      if (r.ok) {
        const form = $("#cloud-form");
        if (form.elements["sql_host"]) form.elements["sql_host"].value = r.host || "";
        if (form.elements["sql_port"]) form.elements["sql_port"].value = r.port || "";
        if (form.elements["sql_db_type"] && r.db_type) form.elements["sql_db_type"].value = r.db_type;
      }
      setStatus("cloud-status", r.message || "", r.ok ? "ok" : "bad");
    } catch (e) { setStatus("cloud-status", e.message, "bad"); }
  });
  $("#cloud-load-saved").addEventListener("click", openCloudLoadSaved);
}

function populateConnSelect(sel) {
  const el = $(sel);
  if (!el) return;
  const current = el.value;
  el.innerHTML = connectionsCache.map((c) =>
    `<option value="${esc(c.name)}">${esc(c.name)}</option>`).join("");
  if (current) el.value = current;
}

// ---- SQL editor ------------------------------------------------------------
let sqlResults = [];      // [{statement, columns, rows, rowcount, time_ms}]
let sqlActiveResult = 0;
let sqlFilter = null;     // {column, text} applied to current result
let sqlAutocomplete = true;
let sqlTabs = [{ id: "tab1", label: "Tab 1", text: "SELECT 1;", conn: "", results: [], active: 0 }];
let sqlActiveTab = "tab1";
let sqlNextTab = 2;

function currentSqlTab() {
  return sqlTabs.find((t) => t.id === sqlActiveTab) || sqlTabs[0];
}

function saveSqlTabState() {
  const tab = currentSqlTab();
  if (!tab) return;
  tab.text = $("#sql-input").value;
  tab.conn = $("#sql-conn").value;
  tab.results = sqlResults;
  tab.active = sqlActiveResult;
}

function renderSqlTabs() {
  const strip = $("#sql-tab-strip");
  if (!strip) return;
  strip.innerHTML = sqlTabs.map((t, i) => `
    <button class="sql-tab ${t.id === sqlActiveTab ? "active" : ""}" data-id="${t.id}">
      ${esc(t.label)}${t.conn ? " · " + esc(t.conn) : ""}${sqlTabs.length > 1 ? ' <span class="x">×</span>' : ""}
    </button>`).join("") + '<button id="sql-tab-new" class="small">+</button>';
  $$("#sql-tab-strip .sql-tab").forEach((b) => b.addEventListener("click", (e) => {
    if (e.target.classList.contains("x")) {
      if (sqlTabs.length <= 1) return;
      const id = b.dataset.id;
      const idx = sqlTabs.findIndex((t) => t.id === id);
      sqlTabs = sqlTabs.filter((t) => t.id !== id);
      if (sqlActiveTab === id) sqlActiveTab = (sqlTabs[Math.max(0, idx - 1)] || sqlTabs[0]).id;
      loadSqlTabState();
      return;
    }
    saveSqlTabState();
    sqlActiveTab = b.dataset.id;
    loadSqlTabState();
  }));
  $("#sql-tab-new").addEventListener("click", () => {
    saveSqlTabState();
    const id = "tab" + sqlNextTab;
    sqlTabs.push({ id, label: "Tab " + sqlNextTab, text: "SELECT 1;", conn: sqlConn(), results: [], active: 0 });
    sqlNextTab += 1;
    sqlActiveTab = id;
    loadSqlTabState();
  });
}

function loadSqlTabState() {
  const tab = currentSqlTab();
  if (!tab) return;
  $("#sql-input").value = tab.text || "";
  if (tab.conn) $("#sql-conn").value = tab.conn;
  sqlResults = tab.results || [];
  sqlActiveResult = tab.active || 0;
  sqlFilter = null;
  renderSqlTabs();
  renderSqlResults();
  refreshAutocommit();
}

function sqlConn() { return $("#sql-conn").value; }

async function ensureSqlConnOpen() {
  const conn = sqlConn();
  if (!conn) return false;
  try { await api.post(`/api/connections/${encodeURIComponent(conn)}/open`); } catch (_) {}
  return true;
}

async function refreshAutocommit() {
  const conn = sqlConn();
  if (!conn) return;
  try {
    const r = await api.get(`/api/query/${encodeURIComponent(conn)}/autocommit`);
    if (r.ok) $("#sql-autocommit").checked = !!r.autocommit;
  } catch (_) {}
}

$("#sql-refresh").addEventListener("click", () => { populateConnSelect("#sql-conn"); refreshAutocommit(); renderSqlTabs(); });
$("#sql-conn").addEventListener("change", () => { saveSqlTabState(); renderSqlTabs(); refreshAutocommit(); });

$("#sql-autocommit").addEventListener("change", async () => {
  const conn = sqlConn();
  if (!conn) return setStatus("sql-status", "Select a connection.", "bad");
  await ensureSqlConnOpen();
  try {
    const r = await api.call("PUT", `/api/query/${encodeURIComponent(conn)}/autocommit`, { enabled: $("#sql-autocommit").checked });
    setStatus("sql-status", r.message || "", r.ok ? "ok" : "bad");
    if (!r.ok) refreshAutocommit();
  } catch (e) { setStatus("sql-status", e.message, "bad"); refreshAutocommit(); }
});

function selectedSql() {
  const ta = $("#sql-input");
  const { selectionStart: s, selectionEnd: e } = ta;
  return s != null && e != null && e > s ? ta.value.slice(s, e) : "";
}

function statementAtCursor() {
  const ta = $("#sql-input");
  const text = ta.value;
  const pos = ta.selectionStart || 0;
  const before = text.slice(0, pos);
  const after = text.slice(pos);
  const start = before.lastIndexOf(";") + 1;
  const next = after.indexOf(";");
  const end = next < 0 ? text.length : pos + next;
  const stmt = text.slice(start, end)
    .split("\n")
    .filter((line) => !line.trim().startsWith("--"))
    .join("\n")
    .trim();
  return stmt || text.trim();
}

async function runSql(sqlText) {
  const conn = sqlConn();
  if (!conn) return setStatus("sql-status", "Select a connection.", "bad");
  const sql = (sqlText != null ? sqlText : $("#sql-input").value).trim();
  if (!sql) return setStatus("sql-status", "Nothing to run.", "bad");
  await ensureSqlConnOpen();
  setStatus("sql-status", "Running…");
  try {
    const r = await api.post("/api/query/multi", { connection: conn, sql });
    sqlResults = (r.results || []).map((x) => ({
      statement: x.statement,
      columns: (x.result && x.result.columns) || [],
      rows: (x.result && x.result.rows) || [],
      rowcount: x.result && x.result.rowcount,
      time_ms: x.result && x.result.time_ms,
      error: x.result && x.result.error,
    }));
    sqlActiveResult = 0;
    sqlFilter = null;
    renderSqlResults();
    addSqlHistory(sql);
    saveSqlTabState();
    if (r.error) setStatus("sql-status", "Error: " + r.error, "bad");
    else setStatus("sql-status", `OK — ${r.count} statement(s) executed.`, "ok");
  } catch (e) { setStatus("sql-status", e.message, "bad"); }
}

function renderSqlResults() {
  const head = $("#sql-results-head");
  const tabsEl = $("#sql-result-tabs");
  if (!sqlResults.length) { head.hidden = true; fillTable($("#sql-grid"), [], []); return; }
  head.hidden = false;
  tabsEl.innerHTML = sqlResults.map((r, i) => {
    const label = r.error ? `#${i + 1} error` :
      `#${i + 1} (${r.rowcount ?? (r.rows || []).length})`;
    return `<button class="rtab ${i === sqlActiveResult ? "active" : ""}" data-i="${i}">${esc(label)}</button>`;
  }).join("");
  $$("#sql-result-tabs .rtab").forEach((b) =>
    b.addEventListener("click", () => { sqlActiveResult = +b.dataset.i; renderSqlResults(); }));
  const r = sqlResults[sqlActiveResult];
  if (r.error) { fillTable($("#sql-grid"), ["error"], [[r.error]]); }
  else {
    let rows = r.rows || [];
    if (sqlFilter && sqlFilter.text) {
      const idx = (r.columns || []).indexOf(sqlFilter.column);
      if (idx >= 0) rows = rows.filter((row) => String(row[idx] ?? "").toLowerCase().includes(sqlFilter.text.toLowerCase()));
    }
    fillTable($("#sql-grid"), r.columns || [], rows);
  }
}

$("#sql-run-cursor").addEventListener("click", () => runSql(statementAtCursor()));
$("#sql-run-sel").addEventListener("click", () => {
  const sel = selectedSql();
  if (!sel) return setStatus("sql-status", "Select SQL text first.", "bad");
  runSql(sel);
});
$("#sql-run-all").addEventListener("click", () => runSql($("#sql-input").value));

$("#sql-input").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runSql(statementAtCursor()); }
  if (e.key === "F5") { e.preventDefault(); runSql(statementAtCursor()); }
  if (sqlAutocomplete && (e.ctrlKey || e.metaKey) && e.key === " ") {
    e.preventDefault();
    setStatus("sql-status", "Autocomplete is enabled. Object suggestions are terminal/desktop assisted; use schema/object browser for names.", "ok");
  }
});

$("#sql-format").addEventListener("click", async () => {
  const sql = $("#sql-input").value;
  if (!sql.trim()) return;
  try {
    const r = await api.post("/api/query/format", { sql });
    if (r.ok) { $("#sql-input").value = r.sql; setStatus("sql-status", "Formatted.", "ok"); }
    else setStatus("sql-status", r.message, "bad");
  } catch (e) { setStatus("sql-status", e.message, "bad"); }
});

$("#sql-stop").addEventListener("click", async () => {
  const conn = sqlConn();
  if (!conn) return;
  try {
    const r = await api.post(`/api/query/${encodeURIComponent(conn)}/cancel`);
    setStatus("sql-status", r.message || "Cancel sent.", r.ok ? "ok" : "bad");
  } catch (e) { setStatus("sql-status", e.message, "bad"); }
});

$("#sql-clear").addEventListener("click", () => { $("#sql-input").value = ""; saveSqlTabState(); });
$("#sql-load").addEventListener("click", () => $("#sql-load-file").click());
$("#sql-load-file").addEventListener("change", async (e) => {
  const file = e.target.files && e.target.files[0];
  if (!file) return;
  $("#sql-input").value = await file.text();
  e.target.value = "";
  saveSqlTabState();
  setStatus("sql-status", "Loaded query from " + file.name, "ok");
});
$("#sql-save").addEventListener("click", () => {
  const sql = $("#sql-input").value;
  if (!sql.trim()) return setStatus("sql-status", "No query to save.", "bad");
  download("query.sql", sql, "text/sql");
  setStatus("sql-status", "Query saved.", "ok");
});
$("#sql-autocomplete-toggle").addEventListener("click", () => {
  sqlAutocomplete = !sqlAutocomplete;
  $("#sql-autocomplete-toggle").textContent = "Autocomplete: " + (sqlAutocomplete ? "On" : "Off");
  setStatus("sql-status", sqlAutocomplete ? "Autocomplete enabled." : "Autocomplete disabled.", "ok");
});
$("#sql-clear-results").addEventListener("click", () => {
  sqlResults = [];
  sqlActiveResult = 0;
  sqlFilter = null;
  renderSqlResults();
  saveSqlTabState();
  setStatus("sql-status", "Results cleared.", "ok");
});

$("#sql-commit").addEventListener("click", () => txn("commit"));
$("#sql-rollback").addEventListener("click", () => txn("rollback"));
async function txn(kind) {
  const conn = sqlConn();
  if (!conn) return setStatus("sql-status", "Select a connection.", "bad");
  try {
    const r = await api.post(`/api/query/${encodeURIComponent(conn)}/${kind}`);
    setStatus("sql-status", r.message || kind + " done", r.ok ? "ok" : "bad");
  } catch (e) { setStatus("sql-status", e.message, "bad"); }
}

// -- result export (client-side) --------------------------------------------
function toCsv(columns, rows) {
  const esc2 = (v) => {
    const s = v === null || v === undefined ? "" : String(v);
    return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  return [columns.map(esc2).join(",")]
    .concat(rows.map((r) => r.map(esc2).join(","))).join("\n");
}
function download(filename, text, mime) {
  const blob = new Blob([text], { type: mime });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}
$("#sql-export-csv").addEventListener("click", () => {
  const r = sqlResults[sqlActiveResult];
  if (!r) return;
  download(`result_${sqlActiveResult + 1}.csv`, toCsv(r.columns || [], r.rows || []), "text/csv");
});
$("#sql-export-json").addEventListener("click", () => {
  const r = sqlResults[sqlActiveResult];
  if (!r) return;
  const objs = (r.rows || []).map((row) => Object.fromEntries((r.columns || []).map((c, i) => [c, row[i]])));
  download(`result_${sqlActiveResult + 1}.json`, JSON.stringify(objs, null, 2), "application/json");
});
$("#sql-result-copy").addEventListener("click", async () => {
  const r = sqlResults[sqlActiveResult];
  if (!r) return;
  const text = toCsv(r.columns || [], r.rows || []);
  try { await navigator.clipboard.writeText(text); setStatus("sql-status", "Copied all result data.", "ok"); }
  catch (_) { setStatus("sql-status", "Clipboard copy is unavailable in this browser.", "bad"); }
});
function sortCurrentResult(ascending) {
  const r = sqlResults[sqlActiveResult];
  if (!r || !(r.columns || []).length) return;
  const col = r.columns[0];
  r.rows = (r.rows || []).slice().sort((a, b) => {
    const av = a[0], bv = b[0];
    const an = Number(av), bn = Number(bv);
    if (!Number.isNaN(an) && !Number.isNaN(bn)) return ascending ? an - bn : bn - an;
    return ascending ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av));
  });
  renderSqlResults();
  saveSqlTabState();
  setStatus("sql-status", `Sorted by '${col}' (${ascending ? "ascending" : "descending"}).`, "ok");
}
$("#sql-result-sort-asc").addEventListener("click", () => sortCurrentResult(true));
$("#sql-result-sort-desc").addEventListener("click", () => sortCurrentResult(false));
$("#sql-result-filter").addEventListener("click", () => {
  const r = sqlResults[sqlActiveResult];
  if (!r || !(r.columns || []).length) return;
  const column = prompt("Column to filter:", r.columns[0]);
  if (!column || !r.columns.includes(column)) return;
  const text = prompt("Filter (contains):", "");
  if (!text) return;
  sqlFilter = { column, text };
  renderSqlResults();
  setStatus("sql-status", `Filtered '${column}'.`, "ok");
});
$("#sql-result-clear-filter").addEventListener("click", () => {
  sqlFilter = null;
  renderSqlResults();
  setStatus("sql-status", "Filter cleared.", "ok");
});

// -- query history (localStorage) -------------------------------------------
function getHistory() {
  try { return JSON.parse(localStorage.getItem("dbtool_sql_history") || "[]"); } catch (_) { return []; }
}
function addSqlHistory(sql) {
  let h = getHistory().filter((x) => x !== sql);
  h.unshift(sql);
  h = h.slice(0, 50);
  localStorage.setItem("dbtool_sql_history", JSON.stringify(h));
  renderHistory();
}
function renderHistory() {
  const ul = $("#sql-history");
  if (!ul) return;
  ul.innerHTML = getHistory().map((s) =>
    `<li><code>${esc(s.length > 120 ? s.slice(0, 120) + "…" : s)}</code></li>`).join("");
  $$("#sql-history li").forEach((li, i) => li.addEventListener("click", () => {
    $("#sql-input").value = getHistory()[i];
  }));
}
$("#sql-history-clear").addEventListener("click", () => {
  localStorage.removeItem("dbtool_sql_history"); renderHistory();
});

// ---- Objects ---------------------------------------------------------------
let objItems = [];
let objOps = [];
let objActiveTitle = "";

function objConn() { return $("#obj-conn").value; }
function objDbType() {
  const c = connectionsCache.find((x) => x.name === objConn());
  return c ? (c.db_type || c.type) : "";
}

async function populateObjTypes() {
  const el = $("#obj-type-buttons");
  const dbType = objDbType();
  if (!dbType) {
    objOps = [];
    el.innerHTML = "";
    $("#obj-info").textContent = "No active connection — connect from the Connections tab.";
    return;
  }
  try {
    const ops = await api.get(`/api/databases/ops?type=${encodeURIComponent(dbType)}`);
    objOps = ops || [];
  } catch (_) {
    objOps = ["Tables", "Views", "Procedures", "Functions", "Indexes", "Sequences", "Triggers", "Schemas", "Databases"]
      .map((t) => ({ display_name: t, operation: t }));
  }
  el.innerHTML = objOps.map((o, i) =>
    `<button class="obj-type-btn" data-index="${i}">${esc(o.display_name)}</button>`).join("");
  $$(".obj-type-btn").forEach((b) => b.addEventListener("click", () => executeObjOperation(parseInt(b.dataset.index, 10))));
  $("#obj-info").textContent = `${dbType} · ${objOps.length} browse operations · connection: ${objConn()}`;
}

$("#obj-refresh").addEventListener("click", () => { populateConnSelect("#obj-conn"); populateObjTypes(); });
$("#obj-conn").addEventListener("change", populateObjTypes);
$("#obj-filter").addEventListener("input", renderObjList);
$("#obj-filter-clear").addEventListener("click", () => { $("#obj-filter").value = ""; renderObjList(); });
$("#obj-clear-results").addEventListener("click", () => {
  objItems = [];
  objActiveTitle = "";
  $("#obj-exp-table").value = "";
  renderObjList();
  setStatus("obj-status", "Results cleared.", "ok");
});
$("#obj-import-jump").addEventListener("click", () => {
  $("#obj-export-import").open = true;
  $("#obj-imp-path").focus();
});

function renderObjList() {
  const filter = $("#obj-filter").value.trim().toLowerCase();
  const rows = objItems.filter((n) => !filter || String(n).toLowerCase().includes(filter));
  $("#obj-results-title").textContent = objActiveTitle || "No objects loaded";
  $("#obj-results-count").textContent = objActiveTitle ? `${rows.length} object(s)` : "";
  const root = $("#obj-results");
  if (!objActiveTitle) {
    root.innerHTML = `<p class="muted">Choose an object type on the left to list database objects.</p>`;
    return;
  }
  if (["tables", "collections"].includes(objActiveTitle.toLowerCase())) {
    root.innerHTML = `<p class="muted">▶ expands schema; Load Sample Data shows one row; Export Data saves rows.</p>`;
    rows.forEach((name) => root.appendChild(renderObjCard(String(name))));
    return;
  }
  const tpl = $("#obj-simple-list-template").content.cloneNode(true);
  tpl.querySelector("tbody").innerHTML = rows.map((n) => `<tr><td>${esc(n)}</td></tr>`).join("");
  root.innerHTML = "";
  root.appendChild(tpl);
}

async function executeObjOperation(index) {
  const conn = objConn();
  if (!conn) return setStatus("obj-status", "Select a connection.", "bad");
  const op = objOps[index];
  if (!op) return;
  await ensureObjConnOpen(conn);
  try {
    const type = op.display_name;
    const r = await api.get(`/api/objects/${encodeURIComponent(conn)}?type=${encodeURIComponent(type)}`);
    objItems = ((r && r.items) || []).map((it) =>
      typeof it === "string" ? it : (it.name || JSON.stringify(it)));
    objActiveTitle = type;
    renderObjList();
    setStatus("obj-status", `Found ${r.count ?? objItems.length} ${type.toLowerCase()}.`, "ok");
  } catch (e) { setStatus("obj-status", e.message, "bad"); }
}

function renderObjCard(name) {
  const node = $("#obj-table-card-template").content.cloneNode(true).querySelector(".object-card");
  node.querySelector(".obj-card-name").textContent = name;
  const detail = node.querySelector(".obj-card-detail");
  node.querySelector(".obj-card-schema").addEventListener("click", async () => {
    const conn = objConn();
    try {
      const r = await api.get(`/api/objects/${encodeURIComponent(conn)}/schema?table=${encodeURIComponent(name)}`);
      if (r.error) return setStatus("obj-status", r.error, "bad");
      const cols = r.columns || [];
      const rows = cols.map((c) => [c.name || c.column || c[0], c.type || c.data_type || c[1], c.nullable ?? c.null ?? "", c.default ?? ""]);
      fillTable(detail, ["Column", "Type", "Nullable", "Default"], rows);
      node.open = true;
      setStatus("obj-status", `Schema of ${name}: ${rows.length} columns.`, "ok");
    } catch (e) { setStatus("obj-status", e.message, "bad"); }
  });
  node.querySelector(".obj-card-sample").addEventListener("click", async () => {
    const conn = objConn();
    try {
      const r = await api.get(`/api/objects/${encodeURIComponent(conn)}/sample?table=${encodeURIComponent(name)}&limit=1`);
      if (r.error) return setStatus("obj-status", r.error, "bad");
      fillTable(detail, r.columns || [], r.rows || []);
      node.open = true;
      setStatus("obj-status", `Sampled ${name}.`, "ok");
    } catch (e) { setStatus("obj-status", e.message, "bad"); }
  });
  node.querySelector(".obj-card-count").addEventListener("click", async () => {
    const conn = objConn();
    try {
      const r = await api.get(`/api/objects/${encodeURIComponent(conn)}/count?table=${encodeURIComponent(name)}`);
      if (r.error) return setStatus("obj-status", r.error, "bad");
      setStatus("obj-status", `${name}: ${r.count} rows.`, "ok");
    } catch (e) { setStatus("obj-status", e.message, "bad"); }
  });
  node.querySelector(".obj-card-export").addEventListener("click", () => {
    $("#obj-exp-table").value = name;
    $("#obj-export-import").open = true;
    $("#obj-exp-path").focus();
    setStatus("obj-status", "Export Data: enter an output path and click Export Data.", "ok");
  });
  return node;
}

async function ensureObjConnOpen(conn) {
  try { await api.post(`/api/connections/${encodeURIComponent(conn)}/open`); } catch (_) {}
}

$("#obj-export").addEventListener("click", async () => {
  const conn = objConn();
  const table = $("#obj-exp-table").value.trim();
  const output_path = $("#obj-exp-path").value.trim();
  if (!conn || !table || !output_path) return setStatus("obj-status", "Connection, table, and output path required.", "bad");
  const limitRaw = $("#obj-exp-limit").value.trim();
  try {
    const r = await api.post(`/api/objects/${encodeURIComponent(conn)}/export`, {
      table, output_path, format: $("#obj-exp-fmt").value,
      limit: limitRaw ? parseInt(limitRaw, 10) : null,
    });
    setStatus("obj-status", r.message || (r.ok ? "Exported." : "Failed."), r.ok ? "ok" : "bad");
  } catch (e) { setStatus("obj-status", e.message, "bad"); }
});

$("#obj-import").addEventListener("click", async () => {
  const conn = objConn();
  const file_path = $("#obj-imp-path").value.trim();
  if (!conn || !file_path) return setStatus("obj-status", "Connection and CSV path required.", "bad");
  await ensureObjConnOpen(conn);
  try {
    const r = await api.post(`/api/objects/${encodeURIComponent(conn)}/import-csv`, {
      file_path, table: $("#obj-imp-table").value.trim() || null,
      create_table: $("#obj-imp-create").checked, chunk_size: 500,
    });
    setStatus("obj-status", r.message || (r.ok ? "Imported." : "Failed."), r.ok ? "ok" : "bad");
  } catch (e) { setStatus("obj-status", e.message, "bad"); }
});

// ---- Migration -------------------------------------------------------------
let migTables = [];

function migSource() { return $("#mig-source").value; }
function migTarget() { return $("#mig-target").value; }
function migTargetType() {
  const c = connectionsCache.find((x) => x.name === migTarget());
  return c ? (c.db_type || c.type) : "";
}

$("#mig-load-tables").addEventListener("click", async () => {
  const src = migSource();
  if (!src) return setStatus("mig-status", "Select a source connection.", "bad");
  try { await api.post(`/api/connections/${encodeURIComponent(src)}/open`); } catch (_) {}
  try {
    const r = await api.get(`/api/objects/${encodeURIComponent(src)}?type=Tables`);
    migTables = ((r && r.items) || []).map((it) => typeof it === "string" ? it : (it.name || String(it)));
    renderMigTables();
    setStatus("mig-status", `Loaded ${migTables.length} source tables.`, "ok");
  } catch (e) { setStatus("mig-status", e.message, "bad"); }
});

// Mirror Tk: when a source database/schema is set, show only tables whose
// schema-qualified prefix matches it (unqualified lists pass through).
function filterTablesBySourceDb(tables) {
  const sel = ($("#mig-source-db").value || "").trim();
  if (!sel) return tables;
  const qualified = tables.filter((t) => String(t).includes("."));
  if (!qualified.length) return tables;
  const matched = qualified.filter((t) => String(t).split(".")[0].trim() === sel);
  return matched.length ? matched : tables;
}

function renderMigTables() {
  const flt = $("#mig-table-filter").value.trim().toLowerCase();
  const box = $("#mig-tables-list");
  box.innerHTML = filterTablesBySourceDb(migTables)
    .filter((t) => !flt || t.toLowerCase().includes(flt))
    .map((t) => `<label class="chk"><input type="checkbox" class="mig-tchk" value="${esc(t)}" /> ${esc(t)}</label>`)
    .join("");
  $$(".mig-tchk").forEach((c) => c.addEventListener("change", updateMigCount));
  updateMigCount();
}
function updateMigCount() {
  $("#mig-table-count").textContent = `${selectedMigTables().length} selected`;
}
$("#mig-table-filter").addEventListener("input", renderMigTables);
$("#mig-source-db").addEventListener("input", renderMigTables);
$("#mig-check-all").addEventListener("click", () => { $$(".mig-tchk").forEach((c) => c.checked = true); updateMigCount(); });
$("#mig-uncheck-all").addEventListener("click", () => { $$(".mig-tchk").forEach((c) => c.checked = false); updateMigCount(); });

function selectedMigTables() {
  const checked = $$(".mig-tchk").filter((c) => c.checked).map((c) => c.value);
  if (checked.length) return checked;
  return $("#mig-tables").value.split(",").map((t) => t.trim()).filter(Boolean);
}

function migCommon() {
  const intOr = (sel) => { const v = $(sel).value.trim(); return v ? parseInt(v, 10) : null; };
  return {
    source_conn: migSource(), target_conn: migTarget(),
    target_db: $("#mig-target-db").value.trim(),
    prefix: $("#mig-prefix").value.trim(), suffix: $("#mig-suffix").value.trim(),
    batch_size: intOr("#mig-batch"), limit: intOr("#mig-limit"),
    column_map: $("#mig-column-map").value.trim(),
    continue_on_error: $("#mig-continue").checked,
    overflow_policy: $("#mig-overflow").value, null_policy: $("#mig-null").value,
    bool_policy: $("#mig-bool").value, timezone_policy: $("#mig-tz").value,
    target_timezone: $("#mig-target-tz").value.trim(),
    reset_sequences: $("#mig-reset-seq").checked, checkpoint: $("#mig-checkpoint").checked,
    report_path: $("#mig-report").value.trim(),
  };
}

function migOut(r) { $("#mig-output").textContent = JSON.stringify(r, null, 2); }
function migPreview(text) { $("#mig-output").textContent = text; }
function setMigStopEnabled(on) { const b = $("#mig-stop"); if (b) b.disabled = !on; }

async function migCall(fn) {
  const src = migSource(), tgt = migTarget();
  if (!src || !tgt) return setStatus("mig-status", "Select source and target.", "bad");
  try { await api.post(`/api/connections/${encodeURIComponent(src)}/open`); } catch (_) {}
  try { await api.post(`/api/connections/${encodeURIComponent(tgt)}/open`); } catch (_) {}
  try { await fn(); }
  catch (e) { migOut({ error: e.message }); setStatus("mig-status", e.message, "bad"); }
}

$("#mig-settings").addEventListener("click", async () => {
  try {
    const r = await api.get("/api/migrator/config");
    $("#mig-output").textContent = JSON.stringify(r, null, 2);
    setStatus("mig-status", "Migration settings loaded.", "ok");
  } catch (e) { setStatus("mig-status", e.message, "bad"); }
});

$("#mig-preview").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  migOpCancel = false;
  setMigStopEnabled(true);
  try {
    const r = await api.post("/api/migrator/convert-multi", {
      source_conn: migSource(), target_type: migTargetType(), tables,
      target_db: $("#mig-target-db").value.trim(),
      prefix: $("#mig-prefix").value.trim(), suffix: $("#mig-suffix").value.trim(),
      type_map: $("#mig-type-map").value.trim(),
    });
    if (migOpCancel) { migPreview("  (Stopped by user)\n"); return setStatus("mig-status", "Preview stopped.", "ok"); }
    migPreview(formatMigPreview(r));
    setStatus("mig-status", "Schema preview complete.", r.error ? "bad" : "ok");
  } finally { setMigStopEnabled(false); }
}));

$("#mig-sample").addEventListener("click", async () => {
  const src = migSource();
  const tables = selectedMigTables();
  if (!src) return setStatus("mig-status", "Select a source connection.", "bad");
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  migOpCancel = false;
  setMigStopEnabled(true);
  try {
    await api.post(`/api/connections/${encodeURIComponent(src)}/open`);
    const r = await api.post(`/api/migrator/${encodeURIComponent(src)}/sample-multi`, { tables, limit: 1 });
    if (migOpCancel) { migPreview("  (Stopped by user)\n"); return setStatus("mig-status", "Sample data stopped.", "ok"); }
    migPreview(formatMigSample(r));
    setStatus("mig-status", "Sample data retrieved.", r.error ? "bad" : "ok");
  } catch (e) {
    migOut({ error: e.message });
    setStatus("mig-status", e.message, "bad");
  } finally { setMigStopEnabled(false); }
});

$("#mig-clear").addEventListener("click", () => {
  $("#mig-output").textContent = "";
  setStatus("mig-status", "Preview cleared.", "ok");
});

$("#mig-stop").addEventListener("click", () => {
  migOpCancel = true;
  setStatus("mig-status", "Stopping operation…", "ok");
});

$("#mig-validate").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  const r = await api.post("/api/migrator/validate", {
    source_conn: migSource(), target_conn: migTarget(), tables,
    target_db: $("#mig-target-db").value.trim(),
    prefix: $("#mig-prefix").value.trim(), suffix: $("#mig-suffix").value.trim(),
    type_map: $("#mig-type-map").value.trim(),
  });
  migOut(r); setStatus("mig-status", "Validation complete.", r.error ? "bad" : "ok");
}));

$("#mig-rowcounts").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  const r = await api.post(`/api/migrator/${encodeURIComponent(migSource())}/row-counts`, { tables, limit: 1 });
  migOut(r); setStatus("mig-status", "Row counts retrieved.", "ok");
}));

$("#mig-convert").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  const body = {
    source_conn: migSource(), target_type: migTargetType(), tables,
    target_db: $("#mig-target-db").value.trim(),
    prefix: $("#mig-prefix").value.trim(), suffix: $("#mig-suffix").value.trim(),
    type_map: $("#mig-type-map").value.trim(),
  };
  const r = await api.post("/api/migrator/convert-multi", body);
  migOut(r);
  const ddl = r.joined_ddl || (r.tables || []).map((x) => (x.all_ddl || []).join("\n")).join("\n\n");
  if (ddl) $("#mig-ddl").value = ddl;
  setStatus("mig-status", "Schema converted — DDL ready to Apply.", r.error ? "bad" : "ok");
}));

$("#mig-apply").addEventListener("click", () => migCall(async () => {
  const ddl = $("#mig-ddl").value.trim();
  if (!ddl) return setStatus("mig-status", "No DDL to apply (run Convert Schema first).", "bad");
  const r = await api.post("/api/migrator/apply", {
    target_conn: migTarget(), ddl, stop_on_error: true,
    create_indexes: $("#mig-create-indexes").checked,
    drop_if_exists: $("#mig-drop-if-exists").checked,
  });
  migOut(r);
  const ok = !r.error;
  setStatus("mig-status", ok ? `DDL applied (${r.executed} statement(s)).` : `Apply failed: ${r.error}`, ok ? "ok" : "bad");
}));

$("#mig-transfer").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select at least one table.", "bad");
  const common = migCommon();
  let r;
  if (tables.length === 1) {
    r = await api.post("/api/migrator/transfer-data", {
      ...common, table: tables[0],
      where: $("#mig-where").value.trim(),
      columns: $("#mig-columns").value.trim(),
    });
  } else {
    const intOr = (sel) => { const v = $(sel).value.trim(); return v ? parseInt(v, 10) : null; };
    r = await api.post("/api/migrator/transfer-data-multi", {
      ...common, tables,
      parallel: $("#mig-parallel").checked, workers: intOr("#mig-workers"),
    });
  }
  migOut(r); setStatus("mig-status", r.message || (r.ok ? "Transfer complete." : "Transfer failed."), r.ok === false ? "bad" : "ok");
}));

$("#mig-compare").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select one table to compare.", "bad");
  const mode = ($("#mig-compare-mode") && $("#mig-compare-mode").value) || "sample";
  const r = await api.post("/api/migrator/compare-data", {
    source_conn: migSource(), target_conn: migTarget(), table: tables[0], mode,
  });
  migOut(r); setStatus("mig-status", `Data compare (${mode}) complete.`, r.error ? "bad" : "ok");
}));

$("#mig-compare-schema").addEventListener("click", () => migCall(async () => {
  const tables = selectedMigTables();
  if (!tables.length) return setStatus("mig-status", "Select one table to compare.", "bad");
  const r = await api.post("/api/migrator/compare-schema", {
    source_conn: migSource(), target_conn: migTarget(), table: tables[0],
  });
  migOut(r); setStatus("mig-status", "Schema compare complete.", r.error ? "bad" : "ok");
}));

// Dump native CREATE TABLE/INDEX DDL for the selected source tables (or all
// tables when none are selected). Needs only the source connection, so it does
// not go through migCall (which requires source+target). Mirrors the Tk
// "Dump Schema" button and the CLI `migrator dump` / API GET .../dump surfaces.
$("#mig-dump").addEventListener("click", async () => {
  const src = migSource();
  if (!src) return setStatus("mig-status", "Select a source connection.", "bad");
  try { await api.post(`/api/connections/${encodeURIComponent(src)}/open`); } catch (_) {}
  const tables = selectedMigTables();
  try {
    const parts = [];
    const queries = tables.length ? tables : [null];
    for (const tbl of queries) {
      const path = `/api/migrator/${encodeURIComponent(src)}/dump`
        + (tbl ? `?table=${encodeURIComponent(tbl)}` : "");
      const r = await api.get(path);
      if (r.error) return setStatus("mig-status", r.error, "bad");
      const ddl = (r.ddl || "").trim();
      if (ddl) parts.push(ddl);
    }
    const ddlText = parts.join("\n\n");
    $("#mig-ddl").value = ddlText;
    const scope = tables.length ? `${tables.length} table(s)` : "all tables";
    migOut({ ok: true, scope, ddl: ddlText });
    setStatus("mig-status",
      ddlText ? `Schema dump complete (${scope}). DDL shown above.`
              : "Schema dump produced no DDL.",
      ddlText ? "ok" : "bad");
  } catch (e) { setStatus("mig-status", e.message, "bad"); }
});

// ---- AI Query --------------------------------------------------------------
async function loadAiBackends() {
  try {
    const r = await api.get("/api/ai/backends");
    const all = r.all || [];
    const ready = r.ready || [];
    // Prefer the expanded options (local LLM shown per-model as
    // "<model> (local <engine>)"); fall back to the flat backend list.
    const options = r.options && r.options.length
      ? r.options
      : all.map((b) => ({ value: b, label: b, ready: ready.includes(b) }));
    const opts = options.map((o) =>
      `<option value="${esc(o.value)}">${esc(o.label)}${o.ready ? " ✓" : ""}</option>`).join("");
    $("#ai-backend").innerHTML = opts;
    $("#ai-set-backend").innerHTML = opts;
    const fb = $("#ai-fallback");
    if (fb) {
      fb.innerHTML = `<option value="">(none)</option>` + opts;
      const fbVal = r.fallback_value || r.fallback || "";
      if (fbVal) fb.value = fbVal;
    }
    const activeVal = r.active_value || r.active;
    if (activeVal) { $("#ai-backend").value = activeVal; $("#ai-set-backend").value = activeVal; }
    $("#ai-provider").textContent = r.provider || r.active || "—";
    $("#ai-model").textContent = r.model || "—";
    const state = $("#ai-backend-state");
    state.textContent = ready.length ? `ready: ${ready.join(", ")}` : "no backend verified";
    state.className = "badge " + (ready.length ? "ok" : "warn");
  } catch (e) {
    $("#ai-backend-state").textContent = "AI unavailable";
    $("#ai-backend-state").className = "badge warn";
  }
}
$("#ai-backends").addEventListener("click", loadAiBackends);
$("#ai-settings-open").addEventListener("click", async () => {
  const section = $("#ai-settings-sec");
  if (section) { section.open = true; section.scrollIntoView({ behavior: "smooth", block: "start" }); }
  try {
    const r = await api.get("/api/ai/config");
    $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("ai-status", "AI settings loaded.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});

function aiDbType() {
  const c = connectionsCache.find((x) => x.name === $("#ai-conn").value);
  return c ? (c.db_type || c.type) : "";
}

let aiInFlight = null;   // AbortController for the active AI request (Stop Query)
let aiSessions = [];
let aiActiveSession = "";

function aiSessionId(s) {
  return s.session_id || s.id || s.name || "";
}
function aiSessionLabel(s) {
  const id = aiSessionId(s);
  const conn = s.connection_name || s.connection || "";
  return `${id.slice(0, 8) || "(session)"}${conn ? " · " + conn : ""}`;
}
async function loadAiSessions() {
  const sel = $("#ai-session");
  if (!sel) return;
  try {
    const r = await api.get("/api/ai/sessions");
    aiSessions = r.sessions || [];
    sel.innerHTML = aiSessions.length
      ? aiSessions.map((s) => `<option value="${esc(aiSessionId(s))}">${esc(aiSessionLabel(s))}</option>`).join("")
      : '<option value="">(none)</option>';
    if (aiActiveSession && aiSessions.some((s) => aiSessionId(s) === aiActiveSession)) {
      sel.value = aiActiveSession;
    } else {
      aiActiveSession = sel.value || "";
    }
  } catch (e) {
    sel.innerHTML = '<option value="">(unavailable)</option>';
    setStatus("ai-status", e.message, "bad");
  }
}
function currentAiSession() {
  return $("#ai-session") ? ($("#ai-session").value || aiActiveSession || "") : "";
}
async function ensureAiSession() {
  let sid = currentAiSession();
  if (sid) return sid;
  const r = await api.post("/api/ai/sessions", {
    connection: $("#ai-conn").value || "",
    backend: $("#ai-backend").value || "",
    share_context: true,
    sql_mode: $("#ai-sql-mode").value || "summary",
  });
  const s = r.session || {};
  sid = aiSessionId(s);
  aiActiveSession = sid;
  await loadAiSessions();
  if ($("#ai-session")) $("#ai-session").value = sid;
  return sid;
}

$("#ai-session-refresh").addEventListener("click", loadAiSessions);
$("#ai-session").addEventListener("change", async () => {
  aiActiveSession = currentAiSession();
  if (!aiActiveSession) return;
  try {
    const r = await api.get(`/api/ai/sessions/${encodeURIComponent(aiActiveSession)}`);
    const s = r.session || {};
    if (s.connection_name || s.connection) $("#ai-conn").value = s.connection_name || s.connection;
    if (s.backend) $("#ai-backend").value = s.backend;
    if (s.sql_mode) $("#ai-sql-mode").value = s.sql_mode;
    if (s.current_sql) $("#ai-sql").value = s.current_sql;
    if (s.sql_execution_rules && $("#ai-exec-rules-text")) {
      $("#ai-exec-rules-text").value = s.sql_execution_rules;
    }
    const log = document.getElementById("ai-chat-log");
    if (log) {
      log.innerHTML = "";
      const mc = s.message_count || 0;
      if (mc) aiChatAppend("system", `Session has ${mc} message(s). Use follow-up to continue.`);
    }
    setStatus("ai-status", "Session loaded.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-session-new").addEventListener("click", async () => {
  try {
    const r = await api.post("/api/ai/sessions", {
      connection: $("#ai-conn").value || "",
      backend: $("#ai-backend").value || "",
      share_context: true,
      sql_mode: $("#ai-sql-mode").value || "summary",
    });
    aiActiveSession = aiSessionId(r.session || {});
    await loadAiSessions();
    if ($("#ai-session")) $("#ai-session").value = aiActiveSession;
    setStatus("ai-status", "New AI session created.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-session-delete").addEventListener("click", async () => {
  const sid = currentAiSession();
  if (!sid) return setStatus("ai-status", "Select a session first.", "bad");
  if (!confirm("Delete selected AI session?")) return;
  try {
    await api.del(`/api/ai/sessions/${encodeURIComponent(sid)}`);
    aiActiveSession = "";
    await loadAiSessions();
    setStatus("ai-status", "Session deleted.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
async function askAiSession(mode) {
  const question = $("#ai-question").value.trim();
  if (!question) return setStatus("ai-status", "Question required.", "bad");
  const sid = await ensureAiSession();
  setStatus("ai-status", mode === "followup" ? "Asking follow-up…" : "Asking session…");
  const r = await api.post(`/api/ai/sessions/${encodeURIComponent(sid)}/messages`, {
    message: question, mode,
  });
  $("#ai-sql").value = r.sql || r.summary_sql || "";
  setStatus("ai-explanation", r.explanation || JSON.stringify(r.cross_tab || r, null, 2));
  if (r.sql || r.summary_sql) addAiHistory(question);
  setStatus("ai-status", r.error ? r.error : "Session response received.", r.error ? "bad" : "ok");
}
$("#ai-session-followup").addEventListener("click", async () => {
  try { await askAiSession("followup"); } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-session-cross").addEventListener("click", async () => {
  const instruction = $("#ai-question").value.trim();
  if (!instruction) return setStatus("ai-status", "Instruction required.", "bad");
  try {
    const sid = await ensureAiSession();
    const r = await api.post(`/api/ai/sessions/${encodeURIComponent(sid)}/cross-tab`, { instruction });
    setStatus("ai-explanation", JSON.stringify(r, null, 2));
    setStatus("ai-status", r.error ? r.error : "Cross-tab action routed.", r.error ? "bad" : "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});

$("#ai-ask").addEventListener("click", async () => {
  const connection = $("#ai-conn").value;
  const question = $("#ai-question").value.trim();
  if (!connection || !question) return setStatus("ai-status", "Connection and question required.", "bad");
  try { await api.post(`/api/connections/${encodeURIComponent(connection)}/open`); } catch (_) {}
  setStatus("ai-status", "Asking AI…");
  aiInFlight = new AbortController();
  try {
    const useRag = $("#ai-use-rag") && $("#ai-use-rag").checked;
    const path = useRag ? "/api/ai/rag/ask" : "/api/ai/query";
    const body = useRag
      ? { connection, question, backend: $("#ai-backend").value || "" }
      : {
          connection, question, backend: $("#ai-backend").value || "",
          sql_mode: $("#ai-sql-mode").value || "summary",
          sql_execution_rules: ($("#ai-exec-rules-text")?.value || "").trim(),
        };
    const r = await api.call("POST", path, body, aiInFlight.signal);
    $("#ai-sql").value = r.sql || r.summary_sql || r.sql || "";
    setStatus("ai-explanation", r.explanation || "");
    if (useRag) renderAiRagContext(r);
    showAiResultTab("explanation");
    if (r.sql || r.summary_sql) {
      aiChatAppend("user", question);
      aiChatAppend("assistant", r.explanation || r.sql || r.summary_sql);
    }
    setStatus("ai-status", r.error ? r.error : "SQL generated.", r.error ? "bad" : "ok");
    if (r.sql || r.summary_sql) addAiHistory(question);
    if ($("#ai-auto-exec")?.checked && (r.sql || r.summary_sql)) {
      $("#ai-exec").click();
    }
  } catch (e) {
    if (e.name === "AbortError") { setStatus("ai-status", "Query stopped.", "ok"); }
    else { setStatus("ai-status", e.message, "bad"); }
  } finally {
    aiInFlight = null;
  }
});

$("#ai-explain").addEventListener("click", () => aiAnalyse("explain"));
$("#ai-optimize").addEventListener("click", () => aiAnalyse("optimize"));
async function aiAnalyse(kind) {
  const sql = $("#ai-sql").value.trim();
  if (!sql) return setStatus("ai-status", "No SQL to analyse.", "bad");
  setStatus("ai-status", kind + "…");
  try {
    const r = await api.post(`/api/ai/${kind}`, { sql, connection: $("#ai-conn").value || "", db_type: aiDbType() });
    const target = kind === "optimize" ? "ai-optimization" : "ai-explanation";
    setStatus(target, r.explanation || r.analysis || r.optimization || JSON.stringify(r));
    showAiResultTab(kind === "optimize" ? "optimization" : "explanation");
    setStatus("ai-status", r.error ? r.error : kind + " complete.", r.error ? "bad" : "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
}

$("#ai-exec").addEventListener("click", async () => {
  const conn = $("#ai-conn").value;
  const sql = $("#ai-sql").value.trim();
  if (!conn || !sql) return setStatus("ai-status", "Connection and SQL required.", "bad");
  try { await api.post(`/api/connections/${encodeURIComponent(conn)}/open`); } catch (_) {}
  try {
    const r = await api.post("/api/ai/execute-sql", { connection: conn, sql });
    if (r.error) return setStatus("ai-status", r.error, "bad");
    fillTable($("#ai-grid"), r.columns || [], r.rows || []);
    showAiResultTab("results");
    setStatus("ai-status", `OK — ${r.rowcount ?? (r.rows || []).length} rows`, "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});

$("#ai-clear").addEventListener("click", () => {
  $("#ai-question").value = ""; $("#ai-sql").value = "";
  setStatus("ai-explanation", ""); setStatus("ai-optimization", "");
  setStatus("ai-review-out", "");
  const rag = document.getElementById("ai-rag-context"); if (rag) rag.textContent = "";
  const log = document.getElementById("ai-chat-log"); if (log) log.innerHTML = "";
  fillTable($("#ai-grid"), [], []);
  showAiResultTab("results");
});

// Fallback backend — failover + repairs wrong/failed SQL (Tk status-row combo).
const aiFallbackSet = $("#ai-fallback-set");
if (aiFallbackSet) aiFallbackSet.addEventListener("click", async () => {
  const name = $("#ai-fallback") ? $("#ai-fallback").value : "";
  setStatus("ai-status", "Setting fallback backend…");
  try {
    const r = await api.call("PUT", "/api/ai/fallback-backend", { backend: name, verify: true });
    setStatus("ai-status", r.message || "Fallback backend updated.", r.ok === false ? "bad" : "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});

// Flag buttons — mark the current query as wrong (syntax/logic) or wrongly
// interpreted; route through svc.correct_sql via the fallback backend.
async function aiFlag(mode) {
  const sql = $("#ai-sql").value.trim();
  if (!sql) return setStatus("ai-status", "Generate a query first, then flag it.", "bad");
  showAiResultTab("chat");
  if (mode === "syntax") {
    aiChatAppend("system", "Flagged as an incorrect query — asking the fallback backend to repair it.");
  } else {
    aiChatAppend("system", "Flagged as a wrong interpretation — asking the fallback backend to re-answer the question.");
  }
  try {
    const r = await api.post("/api/ai/correct-sql", {
      question: $("#ai-question").value.trim(),
      sql,
      connection: $("#ai-conn").value || "",
      db_type: aiDbType(),
      error_text: mode === "syntax"
        ? "User flagged this query as incorrect (syntax or logic — e.g. wrong joins, subqueries, date handling)."
        : "",
      mode,
      backend: $("#ai-fallback") ? $("#ai-fallback").value : "",
    });
    if (r.sql) {
      $("#ai-sql").value = r.sql;
      aiChatAppend("assistant", `Corrected by ${r.backend_used || "fallback"}:\n${r.sql}`);
      setStatus("ai-status", "Corrected query placed in the Generated SQL box — review and Execute.", "ok");
    } else {
      aiChatAppend("assistant", r.error || "No correction produced.");
      setStatus("ai-status", r.error || "Correction failed.", "bad");
    }
  } catch (e) {
    aiChatAppend("system", "Error: " + e.message);
    setStatus("ai-status", e.message, "bad");
  }
}
const aiFlagQuery = $("#ai-flag-query");
if (aiFlagQuery) aiFlagQuery.addEventListener("click", () => aiFlag("syntax"));
const aiFlagInterp = $("#ai-flag-interp");
if (aiFlagInterp) aiFlagInterp.addEventListener("click", () => aiFlag("interpretation"));

// Questions from file — read NL questions and iterate them (Generate, and when
// Auto-execute is on, run each). Mirrors the Tk "Questions from file" button.
let aiQuestionQueue = [];
async function aiAdvanceQuestions() {
  if (!aiQuestionQueue.length) { setStatus("ai-status", "No more questions in the queue.", "ok"); return; }
  const q = aiQuestionQueue.shift();
  $("#ai-question").value = q;
  setStatus("ai-status", `Question: ${q}  (${aiQuestionQueue.length} remaining)`, "");
  await new Promise((res) => { $("#ai-ask").addEventListener("click", res, { once: true }); $("#ai-ask").click(); });
  const uninterrupted = ($("#ai-uninterrupted") && $("#ai-uninterrupted").checked)
    || ($("#ai-auto-exec") && $("#ai-auto-exec").checked);
  if (uninterrupted && aiQuestionQueue.length) setTimeout(aiAdvanceQuestions, 200);
}
const aiQuestionsFile = $("#ai-questions-file");
if (aiQuestionsFile) aiQuestionsFile.addEventListener("click", () => {
  if (aiQuestionQueue.length) { aiAdvanceQuestions(); return; }
  const input = document.createElement("input");
  input.type = "file"; input.accept = ".txt,.csv,.md,text/plain";
  input.onchange = async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    const raw = await file.text();
    aiQuestionQueue = raw.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
    if (!aiQuestionQueue.length) { setStatus("ai-status", "No questions found in the file.", "bad"); return; }
    setStatus("ai-status", `Loaded ${aiQuestionQueue.length} question(s) from ${file.name}.`, "ok");
    aiAdvanceQuestions();
  };
  input.click();
});
$("#ai-stop").addEventListener("click", () => {
  if (aiInFlight) {
    aiInFlight.abort();
    aiInFlight = null;
    setStatus("ai-status", "Query stopped.", "ok");
  } else {
    setStatus("ai-status", "No AI query in progress.", "ok");
  }
});
$("#ai-copy-sql").addEventListener("click", async () => {
  try { await navigator.clipboard.writeText($("#ai-sql").value); setStatus("ai-status", "SQL copied.", "ok"); }
  catch (_) { setStatus("ai-status", "Clipboard copy is unavailable in this browser.", "bad"); }
});
$("#ai-edit-sql").addEventListener("click", () => { $("#ai-sql").focus(); setStatus("ai-status", "Generated SQL is editable.", "ok"); });
$("#ai-send-editor").addEventListener("click", () => {
  const sql = $("#ai-sql").value;
  if (!sql.trim()) return setStatus("ai-status", "No SQL to send.", "bad");
  // Open the generated SQL in a fresh SQL Editor tab (mirrors Tk behaviour),
  // carrying the AI connection across so it is ready to run.
  saveSqlTabState();
  const id = "tab" + sqlNextTab;
  sqlTabs.push({ id, label: "AI " + sqlNextTab, text: sql, conn: $("#ai-conn").value || sqlConn(), results: [], active: 0 });
  sqlNextTab += 1;
  sqlActiveTab = id;
  activateTab("sql");
  loadSqlTabState();
  renderSqlTabs();
  setStatus("ai-status", "SQL sent to a new SQL Editor tab.", "ok");
});
$("#ai-review-rules").addEventListener("click", () => {
  const ta = $("#ai-review-rules-text");
  if (ta) { ta.focus(); setStatus("ai-status", "Edit review rules below, then Run Review.", "ok"); }
});
$("#ai-import-review").addEventListener("click", () => {
  const input = document.createElement("input");
  input.type = "file"; input.accept = ".sql,.txt,text/plain";
  input.onchange = async () => {
    const file = input.files && input.files[0];
    if (!file) return;
    $("#ai-sql").value = await file.text();
    setStatus("ai-status", "SQL loaded for review from " + file.name, "ok");
  };
  input.click();
});
$("#ai-review").addEventListener("click", async () => {
  const sql = $("#ai-sql").value.trim();
  if (!sql) return setStatus("ai-status", "No SQL to review.", "bad");
  setStatus("ai-status", "Running review…");
  try {
    const rules = ($("#ai-review-rules-text")?.value || localStorage.getItem("dbtool_ai_review_rules") || "").trim();
    const r = await api.post("/api/ai/review", {
      sql, rules, connection: $("#ai-conn").value || "", db_type: aiDbType(),
    });
    setStatus("ai-review-out", r.review || r.explanation || r.analysis || JSON.stringify(r));
    showAiResultTab("review");
    setStatus("ai-status", r.error ? r.error : "Review complete.", r.error ? "bad" : "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});

// Chat follow-up (Chat result tab) — mirrors the Tk "Send Follow-up" pane.
$("#ai-followup-send").addEventListener("click", async () => {
  const msg = $("#ai-followup").value.trim();
  if (!msg) return setStatus("ai-status", "Enter a follow-up message.", "bad");
  showAiResultTab("chat");
  aiChatAppend("user", msg);
  $("#ai-followup").value = "";
  setStatus("ai-status", "Processing follow-up…");
  try {
    const sid = await ensureAiSession();
    const r = await api.post(`/api/ai/sessions/${encodeURIComponent(sid)}/messages`, { message: msg, mode: "followup" });
    if (r.sql || r.summary_sql) $("#ai-sql").value = r.sql || r.summary_sql;
    aiChatAppend("assistant", r.explanation || r.summary_sql || r.sql || JSON.stringify(r));
    setStatus("ai-status", r.error ? r.error : "Follow-up processed.", r.error ? "bad" : "ok");
  } catch (e) {
    aiChatAppend("system", "Error: " + e.message);
    setStatus("ai-status", e.message, "bad");
  }
});
$("#ai-chat-clear").addEventListener("click", () => {
  const log = document.getElementById("ai-chat-log"); if (log) log.innerHTML = "";
  $("#ai-followup").value = "";
  setStatus("ai-status", "Chat cleared.", "ok");
});
$("#ai-exec-rules").addEventListener("click", () => {
  const ta = $("#ai-exec-rules-text");
  if (ta) { ta.focus(); setStatus("ai-status", "Edit SQL execution rules below.", "ok"); }
});

$("#ai-session-save").addEventListener("click", async () => {
  try {
    const r = await api.post("/api/ai/sessions/save", {});
    setStatus("ai-status", r.path ? `Sessions saved: ${r.path}` : "Sessions saved.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-session-load").addEventListener("click", async () => {
  try {
    const r = await api.post("/api/ai/sessions/load", {});
    await loadAiSessions();
    setStatus("ai-status", `Loaded ${(r.sessions || []).length} session(s).`, "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-session-exec-sql").addEventListener("click", async () => {
  const sql = $("#ai-sql").value.trim();
  if (!sql) return setStatus("ai-status", "No SQL to execute.", "bad");
  try {
    const sid = await ensureAiSession();
    const rules = ($("#ai-exec-rules-text")?.value || "").trim();
    if (rules) {
      await api.call("PATCH", `/api/ai/sessions/${encodeURIComponent(sid)}`, {
        sql_execution_rules: rules,
      });
    }
    const r = await api.post(`/api/ai/sessions/${encodeURIComponent(sid)}/execute-sql`, { sql });
    if (r.error || r.blocked) return setStatus("ai-status", r.error || "Blocked by rules.", "bad");
    const res = r.result || {};
    if (res.error) return setStatus("ai-status", res.error, "bad");
    fillTable($("#ai-grid"), res.columns || [], res.rows || []);
    showAiResultTab("results");
    setStatus("ai-status", `OK — ${res.rowcount ?? (res.rows || []).length} rows (session rules)`, "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-refresh-conns").addEventListener("click", () => { populateConnSelect("#ai-conn"); setStatus("ai-status", "Connections refreshed.", "ok"); });
$("#ai-rag-index").addEventListener("click", async () => {
  const conn = $("#ai-conn").value;
  if (!conn) { setStatus("ai-status", "Select a connection first.", "bad"); return; }
  setStatus("ai-status", `Building RAG index for '${conn}'…`, "");
  try {
    const r = await api.post("/api/ai/rag/index", { connection: conn, rebuild: false });
    setStatus("ai-status", `Indexed ${r.indexed} docs (provider=${r.provider}, dim=${r.dim}).`, "ok");
  } catch (e) { setStatus("ai-status", "RAG index: " + e.message, "bad"); }
});
$("#ai-rag-manage").addEventListener("click", async () => {
  let activeRows = [];
  try {
    activeRows = await api.get("/api/connections/active");
  } catch (_) { activeRows = []; }
  const connOpts = (activeRows || []).map((c) =>
    `<option value="${esc(c.name)}">${esc(c.name)}</option>`).join("")
    || '<option value="">(none — connect first)</option>';
  openModal("RAG Manager", `
    <p class="hint">Index live database schema, upload documents, glossary terms, NL→SQL examples, analytical patterns, or a codebase folder. Enable <strong>Use RAG</strong> in Generate SQL to ground answers on the selected scope.</p>
    <form id="rag-manager-form">
      <label class="form-row">Active database
        <select id="rag-conn">${connOpts}</select>
      </label>
      <label class="checkbox form-row"><input type="checkbox" id="rag-standalone" /> Standalone collection</label>
      <label class="form-row">Collection name (standalone)
        <input id="rag-scope" value="docs" placeholder="e.g. docs, myapp-code" disabled />
      </label>
      <label class="form-row">Action
        <select id="rag-action">
          <option value="overview">Overview (status + breakdown)</option>
          <option value="index">Index schema</option>
          <option value="reindex">Re-index schema</option>
          <option value="codebase">Add codebase folder</option>
          <option value="document">Add document (upload or paste)</option>
          <option value="docs">List documents</option>
          <option value="preview">Preview search</option>
          <option value="eval">Evaluate retrieval quality</option>
          <option value="drift">Check schema drift</option>
          <option value="reindex_stale">Re-index if stale</option>
          <option value="schedule_status">Scheduled re-index: status</option>
          <option value="schedule_start">Scheduled re-index: start</option>
          <option value="schedule_stop">Scheduled re-index: stop</option>
          <option value="seed">Seed analytical query patterns</option>
          <option value="analytics">Show analytical query library</option>
          <option value="example">Add NL→SQL example</option>
          <option value="examples_file">Import examples from file (JSONL/JSON/CSV/TSV/text)</option>
          <option value="glossary">Add glossary term</option>
          <option value="help">How to use RAG</option>
          <option value="clear">Clear collection</option>
          <option value="remove">Remove document</option>
        </select>
      </label>
      <label class="form-row">Query / question / term / title
        <input id="rag-query" placeholder="e.g. monthly revenue trend, ARR, or document title" />
      </label>
      <label class="form-row">Also search scopes (comma-separated; for Preview)
        <input id="rag-extra-scopes" placeholder="rag_code, docs" />
      </label>
      <label class="form-row">File or folder path (codebase / local document)
        <input id="rag-path" placeholder="/path/to/repo or leave blank for upload" />
      </label>
      <label class="form-row">SQL / definition / pasted document text
        <textarea id="rag-text" rows="6" placeholder="Paste SQL, glossary definition, or document text"></textarea>
      </label>
      <label class="form-row">Document / examples file (browser upload)
        <input id="rag-file" type="file" accept=".txt,.md,.markdown,.rst,.sql,.csv,.tsv,.json,.jsonl,.log,.yaml,.yml,.ini,.cfg,.html,.htm,.xml" />
      </label>
      <div class="row">
        <button type="submit" class="primary">Run</button>
        <button type="button" id="rag-close">Close</button>
      </div>
      <div class="status" id="rag-modal-status"></div>
      <pre id="rag-modal-output" class="prewrap"></pre>
    </form>
  `);
  const scopeInput = $("#rag-scope");
  $("#rag-standalone").addEventListener("change", (e) => {
    scopeInput.disabled = !e.target.checked;
  });
  $("#rag-close").addEventListener("click", closeModal);
  $("#rag-manager-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const scope = $("#rag-standalone").checked
      ? $("#rag-scope").value.trim()
      : ($("#rag-conn").value || "").trim();
    const action = $("#rag-action").value;
    const q = $("#rag-query").value.trim();
    const text = $("#rag-text").value.trim();
    const path = ($("#rag-path").value || "").trim();
    const standalone = $("#rag-standalone").checked;
    const out = $("#rag-modal-output");
    const noScopeActions = ["analytics", "help", "schedule_status", "schedule_start", "schedule_stop"];
    if (!scope && !noScopeActions.includes(action)) {
      setStatus("rag-modal-status", "Select an active database or enter a collection name.", "bad");
      return;
    }
    try {
      let r;
      if (action === "overview") {
        r = await api.get("/api/ai/rag/overview?scope=" + encodeURIComponent(scope));
        const st = r.status || {};
        const br = r.breakdown || {};
        const mm = st.embedder_mismatch || br.embedder_mismatch || {};
        const lines = [
          `scope      : ${scope}`,
          `indexed    : ${st.indexed}`,
          `doc_count  : ${st.doc_count}`,
          `provider   : ${(st.meta || {}).provider || ""}`,
          `dim        : ${(st.meta || {}).dim || ""}`,
          "\nbreakdown:",
          ...Object.entries(br.counts || {}).sort().map(([k, v]) => `  ${k.padEnd(12)} ${v}`),
        ];
        if (mm.mismatch) lines.push(`WARNING    : ${mm.message}`);
        out.textContent = lines.join("\n");
        setStatus("rag-modal-status", "RAG: overview done.", "ok");
        return;
      } else if (action === "index" || action === "reindex") {
        if (standalone) throw new Error("Schema indexing applies to active database connections only.");
        r = await api.post("/api/ai/rag/index", { connection: scope, rebuild: action === "reindex" });
      } else if (action === "codebase") {
        if (!path) throw new Error("Enter a codebase folder path.");
        r = await api.post("/api/ai/rag/add-codebase", { folder: path, scope, standalone, replace: true });
      } else if (action === "preview") {
        if (!q) throw new Error("Enter a query.");
        const extra = ($("#rag-extra-scopes").value || "")
          .split(",").map((s) => s.trim()).filter(Boolean);
        if (extra.length) {
          r = await api.post("/api/ai/rag/search-multi",
            { scopes: [scope, ...extra], query: q, k: 8 });
        } else {
          r = await api.post("/api/ai/rag/preview", { connection: scope, query: q, k: 8 });
        }
        out.textContent = [r.preview || "", "", "Context block:", r.context || ""].join("\n");
        setStatus("rag-modal-status", "RAG: preview done.", "ok");
        return;
      } else if (action === "eval") {
        r = await api.post("/api/ai/rag/eval", { connection: scope, per_case: true });
        const m = r.metrics || {};
        const lines = [
          `Retrieval eval for '${scope}' (k=${r.k}, seeded_from_examples=${r.seeded_from_examples})`,
          `  cases             : ${m.cases || 0}`,
          `  recall@k          : ${(m.recall_at_k || 0).toFixed(4)}`,
          `  MRR               : ${(m.mrr || 0).toFixed(4)}`,
          `  context precision : ${(m.context_precision || 0).toFixed(4)}`,
          "",
          ...(r.cases_detail || []).slice(0, 30).map((c) =>
            `  r@k=${c.recall_at_k.toFixed(2)} rr=${c.reciprocal_rank.toFixed(2)} ` +
            `cp=${c.context_precision.toFixed(2)}  ${(c.question || "").slice(0, 60)}`),
        ];
        out.textContent = lines.join("\n");
        setStatus("rag-modal-status", "RAG: eval done.", "ok");
        return;
      } else if (action === "drift") {
        r = await api.get("/api/ai/rag/drift?connection=" + encodeURIComponent(scope));
        out.textContent = r.message || "";
        setStatus("rag-modal-status", r.changed ? "RAG: schema changed." : "RAG: schema unchanged.",
          r.changed ? "bad" : "ok");
        return;
      } else if (action === "reindex_stale") {
        r = await api.post("/api/ai/rag/reindex-stale", { connections: [scope], force: false });
        out.textContent = (r.results || []).map((x) =>
          `  ${x.connection}: ${x.skipped ? "skipped" : "reindexed"} (${x.reason})`).join("\n");
        setStatus("rag-modal-status", `RAG: re-indexed ${r.reindexed || 0} connection(s).`, "ok");
        return;
      } else if (action === "schedule_status" || action === "schedule_start" || action === "schedule_stop") {
        if (action === "schedule_status") {
          r = await api.get("/api/ai/rag/reindex/schedule");
        } else {
          const verb = action === "schedule_start" ? "start" : "stop";
          r = await api.post("/api/ai/rag/reindex/schedule/" + verb, {});
        }
        out.textContent = [
          `enabled        : ${r.enabled}`,
          `running        : ${r.running}`,
          `start_time     : ${r.start_time}`,
          `duration_hours : ${r.duration_hours}`,
          `connections    : ${(r.connections || []).join(", ") || "(all indexed)"}`,
          `force          : ${r.force}`,
          `next_run       : ${r.next_run || ""}`,
          `last_run_date  : ${r.last_run_date || ""}`,
          `last_result    : ${JSON.stringify(r.last_result || {})}`,
        ].join("\n");
        setStatus("rag-modal-status", `RAG: scheduler ${r.running ? "running" : "stopped"}.`, "ok");
        return;
      } else if (action === "document") {
        const file = $("#rag-file").files[0];
        let content = text;
        let source = q || "pasted-document";
        let title = q || source;
        if (file) {
          content = await file.text();
          source = file.name;
          title = q || file.name.replace(/\.[^.]+$/, "");
        } else if (path) {
          r = await api.post("/api/ai/rag/document", { scope, file_path: path, title: q || path, source: q || path, standalone });
          out.textContent = JSON.stringify(r, null, 2);
          setStatus("rag-modal-status", "RAG: document done.", "ok");
          return;
        }
        if (!content) throw new Error("Choose a file, enter a path, or paste document text.");
        r = await api.post("/api/ai/rag/document", { scope, content, source, title, standalone });
      } else if (action === "docs") {
        r = await api.get("/api/ai/rag/documents?scope=" + encodeURIComponent(scope));
      } else if (action === "seed") {
        r = await api.post("/api/ai/rag/seed-analytics", { scope, categories: [], standalone });
      } else if (action === "analytics") {
        r = await api.get("/api/ai/rag/analytics");
      } else if (action === "example") {
        if (!q || !text) throw new Error("Question and SQL are required.");
        r = await api.post("/api/ai/rag/example", { connection: scope, question: q, sql: text, description: "" });
      } else if (action === "examples_file") {
        const file = $("#rag-file").files[0];
        let content = text;
        let fname = "";
        if (file) { content = await file.text(); fname = file.name; }
        if (!content) throw new Error("Choose an examples file or paste its content in the text box.");
        r = await api.post("/api/ai/rag/examples-file", {
          connection: scope, content, fmt: "auto", standalone,
        });
        out.textContent = JSON.stringify({ source: fname, ...r }, null, 2);
        setStatus("rag-modal-status", `RAG: imported ${r.added || 0} example(s).`, "ok");
        return;
      } else if (action === "glossary") {
        if (!q || !text) throw new Error("Term and definition are required.");
        r = await api.post("/api/ai/rag/glossary", { connection: scope, term: q, definition: text });
      } else if (action === "help") {
        r = {
          ok: true,
          help: [
            "1. Select an active database (or Standalone collection).",
            "2. Index schema to build table/relationship docs.",
            "3. Add documents, glossary, examples, or a codebase folder.",
            "4. Preview search shows ranked hits + context block.",
            "5. Enable Use RAG in Generate SQL to ground answers.",
            "6. Re-index if embedder provider/dim warning appears.",
          ],
        };
      } else if (action === "remove") {
        if (!q) throw new Error("Enter the document name/source to remove in the query field.");
        r = await api.post("/api/ai/rag/remove-document", { scope, source: q });
      } else if (action === "clear") {
        if (!confirm(`Clear all RAG docs for '${scope}'?`)) return;
        r = await api.del("/api/ai/rag?connection=" + encodeURIComponent(scope));
      } else {
        throw new Error("Unknown action.");
      }
      out.textContent = JSON.stringify(r, null, 2);
      $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
      setStatus("rag-modal-status", "RAG: " + action + " done.", "ok");
      setStatus("ai-status", "RAG: " + action + " done.", "ok");
    } catch (err) {
      setStatus("rag-modal-status", err.message, "bad");
      setStatus("ai-status", "RAG: " + err.message, "bad");
    }
  });
});
// ---- LLM background jobs (live SSE progress) -------------------------------
const llmJobState = { jobId: null, eventSource: null, pollTimer: null, eventCursor: 0 };

function llmProgressMessage(ev) {
  const etype = ev.type;
  if (etype === "training_capture") {
    if (ev.status === "collecting") return "Collecting training data…";
    if (ev.status === "captured") {
      return `Collected ${ev.pairs || 0} pair(s) (${ev.source || ""}); training…`;
    }
  }
  if (etype === "training_rag") {
    if (ev.status === "indexing_parallel") return `Indexing RAG for '${ev.connection || ""}'…`;
    if (ev.status === "indexed") return "RAG indexing complete.";
    if (ev.status === "index_failed") return "RAG indexing failed.";
  }
  if (etype === "training_progress") return `Training ${ev.model || "model"}…`;
  if (etype === "training_epoch") {
    return `Training ${ev.model || "model"}: epoch ${ev.epoch ?? "?"}, loss ${ev.loss ?? "?"}`;
  }
  if (etype === "training_done") {
    if (ev.ok) return `Training complete — ${ev.pairs || 0} pair(s) (${ev.source || ""})`;
    return "Training failed.";
  }
  if (etype === "harvest_offline_collected") {
    return `Offline harvest collected ${ev.pairs || 0} validated pairs; training local model…`;
  }
  if (etype === "harvest_train_done") {
    const phase = String(ev.phase || "training").replace(/_/g, " ");
    return ev.ok ? `${phase} training complete.` : (ev.reason || "Training failed.");
  }
  if (etype === "harvest_backend_start") return "Offline model trained; starting optional backend enrichment…";
  if (etype === "harvest_question_bank") {
    if (ev.status === "generating") {
      return `Asking AI to invent ${ev.count || 0} schema-grounded questions… (this backend call can take a while)`;
    }
    if (ev.status === "generated") {
      return `AI proposed ${ev.questions || 0} questions; preparing backend generation…`;
    }
  }
  if (etype === "harvest_followup") {
    const q = (ev.question || "").trim();
    const tail = q ? `: ${q.slice(0, 60)}` : "";
    return `Backend follow-up thread ${ev.done || 0}/${ev.total || 0} [${ev.category || ""}]${tail}…`;
  }
  if (etype === "harvest_generate") {
    if (ev.status === "planned") {
      return `Prepared ${ev.total || 0} backend question(s); generating SQL with ${ev.workers || 1} worker(s)…`;
    }
    const q = (ev.question || "").trim();
    const tail = q ? ` — ${q.slice(0, 60)}` : "";
    return `Generating SQL with backend ${ev.done || 0}/${ev.total || 0} (kept ${ev.kept || 0})${tail}…`;
  }
  if (etype === "harvest_collected") {
    return `Validated ${ev.pairs || 0} total pairs; finalizing training…`;
  }
  if (etype === "harvest_stopped" || etype === "stopped") {
    return "Stopping gracefully — keeping the trained model…";
  }
  return null;
}

function disconnectLlmJobEvents() {
  if (llmJobState.eventSource) {
    llmJobState.eventSource.close();
    llmJobState.eventSource = null;
  }
  if (llmJobState.pollTimer) {
    clearInterval(llmJobState.pollTimer);
    llmJobState.pollTimer = null;
  }
}

function finishLlmJob(result, statusEl, onDone) {
  disconnectLlmJobEvents();
  const jobId = llmJobState.jobId;
  llmJobState.jobId = null;
  llmJobState.eventCursor = 0;
  if (typeof onDone === "function") onDone(result || {}, jobId);
}

function connectLlmJobEvents(jobId, statusEl, onDone) {
  disconnectLlmJobEvents();
  llmJobState.jobId = jobId;
  llmJobState.eventCursor = 0;
  const handle = (ev) => {
    const msg = llmProgressMessage(ev);
    if (msg) setStatus(statusEl, msg, "ok");
  };
  if (typeof EventSource !== "undefined") {
    const es = new EventSource(`/api/ai/llm/jobs/${jobId}/events?cursor=0`);
    llmJobState.eventSource = es;
    es.onmessage = (msg) => {
      try {
        const ev = JSON.parse(msg.data);
        if (ev.type === "job_done") {
          finishLlmJob(ev.result || {}, statusEl, onDone);
          es.close();
          return;
        }
        handle(ev);
      } catch (_) { /* ignore */ }
    };
    es.onerror = () => {
      es.close();
      startLlmJobPolling(jobId, statusEl, onDone, handle);
    };
  } else {
    startLlmJobPolling(jobId, statusEl, onDone, handle);
  }
}

function startLlmJobPolling(jobId, statusEl, onDone, handle) {
  llmJobState.pollTimer = setInterval(async () => {
    try {
      const r = await api.get(
        `/api/ai/llm/jobs/${jobId}/events/poll?cursor=${llmJobState.eventCursor}`,
      );
      for (const ev of r.events || []) {
        llmJobState.eventCursor = (ev.seq ?? llmJobState.eventCursor) + 1;
        handle(ev);
      }
      if (["finished", "stopped", "error"].includes(r.status)) {
        finishLlmJob(r.result || {}, statusEl, onDone);
      }
    } catch (_) { /* keep polling */ }
  }, 1000);
}

async function startLlmJob(kind, body, statusEl, onDone) {
  setStatus(statusEl, kind === "harvest" ? "Auto-harvesting & training…" : "Training…", "ok");
  const r = await api.post("/api/ai/llm/jobs", { kind, ...(body || {}) });
  if (!r.ok || !r.job_id) throw new Error(r.error || "Failed to start LLM job");
  connectLlmJobEvents(r.job_id, statusEl, onDone);
  return r.job_id;
}

async function stopLlmJob(statusEl) {
  if (!llmJobState.jobId) return;
  setStatus(statusEl, "Stop requested — finishing the current step, then saving the model…", "ok");
  try {
    await api.post(`/api/ai/llm/jobs/${llmJobState.jobId}/stop`, {});
  } catch (e) {
    setStatus(statusEl, "Stop failed: " + e.message, "bad");
  }
}

$("#ai-train-llm").addEventListener("click", async () => {
  let engines = [];
  try { const e = await api.get("/api/ai/llm/engines"); engines = e.engines || []; }
  catch (e) { setStatus("ai-status", "LLM: " + e.message, "bad"); return; }
  let models = [];
  try { const m = await api.get("/api/ai/llm/models"); models = (m.models || []).map((x) => x.name).filter(Boolean); }
  catch (e) { models = []; }
  const engOpts = [["", "(config default)"]].concat(
    engines.map((e) => [e.name, `${e.name}${e.available ? "" : " (unavailable)"}`]));
  const conns = Array.from(document.querySelectorAll("#ai-conn option")).map((o) => o.value).filter(Boolean);
  openFormModal("Local LLM — train your own NL→SQL model", [
    { name: "name", label: "Model (pick existing or type a new name)", type: "combo",
      value: models[0] || "default", options: models },
    { name: "engine", label: "Engine", type: "select", value: "", options: engOpts },
    { name: "action", label: "Action", type: "select", value: "rich_train",
      options: [["rich_train", "Rich DB train"], ["train_multi", "Train multi-connection (parallel)"], ["harvest", "Auto-harvest & train"], ["enrich", "Enrich templates"], ["mine", "Preview mined DB queries"], ["train", "Simple train / retrain"], ["status", "Show status"], ["generate", "Generate SQL"], ["eval", "Evaluate model"], ["verify", "Verify training data (is it in the model?)"], ["export", "Export dataset (download)"], ["versions", "Versions"], ["restore", "Restore version"], ["schedule_start", "Schedule training"], ["schedule_stop", "Discard scheduled training"], ["schedule_status", "Schedule status"]] },
    { name: "rag_connection", label: "Train/export with RAG examples from connection", type: "select",
      value: "", options: [["", "(none)"]].concat(conns.map((c) => [c, c])) },
    { name: "multi_connections", label: "Multi-connection train: connections (comma-separated)", value: "" },
    { name: "include_sample", label: "Include built-in sample pairs (export)", type: "checkbox", value: true },
    { name: "index_rag", label: "Index RAG first", type: "checkbox", value: false },
    { name: "mine_db", label: "Mine DB queries (real data, validated)", type: "checkbox", value: true },
    { name: "sample_limit", label: "Sample rows", value: "5" },
    { name: "train_mode", label: "Training mode", type: "select", value: "full",
      options: [["full", "Full retrain"], ["incremental", "Incremental"]] },
    { name: "gen_workers", label: "Backend workers (harvest)", value: "4" },
    { name: "gen_timeout", label: "Per-question timeout (s)", value: "120" },
    { name: "question", label: "Question (for generate)", value: "", placeholder: "count the number of orders" },
    { name: "version", label: "Version id (for restore)", value: "", placeholder: "version id from Versions" },
  ], async (v) => {
    let r;
    if (v.action === "harvest") {
      if (!v.rag_connection) throw new Error("Select a connection to harvest from.");
      await startLlmJob("harvest", {
        connection: v.rag_connection,
        train_new_name: v.name || "default",
        train_engine: v.engine || "",
        generated_questions: 40,
        use_rag: !!v.rag_connection,
        sample_limit: parseInt(v.sample_limit || "5", 10) || 5,
        do_train: true,
        train_mode: v.train_mode || "full",
        gen_workers: parseInt(v.gen_workers || "4", 10) || 4,
        gen_timeout: parseInt(v.gen_timeout || "120", 10) || 120,
      }, "ai-status", (rr) => {
        setStatus("ai-status", `Harvested ${rr.pairs || 0} validated pairs.`, "ok");
        $("#ai-settings-out").textContent = JSON.stringify(rr, null, 2);
      });
      return;
    } else if (v.action === "enrich") {
      r = await api.post("/api/ai/llm/enrich-templates", {
        connections: v.rag_connection ? [v.rag_connection] : [],
        engine: v.engine || "",
        names: [v.name || "default"],
      });
      if (r.ok === false) throw new Error(r.error || "Enrich failed.");
      setStatus("ai-status", `Enriched templates: ${r.enriched ?? r.count ?? 0} item(s).`, "ok");
    } else if (v.action === "versions") {
      r = await api.get("/api/ai/llm/versions?name=" + encodeURIComponent(v.name || "default"));
      const vs = r.versions || [];
      setStatus("ai-status", vs.length ? `${vs.length} version(s) — see details below.` : "No saved versions.", "ok");
    } else if (v.action === "restore") {
      if (!v.version) throw new Error("Enter a version id to restore.");
      r = await api.post("/api/ai/llm/restore", { name: v.name || "default", version: v.version });
      if (r.ok === false) throw new Error(r.error || "Restore failed.");
      setStatus("ai-status", `Restored '${v.name}' to version ${v.version}.`, "ok");
    } else if (v.action === "schedule_start") {
      r = await api.post("/api/ai/llm/harvest/schedule/start", {});
      setStatus("ai-status", r.message || "Scheduled training enabled.", r.ok === false ? "bad" : "ok");
    } else if (v.action === "schedule_stop") {
      r = await api.post("/api/ai/llm/harvest/schedule/stop", {});
      setStatus("ai-status", r.message || "Scheduled training disabled.", r.ok === false ? "bad" : "ok");
    } else if (v.action === "schedule_status") {
      r = await api.get("/api/ai/llm/harvest/schedule");
      setStatus("ai-status", r.message || `Schedule enabled=${r.enabled} next=${r.next_run || ""}`, "ok");
    } else if (v.action === "status") {
      r = await api.get("/api/ai/llm/status?name=" + encodeURIComponent(v.name || "default"));
      setStatus("ai-status", r.trained ? `Model '${r.name}' (${r.engine}).` : `Model '${v.name}' not trained.`, "ok");
    } else if (v.action === "generate") {
      if (!v.question) throw new Error("Enter a question to generate.");
      r = await api.post("/api/ai/llm/generate", {
        question: v.question, name: v.name || "default", engine: v.engine || "",
        connection: v.rag_connection || "",
      });
      const valid = r.valid ? "valid SQL" : (r.reason ? `invalid (${r.reason})` : "invalid SQL");
      setStatus("ai-status", `Generated SQL — ${valid}.`, r.valid ? "ok" : "bad");
    } else if (v.action === "verify") {
      const qs = new URLSearchParams({ name: v.name || "default", query: v.question || "" });
      r = await api.get("/api/ai/llm/model-dataset?" + qs.toString());
      if (!r.available) {
        setStatus("ai-status", r.reason || "No saved training data for this model.", "warn");
      } else if (v.question) {
        setStatus("ai-status", r.matched
          ? `"${v.question}" IS in '${v.name}': ${r.shown} match(es) of ${r.total} pairs.`
          : `"${v.question}" is NOT in '${v.name}' (${r.total} pairs trained).`,
          r.matched ? "ok" : "bad");
      } else {
        setStatus("ai-status", `Model '${v.name}' trained on ${r.total} pair(s).`, "ok");
      }
    } else if (v.action === "eval") {
      r = await api.post("/api/ai/llm/eval", {
        name: v.name || "default",
        connection: v.rag_connection || "",
        include_sample: v.include_sample !== false,
        rag_connection: v.rag_connection || "",
      });
      if (r.ok === false) throw new Error(r.error || "Evaluation failed.");
      setStatus("ai-status",
        `Eval (${r.mode || "eval"}, n=${r.count || 0}): parse=${r.parse_ok_rate} `
        + `exec=${r.executable_rate} match=${r.normalized_match_rate} `
        + `EX=${r.execution_exact_rate} soft_f1=${r.soft_f1_avg}`, "ok");
    } else if (v.action === "export") {
      r = await api.post("/api/ai/llm/dataset", { include_sample: v.include_sample !== false, rag_connection: v.rag_connection || "" });
      if (!r.count) throw new Error("Dataset is empty — enable sample pairs or pick a RAG connection.");
      download("nl2sql_dataset.jsonl", r.content || "", "application/x-ndjson");
      setStatus("ai-status", `Exported ${r.count} NL→SQL pairs (downloaded).`, "ok");
    } else if (v.action === "mine") {
      r = await api.post("/api/ai/llm/mine-training-pairs", {
        connections: v.rag_connection ? [v.rag_connection] : [],
        train_sample_limit: parseInt(v.sample_limit || "5", 10) || 5,
      });
      const s = r.stats || {};
      setStatus("ai-status", `Mined ${s.kept || 0} validated pairs.`, "ok");
    } else if (v.action === "train_multi") {
      const connList = String(v.multi_connections || "")
        .split(",").map((c) => c.trim()).filter(Boolean);
      if (!connList.length) throw new Error("Enter one or more connection names (comma-separated).");
      r = await api.post("/api/ai/llm/train-multi", {
        connections: connList,
        train_new_name: v.name || "default",
        train_engine: v.engine || "",
        gen_workers: parseInt(v.gen_workers || "4", 10) || 4,
        train_sample_limit: parseInt(v.sample_limit || "5", 10) || 5,
      });
      if (r.ok === false) throw new Error(r.error || r.reason || "Multi-connection training failed.");
      const okN = (r.models || []).filter((m) => m.ok).length;
      setStatus("ai-status",
        `Trained ${okN} model(s) from ${(r.connections || []).length} connection(s).`, "ok");
    } else if (v.action === "rich_train") {
      await startLlmJob("train", {
        mode: "from_database",
        connections: v.rag_connection ? [v.rag_connection] : [],
        train_new_name: v.name || "default",
        train_engine: v.engine || "",
        include_sample: v.include_sample !== false,
        use_rag: !!v.rag_connection,
        index_rag: !!v.index_rag,
        rag_strategy: "index_first",
        mine_db: v.mine_db !== false,
        train_sample_limit: parseInt(v.sample_limit || "5", 10) || 5,
        train_mode: v.train_mode || "full",
        gen_workers: parseInt(v.gen_workers || "4", 10) || 4,
        gen_timeout: parseInt(v.gen_timeout || "120", 10) || 120,
      }, "ai-status", (r) => {
        setStatus("ai-status",
          `Trained ${((r.models || []).length)} model(s) on ${r.pairs} pairs; ` +
          `already=${r.already_trained || 0} new=${r.new_pairs || 0}`, "ok");
        const ev = (r.models && r.models[0] && r.models[0].eval) || r.eval;
        if (ev) {
          setStatus("ai-status", `Trained — parse=${ev.parse_ok_rate} exec=${ev.executable_rate} EX=${ev.execution_exact_rate}`, "ok");
        }
        $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
      });
      return;
    } else {
      r = await api.post("/api/ai/llm/train", { name: v.name || "default", engine: v.engine || "", rag_connection: v.rag_connection || "" });
      setStatus("ai-status", `Trained '${r.name}' (${r.engine}) loss=${r.final_loss}.`, "ok");
    }
    $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
  }, "Run");
});
// Session training config for the AI Query Assistant — the chosen model +
// engine persist for the page session and are reused for current Q→SQL and
// chat/follow-up training. RAG use follows the "Use RAG" toggle.
const aiqaTrain = { model: "", engine: "" };
$("#ai-train-current")?.addEventListener("click", async () => {
  let engines = [];
  try { const e = await api.get("/api/ai/llm/engines"); engines = e.engines || []; }
  catch (e) { setStatus("ai-status", "LLM: " + e.message, "bad"); return; }
  let models = [];
  try { const m = await api.get("/api/ai/llm/models"); models = (m.models || []).map((x) => x.name).filter(Boolean); }
  catch (e) { models = []; }
  const curModel = aiqaTrain.model || models[0] || "default";
  const useRag = !!$("#ai-use-rag")?.checked;
  const engOpts = [["", "(config default)"]].concat(
    engines.map((e) => [e.name, `${e.name}${e.available ? "" : " (unavailable)"}`]));
  const modelList = models.map((m) => `<option value="${esc(m)}"></option>`).join("");
  const engSel = engOpts.map(([v, l]) =>
    `<option value="${esc(v)}"${v === (aiqaTrain.engine || "") ? " selected" : ""}>${esc(l)}</option>`).join("");
  openModal("Train LLM — AI Query Assistant", `
    <p class="hint">Pick the target model + engine. They persist for this AI Query session —
    reused for current Q→SQL and chat/follow-up training. Use RAG: ${useRag ? "on" : "off"} (toolbar toggle).</p>
    <label class="form-row">Target model (pick existing or type a new name)
      <input id="aiqa-model" list="aiqa-model-list" value="${esc(curModel)}" placeholder="default"/>
      <datalist id="aiqa-model-list">${modelList}</datalist></label>
    <label class="form-row">Engine<select id="aiqa-engine">${engSel}</select></label>
    <label class="form-row">AI-generated questions (auto-harvest)
      <input id="aiqa-harvest-q" type="number" value="40" min="0" max="500" size="5"/></label>
    <label class="form-row">Training mode
      <select id="aiqa-train-mode"><option value="full">Full retrain</option>
      <option value="incremental">Incremental</option></select></label>
    <label class="form-row">Backend workers / timeout (s)
      <input id="aiqa-gen-workers" type="number" value="4" min="1" max="32" size="4"/>
      <input id="aiqa-gen-timeout" type="number" value="120" min="10" max="600" size="5"/></label>
    <div class="row">
      <button id="aiqa-train-current" class="primary">Train on current Q→SQL</button>
      <button id="aiqa-train-chat">Train from chat (incl. follow-ups)</button>
      <button id="aiqa-harvest">Auto-harvest &amp; train</button>
      <button id="aiqa-harvest-stop" disabled>Stop harvest</button>
      <button id="aiqa-close">Close</button>
    </div>
    <div class="status" id="aiqa-status"></div>`);
  const saveCfg = () => {
    aiqaTrain.model = ($("#aiqa-model")?.value || "").trim() || "default";
    aiqaTrain.engine = $("#aiqa-engine")?.value || "";
  };
  // Use the AI's explanation as the pair description so chat/follow-up turns
  // train from the explanation too (it travels into RAG); fall back otherwise.
  const aiqaPairDescription = (fallback) => {
    const exp = ($("#ai-explanation")?.textContent || "").trim();
    if (exp && !exp.toLowerCase().startsWith("(no explanation")) return exp;
    return fallback;
  };
  $("#aiqa-close")?.addEventListener("click", closeModal);
  $("#aiqa-train-current")?.addEventListener("click", async () => {
    saveCfg();
    const question = ($("#ai-question")?.value || "").trim();
    const sql = ($("#ai-sql")?.value || "").trim();
    const connection = ($("#ai-conn")?.value || "").trim();
    if (!question || !sql) { setStatus("aiqa-status", "Generate SQL first, then train on it.", "bad"); return; }
    try {
      const r = await api.post("/api/ai/llm/train-pairs", {
        names: [aiqaTrain.model], engine: aiqaTrain.engine, connection,
        pairs: [{ question, sql, description: aiqaPairDescription("AI Query current turn") }], use_rag: useRag,
      });
      setStatus("aiqa-status", `Trained '${aiqaTrain.model}' from current Q→SQL (${r.pairs || 0} pair(s)).`, "ok");
      $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
    } catch (e) { setStatus("aiqa-status", "LLM: " + e.message, "bad"); }
  });
  $("#aiqa-train-chat")?.addEventListener("click", async () => {
    saveCfg();
    const connection = ($("#ai-conn")?.value || "").trim();
    if (!connection) { setStatus("aiqa-status", "Select a connection first.", "bad"); return; }
    const question = ($("#ai-question")?.value || "").trim();
    const sql = ($("#ai-sql")?.value || "").trim();
    const body = {
      mode: "from_database", connections: [connection],
      train_new_name: aiqaTrain.model, train_engine: aiqaTrain.engine,
      mine_db: false, use_rag: useRag, include_sample: false,
      train_mode: $("#aiqa-train-mode")?.value || "full",
    };
    if (question && sql) {
      body.extra_pairs = [{
        question, sql, description: aiqaPairDescription("AI Query current Generated SQL"),
      }];
    }
    const startBtn = $("#aiqa-train-chat");
    if (startBtn) startBtn.disabled = true;
    try {
      await startLlmJob("train", body, "aiqa-status", (r) => {
        setStatus("aiqa-status",
          `Trained '${aiqaTrain.model}' from chat/follow-ups (${r.pairs || 0} pair(s)); ` +
          `already=${r.already_trained || 0} new=${r.new_pairs || 0}`, "ok");
        $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
      });
    } catch (e) { setStatus("aiqa-status", "LLM: " + e.message, "bad"); }
    finally { if (startBtn) startBtn.disabled = false; }
  });
  $("#aiqa-harvest")?.addEventListener("click", async () => {
    saveCfg();
    const connection = ($("#ai-conn")?.value || "").trim();
    if (!connection) { setStatus("aiqa-status", "Select a connection first.", "bad"); return; }
    const qcount = parseInt($("#aiqa-harvest-q")?.value || "0", 10) || 0;
    const startBtn = $("#aiqa-harvest");
    const stopBtn = $("#aiqa-harvest-stop");
    if (startBtn) startBtn.disabled = true;
    if (stopBtn) stopBtn.disabled = false;
    try {
      await startLlmJob("harvest", {
        connection, train_new_name: aiqaTrain.model, train_engine: aiqaTrain.engine,
        generated_questions: qcount, use_rag: useRag, do_train: true,
        train_mode: $("#aiqa-train-mode")?.value || "full",
        gen_workers: parseInt($("#aiqa-gen-workers")?.value || "4", 10) || 4,
        gen_timeout: parseInt($("#aiqa-gen-timeout")?.value || "120", 10) || 120,
      }, "aiqa-status", (r) => {
        const srcs = Object.entries(r.sources || {}).map(([k, v]) => `${k}=${v}`).join(", ");
        setStatus("aiqa-status",
          (r.stopped ? "Stopped — " : "") +
          `Harvested ${r.pairs || 0} validated pairs ` +
          `(offline ${r.offline_pairs || 0}, backend ${r.backend_pairs || 0}, ` +
          `skipped-known ${r.skipped_known || 0}, ` +
          `already=${r.already_trained || 0} new=${r.new_pairs || 0}, ` +
          `rejected ${r.rejected || 0}); ` +
          (r.trained ? `trained ${(r.models || []).map((m) => m.name).join(", ")}` : "not trained"),
          "ok");
        $("#ai-settings-out").textContent = JSON.stringify({ sources: r.sources, ...r }, null, 2) + "\n" + srcs;
        if (startBtn) startBtn.disabled = false;
        if (stopBtn) stopBtn.disabled = true;
      });
    } catch (e) {
      setStatus("aiqa-status", "LLM: " + e.message, "bad");
      if (startBtn) startBtn.disabled = false;
      if (stopBtn) stopBtn.disabled = true;
    }
  });
  $("#aiqa-harvest-stop")?.addEventListener("click", async () => {
    $("#aiqa-harvest-stop").disabled = true;
    await stopLlmJob("aiqa-status");
  });
});
$("#ai-build-app")?.addEventListener("click", () => activateTab("app-builder"));
// Legacy modal removed — full App Builder panel in app_builder_ui.js
$("#ai-schema-clear").addEventListener("click", async () => {
  try { $("#ai-settings-out").textContent = JSON.stringify(await api.call("DELETE", "/api/ai/cache"), null, 2); setStatus("ai-status", "Schema cache cleared.", "ok"); }
  catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-schema-show").addEventListener("click", async () => {
  const conn = $("#ai-conn").value || "";
  try {
    const r = await api.get("/api/ai/cache/show" + (conn ? "?connection=" + encodeURIComponent(conn) : ""));
    $("#ai-settings-out").textContent = typeof r === "string" ? r : JSON.stringify(r, null, 2);
    setStatus("ai-status", "Schema sent to AI shown.", "ok");
  } catch (e) { setStatus("ai-status", e.message, "bad"); }
});
$("#ai-auto-exec").addEventListener("change", () => setStatus("ai-status", $("#ai-auto-exec").checked ? "Auto-execute SQL enabled." : "Auto-execute SQL disabled.", "ok"));
$("#ai-sql-mode").addEventListener("change", () => setStatus("ai-status", "SQL mode: " + $("#ai-sql-mode").selectedOptions[0].text, "ok"));

// -- AI settings -------------------------------------------------------------
$("#ai-set-backend-btn").addEventListener("click", async () => {
  try {
    const r = await api.call("PUT", "/api/ai/backend", { backend: $("#ai-set-backend").value, verify: $("#ai-verify").checked });
    $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
    loadAiBackends();
  } catch (e) { $("#ai-settings-out").textContent = e.message; }
});
$("#ai-pii-btn").addEventListener("click", async () => {
  try {
    const r = await api.call("PUT", "/api/ai/pii", { enabled: $("#ai-pii").checked });
    $("#ai-settings-out").textContent = JSON.stringify(r, null, 2);
  } catch (e) { $("#ai-settings-out").textContent = e.message; }
});
$("#ai-cache-info").addEventListener("click", async () => {
  try { $("#ai-settings-out").textContent = JSON.stringify(await api.get("/api/ai/cache"), null, 2); }
  catch (e) { $("#ai-settings-out").textContent = e.message; }
});
$("#ai-cache-clear").addEventListener("click", async () => {
  try { $("#ai-settings-out").textContent = JSON.stringify(await api.call("DELETE", "/api/ai/cache"), null, 2); }
  catch (e) { $("#ai-settings-out").textContent = e.message; }
});

// -- AI history --------------------------------------------------------------
function getAiHistory() {
  try { return JSON.parse(localStorage.getItem("dbtool_ai_history") || "[]"); } catch (_) { return []; }
}
function addAiHistory(q) {
  let h = getAiHistory().filter((x) => x !== q);
  h.unshift(q); h = h.slice(0, 50);
  localStorage.setItem("dbtool_ai_history", JSON.stringify(h));
  renderAiHistory();
}
function renderAiHistory() {
  const ul = $("#ai-history"); if (!ul) return;
  ul.innerHTML = getAiHistory().map((s) => `<li><code>${esc(s)}</code></li>`).join("");
  $$("#ai-history li").forEach((li, i) => li.addEventListener("click", () => { $("#ai-question").value = getAiHistory()[i]; }));
}
$("#ai-history-clear").addEventListener("click", () => { localStorage.removeItem("dbtool_ai_history"); renderAiHistory(); });

// ---- Monitoring ------------------------------------------------------------
function flattenSections(sections) {
  // sections shape: [ [name, [[metric, value], ...]], ... ]
  const rows = [];
  if (Array.isArray(sections)) {
    sections.forEach((sec) => {
      if (Array.isArray(sec) && sec.length === 2 && Array.isArray(sec[1])) {
        const name = sec[0];
        sec[1].forEach((pair) => {
          if (Array.isArray(pair)) rows.push([name, pair[0], pair[1]]);
        });
      } else if (sec && typeof sec === "object") {
        const name = sec.title || sec.name || "";
        const m = sec.metrics || sec.values || {};
        Object.keys(m).forEach((k) => rows.push([name, k, m[k]]));
      }
    });
  } else if (sections && typeof sections === "object") {
    Object.keys(sections).forEach((name) => {
      const m = sections[name];
      if (m && typeof m === "object") Object.keys(m).forEach((k) => rows.push([name, k, m[k]]));
    });
  }
  return rows;
}

function renderAlerts(alerts) {
  const rows = (alerts || []).map((a) => [a.timestamp, a.severity, a.source, (a.message || "").slice(0, 100)]);
  fillTable($("#mon-alerts-grid"), ["Time", "Severity", "Source", "Message"], rows);
}

let monAutoTimer = null;
function monLocalAction(msg) { setStatus("mon-status", msg, "ok"); }
$("#mon-settings").addEventListener("click", async () => {
  try {
    const r = await api.get("/api/monitor/config");
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("mon-status", "Monitor settings loaded.", "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});
$("#mon-threshold-settings").addEventListener("click", () => { $("#mon-thresholds-sec").open = true; $("#mon-thr-load").click(); });

// Notification-channel config (Teams + SMTP email), mirroring the Tk Monitor
// Settings notifications group. Reads /api/monitor/notifications, writes config
// keys via POST and secrets via POST .../secret. Secrets are write-only: blank
// leaves them unchanged; values shown as "configured/not set".
$("#mon-notifications").addEventListener("click", async () => {
  let cfg = {};
  try { cfg = await api.get("/api/monitor/notifications"); }
  catch (e) { return setStatus("mon-status", e.message, "bad"); }
  const teamsSet = cfg.teams_webhook_url_set ? " (configured)" : " (not set)";
  const smtpPwSet = cfg.smtp_password_set ? " (configured)" : " (not set)";
  openFormModal("Notification settings", [
    { name: "enabled", label: "Enable alert notifications", type: "checkbox", value: !!cfg.enabled },
    { name: "min_severity", label: "Minimum severity", type: "select",
      value: cfg.min_severity || "WARNING", options: ["INFO", "WARNING", "CRITICAL"] },
    { name: "teams_enabled", label: "Send to Microsoft Teams", type: "checkbox", value: !!cfg.teams_enabled },
    { name: "teams_webhook_url", label: `Teams webhook URL${teamsSet}`, type: "password", value: "" },
    { name: "email_enabled", label: "Send email alerts", type: "checkbox", value: !!cfg.email_enabled },
    { name: "smtp_host", label: "SMTP host", value: cfg.smtp_host || "" },
    { name: "smtp_port", label: "SMTP port", value: String(cfg.smtp_port || 587) },
    { name: "smtp_use_tls", label: "Use STARTTLS", type: "checkbox", value: cfg.smtp_use_tls !== false },
    { name: "smtp_username", label: "SMTP username", value: cfg.smtp_username || "" },
    { name: "smtp_password", label: `SMTP password${smtpPwSet}`, type: "password", value: "" },
    { name: "email_from", label: "From address", value: cfg.email_from || "" },
    { name: "email_to", label: "Recipient(s), comma-separated", value: cfg.email_to || "" },
  ], async (v) => {
    const secretKeys = new Set(["teams_webhook_url", "smtp_password"]);
    const boolKeys = new Set(["enabled", "teams_enabled", "email_enabled", "smtp_use_tls"]);
    // Config keys first (skip secrets), then secrets only when a value was typed.
    for (const [key, raw] of Object.entries(v)) {
      if (secretKeys.has(key)) continue;
      const value = boolKeys.has(key) ? (raw ? "true" : "false") : String(raw);
      await api.post("/api/monitor/notifications", { key, value });
    }
    for (const key of secretKeys) {
      if (v[key]) await api.post("/api/monitor/notifications/secret", { key, value: v[key] });
    }
    setStatus("mon-status", "Notification settings saved.", "ok");
  }, "Save notifications");
});

// Build a modal form (mirrors Tk Toplevel add-connection dialogs) and wire its
// submit button to `onSubmit(values)`. Fields: [{name,label,type,value,options}].
function openFormModal(title, fields, onSubmit, submitLabel) {
  const body = fields.map((f) => {
    if (f.type === "select") {
      const opts = (f.options || []).map((o) => {
        const val = Array.isArray(o) ? o[0] : o;
        const lbl = Array.isArray(o) ? o[1] : o;
        return `<option value="${esc(val)}"${val === f.value ? " selected" : ""}>${esc(lbl)}</option>`;
      }).join("");
      return `<label class="form-row">${esc(f.label)}<select data-field="${esc(f.name)}">${opts}</select></label>`;
    }
    if (f.type === "checkbox") {
      return `<label class="checkbox form-row"><input type="checkbox" data-field="${esc(f.name)}"${f.value ? " checked" : ""}/> ${esc(f.label)}</label>`;
    }
    if (f.type === "combo") {
      // Free-text input with autocomplete suggestions (pick existing or type new).
      const listId = `combo-${f.name}`;
      const opts = (f.options || []).map((o) => {
        const val = Array.isArray(o) ? o[0] : o;
        return `<option value="${esc(val)}"></option>`;
      }).join("");
      return `<label class="form-row">${esc(f.label)}<input type="text" list="${esc(listId)}" data-field="${esc(f.name)}" value="${esc(f.value || "")}" placeholder="${esc(f.placeholder || "")}"/><datalist id="${esc(listId)}">${opts}</datalist></label>`;
    }
    return `<label class="form-row">${esc(f.label)}<input type="${f.type || "text"}" data-field="${esc(f.name)}" value="${esc(f.value || "")}" placeholder="${esc(f.placeholder || "")}"/></label>`;
  }).join("");
  openModal(title, `<form id="modal-form">${body}<div class="row"><button type="submit" class="primary">${esc(submitLabel || "Save")}</button><button type="button" id="modal-cancel">Cancel</button></div><div class="status" id="modal-status"></div></form>`);
  $("#modal-cancel").addEventListener("click", closeModal);
  $("#modal-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const values = {};
    $$("#modal-form [data-field]").forEach((el) => {
      values[el.dataset.field] = el.type === "checkbox" ? el.checked : el.value.trim();
    });
    try {
      await onSubmit(values);
      closeModal();
    } catch (err) {
      setStatus("modal-status", err.message, "bad");
    }
  });
}

$("#mon-add-ssh").addEventListener("click", () => {
  openFormModal("Add Monitoring Connection (SSH)", [
    { name: "target_type", label: "Target type", type: "select", value: "vm",
      options: [["vm", "VM / host (SSH metrics)"], ["db_server", "DB server (SSH to DB host)"], ["service", "Other service (SSH)"]] },
    { name: "name", label: "Connection name", value: "" },
    { name: "host", label: "Hostname or IP", value: "localhost" },
    { name: "username", label: "SSH username", value: "" },
    { name: "password", label: "Password (optional)", type: "password", value: "" },
  ], async (v) => {
    if (!v.name || !v.host || !v.username) throw new Error("Name, host and username are required.");
    await api.post("/api/monitor/connections/saved", v);
    await populateMonTargets();
    setStatus("mon-status", `Monitor connection '${v.name}' added.`, "ok");
  }, "Add connection");
});

$("#mon-add-db").addEventListener("click", () => {
  openFormModal("Add Database (Monitoring only)", [
    { name: "name", label: "Connection name", value: "" },
    { name: "db_type", label: "Database type", type: "select", value: "MariaDB",
      options: ["MariaDB", "MySQL", "PostgreSQL", "Oracle"] },
    { name: "host", label: "Host", value: "localhost" },
    { name: "port", label: "Port", value: "" },
    { name: "database", label: "Database (non-Oracle)", value: "" },
    { name: "service", label: "Service name (Oracle)", value: "" },
    { name: "username", label: "Username", value: "" },
    { name: "password", label: "Password", type: "password", value: "" },
  ], async (v) => {
    if (!v.name || !v.db_type || !v.host) throw new Error("Name, type and host are required.");
    await api.post("/api/monitor/db-connections", v);
    await populateMonTargets();
    setStatus("mon-status", `Monitor database '${v.name}' added.`, "ok");
  }, "Add database");
});

// ---- Per-section monitoring lifecycle (parity with Tk's three lists) -------
function monSelected(cat) {
  const sel = $(`#mon-${cat}-list`);
  return sel ? sel.value : "";
}

// Select = start monitoring (add to the section's active set; many at once).
function monStartMonitoring(cat) {
  const name = monSelected(cat);
  if (!name) return setStatus("mon-status", "Highlight a saved target first.", "bad");
  if (monActive[cat].has(name)) return setStatus("mon-status", `'${name}' is already monitored.`, "ok");
  monActive[cat].add(name);
  populateMonSection(cat);
  setStatus("mon-status", `Monitoring '${name}'.`, "ok");
  refreshMonNow();
}
["server", "database", "cloud"].forEach((cat) => {
  const el = $(`#mon-${cat}-select`);
  if (el) el.addEventListener("click", () => monStartMonitoring(cat));
});

$("#mon-edit-target").addEventListener("click", async () => {
  const target = monSelected("server");
  if (!target) return setStatus("mon-status", "Select a target to edit.", "bad");
  const src = monTargetSource[target] || "db";
  if (src !== "monitor") {
    return setStatus("mon-status", "Only SSH monitor targets can be edited here. Use Add Database/Cloud or Connections for this target type.", "bad");
  }
  try {
    const current = await api.get(`/api/monitor/connections/saved/${encodeURIComponent(target)}`);
    openFormModal("Edit Monitoring Connection (SSH)", [
      { name: "target_type", label: "Target type", type: "select", value: current.target_type || "vm",
        options: [["vm", "VM / host (SSH metrics)"], ["db_server", "DB server (SSH to DB host)"], ["service", "Other service (SSH)"]] },
      { name: "host", label: "Hostname or IP", value: current.host || "" },
      { name: "username", label: "SSH username", value: current.username || current.user || "" },
      { name: "password", label: "Password (optional, blank preserves existing)", type: "password", value: "" },
    ], async (v) => {
      if (!v.host || !v.username) throw new Error("Host and username are required.");
      await api.call("PUT", `/api/monitor/connections/saved/${encodeURIComponent(target)}`, v);
      await populateMonTargets();
      setStatus("mon-status", `Monitor connection '${target}' updated.`, "ok");
    }, "Save target");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});

$("#mon-test-target").addEventListener("click", async () => {
  const target = monSelected("server");
  if (!target) return setStatus("mon-status", "Select a target to test.", "bad");
  const src = monTargetSource[target] || "db";
  setStatus("mon-status", `Testing '${target}'…`);
  try {
    let r;
    if (src === "monitor-db") {
      r = await api.post(`/api/monitor/db-connections/${encodeURIComponent(target)}/test`);
    } else if (src === "cloud") {
      r = await api.post(`/api/monitor/cloud/connections/${encodeURIComponent(target)}/test`);
    } else if (src === "monitor") {
      r = await api.get(`/api/monitor/connections/saved/${encodeURIComponent(target)}/os-metrics`);
    } else {
      r = await api.get(`/api/metrics/${encodeURIComponent(target)}`);
    }
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("mon-status", `Test complete for '${target}'.`, "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});

// Remove = stop monitoring an active target, else delete the idle saved profile
// (source-aware), matching the Tk Remove semantics.
async function monStopOrDelete(cat) {
  const name = monSelected(cat);
  if (!name) return setStatus("mon-status", "Highlight a target first.", "bad");
  if (monActive[cat].has(name)) {
    monActive[cat].delete(name);
    delete monMetricsCache[`${cat}|${name}`];
    populateMonSection(cat);
    renderMonCategory(cat);
    return setStatus("mon-status", `Stopped monitoring '${name}'.`, "ok");
  }
  if (name === MON_LOCAL_OS) { monActive.server.delete(MON_LOCAL_OS); populateMonSection("server"); renderMonCategory("server"); return; }
  if (!confirm(`Delete saved target '${name}'?`)) return;
  const src = monTargetSource[name] || "db";
  try {
    if (src === "monitor-db") await api.del(`/api/monitor/db-connections/${encodeURIComponent(name)}`);
    else if (src === "monitor") await api.del(`/api/monitor/connections/saved/${encodeURIComponent(name)}`);
    else if (src === "cloud") await api.del(`/api/monitor/cloud/connections/${encodeURIComponent(name)}`);
    else throw new Error("This is a Connections-tab DB profile; remove it from the Connections tab.");
    await loadMonSaved();
    setStatus("mon-status", `Deleted saved target '${name}'.`, "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
}
["server", "database", "cloud"].forEach((cat) => {
  const el = $(`#mon-${cat}-remove`);
  if (el) el.addEventListener("click", () => monStopOrDelete(cat));
});

$("#mon-add-cloud").addEventListener("click", async () => {
  let schema = { providers: {}, target_kinds: [], auth_modes: [] };
  try { schema = await api.get("/api/monitor/cloud/providers/schema"); } catch (_) {}
  const providers = Object.keys(schema.providers || {});
  openFormModal("Add Cloud Resource", [
    { name: "name", label: "Connection name", value: "" },
    { name: "provider", label: "Provider", type: "select",
      value: providers[0] || "AWS", options: providers.length ? providers : ["AWS", "Azure", "GCP", "Other"] },
    { name: "region", label: "Region / location", value: "" },
    { name: "auth_mode", label: "Auth mode", type: "select", value: "keys",
      options: schema.auth_modes && schema.auth_modes.length ? schema.auth_modes : ["keys", "pwd", "sso"] },
    { name: "access_key", label: "Access key / client id", value: "" },
    { name: "secret_key", label: "Secret key", type: "password", value: "" },
  ], async (v) => {
    if (!v.name || !v.provider) throw new Error("Name and provider are required.");
    const profile = { provider: v.provider, region: v.region, auth_mode: v.auth_mode };
    if (v.access_key) profile.access_key = v.access_key;
    if (v.secret_key) profile.secret_key = v.secret_key;
    await api.post("/api/monitor/cloud/connections", { name: v.name, profile });
    await populateMonTargets();
    setStatus("mon-status", `Cloud resource '${v.name}' added.`, "ok");
  }, "Add cloud resource");
});

// Local OS is a pseudo-target in the Server section's active set.
$("#mon-server-localos").addEventListener("click", () => {
  if (monActive.server.has(MON_LOCAL_OS)) {
    monActive.server.delete(MON_LOCAL_OS);
    delete monMetricsCache[`server|${MON_LOCAL_OS}`];
    monLocalAction("Stopped local OS metrics.");
  } else {
    monActive.server.add(MON_LOCAL_OS);
    monLocalAction("Monitoring local OS metrics.");
  }
  populateMonSection("server");
  refreshMonNow();
});

// Fetch one target's metrics (source-aware). Local OS uses /api/os/metrics.
async function monFetchOne(cat, name) {
  if (name === MON_LOCAL_OS) {
    const r = await api.get("/api/os/metrics");
    const m = r.metrics || {};
    return { sections: [["OS", Object.keys(m).map((k) => [k, m[k]])]], alerts: r.alerts || [] };
  }
  return api.get(`/api/metrics/${encodeURIComponent(name)}`);
}

const monViewMode = { server: "text", database: "text", cloud: "text" };
const monGraphHistory = {};

function monSparkline(values, width = 24) {
  if (!values || !values.length) return "";
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const span = hi - lo || 1;
  const chars = "▁▂▃▄▅▆▇█";
  return values.slice(-width).map((v) => chars[Math.min(chars.length - 1, Math.floor(((v - lo) / span) * (chars.length - 1)))]).join("");
}

function monUpdateGraphHistory(cat) {
  Array.from(monActive[cat]).sort().forEach((name) => {
    const res = monMetricsCache[`${cat}|${name}`] || {};
    let floats = res.raw_floats || {};
    if (!Object.keys(floats).length) {
      flattenSections(res.sections).forEach(([, metric, value]) => {
        const n = Number(value);
        if (!Number.isNaN(n)) floats[metric] = n;
      });
    }
    Object.keys(floats).forEach((metric) => {
      const key = `${cat}|${name}|${metric}`;
      const hist = monGraphHistory[key] || [];
      const n = Number(floats[metric]);
      if (!Number.isNaN(n)) {
        hist.push(n);
        monGraphHistory[key] = hist.slice(-40);
      }
    });
  });
}

function monGraphText(cat) {
  const lines = [];
  Array.from(monActive[cat]).sort().forEach((name) => {
    const keys = Object.keys(monGraphHistory).filter((k) => k.startsWith(`${cat}|${name}|`)).sort();
    if (!keys.length) return;
    lines.push(`=== ${name} ===`);
    keys.forEach((key) => {
      const metric = key.split("|")[2];
      const hist = monGraphHistory[key] || [];
      const latest = hist.length ? hist[hist.length - 1] : 0;
      lines.push(`  ${metric}: ${monSparkline(hist)} (${latest})`);
    });
    lines.push("");
  });
  return lines.join("\n").trim() || "No graph data yet — select targets and refresh.";
}

// Render one section's metrics grid from the cache (all active targets at once).
function renderMonCategory(cat) {
  const grid = $(`#mon-${cat}-grid`);
  const graphEl = $(`#mon-${cat}-graph`);
  const mode = monViewMode[cat] || "text";
  if (graphEl) graphEl.hidden = mode !== "graph";
  if (grid) grid.hidden = mode === "graph";
  if (mode === "graph") {
    monUpdateGraphHistory(cat);
    if (graphEl) graphEl.textContent = monGraphText(cat);
    return;
  }
  const active = Array.from(monActive[cat]);
  const rows = [];
  if (!active.length) {
    fillTable($(`#mon-${cat}-grid`), ["Target", "Section", "Metric", "Value"],
      [["(no targets monitored)", "", "", ""]]);
    return;
  }
  active.sort().forEach((name) => {
    const res = monMetricsCache[`${cat}|${name}`];
    if (!res) { rows.push([name, "", "(waiting…)", ""]); return; }
    if (res.__error) { rows.push([name, "", "error", String(res.__error).slice(0, 60)]); return; }
    let secRows = flattenSections(res.sections);
    if (!secRows.length && res.raw_floats) {
      secRows = Object.keys(res.raw_floats).map((k) => ["raw", k, res.raw_floats[k]]);
    }
    if (!secRows.length) { rows.push([name, "", "(no metrics)", ""]); return; }
    secRows.forEach(([sec, metric, value]) => rows.push([name, sec, metric, value]));
  });
  fillTable($(`#mon-${cat}-grid`), ["Target", "Section", "Metric", "Value"], rows);
}

// Poll every active target across all three sections concurrently.
let monRefreshing = false;
async function refreshMonNow() {
  if (monRefreshing) return;
  const work = [];
  ["server", "database", "cloud"].forEach((cat) =>
    monActive[cat].forEach((name) => work.push([cat, name])));
  if (!work.length) {
    ["server", "database", "cloud"].forEach(renderMonCategory);
    return setStatus("mon-status", "No targets monitored. Highlight a saved target and press Select.", "ok");
  }
  monRefreshing = true;
  setStatus("mon-status", `Polling ${work.length} target(s)…`);
  let alerts = [];
  try {
    await Promise.all(work.map(async ([cat, name]) => {
      try {
        const res = await monFetchOne(cat, name);
        monMetricsCache[`${cat}|${name}`] = res;
        (res.alerts || []).forEach((a) => alerts.push({ ...a, source: a.source || name }));
      } catch (e) {
        monMetricsCache[`${cat}|${name}`] = { __error: e.message };
      }
    }));
  } finally {
    monRefreshing = false;
  }
  ["server", "database", "cloud"].forEach(renderMonCategory);
  if (alerts.length) renderAlerts(alerts);
  setStatus("mon-status", `Polled ${work.length} target(s); ${alerts.length} alert(s).`, "ok");
}

["mon-refresh-all", "mon-server-refresh", "mon-database-refresh", "mon-cloud-refresh"].forEach((id) => {
  const el = $("#" + id);
  if (el) el.addEventListener("click", refreshMonNow);
});

["server", "database", "cloud"].forEach((cat) => {
  $(`#mon-${cat}-show-graphs`)?.addEventListener("click", () => {
    monViewMode[cat] = "graph";
    renderMonCategory(cat);
    setStatus("mon-status", `${cat} metrics: graph view.`, "ok");
  });
  $(`#mon-${cat}-show-text`)?.addEventListener("click", () => {
    monViewMode[cat] = "text";
    renderMonCategory(cat);
    setStatus("mon-status", `${cat} metrics: text view.`, "ok");
  });
  $(`#mon-${cat}-clear-graphs`)?.addEventListener("click", () => {
    Object.keys(monGraphHistory).filter((k) => k.startsWith(`${cat}|`)).forEach((k) => delete monGraphHistory[k]);
    if (monViewMode[cat] === "graph") renderMonCategory(cat);
    setStatus("mon-status", `${cat} graphs cleared.`, "ok");
  });
});

$("#mon-auto-refresh").addEventListener("change", () => {
  if (monAutoTimer) { clearInterval(monAutoTimer); monAutoTimer = null; }
  if ($("#mon-auto-refresh").checked) {
    monAutoTimer = setInterval(refreshMonNow, 5000);
    monLocalAction("Auto refresh enabled.");
  } else {
    monLocalAction("Auto refresh disabled.");
  }
});

$("#mon-thresholds").addEventListener("click", () => $("#mon-thr-load").click());
let monThresholdRows = [];
let monThresholdSelected = -1;
function renderThresholdRows(list) {
  monThresholdRows = list || [];
  monThresholdSelected = -1;
  const table = $("#mon-thr-grid");
  const thead = table.querySelector("thead");
  const tbody = table.querySelector("tbody");
  thead.innerHTML = "<tr><th>Source</th><th>Metric</th><th>Warning</th><th>Critical</th><th>Enabled</th></tr>";
  tbody.innerHTML = monThresholdRows.map((t, i) => `
    <tr class="selectable" data-idx="${i}">
      <td>${esc(t.source || t.api || "")}</td>
      <td>${esc(t.metric || t.name || "")}</td>
      <td>${esc(t.warning ?? t.warn ?? "")}</td>
      <td>${esc(t.critical ?? t.crit ?? "")}</td>
      <td>${t.enabled !== false ? "yes" : "no"}</td>
    </tr>`).join("");
  $$("#mon-thr-grid tbody tr").forEach((tr) => tr.addEventListener("click", () => {
    $$("#mon-thr-grid tbody tr").forEach((r) => r.classList.remove("sel"));
    tr.classList.add("sel");
    monThresholdSelected = Number(tr.dataset.idx);
  }));
}
function selectedThreshold() {
  return monThresholdRows[monThresholdSelected] || null;
}
$("#mon-thr-load").addEventListener("click", async () => {
  try {
    const src = $("#mon-thr-source").value;
    const all = $("#mon-thr-all").checked;
    const r = await api.get(`/api/thresholds?all=${all}${src ? "&source=" + encodeURIComponent(src) : ""}`);
    const list = r.thresholds || r.rules || (Array.isArray(r) ? r : []);
    renderThresholdRows(list);
    setStatus("mon-status", `${list.length} threshold rule(s).`, "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});

$("#mon-thr-edit").addEventListener("click", () => {
  const t = selectedThreshold();
  if (!t) return setStatus("mon-status", "Select a threshold rule first.", "bad");
  openFormModal(`Edit threshold: ${t.source || t.api}.${t.metric || t.name}`, [
    { name: "warning", label: "Warning", value: t.warning ?? "" },
    { name: "critical", label: "Critical", value: t.critical ?? "" },
    { name: "info", label: "Info", value: t.info ?? "" },
    { name: "operator", label: "Operator", type: "select", value: t.operator || ">=",
      options: [">=", ">", "<=", "<", "==", "!="] },
    { name: "window", label: "Window", value: t.window || "" },
    { name: "enabled", label: "Enabled", type: "checkbox", value: t.enabled !== false },
    { name: "description", label: "Description", value: t.description || "" },
  ], async (v) => {
    const body = { enabled: v.enabled };
    ["warning", "critical", "info"].forEach((k) => {
      if (v[k] !== "") body[k] = Number(v[k]);
    });
    ["operator", "window", "description"].forEach((k) => {
      if (v[k] !== "") body[k] = v[k];
    });
    if (Array.isArray(t.path) && t.path.length) body.path = t.path;
    await api.call("PATCH", `/api/thresholds/${encodeURIComponent(t.source || t.api || "")}/${encodeURIComponent(t.metric || t.name || "")}`, body);
    $("#mon-thr-load").click();
    setStatus("mon-status", "Threshold updated.", "ok");
  }, "Save threshold");
});

$("#mon-thr-check").addEventListener("click", () => {
  const t = selectedThreshold();
  if (!t) return setStatus("mon-status", "Select a threshold rule first.", "bad");
  openFormModal(`Check threshold: ${t.source || t.api}.${t.metric || t.name}`, [
    { name: "value", label: "Metric value", value: "" },
    { name: "instance", label: "Instance", value: "manual" },
  ], async (v) => {
    if (!v.value) throw new Error("Metric value is required.");
    const body = {
      source: t.source || t.api || "",
      metric: t.metric || t.name || "",
      value: Number(v.value),
      instance: v.instance || "manual",
    };
    if (Array.isArray(t.path) && t.path.length) body.path = t.path;
    const r = await api.post("/api/thresholds/check", body);
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("mon-status", `${r.count || 0} threshold alert(s).`, (r.count || 0) ? "bad" : "ok");
  }, "Check value");
});

$("#mon-alerts").addEventListener("click", async () => {
  try {
    const r = await api.get("/api/alerts?limit=50");
    renderAlerts(r.alerts);
    setStatus("mon-status", `${r.total ?? (r.alerts || []).length} alerts`, "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});

$("#mon-alerts-clear").addEventListener("click", async () => {
  try {
    await api.del("/api/alerts");
    renderAlerts([]);
    setStatus("mon-status", "Alerts cleared.", "ok");
  } catch (e) { setStatus("mon-status", e.message, "bad"); }
});

// ---- Dashboard -------------------------------------------------------------
let dashLayoutRows = [];
let dashLastSnapshot = null;
const DASH_TITLES = {
  connections: "Connections",
  monitor: "Monitoring",
  ai: "AI Query",
  schema: "Data Migration",
  sql_editor: "SQL Editor",
  objects: "Database Objects",
  settings: "Settings",
  modules: "Modules",
  core: "Core",
};
function dashPanelInfo(id, snapshot) {
  const panels = snapshot.panels || {};
  const p = Array.isArray(panels)
    ? panels.find((x) => (x.id || x.name || x.key) === id)
    : panels[id];
  if (p && typeof p === "object") return p;
  if (id === "core") return { status: snapshot.overall_status || "", detail: snapshot.overall_label || "" };
  if (id === "modules") return { status: "modules", detail: `${Object.keys(snapshot.modules || {}).length} module(s)` };
  return { status: p || "", detail: "" };
}
function dashCardBody(id, snapshot) {
  if (id === "core") {
    const c = snapshot.core || {};
    return `<div class="dash-kv">
      <span class="k">Saved connections</span><span class="v">${esc(c.saved_connections_count ?? 0)}</span>
      <span class="k">Active connections</span><span class="v">${esc(c.active_connections_count ?? 0)}</span>
      <span class="k">Active</span><span class="v">${esc((c.active_connections || []).join(", ") || "none")}</span>
    </div>`;
  }
  if (id === "modules") {
    return Object.keys(snapshot.modules || {}).map((k) => {
      const m = snapshot.modules[k] || {};
      return `<div class="dash-panel-row"><span>${esc(m.title || k)}</span><span class="badge ${m.ready ? "ok" : "warn"}">${m.ready ? "ready" : "not ready"}</span></div>`;
    }).join("") || '<span class="muted">No modules</span>';
  }
  const p = dashPanelInfo(id, snapshot);
  const detail = p.detail || p.label || p.message || p.description || "";
  return `<div class="dash-kv">
    <span class="k">Status</span><span class="v">${esc(p.status || "")}</span>
    <span class="k">Detail</span><span class="v">${esc(detail)}</span>
  </div>`;
}
function normaliseDashRows(layout, snapshot) {
  const rows = Array.isArray(layout) && layout.length ? layout : [];
  const seen = new Set();
  rows.flat().forEach((id) => { if (id) seen.add(id); });
  ["core", "modules"].forEach((id) => { if (!seen.has(id)) rows.unshift([id, null]); });
  Object.keys(snapshot.panels || {}).forEach((id) => {
    if (!seen.has(id)) rows.push([id, null]);
  });
  return rows.map((r) => [r[0] || null, r[1] || null]);
}
function renderDashboardCards(snapshot, layoutRows) {
  const grid = $("#dash-card-grid");
  if (!grid) return;
  dashLayoutRows = normaliseDashRows(layoutRows, snapshot);
  const ids = dashLayoutRows.flat().filter(Boolean);
  grid.innerHTML = ids.map((id) => {
    const p = dashPanelInfo(id, snapshot);
    const cls = p.status === "ok" || p.ready ? "ok" : (p.status === "idle" ? "warn" : "");
    return `<article class="dash-card" draggable="true" data-panel="${esc(id)}">
      <div class="dash-card-head"><span>⠿ ${esc(DASH_TITLES[id] || id)}</span><span class="badge ${cls}">${esc(p.status || "")}</span></div>
      <div class="dash-card-body">${dashCardBody(id, snapshot)}</div>
    </article>`;
  }).join("");
  $$("#dash-card-grid .dash-card").forEach((card) => {
    card.addEventListener("dragstart", (e) => e.dataTransfer.setData("text/plain", card.dataset.panel));
    card.addEventListener("dragover", (e) => { e.preventDefault(); card.classList.add("drag-over"); });
    card.addEventListener("dragleave", () => card.classList.remove("drag-over"));
    card.addEventListener("drop", (e) => {
      e.preventDefault(); card.classList.remove("drag-over");
      const from = e.dataTransfer.getData("text/plain");
      const to = card.dataset.panel;
      if (!from || !to || from === to) return;
      const flat = dashLayoutRows.flat();
      const a = flat.indexOf(from), b = flat.indexOf(to);
      if (a >= 0 && b >= 0) {
        [flat[a], flat[b]] = [flat[b], flat[a]];
        dashLayoutRows = [];
        for (let i = 0; i < flat.length; i += 2) dashLayoutRows.push([flat[i] || null, flat[i + 1] || null]);
        renderDashboardCards(dashLastSnapshot, dashLayoutRows);
      }
    });
  });
}
async function loadDashboardLayout() {
  try {
    const r = await api.get("/api/dashboard/layout");
    return r.rows || r.default_rows || [];
  } catch (_) {
    return [];
  }
}
async function loadDashboard() {
  try {
    const d = await api.get("/api/dashboard");
    dashLastSnapshot = d;
    const badge = $("#dash-status");
    badge.textContent = d.overall_label || d.overall_status || "";
    badge.className = "badge " + (d.overall_status === "idle" ? "warn" : "ok");
    renderDashboardCards(d, await loadDashboardLayout());
    const core = d.core || {};
    fillTable($("#dash-core"), ["Metric", "Value"], [
      ["Saved connections", core.saved_connections_count ?? 0],
      ["Active connections", core.active_connections_count ?? 0],
      ["Active", (core.active_connections || []).join(", ")],
    ]);
    const mods = d.modules || {};
    fillTable($("#dash-modules"), ["Module", "Installed", "Ready"],
      Object.keys(mods).map((k) => [mods[k].title || k, mods[k].installed ? "yes" : "no", mods[k].ready ? "yes" : "no"]));
    const panels = d.panels || {};
    const prows = Array.isArray(panels)
      ? panels.map((p) => [p.title || p.name || "", p.status || "", p.detail || ""])
      : Object.keys(panels).map((k) => [k, (panels[k] && panels[k].status) || "", (panels[k] && (panels[k].detail || panels[k].label)) || ""]);
    fillTable($("#dash-panels"), ["Panel", "Status", "Detail"], prows);
  } catch (e) { $("#dash-status").textContent = e.message; $("#dash-status").className = "badge warn"; }
}
$("#dash-refresh").addEventListener("click", loadDashboard);
$("#dash-save-layout").addEventListener("click", async () => {
  try {
    const r = await api.post("/api/dashboard/layout", { rows: dashLayoutRows });
    $("#dash-status").textContent = r.message || "Layout saved";
    $("#dash-status").className = "badge ok";
  } catch (e) { $("#dash-status").textContent = e.message; $("#dash-status").className = "badge warn"; }
});
$("#dash-reset-layout").addEventListener("click", async () => {
  try {
    await api.post("/api/dashboard/layout/reset");
    $("#dash-status").textContent = "Layout reset";
    $("#dash-status").className = "badge ok";
    loadDashboard();
  } catch (e) { $("#dash-status").textContent = e.message; $("#dash-status").className = "badge warn"; }
});

// ---- Settings --------------------------------------------------------------
let settingsCache = [];
let settingsWritable = true;
async function loadSettings() {
  try {
    const r = await api.get("/api/config/settings");
    settingsCache = r.settings || [];
    settingsWritable = r.writable !== false;
    renderSettings();
    setStatus("settings-status", `${r.count ?? settingsCache.length} settings${settingsWritable ? " (editable)" : " (read-only)"}.`, "ok");
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
}
function settingId(s) { return s.id || s.key || ""; }
function settingIsSecret(s) {
  // The API returns `sensitive` (+ target "secret"); older shapes used `secret`.
  return !!(s.sensitive || s.secret || s.target === "secret");
}
function settingHint(s) {
  // Inline metadata mirroring Tk: range, unit, default, restart note.
  const bits = [];
  if (s.minimum != null && s.maximum != null) bits.push(`${s.minimum}–${s.maximum}`);
  if (s.unit) bits.push(String(s.unit));
  if (s.default != null && s.default !== "") bits.push(`default ${s.default}`);
  if (s.requires_restart) bits.push("restart");
  return bits.length ? ` <span class="muted">(${esc(bits.join(", "))})</span>` : "";
}
function renderSettings() {
  const flt = $("#settings-filter").value.trim().toLowerCase();
  const grid = $("#settings-grid");
  const tbody = grid.querySelector("tbody");
  grid.querySelector("thead").innerHTML =
    "<tr><th>Group</th><th>Setting</th><th>Value</th><th>Description</th></tr>";
  const rows = settingsCache.filter((s) => !flt || JSON.stringify(s).toLowerCase().includes(flt));
  tbody.innerHTML = rows.map((s) => {
    const id = settingId(s);
    const val = s.value ?? s.current ?? "";
    const secret = settingIsSecret(s);
    const type = String(s.type || "str");
    const options = s.options || [];
    const editable = settingsWritable && !s.read_only;
    let cell;
    if (!editable) {
      cell = esc(val) + (secret ? " <span class=\"muted\">(secret)</span>" : "");
    } else if (secret) {
      // Write-only: blank keeps the stored secret unchanged (parity with Tk).
      cell = `<input type="password" class="settings-edit" data-id="${esc(id)}" data-type="secret" placeholder="(unchanged)" value="" />`;
    } else if (type === "bool") {
      const cur = String(val).toLowerCase() === "true";
      cell = `<select class="settings-edit" data-id="${esc(id)}" data-type="bool">`
        + `<option value="true"${cur ? " selected" : ""}>true</option>`
        + `<option value="false"${cur ? "" : " selected"}>false</option></select>`;
    } else if (options.length) {
      cell = `<select class="settings-edit" data-id="${esc(id)}" data-type="enum">`
        + options.map((o) => `<option value="${esc(o)}"${String(o) === String(val) ? " selected" : ""}>${esc(o)}</option>`).join("")
        + `</select>`;
    } else {
      cell = `<input class="settings-edit" data-id="${esc(id)}" data-type="${esc(type)}" value="${esc(val)}" />`;
    }
    return `<tr><td>${esc(s.group || s.section || "")}</td><td>${esc(s.label || id)}</td><td>${cell}</td><td>${esc(s.description || "")}${settingHint(s)}</td></tr>`;
  }).join("");
}
function collectSettingEdits() {
  const out = {};
  $$(".settings-edit").forEach((el) => {
    const id = el.dataset.id;
    const type = el.dataset.type || "str";
    if (type === "secret") {
      // Only send when the user typed a new value; blank = keep current.
      if (el.value) out[id] = el.value;
      return;
    }
    const orig = (settingsCache.find((s) => settingId(s) === id) || {});
    const origVal = String(orig.value ?? orig.current ?? "");
    if (el.value !== origVal) out[id] = el.value;
  });
  return out;
}
$("#settings-filter").addEventListener("input", renderSettings);
$("#settings-reload").addEventListener("click", loadSettings);
$("#settings-save").addEventListener("click", async () => {
  const values = collectSettingEdits();
  if (!Object.keys(values).length) return setStatus("settings-status", "No changes to save.", "ok");
  try {
    const r = await api.post("/api/config/settings", { values });
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("settings-status", r.message || `${(r.saved || []).length} setting(s) saved.`, r.ok ? "ok" : "bad");
    await loadSettings();
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});
$("#settings-restore-defaults").addEventListener("click", async () => {
  if (!confirm("Restore config & properties to their default values?")) return;
  try {
    const r = await api.post("/api/config/settings/restore", { target: "all" });
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("settings-status", r.message || "Defaults restored.", r.ok ? "ok" : "bad");
    await loadSettings();
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});
$("#settings-clear-cache").addEventListener("click", async () => {
  try {
    const r = await api.post("/api/app/clear-caches");
    $("#settings-out").textContent = JSON.stringify(r, null, 2);
    setStatus("settings-status", r.summary || "Caches cleared.", "ok");
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});
$("#settings-shortcuts").addEventListener("click", async () => {
  try {
    const r = await api.get("/api/app/shortcuts");
    $("#settings-out").textContent = (r.shortcuts || []).map((s) => `[${s.section}] ${s.shortcut} — ${s.action}`).join("\n");
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});
$("#settings-save-key").addEventListener("click", () => {
  api.key = $("#settings-api-key").value.trim();
  localStorage.setItem("dbtool_api_key", api.key);
  $("#api-key").value = api.key;
  refreshHealth();
  setStatus("settings-status", "API key saved.", "ok");
});

async function loadApiKeys() {
  try {
    const r = await api.get("/ui/apikeys");
    const rows = r.keys || [];
    const tbody = $("#apikey-grid tbody");
    tbody.innerHTML = rows.map((k) => `
      <tr>
        <td>${esc(k.key_id)}</td>
        <td>${esc(k.name)}</td>
        <td>${esc(k.created_at || "")}</td>
        <td>${esc(k.last_used_at || "-")}</td>
        <td>${esc(k.revoked_at || "-")}</td>
        <td>
          <button class="small apikey-action" data-action="regenerate" data-key="${esc(k.key_id)}">Regenerate</button>
          <button class="small danger apikey-action" data-action="revoke" data-key="${esc(k.key_id)}">Revoke</button>
        </td>
      </tr>`).join("");
    setStatus("settings-status", `${rows.length} API key(s).`, "ok");
  } catch (e) {
    setStatus("settings-status", e.message, "bad");
  }
}
$("#apikey-refresh").addEventListener("click", loadApiKeys);
$("#apikey-create").addEventListener("click", async () => {
  try {
    const r = await api.post("/ui/apikeys", { name: $("#apikey-name").value.trim() });
    $("#settings-out").textContent =
      "Save this token now. It will not be shown again.\n\n" + r.token;
    await loadApiKeys();
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});
$("#apikey-grid").addEventListener("click", async (ev) => {
  const btn = ev.target.closest(".apikey-action");
  if (!btn) return;
  const key = btn.dataset.key;
  const action = btn.dataset.action;
  try {
    const r = await api.post(`/ui/apikeys/${encodeURIComponent(key)}/${action}`, {});
    if (action === "regenerate" && r.token) {
      $("#settings-out").textContent =
        "Save this regenerated token now. It will not be shown again.\n\n" + r.token;
    } else {
      $("#settings-out").textContent = JSON.stringify(r, null, 2);
    }
    await loadApiKeys();
  } catch (e) { setStatus("settings-status", e.message, "bad"); }
});

// ---- API key persistence ---------------------------------------------------
const keyInput = $("#api-key");
keyInput.value = api.key;
keyInput.addEventListener("change", () => {
  api.key = keyInput.value.trim();
  localStorage.setItem("dbtool_api_key", api.key);
  refreshHealth();
});

// ---- Init ------------------------------------------------------------------
(async function init() {
  await applyUiConfig();
  await refreshHealth();
  await detectModules();
  await loadConnMetadata();
  applyRemoteSshAuth();
  bindCloudSection();
  await loadCloudSchemas();
  await loadConnections();
  populateConnSelect("#sql-conn");
  renderSqlTabs();
  await loadActiveConnections();
})();
