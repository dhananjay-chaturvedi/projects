/** App Builder Web UI — full parity with Tk (agentic jobs + SSE streaming). */
(function () {
  const FEATURES = ["list", "create", "edit", "delete"];
  const DEFAULT_SVC = new Set(["ci_cd", "document", "hosting", "database", "monitoring"]);
  const state = {
    jobId: null,
    eventSource: null,
    eventCursor: 0,
    workspace: "",
    appName: "",
    building: false,
    pollTimer: null,
  };

  function ab$(id) { return document.getElementById(id); }

  function abLog(id, text, append) {
    const el = ab$(id);
    if (!el) return;
    el.textContent = append ? (el.textContent + text) : text;
    el.scrollTop = el.scrollHeight;
  }

  function abStatus(text, append) { abLog("ab-status", text, append); }

  function abSessionLog(session, text) {
    const map = {
      builder: "ab-log-builder",
      answerer: "ab-log-answerer",
      validator: "ab-log-validator",
      system: "ab-log-builder",
    };
    abLog(map[session] || "ab-log-builder", text, true);
  }

  function setAbButtons(building) {
    state.building = building;
    ab$("ab-build").disabled = building;
    ab$("ab-auto").disabled = building;
    ab$("ab-agent").disabled = building;
    if (ab$("ab-train-build")) ab$("ab-train-build").disabled = building;
    ab$("ab-stop-build").disabled = !building;
    ab$("ab-take-control").disabled = !building;
    ab$("ab-msg-send").disabled = !state.jobId && !state.workspace;
  }

  function setWorkspace(ws, name) {
    state.workspace = ws || "";
    state.appName = name || ab$("ab-name").value.trim() || "myapp";
    const has = !!ws;
    ab$("ab-start-app").disabled = !has;
    ab$("ab-package").disabled = !has;
    ab$("ab-delete").disabled = !has;
    ab$("ab-workspace-path").textContent = ws ? `Workspace: ${ws}` : "";
    if (has) ab$("ab-msg-send").disabled = false;
  }

  function selectedTrainModels() {
    return Array.from(document.querySelectorAll("#ab-train-llm input[data-llm]:checked"))
      .map((el) => el.dataset.llm);
  }

  function collectBody(extra) {
    const body = {
      name: ab$("ab-name").value.trim() || "myapp",
      mode: ab$("ab-mode").value,
      description: ab$("ab-desc").value.trim(),
      entities: ab$("ab-entities").value.split(",").map((s) => s.trim()).filter(Boolean),
      connections: ab$("ab-conn").value ? [ab$("ab-conn").value] : [],
      codebase_path: ab$("ab-codebase").value.trim(),
      variant: ab$("ab-variant").value,
      build_profile: ab$("ab-build-profile").value,
      db_app_variant: ab$("ab-db-variant").value,
      codebase_variant: ab$("ab-cb-variant").value,
      interaction: ab$("ab-interaction").value,
      uninterrupted: ab$("ab-interaction").value === "uninterrupted",
      validation_depth: ab$("ab-validation").value,
      deploy_schema: ab$("ab-deploy").checked,
      use_ai: ab$("ab-use-ai").checked,
      mask_pii: ab$("ab-mask-pii").checked,
      train_llm: selectedTrainModels(),
      train_new_name: ab$("ab-train-new").value.trim(),
      train_engine: ab$("ab-train-engine").value,
      use_rag: ab$("ab-use-rag") ? !!ab$("ab-use-rag").checked : false,
      index_rag: ab$("ab-index-rag") ? !!ab$("ab-index-rag").checked : false,
      rag_strategy: ab$("ab-rag-strategy")?.value || "index_first",
      mine_db: ab$("ab-mine-db") ? !!ab$("ab-mine-db").checked : true,
      train_sample_limit: parseInt(ab$("ab-sample-limit")?.value || "5", 10) || 5,
      rich_train: ab$("ab-rich-train") ? !!ab$("ab-rich-train").checked : false,
      features: FEATURES.filter((f) => ab$(`ab-feat-${f}`)?.checked),
      services: [],
      run_tests: true,
      ...(extra || {}),
    };
    document.querySelectorAll("#ab-services input[data-svc]").forEach((el) => {
      if (el.checked) body.services.push(el.dataset.svc);
    });
    if (body.mode === "from_database" && !body.connections.length) {
      throw new Error("Select a connection for from_database builds.");
    }
    if (body.mode === "from_codebase" && !body.codebase_path) {
      throw new Error("Enter a codebase path for from_codebase builds.");
    }
    return body;
  }

  async function refreshRagStatus() {
    const conn = ab$("ab-conn")?.value;
    const el = ab$("ab-rag-status");
    if (!el || !conn) {
      if (el) el.textContent = "";
      return;
    }
    try {
      const r = await api.get(`/api/app-builder/rag-status?connection=${encodeURIComponent(conn)}`);
      if (r.indexed) {
        el.textContent = `RAG indexed (${r.doc_count || 0} docs)`;
      } else {
        el.textContent = "RAG not indexed";
      }
    } catch (_) {
      el.textContent = "";
    }
  }

  function handleAgentEvent(payload) {
    const session = payload.session || "builder";
    const event = payload.event || {};
    const etype = event.type || "";
    const text = event.text || "";
    if (etype === "assistant_text" && text) {
      abSessionLog(session, text);
      return;
    }
    if (etype === "session_status" && text) {
      abSessionLog(session, `[${session}] ${text}\n`);
      return;
    }
    if (etype === "baseline_ready") {
      const ws = event.detail?.workspace || text;
      if (ws) setWorkspace(ws, ab$("ab-name").value.trim());
      abSessionLog("system", "[system] baseline ready — you can Start app now.\n");
      return;
    }
    if (text) abSessionLog(session, `[${etype || session}] ${text}\n`);
  }

  function handleJobEvent(ev) {
    if (ev.type === "agent_event") {
      handleAgentEvent(ev.payload || {});
    } else if (ev.type === "round") {
      const p = ev.payload || {};
      abStatus(
        `\nround ${p.index} [${p.phase}] score=${p.score} accepted=${p.accepted} — ${p.note || ""}\n`,
        true,
      );
    } else if (ev.type && ev.type.startsWith("training_")) {
      const p = ev.payload || ev;
      abStatus(`\n[train] ${ev.type} ${JSON.stringify(p)}\n`, true);
    } else if (ev.type === "decision") {
      showDecision(ev.decision || {});
    } else if (ev.type === "job_finished" || ev.type === "job_done") {
      finishBuild(ev.result || {});
    } else if (ev.type === "stopped") {
      abStatus("\nBuild stopped by user.\n", true);
    } else if (ev.type === "error") {
      abStatus(`\nError: ${ev.text}\n`, true);
    }
  }

  function showDecision(d) {
    const box = ab$("ab-decision-box");
    if (!box || !state.jobId) return;
    box.hidden = false;
    const opts = (d.options || []).map(
      (o) => `<button type="button" class="small ab-dec-opt" data-val="${esc(o)}">${esc(o)}</button>`,
    ).join(" ");
    box.innerHTML = `
      <strong>Agent question:</strong> ${esc(d.question || "")}<br/>
      <span class="muted">${esc(d.detail || "")}</span>
      <div class="row">${opts}
        <input id="ab-dec-free" placeholder="Your answer" />
        <button type="button" id="ab-dec-send" class="primary small">Send answer</button>
        <button type="button" id="ab-dec-skip" class="small">Skip</button>
      </div>`;
    box.querySelector("#ab-dec-send")?.addEventListener("click", () => {
      const v = box.querySelector("#ab-dec-free")?.value.trim() || "skip";
      answerDecision(v);
    });
    box.querySelector("#ab-dec-skip")?.addEventListener("click", () => answerDecision("skip"));
    box.querySelectorAll(".ab-dec-opt").forEach((btn) => {
      btn.addEventListener("click", () => answerDecision(btn.dataset.val));
    });
  }

  async function answerDecision(value) {
    if (!state.jobId) return;
    try {
      await api.post(`/api/app-builder/jobs/${state.jobId}/answer`, { value });
      ab$("ab-decision-box").hidden = true;
    } catch (e) {
      abStatus(`\nDecision error: ${e.message}\n`, true);
    }
  }

  function finishBuild(result) {
    setAbButtons(false);
    disconnectEvents();
    if (result && result.workspace) setWorkspace(result.workspace, collectBody().name);
    const ok = result?.ok;
    abStatus(
      `\n${ok ? "READY" : "INCOMPLETE"} score=${result?.score ?? "-"} ` +
      `agentic=${result?.agentic} aborted=${result?.aborted}\n`,
      true,
    );
    if (result?.quality) {
      for (const [k, v] of Object.entries(result.quality)) {
        if (typeof v === "object" && v.score !== undefined) {
          abStatus(`  meter ${k}: ${v.score}\n`, true);
        }
      }
    }
    ab$("ab-msg-send").disabled = false;
  }

  function disconnectEvents() {
    if (state.eventSource) {
      state.eventSource.close();
      state.eventSource = null;
    }
    if (state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  function connectEvents(jobId) {
    disconnectEvents();
    state.jobId = jobId;
    state.eventCursor = 0;
    if (typeof EventSource !== "undefined") {
      const es = new EventSource(`/api/app-builder/jobs/${jobId}/events?cursor=0`);
      state.eventSource = es;
      es.onmessage = (msg) => {
        try {
          const ev = JSON.parse(msg.data);
          if (ev.type === "job_done") {
            finishBuild(ev.result || {});
            es.close();
            return;
          }
          handleJobEvent(ev);
        } catch (_) { /* ignore parse errors */ }
      };
      es.onerror = () => {
        es.close();
        startPolling(jobId);
      };
    } else {
      startPolling(jobId);
    }
  }

  function startPolling(jobId) {
    state.pollTimer = setInterval(async () => {
      try {
        const r = await api.get(
          `/api/app-builder/jobs/${jobId}/events/poll?cursor=${state.eventCursor}`,
        );
        for (const ev of r.events || []) {
          state.eventCursor = (ev.seq ?? state.eventCursor) + 1;
          handleJobEvent(ev);
        }
        if (["finished", "stopped", "error"].includes(r.status)) {
          clearInterval(state.pollTimer);
          state.pollTimer = null;
          finishBuild(r.result || {});
        }
      } catch (_) { /* keep polling */ }
    }, 1000);
  }

  async function startJob(extra) {
    const body = collectBody(extra);
    abStatus(`Starting ${extra?.agentic ? "agent" : "auto"} build [${body.mode}] ${body.name}…\n`);
    ab$("ab-log-builder").textContent = "";
    ab$("ab-log-answerer").textContent = "";
    ab$("ab-log-validator").textContent = "";
    ab$("ab-decision-box").hidden = true;
    setAbButtons(true);
    try {
      const r = await api.post("/api/app-builder/jobs", body);
      if (!r.ok || !r.job_id) throw new Error(r.error || "Failed to start job");
      connectEvents(r.job_id);
    } catch (e) {
      setAbButtons(false);
      abStatus(`\nError: ${e.message}\n`, true);
    }
  }

  async function initAbForm() {
    try {
      const s = await api.get("/api/app-builder/services");
      const svcs = s.services || [];
      ab$("ab-services").innerHTML = svcs.map(
        (svc) => `<label class="checkbox"><input type="checkbox" data-svc="${esc(svc)}" ` +
          `${DEFAULT_SVC.has(svc) ? "checked" : ""}/> ${esc(svc)}</label>`,
      ).join("");
    } catch (_) { /* services optional */ }
    ab$("ab-features").innerHTML = FEATURES.map(
      (f) => `<label class="checkbox"><input type="checkbox" id="ab-feat-${f}" checked/> ${f}</label>`,
    ).join("");
    try {
      const pii = await api.get("/api/app-builder/pii");
      if (ab$("ab-mask-pii")) ab$("ab-mask-pii").checked = !!pii.enabled;
    } catch (_) { /* optional */ }
    try {
      const lm = await api.get("/api/app-builder/llm-models");
      const models = lm.models || [];
      ab$("ab-train-llm").innerHTML = models.map(
        (m) => `<label class="checkbox"><input type="checkbox" data-llm="${esc(m.name)}" /> ${esc(m.name)}</label>`,
      ).join("") || "<span class='muted'>No trained models yet</span>";
      const engSel = ab$("ab-train-engine");
      if (engSel) {
        engSel.innerHTML = (lm.engines || []).map(
          (e) => `<option value="${esc(e.name)}">${esc(e.name)}</option>`,
        ).join("");
      }
    } catch (_) { /* optional */ }
    populateAbConnections();
  }

  function populateAbConnections() {
    const sel = ab$("ab-conn");
    if (!sel) return;
    const opts = Array.from(document.querySelectorAll("#ai-conn option"))
      .map((o) => o.value).filter(Boolean);
    sel.innerHTML = '<option value="">(none)</option>' +
      opts.map((c) => `<option value="${esc(c)}">${esc(c)}</option>`).join("");
    sel.onchange = () => refreshRagStatus();
    refreshRagStatus();
  }

  function wireAbButtons() {
    ab$("ab-index-rag")?.addEventListener("change", () => {
      const strat = ab$("ab-rag-strategy");
      if (strat) strat.disabled = !ab$("ab-index-rag").checked;
    });
    ab$("ab-build")?.addEventListener("click", async () => {
      try {
        const body = collectBody({ use_ai: false });
        setAbButtons(true);
        abStatus(`Building [${body.mode}] ${body.name}…\n`);
        const r = await api.post("/api/app-builder/build", body);
        setWorkspace(r.workspace, body.name);
        abStatus(JSON.stringify(r, null, 2));
        setAbButtons(false);
      } catch (e) {
        setAbButtons(false);
        abStatus(`Error: ${e.message}\n`);
      }
    });
    ab$("ab-auto")?.addEventListener("click", () => startJob({ use_ai: true }));
    ab$("ab-agent")?.addEventListener("click", () => startJob({ use_ai: true, agentic: true }));
    ab$("ab-train-build")?.addEventListener("click", async () => {
      try {
        const body = collectBody({ train_mode: "full" });
        if (state.workspace) body.workspace = state.workspace;
        if (!(body.train_llm.length || body.train_new_name)) {
          abStatus("Select an existing model or enter a new model name first.\n", true);
          return;
        }
        setAbButtons(true);
        abStatus("Training LLM from this build's data…\n");
        const r = await api.post("/api/app-builder/build-train-llm", body);
        if (!r.ok) {
          abStatus(`Train failed: ${r.error || r.reason || "unknown"}\n`, true);
        } else {
          const cs = r.corpus_stats || {};
          abStatus(
            `Trained ${(r.models || []).length} model(s) on ${r.pairs} ` +
            `build-data pair(s) (validation=${cs.validation}, ` +
            `rejected=${cs.rejected || 0})\n`, true);
        }
        setAbButtons(false);
      } catch (e) {
        setAbButtons(false);
        abStatus(`Error: ${e.message}\n`);
      }
    });
    ab$("ab-stop-build")?.addEventListener("click", async () => {
      if (!state.jobId) return;
      await api.post(`/api/app-builder/jobs/${state.jobId}/stop`, {});
      abStatus("\nGracefully stopping build…\n", true);
      ab$("ab-stop-build").disabled = true;
    });
    ab$("ab-take-control")?.addEventListener("click", async () => {
      if (!state.jobId) return;
      await api.post(`/api/app-builder/jobs/${state.jobId}/take-control`, {});
      ab$("ab-interaction").value = "interactive";
      abStatus("\nSwitched to interactive mode.\n", true);
    });
    ab$("ab-msg-send")?.addEventListener("click", async () => {
      const text = ab$("ab-msg-text").value.trim();
      if (!text) return;
      const target = ab$("ab-msg-target").value;
      const interactive = ab$("ab-interaction").value === "interactive";
      if (state.jobId) {
        const r = await api.post(`/api/app-builder/jobs/${state.jobId}/message`, {
          text, target, interactive,
        });
        if (r.reply) abSessionLog("answerer", `[reply] ${r.reply}\n`);
      }
      ab$("ab-msg-text").value = "";
    });
    ab$("ab-start-app")?.addEventListener("click", async () => {
      const name = state.appName || ab$("ab-name").value.trim();
      const port = parseInt(ab$("ab-port").value, 10) || 8000;
      const r = await api.post("/api/app-builder/start-app", { name, port });
      if (r.ok) {
        ab$("ab-stop-app").disabled = false;
        ab$("ab-start-app").disabled = true;
        abStatus(`\nStarted app: ${r.url}\n`, true);
        ab$("ab-workspace-path").innerHTML =
          `Workspace: ${esc(r.workspace)} — <a href="${esc(r.url)}" target="_blank">${esc(r.url)}</a>`;
      } else {
        abStatus(`\nStart failed: ${(r.issues || []).join("; ")}\n`, true);
      }
    });
    ab$("ab-stop-app")?.addEventListener("click", async () => {
      const name = state.appName || ab$("ab-name").value.trim();
      await api.post("/api/app-builder/stop-app", { name });
      ab$("ab-stop-app").disabled = true;
      ab$("ab-start-app").disabled = !state.workspace;
      abStatus("\nApp stopped.\n", true);
    });
    ab$("ab-package")?.addEventListener("click", async () => {
      const name = state.appName || ab$("ab-name").value.trim();
      const port = parseInt(ab$("ab-port").value, 10) || 8000;
      abStatus("\nPackaging…\n", true);
      const r = await api.post("/api/app-builder/package", { name, port, archive: true });
      if (r.ok) {
        abStatus(`  packaged: ${(r.created || []).join(", ")}\n`, true);
        if (r.archive) abStatus(`  archive: ${r.archive}\n`, true);
      } else {
        abStatus(`  issues: ${(r.issues || []).join("; ")}\n`, true);
      }
    });
    ab$("ab-delete")?.addEventListener("click", async () => {
      const name = state.appName || ab$("ab-name").value.trim();
      if (!confirm(`Delete build '${name}' completely?`)) return;
      if (state.jobId) await api.post(`/api/app-builder/jobs/${state.jobId}/stop`, {}).catch(() => {});
      await api.post("/api/app-builder/stop-app", { name }).catch(() => {});
      const r = await api.post("/api/app-builder/delete", { name });
      if (r.ok) {
        setWorkspace("", "");
        abStatus(`\nDeleted: ${r.workspace}\n`, true);
        disconnectEvents();
        state.jobId = null;
      }
    });
    ab$("ai-build-app")?.addEventListener("click", (e) => {
      e.preventDefault();
      if (typeof activateTab === "function") activateTab("app-builder");
      populateAbConnections();
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (!ab$("panel-app-builder")) return;
    initAbForm();
    wireAbButtons();
  });
})();
