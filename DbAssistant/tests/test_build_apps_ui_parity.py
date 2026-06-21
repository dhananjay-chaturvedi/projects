"""Parity + smoke tests for the dedicated Build Apps UI screens.

Verifies that the LLM Builder and App Builder screens exist and are wired to the
same services/endpoints across Tk, Textual (TUI) and Web, and that the new CLI
build commands work end-to-end.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# ── Tk ───────────────────────────────────────────────────────────────────────
def test_tk_build_apps_dialogs_exist():
    from common.ui.tk.ai import build_apps_dialogs as d

    # App Builder keeps its standalone opener; the old LLM/RAG capture builders
    # were replaced by the new RAG manager + LLM trainer panels.
    assert hasattr(d, "open_app_builder_dialog")
    assert not hasattr(d, "open_llm_builder_dialog")
    assert not hasattr(d, "open_build_rag_dialog")
    src = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "ai_assistant.app_builder.service" in src
    assert "svc.build(body)" in src
    # The new RAG + LLM panels exist and use the shared services.
    rag = (ROOT / "common/ui/tk/ai/rag_panel.py").read_text()
    llm = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "ai_assistant.rag.service" in rag
    assert "Paste Content" in rag
    assert "Active database" in rag or "Database:" in rag
    assert "Add Codebase" in rag
    assert "Index Schema" in rag
    assert "ai_assistant.llm.service" in llm


def test_tk_build_apps_buttons_on_toolbar_not_options():
    # Build an App + RAG Manager + Build or Train LLM live on the workspace
    # toolbar (right end, colored). The quick Use/Index RAG controls stay in
    # each Generate-SQL tab beside Execute query.
    ws = (ROOT / "common/ui/tk/ai/ai_query_workspace.py").read_text()
    assert '"Build an App"' in ws
    assert '"RAG Manager"' in ws
    assert '"Build or Train LLM"' in ws
    assert '"Build RAG"' not in ws
    assert "open_app_builder_dialog" in ws
    assert "open_rag_panel" in ws
    assert "open_llm_panel" in ws  # LLM button opens the new trainer panel
    assert "side=tk.RIGHT" in ws  # anchored to the right end of the bar
    assert "#ADD8E6" in ws  # light blue coloring

    # The lightweight RAG controls live on the per-tab Generate-SQL UI.
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "Use RAG" in ui
    assert "Index RAG" in ui
    assert "rag_index_current" in ui


def test_tk_auto_scroll_helper_wired_across_main_surfaces():
    """Tk tabs/dialogs use the shared both-axis auto-hiding scroll helper."""
    widgets = (ROOT / "common/ui/tk/widgets.py").read_text()
    init = (ROOT / "common/ui/tk/__init__.py").read_text()
    assert "def make_scrollable" in widgets
    assert "xscrollcommand" in widgets and "yscrollcommand" in widgets
    assert "grid_remove()" in widgets
    assert "<Shift-MouseWheel>" in widgets or "<Shift-Button-4>" in widgets
    assert "make_scrollable" in init

    shell = (ROOT / "common/ui/tk/master_shell.py").read_text()
    assert "make_scrollable(self.welcome_tab" in shell
    assert "make_scrollable(" in shell and "self.connections_tab" in shell
    assert "self.root.minsize(min(int(min_width), 480)" in shell

    for rel in [
        "common/ui/tk/dashboard_ui.py",
        "common/ui/tk/database_objects_panel.py",
        "common/ui/tk/settings_ui.py",
        "common/ui/tk/migrator/schema_converter_ui.py",
        "common/ui/tk/ai/llm_panel.py",
        "common/ui/tk/ai/rag_panel.py",
        "common/ui/tk/ai/build_apps_dialogs.py",
        "common/ui/tk/ai/ai_query_ui.py",
        "common/ui/tk/db_connection_form.py",
        "common/ui/tk/cloud_db_connection_panel.py",
        "common/ui/tk/module_config_dialog.py",
        "common/ui/tk/monitor/server_monitor/mixins/cloud_monitor_mixin.py",
        "common/ui/tk/monitor/server_monitor/mixins/ssh_monitor_mixin.py",
    ]:
        src = (ROOT / rel).read_text()
        assert "make_scrollable" in src, rel


def test_tk_from_database_uses_live_connection_introspection():
    """from_database must introspect the live connected session, not a fresh
    disconnected service, and surface real tables in the generated build."""
    from common.ui.tk.ai import build_apps_dialogs as d

    # Simulate a live db manager + registry introspection.
    class _Reg:
        @staticmethod
        def execute_operation(db_type, op, conn, *args, **kwargs):
            if op == "getTables":
                return [("customers",), ("orders",)]
            if op == "getTableSchema":
                table = args[0]
                return {
                    "customers": [{"name": "id"}, {"name": "email"}],
                    "orders": [{"name": "id"}, {"name": "total"}],
                }.get(table, [])
            return []

    import common.database_registry as reg_mod

    orig = reg_mod.DatabaseRegistry
    reg_mod.DatabaseRegistry = _Reg
    try:
        class _DBM:
            db_type = "MariaDB"
            conn = object()

        schema = d._introspect_live_schema(_DBM())
    finally:
        reg_mod.DatabaseRegistry = orig

    assert schema == {"customers": ["id", "email"], "orders": ["id", "total"]}


def test_tk_active_connection_core_uses_live_manager(tmp_path):
    """Manual Train LLM must work for active Tk connections not found in saved profiles."""
    import sqlite3

    from common.ui.tk.ai import build_apps_dialogs as d

    db_path = tmp_path / "active.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO customers(name) VALUES ('Alice'), ('Bob');
        """
    )

    live_conn = conn

    class _Mgr:
        db_type = "SQLite"
        conn = live_conn

        def execute_query(self, sql):
            cur = self.conn.execute(sql)
            cols = [c[0] for c in cur.description] if cur.description else []
            rows = cur.fetchall()
            return {"columns": cols, "rows": rows, "rowcount": len(rows), "time": 0}, None

    class _Owner:
        active_connections = {"PostgreSQL-db_assistant-1": _Mgr()}

    core = d._ActiveConnectionCore(_Owner())
    mgr = core.get_manager("PostgreSQL-db_assistant-1")
    assert mgr is _Owner.active_connections["PostgreSQL-db_assistant-1"]
    assert core.get_connection_profile("PostgreSQL-db_assistant-1")["db_type"] == "SQLite"

    result = core.execute("PostgreSQL-db_assistant-1", "SELECT name FROM customers")
    assert result["error"] is None
    assert result["rows"] == [["Alice"], ["Bob"]]

    objects = core.get_objects("PostgreSQL-db_assistant-1", "tables")
    assert "customers" in objects
    schema = core.get_table_schema("PostgreSQL-db_assistant-1", "customers")
    assert schema["error"] is None
    assert "id" in [str(c[0]) for c in schema["columns"]]
    conn.close()


def test_from_database_build_writes_real_files_to_disk(tmp_path, monkeypatch):
    """End-to-end: a real schema produces models on disk (the actual 'build')."""
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from ai_assistant.app_builder.service import AppBuilderService

    r = AppBuilderService().build({
        "name": "liveapp", "mode": "from_database",
        "schema": {"customers": ["id", "email"], "orders": ["id", "total"]},
    })
    assert r["ok"] is True
    ws = Path(r["workspace"])
    models = ws / "src" / "models.py"
    assert models.is_file()
    text = models.read_text()
    assert "class Customer" in text and "class Order" in text


def test_tk_start_generated_app_uses_uvicorn(monkeypatch, tmp_path):
    from common.ui.tk.ai import build_apps_dialogs as d

    calls = {}

    class _Proc:
        stdout = []

        def poll(self):
            return None

    def fake_popen(cmd, **kwargs):
        calls["cmd"] = cmd
        calls["kwargs"] = kwargs
        return _Proc()

    import subprocess as sp

    monkeypatch.setattr(sp, "Popen", fake_popen)
    proc = d._start_app_process(str(tmp_path), "8123")
    assert proc is not None
    assert calls["cmd"][:4] == [sys.executable, "-m", "uvicorn", "src.app:app"]
    assert "--port" in calls["cmd"]
    assert "8123" in calls["cmd"]
    assert calls["kwargs"]["cwd"] == str(tmp_path)


# ── TUI ──────────────────────────────────────────────────────────────────────
def test_tui_build_apps_modals_exist():
    from common.ui.textual.screens.build_apps import (
        AppBuilderModal,
        LlmTrainerModal,
        RagManagerModal,
    )

    # Constructable without a running app. The old capture-based LLM-Builder
    # modal was replaced by the LlmTrainerModal (parity with Tk/Web "Build or
    # Train LLM").
    assert AppBuilderModal(connections=["c1"]) is not None
    assert LlmTrainerModal(connections=["c1"]) is not None
    assert RagManagerModal(connections=["c1"]) is not None
    src = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "LlmBuilderModal" not in src
    assert "run_agentic_build" in src and "AppBuilderScreen" in src
    assert '"Add codebase folder", "codebase"' in src or "add-codebase" in src
    assert '"Preview search", "preview"' in src or "rag_preview" in src
    # The TUI trainer drives the same LlmService code path as Tk/Web/CLI.
    assert "ai_assistant.llm.service" in src
    # Evaluate-model action is exposed and wired to LlmService.evaluate (the same
    # code path as the CLI/API eval), surfacing accuracy meters in the TUI.
    assert '("Evaluate model", "eval")' in src
    assert 'action == "eval"' in src and "svc.evaluate(" in src
    assert "format_eval_summary" in src


def test_tui_ai_screen_pushes_modals():
    src = (ROOT / "common/ui/textual/screens/ai_query.py").read_text()
    assert "LlmBuilderModal" not in src
    assert "AppBuilderScreen" in src
    # The TUI exposes RAG Manager and "Build or Train LLM" and opens the modals.
    assert "RAG Manager" in src
    build_apps = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "ab-db-variant" in build_apps
    assert "RagManagerModal" in src
    assert "Build or Train LLM" in src
    assert "LlmTrainerModal" in src


def test_auto_build_wired_across_uis():
    """The autonomous orchestrator is reachable from every UI + the API/CLI."""
    api = (ROOT / "ai_assistant/app_builder/api.py").read_text()
    assert "/auto-build" in api and "service.auto_build" in api
    cli = (ROOT / "ai_assistant/app_builder/cli.py").read_text()
    assert "auto-build" in cli and "svc.auto_build" in cli
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "Auto-build (AiQA)" in tk and "svc.auto_build" in tk and "AiQueryBridge" in tk
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "Auto-build (AiQA)" in tui and "run_agentic_build" in tui
    web = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    web_html = (ROOT / "common/ui/web/static/index.html").read_text()
    assert "ab-auto" in web and "/api/app-builder/jobs" in web
    assert "Auto-build (AiQA)" in web_html
    web_legacy = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "/api/app-builder/auto-build" not in web_legacy or "panel-app-builder" in (
        ROOT / "common/ui/web/static/index.html"
    ).read_text()


def test_delete_build_wired_across_surfaces():
    """Delete build (erase a build's workspace) is exposed in UI + CLI + API."""
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "Delete build" in tk and "svc.delete_app" in tk
    cli = (ROOT / "ai_assistant/app_builder/cli.py").read_text()
    assert '"delete"' in cli and "svc.delete_app" in cli
    api = (ROOT / "ai_assistant/app_builder/api.py").read_text()
    assert "/delete" in api and "service.delete_app" in api


def test_app_builder_jobs_wired_across_surfaces():
    """Background jobs + SSE + runtime endpoints for Web real-time builds."""
    api = (ROOT / "ai_assistant/app_builder/api.py").read_text()
    assert "/jobs" in api and "text/event-stream" in api
    assert "/start-app" in api and "/stop-app" in api
    web = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    assert "EventSource" in web or "events/poll" in web
    assert "ab-agent" in web and "ab-stop-build" in web and "ab-package" in web
    html = (ROOT / "common/ui/web/static/index.html").read_text()
    assert "panel-app-builder" in html and "ab-log-builder" in html
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "AppBuilderScreen" in tui and "run_agentic_build" in tui
    assert "ab-stop" in tui and "ab-package" in tui


def test_mask_pii_and_train_llm_wired_across_surfaces():
    """Mask PII + Train LLM controls exist in Tk/TUI/Web/CLI/API."""
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "Mask PII data" in tk and "mask_pii_var" in tk
    assert "train_llm_manual" not in tk and "_collect_train_body" not in tk
    assert "use_rag_var" in tk and "index_rag_var" in tk
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "ab-mask-pii" in tui and "ab-train-llm" in tui
    assert '"ab-train"' not in tui and "_collect_train_body" not in tui
    assert "ab-use-rag" in tui and "ab-index-rag" in tui
    web_js = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    web_html = (ROOT / "common/ui/web/static/index.html").read_text()
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "ab-mask-pii" in web_html and "ab-train-llm" in web_html
    assert "ab-use-rag" in web_html and "ab-index-rag" in web_html
    assert "collectTrainBody" not in web_js and "ab-train-btn" not in web_html
    assert "refreshRagStatus" in web_js
    assert "mask_pii" in web_js
    assert "/api/app-builder/pii" in web_js and "/api/app-builder/llm-models" in web_js
    assert "/api/app-builder/rag-status" in web_js
    assert "/api/ai/llm/jobs" in web_app and 'startLlmJob("train"' in web_app
    assert "/api/ai/llm/train-pairs" in web_app
    ai_api = (ROOT / "ai_query/api.py").read_text()
    ai_cli = (ROOT / "ai_query/cli.py").read_text()
    assert "/api/ai/llm/eval" in ai_api
    assert "llm_eval" in ai_api
    assert 'llm_sub.add_parser("eval"' in ai_cli
    cli = (ROOT / "ai_assistant/app_builder/cli.py").read_text()
    assert "--mask-pii" in cli and "--train-llm" in cli and "train-llm" in cli
    assert "--use-rag" in cli and "--index-rag" in cli and "--rag-strategy" in cli
    assert "rag-status" in cli and "index-rag" in cli
    assert "llm-models" in cli and "start-app" in cli and "jobs" in cli
    assert "--no-mine" in cli and "--sample-limit" in cli and "mine-pairs" in cli
    api = (ROOT / "ai_assistant/app_builder/api.py").read_text()
    assert "/pii" in api and "/llm-models" in api and "/train-llm" in api
    assert "/rag-status" in api and "/index-rag" in api
    assert "/mine-training-pairs" in api
    ai_ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    # AI Query training opens a session-config dialog (target model + engine),
    # not a dropdown, and the engine is user-chosen (no silent python default).
    assert "open_train_llm_dialog" in ai_ui and "_aiqa_train_current" in ai_ui
    assert "Train LLM ▾" not in ai_ui and "train_llm_current_pair" not in ai_ui
    assert 'engine="python"' not in ai_ui
    # Session config persists for the AI Query tab and reuses the toolbar Use RAG.
    assert "_aiqa_train_model" in ai_ui and "_aiqa_train_engine" in ai_ui
    assert "use_rag = bool(self.use_rag_var.get())" in ai_ui
    tui = (ROOT / "common/ui/textual/screens/ai_query.py").read_text()
    assert "class AiqaTrainModal" in tui and "ai-train-current" in tui
    assert 'engine="python"' not in tui and 'engine": "python"' not in tui
    web_main = (ROOT / "common/ui/web/static/index.html").read_text()
    assert "ai-train-current" in web_main and "/api/ai/llm/train-pairs" in web_app
    # Web AI Query training uses a session-config modal (model + engine), no
    # hard-coded python engine.
    assert "aiqaTrain" in web_app and "aiqa-train-current" in web_app
    assert 'engine: "python"' not in web_app


def test_db_training_miner_wired_across_surfaces():
    """DB-driven mining controls exist in Tk/TUI/Web and service/API/CLI."""
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "mine_db_var" in tk and "sample_limit_var" in tk and "Mine DB queries" in tk
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "ab-mine-db" in tui and "ab-sample-limit" in tui
    web_js = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    web_html = (ROOT / "common/ui/web/static/index.html").read_text()
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "ab-mine-db" in web_html and "ab-sample-limit" in web_html
    assert "mine_db" in web_js
    llm_panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "Preview mined queries" in llm_panel and "Rich DB training (from database)" in llm_panel
    ai_ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "extra_pairs" in ai_ui
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "extra_pairs" in web_app
    tui_ai = (ROOT / "common/ui/textual/screens/ai_query.py").read_text()
    assert "extra_pairs" in tui_ai
    assert "_exact_recall_sql" in (ROOT / "ai_assistant/llm/service.py").read_text()
    assert "/api/ai/llm/mine-training-pairs" in web_app
    svc = (ROOT / "ai_assistant/app_builder/service.py").read_text()
    assert "def mine_training_pairs" in svc and "LlmTrainingService" in svc
    miner = (ROOT / "ai_assistant/llm/db_query_miner.py").read_text()
    assert "class DbTrainingMiner" in miner and "catalog_pairs" in miner
    ai_cli = (ROOT / "ai_query/cli.py").read_text()
    assert '"show"' in ai_cli and "ai_session_get" in ai_cli
    assert '"dataset"' in ai_cli and "llm_dataset" in ai_cli


def test_llm_harvest_wired_across_surfaces():
    """Auto-harvest & train is available on Tk, Textual, Web, CLI, and API."""
    llm_panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "Auto-harvest & train" in llm_panel and "do_harvest" in llm_panel
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "Auto-harvest & train" in tui and '"harvest"' in tui
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "aiqa-harvest" in web_app and "startLlmJob" in web_app
    assert "/api/ai/llm/jobs" in web_app
    ai_api = (ROOT / "ai_query/api.py").read_text()
    ai_cli = (ROOT / "ai_query/cli.py").read_text()
    assert "/api/ai/llm/harvest" in ai_api and '"harvest"' in ai_cli
    assert "generated_questions" in ai_cli and "no-curated" in ai_cli
    cfg = (ROOT / "ai_query/module_config.py").read_text()
    assert "ai.llm.harvest" in cfg and "generated_questions" in cfg
    assert "train_mode" in cfg and "gen_workers" in cfg
    assert "ai.llm.harvest" in (ROOT / "ai_query/config.ini.example").read_text()
    assert (ROOT / "ai_assistant/llm/seed_corpus.py").is_file()
    assert (ROOT / "ai_assistant/llm/data/seed_problems.yaml").is_file()
    assert "class LlmHarvestService" in (ROOT / "ai_assistant/llm/harvest_service.py").read_text()
    assert "train_mode_var" in llm_panel and "gen_workers_var" in llm_panel
    assert "already_trained" in llm_panel and "skipped_known" in llm_panel
    assert "_harvest_extras" in tui and "llm-train-mode" in tui
    assert "aiqa-train-mode" in web_app and "aiqa-gen-workers" in web_app
    assert "--train-mode" in ai_cli and "--gen-workers" in ai_cli
    assert "--gen-timeout" in ai_cli and "--no-retry-backlog" in ai_cli


def test_llm_training_progress_wired_across_surfaces():
    """Live training progress (phase + epoch/loss) on Tk, Textual, Web, CLI, API."""
    llm_panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "on_progress=progress" in llm_panel
    assert "training_epoch" in llm_panel and "_training_progress_msg" in llm_panel
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "progress=progress" in tui and "_training_progress_msg" in tui
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "llmProgressMessage" in web_app and "training_epoch" in web_app
    assert "/api/ai/llm/jobs" in web_app
    ai_api = (ROOT / "ai_query/api.py").read_text()
    ai_cli = (ROOT / "ai_query/cli.py").read_text()
    assert "/api/ai/llm/jobs" in ai_api and "llm_job_events" in ai_api
    assert "/api/ai/llm/jobs/{job_id}/stop" in ai_api
    assert "progress=progress" in ai_cli and "training_epoch" in ai_cli
    training_svc = (ROOT / "ai_assistant/llm/training_service.py").read_text()
    assert "_model_epoch_progress" in training_svc
    assert (ROOT / "ai_assistant/llm/jobs.py").is_file()
    assert "class LlmJobManager" in (ROOT / "ai_assistant/llm/jobs.py").read_text()


def test_llm_harvest_stop_wired_across_surfaces():
    """Graceful 'Stop harvest' is available on Tk, Textual, Web, and API."""
    llm_panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "Stop harvest" in llm_panel and "do_stop_harvest" in llm_panel
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "Stop harvest" in tui and "llm-stop" in tui and "llm_harvest_stop" in tui
    web_app = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "aiqa-harvest-stop" in web_app and "stopLlmJob" in web_app
    assert "/api/ai/llm/jobs/" in web_app
    ai_api = (ROOT / "ai_query/api.py").read_text()
    assert "/api/ai/llm/harvest/stop" in ai_api and "llm_harvest_stop" in ai_api
    assert "/api/ai/llm/jobs/{job_id}/stop" in ai_api
    svc = (ROOT / "ai_query/service.py").read_text()
    assert "def llm_harvest_stop" in svc
    harvest = (ROOT / "ai_assistant/llm/harvest_service.py").read_text()
    assert "should_stop" in harvest


def test_ai_query_rules_and_session_wired():
    """AI Query TUI + Web expose rules editors and session save/load/exec."""
    web = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "ai-exec-rules-text" in web or "ai-exec-rules-text" in (
        ROOT / "common/ui/web/static/index.html"
    ).read_text()
    assert "ai-session-exec-sql" in web
    assert "sql_mode" in web and "ai-auto-exec" in web
    tui = (ROOT / "common/ui/textual/screens/ai_query.py").read_text()
    assert "ai-exec-rules-text" in tui and "ai-session-exec-sql" in tui
    assert "sql_mode=self._sql_mode" in tui


def test_stop_build_keeps_sessions_active():
    """A graceful stop must keep the A/B/C sessions alive for chat/take-control."""
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    # Stop no longer disables take-control, and post-build chat is enabled even
    # when the build was aborted by the user.
    assert "Gracefully stopping build" in tk
    assert "_enable_post_build_chat(agentic=bool(r.get(\"agentic\")))" in tk


# ── Web ──────────────────────────────────────────────────────────────────────
def test_web_build_apps_handlers():
    src = (ROOT / "common/ui/web/static/app.js").read_text()
    ab = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    html = (ROOT / "common/ui/web/static/index.html").read_text()
    # The local LLM trainer + RAG manager use the new ai endpoints.
    assert "/api/ai/llm/train" in src
    assert "/api/ai/rag/index" in src
    assert "/api/ai/rag/ask" in src
    assert "/api/ai/rag/remove-document" in src or "remove-document" in src
    # App Builder full panel uses jobs API for agentic builds.
    assert "panel-app-builder" in html
    assert "/api/app-builder/jobs" in ab
    assert "/api/app-builder/build" in ab
    assert "from_database" in ab or "from_database" in html


# ── shared spec parity ─────────────────────────────────────────────────────--
def test_shared_spec_build_apps_actions():
    from common.ui.shared import specs

    ids = [a["id"] for a in specs.AI_BUILD_APPS_ACTIONS]
    assert ids == ["app_builder"]


# ── CLI smoke for new build commands ─────────────────────────────────────────-
def test_app_builder_build_cli(tmp_path):
    env = {**os.environ, "DBASSISTANT_HOME": str(tmp_path)}
    p = subprocess.run(
        [sys.executable, "-m", "ai_assistant.app_builder", "app-builder",
         "build", "--name", "cliscratch", "--mode", "from_scratch",
         "--description", "demo"],
        cwd=ROOT, capture_output=True, text=True, timeout=40, env=env,
    )
    assert p.returncode == 0, (p.stdout or "") + (p.stderr or "")
    # from_scratch emits only the minimal runnable stub; the agent designs the
    # rest of the structure freely, so we assert the stub contract, not a fixed
    # tests/ layout.
    base = tmp_path / "ai_assistant" / "app_builder" / "cliscratch"
    assert (base / "src" / "app.py").is_file()
    assert (base / "requirements.txt").is_file()


def test_app_builder_build_from_database_cli(tmp_path):
    env = {**os.environ, "DBASSISTANT_HOME": str(tmp_path)}
    # No connection/schema -> blueprint should be rejected (exit 1), not crash.
    p = subprocess.run(
        [sys.executable, "-m", "ai_assistant.app_builder", "app-builder",
         "build", "--name", "clidb", "--mode", "from_database"],
        cwd=ROOT, capture_output=True, text=True, timeout=40, env=env,
    )
    assert p.returncode == 1
    assert "connection" in (p.stdout + p.stderr).lower()


def test_app_builder_train_llm_cli_dispatch(monkeypatch):
    """CLI train-llm dispatch prints reason/source on success."""
    from argparse import Namespace
    from ai_assistant.app_builder import cli as ab_cli
    from ai_assistant.app_builder.service import AppBuilderService

    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.app_builder.training.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )
    monkeypatch.setattr(
        "ai_assistant.app_builder.service.make_service",
        lambda: AppBuilderService(),
    )

    args = Namespace(
        app_action="train-llm",
        mode="from_scratch",
        description="",
        connection="",
        codebase_path="",
        train_llm=[],
        train_new_name="cli_demo",
        train_engine="",
        use_rag=False,
        no_rag=False,
        index_rag=False,
        rag_strategy="index_first",
        include_sample=True,
    )
    rc = ab_cli.dispatch_cli(args)
    assert rc == 0


def test_ai_llm_engines_cli(tmp_path):
    """The local LLM CLI lists engines (python always available)."""
    env = {**os.environ, "DBASSISTANT_HOME": str(tmp_path)}
    p = subprocess.run(
        [sys.executable, "-m", "ai_query", "ai", "llm", "engines"],
        cwd=ROOT, capture_output=True, text=True, timeout=40, env=env,
    )
    assert p.returncode == 0, (p.stdout or "") + (p.stderr or "")
    assert "python" in (p.stdout + p.stderr)
