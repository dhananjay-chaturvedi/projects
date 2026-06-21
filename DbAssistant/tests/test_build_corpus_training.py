"""Tests for build-data LLM training (rich, build-grounded NL->SQL corpus).

Covers the new ``ai_assistant.llm.build_corpus`` extractor, the
``AppBuilderService.build_train_llm`` orchestration, and the CLI / API / UI
wiring so the feature is reachable from all surfaces (Tk, Textual, Web, CLI,
API, headless).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_SCHEMA = (
    "-- Generated DDL (SQLite dialect)\n"
    "CREATE TABLE IF NOT EXISTS customers (id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "name TEXT, email TEXT, balance REAL);\n"
    "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "customer_id TEXT, total REAL, status TEXT);\n"
)


def _make_workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "buildws"
    (ws / "src" / "db").mkdir(parents=True)
    (ws / "src" / "db" / "schema.sql").write_text(_SCHEMA, encoding="utf-8")
    return ws


# ── unit: corpus extraction ───────────────────────────────────────────────────
def test_parse_schema_sql_extracts_tables_and_columns():
    from ai_assistant.llm.build_corpus import parse_schema_sql

    tables = parse_schema_sql(_SCHEMA)
    assert set(tables) == {"customers", "orders"}
    cust = dict(tables["customers"])
    assert cust["id"].upper().startswith("INT")
    assert "name" in cust and "email" in cust
    # PRIMARY/constraint lines are not treated as columns.
    assert all(c.lower() not in {"primary", "foreign", "constraint"}
               for c, _ in tables["customers"])


def test_grounded_pairs_are_schema_specific():
    from ai_assistant.llm.build_corpus import grounded_pairs_for_table

    pairs = grounded_pairs_for_table(
        "customers", [("id", "INTEGER"), ("name", "TEXT"), ("balance", "REAL")])
    sqls = [p["sql"] for p in pairs]
    assert "SELECT * FROM customers" in sqls
    assert any("COUNT(*) FROM customers" in s for s in sqls)
    assert any("AVG(balance)" in s for s in sqls)
    assert all(p["question"] and p["sql"] for p in pairs)


def test_collect_build_corpus_validates_against_generated_sqlite(tmp_path):
    from ai_assistant.llm.build_corpus import collect_build_corpus

    ws = _make_workspace(tmp_path)
    out = collect_build_corpus(ws)
    stats = out["stats"]
    assert stats["validation"] == "generated_sqlite"
    assert stats["tables"] == 2
    assert stats["validated"] > 0
    # Every kept pair must reference a real generated table.
    joined = " ".join(p["sql"].lower() for p in out["pairs"])
    assert "customers" in joined and "orders" in joined


def test_collect_build_corpus_rejects_sql_for_missing_table(tmp_path):
    from ai_assistant.llm.build_corpus import build_sqlite_executor

    ws = _make_workspace(tmp_path)
    ex = build_sqlite_executor(ws)
    assert ex is not None
    _, ok_err = ex("SELECT * FROM customers")
    assert ok_err == ""
    _, bad_err = ex("SELECT * FROM does_not_exist")
    assert bad_err  # SQLite reports "no such table"


# ── service: build_train_llm ──────────────────────────────────────────────────
def test_build_train_llm_from_scratch_trains_new_model(tmp_path):
    from ai_assistant.app_builder.service import AppBuilderService

    ws = _make_workspace(tmp_path)
    svc = AppBuilderService()
    r = svc.build_train_llm({
        "name": "demoapp", "mode": "from_scratch", "workspace": str(ws),
        "train_new_name": "scratchmodel", "train_engine": "python",
    })
    assert r.get("ok"), r
    assert r.get("source") == "build_data"
    assert (r.get("corpus_stats") or {}).get("validation") == "generated_sqlite"
    assert r.get("pairs", 0) > 0
    models = r.get("models") or []
    assert models and models[0].get("ok") and models[0].get("name") == "scratchmodel"


def test_build_train_llm_requires_a_model_name(tmp_path):
    from ai_assistant.app_builder.service import AppBuilderService

    ws = _make_workspace(tmp_path)
    r = AppBuilderService().build_train_llm({
        "name": "demoapp", "mode": "from_scratch", "workspace": str(ws)})
    assert r.get("ok") is False
    assert "model" in (r.get("reason") or r.get("error") or "").lower()


def test_build_train_llm_uses_persisted_insight(tmp_path):
    """A standalone train after a build reads var/build_insight.json."""
    from ai_assistant.app_builder.service import AppBuilderService

    ws = tmp_path / "ws2"
    (ws / "var").mkdir(parents=True)
    # No schema.sql; insight provides the tables instead.
    (ws / "var" / "build_insight.json").write_text(json.dumps({
        "tables": [{"name": "widgets", "columns": ["id", "label", "qty"]}],
    }), encoding="utf-8")
    r = AppBuilderService().build_train_llm({
        "name": "demoapp", "mode": "from_scratch", "workspace": str(ws),
        "train_new_name": "insightmodel", "train_engine": "python",
        "validate": False,
    })
    assert r.get("ok"), r
    assert r.get("pairs", 0) > 0


# ── API ───────────────────────────────────────────────────────────────────────
def test_api_build_train_llm_route(api_client, tmp_path):
    ws = _make_workspace(tmp_path)
    r = api_client.post("/api/app-builder/build-train-llm", json={
        "name": "demoapp", "mode": "from_scratch", "workspace": str(ws),
        "train_new_name": "apimodel", "train_engine": "python",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("ok") is True
    assert body.get("source") == "build_data"
    assert (body.get("corpus_stats") or {}).get("validation") == "generated_sqlite"


# ── CLI ────────────────────────────────────────────────────────────────────────
def test_cli_build_train_llm(tmp_path):
    ws = _make_workspace(tmp_path)
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT)
    proc = subprocess.run(
        [sys.executable, str(ROOT / "app" / "dbtool.py"), "app-builder",
         "build-train-llm", "--name", "demoapp", "--workspace", str(ws),
         "--new-name", "climodel", "--engine", "python", "--format", "json"],
        capture_output=True, text=True, env=env, timeout=180,
    )
    assert proc.returncode == 0, proc.stderr
    out = json.loads(proc.stdout)
    assert out.get("ok") is True
    assert out.get("source") == "build_data"


# ── UI parity wiring ────────────────────────────────────────────────────────────
def test_build_train_wired_across_surfaces():
    # Service + API + CLI.
    svc = (ROOT / "ai_assistant/app_builder/service.py").read_text()
    assert "def build_train_llm" in svc and "collect_build_corpus" in svc
    assert "rich_train" in svc and "_persist_build_data" in svc
    api = (ROOT / "ai_assistant/app_builder/api.py").read_text()
    assert "/build-train-llm" in api and "build_train_llm" in api
    cli = (ROOT / "ai_assistant/app_builder/cli.py").read_text()
    assert "build-train-llm" in cli and "--rich-train" in cli

    # Tk.
    tk = (ROOT / "common/ui/tk/ai/build_apps_dialogs.py").read_text()
    assert "Train from build" in tk and "build_train_llm" in tk
    assert "rich_train" in tk and "rich_train_var" in tk

    # Textual.
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "ab-train-build" in tui and "ab-rich-train" in tui
    assert "build_train_llm" in tui and "_train_from_build" in tui

    # Web.
    web_html = (ROOT / "common/ui/web/static/index.html").read_text()
    web_js = (ROOT / "common/ui/web/static/app_builder_ui.js").read_text()
    assert "ab-train-build" in web_html and "ab-rich-train" in web_html
    assert "/api/app-builder/build-train-llm" in web_js and "rich_train" in web_js
