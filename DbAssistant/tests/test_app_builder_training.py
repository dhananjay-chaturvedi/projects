"""Tests for App Builder training collector and train_llm service."""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_assistant.app_builder.training import (
    _dedupe_pairs,
    _fold_question,
    _valid_sql,
    collect_build_pairs,
    collect_codebase_pairs,
    collect_connection_pairs,
    collect_scratch_pairs,
    persist_pairs,
    resolve_train_names,
)


def test_fold_question_keeps_nl_only():
    """Descriptions must not be folded into questions (tokenizer pollution)."""
    q = _fold_question("List customers", "Customer master table")
    assert "List customers" in q
    assert "Customer master table" not in q


def test_dedupe_pairs_normalizes_sql():
    pairs = _dedupe_pairs([
        {"question": "List all users", "sql": "SELECT * FROM users;"},
        {"question": "List all users", "sql": "SELECT * FROM users"},
    ])
    assert len(pairs) == 1
    assert pairs[0]["sql"] == "SELECT * FROM users"


def test_valid_sql_accepts_select():
    assert _valid_sql("SELECT id, name FROM customers WHERE active = 1")


def test_collect_build_pairs_from_workspace(tmp_path: Path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "repo.py").write_text(
        'def fetch_all():\n    """Load all customers"""\n'
        '    sql = "SELECT id, email FROM customers"\n    return sql\n',
        encoding="utf-8",
    )
    pairs = collect_build_pairs(tmp_path, "myconn")
    assert pairs
    assert any("customers" in p["sql"].lower() for p in pairs)


def test_collect_connection_pairs_from_insight():
    class _T:
        name = "orders"
        columns = ["id", "total"]
        note = "Order transactions"

    class _Insight:
        tables = [_T()]
        app_summary = "E-commerce orders"
        app_features = ["view orders"]

    pairs = collect_connection_pairs("myconn", _Insight())
    assert pairs
    assert any("orders" in p["sql"].lower() for p in pairs)


def test_resolve_train_names_merges_new():
    body = {"train_llm": ["default"], "train_new_name": "shop_model"}
    assert resolve_train_names(body) == ["default", "shop_model"]


def test_persist_pairs_writes_jsonl(monkeypatch, tmp_path):
    out = tmp_path / "pairs.jsonl"

    def _mkstemp(**_kwargs):
        fd = out.open("w").fileno() if False else 99
        return 99, str(out)

    monkeypatch.setattr("ai_assistant.app_builder.training.tempfile.mkstemp", _mkstemp)
    import os

    monkeypatch.setattr(os, "close", lambda _fd: None)
    path, count = persist_pairs("", [{"question": "Q", "sql": "SELECT id FROM t"}])
    assert count == 1
    assert Path(path).read_text(encoding="utf-8").strip()


def test_collect_connection_pairs_skips_rag_when_disabled():
    class _T:
        name = "orders"
        columns = ["id"]
        note = "Order transactions"

    class _Insight:
        tables = [_T()]
        app_summary = ""
        app_features = []

    pairs = collect_connection_pairs("myconn", _Insight(), use_rag=False)
    assert pairs
    assert any("orders" in p["sql"].lower() for p in pairs)


def test_collect_scratch_pairs_from_description():
    pairs = collect_scratch_pairs("Inventory tracking app for warehouses")
    assert pairs
    assert "warehouse" in pairs[0]["description"].lower() or "Inventory" in pairs[0]["description"]


def test_collect_codebase_pairs_from_py_files(tmp_path: Path):
    (tmp_path / "repo.py").write_text(
        'def q():\n    return "SELECT id FROM items"\n',
        encoding="utf-8",
    )
    pairs = collect_codebase_pairs(tmp_path)
    assert pairs
    assert "items" in pairs[0]["sql"].lower()


def test_persist_pairs_calls_rag_add_example(monkeypatch, tmp_path):
    out = tmp_path / "pairs.jsonl"
    calls = []

    class _FakeRag:
        def __init__(self, core):
            self.core = core

        def add_example(self, connection, question, sql, description=""):
            calls.append({
                "connection": connection,
                "question": question,
                "sql": sql,
                "description": description,
            })
            return {"ok": True}

    monkeypatch.setattr(
        "ai_assistant.rag.service.RagService",
        _FakeRag,
    )

    def _mkstemp(**_kwargs):
        return 99, str(out)

    monkeypatch.setattr("ai_assistant.app_builder.training.tempfile.mkstemp", _mkstemp)
    import os

    monkeypatch.setattr(os, "close", lambda _fd: None)
    path, count = persist_pairs(
        "myconn",
        [{"question": "Q", "sql": "SELECT id FROM t", "description": "d"}],
        core=object(),
    )
    assert count == 1
    assert len(calls) == 1
    assert calls[0]["connection"] == "myconn"
    assert Path(path).read_text(encoding="utf-8").strip()


def test_train_llm_service(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()

    class _T:
        name = "items"
        columns = ["id"]
        note = "catalog"

    class _Insight:
        tables = [_T()]
        app_summary = "Store catalog"
        app_features = []

    monkeypatch.setattr(svc, "_insight_for_training", lambda body: _Insight())

    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr(
        "ai_assistant.llm.service.LlmService",
        _FakeLlm,
    )
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )

    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["testdb"],
        "train_llm": ["demo"],
        "train_engine": "python",
    })
    assert r["ok"]
    assert r["pairs"] >= 1
    assert r["models"][0]["name"] == "demo"
    assert r.get("reason")


def test_train_llm_scratch_sample_fallback(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()

    class _FakeLlm:
        def train(self, **kwargs):
            assert kwargs.get("include_sample") is True
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )

    r = svc.train_llm({
        "mode": "from_scratch",
        "description": "",
        "train_llm": ["demo"],
    })
    assert r["ok"]
    assert r["source"] == "sample_seed"
    assert r.get("include_sample") is True


def test_train_llm_index_rag_first(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    order = []

    class _T:
        name = "items"
        columns = ["id"]
        note = "catalog"

    class _Insight:
        tables = [_T()]
        app_summary = "Store catalog"
        app_features = []

    monkeypatch.setattr(svc, "_insight_for_training", lambda body: _Insight())

    def _index_rag(conn, *, rebuild=False):
        order.append("index")
        return {"ok": True, "connection": conn}

    monkeypatch.setattr(svc, "index_rag", _index_rag)

    class _FakeLlm:
        def train(self, **kwargs):
            order.append("train")
            return {"ok": True, "name": kwargs["name"]}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )

    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["testdb"],
        "train_llm": ["demo"],
        "index_rag": True,
        "rag_strategy": "index_first",
        "use_rag": True,
    })
    assert r["ok"]
    assert order == ["index", "train"]
    assert r["rag_indexed"] is True


def test_train_llm_index_rag_failure_blocks(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    monkeypatch.setattr(svc, "index_rag", lambda c, **kw: {"ok": False, "error": "no core"})

    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["testdb"],
        "train_llm": ["demo"],
        "index_rag": True,
        "rag_strategy": "index_first",
    })
    assert not r["ok"]
    assert "no core" in (r.get("error") or r.get("reason") or "")


def test_train_llm_api_rag_endpoints():
    from ai_assistant.app_builder.api import build_router

    class _Svc:
        def rag_status(self, connection=""):
            return {"ok": True, "connection": connection, "indexed": False, "doc_count": 0}

        def index_rag(self, connection, *, rebuild=False):
            return {"ok": True, "connection": connection}

        def train_llm(self, body):
            return {"ok": True, "pairs": 1, "source": "sample_seed", "reason": "ok"}

    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(build_router(_Svc()))
    client = TestClient(app)

    r = client.get("/api/app-builder/rag-status", params={"connection": "mydb"})
    assert r.status_code == 200
    assert r.json()["connection"] == "mydb"

    r = client.post("/api/app-builder/index-rag", json={"connection": "mydb"})
    assert r.status_code == 200
    assert r.json()["ok"] is True

    r = client.post("/api/app-builder/train-llm", json={
        "mode": "from_scratch",
        "train_llm": ["demo"],
        "use_rag": True,
        "index_rag": False,
    })
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_get_pii_masking():
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    r = svc.get_pii_masking()
    assert "enabled" in r


def test_mask_if_enabled_masks_email():
    from ai_assistant.app_builder.pii_util import mask_if_enabled

    out = mask_if_enabled("Contact user@example.com", True)
    assert "user@example.com" not in out
    assert mask_if_enabled("plain text", False) == "plain text"


def test_direct_chat_bridge_masks_prompt(monkeypatch):
    from ai_assistant.app_builder.ai_bridge import DirectChatBridge

    seen = {}

    class _Agent:
        def _call_ai(self, prompt, timeout=180):
            seen["prompt"] = prompt
            return {"response": "ok"}

    bridge = DirectChatBridge(_Agent(), mask_pii=True)
    bridge.generate("Email me at user@example.com")
    assert "user@example.com" not in seen.get("prompt", "")


def test_cli_backend_agent_masks_prompt(monkeypatch):
    from ai_assistant.app_builder.agent import AgentRequest, CliBackendAgent
    from ai_assistant.app_builder.engine import BuildMode

    seen = {}

    class _Backend:
        name = "test"

        def call(self, prompt, timeout=180):
            seen["prompt"] = prompt
            return {"response": ""}

    agent = CliBackendAgent(_Backend(), mask_pii=True)
    req = AgentRequest(mode=BuildMode.FROM_SCRATCH, app_name="x", description="secret@x.com")
    resp = agent.generate(req)
    assert resp.backend != "deterministic" or seen.get("prompt")
    if seen.get("prompt"):
        assert "secret@x.com" not in seen["prompt"]
