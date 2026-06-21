"""Standalone LLM training service and template tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


@pytest.fixture()
def sample_db(tmp_path: Path) -> str:
    path = str(tmp_path / "shop.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT, country TEXT);
        CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT);
        INSERT INTO customers(name, country) VALUES ('Alice','US'), ('Bob','UK');
        INSERT INTO orders(customer_id, total, status) VALUES (1, 100.0, 'paid'), (2, 75.0, 'open');
        """
    )
    con.commit()
    con.close()
    return path


class FakeCore:
    def __init__(self, path: str, db_type: str = "SQLite") -> None:
        self.path = path
        self.db_type = db_type

    def get_connection_profile(self, name: str):
        return {"db_type": self.db_type, "service_or_db": self.path}

    def get_objects(self, name: str, obj_type: str = "tables"):
        if obj_type != "tables":
            return []
        con = sqlite3.connect(self.path)
        try:
            rows = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            con.close()
        return [r[0] for r in rows]

    def get_table_schema(self, name: str, table: str):
        con = sqlite3.connect(self.path)
        try:
            cur = con.execute(f"PRAGMA table_info({table})")
            cols = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
        finally:
            con.close()
        return {"error": None, "table": table, "columns": cols, "indexes": []}

    def execute(self, name: str, sql: str):
        con = sqlite3.connect(self.path)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = [[("" if v is None else str(v)) for v in r] for r in cur.fetchall()]
            return {"error": None, "columns": cols, "rows": rows, "rowcount": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0}
        finally:
            con.close()


def test_llm_training_service_train_pairs(monkeypatch):
    from ai_assistant.llm.training_service import LlmTrainingService

    class _FakeLlm:
        def train(self, **kwargs):
            assert kwargs["dataset_path"]
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda c, p, **kw: ("/tmp/pairs.jsonl", len(p)),
    )
    r = LlmTrainingService().train_pairs(
        [{"question": "List customers", "sql": "SELECT id FROM customers"}],
        names=["demo"],
    )
    assert r["ok"]
    assert r["source"] == "explicit_pairs"
    assert r["pairs"] == 1


def test_llm_training_service_db_mining(sample_db, monkeypatch):
    from ai_assistant.llm.training_service import LlmTrainingService

    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda c, p, **kw: ("/tmp/pairs.jsonl", len(p)),
    )
    svc = LlmTrainingService(FakeCore(sample_db))
    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["shop"],
        "train_llm": ["demo"],
        "mine_db": True,
        "train_sample_limit": 3,
    })
    assert r["ok"]
    assert r["source"] == "db_mined"
    assert r["mine_stats"]["kept"] >= 8


def test_query_templates_cover_supported_engines():
    from ai_assistant.llm.query_templates import CATALOG_TEMPLATES, OBJECT_TEMPLATES

    for db_type in ("SQLite", "MySQL", "MariaDB", "PostgreSQL", "SQLServer", "Oracle"):
        assert CATALOG_TEMPLATES.get(db_type), db_type
    assert any(t.id == "object.window_rank" for t in OBJECT_TEMPLATES)


def test_training_policy_builds_corpus(sample_db):
    from ai_assistant.llm.training_policy import build_training_corpus

    r = build_training_corpus(
        FakeCore(sample_db), "shop", sample_limit=3, include_capture=False,
    )
    assert r["ok"]
    assert r["stats"]["policy"]["execution_validated"] is True
    assert any("complexity=" in p["description"] for p in r["pairs"])
