"""Tests for the DB-driven NL->SQL training miner.

Uses a real on-disk SQLite database via a thin fake ``core`` that mirrors the
``CoreDBService`` surface the miner depends on (get_connection_profile,
get_objects, get_table_schema, execute).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ai_assistant.app_builder.db_training_miner import (
    DbTrainingMiner,
    _Dialect,
    mine_connection_pairs,
)


class FakeCore:
    """Minimal CoreDBService-compatible shim backed by a real SQLite file."""

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


@pytest.fixture()
def sample_db(tmp_path: Path) -> str:
    path = str(tmp_path / "shop.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL,
            status TEXT, created_at TEXT
        );
        INSERT INTO customers (name, email, country) VALUES
            ('Alice', 'a@x.com', 'US'),
            ('Bob', 'b@x.com', 'UK'),
            ('Cara', 'c@x.com', 'US');
        INSERT INTO orders (customer_id, total, status, created_at) VALUES
            (1, 100.0, 'paid', '2023-01-15'),
            (1, 50.5, 'pending', '2024-02-20'),
            (2, 75.0, 'paid', '2024-03-01');
        """
    )
    con.commit()
    con.close()
    return path


def test_miner_generates_and_validates_pairs(sample_db: str):
    core = FakeCore(sample_db)
    result = DbTrainingMiner(core, "shop", sample_limit=3).mine()
    assert result["ok"] is True
    pairs = result["pairs"]
    assert len(pairs) >= 8
    stats = result["stats"]
    assert stats["tables"] == 2
    assert stats["validated"] == stats["kept"]
    assert stats["failed"] >= 0
    cats = stats["by_category"]
    # System-catalog metadata queries are always present.
    assert cats.get("catalog", 0) >= 1
    # Per-table sample + count + aggregation categories present.
    assert cats.get("sample", 0) >= 1
    assert cats.get("count", 0) >= 1


def test_every_kept_query_executes_on_real_db(sample_db: str):
    core = FakeCore(sample_db)
    result = DbTrainingMiner(core, "shop", sample_limit=3).mine()
    # Re-run each kept SQL: it must succeed against the real DB (no fakes).
    for p in result["pairs"]:
        res = core.execute("shop", p["sql"])
        assert res["error"] is None, f"{p['sql']} -> {res['error']}"


def test_no_dml_in_generated_sql(sample_db: str):
    core = FakeCore(sample_db)
    result = DbTrainingMiner(core, "shop", sample_limit=3).mine()
    for p in result["pairs"]:
        low = p["sql"].lower()
        for bad in ("insert ", "update ", "delete ", "drop ", "alter ", "truncate "):
            assert bad not in low


def test_table_queries_are_row_limited(sample_db: str):
    core = FakeCore(sample_db)
    result = DbTrainingMiner(core, "shop", sample_limit=2).mine()
    sample_pairs = [p for p in result["pairs"] if p["description"] == "sample"]
    assert sample_pairs
    for p in sample_pairs:
        assert "limit 2" in p["sql"].lower()


def test_read_only_guard_rejects_dml(sample_db: str):
    miner = DbTrainingMiner(FakeCore(sample_db), "shop")
    assert miner._is_read_only("SELECT * FROM customers") is True
    assert miner._is_read_only("WITH x AS (SELECT 1) SELECT * FROM x") is True
    assert miner._is_read_only("DROP TABLE customers") is False
    assert miner._is_read_only("UPDATE customers SET name='x'") is False


def test_dialect_catalog_selection():
    assert any("sqlite_master" in sql
               for _, sql in _Dialect("SQLite").catalog_pairs())
    assert any("information_schema" in sql.lower()
               for _, sql in _Dialect("PostgreSQL").catalog_pairs())
    assert any("information_schema" in sql.lower() or "DATABASE()" in sql
               for _, sql in _Dialect("MariaDB").catalog_pairs())
    assert any("sys.tables" in sql.lower()
               for _, sql in _Dialect("SQLServer").catalog_pairs())
    assert any("user_tables" in sql.lower()
               for _, sql in _Dialect("Oracle").catalog_pairs())


def test_dialect_limit_syntax():
    assert _Dialect("SQLite").limit("SELECT * FROM t", 5).endswith("LIMIT 5")
    assert "TOP 5" in _Dialect("SQLServer").limit("SELECT * FROM t", 5)
    assert "FETCH FIRST 5 ROWS ONLY" in _Dialect("Oracle").limit("SELECT * FROM t", 5)


def test_unsupported_engine_returns_error():
    result = mine_connection_pairs(FakeCore("x", db_type="MongoDB"), "m")
    assert result["ok"] is False
    assert "not supported" in (result["error"] or "").lower()


def test_join_pairs_generated(sample_db: str):
    core = FakeCore(sample_db)
    result = DbTrainingMiner(core, "shop", sample_limit=3).mine()
    # orders.customer_id -> customers heuristic join.
    joins = [p for p in result["pairs"] if p["description"] == "join"]
    assert joins
    assert any("join" in p["sql"].lower() for p in joins)


# ── service integration ──────────────────────────────────────────────────────
def test_service_mine_training_pairs(sample_db: str):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService(core=FakeCore(sample_db))
    r = svc.mine_training_pairs({"connections": ["shop"], "train_sample_limit": 3})
    assert r["ok"] is True
    assert r["stats"]["kept"] >= 8


def test_service_train_llm_uses_db_mining(sample_db: str, monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService(core=FakeCore(sample_db))

    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs["name"], "engine": "python"}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.app_builder.training.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )
    # Avoid expensive AI insight collection in this unit test.
    monkeypatch.setattr(svc, "_insight_for_training", lambda body: None)

    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["shop"],
        "train_llm": ["demo"],
        "mine_db": True,
        "train_sample_limit": 3,
    })
    assert r["ok"] is True
    assert r["source"] == "db_mined"
    assert r["pairs"] >= 8
    assert r["mine_stats"]["kept"] >= 8


def test_service_train_llm_mining_disabled_falls_back(sample_db: str, monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService(core=FakeCore(sample_db))

    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs["name"]}

    monkeypatch.setattr("ai_assistant.llm.service.LlmService", _FakeLlm)
    monkeypatch.setattr(
        "ai_assistant.app_builder.training.persist_pairs",
        lambda c, p, **kw: ("/tmp/x.jsonl", len(p)),
    )
    monkeypatch.setattr(svc, "_insight_for_training", lambda body: None)

    r = svc.train_llm({
        "mode": "from_database",
        "connections": ["shop"],
        "train_llm": ["demo"],
        "mine_db": False,
    })
    # No mining, no insight -> sample-seed fallback keeps the button working.
    assert r["ok"] is True
    assert r["source"] == "sample_seed"
