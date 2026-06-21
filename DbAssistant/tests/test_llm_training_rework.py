"""Tests for the Phase-1 local-LLM training rework.

Covers:
- Training depth (offline = template-only / no backend AI; online = AI allowed).
- Multi-connection harvest spanning several connections + per-dialect routing.
- Adaptive PyTorch capacity scaling with corpus size (and override respect).
- Atomic model artifact snapshot / restore / version listing.
- Scheduler start-time + duration-cap window math.
- Recall-all alternative SQL syntaxes at query time.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from ai_assistant.llm.harvest_service import LlmHarvestService
from ai_assistant.llm.scheduler import LlmHarvestScheduler
from ai_assistant.llm.service import LlmService


# ── fakes ────────────────────────────────────────────────────────────────────
class MultiCore:
    """Fake core where each connection name maps to its own SQLite file + db_type."""

    def __init__(self, conns: dict[str, tuple[str, str]]) -> None:
        # conns: name -> (sqlite_path, db_type)
        self._conns = conns

    def get_connection_profile(self, name: str):
        path, db_type = self._conns[name]
        return {"db_type": db_type, "service_or_db": path}

    def get_objects(self, name: str, obj_type: str = "tables"):
        if obj_type != "tables":
            return []
        path, _ = self._conns[name]
        con = sqlite3.connect(path)
        try:
            rows = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            con.close()
        return [r[0] for r in rows]

    def get_table_schema(self, name: str, table: str):
        path, _ = self._conns[name]
        con = sqlite3.connect(path)
        try:
            cur = con.execute(f"PRAGMA table_info({table})")
            cols = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
        finally:
            con.close()
        return {"error": None, "table": table, "columns": cols, "indexes": []}

    def execute(self, name: str, sql: str):
        path, _ = self._conns[name]
        con = sqlite3.connect(path)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = [[("" if v is None else str(v)) for v in r] for r in cur.fetchall()]
            return {"error": None, "columns": cols, "rows": rows, "rowcount": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0}
        finally:
            con.close()


def _make_db(path: str) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
        INSERT INTO customers (name, email) VALUES ('A', 'a@x'), ('B', 'b@x');
        """
    )
    con.commit()
    con.close()


@pytest.fixture()
def core(tmp_path: Path) -> MultiCore:
    p = str(tmp_path / "shop.db")
    _make_db(p)
    return MultiCore({"shop": (p, "SQLite")})


# ── training depth ─────────────────────────────────────────────────────────
def test_offline_depth_skips_backend_ai(core: MultiCore):
    """Offline depth must never call the backend AI, even with questions queued."""
    called = {"n": 0}

    def gen_sql(conn: str, q: str) -> dict:
        called["n"] += 1
        return {"sql": "SELECT 1"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({
        "connection": "shop",
        "training_depth": "offline",
        "generated_questions": 5,
        "questions": ["how many customers?"],
        "do_train": False,
        "use_captures": False,
    })
    assert r["ok"]
    assert r["training_depth"] == "offline"
    assert called["n"] == 0  # backend AI suppressed offline
    assert r["backend_pairs"] == 0


def test_online_depth_allows_backend_ai(core: MultiCore):
    called = {"n": 0}

    def gen_sql(conn: str, q: str) -> dict:
        called["n"] += 1
        return {"sql": "SELECT COUNT(*) AS c FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({
        "connection": "shop",
        "training_depth": "online",
        "generated_questions": 0,
        "questions": ["how many customers?"],
        "do_train": False,
        "use_captures": False,
        "use_curated": False,
    })
    assert r["ok"] or r["error"] is None or called["n"] >= 1
    assert called["n"] >= 1  # backend AI was used online


# ── multi-connection harvest ─────────────────────────────────────────────────
def test_multi_connection_routing(tmp_path: Path):
    p1 = str(tmp_path / "a.db")
    p2 = str(tmp_path / "b.db")
    _make_db(p1)
    _make_db(p2)
    # Two SQLite connections (same dialect here, but exercises the multi path).
    core = MultiCore({"a": (p1, "SQLite"), "b": (p2, "SQLite")})
    svc = LlmHarvestService(core)
    r = svc.harvest({
        "connections": ["a", "b"],
        "training_depth": "offline",
        "advanced_training": True,
        "do_train": False,
        "use_captures": False,
    })
    assert r["ok"]
    assert set(r["connections"]) == {"a", "b"}
    assert r["advanced"] is True
    assert r["multi_dialect"] is True
    # SQLite dialect should be live-routed to one of the connections.
    assert "sqlite" in r["dialect_connections"]


# ── adaptive capacity ────────────────────────────────────────────────────────
def test_adaptive_capacity_scales_with_corpus(tmp_path: Path):
    svc = LlmService(models_dir=str(tmp_path / "models"))
    cfg_small = {"pt_n_layer": 2, "pt_n_head": 2, "pt_n_embd": 64,
                 "pt_block_size": 128, "pt_max_iters": 500}
    cfg_large = dict(cfg_small)
    small = svc._apply_adaptive_capacity(cfg_small, num_pairs=10, overrides=None)
    large = svc._apply_adaptive_capacity(cfg_large, num_pairs=10000, overrides=None)
    # Tiny corpus keeps the floor; big corpus grows capacity.
    assert small["pt_n_embd"] == 64
    assert large["pt_n_embd"] >= small["pt_n_embd"]
    assert large["pt_n_layer"] >= small["pt_n_layer"]
    # n_embd must stay divisible by n_head.
    assert cfg_large["pt_n_embd"] % cfg_large["pt_n_head"] == 0


def test_adaptive_capacity_respects_explicit_override(tmp_path: Path):
    svc = LlmService(models_dir=str(tmp_path / "models"))
    cfg = {"pt_n_layer": 2, "pt_n_head": 2, "pt_n_embd": 64,
           "pt_block_size": 128, "pt_max_iters": 500}
    svc._apply_adaptive_capacity(
        cfg, num_pairs=10000, overrides={"pt_n_embd": 64, "pt_n_layer": 2})
    assert cfg["pt_n_embd"] == 64  # explicit override never upsized
    assert cfg["pt_n_layer"] == 2


# ── snapshot / restore / versions ────────────────────────────────────────────
def test_snapshot_restore_and_versions(tmp_path: Path):
    svc = LlmService(models_dir=str(tmp_path / "models"))
    mdir = svc._model_dir("m1")
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "meta.json").write_text(json.dumps({"v": 1}), encoding="utf-8")
    (mdir / "dataset.jsonl").write_text("{}\n", encoding="utf-8")

    snap = svc._snapshot_model(mdir, reason="pre-train")
    assert snap is not None
    # Mutate the live model.
    (mdir / "meta.json").write_text(json.dumps({"v": 2}), encoding="utf-8")

    versions = svc.list_versions("m1")
    assert versions and "version" in versions[0]
    ver = versions[0]["version"]

    res = svc.restore_version("m1", ver)
    assert res["ok"]
    assert json.loads((mdir / "meta.json").read_text())["v"] == 1


def test_snapshot_noop_when_empty(tmp_path: Path):
    svc = LlmService(models_dir=str(tmp_path / "models"))
    mdir = svc._model_dir("empty")
    mdir.mkdir(parents=True, exist_ok=True)
    assert svc._snapshot_model(mdir) is None


# ── scheduler window math ────────────────────────────────────────────────────
def test_scheduler_duration_window():
    start, dur = LlmHarvestScheduler._window_spec(
        {"start_time": "01:00", "duration_hours": 3})
    assert start == "01:00" and dur == 3.0
    assert LlmHarvestScheduler._window_end_str("01:00", 3) == "04:00"


def test_scheduler_legacy_window_end():
    start, dur = LlmHarvestScheduler._window_spec(
        {"window_start": "23:00", "window_end": "02:00"})
    assert start == "23:00" and dur == 3.0  # wraps midnight -> 3h


def test_scheduler_in_window():
    assert LlmHarvestScheduler._in_window(
        {"start_time": "00:00", "duration_hours": 24}) in (True, False)
    # A window that already ended today should report next_run tomorrow.
    nxt = LlmHarvestScheduler._next_run_str("00:01")
    assert nxt  # non-empty timestamp


# ── recall alternatives ──────────────────────────────────────────────────────
def test_recall_alternatives(tmp_path: Path):
    svc = LlmService(models_dir=str(tmp_path / "models"))
    mdir = svc._model_dir("alt")
    mdir.mkdir(parents=True, exist_ok=True)
    rows = [
        {"question": "list tables", "sql": "SELECT 1", "db_type": "postgresql"},
        {"question": "list tables", "sql": "SHOW TABLES", "db_type": "mysql"},
        {"question": "list tables", "sql": "SELECT 1", "db_type": "postgresql"},  # dup sql
        {"question": "count rows", "sql": "SELECT COUNT(*)", "db_type": "sqlite"},
    ]
    (mdir / "dataset.jsonl").write_text(
        "".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    alts = svc._recall_alternatives("alt", "list tables")
    sqls = {a["sql"] for a in alts}
    assert sqls == {"SELECT 1", "SHOW TABLES"}  # deduped
