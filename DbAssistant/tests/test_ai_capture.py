"""Tests for ai_assistant.capture — isolation, meters gating, export."""

from __future__ import annotations

from pathlib import Path

from ai_assistant.capture.identity import capture_scope, resolve_project_id
from ai_assistant.capture.pipeline import CapturePipeline, CaptureTurn
from ai_assistant.capture.store import IsolatedCaptureStore


class _FakeMgr:
    db_type = "MariaDB"
    host = "localhost"
    database = "test"


def test_project_id_explicit_vs_derived():
    assert resolve_project_id(explicit="My Project") == "My_Project"
    a = resolve_project_id(connection_name="local_mariadb", host="localhost", database="test")
    b = resolve_project_id(connection_name="local_mariadb", host="localhost", database="test")
    assert a == b
    assert a != resolve_project_id(connection_name="other", host="localhost", database="test")


def test_capture_scope():
    pid, conn, db = capture_scope(
        project_id="acme", connection_name="local_mariadb", db_manager=_FakeMgr()
    )
    assert pid == "acme"
    assert conn == "local_mariadb"
    assert db == "test"


def test_isolated_store_append_and_export(tmp_path: Path):
    store = IsolatedCaptureStore(tmp_path)
    pipe = CapturePipeline(store=store)
    parsed = {"summary_sql": "SELECT COUNT(*) FROM customers", "explanation": "count"}
    context = {"schema": {"table_schemas": {"customers": {"columns": [{"name": "customer_id"}]}}}}
    rec = pipe.record_turn(
        CaptureTurn(
            question="how many customers?",
            prompt="USER QUESTION: how many customers?\nschema...",
            raw_response="SUMMARY_SQL: SELECT COUNT(*) FROM customers",
            parsed=parsed,
            context=context,
            connection_name="local_mariadb",
            db_manager=_FakeMgr(),
            backend="cursor",
            execution={"ok": True, "rowcount": 1},
        ),
    )
    assert rec is not None
    assert rec.quality_accepted
    stats = store.stats(rec.project_id)
    assert stats["total"] == 1
    out = tmp_path / "train.jsonl"
    n = store.export_training_jsonl(rec.project_id, out, accepted_only=True)
    assert n == 1
    assert "instruction" in out.read_text()


def test_projects_isolated(tmp_path: Path):
    store = IsolatedCaptureStore(tmp_path)
    pipe = CapturePipeline(store=store)
    ctx = {"schema": {"table_schemas": {"t": {"columns": [{"name": "id"}]}}}}
    for proj, q, sql in (
        ("proj_a", "count a", "SELECT COUNT(*) FROM t"),
        ("proj_b", "count b", "SELECT COUNT(*) FROM t"),
    ):
        pipe.record_turn(
            CaptureTurn(
                question=q, prompt=q, raw_response=sql,
                parsed={"summary_sql": sql}, context=ctx,
                connection_name="c1", db_manager=_FakeMgr(),
                project_id=proj, execution={"ok": True, "rowcount": 1},
            ),
        )
    assert store.stats("proj_a")["total"] == 1
    assert store.stats("proj_b")["total"] == 1
