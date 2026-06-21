"""Tests for per-commit governance gate."""

from __future__ import annotations

from ai_assistant.app_builder.commit_gate import (
    CommitGate,
    restore_snapshot,
    snapshot_workspace,
)
from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode


def test_snapshot_and_restore(tmp_path):
    ws = tmp_path / "app"
    ws.mkdir()
    (ws / "src").mkdir()
    (ws / "src" / "app.py").write_text("v1", encoding="utf-8")
    snap = snapshot_workspace(ws)
    (ws / "src" / "app.py").write_text("v2", encoding="utf-8")
    (ws / "new.txt").write_text("x", encoding="utf-8")
    restore_snapshot(ws, snap)
    assert (ws / "src" / "app.py").read_text() == "v1"
    assert not (ws / "new.txt").exists()


def test_snapshot_preserves_binary_db_and_pyc(tmp_path):
    import sqlite3

    ws = tmp_path / "app"
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text("v1", encoding="utf-8")
    db_path = ws / "src" / "data.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER)")
    conn.commit()
    conn.close()
    original_db = db_path.read_bytes()
    pyc = ws / "__pycache__" / "mod.pyc"
    pyc.parent.mkdir(parents=True)
    pyc.write_bytes(b"\x00\x01binary")

    snap = snapshot_workspace(ws)
    assert "src/data.db" not in snap
    assert "__pycache__/mod.pyc" not in snap

    (ws / "src" / "app.py").write_text("v2", encoding="utf-8")
    (ws / "new.txt").write_text("x", encoding="utf-8")
    pyc.write_bytes(b"changed")

    restore_snapshot(ws, snap)
    assert (ws / "src" / "app.py").read_text(encoding="utf-8") == "v1"
    assert not (ws / "new.txt").exists()
    assert db_path.read_bytes() == original_db
    assert pyc.read_bytes() == b"changed"

    conn = sqlite3.connect(db_path)
    conn.execute("SELECT 1 FROM t")
    conn.close()


def test_commit_gate_reverts_failed_change(tmp_path):
    ws = tmp_path / "ws"
    ws.mkdir()
    engine = AiAppEngine()
    bp = AppBlueprint(
        name="bad", mode=BuildMode.FROM_SCRATCH,
        services=["ci_cd", "document", "hosting", "database"],
    )
    gate = CommitGate(engine, bp, target_coverage=0.99)

    before = snapshot_workspace(ws)
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text(
        "def bad():\n    try:\n        pass\n    except:\n        pass\n",
        encoding="utf-8",
    )
    verdict = gate.gate(ws, before)
    assert verdict.files_changed
    # Revert should restore empty workspace
    after_revert = snapshot_workspace(ws)
    assert "src/app.py" not in after_revert or after_revert == before
