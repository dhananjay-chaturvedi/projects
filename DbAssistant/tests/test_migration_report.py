"""Unit tests for migration report (G10) and checkpoint store (G9)."""

from __future__ import annotations

import json
import os

from schema_converter.migration_report import CheckpointStore, MigrationReport


def test_migration_report_writes_summary(tmp_path):
    path = tmp_path / "report.json"
    report = MigrationReport(str(path), source_conn="src", target_conn="tgt")
    report.add_table(
        {
            "source_table": "a",
            "target_table": "test.a",
            "ok": True,
            "rows_transferred": 10,
            "skipped": 2,
            "error_count": 1,
            "source_count": 12,
            "target_count": 10,
        }
    )
    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["summary"]["rows_transferred"] == 10
    assert data["summary"]["rows_skipped"] == 2
    assert data["summary"]["row_errors"] == 1
    assert data["summary"]["count_mismatches"] == 1
    assert len(data["tables"]) == 1


def test_checkpoint_store_roundtrip(tmp_path):
    path = tmp_path / "cp.json"
    store = CheckpointStore(str(path))
    assert store.get("a", "test.a") == 0
    store.set("a", "test.a", 500)

    reopened = CheckpointStore(str(path))
    assert reopened.get("a", "test.a") == 500

    reopened.clear("a", "test.a")
    assert CheckpointStore(str(path)).get("a", "test.a") == 0


def test_checkpoint_default_path_is_stable():
    p1 = CheckpointStore.default_path("src", "tgt")
    p2 = CheckpointStore.default_path("src", "tgt")
    assert p1 == p2
    assert os.path.basename(p1).endswith(".json")
