"""Unit tests for continue-on-error (G3), sequence reset (G8),
column-limit lookup (G4) and the dry-run validator (G5)."""

from __future__ import annotations

import pytest

from schema_converter.converter import DataConverter


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if self.conn.fail_rows and params in self.conn.fail_rows:
            raise ValueError(f"bad row {params}")

    def executemany(self, sql, rows):
        if self.conn.fail_executemany:
            raise ValueError("executemany blew up")
        self.conn.committed_rows.extend(rows)

    def fetchone(self):
        return self.conn.fetchone_result

    def fetchall(self):
        return self.conn.fetchall_result

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.executed = []
        self.committed_rows = []
        self.fail_executemany = False
        self.fail_rows = set()
        self.commits = 0
        self.rollbacks = 0
        self.fetchone_result = None
        self.fetchall_result = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class FakeManager:
    def __init__(self, db_type="MySQL"):
        self.db_type = db_type
        self.conn = FakeConn()


def _converter(src_type="MySQL", tgt_type="MySQL"):
    return DataConverter(FakeManager(src_type), FakeManager(tgt_type))


# --------------------------------------------------------------------------- #
# G3 continue-on-error
# --------------------------------------------------------------------------- #
def test_resilient_insert_falls_back_to_per_row(monkeypatch):
    conv = _converter()
    conn = conv.target_manager.conn
    conn.fail_executemany = True
    conn.fail_rows = {(2,)}  # second row fails individually
    cursor = conn.cursor()
    errors = []
    committed = conv._insert_batch_resilient(
        cursor, conn, "INSERT", [(1,), (2,), (3,)], errors
    )
    assert committed == 2
    assert len(errors) == 1
    assert errors[0]["type"] == "row"


def test_resilient_insert_fast_path_when_executemany_ok():
    conv = _converter()
    conn = conv.target_manager.conn
    cursor = conn.cursor()
    errors = []
    committed = conv._insert_batch_resilient(
        cursor, conn, "INSERT", [(1,), (2,)], errors
    )
    assert committed == 2
    assert errors == []


# --------------------------------------------------------------------------- #
# G8 sequence reset
# --------------------------------------------------------------------------- #
def test_reset_target_sequence_mysql(monkeypatch):
    conv = _converter(tgt_type="MariaDB")
    monkeypatch.setattr(conv, "_find_autoincrement_column", lambda t: "id")
    conn = conv.target_manager.conn
    conn.fetchone_result = (42,)
    assert conv.reset_target_sequence("orders") is True
    assert any("AUTO_INCREMENT = 43" in sql for sql, _ in conn.executed)


def test_reset_target_sequence_no_autocol_returns_false(monkeypatch):
    conv = _converter(tgt_type="MySQL")
    monkeypatch.setattr(conv, "_find_autoincrement_column", lambda t: None)
    assert conv.reset_target_sequence("orders") is False


# --------------------------------------------------------------------------- #
# G4 column limits
# --------------------------------------------------------------------------- #
def test_fetch_target_column_meta_mysql():
    conv = _converter(tgt_type="MySQL")
    conn = conv.target_manager.conn
    conn.fetchall_result = [
        ("name", 50, None, None, "varchar"),
        ("amount", None, 10, 2, "decimal"),
    ]
    meta = conv._fetch_target_column_meta("orders")
    assert meta["name"]["char_max"] == 50
    assert meta["name"]["is_text"] is True
    assert meta["amount"]["num_precision"] == 10
    assert meta["amount"]["num_scale"] == 2


def test_get_target_column_limits_alignment():
    conv = _converter(tgt_type="MySQL")
    conn = conv.target_manager.conn
    conn.fetchall_result = [("name", 50, None, None, "varchar")]
    limits = conv._get_target_column_limits("orders", ["name", "missing"])
    assert limits[0]["char_max"] == 50
    assert limits[1] is None


# --------------------------------------------------------------------------- #
# G5 dry-run validation
# --------------------------------------------------------------------------- #
def test_validate_table_detects_oversize_and_default(monkeypatch):
    from schema_converter import migration_validation as mv

    src_schema = {
        "table_name": "users",
        "columns": [
            {"name": "id", "type": "INT", "nullable": False, "default": None},
            {"name": "bio", "type": "VARCHAR(500)", "nullable": True, "default": None},
            {"name": "created", "type": "TIMESTAMP", "nullable": True,
             "default": "CURRENT_TIMESTAMP"},
        ],
        "primary_key": ["id"],
    }

    monkeypatch.setattr(
        "schema_converter.converter.SchemaConverter.get_table_schema",
        lambda self, t: src_schema,
    )
    monkeypatch.setattr(
        "schema_converter.converter.SchemaConverter.convert_schema",
        lambda self, s, **kw: s,
    )
    monkeypatch.setattr(
        "schema_converter.converter.DataConverter._fetch_target_column_meta",
        lambda self, t: {
            "id": {"char_max": None, "num_precision": 10, "num_scale": 0, "is_text": False},
            "bio": {"char_max": 100, "num_precision": None, "num_scale": 0, "is_text": True},
            "created": {"char_max": None, "num_precision": None, "num_scale": 0, "is_text": False},
        },
    )

    result = mv.validate_table(
        FakeManager("PostgreSQL"), FakeManager("MySQL"), "users", "test.users"
    )
    cats = {i["category"] for i in result["issues"]}
    assert "oversized_column" in cats
    assert "unsupported_default" in cats
    assert result["ok"] is False  # oversize is an error


def test_validate_migration_summary(monkeypatch):
    from schema_converter import migration_validation as mv

    monkeypatch.setattr(
        mv,
        "validate_table",
        lambda *a, **k: {
            "source_table": "t",
            "target_table": "test.t",
            "target_exists": True,
            "ok": True,
            "issues": [{"severity": "warning", "category": "x", "column": "", "message": "m"}],
        },
    )
    report = mv.validate_migration(
        FakeManager(), FakeManager(), [("t", "test.t")]
    )
    assert report["ok"] is True
    assert report["summary"]["warnings"] == 1
    assert report["summary"]["errors"] == 0
