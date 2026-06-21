"""Regression tests for error-boundary hardening (audit follow-ups)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def tmp_conn_manager(tmp_path, monkeypatch):
    import common.connection_manager as cm
    from common import paths as _paths

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()
    return cm.ConnectionManager()


def _conn_params(name, **over):
    from common.connection_params import ConnectionParams

    values = {
        "name": name,
        "db_type": "MySQL",
        "host": "h",
        "port": 3306,
        "service_or_db": "d",
        "username": "u",
        "password": "p",
    }
    values.update(over)
    return ConnectionParams.from_mapping(values)


class TestConnectionManagerRollback:
    def test_add_rolls_back_in_memory_on_save_failure(self, tmp_conn_manager, monkeypatch):
        params = _conn_params("rollback-test")
        monkeypatch.setattr(tmp_conn_manager, "save_connections", lambda: False)
        ok, msg = tmp_conn_manager.add_connection(params)
        assert ok is False
        assert "failed" in msg.lower()
        assert tmp_conn_manager.connection_exists("rollback-test") is False

    def test_update_rolls_back_in_memory_on_save_failure(self, tmp_conn_manager, monkeypatch):
        params = _conn_params("orig", host="h1")
        tmp_conn_manager.add_connection(params)
        monkeypatch.setattr(tmp_conn_manager, "save_connections", lambda: False)
        ok, _ = tmp_conn_manager.update_connection(
            "orig",
            _conn_params("orig", host="h2"),
        )
        assert ok is False
        assert tmp_conn_manager.get_connection("orig")["host"] == "h1"

    def test_delete_rolls_back_in_memory_on_save_failure(self, tmp_conn_manager, monkeypatch):
        tmp_conn_manager.add_connection(_conn_params("keep-me"))
        monkeypatch.setattr(tmp_conn_manager, "save_connections", lambda: False)
        ok, _ = tmp_conn_manager.delete_connection("keep-me")
        assert ok is False
        assert tmp_conn_manager.connection_exists("keep-me") is True


def test_apply_ddl_surfaces_ok_and_partial_flags():
    from schema_converter.bridge import SchemaBridge

    calls = {"n": 0}

    class Core:
        def _split_sql_statements(self, ddl):
            return [s.strip() for s in ddl.split(";") if s.strip()]

        def execute(self, _conn, _sql):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"error": None}
            return {"error": "boom"}

    result = SchemaBridge(Core()).apply_ddl_to_target(
        "tgt",
        "CREATE TABLE a (id int); CREATE TABLE b (id int);",
    )
    assert result["executed"] == 1
    assert result["failed"] == 1
    assert result["ok"] is False
    assert result["partial"] is True


def test_transfer_data_reports_partial_rows_on_failure(monkeypatch):
    from schema_converter.bridge import SchemaBridge
    from schema_converter.transfer_options import TransferRequest

    def fake_transfer(_src, _tgt, _table, _target_table, **kw):
        runtime = kw.get("runtime")
        if runtime and runtime.stats_out is not None:
            runtime.stats_out.update(
                {"rows_transferred": 42, "skipped": 1, "error_count": 2}
            )
        raise RuntimeError("mid-transfer")

    monkeypatch.setattr("schema_converter.adapters.transfer_object", fake_transfer)
    bridge = SchemaBridge(MagicMock(get_manager=lambda _n: MagicMock(db_type="PostgreSQL")))
    result = bridge.transfer_data(TransferRequest("src", "tgt", "users"))
    assert result["ok"] is False
    assert result["rows_transferred"] == 42
    assert result["partial"] is True
    assert result["skipped"] == 1
    assert result["errors"] == 2


def test_standalone_runner_returns_one_on_launch_error(monkeypatch):
    from common.core import standalone_runner as sr

    def boom(_key):
        raise RuntimeError("ui launch failed")

    monkeypatch.setattr(sr, "launch_module_ui", boom)
    code = sr.run_standalone_module(
        module_key="migrator",
        prog="migrator",
        description="test",
        register_cli=lambda _sub: None,
        dispatch_cli=lambda _args: 0,
        module_commands=set(),
        argv=["--ui"],
    )
    assert code == 1


def test_orchestrator_write_is_atomic_per_file(tmp_path):
    from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator

    orch = AppBuildOrchestrator.__new__(AppBuildOrchestrator)
    target = tmp_path / "src" / "app.py"
    target.parent.mkdir(parents=True)
    target.write_text("ORIGINAL", encoding="utf-8")

    orch._write(tmp_path, {"src/app.py": "NEW CONTENT"})
    assert target.read_text(encoding="utf-8") == "NEW CONTENT"
    # No leftover temp staging files.
    assert not list(target.parent.glob(".app.py.*.tmp"))


def test_orchestrator_write_leaves_original_on_failure(tmp_path, monkeypatch):
    import os

    from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator

    orch = AppBuildOrchestrator.__new__(AppBuildOrchestrator)
    target = tmp_path / "app.py"
    target.write_text("ORIGINAL", encoding="utf-8")

    real_replace = os.replace

    def boom(src, dst):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        orch._write(tmp_path, {"app.py": "NEW"})
    monkeypatch.setattr(os, "replace", real_replace)
    # Original untouched, no temp leftovers.
    assert target.read_text(encoding="utf-8") == "ORIGINAL"
    assert not list(tmp_path.glob(".app.py.*.tmp"))


def test_core_db_service_execute_normalizes_driver_exception(monkeypatch):
    from common.headless.db_service import CoreDBService

    svc = CoreDBService()
    mgr = MagicMock()
    mgr.execute_query.side_effect = RuntimeError("driver blew up")
    monkeypatch.setattr(svc, "_get_or_connect", lambda _name: mgr)
    monkeypatch.setattr(svc, "_lock", lambda _name: MagicMock(__enter__=lambda s: s, __exit__=lambda *a: None))

    result = svc.execute("dev", "SELECT 1")
    assert result["error"] == "driver blew up"
    assert result["rows"] == []
    assert result["rowcount"] == 0
