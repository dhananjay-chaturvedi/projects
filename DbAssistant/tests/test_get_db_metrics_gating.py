"""get_db_metrics liveness gating integration-style unit tests."""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI


def _metrics_ui():
    ui = SimpleNamespace(
        refresh_interval=5000,
        _db_metric_skip_ping_if_used_within=0,
        _db_last_metric_at={},
        _db_sections_cache={},
        _db_os_note_cache={},
        _threshold_checker=None,
        _db_locks={},
        _db_locks_meta=__import__("threading").Lock(),
        connection_manager=MagicMock(),
    )
    ui.connection_manager.get_connection.return_value = {"host": "localhost"}
    cls = ServerMonitorUI
    for name in (
        "_get_db_lock",
        "_get_db_host",
        "_should_skip_liveness",
        "_liveness_window_seconds",
        "get_db_metrics",
    ):
        setattr(ui, name, getattr(cls, name).__get__(ui, cls))
    return ui


def test_ping_skipped_when_recent_sql_ok():
    ui = _metrics_ui()
    db = MagicMock()
    db.db_type = "MySQL"
    db.ping_or_reconnect = MagicMock()
    db.execute_query = MagicMock(return_value=({"rows": [[1]]}, None))
    ui._db_last_metric_at["db1"] = time.time()

    def _collect(mgr, **kw):
        mgr.execute_query("SELECT 1")
        return ([], {"Active Connections": 1.0}, "")

    with patch("monitoring.server_monitor.server_monitor_ui._collect_db_metrics", side_effect=_collect):
        out = ui.get_db_metrics(db, db_name="db1")
    db.ping_or_reconnect.assert_not_called()
    assert out is not None


def test_ping_runs_when_no_recent_ok():
    ui = _metrics_ui()
    db = MagicMock()
    db.db_type = "MySQL"
    db.ping_or_reconnect = MagicMock(return_value=True)

    def _collect(mgr, **kw):
        mgr.execute_query("SELECT 1")
        return ([], {"x": 1.0}, "")

    with patch("monitoring.server_monitor.server_monitor_ui._collect_db_metrics", side_effect=_collect):
        ui.get_db_metrics(db, db_name="db1")
    db.ping_or_reconnect.assert_called_once()


def test_timestamp_cleared_when_no_sql_ok():
    ui = _metrics_ui()
    db = MagicMock()
    db.db_type = "MySQL"
    db.ping_or_reconnect = MagicMock(return_value=True)
    ui._db_last_metric_at["db1"] = time.time() - 9999

    with patch("monitoring.server_monitor.server_monitor_ui._collect_db_metrics") as coll:
        coll.return_value = ([], {}, "")  # no numeric from SQL
        db.execute_query = MagicMock(return_value=(None, "fail"))
        ui.get_db_metrics(db, db_name="db1")
    assert "db1" not in ui._db_last_metric_at
