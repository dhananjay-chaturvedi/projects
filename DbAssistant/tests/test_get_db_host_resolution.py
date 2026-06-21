"""_get_db_host resolves hosts from core and monitor-db stores."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI


def _host_ui():
    ui = SimpleNamespace(
        connection_manager=MagicMock(),
        monitor_db_connection_manager=MagicMock(),
        monitored_databases={},
    )
    ui.connection_manager.get_connection.return_value = None
    ui.monitor_db_connection_manager.get_connection.return_value = None
    meth = ServerMonitorUI._get_db_host
    ui._get_db_host = lambda name: meth(ui, name)
    return ui


def test_get_db_host_from_core_store():
    ui = _host_ui()
    ui.connection_manager.get_connection.return_value = {
        "host": "db.example.com",
    }
    assert ui._get_db_host("core_db") == "db.example.com"


def test_get_db_host_falls_back_to_monitor_db_store():
    ui = _host_ui()
    ui.monitor_db_connection_manager.get_connection.return_value = {
        "host": "remote.monitor.db",
    }
    assert ui._get_db_host("mon_only") == "remote.monitor.db"


def test_get_db_host_from_active_manager_reconnect_params():
    ui = _host_ui()
    mgr = MagicMock()
    mgr._last_connect_params = {"host": "10.1.2.3"}
    ui.monitored_databases["active"] = mgr
    assert ui._get_db_host("active") == "10.1.2.3"


def test_get_db_host_unknown_returns_empty_not_localhost():
    ui = _host_ui()
    assert ui._get_db_host("missing") == ""
