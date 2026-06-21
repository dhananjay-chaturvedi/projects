"""Non-Tk ServerMonitorUI helper tests (liveness + SSH path builder)."""

from __future__ import annotations

import os
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI


def _bind_methods(ui, names):
    cls = ServerMonitorUI
    for name in names:
        setattr(ui, name, getattr(cls, name).__get__(ui, cls))


@pytest.fixture
def ui_shell():
    ui = SimpleNamespace(
        refresh_interval=5000,
        connection_manager=MagicMock(),
        _db_metric_skip_ping_if_used_within=0,
        _cloud_health_skip_if_used_within=0,
        _ssh_keepalive_skip_if_used_within=0,
        _cloud_force_refresh_interval=1800,
        _db_last_metric_at={},
        _ssh_last_cmd_ok_at={},
        _cloud_last_ok_at={},
        _cloud_consecutive_failures={},
        _cloud_needs_refresh={},
        _db_sections_cache={},
        _db_os_note_cache={},
        _threshold_checker=None,
        ssh_test_timeout=5,
        ssh_control_persist=600,
        monitored_databases={},
        active_connections={},
        _db_locks={},
        _db_locks_meta=__import__("threading").Lock(),
    )
    _bind_methods(
        ui,
        [
            "_get_db_host",
            "_get_db_lock",
            "_liveness_window_seconds",
            "_should_skip_liveness",
            "_seconds_until_expiry",
            "_cloud_should_refresh_keepalive",
            "_clear_cloud_liveness_state",
            "_build_ssh_master_command",
            "_fetch_cloud_metrics",
            "get_db_metrics",
            "_fire_alerts",
        ],
    )
    ui._format_metric_block = lambda **kwargs: ServerMonitorUI._format_metric_block(
        **kwargs
    )
    ui.connection_manager.get_connection.return_value = {"host": "db.example.com"}
    return ui


class TestGetDbHost:
    def test_returns_host(self, ui_shell):
        assert ui_shell._get_db_host("mydb") == "db.example.com"

    def test_missing_connection(self, ui_shell):
        ui_shell.connection_manager.get_connection.side_effect = Exception("nope")
        assert ui_shell._get_db_host("x") == ""


class TestBuildSshMasterCommand:
    def test_includes_control_path(self, ui_shell):
        conn = {"username": "u", "host": "h"}
        cmd = ui_shell._build_ssh_master_command(
            conn, "/tmp/ssh_test", "u@h"
        )
        assert "-M" in cmd
        assert "ControlPath=/tmp/ssh_test" in " ".join(cmd)


class TestFetchCloudMetricsGating:
    def test_skips_check_health_when_recent_ok(self, ui_shell):
        ui_shell._cloud_last_ok_at["cloud1"] = time.time()
        monitor = MagicMock()
        monitor.check_health = MagicMock(return_value=[])

        with patch(
            "monitoring.server_monitor.server_monitor_ui.CloudProviderRegistry.fetch_metrics",
            return_value=([("Perf", [("CPU", "10")])], {"cpu": 10.0}, []),
        ):
            text, graph = ui_shell._fetch_cloud_metrics(
                "cloud1", {"provider": "AWS"}, monitor
            )
        monitor.check_health.assert_not_called()
        assert "OK" in text
        assert graph.get("cpu") == 10.0

    def test_failure_clears_last_ok(self, ui_shell):
        ui_shell._cloud_last_ok_at["cloud1"] = time.time() - 9999
        monitor = MagicMock()
        monitor.check_health.return_value = []

        with patch(
            "monitoring.server_monitor.server_monitor_ui.CloudProviderRegistry.fetch_metrics",
            return_value=([], {}, []),
        ):
            ui_shell._fetch_cloud_metrics(
                "cloud1", {"provider": "AWS"}, monitor
            )
        assert "cloud1" not in ui_shell._cloud_last_ok_at
        assert ui_shell._cloud_needs_refresh.get("cloud1") is True
        assert ui_shell._cloud_consecutive_failures.get("cloud1", 0) >= 1
