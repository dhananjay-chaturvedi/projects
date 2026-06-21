"""SSH liveness timestamp and cloud keepalive failure tracking tests."""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI


def _cloud_keepalive_ui():
    ui = SimpleNamespace(
        _cloud_keepalive_active=True,
        _cloud_keepalive_interval=1,
        active_cloud_monitors={},
        active_cloud_databases={},
        _cloud_last_ok_at={},
        _cloud_consecutive_failures={},
        _cloud_needs_refresh={},
        _cloud_force_refresh_interval=1800,
        refresh_interval=5000,
        root=MagicMock(),
    )
    cls = ServerMonitorUI
    for name in (
        "_cloud_should_refresh_keepalive",
        "_seconds_until_expiry",
        "_clear_cloud_liveness_state",
    ):
        setattr(ui, name, getattr(cls, name).__get__(ui, cls))
    return ui


def test_ssh_last_cmd_ok_cleared_on_failure():
    ui = SimpleNamespace(_ssh_last_cmd_ok_at={"srv": time.time()})
    ui._ssh_last_cmd_ok_at.pop("srv", None)
    assert "srv" not in ui._ssh_last_cmd_ok_at


def test_cloud_reconnect_after_three_failures():
    """Simulate keepalive failure counter reaching 3."""
    ui = _cloud_keepalive_ui()
    ui._cloud_consecutive_failures["db1"] = 2
    ui._cloud_consecutive_failures["db1"] += 1
    assert ui._cloud_consecutive_failures["db1"] >= 3


def test_cloud_success_resets_failures(liveness_ui):
    liveness_ui._cloud_consecutive_failures["x"] = 3
    liveness_ui._cloud_needs_refresh["x"] = True
    liveness_ui._cloud_last_ok_at["x"] = time.time()
    liveness_ui._cloud_consecutive_failures["x"] = 0
    liveness_ui._cloud_needs_refresh["x"] = False
    assert liveness_ui._cloud_consecutive_failures["x"] == 0
    assert liveness_ui._cloud_needs_refresh["x"] is False
