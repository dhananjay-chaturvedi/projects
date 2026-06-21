"""MonitorDaemon PID-file lifecycle tests."""

from __future__ import annotations

import os
import json
from unittest.mock import MagicMock, patch

from monitoring.daemon import MonitorDaemon


def test_daemon_status_no_pid_file(tmp_path):
    pid_path = tmp_path / "missing.pid"
    r = MonitorDaemon.daemon_status(str(pid_path))
    assert isinstance(r, dict)
    assert r.get("running") is False or r.get("ok") is False


def test_stop_daemon_missing_pid(tmp_path):
    r = MonitorDaemon.stop_daemon(str(tmp_path / "nope.pid"))
    assert r["ok"] is False


def test_read_pid_from_file(tmp_path):
    pid_path = tmp_path / "d.pid"
    pid_path.write_text(str(os.getpid()))
    d = MonitorDaemon(pid_file=str(pid_path))
    assert d._read_pid() == os.getpid()


def test_interval_is_clamped_to_positive(tmp_path):
    d = MonitorDaemon(interval=0, pid_file=str(tmp_path / "d.pid"))
    assert d.interval == 1
    d2 = MonitorDaemon(interval=-99, pid_file=str(tmp_path / "d2.pid"))
    assert d2.interval == 1


def test_write_pid_is_atomic_and_private(tmp_path):
    pid_path = tmp_path / "daemon.pid"
    d = MonitorDaemon(pid_file=str(pid_path))
    d._write_pid()
    assert pid_path.read_text() == str(os.getpid())
    assert oct(pid_path.stat().st_mode & 0o777) == "0o600"
    assert not list(tmp_path.glob("daemon.pid.*.tmp"))


def test_handle_signal_without_logger_sets_stop_event(tmp_path):
    d = MonitorDaemon(pid_file=str(tmp_path / "d.pid"))
    d._handle_signal(15, None)
    assert d._stop_event.is_set()


def test_poll_once_uses_unified_connection_list_and_atomic_metrics(tmp_path):
    d = MonitorDaemon(pid_file=str(tmp_path / "d.pid"), metrics_file=str(tmp_path / "metrics.json"))
    svc = MagicMock()
    svc.list_all_connections.return_value = [
        {"name": "db1", "source": "db"},
        {"name": "cloud1", "source": "cloud"},
        {"error": "bad"},
    ]
    svc.monitor_any.side_effect = [
        {
            "error": None,
            "source": "db",
            "sections": [("s", [("m", 1)])],
            "raw_floats": {"m": 1.0},
            "timestamp": "t1",
            "alerts": [],
        },
        {
            "error": None,
            "source": "cloud",
            "sections": [("c", [("cm", 2)])],
            "raw_floats": {},
            "timestamp": "t2",
            "alerts": [],
        },
    ]
    d._svc = svc
    d._logger = MagicMock()
    d._poll_once()

    assert [c.args[0] for c in svc.monitor_any.call_args_list] == ["db1", "cloud1"]
    data = json.loads((tmp_path / "metrics.json").read_text())
    assert set(data) == {"db1", "cloud1"}
    assert not list(tmp_path.glob("metrics.json.*.tmp"))


def test_poll_once_logs_alert_before_continuing_on_notification_failure(tmp_path):
    d = MonitorDaemon(pid_file=str(tmp_path / "d.pid"), metrics_file=str(tmp_path / "metrics.json"))
    svc = MagicMock()
    svc.list_all_connections.return_value = [{"name": "db1"}]
    svc.list_connections.return_value = [{"name": "db1"}]
    svc.monitor_any.return_value = {
        "error": None,
        "source": "db",
        "sections": [("s", [("m", 1)])],
        "raw_floats": {},
        "timestamp": "t",
        "alerts": [{"severity": "CRITICAL", "message": "bad", "source": "db"}],
    }
    d._svc = svc
    d._logger = MagicMock()
    with patch(
        "common.notifications.dispatch_alert",
        return_value={"ok": False, "delivered": [], "skipped": None, "results": [{"ok": False, "message": "403"}]},
    ):
        d._poll_once()
    svc.log_alert.assert_called_once_with("CRITICAL", "bad", source="db", instance="db1")
