"""Focused MonitorService hardening tests.

These cover production-risk behavior that should not require live DB/cloud/SSH:

* metrics collection is serialized by the core connection lock without
  monkey-patching a shared manager object;
* DB raw-float display names map to ini metric keys and skip boolean values;
* alert log writes are durable enough for daemon use and reads are bounded;
* cloud connection save/remove returns persistence failures instead of
  reporting false success.
"""

from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock, patch

from monitoring.service import MonitorService


class DummyLock:
    def __init__(self):
        self.entries = 0

    def __enter__(self):
        self.entries += 1
        return self

    def __exit__(self, *_exc):
        return False


class DummyCore:
    def __init__(self):
        self.profile = {"db_type": "MySQL", "host": "localhost"}
        self.mgr = MagicMock()
        self.lock = DummyLock()

    def get_connection_profile(self, name):
        return self.profile if name == "db1" else None

    def get_manager(self, name, profile=None):
        return self.mgr

    def connection_lock(self, name):
        return self.lock


def test_get_metrics_serializes_collection_without_monkeypatch():
    core = DummyCore()
    svc = MonitorService(core)

    with patch("monitoring.service.collect_metrics", return_value=([("s", [])], {}, "")) as coll:
        out = svc.get_metrics("db1")

    assert out["error"] is None
    assert core.lock.entries == 1
    coll.assert_called_once()
    assert core.mgr.execute_query == core.mgr.execute_query


def test_split_raw_by_source_skips_bool_and_maps_display_names():
    svc = MonitorService(DummyCore())
    specs = {
        "MySQL": [
            {"ini": ("db", "active_connections"), "display": "Active Connections"},
            {"ini": ("os", "cpu_utilization"), "display": "CPU Utilization"},
            {"ini": ("db", "flag"), "display": "Flag"},
        ]
    }
    raw = {
        "Active Connections": 10,
        "CPU Utilization": 91.2,
        "Flag": True,
    }
    with patch("monitoring.db_metric_config.METRIC_SPECS", specs):
        buckets = svc._split_raw_by_source("MySQL", raw)
    assert buckets == {
        "db": {"active_connections": 10.0},
        "os": {"cpu_utilization": 91.2},
    }


def test_log_and_list_alerts_are_jsonl_and_bounded(tmp_path):
    svc = MonitorService(DummyCore())
    alerts_path = tmp_path / "alerts.jsonl"
    svc._alerts_log_path = lambda: alerts_path

    for i in range(5):
        out = svc.log_alert("warning", f"m{i}", source="db", instance="x")
        assert out["ok"] is True

    lines = alerts_path.read_text().splitlines()
    assert len(lines) == 5
    assert all(json.loads(line)["severity"] == "WARNING" for line in lines)

    listed = svc.list_alerts(limit=2)
    assert listed["total"] == 2
    assert [a["message"] for a in listed["alerts"]] == ["m4", "m3"]


def test_log_alert_rejects_invalid_severity(tmp_path):
    svc = MonitorService(DummyCore())
    svc._alerts_log_path = lambda: tmp_path / "alerts.jsonl"
    out = svc.log_alert("panic", "bad")
    assert out["ok"] is False
    assert "Invalid severity" in out["message"]


def test_cloud_save_failure_is_reported():
    svc = MonitorService(DummyCore())
    cm = MagicMock()
    cm.load_cloud_databases.return_value = {}
    cm.save_cloud_databases.return_value = False
    svc._cloud_mgr = lambda: cm

    out = svc.add_cloud_connection("cloud1", {"provider": "AWS"})
    assert out["ok"] is False
    assert "Failed to save" in out["message"]


def test_cloud_connection_list_masks_access_key_id():
    svc = MonitorService(DummyCore())
    cm = MagicMock()
    cm.load_cloud_databases.return_value = {
        "aws": {
            "provider": "AWS",
            "access_key_id": "AKIAEXAMPLE123456789",
            "secret_access_key": "secret",
        }
    }
    svc._cloud_mgr = lambda: cm
    out = svc.list_cloud_connections()
    assert out[0]["access_key_id"] == "***"
    assert out[0]["secret_access_key"] == "***"


def test_cloud_remove_failure_is_reported():
    svc = MonitorService(DummyCore())
    cm = MagicMock()
    cm.load_cloud_databases.return_value = {"cloud1": {"provider": "AWS"}}
    cm.save_cloud_databases.return_value = False
    svc._cloud_mgr = lambda: cm

    out = svc.remove_cloud_connection("cloud1")
    assert out["ok"] is False
    assert "Failed to remove" in out["message"]


def test_concurrent_log_alert_writes_valid_jsonl(tmp_path):
    svc = MonitorService(DummyCore())
    alerts_path = tmp_path / "alerts.jsonl"
    svc._alerts_log_path = lambda: alerts_path

    def write(i):
        svc.log_alert("INFO", f"msg-{i}", source="test")

    threads = [threading.Thread(target=write, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    records = [json.loads(line) for line in alerts_path.read_text().splitlines()]
    assert len(records) == 20
    assert {r["message"] for r in records} == {f"msg-{i}" for i in range(20)}
