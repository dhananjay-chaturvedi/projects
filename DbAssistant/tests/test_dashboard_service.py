"""Unit tests for dashboard data aggregation."""

from __future__ import annotations

from common.dashboard.service import (
    DashboardCapabilities,
    DashboardRuntime,
    DashboardService,
    _summarize_alerts,
)


def _svc(runtime=None, capabilities=None):
    return DashboardService(
        DashboardRuntime(**(runtime or {})),
        DashboardCapabilities(**(capabilities or {})),
    )


def test_collect_core_idle():
    svc = _svc(
        runtime={
            "get_active_connections": lambda: {},
            "get_saved_connections": lambda: [{"name": "a", "db_type": "MySQL"}],
        },
        capabilities={"has_schema": False, "has_ai": False, "has_monitor": False},
    )
    snap = svc.collect()
    assert snap["overall_status"] == "idle"
    assert snap["core"]["saved_connections_count"] == 1
    panel_ids = [p["id"] for p in snap["panels"]]
    assert "connections" in panel_ids
    assert "objects" in panel_ids
    assert "sql_editor" in panel_ids


def test_collect_with_active_connection():
    class FakeMgr:
        db_type = "SQLite"
        conn = object()

    svc = _svc(runtime={
        "get_active_connections": lambda: {"local": FakeMgr()},
        "get_saved_connections": lambda: [],
    })
    snap = svc.collect()
    assert snap["overall_status"] == "healthy"


def test_monitor_runtime_resources():
    svc = _svc(
        capabilities={"has_monitor": True},
        runtime={"get_monitor_runtime": lambda: {
            "os_servers_active": 1,
            "local_databases_active": 2,
            "cloud_databases_active": 1,
            "actively_monitoring": 4,
            "polling_active": True,
        }},
    )
    snap = svc.collect()
    mon = snap["monitor"]
    assert mon["actively_monitoring"] == 4
    assert mon["card_status"] == "monitoring"
    mon_panel = next(p for p in snap["panels"] if p["id"] == "monitor")
    assert mon_panel["status"] == "monitoring"


def test_monitor_session_alerts():
    svc = _svc(
        capabilities={"has_monitor": True},
        runtime={"get_monitor_runtime": lambda: {
            "recent_alerts": [
                {"severity": "WARNING", "message": "CPU high", "source": "monitor_tab"}
            ],
            "actively_monitoring": 2,
            "polling_active": True,
        }},
    )
    snap = svc.collect()
    assert snap["alert_summary"]["WARNING"] == 1
    assert snap["monitor"]["health"] == "degraded"
    mon_panel = next(p for p in snap["panels"] if p["id"] == "monitor")
    alerts_metric = next(m for m in mon_panel["metrics"] if m["label"] == "Session alerts")
    assert alerts_metric["tone"] == "warning"


def test_ai_runtime_running():
    svc = _svc(
        capabilities={"has_ai": True},
        runtime={"get_ai_runtime": lambda: {
            "installed": True,
            "tab_count": 2,
            "running_sessions": 1,
            "ui_busy": True,
            "working_on": "Tab 1: prod — show top customers",
            "sessions": [{"tab": 1, "status": "running", "connection": "prod"}],
        }},
    )
    snap = svc.collect()
    assert snap["overall_status"] == "busy"
    ai_panel = next(p for p in snap["panels"] if p["id"] == "ai")
    assert ai_panel["status"] == "running"


def test_schema_runtime_running():
    svc = _svc(
        capabilities={"has_schema": True},
        runtime={"get_schema_runtime": lambda: {
            "installed": True,
            "running": True,
            "status_text": "Converting schema…",
            "overview": "3 tables — prod → MySQL",
            "selected_tables": 3,
        }},
    )
    snap = svc.collect()
    assert snap["overall_status"] == "busy"
    schema_panel = next(p for p in snap["panels"] if p["id"] == "schema")
    assert schema_panel["status"] == "running"


def test_sql_running():
    svc = _svc(runtime={"get_sql_runtime": lambda: {
            "initialized": True,
            "query_running": True,
            "connection": "prod",
            "overview": "Executing query on prod",
        }})
    snap = svc.collect()
    assert snap["overall_status"] == "busy"
    sql_panel = next(p for p in snap["panels"] if p["id"] == "sql_editor")
    assert sql_panel["status"] == "running"


def test_not_installed_module_cards():
    svc = _svc(capabilities={"has_monitor": False, "has_ai": False, "has_schema": False})
    snap = svc.collect()
    for mid in ("monitor", "ai", "schema"):
        panel = next(p for p in snap["panels"] if p["id"] == mid)
        assert panel["installed"] is False
        assert panel["status"] == "missing"
        assert panel.get("navigate") is None
        assert panel["detail_lines"]


def test_standalone_shows_not_installed_modules():
    svc = _svc(capabilities={
        "feature_module": "monitor",
        "has_schema": False,
        "has_ai": False,
        "has_monitor": True,
    })
    snap = svc.collect()
    ids = {p["id"] for p in snap["panels"]}
    assert "monitor" in ids
    assert "ai" in ids
    assert "schema" in ids
    assert "connections" in ids
    ai_panel = next(p for p in snap["panels"] if p["id"] == "ai")
    assert ai_panel["installed"] is False


def test_alert_summary():
    alerts = [
        {"severity": "CRITICAL", "message": "cpu high"},
        {"severity": "WARNING", "message": "disk"},
    ]
    summary = _summarize_alerts(alerts)
    assert summary["CRITICAL"] == 1
    assert summary["total"] == 2
