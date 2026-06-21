"""Monitoring parity tests.

The Tk Monitoring tab is the reference: a status bar (Monitor Settings / Alert
Thresholds) over three sections — Server / Database / Cloud — each with its own
Add / Select / Remove target controls and a shared metrics view toolbar
(Show Graphs / Show Text / Clear Graphs / Refresh / Alerts).

These tests assert the shared spec shape, that each UI builds that layout from the
spec (not hardcoded literals), and that the wiring behaves the same way.
"""

from __future__ import annotations

import inspect

import pytest

from common.ui.shared import specs


def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


# --------------------------------------------------------------------------- #
# Shared spec (single source of truth)
# --------------------------------------------------------------------------- #
def test_shared_monitoring_payload_shape():
    p = specs.monitoring_payload()

    assert [a["id"] for a in p["topActions"]] == ["settings", "thresholds_settings"]

    assert [s["id"] for s in p["sections"]] == ["server", "database", "cloud"]
    titles = {s["id"]: s["title"] for s in p["sections"]}
    assert titles == {
        "server": "Server monitoring",
        "database": "Database Monitoring",
        "cloud": "Cloud Resource Monitoring",
    }
    metrics_titles = {s["id"]: s["metricsTitle"] for s in p["sections"]}
    assert metrics_titles == {
        "server": "OS metrics",
        "database": "Database Metrics",
        "cloud": "Cloud Resource Metrics",
    }
    for sec in p["sections"]:
        assert [a["id"] for a in sec["targetActions"]] == ["add", "select", "remove"]

    # Per-section Add labels match the Tk reference.
    server = specs.monitoring_section("server")
    assert server["targetActions"][0]["label"] == "Add Connection"
    assert specs.monitoring_section("database")["targetActions"][0]["label"] == "Add Database"
    assert specs.monitoring_section("cloud")["targetActions"][0]["label"] == "Add Cloud Resource"

    assert [a["id"] for a in p["viewActions"]] == [
        "show_graphs", "show_text", "clear_graphs", "refresh", "alerts"]
    assert [a["id"] for a in p["thresholdActions"]] == [
        "load", "edit", "check", "clear_alerts"]


def test_routes_registered_and_offline_endpoints():
    c = _client()
    paths = {r.path for r in c.app.routes}
    for p in ("/api/metrics/{connection}", "/api/os/metrics",
              "/api/thresholds", "/api/alerts", "/api/monitor/connections"):
        assert p in paths, p
    assert c.get("/api/thresholds?all=true").status_code == 200
    r = c.get("/api/os/metrics")
    assert r.status_code == 200 and "metrics" in r.json()


# --------------------------------------------------------------------------- #
# Tk reference — builds the three sections from the shared spec
# --------------------------------------------------------------------------- #
def test_tk_monitoring_builds_sections_from_shared_spec():
    from common.ui.tk.monitor.server_monitor.server_monitor_ui import ServerMonitorUI

    src = inspect.getsource(ServerMonitorUI.create_ui)
    # Sources its labels/section titles from the shared spec.
    assert "shared_specs.monitoring_payload()" in src
    assert 'section_specs["server"]["title"]' in src
    assert 'section_specs["database"]["title"]' in src
    assert 'section_specs["cloud"]["title"]' in src
    assert 'section_specs["server"]["metricsTitle"]' in src
    # Target actions + view toolbar come from spec helpers, not literals.
    assert '_ta("server", "add")' in src
    assert '_ta("database", "remove")' in src
    assert '_ta("cloud", "select")' in src
    assert 'view_labels["show_graphs"]' in src
    assert 'view_labels["alerts"]' in src
    assert 'top_labels["settings"]' in src
    # The old hardcoded titles are gone (proving the refactor took hold).
    assert '"Server monitoring"' not in src
    assert '"Add Connection"' not in src
    assert 'text="📊 Show Graphs"' not in src


# --------------------------------------------------------------------------- #
# Web — three-section layout, /ui/config spec, app.js wiring
# --------------------------------------------------------------------------- #
def _web_client():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend
    return TestClient(build_web_backend())


def test_web_monitoring_has_three_lists_and_controls():
    html = _web_client().get("/").text
    # Top settings + global controls.
    for token in ("mon-settings", "mon-threshold-settings", "mon-refresh-all",
                  "mon-auto-refresh", "mon-thresholds", "mon-alerts"):
        assert token in html, token
    # Each Tk section owns a saved-targets LIST, a metrics GRID, and
    # Add / Select / Remove / Refresh controls.
    for sid in ("server", "database", "cloud"):
        assert f'id="mon-sec-{sid}"' in html, sid
        assert f'data-section="{sid}"' in html, sid
        assert f'id="mon-{sid}-list"' in html, sid
        assert f'id="mon-{sid}-grid"' in html, sid
        assert f'id="mon-{sid}-select"' in html, sid
        assert f'id="mon-{sid}-remove"' in html, sid
        assert f'id="mon-{sid}-refresh"' in html, sid
    for token in ("mon-add-ssh", "mon-add-db", "mon-add-cloud", "mon-server-localos"):
        assert token in html, token


def test_web_ui_config_exposes_monitoring_spec():
    cfg = _web_client().get("/ui/config").json()
    mon = cfg["specs"]["monitoring"]
    assert mon == specs.monitoring_payload()
    assert [s["id"] for s in mon["sections"]] == ["server", "database", "cloud"]


def test_web_monitoring_appjs_applies_labels_and_wires_lists():
    js = _web_client().get("/ui/app.js").text
    assert "applyMonitoringLabels" in js
    assert "specs.monitoring" in js
    assert "SHARED_MON_SECTION_TO_FIELDSET" in js
    assert "SHARED_MON_TARGET_TO_DOM" in js
    # Three-list concurrent model: per-category lists + active sets + a single
    # concurrent poll across all sections.
    assert "loadMonSaved" in js
    assert "monStartMonitoring" in js
    assert "monStopOrDelete" in js
    assert "refreshMonNow" in js
    assert "Promise.all" in js  # active targets are polled concurrently
    assert "monActive" in js
    for token in ("mon-server-select", "mon-database-select", "mon-cloud-select",
                  "mon-server-remove", "mon-database-remove", "mon-cloud-remove"):
        assert token in js, token
    assert 'api.get("/api/monitor/config")' in js


# --------------------------------------------------------------------------- #
# TUI — flatten helper, three-section layout, behavioural wiring
# --------------------------------------------------------------------------- #
def test_flatten_sections_list_shape():
    """The list-of-pairs section shape flattens to (section, metric, value)."""
    from common.ui.textual.screens.monitoring import MonitoringScreen

    sections = [
        ["Connections", [["Active Connections", "1"], ["Max Connections", "151"]]],
        ["Memory", [["Buffer Pool Used", "18.8 MB"]]],
    ]
    rows = MonitoringScreen._flatten_sections(sections)
    assert ("Connections", "Active Connections", "1") in rows
    assert ("Memory", "Buffer Pool Used", "18.8 MB") in rows
    assert len(rows) == 3


class _FakeMonSvc:
    """Minimal monitor-capable service for exercising the screen handlers."""

    def __init__(self):
        self.calls = {}
        self.polled: list[str] = []

    def list_connections(self):
        return []

    def list_all_connections(self, source="all"):
        return [
            {"name": "web1", "source": "monitor", "kind": "vm", "host": "10.0.0.1"},
            {"name": "salesdb", "source": "monitor-db", "kind": "MariaDB", "host": "db1"},
            {"name": "rds-prod", "source": "cloud", "kind": "AWS", "host": "aws"},
        ]

    def monitor_any(self, name, disk_path="/"):
        self.polled.append(name)
        return {"source": "db", "sections": [["Connections", [["Active", "5"]]]],
                "raw_floats": {}, "alerts": [], "timestamp": "now"}

    def get_os_metrics(self, disk_path="/"):
        return {"error": None, "metrics": {"cpu_percent": 11.0, "memory_percent": 42.0}}

    def add_monitor_connection(self, name, host, username, password="", target_type="vm"):
        self.calls["add_ssh"] = {"name": name, "host": host, "username": username,
                                 "target_type": target_type}
        return {"ok": True, "message": "added"}

    def remove_monitor_db_connection(self, name):
        self.calls["remove_db"] = name
        return {"ok": True, "message": "removed"}

    def remove_monitor_connection(self, name):
        self.calls["remove_server"] = name
        return {"ok": True, "message": "removed"}


@pytest.mark.anyio
async def test_tui_monitoring_mirrors_tk_three_lists():
    """The TUI builds three Collapsible sections, each with a saved list + metrics."""
    from textual.widgets import Button, Collapsible, DataTable, OptionList

    from common.ui.textual.app import DbToolApp

    p = specs.monitoring_payload()
    sections = {s["id"]: s for s in p["sections"]}

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("monitor")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeMonSvc()
        scr._reload_saved()
        await pilot.pause()

        for cat in ("server", "database", "cloud"):
            coll = scr.query_one(f"#mon-sec-{cat}", Collapsible)
            assert str(coll.title) == sections[cat]["title"]
            # Each section has its own saved-targets list + metrics table.
            ol = scr.query_one(f"#mon-{cat}-list", OptionList)
            assert ol.option_count == 1  # one saved target per category from the fake
            scr.query_one(f"#mon-{cat}-metrics", DataTable)

            def _label(btn_id):
                return str(scr.query_one(btn_id, Button).label)

            assert _label(f"#mon-{cat}-add") == sections[cat]["targetActions"][0]["label"]
            assert _label(f"#mon-{cat}-select") == sections[cat]["targetActions"][1]["label"]
            assert _label(f"#mon-{cat}-remove") == sections[cat]["targetActions"][2]["label"]


@pytest.mark.anyio
async def test_tui_monitoring_select_starts_concurrent_monitoring():
    """Select adds a target to the section's active set; refresh polls all at once."""
    from textual.widgets import Button, DataTable, OptionList

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("monitor")
        await pilot.pause()
        scr = app.screen
        fake = _FakeMonSvc()
        scr.svc = fake
        scr._reload_saved()
        await pilot.pause()

        # Start monitoring a server AND a database concurrently.
        scr.query_one("#mon-server-list", OptionList).highlighted = 0
        scr.query_one("#mon-server-select", Button).press()
        scr.query_one("#mon-database-list", OptionList).highlighted = 0
        scr.query_one("#mon-database-select", Button).press()
        await pilot.pause()
        await scr._refresh_now()
        await pilot.pause()

        assert "web1" in scr._active["server"]
        assert "salesdb" in scr._active["database"]
        # Both active targets were polled in the same sweep.
        assert "web1" in fake.polled and "salesdb" in fake.polled
        # Each section's metrics table shows its own target's rows.
        assert scr.query_one("#mon-server-metrics", DataTable).row_count >= 1
        assert scr.query_one("#mon-database-metrics", DataTable).row_count >= 1


@pytest.mark.anyio
async def test_tui_monitoring_add_ssh_opens_form_and_calls_service():
    """Add Connection opens the modal; submitting calls add_monitor_connection."""
    from textual.widgets import Button, Input

    from common.ui.textual.app import DbToolApp
    from common.ui.textual.screens.form_modal import FormModal

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("monitor")
        await pilot.pause()
        scr = app.screen
        fake = _FakeMonSvc()
        scr.svc = fake

        scr.query_one("#mon-server-add", Button).press()
        await pilot.pause()
        assert isinstance(app.screen, FormModal)

        app.screen.query_one("#field-name", Input).value = "tui-host"
        app.screen.query_one("#field-host", Input).value = "10.0.0.9"
        app.screen.query_one("#field-username", Input).value = "ec2-user"
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()

        assert fake.calls.get("add_ssh", {}).get("name") == "tui-host"


@pytest.mark.anyio
async def test_tui_monitoring_remove_stops_then_deletes_by_source():
    """Remove stops an active target first; a second Remove deletes the saved one."""
    from textual.widgets import Button, OptionList

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("monitor")
        await pilot.pause()
        scr = app.screen
        fake = _FakeMonSvc()
        scr.svc = fake
        scr._reload_saved()
        await pilot.pause()

        scr.query_one("#mon-database-list", OptionList).highlighted = 0
        scr.query_one("#mon-database-select", Button).press()
        await pilot.pause()
        assert "salesdb" in scr._active["database"]

        # First Remove → stops monitoring (no delete call yet).
        scr.query_one("#mon-database-remove", Button).press()
        await pilot.pause()
        assert "salesdb" not in scr._active["database"]
        assert "remove_db" not in fake.calls

        # Second Remove on the now-idle saved target → source-aware delete.
        scr.query_one("#mon-database-list", OptionList).highlighted = 0
        scr.query_one("#mon-database-remove", Button).press()
        await pilot.pause()
        assert fake.calls.get("remove_db") == "salesdb"
