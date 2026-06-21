"""Verify headless stack works when optional UI packages are absent."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch


def test_headless_imports_without_ui_packages():
    mods = [
        "common.headless.db_service",
        "common.headless.app_factory",
        "common.headless.composite",
        "common.io.export_utils",
        "common.dashboard.service",
        "common.dashboard.layout_store",
        "schema_converter.bridge",
        "schema_converter.service",
        "ai_query.service",
        "monitoring.service",
    ]
    for mod in mods:
        if mod in sys.modules:
            del sys.modules[mod]
        m = importlib.import_module(mod)
        assert m is not None


def test_api_does_not_import_ui_web():
    """create_app must not pull in the web UI package (one-way dependency)."""
    for name in list(sys.modules):
        if name == "common.ui.web" or name.startswith("common.ui.web."):
            del sys.modules[name]
    if "common.headless.app_factory" in sys.modules:
        del sys.modules["common.headless.app_factory"]
    from common.headless.app_factory import create_app

    create_app(module_key=None)
    assert "common.ui.web" not in sys.modules, "API import must not load web UI"


def test_dashboard_package_init_does_not_import_tk():
    if "common.dashboard" in sys.modules:
        del sys.modules["common.dashboard"]
    pkg = importlib.import_module("common.dashboard")
    assert hasattr(pkg, "DashboardService")
    # DashboardUI must not be eagerly imported at package init
    assert "DashboardUI" not in pkg.__dict__


def test_db_service_export_uses_common_io():
    from common.headless.db_service import CoreDBService

    svc = CoreDBService()
    with patch.object(svc, "_get_or_connect", side_effect=RuntimeError("no db")):
        r = svc.export_table("x", "t", "/tmp/out.csv")
    assert r["ok"] is False


def test_ui_registry_reports_missing_tk(capsys):
    from common.core import ui_registry

    with patch.object(ui_registry, "_package_available", return_value=False):
        with patch.object(sys, "exit", side_effect=SystemExit(1)):
            try:
                ui_registry.launch_tk_ui()
            except SystemExit:
                pass
    err = capsys.readouterr().err
    assert "not installed" in err.lower() or "tk" in err


def test_ui_registry_reports_missing_textual(capsys):
    from common.core import ui_registry

    with patch.object(ui_registry, "_package_available", return_value=False):
        with patch.object(sys, "exit", side_effect=SystemExit(1)):
            try:
                ui_registry.launch_textual_ui()
            except SystemExit:
                pass
    err = capsys.readouterr().err
    assert "not installed" in err.lower() or "textual" in err


def test_ui_registry_reports_missing_web(capsys):
    from common.core import ui_registry

    with patch.object(ui_registry, "_package_available", return_value=False):
        with patch.object(sys, "exit", side_effect=SystemExit(1)):
            try:
                ui_registry.launch_web_ui()
            except SystemExit:
                pass
    err = capsys.readouterr().err
    assert "not installed" in err.lower() or "web" in err


def test_web_ui_is_standalone_and_does_not_import_api():
    """The Web UI builds its OWN app and never imports the public REST API."""
    for name in list(sys.modules):
        if name == "common.headless.app_factory":
            del sys.modules[name]
    from common.ui.web.mount import static_dir
    from common.ui.web.server import build_web_app

    assert static_dir().is_dir()
    app = build_web_app(feature_module=None)
    paths = {getattr(r, "path", "") for r in app.routes}
    # Serves the SPA and its own data routes...
    assert any(p == "/ui" or p.startswith("/ui") for p in paths)
    assert "/api/connections" in paths
    assert "/ui/config" in paths
    # ...without dragging in the public REST API module.
    assert "common.headless.app_factory" not in sys.modules, (
        "Web UI must not import the public REST API (app_factory)"
    )


def test_web_ui_served_index_via_testclient():
    from fastapi.testclient import TestClient

    from common.ui.web.server import build_web_app

    app = build_web_app(feature_module=None)
    client = TestClient(app)
    r = client.get("/ui/")
    assert r.status_code == 200
    assert "DbManagementTool" in r.text


def test_cli_modules_work_without_ui(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as p

    p.bootstrap()
    root = Path(__file__).resolve().parents[1]
    import subprocess

    r = subprocess.run(
        [sys.executable, str(root / "app" / "dbtool.py"), "modules"],
        capture_output=True,
        text=True,
        cwd=str(root),
        env={**dict(**__import__("os").environ), "PYTHONPATH": str(root)},
    )
    assert r.returncode == 0, r.stderr
