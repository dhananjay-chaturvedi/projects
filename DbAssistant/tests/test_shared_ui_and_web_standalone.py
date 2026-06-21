"""Shared UI properties + standalone Web UI backend (decoupled from the API)."""

from __future__ import annotations

import sys


# --- shared UI config (single source of truth) -----------------------------
def test_shared_tabs_order_matches_tk():
    from common.ui import shared

    full = [t.id for t in shared.visible_tabs()]
    # Welcome first, Clear Cache last, core tabs in Tk order.
    assert full[0] == "welcome"
    assert full[-1] == "clear_cache"
    for core in ("connections", "dashboard", "objects", "sql_editor", "settings"):
        assert core in full
    assert full.index("objects") < full.index("sql_editor")


def test_shared_standalone_tabs_drop_welcome_and_clear_cache():
    from common.ui import shared

    tabs = [t.id for t in shared.visible_tabs("migrator")]
    assert "welcome" not in tabs and "clear_cache" not in tabs
    assert "migrator" in tabs
    assert "ai" not in tabs and "monitor" not in tabs


def test_shared_titles_and_theme():
    from common.ui import shared

    assert shared.app_title("ai").endswith("AI Query Assistant")
    theme = shared.theme().as_dict()
    for k in ("primary", "bgMain", "textPrimary", "error", "success"):
        assert k in theme and theme[k].startswith("#")


# --- standalone Web UI ------------------------------------------------------
def test_web_backend_serves_own_config_and_routes():
    from fastapi.testclient import TestClient

    from common.ui.web.server import build_web_app

    app = build_web_app(feature_module=None)
    client = TestClient(app)

    cfg = client.get("/ui/config")
    assert cfg.status_code == 200
    body = cfg.json()
    assert "title" in body and "tabs" in body and "theme" in body
    assert any(t["id"] == "connections" for t in body["tabs"])

    # Its own data route works (reads the service directly).
    assert client.get("/api/connections").status_code == 200
    # And it serves the SPA.
    assert client.get("/ui/").status_code == 200


def test_web_backend_does_not_import_public_api():
    for name in list(sys.modules):
        if name == "common.headless.app_factory":
            del sys.modules[name]
    from common.ui.web.server import build_web_app

    build_web_app(feature_module=None)
    assert "common.headless.app_factory" not in sys.modules
