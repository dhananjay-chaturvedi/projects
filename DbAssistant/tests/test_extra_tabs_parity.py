"""Dashboard / Settings / Clear-cache parity tests (API + TUI, offline-safe)."""

from __future__ import annotations

import pytest


def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def test_dashboard_endpoint():
    c = _client()
    r = c.get("/api/dashboard")
    assert r.status_code == 200
    body = r.json()
    assert "core" in body and "modules" in body


def test_settings_endpoint():
    c = _client()
    r = c.get("/api/config/settings")
    assert r.status_code == 200
    assert "settings" in r.json()


def test_clear_caches_endpoint():
    c = _client()
    r = c.post("/api/app/clear-caches")
    assert r.status_code == 200
    assert r.json()["ok"] is True


def test_shortcuts_endpoint():
    c = _client()
    r = c.get("/api/app/shortcuts")
    assert r.status_code == 200
    assert "shortcuts" in r.json()


# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_dashboard_and_settings_screens():
    from textual.widgets import DataTable

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("dashboard")
        await pilot.pause()
        dash = app.screen
        assert dash.query_one("#dash-core", DataTable).row_count >= 1
        assert dash.query_one("#dash-modules", DataTable).row_count >= 1

        app.pop_screen()
        await pilot.pause()
        app.push_screen_by_name("settings")
        await pilot.pause()
        sett = app.screen
        assert sett.query_one("#set-grid", DataTable).row_count >= 1
