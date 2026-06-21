"""Smoke tests for the Textual UI app (headless via run_test pilot)."""

from __future__ import annotations

import pytest

pytest.importorskip("textual")


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as p

    p.bootstrap()


async def _boot(feature_module=None):
    from common.ui.textual.app import DbToolApp

    app = DbToolApp(feature_module=feature_module)
    async with app.run_test() as pilot:
        await pilot.pause()
        names = set(app._screen_names)
        current = app.screen.__class__.__name__
        app.push_screen_by_name("connections")
        await pilot.pause()
        after_nav = app.screen.__class__.__name__
        app.action_home()
        await pilot.pause()
        home = app.screen.__class__.__name__
    return names, current, after_nav, home


@pytest.mark.anyio
async def test_full_app_boots_and_navigates():
    names, current, after_nav, home = await _boot(None)
    assert {"home", "connections", "sql", "objects"} <= names
    assert current == "HomeScreen"
    assert after_nav == "ConnectionsScreen"
    assert home == "HomeScreen"


@pytest.mark.anyio
async def test_single_module_app_only_installs_that_module():
    names, current, _, _ = await _boot("migrator")
    assert "migration" in names
    assert "ai" not in names
    assert "monitor" not in names
    assert current == "HomeScreen"


@pytest.mark.anyio
async def test_content_screens_show_horizontal_tab_bar_and_switch():
    """Home and content screens show a one-row tab bar (Tk notebook parity), and
    a tab button switches the active screen without growing the stack."""
    from textual.containers import Horizontal
    from textual.widgets import Button

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        home = app.screen

        # The launch screen shown by ``python dbtool.py tui`` is also a tab row,
        # not the old vertical menu.
        home_bar = home.query_one("#tabbar", Horizontal)
        home_buttons = list(home_bar.query(Button))
        home_labels = [b.label.plain for b in home_buttons]
        assert home_labels[:5] == [
            "Welcome", "Connections", "Dashboard", "Database Objects", "SQL Editor",
        ]
        assert {b.region.y for b in home_buttons} == {home_buttons[0].region.y}

        home.query_one("#tabnav-connections", Button).press()
        await pilot.pause()
        scr = app.screen

        # One-row tab bar exists and lists tabs in canonical Tk order.
        bar = scr.query_one("#tabbar", Horizontal)
        buttons = list(bar.query(Button))
        labels = [b.label.plain for b in buttons]
        assert labels[:5] == [
            "Welcome", "Connections", "Dashboard", "Database Objects", "SQL Editor",
        ]
        assert {b.region.y for b in buttons} == {buttons[0].region.y}

        depth_before = len(app.screen_stack)
        # Clicking the SQL Editor tab switches screens (switch, not push).
        scr.query_one("#tabnav-sql", Button).press()
        await pilot.pause()
        assert app.screen.__class__.__name__ == "SqlEditorScreen"
        assert len(app.screen_stack) == depth_before


@pytest.mark.anyio
async def test_escape_from_content_screen_returns_to_welcome_not_blank():
    """Pressing Escape on a content screen must return to the Welcome/home
    screen — not pop to Textual's empty default screen (regression)."""
    from common.ui.textual.app import DbToolApp
    from common.ui.textual.screens.home import HomeScreen

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        assert app.screen.__class__.__name__ == "ConnectionsScreen"

        await pilot.press("escape")
        await pilot.pause()

        # Back on the Welcome screen (which carries the Quit binding), not blank.
        assert isinstance(app.screen, HomeScreen)
        assert app.screen.query_one("#tabbar")


@pytest.fixture
def anyio_backend():
    return "asyncio"
