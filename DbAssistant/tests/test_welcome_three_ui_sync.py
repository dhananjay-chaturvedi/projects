"""Welcome tab parity — Tk, Textual and Web all render the SAME Welcome
content from one source of truth (common.ui.shared.specs.welcome_payload).

A change to the Welcome copy is made once in the shared spec and must show up
in every UI; these tests guard against drift / re-duplication.
"""

from __future__ import annotations

import pytest


# --------------------------------------------------------------------------- #
# Shared spec is the single source of truth
# --------------------------------------------------------------------------- #
def test_shared_welcome_payload_shape():
    from common.ui.shared import specs

    w = specs.welcome_payload()
    assert w["tagline"]
    assert isinstance(w["overview"], list) and len(w["overview"]) >= 4
    assert isinstance(w["tabGuide"], list) and len(w["tabGuide"]) >= 5
    for guide in w["tabGuide"]:
        assert guide["title"] and isinstance(guide["lines"], list) and guide["lines"]
    assert isinstance(w["access"], list) and w["access"]
    assert isinstance(w["platforms"], list) and w["platforms"]
    assert isinstance(w["tips"], list) and w["tips"]
    # Shortcuts are reused from the shared KEYBOARD_SHORTCUTS (single source).
    assert w["shortcuts"] == specs.KEYBOARD_SHORTCUTS
    assert w["footer"]


# --------------------------------------------------------------------------- #
# Web SPA: tab + panel + spec-driven renderer, content served via /ui/config
# --------------------------------------------------------------------------- #
@pytest.fixture
def web_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as p

    p.bootstrap()
    from starlette.testclient import TestClient

    from common.ui.web.backend import build_web_backend

    return TestClient(build_web_backend())


def test_web_has_welcome_tab_and_panel(web_client):
    html = web_client.get("/").text
    assert 'data-tab="welcome"' in html
    assert 'id="panel-welcome"' in html
    assert 'id="welcome-content"' in html


def test_web_appjs_renders_welcome_from_shared_spec(web_client):
    js = web_client.get("/ui/app.js").text
    assert "renderWelcome" in js
    # Reads the welcome content from the /ui/config specs payload, not hardcoded.
    assert "specs.welcome" in js
    # Welcome tab is mapped so applySharedTabs relabels/reorders it.
    assert "welcome:" in js


def test_web_ui_config_exposes_welcome_content(web_client):
    cfg = web_client.get("/ui/config").json()
    w = cfg["specs"]["welcome"]
    from common.ui.shared import specs

    assert w["tagline"] == specs.WELCOME_TAGLINE
    assert w["overview"] == specs.WELCOME_OVERVIEW
    assert [g["title"] for g in w["tabGuide"]] == [
        g["title"] for g in specs.WELCOME_TAB_GUIDE
    ]


# --------------------------------------------------------------------------- #
# Tk desktop: Welcome tab reads the shared spec (no hardcoded copy)
# --------------------------------------------------------------------------- #
def test_tk_welcome_tab_reads_shared_spec():
    import inspect

    pytest.importorskip("tkinter")
    from common.ui.tk import master_shell as ms

    src = inspect.getsource(ms.UnifiedDBManagerUI.create_welcome_tab_ui)
    # Content comes from the shared spec helper, not inline literals.
    assert "WELCOME_TAGLINE" in src
    assert "WELCOME_OVERVIEW" in src
    assert "WELCOME_TAB_GUIDE" in src
    assert "WELCOME_ACCESS" in src
    assert "WELCOME_PLATFORMS" in src
    assert "WELCOME_TIPS" in src
    assert "WELCOME_FOOTER" in src


# --------------------------------------------------------------------------- #
# Textual TUI: the home/Welcome screen renders the shared content
# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_home_renders_shared_welcome_content(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as p

    p.bootstrap()
    from textual.widgets import Static

    from common.ui.shared import specs
    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        # The launch (Welcome) screen carries the shared content.
        body = app.screen.query_one("#welcome-body")
        texts = [str(s.render()) for s in body.query(Static)]
        blob = "\n".join(texts)
        assert specs.WELCOME_TAGLINE in blob
        # A representative overview line and a tab-guide title are present.
        assert specs.WELCOME_OVERVIEW[0] in blob
        assert specs.WELCOME_TAB_GUIDE[0]["title"] in blob
        assert specs.WELCOME_FOOTER in blob
