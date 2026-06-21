"""Connections-tab parity across the three UIs.

Verifies that the Web SPA and the Textual TUI both expose the SAME Connections
sections, fields and button sets that the Tk desktop tab has, sourced from
``common.ui.shared.specs``:

* Active connections
* Add or select database connection (direct)   — Connect / Test Connection /
  Load Saved / Save Connection / Clear
* Add or select remote database connection (SSH) — Connect / Test Connection /
  Load Saved / Save / Clear

The headless service is exercised with sample data so save/load paths are
covered, not just static layout.
"""

from __future__ import annotations

import pytest


# --------------------------------------------------------------------------- #
# Shared spec is the single source of truth
# --------------------------------------------------------------------------- #
def test_shared_spec_defines_split_sections():
    from common.ui.shared import specs

    payload = specs.as_payload()["connection"]
    section_ids = [s["id"] for s in payload["sections"]]
    assert section_ids == ["active", "saved", "direct", "remote", "cloud"]

    direct = [a["label"] for a in payload["directActions"]]
    remote = [a["label"] for a in payload["remoteActions"]]
    assert direct == ["Connect", "Test Connection", "Load Saved", "Save Connection", "Clear"]
    assert remote == ["Connect", "Test Connection", "Load Saved", "Save", "Clear"]
    # Remote carries its own SSH fields; direct does not.
    assert any(f["id"] == "ssh_host" for f in payload["remoteSshFields"])


def test_shared_spec_defines_collapse_defaults():
    """The shared spec is the single source of truth for collapse defaults:
    only Active connections is expanded; every other section starts collapsed."""
    from common.ui.shared import specs

    collapsed = {s["id"]: s.get("collapsed", True) for s in specs.CONNECTION_SECTIONS}
    assert collapsed["active"] is False
    for sid in ("saved", "direct", "remote", "cloud"):
        assert collapsed[sid] is True
    # Helper mirrors the spec.
    assert specs.connection_section_collapsed("active") is False
    assert specs.connection_section_collapsed("saved") is True
    assert specs.connection_section_collapsed("missing") is True


# --------------------------------------------------------------------------- #
# Web SPA: served HTML + JS carry both forms and the same buttons
# --------------------------------------------------------------------------- #
@pytest.fixture
def web_client(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as p

    p.bootstrap()
    from starlette.testclient import TestClient

    from common.ui.web.backend import build_web_backend

    return TestClient(build_web_backend())


def test_web_index_has_direct_and_remote_sections(web_client):
    html = web_client.get("/").text
    # Two separate sections
    assert 'id="sec-add"' in html
    assert 'id="sec-remote"' in html
    assert "Add or select remote database connection" in html
    # Direct buttons
    for bid in ("conn-connect-form", "conn-test-form", "conn-load-saved"):
        assert f'id="{bid}"' in html
    # Remote buttons + fields
    for bid in ("r-connect", "r-test", "r-load-saved", "r-ssh-host", "r-ssh-auth"):
        assert f'id="{bid}"' in html
    # The old combined location dropdown is gone
    assert 'id="conn-location"' not in html


def test_web_connections_collapse_defaults_and_saved_order(web_client):
    import re

    html = web_client.get("/").text
    # Saved connections sits directly below Active connections, before the forms.
    i_active = html.index('id="sec-active"')
    i_saved = html.index('id="sec-saved"')
    i_add = html.index('id="sec-add"')
    i_remote = html.index('id="sec-remote"')
    i_cloud = html.index('id="sec-cloud"')
    assert i_active < i_saved < i_add < i_remote < i_cloud
    # Only Active connections is expanded by default; all others collapsed.
    assert re.search(r'id="sec-active"[^>]*\bopen\b', html)
    for sid in ("sec-saved", "sec-add", "sec-remote", "sec-cloud"):
        assert not re.search(r'id="%s"[^>]*\bopen\b' % sid, html), \
            f"{sid} must be collapsed by default"


def test_tk_has_saved_connections_panel_below_active():
    """Tk gains an inline Saved connections panel, placed directly below
    Active connections and collapsed by default (parity with TUI and Web)."""
    import inspect

    pytest.importorskip("tkinter")
    from common.ui.tk import master_shell as ms

    cls = ms.UnifiedDBManagerUI
    assert hasattr(cls, "create_saved_connections_frame")

    # The tab order is data-driven from the shared spec, not a hardcoded
    # sequence of calls; the builder map covers every section.
    tab_src = inspect.getsource(cls.create_connections_tab_ui)
    assert "_connection_sections()" in tab_src
    for builder in ("create_active_connections_frame", "create_saved_connections_frame",
                    "create_connection_frame", "create_remote_connection_frame",
                    "create_cloud_connection_frame"):
        assert builder in tab_src

    # Collapse state is read from the shared spec helper, not hardcoded.
    panel_src = inspect.getsource(cls.create_saved_connections_frame)
    assert "Saved connections" in panel_src
    assert '_conn_section_expanded("saved")' in panel_src
    for label in ('text="Refresh"', 'text="Connect"', 'text="Test"', 'text="Remove"'):
        assert label in panel_src

    # The shared spec drives order: saved sits directly below active.
    order = [s["id"] for s in ms.UnifiedDBManagerUI._connection_sections()]
    assert order.index("active") < order.index("saved") < order.index("direct")


def test_web_appjs_has_remote_form_logic(web_client):
    js = web_client.get("/ui/app.js").text
    for fn in ("buildRemoteBody", "saveRemoteConnection", "loadRemoteIntoForm",
               "applyRemoteEngine", "applyRemoteSshAuth", "upsertConnection"):
        assert fn in js
    assert "applyLocationToForm" not in js
    assert "delete body.save_password" in js
    assert "formatTestResult" in js
    assert "r.version" in js


def test_web_ui_config_exposes_connection_sections(web_client):
    cfg = web_client.get("/ui/config").json()
    sections = cfg["specs"]["connection"]["sections"]
    section_ids = [s["id"] for s in sections]
    assert section_ids == ["active", "saved", "direct", "remote", "cloud"]
    # Collapse defaults travel to the Web UI via /ui/config (single source).
    collapsed = {s["id"]: s.get("collapsed", True) for s in sections}
    assert collapsed["active"] is False
    assert all(collapsed[s] for s in ("saved", "direct", "remote", "cloud"))


def test_web_appjs_applies_connection_layout_from_shared_spec(web_client):
    """The SPA derives section order + collapse from /ui/config, not hardcoded."""
    js = web_client.get("/ui/app.js").text
    assert "applyConnectionLayout" in js
    assert "SHARED_CONN_SECTION_TO_DOM" in js
    # It must consume the shared connection sections from /ui/config (cfg.specs).
    assert "specs.connection" in js and "sections" in js


# --------------------------------------------------------------------------- #
# Textual TUI: widget tree + save/load round trip
# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "tui_home"))
    from common import paths as p

    p.bootstrap()


@pytest.mark.anyio
async def test_tui_connections_has_both_forms_and_buttons():
    from textual.widgets import Button, Input

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen

        # Both forms present
        for wid in ("add-name", "add-host", "r-name", "r-host", "r-ssh-host"):
            assert screen.query_one("#" + wid, Input) is not None
        # Direct + remote button sets
        for bid in ("conn-connect-form", "conn-test-form", "conn-load-saved",
                    "conn-add", "conn-clear",
                    "r-connect", "r-test", "r-load-saved", "r-save", "r-clear"):
            assert screen.query_one("#" + bid, Button) is not None


@pytest.mark.anyio
async def test_tui_cloud_section_builds_and_saves():
    from textual.widgets import Button, Select

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen

        # Cloud provider/auth selects + the full Tk button set are present.
        assert screen.query_one("#cloud-provider", Select) is not None
        for bid in ("cloud-connect", "cloud-test-login", "cloud-test-db",
                    "cloud-resolve", "cloud-load-saved", "cloud-save", "cloud-clear"):
            assert screen.query_one("#" + bid, Button) is not None

        # Fill the schema-driven fields and save a profile end to end.
        screen._cf_set("display_name", "tui_cloud")
        screen._cf_set("region", "us-east-1")
        screen._cf_set("resource_name", "mydb")
        screen._cf_set("access_key_id", "AKIAEXAMPLE")
        screen._cf_set("secret_access_key", "secretvalue")
        screen._cf_set("sql_host", "db.example.com")
        screen._cf_set("sql_port", "3306")
        screen._cf_set("sql_db_type", "MySQL")
        screen._cf_set("sql_username", "u")
        screen._cf_set("sql_password", "p")
        name = screen._save_cloud()
        await pilot.pause()
        assert name == "tui_cloud"
        saved = app.svc.list_cloud_db_connections()
        assert any(c["name"] == "tui_cloud" for c in saved)


@pytest.mark.anyio
async def test_tui_save_then_load_direct_connection():
    from textual.widgets import Input

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen

        screen.query_one("#add-name", Input).value = "sample_pg"
        screen.query_one("#add-host", Input).value = "localhost"
        screen.query_one("#add-user", Input).value = "tester"
        name = screen._save_direct()
        await pilot.pause()
        assert name == "sample_pg"
        assert any(c.get("name") == "sample_pg" for c in screen._saved)

        # Clear, then Load Saved repopulates the form.
        screen._clear_direct()
        assert screen.query_one("#add-name", Input).value == ""
        # Select the saved row and load it back.
        screen.query_one("#conn-table").move_cursor(row=0)
        c = screen._selected_saved()
        assert c and c["name"] == "sample_pg"
        screen._load_into_direct(c)
        assert screen.query_one("#add-name", Input).value == "sample_pg"
        assert screen._direct_edit == "sample_pg"


@pytest.mark.anyio
async def test_tui_load_saved_button_opens_picker_with_all_connections():
    """Load Saved must open a chooser listing every saved connection,
    not silently fill the form with the first/selected row."""
    from textual.widgets import Button, Input

    from common.ui.textual.app import DbToolApp
    from common.ui.textual.screens.form_modal import SelectModal

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen

        # Save two distinct direct connections.
        for name, host in (("conn_a", "host-a"), ("conn_b", "host-b")):
            screen.query_one("#add-name", Input).value = name
            screen.query_one("#add-host", Input).value = host
            screen.query_one("#add-user", Input).value = "tester"
            screen._save_direct()
            await pilot.pause()
            screen._clear_direct()

        # Press Load Saved -> a SelectModal must appear with BOTH connections.
        screen.query_one("#conn-load-saved", Button).press()
        await pilot.pause()
        assert isinstance(app.screen, SelectModal), \
            "Load Saved should open a selection list, not auto-fill the form"
        picker = app.screen
        labels = [label for label, _value in picker._options]
        assert any("conn_a" in lbl for lbl in labels)
        assert any("conn_b" in lbl for lbl in labels)

        # Choosing the second one loads exactly that connection.
        picker.dismiss("conn_b")
        await pilot.pause()
        assert screen.query_one("#add-name", Input).value == "conn_b"
        assert screen.query_one("#add-host", Input).value == "host-b"
        assert screen._direct_edit == "conn_b"


@pytest.mark.anyio
async def test_tui_connections_default_collapse_and_saved_order():
    """Only Active connections is expanded by default; Saved connections sits
    directly below it (mirrors the requested layout across all UIs)."""
    from textual.widgets import Collapsible

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        scr = app.screen

        sections = list(scr.query(Collapsible))
        titles = [c.title for c in sections]
        assert titles[0] == "Active connections"
        assert titles[1] == "Saved connections"
        for c in sections:
            if c.title == "Active connections":
                assert c.collapsed is False
            else:
                assert c.collapsed is True, f"{c.title} must be collapsed by default"


@pytest.mark.anyio
async def test_tui_test_connection_buttons_do_not_pass_save_password(monkeypatch):
    """Test Connection uses live form credentials only and never persistence flags."""
    from textual.widgets import Button, Input

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    calls = []

    def fake_test_connection_inline(params):
        calls.append(params)
        return {"ok": True, "message": "tested"}

    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen
        monkeypatch.setattr(app.svc._core, "test_connection_inline", fake_test_connection_inline)

        screen.query_one("#add-name", Input).value = "direct_test"
        screen.query_one("#add-host", Input).value = "localhost"
        screen.query_one("#add-user", Input).value = "tester"
        screen.query_one("#add-password", Input).value = "secret"
        screen.query_one("#conn-test-form", Button).press()
        await pilot.pause()
        await app.workers.wait_for_complete()
        await pilot.pause()

        screen.query_one("#r-name", Input).value = "remote_test"
        screen.query_one("#r-host", Input).value = "localhost"
        screen.query_one("#r-service", Input).value = "db"
        screen.query_one("#r-user", Input).value = "tester"
        screen.query_one("#r-password", Input).value = "secret"
        screen.query_one("#r-ssh-host", Input).value = "bastion"
        screen.query_one("#r-ssh-user", Input).value = "ubuntu"
        screen.query_one("#r-test", Button).press()
        await pilot.pause()
        await app.workers.wait_for_complete()
        await pilot.pause()

        assert [c.name for c in calls] == ["direct_test", "remote_test"]
        assert all(c.save_password in (None, False) for c in calls)
        assert calls[0].password == "secret"
        assert calls[1].ssh_tunnel["ssh_password"] == ""


@pytest.mark.anyio
async def test_tui_test_connection_shows_parsed_pass_fail(monkeypatch):
    """The form must surface the DB's actual success/failure, not stay blank."""
    from textual.widgets import Button, Input, Static

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    outcomes = iter([
        {"ok": True, "version": "PostgreSQL 16.2", "latency_ms": 12.3,
         "message": "Connected to PostgreSQL in 12.3 ms"},
        {"ok": False, "version": None, "latency_ms": 8.0,
         "message": "FATAL: password authentication failed for user 'tester'"},
    ])

    def fake_test_connection_inline(params):
        return next(outcomes)

    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen
        monkeypatch.setattr(app.svc._core, "test_connection_inline", fake_test_connection_inline)

        screen.query_one("#add-name", Input).value = "pg"
        screen.query_one("#add-host", Input).value = "localhost"
        screen.query_one("#add-user", Input).value = "tester"
        screen.query_one("#add-password", Input).value = "secret"

        # Success path is parsed and shown.
        screen.query_one("#conn-test-form", Button).press()
        await pilot.pause()
        await app.workers.wait_for_complete()
        await pilot.pause()
        ok_text = str(screen.query_one("#conn-status", Static).content)
        assert "PostgreSQL 16.2" in ok_text
        assert "Testing" not in ok_text  # progress line was replaced by the result

        # Failure path surfaces the DB error message.
        screen.query_one("#conn-test-form", Button).press()
        await pilot.pause()
        await app.workers.wait_for_complete()
        await pilot.pause()
        fail_text = str(screen.query_one("#conn-status", Static).content)
        assert "password authentication failed" in fail_text


@pytest.mark.anyio
async def test_tui_connection_field_groups_are_auto_height():
    """SSL/TLS/SSH/cloud groups must size to content so nothing is clipped;
    the scrollable body then exposes everything via its scrollbar."""
    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.push_screen_by_name("connections")
        await pilot.pause()
        screen = app.screen

        for gid in ("#ssl-group", "#tls-group", "#r-ssh-group",
                    "#cloud-resource-fields", "#cloud-auth-fields",
                    "#cloud-sql-fields"):
            group = screen.query_one(gid)
            assert group.styles.height is not None and group.styles.height.is_auto, (
                f"{gid} must be height:auto so its fields aren't clipped"
            )

        # The body is the scroll host (height:1fr, overflow-y:auto).
        body = screen.query_one("#body")
        assert body.styles.overflow_y == "auto"
