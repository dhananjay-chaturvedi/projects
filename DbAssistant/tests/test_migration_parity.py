"""Data Migration parity tests (API routes + bridge + TUI screen, offline-safe)."""

from __future__ import annotations

import pytest


def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def test_migrator_routes_registered():
    c = _client()
    paths = {r.path for r in c.app.routes}
    for p in ("/api/migrator/validate", "/api/migrator/convert-multi",
              "/api/migrator/apply", "/api/migrator/transfer-data",
              "/api/migrator/transfer-data-multi"):
        assert p in paths, p


def test_transfer_request_validation():
    c = _client()
    # Missing required tables -> 422
    assert c.post("/api/migrator/validate", json={"source_conn": "a", "target_conn": "b"}).status_code == 422


def test_joined_ddl_terminates_statements():
    """convert_schema_multi joins statements with semicolons (offline shape test)."""
    from schema_converter.bridge import SchemaBridge

    bridge = SchemaBridge(core=None)

    # Patch convert_schema to avoid any live connection — we only verify the
    # joining logic terminates each statement so apply() can split them.
    def fake_convert(source_conn, target_db_type, table, **kw):
        return {"ddl": f"CREATE TABLE {table} (id INT)",
                "all_ddl": [f"CREATE TABLE {table} (id INT)",
                            f"CREATE INDEX ix_{table} ON {table}(id)"],
                "indexes_ddl": [], "issues": [], "error": None,
                "target_table": table}

    bridge.convert_schema = fake_convert  # type: ignore
    r = bridge.convert_schema_multi("src", "PostgreSQL", ["t1", "t2"])
    joined = r["joined_ddl"]
    # Each of the 4 statements (2 tables x 2 ddl) ends with ';'
    assert joined.count(";") == 4
    # Splitting on ';' yields the individual statements
    parts = [s.strip() for s in joined.split(";") if s.strip()]
    assert len(parts) == 4


def test_web_migration_exposes_tk_controls():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    html = c.get("/").text
    for token in (
        "mig-settings",
        "mig-preview",
        "mig-sample",
        "mig-clear",
        "mig-stop",
        "mon-server-show-graphs",
        "Data conversion and migration services",
        "mig-source-db",
        "mig-target-db",
        "mig-create-indexes",
        "Create Indexes (with schema)",
        "mig-drop-if-exists",
        "Drop Table If Exists (before schema conversion)",
        "mig-load-tables",
        "mig-check-all",
        "mig-uncheck-all",
    ):
        assert token in html


def test_web_migration_appjs_wires_settings():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    assert "mig-settings" in js
    assert 'api.get("/api/migrator/config")' in js


def test_web_migration_exposes_dump_schema():
    """Web SPA has the Dump Schema control wired to the dump API (Tk parity)."""
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    assert "mig-dump" in c.get("/").text
    js = c.get("/ui/app.js").text
    assert "mig-dump" in js
    assert "/api/migrator/" in js and "/dump" in js


def test_tk_migration_has_dump_button_and_handler():
    """Tk reference exposes a Dump Schema button + handler using the service."""
    import inspect
    from common.ui.tk.migrator import schema_converter_ui as mod

    src = inspect.getsource(mod)
    assert 'text="Dump Schema"' in src
    assert "def dump_schema_ddl" in src
    assert "SchemaService" in src


# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_migration_screen_has_all_options():
    from textual.widgets import Button, Checkbox, Input, Select

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("migration")
        await pilot.pause()
        scr = app.screen
        # G4/G6/G7 policy selectors present.
        for wid in ("#mig-overflow", "#mig-null", "#mig-bool", "#mig-tz"):
            assert scr.query_one(wid, Select) is not None
        # G3/G8/G9 + parallel toggles present.
        for wid in (
            "#mig-create-indexes",
            "#mig-drop-if-exists",
            "#mig-continue",
            "#mig-reset-seq",
            "#mig-checkpoint",
            "#mig-parallel",
        ):
            assert scr.query_one(wid, Checkbox) is not None
        assert scr.query_one("#mig-source-db", Input) is not None
        assert scr.query_one("#mig-settings", Button) is not None
        # Dump Schema control (Tk parity) present and wired.
        assert scr.query_one("#mig-dump", Button) is not None
        assert scr.query_one("#mig-preview", Button) is not None
        assert scr.query_one("#mig-sample", Button) is not None
        assert scr.query_one("#mig-clear", Button) is not None
        assert scr.query_one("#mig-stop", Button) is not None
        assert hasattr(scr, "_dump_schema")
        assert hasattr(scr, "_preview_schema")
        assert hasattr(scr, "_sample_data")
