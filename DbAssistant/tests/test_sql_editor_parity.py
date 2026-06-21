"""SQL Editor parity tests (service + API + TUI screen, offline-safe)."""

from __future__ import annotations

import pytest


def _svc():
    from common.headless.db_service import CoreDBService
    return CoreDBService()


# --------------------------------------------------------------------------- #
def test_format_sql_service():
    r = _svc().format_sql("select id,name from users where id=1")
    assert r["ok"]
    assert "SELECT" in r["sql"]
    assert "FROM" in r["sql"]
    assert "\n" in r["sql"]  # reindented


def test_format_sql_empty():
    r = _svc().format_sql("")
    assert r["ok"]
    assert r["sql"] == ""


# --------------------------------------------------------------------------- #
def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def test_api_format_endpoint():
    c = _client()
    r = c.post("/api/query/format", json={"sql": "select 1 as a"})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"]
    assert "SELECT" in body["sql"]


def test_api_format_requires_sql():
    c = _client()
    assert c.post("/api/query/format", json={}).status_code == 422


def test_api_schema_route_exists():
    """Route registered (errors cleanly without a live connection)."""
    c = _client()
    r = c.get("/api/objects/no_such_conn/schema", params={"table": "t"})
    # Either a clean 4xx error envelope or 200 — never a 404 for missing route.
    assert r.status_code != 404


def test_web_sql_editor_exposes_tk_toolbar():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    html = c.get("/").text
    for token in (
        "sql-tab-strip",
        "sql-run-cursor",
        "sql-run-sel",
        "sql-run-all",
        "sql-load",
        "sql-save",
        "sql-format",
        "sql-autocomplete-toggle",
        "sql-commit",
        "sql-rollback",
        "sql-stop",
        "sql-clear-results",
        "sql-result-copy",
        "sql-result-sort-asc",
        "sql-result-filter",
        "sql-export-csv",
    ):
        assert token in html


def test_web_sql_editor_appjs_exposes_tk_actions():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    for token in (
        "statementAtCursor",
        "renderSqlTabs",
        "sqlAutocomplete",
        "sql-result-filter",
        "sql-clear-results",
    ):
        assert token in js


# --------------------------------------------------------------------------- #
# Shared-spec single-source governance for the SQL Editor.
# --------------------------------------------------------------------------- #
def test_shared_sql_editor_payload_shape():
    from common.ui.shared import specs

    payload = specs.sql_editor_payload()
    for key in ("connectionActions", "editorActions", "resultActions",
                "resultMenu", "tabActions"):
        assert key in payload, f"sql editor payload missing {key}"

    conn = {a["id"]: a["label"] for a in payload["connectionActions"]}
    assert conn["refresh"] == "Refresh connections"
    assert payload["autocommitLabel"] == "Auto-commit"

    editor_ids = [a["id"] for a in payload["editorActions"]]
    assert editor_ids[:4] == ["run_cursor", "run_selected", "run_all", "stop"]
    ac = next(a for a in payload["editorActions"] if a["id"] == "autocomplete")
    assert ac["label"] == "Autocomplete: On" and ac["labelOff"] == "Autocomplete: Off"

    result_ids = [a["id"] for a in payload["resultActions"]]
    assert result_ids == ["copy_all", "sort_asc", "sort_desc", "filter",
                          "clear_filter", "clear_results", "export"]


def test_tk_sql_editor_results_toolbar_matches_tui_web():
    """Tk gains the same Query Results toolbar buttons as TUI/Web, single-sourced
    from the shared spec and wired to working handlers (not stubs)."""
    import inspect

    from common.ui.tk import sql_editor_pane

    src = inspect.getsource(sql_editor_pane)
    # Labels come from the shared spec, not hardcoded literals.
    assert 'shared_specs.sql_editor_payload()["resultActions"]' in src
    # Each result action is wired to a toolbar handler.
    for handler in ("_toolbar_copy_all", "_toolbar_sort", "_toolbar_filter",
                    "_toolbar_clear_filter"):
        assert f"def {handler}" in src, f"missing {handler}"
        assert handler in src.replace(f"def {handler}", ""), f"{handler} not wired"

    cls = sql_editor_pane.SQLEditorPane
    # Handlers route to the real tree operations the right-click menu uses.
    for method in ("_toolbar_copy_all", "_toolbar_sort", "_toolbar_filter",
                   "_toolbar_clear_filter", "_current_result_tree"):
        assert callable(getattr(cls, method, None)), f"missing method {method}"
    assert "self._copy_tree_all(tree)" in inspect.getsource(cls._toolbar_copy_all)
    assert "self._sort_tree_column(" in inspect.getsource(cls._toolbar_sort)
    assert "self._filter_tree_column(" in inspect.getsource(cls._toolbar_filter)
    assert "self._clear_tree_filter(" in inspect.getsource(cls._toolbar_clear_filter)


def test_web_ui_config_exposes_sql_editor_spec():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    cfg = c.get("/ui/config").json()
    sql = cfg["specs"]["sqlEditor"]
    editor_ids = [a["id"] for a in sql["editorActions"]]
    assert "run_cursor" in editor_ids
    assert {a["id"]: a["label"] for a in sql["connectionActions"]}["refresh"] == \
        "Refresh connections"


def test_web_sql_appjs_applies_labels_from_shared_spec():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    assert "applySqlLabels" in js
    assert "SHARED_SQL_ACTION_TO_DOM" in js
    assert "specs.sqlEditor" in js


# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_sql_editor_format_and_history():
    from textual.widgets import Button, TextArea

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen

        ta = scr.query_one("#sql-input", TextArea)
        ta.text = "select a,b from t where a=1"
        scr.query_one("#sql-format", Button).press()
        await pilot.pause()
        assert "SELECT" in ta.text
        assert "\n" in ta.text  # reindented

        # History helper records statements.
        scr._add_history("SELECT 1;")
        scr._add_history("SELECT 2;")
        assert scr._history[0] == "SELECT 2;"
        assert "SELECT 1;" in scr._history


class _FakeSqlSvc:
    """Minimal service stub for exercising the TUI SQL editor offline."""

    def list_connections(self):
        return [{"name": "local_mariadb"}]

    def get_autocommit(self, conn):
        return {"ok": True, "autocommit": False}

    def open_connection(self, conn, form=None):
        return {"ok": True, "message": "open"}

    def execute_multi(self, conn, sql):
        return {
            "count": 1,
            "results": [
                {"result": {"columns": ["Tables_in_db"], "rows": [["t1"], ["t2"]],
                            "rowcount": 2}},
            ],
        }


@pytest.mark.anyio
async def test_tui_sql_editor_rerun_does_not_crash_on_blank_result_pick():
    """Re-running resets the result picker options, which fires a Select.Changed
    carrying Select.BLANK; the handler must not pass that into int()."""
    from textual.widgets import Button, Select, TextArea

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeSqlSvc()

        sel = scr.query_one("#sql-conn", Select)
        sel.set_options([("local_mariadb", "local_mariadb")])
        sel.value = "local_mariadb"
        await pilot.pause()

        scr.query_one("#sql-input", TextArea).text = "show tables;"
        # First run populates the picker.
        scr.query_one("#sql-run-all", Button).press()
        await pilot.pause()
        assert len(scr._results) == 1

        # Second run resets options (Changed -> Select.BLANK) and must not raise.
        scr.query_one("#sql-run-all", Button).press()
        await pilot.pause()
        assert len(scr._results) == 1
        # The selected result is still shown (no crash, picker re-selected #0).
        assert str(scr.query_one("#sql-result-pick", Select).value) == "0"


@pytest.mark.anyio
async def test_tui_sql_editor_labels_from_shared_spec():
    """TUI toolbar/result labels are stamped from the shared spec (no drift:
    'Refresh connections', 'Auto-commit')."""
    from textual.widgets import Button, Checkbox

    from common.ui.shared import specs
    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        sp = specs.sql_editor_payload()
        conn = {a["id"]: a["label"] for a in sp["connectionActions"]}
        tool = {a["id"]: a["label"] for a in sp["editorActions"]}
        res = {a["id"]: a["label"] for a in sp["resultActions"]}
        assert str(scr.query_one("#sql-refresh", Button).label) == conn["refresh"]
        assert str(scr.query_one("#sql-autocommit", Checkbox).label) == sp["autocommitLabel"]
        assert str(scr.query_one("#sql-run-cursor", Button).label) == tool["run_cursor"]
        assert str(scr.query_one("#sql-clear-results", Button).label) == res["clear_results"]


@pytest.mark.anyio
async def test_tui_sql_editor_multi_tab_buffers():
    """New-tab strip mirrors Tk/Web: each tab keeps its own SQL buffer and
    switching restores it."""
    from textual.widgets import Button, TextArea, Tabs

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen

        scr.query_one("#sql-input", TextArea).text = "SELECT 100;"
        scr.query_one("#sql-tab-new", Button).press()
        await pilot.pause()
        await pilot.pause()
        assert len(scr._tabs_state) == 2
        assert scr._active_tab == 1
        assert scr.query_one("#sql-input", TextArea).text == ""

        scr.query_one("#sql-input", TextArea).text = "SELECT 200;"
        tabs = scr.query_one("#sql-tab-strip", Tabs)
        tabs.active = "sqltab-0"
        await pilot.pause()
        assert scr._active_tab == 0
        assert scr.query_one("#sql-input", TextArea).text == "SELECT 100;"

        tabs.active = "sqltab-1"
        await pilot.pause()
        assert scr.query_one("#sql-input", TextArea).text == "SELECT 200;"


@pytest.mark.anyio
async def test_tui_sql_editor_save_and_load_roundtrip(tmp_path):
    """Save query writes the editor to a file; Load reads it back (Tk parity)."""
    from textual.widgets import Button, Input, TextArea

    from common.ui.textual.app import DbToolApp

    target = tmp_path / "q.sql"
    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        scr.query_one("#sql-input", TextArea).text = "SELECT 42 AS answer;"

        scr.query_one("#sql-save", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-path", Input).value = str(target)
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        assert target.read_text().strip() == "SELECT 42 AS answer;"

        # Clear and load it back.
        scr.query_one("#sql-input", TextArea).text = ""
        scr.query_one("#sql-load", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-path", Input).value = str(target)
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        assert "SELECT 42 AS answer;" in scr.query_one("#sql-input", TextArea).text


@pytest.mark.anyio
async def test_tui_sql_editor_export_csv_and_json(tmp_path):
    """Export current result to CSV/JSON using the shared export helpers."""
    import json as _json
    from textual.widgets import Button, Input, Select, TextArea

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeSqlSvc()
        sel = scr.query_one("#sql-conn", Select)
        sel.set_options([("local_mariadb", "local_mariadb")])
        sel.value = "local_mariadb"
        await pilot.pause()
        scr.query_one("#sql-input", TextArea).text = "show tables;"
        scr.query_one("#sql-run-all", Button).press()
        await pilot.pause()

        csv_path = tmp_path / "out.csv"
        scr.query_one("#sql-export", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-path", Input).value = str(csv_path)
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        text = csv_path.read_text()
        assert "Tables_in_db" in text and "t1" in text and "t2" in text

        json_path = tmp_path / "out.json"
        scr.query_one("#sql-export", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-path", Input).value = str(json_path)
        app.screen.query_one("#field-format", Select).value = "json"
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        payload = _json.loads(json_path.read_text())
        assert payload["columns"] == ["Tables_in_db"]
        assert payload["rows"] == [["t1"], ["t2"]]


@pytest.mark.anyio
async def test_tui_sql_editor_filter_and_clear():
    """Filter narrows the current result to matching rows; Clear restores them."""
    from textual.widgets import Button, Input, Select, TextArea

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeSqlSvc()
        sel = scr.query_one("#sql-conn", Select)
        sel.set_options([("local_mariadb", "local_mariadb")])
        sel.value = "local_mariadb"
        await pilot.pause()
        scr.query_one("#sql-input", TextArea).text = "show tables;"
        scr.query_one("#sql-run-all", Button).press()
        await pilot.pause()
        assert len(scr._results[0]["rows"]) == 2

        scr.query_one("#sql-filter", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-contains", Input).value = "t1"
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        assert scr._results[0]["rows"] == [["t1"]]

        scr.query_one("#sql-clear-filter", Button).press()
        await pilot.pause()
        assert len(scr._results[0]["rows"]) == 2


@pytest.mark.anyio
async def test_tui_sql_editor_exposes_tk_toolbar():
    from textual.widgets import Button

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("sql")
        await pilot.pause()
        scr = app.screen
        for bid in (
            "sql-run-cursor",
            "sql-run-sel",
            "sql-run-all",
            "sql-stop",
            "sql-clear",
            "sql-load",
            "sql-save",
            "sql-format",
            "sql-autocomplete",
            "sql-commit",
            "sql-rollback",
            "sql-copy-all",
            "sql-sort-asc",
            "sql-sort-desc",
            "sql-filter",
            "sql-clear-filter",
            "sql-clear-results",
            "sql-export",
        ):
            assert scr.query_one("#" + bid, Button) is not None
