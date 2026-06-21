"""Objects tab parity tests (service + API + TUI screen, offline-safe)."""

from __future__ import annotations

import pytest


def _svc():
    from common.headless.db_service import CoreDBService
    return CoreDBService()


def test_list_db_ops_returns_display_names():
    rows = _svc().list_db_ops("PostgreSQL")
    assert rows, "expected operations for PostgreSQL"
    assert all("display_name" in r and "operation" in r for r in rows)
    names = {r["display_name"] for r in rows}
    assert "Tables" in names


def test_supported_object_types_normalisation():
    svc = _svc()
    types = svc.supported_object_types()
    # Display name "Tables" normalises into the alias map key "tables".
    assert "tables" in types
    assert "materializedviews" in types


# --------------------------------------------------------------------------- #
def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def test_api_ops_endpoint():
    c = _client()
    r = c.get("/api/databases/ops", params={"type": "PostgreSQL"})
    assert r.status_code == 200
    assert any(o["display_name"] == "Tables" for o in r.json())


def test_api_schema_route_registered():
    c = _client()
    r = c.get("/api/objects/nope/schema", params={"table": "t"})
    assert r.status_code != 404


def test_web_objects_exposes_tk_controls():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    html = c.get("/").text
    for token in (
        "objects-browser",
        "objects-types-pane",
        "objects-results-pane",
        "obj-type-buttons",
        "obj-results-title",
        "obj-results-count",
        "obj-table-card-template",
        "obj-import-jump",
        "obj-clear-results",
        "obj-card-schema",
        "obj-card-sample",
        "Load Sample Data",
        "obj-card-count",
        "obj-card-export",
        "Export Data",
        "obj-import",
    ):
        assert token in html
    # Tk parity: object types are clickable buttons in a left pane, not a
    # dropdown + List button workflow.
    assert 'id="obj-type"' not in html
    assert 'id="obj-list"' not in html
    assert 'id="obj-selected"' not in html


def test_web_objects_appjs_wires_tk_controls():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    for token in (
        "executeObjOperation",
        "renderObjCard",
        "obj-type-btn",
        "obj-card-schema",
        "obj-card-sample",
        "obj-card-count",
        "obj-card-export",
    ):
        assert token in js
    assert "$(\"#obj-type\")" not in js
    assert "$(\"#obj-list\")" not in js


# --------------------------------------------------------------------------- #
# Shared-spec single-source governance: the Objects action set lives in
# common/ui/shared/specs.py and every UI must consume it (no triple maintenance).
# --------------------------------------------------------------------------- #
def test_shared_objects_payload_shape():
    from common.ui.shared import specs

    payload = specs.objects_payload()
    for key in ("layout", "toolbarActions", "listActions", "detailActions",
                "exportFields", "importFields"):
        assert key in payload, f"objects payload missing {key}"

    assert payload["layout"]["objectTypesTitle"] == "Object types"
    assert payload["layout"]["resultsTitle"] == "Results"
    assert payload["layout"]["emptyResultsHint"] == \
        "Choose an object type on the left to list database objects."

    toolbar = {a["id"]: a["label"] for a in payload["toolbarActions"]}
    assert toolbar == {"refresh": "Refresh", "import_jump": "Import Data"}

    list_ids = [a["id"] for a in payload["listActions"]]
    assert list_ids == ["clear_results"]

    detail = [(a["id"], a["label"]) for a in payload["detailActions"]]
    assert detail == [
        ("schema", "Schema"),
        ("sample", "Load Sample Data"),
        ("count", "Row count"),
        ("export_selected", "Export Data"),
    ]


def test_web_ui_config_exposes_objects_spec():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    cfg = c.get("/ui/config").json()
    objects = cfg["specs"]["objects"]
    detail_ids = [a["id"] for a in objects["detailActions"]]
    assert detail_ids == ["schema", "sample", "count", "export_selected"]


def test_web_objects_appjs_applies_labels_from_shared_spec():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    assert "applyObjectsLabels" in js
    assert "SHARED_OBJ_ACTION_TO_DOM" in js
    assert "SHARED_OBJ_ACTION_TO_CLASS" in js
    assert "specs.objects" in js


# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeObjectsSvc:
    def list_connections(self):
        return [{"name": "local_pg", "db_type": "PostgreSQL"}]

    def list_db_ops(self, db_type):
        return [
            {"display_name": "Tables", "operation": "getTables"},
            {"display_name": "Views", "operation": "getViews"},
            {"display_name": "Functions", "operation": "getFunctions"},
        ]

    def supported_object_types(self):
        return ["tables", "views", "functions"]

    def open_connection(self, conn):
        return {"ok": True}

    def get_objects(self, conn, obj_type):
        if obj_type == "Tables":
            return ["users", "orders"]
        if obj_type == "Views":
            return ["active_users"]
        return ["fn_audit"]

    def get_table_schema(self, conn, table):
        return {"columns": [
            {"name": "id", "type": "INTEGER", "nullable": False, "default": None},
            {"name": "name", "type": "TEXT", "nullable": True, "default": None},
        ]}

    def sample_table(self, conn, table, limit=1):
        return {"columns": ["id", "name"], "rows": [[1, "Ada"]]}

    def count_table(self, conn, table):
        return {"count": 2}

    def export_table(self, conn, table, path, fmt="csv"):
        return {"ok": True, "message": f"Exported {table}."}

    def import_csv_to_table(self, conn, path, table=None, create_table=True):
        return {"ok": True, "message": "Imported."}


@pytest.mark.anyio
async def test_tui_objects_screen_composes_and_lists_types():
    from textual.widgets import Button, Select

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("objects")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeObjectsSvc()
        sel = scr.query_one("#obj-conn", Select)
        sel.set_options([("local_pg", "local_pg")])
        sel.value = "local_pg"
        await scr._populate_type_buttons()
        await pilot.pause()

        # Tk parity: object-type operations are clickable buttons in the left
        # pane. There is no object-type dropdown or separate List button.
        assert scr.query_one("#obj-type-0", Button).label.plain == "Tables"
        assert scr.query_one("#obj-type-1", Button).label.plain == "Views"
        for bid in (
            "obj-import-jump",
            "obj-clear-results",
            "obj-export",
            "obj-import",
        ):
            assert scr.query_one("#" + bid, Button) is not None


@pytest.mark.anyio
async def test_tui_objects_screen_mirrors_tk_layout_from_shared_spec():
    """TUI Objects screen shows the Tk layout: Object types pane + Results pane,
    with table cards for table/collection results."""
    from textual.widgets import Button, Static

    from common.ui.shared import specs
    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("objects")
        await pilot.pause()
        scr = app.screen
        payload = specs.objects_payload()
        layout = payload["layout"]
        assert scr.query_one("#obj-types-pane") is not None
        pane_text = " ".join(str(s.render()) for s in scr.query("#obj-types-pane Static"))
        assert layout["objectTypesTitle"] in pane_text
        assert scr.query_one("#obj-results-title", Static) is not None

        clear = scr.query_one("#obj-clear-results", Button)
        assert clear.label.plain == payload["listActions"][0]["label"]


@pytest.mark.anyio
async def test_tui_objects_clicking_object_type_renders_table_cards_and_actions():
    from textual.widgets import Button, DataTable, Select, Static

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("objects")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeObjectsSvc()
        sel = scr.query_one("#obj-conn", Select)
        sel.set_options([("local_pg", "local_pg")])
        sel.value = "local_pg"
        await scr._populate_type_buttons()
        await pilot.pause()

        scr.query_one("#obj-type-0", Button).press()
        await pilot.pause()
        assert "Tables" in str(scr.query_one("#obj-results-title", Static).render())
        assert "2 object(s)" in str(scr.query_one("#obj-results-count", Static).render())
        assert scr.query_one("#obj-card-0") is not None
        assert scr.query_one("#obj-card-schema-0", Button).label.plain == "Schema"
        assert scr.query_one("#obj-card-sample-0", Button).label.plain == "Load Sample Data"
        assert scr.query_one("#obj-card-count-0", Button).label.plain == "Row count"
        assert scr.query_one("#obj-card-export-0", Button).label.plain == "Export Data"

        scr.query_one("#obj-card-schema-0", Button).press()
        await pilot.pause()
        detail = scr.query_one("#obj-card-detail-0", DataTable)
        assert detail.row_count == 2
