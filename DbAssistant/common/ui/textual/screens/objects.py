"""Database objects browser screen.

The layout intentionally mirrors the Tk Database Objects tab: a header selector,
an Object types pane with one button per engine operation, and a Results pane
that renders tables/collections as expandable cards with schema/sample/export
actions.
"""

from __future__ import annotations

import asyncio
from typing import Any

from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Checkbox, Collapsible, DataTable, Input, Label, Select, Static

from common.ui.shared import specs
from common.ui.textual.screens.base import BaseScreen


class ObjectsScreen(BaseScreen):
    """Browse database objects and inspect table-like objects."""

    NAV_ID = "objects"

    DEFAULT_CSS = """
    #obj-browser {
        height: 1fr;
    }
    #obj-types-pane {
        width: 30%;
        min-width: 24;
        height: 1fr;
    }
    #obj-results-pane {
        width: 70%;
        height: 1fr;
    }
    #obj-type-buttons, #obj-results-content {
        height: 1fr;
        overflow-y: auto;
    }
    .object-card-actions {
        height: auto;
    }
    """

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._items: list[str] = []
        self._ops: list[dict[str, str]] = []
        self._active_title = ""
        self._active_type = ""
        self._card_names: dict[int, str] = {}
        self._export_target = ""
        self._type_buttons_lock = asyncio.Lock()

    def screen_title(self) -> str:
        return "Database Objects"

    def compose_body(self):
        spec = specs.objects_payload()
        layout = spec["layout"]
        toolbar = {a["id"]: a["label"] for a in spec["toolbarActions"]}
        list_actions = {a["id"]: a["label"] for a in spec["listActions"]}

        names = [c["name"] for c in self.svc.list_connections()]

        yield Static(layout["headerTitle"], classes="section")
        with Horizontal(classes="actions-row"):
            yield Label("Connection:")
            yield Select([(n, n) for n in names] or [("(none)", "")],
                         id="obj-conn", allow_blank=True)
            yield Button(toolbar["refresh"], id="obj-refresh", classes="mini")
            yield Button(toolbar["import_jump"], id="obj-import-jump")
        yield Static("Connect from the Connections tab to browse objects.",
                     id="obj-info", classes="status")

        with Horizontal(id="obj-browser"):
            with Vertical(id="obj-types-pane"):
                yield Static(layout["objectTypesTitle"], classes="section")
                yield Button(list_actions["clear_results"], id="obj-clear-results")
                yield Static(layout["objectTypesHint"], classes="status")
                yield Vertical(id="obj-type-buttons")

            with Vertical(id="obj-results-pane"):
                with Horizontal(classes="actions-row"):
                    yield Static(layout["emptyResultsTitle"], id="obj-results-title",
                                 classes="section")
                    yield Static("", id="obj-results-count", classes="status")
                with Horizontal(classes="actions-row"):
                    yield Label(layout["filterLabel"])
                    yield Input(id="obj-filter", placeholder="substring")
                    yield Button(layout["filterClearLabel"], id="obj-filter-clear",
                                 classes="mini")
                yield Vertical(Static(layout["emptyResultsHint"], classes="status"),
                               id="obj-results-content")

        with Collapsible(title="Export / Import (server-side paths)", collapsed=True):
            yield Label("Export table")
            yield Input(id="obj-exp-table", placeholder="table")
            yield Label("Export: output path")
            yield Input(id="obj-exp-path", placeholder="/tmp/out.csv")
            with Horizontal(classes="actions-row"):
                yield Label("Format ")
                yield Select([("csv", "csv"), ("json", "json")],
                             id="obj-exp-fmt", value="csv", allow_blank=False)
                yield Button("Export Data", id="obj-export", variant="primary")
            yield Label("Import: CSV path")
            yield Input(id="obj-imp-path", placeholder="/tmp/in.csv")
            with Horizontal(classes="actions-row"):
                yield Label("Target table ")
                yield Input(id="obj-imp-table", placeholder="(optional)")
                yield Checkbox("Create if missing", value=True, id="obj-imp-create")
                yield Button("Import CSV", id="obj-import", variant="primary")
        yield Static("", id="obj-status", classes="status")

    async def on_mount(self) -> None:
        await self._populate_type_buttons()

    # ------------------------------------------------------------------ #
    def _conn(self) -> str:
        return str(self.query_one("#obj-conn", Select).value or "")

    def _db_type(self) -> str:
        name = self._conn()
        for c in self.svc.list_connections():
            if c.get("name") == name:
                return c.get("db_type", c.get("type", ""))
        return ""

    def _status(self, msg: str) -> None:
        self.query_one("#obj-status", Static).update(msg)

    def _ensure_open(self, conn: str) -> None:
        if hasattr(self.svc, "open_connection"):
            try:
                self.svc.open_connection(conn)
            except Exception:
                pass

    def _operation_specs(self) -> list[dict[str, str]]:
        db_type = self._db_type()
        if db_type and hasattr(self.svc, "list_db_ops"):
            rows = self.svc.list_db_ops(db_type) or []
            if rows:
                return [
                    {"display_name": str(r["display_name"]),
                     "operation": str(r.get("operation", ""))}
                    for r in rows
                ]
        return [
            {"display_name": str(t).replace("_", " ").title(), "operation": str(t)}
            for t in self.svc.supported_object_types()
        ]

    async def _populate_type_buttons(self) -> None:
        async with self._type_buttons_lock:
            self._ops = self._operation_specs()
            container = self.query_one("#obj-type-buttons", Vertical)
            await container.remove_children()
            connected = bool(self._conn())
            for idx, op in enumerate(self._ops):
                await container.mount(
                    Button(op["display_name"], id=f"obj-type-{idx}", disabled=not connected)
                )
            db_type = self._db_type()
            if connected:
                self.query_one("#obj-info", Static).update(
                    f"{db_type}  ·  {len(self._ops)} browse operations  ·  connection: {self._conn()}"
                )
            else:
                self.query_one("#obj-info", Static).update(
                    "No active connection — connect from the Connections tab."
                )

    async def on_select_changed(self, event: Select.Changed) -> None:
        if (event.select.id or "") == "obj-conn":
            await self._populate_type_buttons()

    async def on_input_changed(self, event: Input.Changed) -> None:
        if (event.input.id or "") == "obj-filter":
            await self._render_results()

    def _filtered_items(self) -> list[str]:
        needle = self.query_one("#obj-filter", Input).value.strip().lower()
        return [n for n in self._items if not needle or needle in str(n).lower()]

    @staticmethod
    def _is_tabular_list(title: str) -> bool:
        return title.lower() in ("tables", "collections")

    async def _render_results(self) -> None:
        content = self.query_one("#obj-results-content", Vertical)
        await content.remove_children()
        rows = self._filtered_items()
        self.query_one("#obj-results-title", Static).update(
            self._active_title or specs.objects_payload()["layout"]["emptyResultsTitle"]
        )
        self.query_one("#obj-results-count", Static).update(
            f"{len(rows)} object(s)" if self._active_title else ""
        )
        self._card_names.clear()

        if not self._active_title:
            await content.mount(Static(specs.objects_payload()["layout"]["emptyResultsHint"],
                                       classes="status"))
            return

        if self._is_tabular_list(self._active_title):
            await content.mount(Static(specs.objects_payload()["layout"]["tableCardHint"],
                                       classes="status"))
            for idx, name in enumerate(rows):
                self._card_names[idx] = str(name)
                await content.mount(self._table_card(idx, str(name)))
            return

        table = DataTable(id="obj-list-table", zebra_stripes=True)
        table.add_columns("Object name")
        for name in rows:
            table.add_row(str(name))
        await content.mount(table)

    def _table_card(self, idx: int, name: str) -> Collapsible:
        actions = {a["id"]: a["label"] for a in specs.objects_payload()["detailActions"]}
        detail = DataTable(id=f"obj-card-detail-{idx}", zebra_stripes=True)
        body = Vertical(
            Horizontal(
                Button(actions["schema"], id=f"obj-card-schema-{idx}", variant="primary"),
                Button(actions["sample"], id=f"obj-card-sample-{idx}"),
                Button(actions["count"], id=f"obj-card-count-{idx}"),
                Button(actions["export_selected"], id=f"obj-card-export-{idx}"),
                classes="actions-row object-card-actions",
            ),
            detail,
        )
        return Collapsible(body, title=name, collapsed=True, id=f"obj-card-{idx}")

    def _table_name_from_button(self, bid: str, prefix: str) -> str:
        try:
            return self._card_names[int(bid.removeprefix(prefix))]
        except (KeyError, ValueError):
            return ""

    def _show_card_detail(self, idx: int, columns: list[str], rows: list[list]) -> None:
        out = self.query_one(f"#obj-card-detail-{idx}", DataTable)
        out.clear(columns=True)
        if columns:
            out.add_columns(*[str(c) for c in columns])
        for row in rows:
            out.add_row(*[str(v) if v is not None else "" for v in row])

    # ------------------------------------------------------------------ #
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        conn = self._conn()

        if bid == "obj-refresh":
            sel = self.query_one("#obj-conn", Select)
            names = [c["name"] for c in self.svc.list_connections()]
            sel.set_options([(n, n) for n in names])
            await self._populate_type_buttons()
            self._status("Refreshed.")
            return
        if bid == "obj-import-jump":
            self._status("Import Data: fill Import CSV path and click Import CSV.")
            return
        if bid == "obj-filter-clear":
            self.query_one("#obj-filter", Input).value = ""
            await self._render_results()
            return
        if bid == "obj-clear-results":
            self._items = []
            self._active_title = ""
            self._active_type = ""
            self._export_target = ""
            self.query_one("#obj-exp-table", Input).value = ""
            await self._render_results()
            self._status("Results cleared.")
            return
        if bid.startswith("obj-type-"):
            if not conn:
                self._status("Select a connection.")
                return
            await self._execute_operation(int(bid.removeprefix("obj-type-")))
            return
        if bid.startswith("obj-card-schema-"):
            idx = int(bid.removeprefix("obj-card-schema-"))
            self._load_schema(idx, self._table_name_from_button(bid, "obj-card-schema-"))
            return
        if bid.startswith("obj-card-sample-"):
            idx = int(bid.removeprefix("obj-card-sample-"))
            self._load_sample(idx, self._table_name_from_button(bid, "obj-card-sample-"))
            return
        if bid.startswith("obj-card-count-"):
            self._load_count(self._table_name_from_button(bid, "obj-card-count-"))
            return
        if bid.startswith("obj-card-export-"):
            table = self._table_name_from_button(bid, "obj-card-export-")
            self._export_target = table
            self.query_one("#obj-exp-table", Input).value = table
            self._status("Export Data: enter an output path and click Export Data.")
            return
        if bid == "obj-export":
            self._export_table(conn)
            return
        if bid == "obj-import":
            self._import_csv(conn)

    async def _execute_operation(self, idx: int) -> None:
        conn = self._conn()
        if not (0 <= idx < len(self._ops)):
            return
        op = self._ops[idx]
        obj_type = op["display_name"]
        self._ensure_open(conn)
        items = self.svc.get_objects(conn, obj_type)
        if items and isinstance(items[0], dict) and items[0].get("error"):
            self._status(items[0]["error"])
            return
        self._active_title = obj_type
        self._active_type = op.get("operation", obj_type)
        self._items = [
            i if isinstance(i, str) else i.get("name", str(i))
            for i in (items or [])
        ]
        await self._render_results()
        self._status(f"Found {len(self._items)} {obj_type.lower()}.")

    def _load_schema(self, idx: int, table: str) -> None:
        if not table:
            return
        r = self.svc.get_table_schema(self._conn(), table)
        if r.get("error"):
            self._status(r["error"])
            return
        rows = [
            [c.get("name", c.get("column", "")),
             c.get("type", c.get("data_type", "")),
             c.get("nullable", c.get("null", "")),
             c.get("default", "")]
            for c in (r.get("columns") or [])
        ]
        self._show_card_detail(idx, ["Column", "Type", "Nullable", "Default"], rows)
        self._status(f"Schema of {table}: {len(rows)} columns.")

    def _load_sample(self, idx: int, table: str) -> None:
        if not table:
            return
        r = self.svc.sample_table(self._conn(), table, limit=1)
        if r.get("error"):
            self._status(r["error"])
            return
        self._show_card_detail(idx, r.get("columns") or [], r.get("rows") or [])
        self._status(f"Sampled {table} ({len(r.get('rows') or [])} rows).")

    def _load_count(self, table: str) -> None:
        if not table:
            return
        r = self.svc.count_table(self._conn(), table)
        if r.get("error"):
            self._status(r["error"])
            return
        self._status(f"{table}: {r.get('count')} rows.")

    def _export_table(self, conn: str) -> None:
        if not conn:
            self._status("Select a connection.")
            return
        table = self.query_one("#obj-exp-table", Input).value.strip() or self._export_target
        path = self.query_one("#obj-exp-path", Input).value.strip()
        if not table or not path:
            self._status("Enter an export table and output path.")
            return
        r = self.svc.export_table(conn, table, path,
                                  fmt=str(self.query_one("#obj-exp-fmt", Select).value))
        self._status(r.get("message", str(r)))

    def _import_csv(self, conn: str) -> None:
        if not conn:
            self._status("Select a connection.")
            return
        path = self.query_one("#obj-imp-path", Input).value.strip()
        if not path:
            self._status("Enter a CSV path.")
            return
        r = self.svc.import_csv_to_table(
            conn, path,
            table=self.query_one("#obj-imp-table", Input).value.strip() or None,
            create_table=self.query_one("#obj-imp-create", Checkbox).value,
        )
        self._status(r.get("message", str(r)))
