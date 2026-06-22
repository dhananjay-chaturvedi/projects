"""SQL editor and results screen.

Functional parity with the desktop SQL Editor: connection select + refresh,
autocommit toggle, Execute at cursor / Execute selected / Execute all, Load /
Save query, Format SQL, Autocomplete toggle, Commit / Rollback / Stop Query /
Clear editor, multi-result navigation, result export/copy helpers, and query
history.
"""

from __future__ import annotations

from typing import Any

from textual.containers import Horizontal
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Label,
    ListItem,
    ListView,
    Select,
    Static,
    Tab,
    Tabs,
    TextArea,
)

from common.ui.shared import specs
from common.ui.textual.screens.base import BaseScreen
from common.ui.textual.screens.form_modal import FormModal

# Shared-spec action id -> TUI button id (ids are stable; labels come from spec).
_TOOLBAR_BTN = {
    "run_cursor": "sql-run-cursor",
    "run_selected": "sql-run-sel",
    "run_all": "sql-run-all",
    "stop": "sql-stop",
    "clear": "sql-clear",
    "load": "sql-load",
    "save": "sql-save",
    "format": "sql-format",
    "autocomplete": "sql-autocomplete",
    "commit": "sql-commit",
    "rollback": "sql-rollback",
}
_RESULT_BTN = {
    "copy_all": "sql-copy-all",
    "sort_asc": "sql-sort-asc",
    "sort_desc": "sql-sort-desc",
    "filter": "sql-filter",
    "clear_filter": "sql-clear-filter",
    "clear_results": "sql-clear-results",
    "export": "sql-export",
}


class SqlEditorScreen(BaseScreen):
    """Execute SQL against a selected connection."""

    NAV_ID = "sql"

    BINDINGS = BaseScreen.BINDINGS + [
        ("ctrl+enter", "run", "Run"),
        ("f5", "run", "Run"),
    ]

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._results: list[dict] = []
        self._history: list[str] = []
        self._autocomplete = True
        self._autocomplete_labels = ("Autocomplete: On", "Autocomplete: Off")
        # Multi-tab buffers (parity with Tk/Web): each tab keeps its own SQL +
        # results. Tabs are append-only here, so a tab's id suffix == its index.
        self._tabs_state: list[dict] = [{"sql": "SELECT 1;", "results": []}]
        self._active_tab = 0
        # Unfiltered rows of the result currently shown, kept so "Clear Filter"
        # can restore the full set (parity with Tk's tab_frame.original_rows).
        self._filter_backup: tuple[int, list] | None = None

    def screen_title(self) -> str:
        return "SQL Editor"

    def compose_body(self):
        spec = specs.sql_editor_payload()
        conn = {a["id"]: a["label"] for a in spec["connectionActions"]}
        tool = {a["id"]: a for a in spec["editorActions"]}
        res = {a["id"]: a for a in spec["resultActions"]}
        self._autocomplete_labels = (
            tool["autocomplete"]["label"],
            tool["autocomplete"].get("labelOff", "Autocomplete: Off"),
        )
        names = [c["name"] for c in self.svc.list_connections()]

        # Multi-tab strip: Tabs + a "+" new-tab button (mirrors Tk/Web).
        with Horizontal(classes="actions-row"):
            yield Tabs(Tab("Tab 1", id="sqltab-0"), id="sql-tab-strip")
            yield Button(spec["tabActions"][0]["label"], id="sql-tab-new", classes="mini")

        with Horizontal(classes="actions-row"):
            yield Label("Connection ")
            yield Select([(n, n) for n in names] or [("(none)", "")],
                         id="sql-conn", allow_blank=True)
            yield Button(conn["refresh"], id="sql-refresh", classes="mini")
            yield Checkbox(spec["autocommitLabel"], id="sql-autocommit")

        yield TextArea("SELECT 1;", id="sql-input", language="sql")

        with Horizontal(classes="actions-row"):
            yield Button(tool["run_cursor"]["label"], id="sql-run-cursor", variant="primary")
            yield Button(tool["run_selected"]["label"], id="sql-run-sel")
            yield Button(tool["run_all"]["label"], id="sql-run-all")
            yield Button(tool["stop"]["label"], id="sql-stop", variant="error")
            yield Button(tool["clear"]["label"], id="sql-clear")
            yield Button(tool["load"]["label"], id="sql-load")
            yield Button(tool["save"]["label"], id="sql-save")
            yield Button(tool["format"]["label"], id="sql-format")
            yield Button(tool["autocomplete"]["label"], id="sql-autocomplete")
            yield Button(tool["commit"]["label"], id="sql-commit")
            yield Button(tool["rollback"]["label"], id="sql-rollback")
        yield Static("", id="sql-status", classes="status")

        with Horizontal(classes="actions-row"):
            yield Label("Result ")
            yield Select([], id="sql-result-pick", allow_blank=True)
            yield Button(res["copy_all"]["label"], id="sql-copy-all")
            yield Button(res["sort_asc"]["label"], id="sql-sort-asc")
            yield Button(res["sort_desc"]["label"], id="sql-sort-desc")
            yield Button(res["filter"]["label"], id="sql-filter")
            yield Button(res["clear_filter"]["label"], id="sql-clear-filter")
            yield Button(res["clear_results"]["label"], id="sql-clear-results")
            yield Button(res["export"]["label"], id="sql-export")
        yield DataTable(id="sql-results", zebra_stripes=True)

        yield Static("[b]Query history[/] (select to load)", classes="section")
        yield ListView(id="sql-history")

    def on_mount(self) -> None:
        self._refresh_autocommit()
        self._consume_pending_sql()

    def on_screen_resume(self) -> None:
        self._consume_pending_sql()

    def _consume_pending_sql(self) -> None:
        """Load SQL handed off from another screen (e.g. AI → Send to Editor)."""
        pending = getattr(self.app, "_pending_sql_editor", None)
        if not pending:
            return
        self.app._pending_sql_editor = None  # type: ignore[attr-defined]
        self.query_one("#sql-input", TextArea).text = pending.get("sql", "")
        conn = pending.get("conn") or ""
        if conn:
            try:
                self.query_one("#sql-conn", Select).value = conn
            except Exception:  # noqa: BLE001
                pass
        self._status("Loaded SQL from AI Query.")

    # ------------------------------------------------------------------ #
    def _conn(self) -> str:
        return str(self.query_one("#sql-conn", Select).value or "")

    def _status(self, msg: str) -> None:
        self.query_one("#sql-status", Static).update(msg)

    def _ensure_open(self, conn: str) -> None:
        if hasattr(self.svc, "open_connection"):
            try:
                self.svc.open_connection(conn)
            except Exception:
                pass

    def _refresh_autocommit(self) -> None:
        conn = self._conn()
        if not conn or not hasattr(self.svc, "get_autocommit"):
            return
        r = self.svc.get_autocommit(conn)
        if r.get("ok"):
            self.query_one("#sql-autocommit", Checkbox).value = bool(r.get("autocommit"))

    def on_select_changed(self, event: Select.Changed) -> None:
        sid = event.select.id or ""
        if sid == "sql-conn":
            self._refresh_autocommit()
        elif sid == "sql-result-pick" and event.value not in (None, Select.BLANK):
            try:
                self._show_result(self._results[int(event.value)])
            except (TypeError, ValueError, IndexError):
                pass

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if (event.checkbox.id or "") != "sql-autocommit":
            return
        conn = self._conn()
        if not conn:
            return
        self._ensure_open(conn)
        if hasattr(self.svc, "set_autocommit"):
            r = self.svc.set_autocommit(conn, event.value)
            self._status(r.get("message", ""))

    # ------------------------------------------------------------------ #
    def _show_result(self, result: dict) -> None:
        table = self.query_one("#sql-results", DataTable)
        table.clear(columns=True)
        if result.get("error"):
            table.add_columns("error")
            table.add_row(str(result["error"]))
            return
        cols = result.get("columns") or []
        if cols:
            table.add_columns(*[str(c) for c in cols])
        for row in result.get("rows") or []:
            table.add_row(*[str(v) if v is not None else "" for v in row])

    # ------------------------------------------------------------------ #
    # Multi-tab editor buffers.
    def _save_active_tab(self) -> None:
        if 0 <= self._active_tab < len(self._tabs_state):
            self._tabs_state[self._active_tab]["sql"] = \
                self.query_one("#sql-input", TextArea).text
            self._tabs_state[self._active_tab]["results"] = list(self._results)

    def _render_active_results(self) -> None:
        self.query_one("#sql-result-pick", Select).set_options([])
        self.query_one("#sql-results", DataTable).clear(columns=True)
        if self._results:
            self._populate_result_picker()

    def _switch_to_tab(self, idx: int) -> None:
        if idx == self._active_tab or not (0 <= idx < len(self._tabs_state)):
            return
        self._save_active_tab()
        self._active_tab = idx
        st = self._tabs_state[idx]
        self.query_one("#sql-input", TextArea).text = st.get("sql", "")
        self._results = list(st.get("results") or [])
        self._render_active_results()

    def _activate_new_tab(self, idx: int) -> None:
        try:
            self.query_one("#sql-tab-strip", Tabs).active = f"sqltab-{idx}"
        except Exception:  # noqa: BLE001
            pass

    def on_tabs_tab_activated(self, event: Tabs.TabActivated) -> None:
        if (event.tabs.id or "") != "sql-tab-strip" or event.tab is None:
            return
        try:
            idx = int((event.tab.id or "").split("-")[1])
        except (ValueError, IndexError):
            return
        self._switch_to_tab(idx)

    def _populate_result_picker(self) -> None:
        pick = self.query_one("#sql-result-pick", Select)
        opts = []
        for i, r in enumerate(self._results):
            label = f"#{i+1} error" if r.get("error") else \
                f"#{i+1} ({r.get('rowcount', len(r.get('rows') or []))} rows)"
            opts.append((label, str(i)))
        pick.set_options(opts)
        if opts:
            pick.value = "0"
            self._show_result(self._results[0])

    def _add_history(self, sql: str) -> None:
        sql = sql.strip()
        if not sql:
            return
        self._history = [sql] + [h for h in self._history if h != sql]
        self._history = self._history[:50]
        lv = self.query_one("#sql-history", ListView)
        lv.clear()
        for h in self._history:
            short = h if len(h) <= 100 else h[:100] + "…"
            lv.append(ListItem(Static(short)))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if (event.list_view.id or "") != "sql-history":
            return
        idx = event.list_view.index
        if idx is not None and 0 <= idx < len(self._history):
            self.query_one("#sql-input", TextArea).text = self._history[idx]

    # ------------------------------------------------------------------ #
    def action_run(self) -> None:
        self._run(self._statement_at_cursor())

    def _statement_at_cursor(self) -> str:
        text = self.query_one("#sql-input", TextArea).text
        # Textual's cursor API has varied across versions; use full text as a
        # stable fallback while keeping the same visible Tk action.
        return text.strip()

    def _run(self, sql_text: str | None = None) -> None:
        conn = self._conn()
        if not conn:
            self._status("Select a connection.")
            return
        sql = (sql_text if sql_text is not None
               else self.query_one("#sql-input", TextArea).text).strip()
        if not sql:
            self._status("Nothing to run.")
            return
        self._ensure_open(conn)
        if hasattr(self.svc, "execute_multi"):
            r = self.svc.execute_multi(conn, sql)
            self._results = [x.get("result", {}) for x in (r.get("results") or [])]
            self._populate_result_picker()
            self._add_history(sql)
            if r.get("error"):
                self._status(f"Error: {r['error']}")
            else:
                self._status(f"OK — {r.get('count')} statement(s) executed.")
        else:
            res = self.svc.execute(conn, sql)
            self._results = [res]
            self._populate_result_picker()
            self._add_history(sql)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        conn = self._conn()
        if bid == "sql-tab-new":
            self._save_active_tab()
            n = len(self._tabs_state)
            self._tabs_state.append({"sql": "", "results": []})
            tabs = self.query_one("#sql-tab-strip", Tabs)
            tabs.add_tab(Tab(f"Tab {n + 1}", id=f"sqltab-{n}"))
            # Activate once mounted; the activation handler switches buffers.
            self.call_after_refresh(self._activate_new_tab, n)
            self._status(f"Opened Tab {n + 1}.")
        elif bid == "sql-refresh":
            sel = self.query_one("#sql-conn", Select)
            names = [c["name"] for c in self.svc.list_connections()]
            sel.set_options([(n, n) for n in names])
            self._status("Connections refreshed.")
        elif bid == "sql-run-cursor":
            self._run(self._statement_at_cursor())
        elif bid == "sql-run-sel":
            sel_text = self.query_one("#sql-input", TextArea).selected_text
            if not sel_text.strip():
                self._status("Select SQL text first.")
                return
            self._run(sel_text)
        elif bid == "sql-run-all":
            self._run(self.query_one("#sql-input", TextArea).text)
        elif bid == "sql-format":
            if hasattr(self.svc, "format_sql"):
                ta = self.query_one("#sql-input", TextArea)
                r = self.svc.format_sql(ta.text)
                if r.get("ok"):
                    ta.text = r["sql"]
                    self._status("Formatted.")
                else:
                    self._status(r.get("message", "Format failed."))
        elif bid == "sql-load":
            self._load_query()
        elif bid == "sql-save":
            self._save_query()
        elif bid == "sql-autocomplete":
            self._autocomplete = not self._autocomplete
            on_label, off_label = self._autocomplete_labels
            event.button.label = on_label if self._autocomplete else off_label
            self._status("Autocomplete enabled." if self._autocomplete else "Autocomplete disabled.")
        elif bid == "sql-stop":
            if conn and hasattr(self.svc, "cancel_query"):
                r = self.svc.cancel_query(conn)
                self._status(r.get("message", ""))
        elif bid == "sql-clear":
            self.query_one("#sql-input", TextArea).text = ""
        elif bid == "sql-clear-results":
            self._results = []
            self.query_one("#sql-results", DataTable).clear(columns=True)
            self.query_one("#sql-result-pick", Select).set_options([])
            self._status("Results cleared.")
        elif bid == "sql-copy-all":
            self._status("Copy All Data: select rows in the table and use terminal copy.")
        elif bid == "sql-sort-asc":
            self._sort_current_result(True)
        elif bid == "sql-sort-desc":
            self._sort_current_result(False)
        elif bid == "sql-filter":
            self._filter_result()
        elif bid == "sql-clear-filter":
            self._clear_filter()
        elif bid == "sql-export":
            self._export_result()
        elif bid == "sql-commit":
            if conn:
                self._status(self.svc.commit(conn).get("message", ""))
        elif bid == "sql-rollback":
            if conn:
                self._status(self.svc.rollback(conn).get("message", ""))

    def _sort_current_result(self, ascending: bool) -> None:
        if not self._results:
            self._status("No result to sort.")
            return
        r = self._results[0]
        rows = r.get("rows") or []
        if not rows:
            self._status("No rows to sort.")
            return
        r["rows"] = sorted(rows, key=lambda row: str(row[0] if row else ""), reverse=not ascending)
        self._show_result(r)
        self._status("Sorted ascending." if ascending else "Sorted descending.")

    # ------------------------------------------------------------------ #
    # File load/save, export, and result filtering (Tk parity).
    def _current_index(self) -> int:
        """Index of the result currently shown in the picker (0 by default)."""
        try:
            v = self.query_one("#sql-result-pick", Select).value
            return int(v) if v not in (None, Select.BLANK) else 0
        except (TypeError, ValueError):
            return 0

    def _current_result(self) -> dict | None:
        idx = self._current_index()
        if 0 <= idx < len(self._results):
            return self._results[idx]
        return None

    def _load_query(self) -> None:
        def _done(v: dict | None) -> None:
            if not v:
                return
            path = (v.get("path") or "").strip()
            if not path:
                self._status("No file path given.")
                return
            try:
                import os
                with open(os.path.expanduser(path), "r", encoding="utf-8") as fh:
                    text = fh.read()
            except OSError as exc:
                self._status(f"Load failed: {exc}")
                return
            self.query_one("#sql-input", TextArea).text = text
            self._status(f"Loaded query from {path}.")

        self.app.push_screen(
            FormModal("Load query from file",
                      [{"name": "path", "label": "File path (.sql / .txt)",
                        "placeholder": "/path/to/query.sql"}],
                      submit_label="Load"),
            _done,
        )

    def _save_query(self) -> None:
        sql = self.query_one("#sql-input", TextArea).text
        if not sql.strip():
            self._status("No query to save.")
            return

        def _done(v: dict | None) -> None:
            if not v:
                return
            path = (v.get("path") or "").strip()
            if not path:
                self._status("No file path given.")
                return
            try:
                import os
                with open(os.path.expanduser(path), "w", encoding="utf-8") as fh:
                    fh.write(sql)
            except OSError as exc:
                self._status(f"Save failed: {exc}")
                return
            self._status(f"Saved query to {path}.")

        self.app.push_screen(
            FormModal("Save query to file",
                      [{"name": "path", "label": "File path (.sql / .txt)",
                        "value": "query.sql"}],
                      submit_label="Save"),
            _done,
        )

    def _export_result(self) -> None:
        result = self._current_result()
        if not result or result.get("error") or not (result.get("rows")):
            self._status("No result rows to export.")
            return

        def _done(v: dict | None) -> None:
            if not v:
                return
            path = (v.get("path") or "").strip()
            fmt = (v.get("format") or "csv").lower()
            if not path:
                self._status("No file path given.")
                return
            try:
                import os
                from common.io.export_utils import (
                    export_result_to_csv, export_rows_to_json,
                )
                full = os.path.expanduser(path)
                if fmt == "json":
                    export_rows_to_json(full, result.get("rows") or [],
                                        columns=result.get("columns") or [])
                else:
                    export_result_to_csv(full, result)
            except OSError as exc:
                self._status(f"Export failed: {exc}")
                return
            n = len(result.get("rows") or [])
            self._status(f"Exported {n} row(s) as {fmt.upper()} to {path}.")

        self.app.push_screen(
            FormModal("Export current result",
                      [{"name": "path", "label": "File path",
                        "value": "result.csv"},
                       {"name": "format", "label": "Format", "type": "select",
                        "options": [("csv", "csv"), ("json", "json")],
                        "value": "csv"}],
                      submit_label="Export"),
            _done,
        )

    def _filter_result(self) -> None:
        result = self._current_result()
        if not result or result.get("error") or not (result.get("rows")):
            self._status("No result rows to filter.")
            return
        cols = [str(c) for c in (result.get("columns") or [])]
        if not cols:
            self._status("Result has no columns to filter.")
            return
        idx = self._current_index()

        def _done(v: dict | None) -> None:
            if not v:
                return
            column = v.get("column") or cols[0]
            needle = (v.get("contains") or "").strip()
            if not needle:
                self._status("Enter a value to filter by.")
                return
            try:
                col_pos = cols.index(str(column))
            except ValueError:
                col_pos = 0
            if self._filter_backup is None or self._filter_backup[0] != idx:
                self._filter_backup = (idx, list(result.get("rows") or []))
            base_rows = self._filter_backup[1]
            low = needle.lower()
            filtered = [r for r in base_rows
                        if col_pos < len(r) and low in str(r[col_pos]).lower()]
            result["rows"] = filtered
            self._show_result(result)
            self._status(
                f"Filtered {column} contains '{needle}': {len(filtered)} row(s)."
            )

        self.app.push_screen(
            FormModal("Filter result column",
                      [{"name": "column", "label": "Column", "type": "select",
                        "options": [(c, c) for c in cols], "value": cols[0]},
                       {"name": "contains", "label": "Contains (case-insensitive)",
                        "placeholder": "substring"}],
                      submit_label="Apply filter"),
            _done,
        )

    def _clear_filter(self) -> None:
        if self._filter_backup is None:
            self._status("No active filter.")
            return
        idx, rows = self._filter_backup
        if 0 <= idx < len(self._results):
            self._results[idx]["rows"] = rows
            if idx == self._current_index():
                self._show_result(self._results[idx])
        self._filter_backup = None
        self._status("Filter cleared.")
