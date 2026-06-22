"""
Database Objects tab — browse engine objects, view table schemas, export table data.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import threading
import tkinter as tk
from dataclasses import dataclass
from io import StringIO
from tkinter import filedialog, messagebox, ttk
from typing import Callable

from common.config_loader import console_print, properties
from common.database_registry import DatabaseRegistry
from common.ui.tk import ColorTheme, create_horizontal_scrollable, make_scrollable
from common.io.export_utils import export_result_to_csv, export_rows_to_json, cell_to_str


@dataclass(frozen=True)
class ObjectsConnectionContext:
    get_connections: Callable[[], dict]
    get_current_connection: Callable[[], str | None]
    set_current_connection: Callable[[str | None], None]
    get_db_type: Callable[[], str | None]
    set_db_type: Callable[[str | None], None]
    db_query_lock: object
    get_available_operations: Callable[[str | None], list]


@dataclass(frozen=True)
class ObjectsPanelActions:
    update_status: Callable[[str, str], None]
    import_data_callback: Callable[[], None]


@dataclass(frozen=True)
class ObjectsPanelFonts:
    ui: object
    mono: object


class DatabaseObjectsPanel:
    """Database Objects workspace embedded in the master notebook."""

    def __init__(
        self,
        parent: tk.Widget,
        root: tk.Tk,
        context: ObjectsConnectionContext,
        actions: ObjectsPanelActions,
        fonts: ObjectsPanelFonts,
    ):
        self.parent = parent
        self.root = root
        self._get_connections = context.get_connections
        self._get_current_connection = context.get_current_connection
        self._set_current_connection = context.set_current_connection
        self._get_db_type = context.get_db_type
        self._set_db_type = context.set_db_type
        self.db_query_lock = context.db_query_lock
        self.update_status = actions.update_status
        self._import_data = actions.import_data_callback
        self._get_operations = context.get_available_operations
        self._font_ui = fonts.ui
        self._font_mono = fonts.mono
        self._title_font = (fonts.ui[0], fonts.ui[1] + 1, "bold")

        self.connection_combo: ttk.Combobox | None = None
        self.operation_buttons: list[ttk.Button] = []
        self.paned: ttk.Panedwindow | None = None
        self._info_label: ttk.Label | None = None
        self._results_title_label: ttk.Label | None = None
        self._results_count_label: ttk.Label | None = None
        self._filter_var = tk.StringVar(value="")
        self._active_operation: str | None = None

        self._results_canvas_container: ttk.Frame | None = None
        self._results_list_container: ttk.Frame | None = None
        self._results_canvas: tk.Canvas | None = None
        self._results_content_frame: ttk.Frame | None = None
        self._results_text_container: ttk.Frame | None = None
        self._results_text = None
        self._objects_tree: ttk.Treeview | None = None

        self._current_result_title = ""
        self._current_result_items: list = []
        self._table_section_widgets: dict[str, tk.Frame] = {}

    @staticmethod
    def _is_tabular_list(title: str) -> bool:
        return title.lower() in ("tables", "collections")

    def build(self) -> None:
        header = ttk.Frame(self.parent)
        header.pack(fill=tk.X, padx=10, pady=(8, 4))

        selector_outer = ttk.Frame(header)
        selector_outer.pack(fill=tk.X)
        selector_frame = create_horizontal_scrollable(selector_outer)

        ttk.Label(selector_frame, text="Connection:", font=self._font_ui).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        self.connection_combo = ttk.Combobox(
            selector_frame, state="readonly", width=36, font=self._font_ui
        )
        self.connection_combo.pack(side=tk.LEFT, padx=4)
        self.connection_combo.bind("<<ComboboxSelected>>", self._on_connection_changed)

        ttk.Button(
            selector_frame, text="Refresh", command=self.refresh_connections, width=10
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(
            selector_frame,
            text="Import Data",
            style="Success.TButton",
            command=self._import_data,
            width=12,
        ).pack(side=tk.LEFT, padx=4)

        self._info_label = ttk.Label(
            header,
            text="Connect from the Connections tab to browse objects.",
            foreground=ColorTheme.TEXT_SECONDARY,
            font=self._font_ui,
        )
        self._info_label.pack(anchor=tk.W, pady=(6, 0))

        self.paned = ttk.Panedwindow(self.parent, orient=tk.HORIZONTAL)
        self.paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        self._build_operations_pane()
        self._build_results_pane()

    def _build_operations_pane(self) -> None:
        assert self.paned is not None
        outer = ttk.LabelFrame(self.paned, text="Object types", padding=8)
        self.paned.add(outer, weight=0)

        ttk.Button(
            outer,
            text="Clear results",
            command=self.clear_results,
            width=22,
        ).pack(fill=tk.X, pady=(0, 8))

        hint = ttk.Label(
            outer,
            text="Choose an object type to browse.",
            foreground=ColorTheme.TEXT_SECONDARY,
            font=(self._font_ui[0], max(self._font_ui[1] - 1, 8)),
            wraplength=240,
            justify=tk.LEFT,
        )
        hint.pack(anchor=tk.W, pady=(0, 6))

        self.btn_frame = make_scrollable(outer, bg=ColorTheme.BG_MAIN)

    def _build_results_pane(self) -> None:
        assert self.paned is not None
        container = ttk.LabelFrame(self.paned, text="Results", padding=8)
        self.paned.add(container, weight=1)

        toolbar = ttk.Frame(container)
        toolbar.pack(fill=tk.X, pady=(0, 6))

        self._results_title_label = ttk.Label(
            toolbar, text="No objects loaded", font=self._title_font
        )
        self._results_title_label.pack(side=tk.LEFT)

        self._results_count_label = ttk.Label(
            toolbar, text="", foreground=ColorTheme.TEXT_SECONDARY
        )
        self._results_count_label.pack(side=tk.RIGHT, padx=(8, 0))

        filter_row = ttk.Frame(container)
        filter_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(filter_row, text="Filter:").pack(side=tk.LEFT, padx=(0, 4))
        filter_entry = ttk.Entry(filter_row, textvariable=self._filter_var, width=32)
        filter_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._filter_var.trace_add("write", lambda *_: self._apply_filter())
        ttk.Button(filter_row, text="Clear", command=self._clear_filter, width=8).pack(
            side=tk.LEFT, padx=(6, 0)
        )

        # Table cards (collapsible sections)
        self._results_canvas_container = ttk.Frame(container)
        self._results_content_frame = make_scrollable(
            self._results_canvas_container, bg=ColorTheme.BG_SECONDARY
        )
        self._results_canvas = self._results_content_frame.scroll_canvas

        # Simple list (Treeview) for non-table object types
        self._results_list_container = ttk.Frame(container)
        columns = ("name",)
        self._objects_tree = ttk.Treeview(
            self._results_list_container,
            columns=columns,
            show="headings",
            selectmode="browse",
            height=16,
        )
        self._objects_tree.heading("name", text="Object name")
        self._objects_tree.column("name", width=480, anchor=tk.W)
        tree_scroll = ttk.Scrollbar(
            self._results_list_container, orient=tk.VERTICAL, command=self._objects_tree.yview
        )
        self._objects_tree.configure(yscrollcommand=tree_scroll.set)
        self._objects_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Fallback text area
        from tkinter import scrolledtext

        self._results_text_container = ttk.Frame(container)
        self._results_text = scrolledtext.ScrolledText(
            self._results_text_container,
            wrap=tk.WORD,
            font=self._font_mono,
            bg=ColorTheme.BG_SECONDARY,
            fg=ColorTheme.TEXT_PRIMARY,
            relief=tk.FLAT,
            borderwidth=0,
        )
        self._results_text.pack(fill=tk.BOTH, expand=True)

        placeholder = ttk.Label(
            container,
            text="Choose an object type on the left to list database objects.",
            foreground=ColorTheme.TEXT_SECONDARY,
            font=self._font_ui,
        )
        placeholder.pack(expand=True)
        self._placeholder = placeholder

    def _update_info_bar(self) -> None:
        if not self._info_label:
            return
        conn = self._get_current_connection()
        db_type = self._get_db_type()
        if not conn:
            self._info_label.config(
                text="No active connection — connect from the Connections tab."
            )
            return
        db_manager = self._get_connections().get(conn)
        version = ""
        if db_manager:
            try:
                version = db_manager.get_version() or ""
            except Exception:
                pass
        ops = len(self.operation_buttons)
        ver = f" v{version}" if version else ""
        self._info_label.config(
            text=f"{db_type}{ver}  ·  {ops} browse operations  ·  connection: {conn}"
        )

    def recreate_operation_buttons(self) -> None:
        if not hasattr(self, "btn_frame"):
            return
        for btn in self.operation_buttons:
            btn.destroy()
        self.operation_buttons.clear()

        db_type = self._get_db_type()
        operations = self._get_operations(db_type)

        for text, func_name in operations:
            btn = ttk.Button(
                self.btn_frame,
                text=text,
                command=lambda t=text, f=func_name: self.execute_operation(t, f),
                state=tk.DISABLED,
                width=22,
            )
            btn.pack(padx=4, pady=3, fill=tk.X)
            self.operation_buttons.append(btn)

        self._update_info_bar()

    def sync_for_connection(self, conn_name: str, *, status_prefix: str = "Using") -> None:
        connections = self._get_connections()
        if conn_name not in connections:
            return
        db_manager = connections[conn_name]
        self._set_current_connection(conn_name)
        self._set_db_type(db_manager.db_type)
        if self.connection_combo:
            self.connection_combo.set(conn_name)
        self.recreate_operation_buttons()
        for btn in self.operation_buttons:
            btn.config(state=tk.NORMAL)
        self.update_status(
            f"{status_prefix} {conn_name} in Database Objects ({db_manager.db_type})",
            "info",
        )

    def refresh_connections(self) -> None:
        if not self.connection_combo:
            return
        names = list(self._get_connections().keys())
        self.connection_combo["values"] = names
        selected = self.connection_combo.get().strip()
        if selected and selected in names:
            self.sync_for_connection(selected, status_prefix="Using")
        else:
            self.connection_combo.set("")
            self._set_current_connection(None)
            self._set_db_type(None)
            for btn in self.operation_buttons:
                btn.config(state=tk.DISABLED)
            self._update_info_bar()

    def _on_connection_changed(self, _event=None) -> None:
        if not self.connection_combo:
            return
        selected = self.connection_combo.get().strip()
        if selected and selected in self._get_connections():
            self.sync_for_connection(selected)
        else:
            self._set_current_connection(None)
            self._set_db_type(None)
            for btn in self.operation_buttons:
                btn.config(state=tk.DISABLED)
            self._update_info_bar()

    def clear_results(self) -> None:
        self._current_result_title = ""
        self._current_result_items = []
        self._table_section_widgets.clear()
        self._filter_var.set("")

        for frame in (
            self._results_canvas_container,
            self._results_list_container,
            self._results_text_container,
        ):
            if frame:
                frame.pack_forget()

        if self._results_content_frame:
            for w in self._results_content_frame.winfo_children():
                w.destroy()
        if self._objects_tree:
            for item in self._objects_tree.get_children():
                self._objects_tree.delete(item)
        if self._results_text:
            self._results_text.delete("1.0", tk.END)

        if self._results_title_label:
            self._results_title_label.config(text="No objects loaded")
        if self._results_count_label:
            self._results_count_label.config(text="")
        self._placeholder.pack(expand=True)

    def _clear_filter(self) -> None:
        self._filter_var.set("")

    def _apply_filter(self) -> None:
        needle = self._filter_var.get().strip().lower()
        if self._is_tabular_list(self._current_result_title) and self._table_section_widgets:
            for name, shell in self._table_section_widgets.items():
                if not needle or needle in name.lower():
                    shell.pack(fill=tk.X, pady=3, padx=4)
                else:
                    shell.pack_forget()
            return
        if self._objects_tree and self._current_result_items:
            for item in self._objects_tree.get_children():
                self._objects_tree.delete(item)
            for idx, item in enumerate(self._current_result_items, 1):
                label = str(item)
                if needle and needle not in label.lower():
                    continue
                self._objects_tree.insert("", tk.END, iid=str(idx), values=(label,))

    def display_results(self, title: str, items: list) -> None:
        self.clear_results()
        self._placeholder.pack_forget()
        self._current_result_title = title
        self._current_result_items = list(items or [])

        db_type = self._get_db_type() or ""
        if self._results_title_label:
            self._results_title_label.config(text=f"{title}  ({db_type})")
        if self._results_count_label:
            self._results_count_label.config(text=f"{len(items)} object(s)")

        if self._is_tabular_list(title) and items:
            self._results_canvas_container.pack(fill=tk.BOTH, expand=True)
            self._render_table_cards(title, items)
        elif items:
            self._results_list_container.pack(fill=tk.BOTH, expand=True)
            self._apply_filter()
        else:
            self._results_text_container.pack(fill=tk.BOTH, expand=True)
            self._results_text.insert(
                tk.END, f"No {title.lower()} found for this connection.\n"
            )

        self.update_status(f"Found {len(items)} {title.lower()}", "success")

    def _render_table_cards(self, title: str, tables: list) -> None:
        assert self._results_content_frame is not None
        db_manager = self._get_connections().get(self._get_current_connection() or "")
        supports_schema = bool(
            db_manager
            and DatabaseRegistry.supports_operation(db_manager.db_type, "getTableSchema")
        )
        caps = db_manager.capabilities if db_manager else None
        supports_export = bool(
            db_manager
            and caps
            and (caps.supports_sql_editor or caps.supports_document_query)
        )

        object_label = "collection" if title.lower() == "collections" else "table"
        if not supports_schema:
            note = ttk.Label(
                self._results_content_frame,
                text=(
                    f"Column details are not available for "
                    f"{db_manager.db_type if db_manager else 'this engine'}."
                ),
                foreground=ColorTheme.WARNING,
            )
            note.pack(anchor=tk.W, padx=8, pady=(0, 6))
        elif object_label == "collection":
            hint = ttk.Label(
                self._results_content_frame,
                text="▶ expands schema; Load Sample Data shows one document; Export Data saves rows.",
                foreground=ColorTheme.TEXT_SECONDARY,
            )
            hint.pack(anchor=tk.W, padx=8, pady=(0, 6))

        for table_name in tables:
            self._create_table_section(
                str(table_name),
                supports_schema=supports_schema,
                supports_export=supports_export,
            )
        self._apply_filter()

    def _create_table_section(
        self,
        table_name: str,
        *,
        supports_schema: bool,
        supports_export: bool,
    ) -> None:
        assert self._results_content_frame is not None
        conn_name = self._get_current_connection()
        connections = self._get_connections()
        if not conn_name or conn_name not in connections:
            return

        shell = tk.Frame(
            self._results_content_frame,
            bg=ColorTheme.BG_MAIN,
            highlightbackground=ColorTheme.BORDER,
            highlightthickness=1,
        )
        self._table_section_widgets[table_name] = shell

        header = tk.Frame(shell, bg=ColorTheme.BG_SECONDARY)
        header.pack(fill=tk.X, padx=1, pady=1)

        state = {"expanded": False, "schema_loaded": False, "sample_loaded": False}

        toggle_btn = ttk.Button(header, text="▶", width=3)
        toggle_btn.pack(side=tk.LEFT, padx=(4, 0))

        display_name = table_name if len(table_name) <= 55 else table_name[:52] + "..."
        name_lbl = tk.Label(
            header,
            text=display_name,
            font=self._title_font,
            bg=ColorTheme.BG_SECONDARY,
            fg=ColorTheme.TEXT_PRIMARY,
            anchor=tk.W,
        )
        name_lbl.pack(side=tk.LEFT, padx=(4, 8))

        status_lbl = tk.Label(
            header,
            text="click ▶ for schema",
            font=(self._font_ui[0], max(self._font_ui[1] - 1, 8)),
            fg=ColorTheme.TEXT_SECONDARY,
            bg=ColorTheme.BG_SECONDARY,
        )
        status_lbl.pack(side=tk.LEFT, padx=(0, 8))

        btn_row = ttk.Frame(header)
        btn_row.pack(side=tk.RIGHT, padx=6, pady=4)

        content = ttk.Frame(shell)

        def _show_content():
            content.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 8))
            toggle_btn.config(text="▼")
            state["expanded"] = True

        def _hide_content():
            content.pack_forget()
            toggle_btn.config(text="▶")
            state["expanded"] = False

        def _load_schema(*, from_toggle: bool = False):
            if not supports_schema:
                if from_toggle:
                    status_lbl.config(text="schema n/a", fg=ColorTheme.WARNING)
                    for w in content.winfo_children():
                        w.destroy()
                    ttk.Label(
                        content,
                        text="Schema is not available for this engine.",
                        foreground=ColorTheme.WARNING,
                    ).pack(anchor=tk.W, pady=4)
                    _show_content()
                return
            if state["schema_loaded"] and from_toggle:
                _show_content()
                return

            status_lbl.config(text="loading schema…", fg=ColorTheme.PRIMARY)
            db_manager = connections[conn_name]

            def fetch():
                try:
                    with self.db_query_lock:
                        schema = DatabaseRegistry.execute_operation(
                            db_manager.db_type,
                            "getTableSchema",
                            db_manager.conn,
                            table_name,
                        )

                    def update_ui():
                        for w in content.winfo_children():
                            w.destroy()
                        state["sample_loaded"] = False
                        if schema:
                            self._populate_table_schema(content, schema)
                            status_lbl.config(
                                text=f"{len(schema)} column(s)", fg=ColorTheme.SUCCESS
                            )
                            state["schema_loaded"] = True
                            _show_content()
                        else:
                            status_lbl.config(text="no columns", fg=ColorTheme.WARNING)
                            ttk.Label(
                                content,
                                text="Could not load schema (permissions or empty object).",
                                foreground=ColorTheme.WARNING,
                            ).pack(anchor=tk.W, pady=4)
                            _show_content()

                    self.root.after(0, update_ui)
                except Exception as exc:
                    msg = str(exc)
                    def show_err():
                        status_lbl.config(text="schema error", fg=ColorTheme.ERROR)
                        for w in content.winfo_children():
                            w.destroy()
                        ttk.Label(
                            content, text=msg, foreground=ColorTheme.ERROR, wraplength=520
                        ).pack(anchor=tk.W, pady=4)
                        _show_content()

                    self.root.after(0, show_err)

            threading.Thread(target=fetch, daemon=True).start()

        def _load_sample_data():
            if not supports_export:
                messagebox.showinfo(
                    "Sample data",
                    "Sample data is not supported for this connection.",
                )
                return
            status_lbl.config(text="loading sample…", fg=ColorTheme.PRIMARY)
            db_manager = connections[conn_name]

            def fetch():
                try:
                    result = self._fetch_table_rows(db_manager, table_name, limit=1)
                    rows = result.get("rows") or []
                    columns = result.get("columns") or []

                    def update_ui():
                        for w in content.winfo_children():
                            w.destroy()
                        state["schema_loaded"] = False
                        if rows:
                            self._populate_sample_data(content, columns, rows)
                            status_lbl.config(text="1 sample row", fg=ColorTheme.SUCCESS)
                            state["sample_loaded"] = True
                        else:
                            status_lbl.config(text="no rows", fg=ColorTheme.WARNING)
                            ttk.Label(
                                content,
                                text="Table is empty or no readable rows.",
                                foreground=ColorTheme.WARNING,
                            ).pack(anchor=tk.W, pady=4)
                        _show_content()

                    self.root.after(0, update_ui)
                except Exception as exc:
                    msg = str(exc)
                    def show_err():
                        status_lbl.config(text="sample error", fg=ColorTheme.ERROR)
                        messagebox.showerror(
                            "Load Sample Data",
                            f"Could not load sample row from {table_name}:\n{msg}",
                        )

                    self.root.after(0, show_err)

            threading.Thread(target=fetch, daemon=True).start()

        def _toggle():
            if state["expanded"]:
                _hide_content()
            else:
                _load_schema(from_toggle=True)

        if supports_export:
            ttk.Button(
                btn_row,
                text="Load Sample Data",
                command=_load_sample_data,
                width=14,
            ).pack(side=tk.LEFT, padx=2)
            ttk.Button(
                btn_row,
                text="Export Data",
                style="Success.TButton",
                command=lambda: self.export_table_data(table_name),
                width=12,
            ).pack(side=tk.LEFT, padx=2)

        toggle_btn.config(command=_toggle)

    @staticmethod
    def _normalize_sample_rows(columns: list, rows: list) -> tuple[list[str], list[list[str]]]:
        if not columns and rows and isinstance(rows[0], dict):
            columns = list(rows[0].keys())
            display_rows = [[row.get(c) for c in columns] for row in rows]
        else:
            display_rows = list(rows)
        col_names = [str(c) for c in columns]
        normalized: list[list[str]] = []
        for row in display_rows:
            values = [cell_to_str(v) for v in row]
            if len(values) < len(col_names):
                values.extend([""] * (len(col_names) - len(values)))
            normalized.append(values[: len(col_names)])
        return col_names, normalized

    def _sample_as_tsv(self, columns: list[str], rows: list[list[str]]) -> str:
        lines = ["\t".join(columns)]
        lines.extend("\t".join(row) for row in rows)
        return "\n".join(lines)

    def _sample_as_csv(self, columns: list[str], rows: list[list[str]]) -> str:
        buf = StringIO()
        writer = csv.writer(buf)
        writer.writerow(columns)
        writer.writerows(rows)
        return buf.getvalue().rstrip("\n")

    def _copy_sample_data(self, columns: list[str], rows: list[list[str]], *, fmt: str = "tsv") -> None:
        if not columns or not rows:
            return
        text = self._sample_as_csv(columns, rows) if fmt == "csv" else self._sample_as_tsv(columns, rows)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self.update_status("Sample data copied to clipboard", "success")

    def _bind_sample_copy(
        self,
        tree: ttk.Treeview,
        columns: list[str],
        rows: list[list[str]],
        *,
        scroll_host: tk.Widget | None = None,
    ) -> None:
        def _copy(_event=None):
            self._copy_sample_data(columns, rows)
            return "break"

        for widget in (tree, scroll_host):
            if widget is None:
                continue
            widget.bind("<Control-c>", _copy)
            widget.bind("<Control-C>", _copy)
            widget.bind("<Command-c>", _copy)
            widget.bind("<Command-C>", _copy)

        def _popup_menu(event):
            try:
                tree.focus_set()
            except tk.TclError:
                pass
            try:
                rel_y = event.y
                if event.widget is not tree:
                    rel_y = event.y_root - tree.winfo_rooty()
                row_id = tree.identify_row(rel_y)
                if row_id:
                    tree.selection_set(row_id)
                    tree.focus(row_id)
            except tk.TclError:
                pass

            menu = tk.Menu(self.root, tearoff=0)
            menu.add_command(
                label="Copy (tab-separated)",
                command=lambda: self._copy_sample_data(columns, rows, fmt="tsv"),
            )
            menu.add_command(
                label="Copy (CSV)",
                command=lambda: self._copy_sample_data(columns, rows, fmt="csv"),
            )
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()

        for widget in (tree, scroll_host):
            if widget is None:
                continue
            for seq in ("<Button-2>", "<Button-3>", "<Control-Button-1>"):
                widget.bind(seq, _popup_menu, add="+")

    def _populate_sample_data(
        self, parent: tk.Widget, columns: list, rows: list
    ) -> None:
        header_row = ttk.Frame(parent)
        header_row.pack(fill=tk.X, pady=(2, 4))
        ttk.Label(
            header_row,
            text="Sample row (1 record)",
            font=(self._font_ui[0], self._font_ui[1], "bold"),
        ).pack(side=tk.LEFT)
        ttk.Label(
            header_row,
            text="Cmd+C / Ctrl+C or right-click to copy",
            foreground=ColorTheme.TEXT_SECONDARY,
            font=(self._font_ui[0], max(self._font_ui[1] - 1, 8)),
        ).pack(side=tk.LEFT, padx=(10, 0))

        col_names, display_rows = self._normalize_sample_rows(columns, rows)
        if not col_names:
            ttk.Label(parent, text="No columns in sample.", foreground=ColorTheme.TEXT_SECONDARY).pack(
                anchor=tk.W
            )
            return

        scroll_host = ttk.Frame(parent)
        scroll_host.pack(fill=tk.BOTH, expand=True, pady=(0, 4))

        tree = ttk.Treeview(
            scroll_host,
            columns=[f"c{i}" for i in range(len(col_names))],
            show="headings",
            height=1,
            selectmode="browse",
        )
        hsb = ttk.Scrollbar(scroll_host, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(xscrollcommand=hsb.set)

        sample_values = display_rows[0] if display_rows else []
        for i, col in enumerate(col_names):
            cid = f"c{i}"
            tree.heading(cid, text=col)
            cell_preview = sample_values[i] if i < len(sample_values) else ""
            col_width = min(
                320,
                max(96, len(col) * 8, len(cell_preview) * 7),
            )
            tree.column(cid, width=col_width, minwidth=72, stretch=False, anchor=tk.W)

        for row in display_rows:
            tree.insert("", tk.END, values=row)

        if tree.get_children():
            first = tree.get_children()[0]
            tree.selection_set(first)
            tree.focus(first)

        self._bind_sample_copy(tree, col_names, display_rows, scroll_host=scroll_host)

        tree.grid(row=0, column=0, sticky="ew")
        hsb.grid(row=1, column=0, sticky="ew")
        scroll_host.columnconfigure(0, weight=1)

    def _fetch_table_rows(self, db_manager, table_name: str, *, limit: int | None) -> dict:
        db_type = db_manager.db_type
        if db_type in ("MongoDB", "DocumentDB"):
            payload: dict = {
                "collection": table_name,
                "operation": "find",
                "filter": {},
            }
            if limit is not None:
                payload["limit"] = limit
            query = json.dumps(payload)
            with self.db_query_lock:
                result, error = db_manager.execute_document_query(query)
            if error:
                raise RuntimeError(error)
            return result or {"columns": [], "rows": []}

        quoted = self._quote_table(db_type, table_name)
        sql = f"SELECT * FROM {quoted}"
        if limit is not None and limit > 0 and db_type in (
            "Oracle",
            "MySQL",
            "MariaDB",
            "PostgreSQL",
            "SQLServer",
            "SQLite",
        ):
            sql = self._apply_row_limit(db_type, sql, limit)
        with self.db_query_lock:
            result, error = db_manager.execute_query(sql)
        if error:
            raise RuntimeError(error)
        if not result:
            return {"columns": [], "rows": []}
        return result

    def _populate_table_schema(self, parent: tk.Widget, columns: list[dict]) -> None:
        if not columns:
            ttk.Label(parent, text="No columns found.", foreground=ColorTheme.TEXT_SECONDARY).pack(
                anchor=tk.W
            )
            return

        tree = ttk.Treeview(
            parent,
            columns=("name", "type", "nullable", "default"),
            show="headings",
            height=min(max(len(columns), 3), 12),
        )
        tree.heading("name", text="Column")
        tree.heading("type", text="Data type")
        tree.heading("nullable", text="Nullable")
        tree.heading("default", text="Default")
        tree.column("name", width=220, anchor=tk.W)
        tree.column("type", width=180, anchor=tk.W)
        tree.column("nullable", width=90, anchor=tk.W)
        tree.column("default", width=160, anchor=tk.W)

        for col in columns:
            nullable = "NULL" if col.get("nullable") else "NOT NULL"
            default = str(col.get("default") or "—")
            tree.insert(
                "",
                tk.END,
                values=(
                    str(col.get("name", "")),
                    str(col.get("type", "")),
                    nullable,
                    default,
                ),
            )
        tree.pack(fill=tk.X, pady=(4, 0))

    def export_table_data(self, table_name: str) -> None:
        conn_name = self._get_current_connection()
        connections = self._get_connections()
        if not conn_name or conn_name not in connections:
            messagebox.showwarning("Export", "No active connection selected.")
            return
        db_manager = connections[conn_name]
        if not db_manager.conn:
            messagebox.showerror("Export", "Database connection is not active.")
            return

        max_rows = properties.get_int("ui.limits", "table_export_max_rows", default=0)
        export_limit = max_rows if max_rows > 0 else None
        db_type = db_manager.db_type

        if db_type in ("MongoDB", "DocumentDB"):
            default_ext = ".json"
            filetypes = [("JSON files", "*.json"), ("CSV files", "*.csv"), ("All files", "*.*")]
        else:
            default_ext = ".csv"
            filetypes = [("CSV files", "*.csv"), ("All files", "*.*")]

        safe_name = re.sub(r"[^\w.\-]+", "_", table_name)[:80]
        filename = filedialog.asksaveasfilename(
            title=f"Export data — {table_name}",
            initialfile=f"{safe_name}{default_ext}",
            defaultextension=default_ext,
            filetypes=filetypes,
        )
        if not filename:
            return

        self.update_status(f"Exporting {table_name}…", "info")

        def worker():
            try:
                result = self._fetch_table_rows(
                    db_manager, table_name, limit=export_limit
                )
                rows = result.get("rows") or []
                cols = result.get("columns") or []
                if not rows and not cols:
                    raise RuntimeError("Query returned no data.")

                if db_type in ("MongoDB", "DocumentDB"):
                    if filename.lower().endswith(".json"):
                        export_rows_to_json(filename, rows, columns=cols)
                    else:
                        export_result_to_csv(filename, {"columns": cols, "rows": rows})
                else:
                    export_result_to_csv(filename, result)
                rowcount = len(rows)

                def done():
                    self.update_status(
                        f"Exported {rowcount} row(s) from {table_name}", "success"
                    )
                    messagebox.showinfo(
                        "Export complete",
                        f"Exported {rowcount} row(s) to:\n{filename}",
                    )

                self.root.after(0, done)
            except Exception as exc:
                msg = str(exc)
                self.root.after(
                    0,
                    lambda: messagebox.showerror("Export failed", msg),
                )
                self.root.after(0, lambda: self.update_status("Export failed", "error"))

        threading.Thread(target=worker, daemon=True).start()

    @staticmethod
    def _quote_table(db_type: str, table_name: str) -> str:
        if db_type == "SQLite":
            return f'"{table_name.replace(chr(34), chr(34)*2)}"'
        if db_type == "SQLServer":
            parts = table_name.split(".")
            return ".".join(f"[{p}]" for p in parts)
        if db_type in ("MySQL", "MariaDB"):
            parts = table_name.split(".")
            return ".".join(f"`{p}`" for p in parts)
        parts = table_name.split(".")
        return ".".join(f'"{p}"' for p in parts)

    @staticmethod
    def _apply_row_limit(db_type: str, sql: str, limit: int) -> str:
        if db_type in ("MySQL", "MariaDB", "PostgreSQL", "SQLite"):
            return f"{sql} LIMIT {int(limit)}"
        if db_type == "SQLServer":
            return sql.replace("SELECT *", f"SELECT TOP {int(limit)} *", 1)
        if db_type == "Oracle":
            return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {int(limit)}"
        return sql

    def execute_operation(self, title: str, func_name: str) -> None:
        conn_name = self._get_current_connection()
        if not conn_name or conn_name not in self._get_connections():
            messagebox.showwarning("Warning", "Please select an active connection first.")
            return
        self._active_operation = title
        self.update_status(f"Fetching {title.lower()}…", "info")
        threading.Thread(
            target=self._fetch_and_display, args=(title, func_name), daemon=True
        ).start()

    def _fetch_and_display(self, title: str, func_name: str) -> None:
        try:
            conn_name = self._get_current_connection()
            connections = self._get_connections()
            if not conn_name or conn_name not in connections:
                self.root.after(
                    0, messagebox.showerror, "Error", "No active connection selected"
                )
                return
            db_manager = connections[conn_name]
            console_print(
                f"Fetching {title} using {func_name} for {conn_name} ({db_manager.db_type})"
            )
            with self.db_query_lock:
                items = db_manager.execute_operation(func_name)
            console_print(f"Got {len(items) if items else 0} items")
            self.root.after(0, self.display_results, title, items or [])
        except Exception as exc:
            import traceback

            details = f"Failed to fetch {title.lower()}:\n{exc}\n\n{traceback.format_exc()}"
            print(details, file=sys.stderr)
            self.root.after(0, messagebox.showerror, "Error", details)
            self.root.after(
                0, self.update_status, f"Error fetching {title.lower()}", "error"
            )

    def runtime_snapshot(self) -> dict:
        conn = self._get_current_connection() or ""
        db_type = self._get_db_type() or ""
        ops = len(self.operation_buttons)
        if not conn:
            return {
                "initialized": True,
                "connection": "",
                "db_type": "",
                "operations_available": ops,
                "overview": "No active connection — connect from Connections tab",
            }
        overview = f"Browsing {conn} ({db_type})"
        if self._active_operation and self._current_result_items:
            overview = f"{self._active_operation}: {len(self._current_result_items)} item(s) on {conn}"
        return {
            "initialized": True,
            "connection": conn,
            "db_type": db_type,
            "operations_available": ops,
            "overview": overview,
        }
