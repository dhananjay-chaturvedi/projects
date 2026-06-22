"""Single SQL / document query editor pane (one notebook tab)."""
from __future__ import annotations

import csv
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Callable, Optional

from common.autocommit import default_autocommit, get_autocommit, set_autocommit
from common.config_loader import config, console_print, get_window_size, properties
from common.database_registry import DatabaseRegistry
from common.ui.tk import (
    ColorTheme,
    create_horizontal_scrollable,
    default_ui_font,
    default_ui_mono,
    make_collapsible_section,
)
from common.ui.tk.sql_editor_assist import (
    SqlCompleter,
    SqlFormatter,
    SqlHighlighter,
    editor_settings,
)
from common.ui.shared import specs as shared_specs


class _LineNumberCanvas(tk.Canvas):
    """A gutter that paints line numbers aligned with a Text widget."""

    def __init__(self, master, text_widget, font, **kwargs):
        super().__init__(master, width=46, highlightthickness=0, bd=0, **kwargs)
        self._text = text_widget
        self._font = font

    def redraw(self, *_args):
        """Repaint the visible line numbers."""
        self.delete("all")
        try:
            index = self._text.index("@0,0")
        except tk.TclError:
            return
        while True:
            dline = self._text.dlineinfo(index)
            if dline is None:
                break
            y = dline[1]
            line_no = index.split(".")[0]
            self.create_text(
                40, y, anchor="ne", text=line_no, font=self._font, fill="#7A7A7A"
            )
            index = self._text.index(f"{index}+1line")


class SQLEditorPane:
    """SQL Editor workspace"""

    @staticmethod
    def _ui_limit(key: str, default: int, *, minimum: int = 0) -> int:
        value = properties.get_int("ui.limits", key, default=default)
        return max(minimum, value)

    @staticmethod
    def _preview(text: str, limit: int) -> str:
        if limit <= 0 or len(text) <= limit:
            return text
        return text[:limit] + "..."

    def __init__(
        self,
        parent,
        get_connections_callback,
        status_callback,
        font_ui=None,
        font_mono=None,
        on_meta_changed: Optional[Callable[[], None]] = None,
    ):
        self.parent = parent
        self.get_connections_callback = (
            get_connections_callback  # Function to get active connections
        )
        self.status_callback = status_callback
        self.on_meta_changed = on_meta_changed
        self.query_history = []
        self.selected_connection_name = None
        # Each tab owns exactly one physical DB session at a time, cloned from
        # the selected connection's saved profile and opened lazily on first
        # use. This isolates each tab's transaction from other tabs and from the
        # Objects browser (which keeps using the shared "primary" manager).
        # Switching the tab to another connection closes the previous session;
        # closing the tab disposes it.
        self._own_session = None
        self._own_session_name = None
        self._session_lock = threading.Lock()
        self._font_ui = font_ui or default_ui_font()
        self._font_mono = font_mono or default_ui_mono()
        self._title_font = (self._font_ui[0], self._font_ui[1] + 2, "bold")
        self.autocommit_var: tk.BooleanVar  # Set in create_editor_ui()

        # Query execution state tracking
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        self.create_editor_ui()

    def create_editor_ui(self):
        # Main container with vertical split: drag sash between editor and results
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True)

        paned = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)
        self._editor_results_paned = paned

        # SQL Editor Frame
        editor_frame = ttk.LabelFrame(paned, text="SQL Query Editor", padding="8")
        paned.add(editor_frame, weight=2)

        tool_host = make_collapsible_section(
            editor_frame,
            "Connection & actions (collapse to enlarge editor)",
            self._title_font,
            expanded=True,
        )

        # Create scrollable wrapper for toolbars using optimized helper
        toolbar_container = create_horizontal_scrollable(tool_host)

        # Connection Selector Toolbar
        connection_toolbar = ttk.Frame(toolbar_container)
        connection_toolbar.pack(fill=tk.X, pady=(0, 5))

        ttk.Label(connection_toolbar, text="Active connection:").pack(
            side=tk.LEFT, padx=5
        )
        self.connection_combo = ttk.Combobox(
            connection_toolbar, width=40, state="readonly"
        )
        self.connection_combo.pack(side=tk.LEFT, padx=5)
        self.connection_combo.bind("<<ComboboxSelected>>", self.on_connection_changed)

        ttk.Button(
            connection_toolbar,
            text="Refresh connections",
            command=self.refresh_connections,
        ).pack(side=tk.LEFT, padx=5)

        # Autocommit toggle
        self.autocommit_var = tk.BooleanVar(
            value=config.get_bool(
                "database.connection", "default_autocommit", default=False
            )
        )  # take from config file Default: OFF if not set
        autocommit_cb = ttk.Checkbutton(
            connection_toolbar,
            text="Auto-commit",
            variable=self.autocommit_var,
            command=self.toggle_autocommit,
        )
        autocommit_cb.pack(side=tk.LEFT, padx=10)
        self._autocommit_cb = autocommit_cb

        # Toolbar
        toolbar = ttk.Frame(toolbar_container)
        toolbar.pack(fill=tk.X, pady=(0, 5))

        # Button container frame - use grid to maintain fixed positions
        self.execute_buttons_container = ttk.Frame(toolbar)
        self.execute_buttons_container.pack(side=tk.LEFT, padx=0)

        self.execute_cursor_btn = ttk.Button(
            self.execute_buttons_container,
            text="Execute at cursor (F5)",
            command=self.execute_at_cursor,
        )
        self.execute_cursor_btn.grid(row=0, column=0, padx=2)
        self.execute_selected_btn = ttk.Button(
            self.execute_buttons_container,
            text="Execute selected",
            command=self.execute_selected,
        )
        self.execute_selected_btn.grid(row=0, column=1, padx=2)
        self.execute_all_btn = ttk.Button(
            self.execute_buttons_container, text="Execute all", command=self.execute_all
        )
        self.execute_all_btn.grid(row=0, column=2, padx=2)

        self.stop_query_btn = ttk.Button(
            self.execute_buttons_container, text="Stop Query", command=self.stop_query
        )
        self.stop_query_btn.grid(row=0, column=0, columnspan=3, padx=2, sticky="ew")
        self.stop_query_btn.grid_remove()  # Initially hidden (keeps position reserved)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        ttk.Button(toolbar, text="Clear editor", command=self.clear_editor).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(toolbar, text="Load query", command=self.load_query).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(toolbar, text="Save query", command=self.save_query).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(toolbar, text="Format SQL", command=self.format_sql).pack(
            side=tk.LEFT, padx=2
        )
        self._autocomplete_btn = ttk.Button(
            toolbar, text="Autocomplete: On", command=self.toggle_autocomplete
        )
        self._autocomplete_btn.pack(side=tk.LEFT, padx=2)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        self._commit_btn = ttk.Button(toolbar, text="Commit", command=self.commit_transaction)
        self._commit_btn.pack(side=tk.LEFT, padx=2)
        self._rollback_btn = ttk.Button(
            toolbar, text="Rollback", command=self.rollback_transaction
        )
        self._rollback_btn.pack(side=tk.LEFT, padx=2)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        ttk.Button(toolbar, text="Query history", command=self.show_history).pack(
            side=tk.LEFT, padx=2
        )

        # SQL Text Editor with line numbers, scrollbars and clipboard support.
        # (height is a hint; pane expansion fills available space)
        self._editor_frame = editor_frame
        editor_host = ttk.Frame(editor_frame)
        editor_host.pack(fill=tk.BOTH, expand=True)
        editor_host.rowconfigure(0, weight=1)
        editor_host.columnconfigure(1, weight=1)

        self.sql_text = tk.Text(
            editor_host,
            wrap=tk.NONE,
            height=8,
            font=self._font_mono,
            undo=True,
            autoseparators=True,
            maxundo=-1,
        )
        self._editor_linenumbers = _LineNumberCanvas(
            editor_host, self.sql_text, self._font_mono, bg=ColorTheme.BG_MAIN
        )
        editor_vbar = ttk.Scrollbar(editor_host, orient=tk.VERTICAL)
        editor_hbar = ttk.Scrollbar(editor_host, orient=tk.HORIZONTAL)

        def _on_editor_yscroll(*args):
            editor_vbar.set(*args)
            self._editor_linenumbers.redraw()

        self.sql_text.configure(
            yscrollcommand=_on_editor_yscroll, xscrollcommand=editor_hbar.set
        )
        editor_vbar.configure(command=self.sql_text.yview)
        editor_hbar.configure(command=self.sql_text.xview)

        self._editor_linenumbers.grid(row=0, column=0, sticky="ns")
        self.sql_text.grid(row=0, column=1, sticky="nsew")
        editor_vbar.grid(row=0, column=2, sticky="ns")
        editor_hbar.grid(row=1, column=1, sticky="ew")

        self._install_editor_change_events()
        self._setup_editor_clipboard()
        self._setup_editor_assist()
        self.sql_text.bind("<F5>", lambda e: self.execute_at_cursor())
        self.sql_text.bind("<Control-Return>", lambda e: self.execute_at_cursor())

        # Add some example queries as comment
        example = (
            "-- SQL Query Editor — F5 or Ctrl+Enter to execute at cursor. "
            "Use semicolons between multiple queries.\n\n"
        )
        self.sql_text.insert(1.0, example)

        # Results Frame
        results_frame = ttk.LabelFrame(paned, text="Query Results", padding="8")
        paned.add(results_frame, weight=1)

        # Results toolbar. Result-action buttons (Copy All Data / Sort / Filter /
        # Clear Filter) mirror the TUI and Web result toolbars and are single-
        # sourced from the shared spec so all three UIs stay in step. They act on
        # the currently selected result tab's tree.
        results_toolbar = ttk.Frame(results_frame)
        results_toolbar.pack(fill=tk.X, pady=(0, 5))

        self.result_info_label = ttk.Label(results_toolbar, text="Ready")
        self.result_info_label.pack(side=tk.LEFT, padx=5)

        res_labels = {
            a["id"]: a["label"]
            for a in shared_specs.sql_editor_payload()["resultActions"]
        }
        ttk.Button(
            results_toolbar, text=res_labels["copy_all"],
            command=self._toolbar_copy_all,
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            results_toolbar, text=res_labels["sort_asc"],
            command=lambda: self._toolbar_sort(True),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            results_toolbar, text=res_labels["sort_desc"],
            command=lambda: self._toolbar_sort(False),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            results_toolbar, text=res_labels["filter"],
            command=self._toolbar_filter,
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            results_toolbar, text=res_labels["clear_filter"],
            command=self._toolbar_clear_filter,
        ).pack(side=tk.LEFT, padx=2)

        ttk.Button(
            results_toolbar, text=res_labels["clear_results"], command=self.clear_results
        ).pack(side=tk.RIGHT, padx=2)
        ttk.Button(
            results_toolbar,
            text=res_labels["export"],
            style="Success.TButton",
            command=self.export_results,
        ).pack(side=tk.RIGHT, padx=2)

        # Results display with Notebook for multiple result tabs
        self.results_notebook = ttk.Notebook(results_frame)
        self.results_notebook.pack(fill=tk.BOTH, expand=True)

        self.result_tabs = []  # Store result tab references

        # Initialize connections list
        self.refresh_connections()

        def _position_sql_sash(attempt=0):
            try:
                p = self._editor_results_paned
                p.update_idletasks()
                h = p.winfo_height()
                if h <= 100 and attempt < 8:
                    self.parent.after(50, lambda: _position_sql_sash(attempt + 1))
                    return
                if h > 100:
                    p.sashpos(0, max(160, int(h * 0.5)))
            except tk.TclError:
                pass

        self.parent.after_idle(_position_sql_sash)
        self.parent.after_idle(self._editor_linenumbers.redraw)

    def _install_editor_change_events(self):
        """Emit <<EditorChanged>> on edits/scroll so line numbers stay in sync."""
        text = self.sql_text
        orig = str(text) + "_orig"
        text.tk.call("rename", str(text), orig)

        def proxy(*args):
            try:
                result = text.tk.call((orig,) + args)
            except tk.TclError:
                return ""
            cmd = args[0] if args else ""
            if cmd in ("insert", "delete", "replace") or (
                cmd in ("yview", "xview")
            ) or (len(args) >= 2 and cmd == "mark" and args[1] == "set"):
                text.event_generate("<<EditorChanged>>", when="tail")
            return result

        text.tk.createcommand(str(text), proxy)
        redraw = lambda _e=None: self._editor_linenumbers.redraw()
        text.bind("<<EditorChanged>>", redraw, add="+")
        text.bind("<Configure>", redraw, add="+")
        text.bind("<KeyRelease>", redraw, add="+")
        text.bind("<MouseWheel>", redraw, add="+")
        text.bind("<Button-4>", redraw, add="+")
        text.bind("<Button-5>", redraw, add="+")

    def _setup_editor_clipboard(self):
        """Right-click menu and keyboard shortcuts for cut/copy/paste/select-all."""
        text = self.sql_text
        menu = tk.Menu(text, tearoff=0)
        menu.add_command(label="Cut", command=lambda: self._editor_event("<<Cut>>"))
        menu.add_command(label="Copy", command=lambda: self._editor_event("<<Copy>>"))
        menu.add_command(label="Paste", command=lambda: self._editor_event("<<Paste>>"))
        menu.add_separator()
        menu.add_command(label="Select All", command=self._editor_select_all)
        self._editor_menu = menu

        def popup(event):
            text.focus_set()
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
            return "break"

        text.bind("<Button-3>", popup)  # Windows/Linux
        text.bind("<Button-2>", popup)  # macOS

        for seq in ("<Control-c>", "<Command-c>"):
            text.bind(seq, lambda e: self._editor_event("<<Copy>>"))
        for seq in ("<Control-x>", "<Command-x>"):
            text.bind(seq, lambda e: self._editor_event("<<Cut>>"))
        for seq in ("<Control-v>", "<Command-v>"):
            text.bind(seq, lambda e: self._editor_event("<<Paste>>"))
        for seq in ("<Control-a>", "<Command-a>"):
            text.bind(seq, lambda e: self._editor_select_all())

    def _setup_editor_assist(self):
        """Wire syntax highlighting, formatting shortcuts, and autocomplete."""
        settings = editor_settings()
        self._assist_settings = settings
        self._sql_mode = True

        self._highlighter = SqlHighlighter(
            self.sql_text,
            enabled=settings["syntax_highlight"],
            max_chars=settings["highlight_max_chars"],
            debounce_ms=settings["highlight_debounce_ms"],
        )
        # As-you-type popups follow the dedicated config flag (default ON) so
        # suggestions work even though macOS reserves Cmd+Space for Spotlight.
        self._completer = SqlCompleter(
            self.sql_text,
            get_db_manager=self.get_current_db_manager,
            get_connection_name=lambda: self.selected_connection_name,
            get_db_type=self._current_db_type,
            enabled=settings["autocomplete"],
            as_you_type=settings["autocomplete_as_you_type"],
            debounce_ms=settings["autocomplete_debounce_ms"],
            max_tables=settings["autocomplete_max_tables"],
        )
        self._update_autocomplete_button()

        def on_editor_changed(_event=None):
            if self._sql_mode:
                self._highlighter.schedule()

        self.sql_text.bind("<<EditorChanged>>", on_editor_changed, add="+")

        text = self.sql_text
        # Ctrl+Space works as a manual trigger on most platforms. Cmd+Space is
        # intentionally NOT bound: macOS reserves it for Spotlight.
        text.bind("<Control-space>", self._completer.trigger)
        for seq in ("<Control-Shift-F>", "<Command-Shift-F>"):
            text.bind(seq, lambda e: self.format_sql())

        text.bind("<KeyRelease>", self._completer.on_keyrelease, add="+")
        text.bind("<Escape>", self._completer.hide, add="+")
        text.bind("<Return>", self._completer_accept_or_pass, add="+")
        text.bind("<Tab>", self._completer_accept_or_pass, add="+")
        text.bind("<Up>", self._completer_nav_up, add="+")
        text.bind("<Down>", self._completer_nav_down, add="+")

    def _update_autocomplete_button(self) -> None:
        btn = getattr(self, "_autocomplete_btn", None)
        if btn is None:
            return
        enabled = getattr(self._completer, "enabled", False)
        btn.config(text="Autocomplete: On" if enabled else "Autocomplete: Off")

    def toggle_autocomplete(self) -> None:
        """Enable/disable keyword + object-name autocomplete (single toggle)."""
        completer = getattr(self, "_completer", None)
        if completer is None:
            return
        new_state = not completer.enabled
        completer.set_enabled(new_state)
        # As-you-type follows the config flag (the reliable path on macOS,
        # where Cmd+Space is taken by Spotlight); disabled outright when off.
        completer.set_as_you_type(
            new_state and self._assist_settings.get("autocomplete_as_you_type", True)
        )
        if new_state and self._sql_mode:
            # Warm the table cache in the background for the current connection.
            completer.prefetch_tables(self.selected_connection_name)
        self._assist_settings["autocomplete"] = new_state
        try:
            properties.save_value(
                "ui.sql_editor", "autocomplete", "true" if new_state else "false"
            )
        except Exception:
            pass
        self._update_autocomplete_button()
        self.status_callback(
            "Autocomplete enabled." if new_state else "Autocomplete disabled."
        )

    def _current_db_type(self) -> str | None:
        primary = self._primary_manager()
        return primary.db_type if primary else None

    def _completer_accept_or_pass(self, event=None):
        if getattr(self._completer, "_visible", False):
            return self._completer.accept(event)
        return None

    def _completer_nav_up(self, event=None):
        if getattr(self._completer, "_visible", False):
            return self._completer.popup_up(event)
        return None

    def _completer_nav_down(self, event=None):
        if getattr(self._completer, "_visible", False):
            return self._completer.popup_down(event)
        return None

    def format_sql(self):
        """Format the selected SQL or the entire editor buffer."""
        if not getattr(self, "_sql_mode", True):
            self.status_callback("Formatting is not available in document query mode.")
            return "break"
        if not SqlFormatter.available():
            self.status_callback(
                "SQL formatting requires sqlparse (pip install sqlparse)."
            )
            return "break"

        selected = self.get_selected_text()
        if selected:
            formatted = SqlFormatter.format_sql(selected)
            try:
                self.sql_text.edit_separator()
                self.sql_text.delete(tk.SEL_FIRST, tk.SEL_LAST)
                self.sql_text.insert(tk.INSERT, formatted)
                self.sql_text.edit_separator()
            except tk.TclError:
                pass
        else:
            content = self.sql_text.get("1.0", "end-1c")
            formatted = SqlFormatter.format_sql(content)
            try:
                self.sql_text.edit_separator()
                self.sql_text.delete("1.0", tk.END)
                self.sql_text.insert("1.0", formatted)
                self.sql_text.edit_separator()
            except tk.TclError:
                pass

        self._highlighter.schedule()
        self.status_callback("SQL formatted.")
        return "break"

    def _editor_event(self, virtual_event: str):
        try:
            self.sql_text.event_generate(virtual_event)
        except tk.TclError:
            pass
        return "break"

    def _editor_select_all(self):
        self.sql_text.tag_add(tk.SEL, "1.0", "end-1c")
        self.sql_text.mark_set(tk.INSERT, "1.0")
        self.sql_text.see(tk.INSERT)
        return "break"

    def refresh_connections(self):
        """Refresh the list of active connections"""
        connections = self.get_connections_callback()

        # Update combo box; keep empty selection until the user picks a connection
        connection_names = list(connections.keys())
        self.connection_combo["values"] = connection_names

        if self.selected_connection_name and self.selected_connection_name in connections:
            self.connection_combo.set(self.selected_connection_name)
        else:
            # The selected connection was removed/disconnected — drop this tab's
            # private session so we don't keep a stale socket open.
            self._release_own_session()
            self.selected_connection_name = None
            self.connection_combo.set("")

        self.status_callback(f"Found {len(connections)} active connection(s)")

    def on_connection_changed(self, event=None):
        """Handle connection selection change"""
        selected = self.connection_combo.get().strip()
        if selected:
            changed = selected != self.selected_connection_name
            self.selected_connection_name = selected
            self.status_callback(f"Using connection: {selected}")

            # Switching connections closes this tab's previous session; the new
            # one is opened lazily on first run (no blocking connect on select).
            if changed:
                self._release_own_session()
                if getattr(self, "_completer", None):
                    self._completer.prefetch_tables(selected)

            primary = self._primary_manager()
            if primary is not None:
                self._apply_query_mode(primary.db_type)

            self._sync_autocommit_from_connection()
        else:
            self.selected_connection_name = None
            self._release_own_session()

        if self.on_meta_changed:
            self.on_meta_changed()

    def _apply_query_mode(self, db_type: str) -> None:
        """Switch SQL editor vs document query UI based on engine capabilities."""
        caps = DatabaseRegistry.get_capabilities(db_type)
        if caps.query_language == "document":
            self._sql_mode = False
            if getattr(self, "_highlighter", None):
                self._highlighter.set_enabled(False)
            if getattr(self, "_completer", None):
                self._completer.set_enabled(False)
            self._update_autocomplete_button()
            self._editor_frame.config(text="Document Query Editor (JSON)")
            hint = (
                '-- Document query (JSON). Example find:\n'
                '{"collection": "users", "operation": "find", "filter": {}, "limit": 50}\n\n'
            )
            if not self._document_query_text(self.sql_text.get("1.0", "end-1c")):
                self.sql_text.delete("1.0", tk.END)
                self.sql_text.insert("1.0", hint)
            self.execute_all_btn.config(state=tk.DISABLED)
        else:
            self._sql_mode = True
            settings = getattr(self, "_assist_settings", editor_settings())
            if getattr(self, "_highlighter", None):
                self._highlighter.set_enabled(settings["syntax_highlight"])
            if getattr(self, "_completer", None):
                self._completer.set_enabled(settings["autocomplete"])
                self._completer.set_as_you_type(
                    settings["autocomplete"]
                    and settings["autocomplete_as_you_type"]
                )
                self._completer.prefetch_tables(self.selected_connection_name)
            self._update_autocomplete_button()
            self._editor_frame.config(text="SQL Query Editor")
            self.execute_all_btn.config(state=tk.NORMAL)

        if caps.supports_autocommit:
            self._autocommit_cb.config(state=tk.NORMAL)
        else:
            self._autocommit_cb.config(state=tk.DISABLED)

        if caps.supports_transactions:
            self._commit_btn.config(state=tk.NORMAL)
            self._rollback_btn.config(state=tk.NORMAL)
        else:
            self._commit_btn.config(state=tk.DISABLED)
            self._rollback_btn.config(state=tk.DISABLED)

    def _primary_manager(self):
        """The shared manager for the selected connection (no connect).

        Used for cheap metadata (db_type) and as the template for cloning this
        tab's own session. Returns ``None`` if the connection is not active.
        """
        if not self.selected_connection_name:
            return None
        connections = self.get_connections_callback()
        return connections.get(self.selected_connection_name)

    def get_current_db_manager(self):
        """Return this tab's own DB session, opening it lazily on first use.

        Each tab keeps a single private session; selecting a different
        connection closes the old one and opens a fresh session for the new one.
        Falls back to the shared manager only if a dedicated session cannot be
        opened (e.g. no saved connect params).
        """
        primary = self._primary_manager()
        if primary is None:
            self._release_own_session()
            return None

        with self._session_lock:
            if (
                self._own_session is not None
                and self._own_session_name == self.selected_connection_name
                and getattr(self._own_session, "conn", None) is not None
            ):
                return self._own_session

            # Connection changed or first use → drop the old session, clone one.
            self._release_own_session_locked()
            session = self._clone_session(primary)
            if session is None:
                return primary  # graceful fallback: share the primary session
            self._own_session = session
            self._own_session_name = self.selected_connection_name
            return session

    def _clone_session(self, primary):
        """Open a new independent session using *primary*'s connect params."""
        params = getattr(primary, "_last_connect_params", None)
        if not params:
            return None
        try:
            from common.db_manager import DatabaseManager

            session = DatabaseManager(primary.db_type)
            session.connect(**params)
            return session
        except Exception as exc:
            self.status_callback(
                f"Warning: could not open a dedicated session for this tab "
                f"({exc}); sharing the primary connection."
            )
            return None

    def _release_own_session_locked(self):
        session = self._own_session
        self._own_session = None
        self._own_session_name = None
        if session is not None:
            try:
                session.disconnect()
            except Exception:
                pass

    def _release_own_session(self):
        with self._session_lock:
            self._release_own_session_locked()

    def dispose(self):
        """Close this tab's private session (call when the tab is closed)."""
        if getattr(self, "_completer", None):
            self._completer.dispose()
        if getattr(self, "_highlighter", None):
            self._highlighter._cancel()
        self._release_own_session()

    def get_query_text(self):
        """Get the SQL query text from editor"""
        return self.sql_text.get(1.0, tk.END).strip()

    def get_selected_text(self):
        """Get selected text from editor"""
        try:
            return self.sql_text.get(tk.SEL_FIRST, tk.SEL_LAST).strip()
        except tk.TclError:
            return None

    def _iter_sql_statements(self, text: str):
        """Yield (query, start_line, end_line) for each statement; ignores -- comment lines."""
        current_query_lines: list[str] = []
        start_line: int | None = None
        end_line: int | None = None
        in_string = False
        string_char = None

        for line_num, line in enumerate(text.split("\n"), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("--"):
                continue

            if start_line is None:
                start_line = line_num

            has_semicolon = False
            line_without_semicolon = line

            i = 0
            while i < len(line):
                char = line[i]

                if char in ('"', "'") and not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char and in_string:
                    in_string = False
                    string_char = None
                elif char == ";" and not in_string:
                    has_semicolon = True
                    line_without_semicolon = line[:i].strip()
                    break

                i += 1

            if line_without_semicolon:
                current_query_lines.append(line_without_semicolon)
                end_line = line_num

            if has_semicolon:
                query = "\n".join(current_query_lines).strip()
                if query and start_line is not None:
                    yield query, start_line, end_line or start_line
                current_query_lines = []
                start_line = None
                end_line = None
                in_string = False
                string_char = None

        if current_query_lines:
            query = "\n".join(current_query_lines).strip()
            if query and start_line is not None:
                yield query, start_line, end_line or start_line

    def parse_queries(self, text):
        """Parse multiple SQL queries separated by semicolons"""
        return [query for query, _, _ in self._iter_sql_statements(text)]

    def get_query_at_cursor(self):
        """Get the SQL query at cursor position (comment-aware)."""
        cursor_line = int(self.sql_text.index(tk.INSERT).split(".")[0])
        text = self.get_query_text()
        if not text:
            return None

        statements = list(self._iter_sql_statements(text))
        if not statements:
            return None

        for query, start_line, end_line in statements:
            if start_line <= cursor_line <= end_line:
                return query

        for query, start_line, _end_line in statements:
            if start_line >= cursor_line:
                return query

        return statements[-1][0]

    def execute_at_cursor(self):
        """Execute query at cursor position"""
        sql = self.get_query_at_cursor()
        if not sql:
            messagebox.showwarning("Warning", "No query found at cursor position!")
            return

        self._execute_sql(sql, "result")

    def execute_all(self):
        """Execute all queries in the editor"""
        text = self.get_query_text()
        if not text:
            messagebox.showwarning("Warning", "No queries to execute!")
            return

        queries = self.parse_queries(text)
        if not queries:
            messagebox.showwarning("Warning", "No valid queries found!")
            return

        if messagebox.askyesno("Execute All", f"Execute {len(queries)} queries?"):
            self._execute_multiple_queries(queries)

    def _document_query_text(self, text: str) -> str:
        """Strip SQL-style comment lines from the document query editor."""
        lines = [
            line
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        return "\n".join(lines).strip()

    def execute_query(self):
        """Execute the SQL or document query"""
        db_manager = self.get_current_db_manager()
        caps = (
            DatabaseRegistry.get_capabilities(db_manager.db_type)
            if db_manager
            else None
        )
        raw_text = self.get_query_text()
        if caps and caps.query_language == "document":
            sql = self._document_query_text(raw_text)
        else:
            queries = self.parse_queries(raw_text)
            sql = self.get_query_at_cursor() or (queries[0] if queries else "")
        if not sql:
            if caps and caps.query_language == "document":
                messagebox.showwarning("Warning", "Please enter a JSON document query!")
            else:
                messagebox.showwarning("Warning", "Please enter a SQL query!")
            return

        self._execute_sql(sql)

    def execute_selected(self):
        """Execute selected SQL text"""
        sql = self.get_selected_text()
        if not sql:
            messagebox.showwarning("Warning", "Please select a SQL query to execute!")
            return

        self._execute_sql(sql, "Selected Query")

    def _execute_sql(self, sql, query_name="Query"):
        """Execute SQL in background thread"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror(
                "Error", "Not connected to any database! Please select a connection."
            )
            return

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        # Update UI to show stop button and hide execute buttons
        self._show_stop_button()

        self.status_callback("Executing query...")
        self.result_info_label.config(text="Executing query...")

        # Add to history (timestamp in the configured display timezone)
        from common.tzutil import now as _tz_now

        self.query_history.append(
            {"sql": sql, "timestamp": _tz_now().strftime("%Y-%m-%d %H:%M:%S")}
        )
        max_history = self._ui_limit("sql_history_limit", 100, minimum=0)
        if max_history and len(self.query_history) > max_history:
            del self.query_history[:-max_history]

        thread = threading.Thread(
            target=self._execute_query_thread, args=(sql, query_name)
        )
        thread.daemon = True
        self.current_execution_thread = thread
        thread.start()

    def _execute_multiple_queries(self, queries):
        """Execute multiple queries sequentially"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror(
                "Error", "Not connected to any database! Please select a connection."
            )
            return

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        # Update UI to show stop button and hide execute buttons
        self._show_stop_button()

        self.status_callback(f"Executing {len(queries)} queries...")
        self.result_info_label.config(text=f"Executing {len(queries)} queries...")

        thread = threading.Thread(target=self._execute_multiple_thread, args=(queries,))
        thread.daemon = True
        self.current_execution_thread = thread
        thread.start()

    def _execute_query_thread(self, sql, query_name="Query"):
        """Execute query in background thread"""
        try:
            # Check if cancellation was requested before starting
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            db_manager = self.get_current_db_manager()
            if not db_manager:
                self.parent.after(
                    0, self._show_error, "No active connection", query_name
                )
                return

            result, error = db_manager.execute_query(sql)

            # Check if query was cancelled during execution
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            if error:
                self.parent.after(0, self._show_error, error, query_name)
            else:
                self.parent.after(0, self._show_results, result, query_name, sql)
        except Exception as e:
            self.parent.after(0, self._show_error, str(e), query_name)
        finally:
            # Always restore UI state when query completes
            self.parent.after(0, self._restore_query_ui_state)

    def _execute_multiple_thread(self, queries):
        """Execute multiple queries in background thread"""
        try:
            # Check if cancellation was requested before starting
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            db_manager = self.get_current_db_manager()
            if not db_manager:
                self.parent.after(
                    0, messagebox.showerror, "Error", "No active connection"
                )
                return

            results = []
            for idx, sql in enumerate(queries, 1):
                # Check for cancellation before each query
                if self.cancellation_requested:
                    self.parent.after(0, self._handle_query_cancelled)
                    return

                query_name = f"Query {idx}"
                try:
                    result, error = db_manager.execute_query(sql)
                    if error:
                        results.append((query_name, None, error, sql))
                    else:
                        results.append((query_name, result, None, sql))
                except Exception as e:
                    results.append((query_name, None, str(e), sql))

            # Check for cancellation after all queries
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            self.parent.after(0, self._show_multiple_results, results)
        finally:
            # Always restore UI state when query completes
            self.parent.after(0, self._restore_query_ui_state)

    def stop_query(self):
        """Stop the currently executing query"""
        self.status_callback("Stopping query...")
        self.query_running = True
        console_print("Stop query requested by user")
        if not self.query_running:
            return

        # Set cancellation flag
        self.cancellation_requested = True

        # Try to cancel at database level
        if self.current_db_manager:
            try:
                self.current_db_manager.cancel_query()
                self.status_callback("Query cancellation requested...")
                self.result_info_label.config(text="Query cancellation requested...")
            except Exception as e:
                print(f"Error cancelling query: {e}", file=sys.stderr)
                # Even if cancellation fails, the flag is set and thread will check it

    def _show_stop_button(self):
        """Show stop button and hide execute buttons"""
        self.execute_cursor_btn.grid_remove()
        self.execute_selected_btn.grid_remove()
        self.execute_all_btn.grid_remove()
        self.stop_query_btn.grid()  # Show in same position
        if self.on_meta_changed:
            self.on_meta_changed()

    def _restore_query_ui_state(self):
        """Restore UI state after query execution completes"""
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        # Hide stop button, show execute buttons
        self.stop_query_btn.grid_remove()
        self.execute_cursor_btn.grid()
        self.execute_selected_btn.grid()
        self.execute_all_btn.grid()
        if self.on_meta_changed:
            self.on_meta_changed()

    def _handle_query_cancelled(self):
        """Handle UI updates when query is cancelled"""
        self.result_info_label.config(text="Query execution cancelled by user")
        self.status_callback("Query cancelled")
        messagebox.showinfo("Query Cancelled", "Query execution was cancelled by user")

    def _create_result_tab(self, query_name):
        """Create a new result tab"""
        tab_frame = ttk.Frame(self.results_notebook)

        # Create treeview for this tab
        tree_container = ttk.Frame(tab_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(tree_container, orient="vertical")
        hsb = ttk.Scrollbar(tree_container, orient="horizontal")

        tree = ttk.Treeview(
            tree_container, yscrollcommand=vsb.set, xscrollcommand=hsb.set
        )
        vsb.config(command=tree.yview)
        hsb.config(command=tree.xview)

        # Configure style for better column separation
        style = ttk.Style()
        style.configure("Treeview", rowheight=25, borderwidth=1, relief=tk.SOLID)
        style.configure(
            "Treeview.Heading", font=self._font_ui, relief=tk.RAISED, borderwidth=1
        )
        style.layout(
            "Treeview", [("Treeview.treearea", {"sticky": "nswe"})]
        )  # Enable borders

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)

        # Add context menu and sorting for the tree
        self._setup_tree_features(tree, tab_frame)

        self.results_notebook.add(tab_frame, text=query_name)
        self.results_notebook.select(tab_frame)

        return tree, tab_frame

    def _show_results(self, result, query_name="Query", sql=""):
        """Display query results in a new tab"""
        if "message" in result:
            # DML/DDL statement result
            self.result_info_label.config(
                text=f"{result['message']} (Time: {result['time']:.3f}s)"
            )
            self.status_callback(result["message"])
            messagebox.showinfo("Success", result["message"])
        else:
            # SELECT query result
            columns = result["columns"]
            rows = result["rows"]
            rowcount = result["rowcount"]
            exec_time = result["time"]

            # Create new tab
            tree, tab_frame = self._create_result_tab(f"{query_name} ({rowcount} rows)")

            # Store result for export
            tab_frame.result_data = result  # type: ignore[attr-defined]

            # Store original rows for filtering (decoded rows will be stored after insertion)
            tab_frame.original_rows = []  # type: ignore[attr-defined]

            # Configure data columns only. Row numbers are shown in the
            # built-in tree column ("#0") for display purposes only, so they
            # are NOT part of the row values and never get copied/exported.
            tree["columns"] = columns
            tree["show"] = (
                "tree headings"  # Show tree column (for row numbers) and headings
            )

            # Use the built-in tree column to display row numbers (display-only)
            tree.heading("#0", text="Row #")
            tree.column("#0", width=60, minwidth=60, anchor=tk.CENTER, stretch=False)

            # Configure data columns with borders
            for col in columns:
                tree.heading(col, text=str(col))
                tree.column(col, width=150, minwidth=80, anchor=tk.W)

            # Add tag for alternating row colors and borders
            tree.tag_configure("oddrow", background="#FFFFFF")
            tree.tag_configure("evenrow", background="#F5F5F5")

            # Add rows; row number goes in the tree column text (display only)
            for idx, row in enumerate(rows, start=1):
                decoded_row = []  # Data values only (no row number)
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        if val:
                            # Try multiple encodings for Oracle compatibility
                            for encoding in [
                                "utf-8",
                                "windows-1252",
                                "iso-8859-1",
                                "latin1",
                            ]:
                                try:
                                    decoded_row.append(val.decode(encoding))
                                    break
                                except (UnicodeDecodeError, AttributeError):
                                    continue
                            else:
                                # If all encodings fail, use replace strategy
                                decoded_row.append(
                                    val.decode("utf-8", errors="replace")
                                )
                        else:
                            decoded_row.append("")
                    else:
                        decoded_row.append(str(val) if val is not None else "")

                # Store decoded row for filtering (data only)
                tab_frame.original_rows.append(decoded_row)  # type: ignore[attr-defined]

                # Alternate row colors for better readability
                tag = "evenrow" if idx % 2 == 0 else "oddrow"
                tree.insert("", tk.END, text=str(idx), values=decoded_row, tags=(tag,))

            info_text = f"Rows: {rowcount} | Time: {exec_time:.3f}s"
            self.result_info_label.config(text=info_text)
            self.status_callback(
                f"Query executed successfully - {rowcount} rows returned"
            )

    def _show_multiple_results(self, results):
        """Display results from multiple queries"""
        success_count = 0
        error_count = 0

        for query_name, result, error, sql in results:
            if error:
                # Show error in a message tab
                error_count += 1
                tab_frame = ttk.Frame(self.results_notebook)
                error_text = scrolledtext.ScrolledText(
                    tab_frame, wrap=tk.WORD, height=10
                )
                error_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
                error_text.insert(1.0, f"SQL:\n{sql}\n\nError:\n{error}")
                error_text.config(state=tk.DISABLED)
                self.results_notebook.add(tab_frame, text=f"{query_name} - Error")
            else:
                # Show result
                success_count += 1
                self._show_results(result, query_name, sql)

        summary = f"Executed {len(results)} queries: {success_count} succeeded, {error_count} failed"
        self.result_info_label.config(text=summary)
        self.status_callback(summary)

    def _show_error(self, error, query_name="Query"):
        """Display error message"""
        self.result_info_label.config(text="Query failed")
        self.status_callback("Query execution failed")
        messagebox.showerror("Query Error", f"Error executing {query_name}:\n\n{error}")

    def clear_editor(self):
        """Clear the SQL editor"""
        self.sql_text.delete(1.0, tk.END)

    def clear_results(self):
        """Clear all result tabs"""
        for tab in self.results_notebook.tabs():
            self.results_notebook.forget(tab)
        self.result_tabs = []
        self.result_info_label.config(text="Ready")
        self.status_callback("Results cleared")

    def _setup_tree_features(self, tree, tab_frame):
        """Setup sorting, filtering, and copy features for result tree"""
        # Store reference to tree
        tab_frame.tree = tree

        # Track which cell (column) was clicked for copy operations
        tree.clicked_column = None
        tree.clicked_column_index = None

        # Context menu for tree (right-click on data)
        tree_menu = tk.Menu(tree, tearoff=0)
        tree_menu.add_command(
            label="Copy Cell", command=lambda: self._copy_tree_cell(tree)
        )
        tree_menu.add_command(
            label="Copy Row", command=lambda: self._copy_tree_row(tree)
        )
        tree_menu.add_command(
            label="Copy Column", command=lambda: self._copy_tree_column(tree)
        )
        tree_menu.add_command(
            label="Copy All Data", command=lambda: self._copy_tree_all(tree)
        )
        tree_menu.add_separator()
        tree_menu.add_command(
            label="Sort Ascending", command=lambda: self._sort_tree_column(tree, True)
        )
        tree_menu.add_command(
            label="Sort Descending", command=lambda: self._sort_tree_column(tree, False)
        )
        tree_menu.add_separator()
        tree_menu.add_command(
            label="Filter Column...",
            command=lambda: self._filter_tree_column(tree, tab_frame),
        )
        tree_menu.add_command(
            label="Clear Filter",
            command=lambda: self._clear_tree_filter(tree, tab_frame),
        )

        def show_tree_menu(event):
            try:
                # Track which cell was right-clicked
                region = tree.identify_region(event.x, event.y)
                if region == "cell":
                    column = tree.identify_column(event.x)
                    col_index = int(column.replace("#", "")) - 1
                    tree.clicked_column = column
                    tree.clicked_column_index = col_index
                tree_menu.post(event.x_root, event.y_root)
            finally:
                tree_menu.grab_release()

        tree.bind("<Button-2>", show_tree_menu)  # macOS
        tree.bind("<Button-3>", show_tree_menu)  # Windows/Linux

        # Keyboard copy / select-all on results (works alongside right-click)
        for seq in ("<Control-c>", "<Command-c>"):
            tree.bind(seq, lambda e, t=tree: self._copy_tree_selection(t))
        for seq in ("<Control-a>", "<Command-a>"):
            tree.bind(seq, lambda e, t=tree: self._select_all_tree(t))

        # Tab context menu for closing tabs
        self.results_notebook.bind("<Button-2>", self._show_tab_menu)  # macOS
        self.results_notebook.bind("<Button-3>", self._show_tab_menu)  # Windows/Linux

        # Bind combined handler for both cell clicks and header clicks
        tree.bind("<ButtonRelease-1>", lambda e: self._handle_tree_click(e, tree))

    def _handle_tree_click(self, event, tree):
        """Handle click on tree - either cell or column header"""
        region = tree.identify_region(event.x, event.y)

        if region == "cell":
            # Track which cell was clicked for copy operations
            column = tree.identify_column(event.x)
            col_index = int(column.replace("#", "")) - 1
            tree.clicked_column = column
            tree.clicked_column_index = col_index

        elif region == "heading":
            # Sort by clicked column header
            column = tree.identify_column(event.x)
            col_index = int(column.replace("#", "")) - 1
            if col_index >= 0:
                columns = tree["columns"]
                if col_index < len(columns):
                    col_name = columns[col_index]
                    # Toggle sort direction
                    if hasattr(tree, "_sort_column") and tree._sort_column == col_name:
                        tree._sort_ascending = not getattr(
                            tree, "_sort_ascending", True
                        )
                    else:
                        tree._sort_ascending = True
                    tree._sort_column = col_name
                    self._sort_tree_column(tree, tree._sort_ascending, col_name)

    def _copy_tree_cell(self, tree):
        """Copy selected cell value"""
        selection = tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select a row first")
            return

        item = tree.item(selection[0])
        values = item["values"]

        # Use the tracked clicked column if available
        if (
            hasattr(tree, "clicked_column_index")
            and tree.clicked_column_index is not None
        ):
            col_index = tree.clicked_column_index
            if col_index >= 0 and col_index < len(values):
                cell_value = str(values[col_index])
                self.parent.clipboard_clear()
                self.parent.clipboard_append(cell_value)
                columns = tree["columns"]
                col_name = (
                    columns[col_index]
                    if col_index < len(columns)
                    else f"Column {col_index}"
                )
                copy_limit = self._ui_limit("cell_copy_limit", 50, minimum=0)
                self.status_callback(
                    f"Copied cell from '{col_name}': {self._preview(cell_value, copy_limit)}"
                )
            else:
                messagebox.showwarning(
                    "Invalid Column", "Could not identify cell to copy"
                )
        else:
            # Fallback: copy first data column
            if len(values) > 0:
                self.parent.clipboard_clear()
                self.parent.clipboard_append(str(values[0]))
                self.status_callback("Cell value copied (first column)")

    def _copy_tree_selection(self, tree):
        """Copy all selected rows as TSV (keyboard Ctrl/Cmd+C)."""
        selection = tree.selection()
        if not selection:
            return "break"
        lines = []
        for item_id in selection:
            values = tree.item(item_id)["values"]
            lines.append("\t".join(str(v) for v in values))
        text = "\n".join(lines)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(text)
        self.status_callback(f"Copied {len(selection)} row(s)")
        return "break"

    def _select_all_tree(self, tree):
        """Select every row in the results grid (keyboard Ctrl/Cmd+A)."""
        items = tree.get_children()
        if items:
            tree.selection_set(items)
        return "break"

    def _copy_tree_row(self, tree):
        """Copy selected row as tab-separated values"""
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        values = item["values"]
        row_text = "\t".join(str(v) for v in values)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(row_text)
        self.status_callback("Row copied")

    def _copy_tree_column(self, tree):
        """Copy entire column"""
        columns = tree["columns"]
        if not columns:
            return

        # Use the tracked clicked column if available
        if (
            hasattr(tree, "clicked_column_index")
            and tree.clicked_column_index is not None
        ):
            col_index = tree.clicked_column_index
        else:
            # Fallback: copy first data column
            col_index = 0

        if col_index < 0 or col_index >= len(columns):
            messagebox.showwarning(
                "Invalid Column", "Could not identify column to copy"
            )
            return

        all_data = []
        # Add header
        all_data.append(columns[col_index])
        # Add all values
        for item_id in tree.get_children():
            item = tree.item(item_id)
            values = item["values"]
            if col_index < len(values):
                all_data.append(str(values[col_index]))

        column_text = "\n".join(all_data)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(column_text)
        self.status_callback(
            f"Column '{columns[col_index]}' copied ({len(all_data)-1} rows)"
        )

    def _current_result_tree(self):
        """Return (tree, tab_frame) for the active result tab, or (None, None).

        Lets the Query Results toolbar buttons act on whichever result tab is
        currently showing — the same target the right-click menu operates on.
        """
        current = self.results_notebook.select()
        if not current:
            return None, None
        try:
            tab_frame = self.results_notebook.nametowidget(current)
        except (KeyError, tk.TclError):
            return None, None
        return getattr(tab_frame, "tree", None), tab_frame

    def _toolbar_copy_all(self):
        """Copy All Data button → copy the active result tab as TSV."""
        tree, _ = self._current_result_tree()
        if tree is None or not tree["columns"]:
            messagebox.showinfo("Query Results", "No result data to copy.")
            return
        self._copy_tree_all(tree)

    def _toolbar_sort(self, ascending=True):
        """Sort Ascending/Descending button → sort the active tab's tree.

        Uses the last column the user clicked (header or cell) when available,
        otherwise the first column — matching the right-click menu behaviour.
        """
        tree, _ = self._current_result_tree()
        if tree is None or not tree["columns"]:
            messagebox.showinfo("Query Results", "No results to sort.")
            return
        columns = list(tree["columns"])
        col_index = getattr(tree, "clicked_column_index", None)
        column = (
            columns[col_index]
            if isinstance(col_index, int) and 0 <= col_index < len(columns)
            else columns[0]
        )
        tree._sort_column = column  # type: ignore[attr-defined]
        tree._sort_ascending = ascending  # type: ignore[attr-defined]
        self._sort_tree_column(tree, ascending, column)

    def _toolbar_filter(self):
        """Filter Column... button → open the filter dialog for the active tab."""
        tree, tab_frame = self._current_result_tree()
        if tree is None or not tree["columns"]:
            messagebox.showinfo("Query Results", "No results to filter.")
            return
        self._filter_tree_column(tree, tab_frame)

    def _toolbar_clear_filter(self):
        """Clear Filter button → restore all rows for the active tab."""
        tree, tab_frame = self._current_result_tree()
        if tree is None:
            messagebox.showinfo("Query Results", "No results to clear the filter on.")
            return
        self._clear_tree_filter(tree, tab_frame)

    def _copy_tree_all(self, tree):
        """Copy all data as TSV"""
        columns = tree["columns"]
        if not columns:
            return

        all_data = []
        # Add header
        all_data.append("\t".join(columns))
        # Add all rows
        for item_id in tree.get_children():
            item = tree.item(item_id)
            values = item["values"]
            all_data.append("\t".join(str(v) for v in values))

        data_text = "\n".join(all_data)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(data_text)
        self.status_callback(f"Copied {len(all_data)-1} rows")

    def _sort_tree_column(self, tree, ascending=True, column=None):
        """Sort tree by column"""
        if column is None:
            columns = tree["columns"]
            if not columns:
                return
            column = columns[0]  # Default to first column

        # Get all items
        items = [(tree.set(item, column), item) for item in tree.get_children("")]

        # Sort items
        try:
            # Try numeric sort first
            items.sort(
                key=lambda x: float(x[0]) if x[0] and x[0] != "NULL" else float("inf"),
                reverse=not ascending,
            )
        except (ValueError, TypeError):
            # Fall back to string sort
            items.sort(key=lambda x: str(x[0]).lower(), reverse=not ascending)

        # Rearrange items in sorted order
        for index, (val, item) in enumerate(items):
            tree.move(item, "", index)

        self.status_callback(
            f"Sorted by '{column}' ({'ascending' if ascending else 'descending'})"
        )

    def _filter_tree_column(self, tree, tab_frame):
        """Filter tree by column value"""
        columns = tree["columns"]
        if not columns:
            return

        # Create filter dialog
        dialog = tk.Toplevel(self.parent)
        dialog.title("Filter Column")
        dialog.geometry("400x200")
        dialog.transient(self.parent)

        ttk.Label(dialog, text="Column:", font=self._font_ui).pack(pady=(10, 5))
        col_combo = ttk.Combobox(dialog, values=columns, state="readonly", width=30)
        col_combo.pack(pady=5)
        if columns:
            col_combo.current(0)

        ttk.Label(dialog, text="Filter (contains):", font=self._font_ui).pack(
            pady=(10, 5)
        )
        filter_entry = ttk.Entry(dialog, width=35)
        filter_entry.pack(pady=5)
        filter_entry.focus()

        def apply_filter():
            column = col_combo.get()
            filter_text = filter_entry.get().strip().lower()

            if not filter_text:
                dialog.destroy()
                return

            # Use original_rows stored during result display
            if not hasattr(tab_frame, "original_rows") or not tab_frame.original_rows:
                messagebox.showwarning("Warning", "No data available to filter")
                dialog.destroy()
                return

            # Clear tree
            for item in tree.get_children():
                tree.delete(item)

            # Re-add filtered items. Row values are data-only now, so the
            # column index maps directly; row numbers are re-sequenced for the
            # filtered view via the display-only tree column.
            filtered_count = 0
            col_index = columns.index(column)
            for values in tab_frame.original_rows:
                if col_index < len(values):
                    cell_value = str(values[col_index]).lower()
                    if filter_text in cell_value:
                        tag = "evenrow" if filtered_count % 2 == 0 else "oddrow"
                        tree.insert(
                            "", "end", text=str(filtered_count + 1),
                            values=values, tags=(tag,),
                        )
                        filtered_count += 1

            self.status_callback(
                f"Filtered '{column}': showing {filtered_count} of {len(tab_frame.original_rows)} rows"
            )
            dialog.destroy()

        button_frame = ttk.Frame(dialog)
        button_frame.pack(pady=15)
        ttk.Button(button_frame, text="Apply", command=apply_filter).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(
            side=tk.LEFT, padx=5
        )

        filter_entry.bind("<Return>", lambda e: apply_filter())

        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    def _clear_tree_filter(self, tree, tab_frame):
        """Clear filter and restore original data"""
        if not hasattr(tab_frame, "original_rows") or not tab_frame.original_rows:
            self.status_callback("No active filter")
            return

        # Clear tree
        for item in tree.get_children():
            tree.delete(item)

        # Restore original data with alternating row colors and row numbers
        for idx, values in enumerate(tab_frame.original_rows, 1):
            tag = "evenrow" if idx % 2 == 0 else "oddrow"
            tree.insert("", "end", text=str(idx), values=values, tags=(tag,))

        self.status_callback(
            f"Filter cleared - showing all {len(tab_frame.original_rows)} rows"
        )

    def _show_tab_menu(self, event):
        """Show context menu on tab right-click"""
        try:
            # Identify which tab was clicked
            clicked_tab = self.results_notebook.tk.call(self.results_notebook._w, "identify", "tab", event.x, event.y)  # type: ignore[attr-defined]
            if clicked_tab != "":
                self.results_notebook.select(clicked_tab)

                # Create tab menu
                tab_menu = tk.Menu(self.results_notebook, tearoff=0)
                tab_menu.add_command(
                    label="Close This Tab", command=self._close_current_tab
                )
                tab_menu.add_command(label="Close All Tabs", command=self.clear_results)
                tab_menu.add_separator()
                tab_menu.add_command(
                    label="Close Other Tabs", command=self._close_other_tabs
                )

                tab_menu.post(event.x_root, event.y_root)
        except tk.TclError:
            pass

    def _close_current_tab(self):
        """Close currently selected tab"""
        current_tab = self.results_notebook.select()
        if current_tab:
            self.results_notebook.forget(current_tab)
            if current_tab in self.result_tabs:
                self.result_tabs.remove(current_tab)
            if not self.results_notebook.tabs():
                self.result_info_label.config(text="Ready")
            self.status_callback("Tab closed")

    def _close_other_tabs(self):
        """Close all tabs except current"""
        current_tab = self.results_notebook.select()
        if not current_tab:
            return
        tabs_to_close = [
            tab for tab in self.results_notebook.tabs() if tab != current_tab
        ]
        for tab in tabs_to_close:
            self.results_notebook.forget(tab)
            if tab in self.result_tabs:
                self.result_tabs.remove(tab)
        self.status_callback(f"Closed {len(tabs_to_close)} other tab(s)")

    def load_query(self):
        """Load query from file"""
        filename = filedialog.askopenfilename(
            title="Load SQL Query",
            filetypes=[
                ("SQL Files", "*.sql"),
                ("Text Files", "*.txt"),
                ("All Files", "*.*"),
            ],
        )
        if filename:
            try:
                with open(filename, "r") as f:
                    content = f.read()
                    self.sql_text.delete(1.0, tk.END)
                    self.sql_text.insert(1.0, content)
                self.status_callback(f"Loaded query from {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load file:\n{str(e)}")

    def save_query(self):
        """Save query to file"""
        sql = self.get_query_text()
        if not sql:
            messagebox.showwarning("Warning", "No query to save!")
            return

        filename = filedialog.asksaveasfilename(
            title="Save SQL Query",
            defaultextension=".sql",
            filetypes=[
                ("SQL Files", "*.sql"),
                ("Text Files", "*.txt"),
                ("All Files", "*.*"),
            ],
        )
        if filename:
            try:
                with open(filename, "w") as f:
                    f.write(sql)
                self.status_callback(f"Saved query to {filename}")
                messagebox.showinfo("Success", "Query saved successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")

    def commit_transaction(self):
        """Commit current transaction"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to database!")
            return

        if db_manager.commit():
            self.status_callback("Transaction committed")
            messagebox.showinfo("Success", "Transaction committed successfully!")
        else:
            messagebox.showerror("Error", "Failed to commit transaction")

    def rollback_transaction(self):
        """Rollback current transaction"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to database!")
            return

        if db_manager.rollback():
            self.status_callback("Transaction rolled back")
            messagebox.showinfo("Success", "Transaction rolled back successfully!")
        else:
            messagebox.showerror("Error", "Failed to rollback transaction")

    def _sync_autocommit_from_connection(self):
        """Reflect the live connection's autocommit state in this tab.

        Uses this tab's own session if it is already open; otherwise shows the
        configured default (the session adopts that default when it connects on
        first run, so no blocking connect happens just from selecting a tab).
        """
        db_manager = self._own_session if (
            self._own_session is not None
            and self._own_session_name == self.selected_connection_name
        ) else None
        if not db_manager or not db_manager.conn:
            self.autocommit_var.set(default_autocommit())
            return False
        try:
            self.autocommit_var.set(get_autocommit(db_manager.conn, db_manager.db_type))
            return True
        except Exception as e:
            self.status_callback(
                f"Warning: Could not read autocommit state: {str(e)}"
            )
            self.autocommit_var.set(default_autocommit())
            return False

    def _apply_autocommit_setting(self, enabled: Optional[bool] = None):
        """Apply current autocommit setting to the active connection."""
        if enabled is None:
            enabled = self.autocommit_var.get()
        else:
            self.autocommit_var.set(bool(enabled))
        is_enabled = self.autocommit_var.get()

        # Get current database connection
        db_manager = self.get_current_db_manager()
        if db_manager and db_manager.conn:
            try:
                set_autocommit(db_manager.conn, db_manager.db_type, is_enabled)
                self.autocommit_var.set(get_autocommit(db_manager.conn, db_manager.db_type))
                return True
            except Exception as e:
                self.status_callback(
                    f"Warning: Could not set autocommit on connection: {str(e)}"
                )
                return False
        return False

    def toggle_autocommit(self):
        """Toggle autocommit mode"""
        is_enabled = self.autocommit_var.get()
        status = "ON" if is_enabled else "OFF"

        # Apply to current connection
        success = self._apply_autocommit_setting()

        if success:
            self.status_callback(f" Auto-commit mode: {status}")

            if is_enabled:
                self.status_callback(
                    " Auto-commit ON: Changes will be committed automatically after each statement"
                )
            else:
                self.status_callback(
                    " Auto-commit OFF: Use Commit/Rollback buttons to finalize changes"
                )
        else:
            self.status_callback(
                f"Auto-commit mode set to: {status} (will apply when connected)"
            )

            if is_enabled:
                self.status_callback(
                    " Auto-commit ON: Changes will be committed automatically"
                )
            else:
                self.status_callback(" Auto-commit OFF: Use Commit/Rollback buttons")

    def apply_default_autocommit(self):
        """Apply the saved default autocommit setting to this tab's connection."""
        enabled = default_autocommit()
        success = self._apply_autocommit_setting(enabled)
        if success:
            status = "ON" if enabled else "OFF"
            self.status_callback(f"Auto-commit default applied to SQL tab: {status}")
        return success

    def show_history(self):
        """Show query history"""
        if not self.query_history:
            messagebox.showinfo("Query History", "No queries executed yet!")
            return

        history_window = tk.Toplevel(self.parent)
        history_window.title("Query History")
        width, height = get_window_size("history")
        history_window.geometry(f"{width}x{height}")

        # History list
        list_frame = ttk.Frame(history_window)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        history_listbox = tk.Listbox(
            list_frame, yscrollcommand=scrollbar.set, font=self._font_mono
        )
        history_listbox.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=history_listbox.yview)

        # Populate history
        preview_limit = self._ui_limit("sql_preview_limit", 100, minimum=0)
        for idx, item in enumerate(reversed(self.query_history)):
            display_sql = self._preview(item["sql"].replace("\n", " "), preview_limit)
            history_listbox.insert(tk.END, f"[{item['timestamp']}] {display_sql}")

        # Buttons
        btn_frame = ttk.Frame(history_window)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        def load_selected():
            selection = history_listbox.curselection()
            if selection:
                idx = len(self.query_history) - 1 - selection[0]
                sql = self.query_history[idx]["sql"]
                self.sql_text.delete(1.0, tk.END)
                self.sql_text.insert(1.0, sql)
                history_window.destroy()

        ttk.Button(btn_frame, text="Load Selected", command=load_selected).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Close", command=history_window.destroy).pack(
            side=tk.RIGHT, padx=5
        )

    def export_results(self):
        """Export results from current tab to CSV"""
        # Get currently selected tab
        current_tab = self.results_notebook.select()
        if not current_tab:
            messagebox.showwarning("Warning", "No result tab selected!")
            return

        # Get the tab frame
        tab_frame = self.results_notebook.nametowidget(current_tab)

        # Check if tab has result data
        if not hasattr(tab_frame, "result_data"):
            messagebox.showwarning("Warning", "Current tab has no exportable results!")
            return

        result_data = tab_frame.result_data

        filename = filedialog.asksaveasfilename(
            title="Export Results",
            defaultextension=".csv",
            filetypes=[
                ("CSV Files", "*.csv"),
                ("All Files", "*.*"),
            ],
        )

        if filename:
            try:
                self._export_to_csv(filename, result_data)

                self.status_callback(
                    f"Exported {result_data['rowcount']} rows to {filename}"
                )
                messagebox.showinfo(
                    "Success", f"Exported {result_data['rowcount']} rows!"
                )
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export:\n{str(e)}")
                import traceback

                traceback.print_exc()

    def _export_to_csv(self, filename, result_data):
        """Export result data to CSV file"""
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Write headers
            writer.writerow(result_data["columns"])
            # Write rows
            for row in result_data["rows"]:
                decoded_row = []
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        if val:
                            # Try multiple encodings for Oracle compatibility
                            for encoding in [
                                "utf-8",
                                "windows-1252",
                                "iso-8859-1",
                                "latin1",
                            ]:
                                try:
                                    decoded_row.append(val.decode(encoding))
                                    break
                                except (UnicodeDecodeError, AttributeError):
                                    continue
                            else:
                                # If all encodings fail, use replace strategy
                                decoded_row.append(
                                    val.decode("utf-8", errors="replace")
                                )
                        else:
                            decoded_row.append("")
                    else:
                        decoded_row.append(str(val) if val is not None else "")
                writer.writerow(decoded_row)

    def get_dashboard_snapshot(self) -> dict:
        """Runtime state for the operational dashboard."""
        last_sql = ""
        last_time = ""
        if self.query_history:
            last = self.query_history[-1]
            preview_limit = self._ui_limit("sql_preview_limit", 100, minimum=0)
            last_sql = self._preview((last.get("sql") or "").replace("\n", " "), preview_limit)
            last_time = last.get("timestamp") or ""

        conn = self.selected_connection_name or ""
        if self.query_running:
            overview = f"Executing query on {conn or '?'}"
        elif last_sql:
            overview = f"Last query on {conn or '?'} at {last_time or '—'}"
        elif conn:
            overview = f"Ready on {conn} — enter SQL and execute"
        else:
            overview = "Ready — select a connection and run SQL"

        return {
            "initialized": True,
            "query_running": self.query_running,
            "connection": conn,
            "last_query_preview": last_sql,
            "last_query_time": last_time,
            "history_count": len(self.query_history),
            "overview": overview,
        }
