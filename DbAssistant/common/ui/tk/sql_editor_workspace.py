"""Multi-tab shell for the SQL Editor (one pane per notebook tab)."""

from __future__ import annotations

import tkinter as tk
import uuid
from tkinter import messagebox, ttk
from typing import Any, Callable, Optional

from common.ui.tk import ColorTheme
from common.ui.tk.sql_editor_pane import SQLEditorPane


class SQLEditorTab:
    """Notebook of independent SQL editor panes."""

    _TAB_ACTIVE_BG = ColorTheme.PRIMARY_LIGHT  # light blue for the active tab
    _TAB_INACTIVE_BG = ColorTheme.INFO_BG  # subtle light-blue tint for others
    _TAB_BORDER = ColorTheme.PRIMARY
    _TAB_CLOSE_FG = ColorTheme.TEXT_PRIMARY
    _TAB_CLOSE_FONT = ("TkDefaultFont", 11, "bold")

    def __init__(
        self,
        parent,
        get_connections_callback: Callable[[], dict],
        status_callback: Callable[[str], None],
        font_ui=None,
        font_mono=None,
    ):
        self.parent = parent
        self.root = parent.winfo_toplevel()
        self.get_connections_callback = get_connections_callback
        self.status_callback = status_callback
        self._font_ui = font_ui
        self._font_mono = font_mono
        self._tab_strip: tk.Frame | None = None
        self._content_host: ttk.Frame | None = None
        self._plus_btn: tk.Button | None = None
        self._panes: dict[str, SQLEditorPane] = {}
        self._strip_tabs: dict[str, dict[str, tk.Widget]] = {}
        self._tab_numbers: dict[str, int] = {}
        self._current_tab_id: Optional[str] = None
        self._next_tab = 1
        self._create_workspace_ui()

    def _create_workspace_ui(self) -> None:
        outer = ttk.Frame(self.parent)
        outer.pack(fill=tk.BOTH, expand=True)

        self._tab_strip = tk.Frame(outer, bg=ColorTheme.BG_MAIN, highlightthickness=0)
        self._tab_strip.pack(fill=tk.X, padx=4, pady=(2, 0))

        self._plus_btn = tk.Button(
            self._tab_strip,
            text="+",
            width=2,
            relief=tk.FLAT,
            bd=0,
            padx=4,
            pady=0,
            bg=ColorTheme.BG_MAIN,
            fg=ColorTheme.TEXT_PRIMARY,
            activebackground=ColorTheme.PRIMARY_LIGHT,
            command=self.add_tab,
        )
        self._plus_btn.pack(side=tk.LEFT, padx=(2, 0))

        self._content_host = ttk.Frame(outer)
        self._content_host.pack(fill=tk.BOTH, expand=True)

        self.add_tab()

    def add_tab(self, connection_name: str = "") -> None:
        if not self._content_host or not self._tab_strip:
            return
        tab_id = str(uuid.uuid4())
        tab_num = self._next_tab
        self._next_tab += 1
        frame = ttk.Frame(self._content_host)
        label = self._tab_label(tab_num, connection_name)

        pane = SQLEditorPane(
            frame,
            self.get_connections_callback,
            self.status_callback,
            font_ui=self._font_ui,
            font_mono=self._font_mono,
            on_meta_changed=lambda tid=tab_id: self._refresh_tab_label(tid),
        )
        pane.refresh_connections()
        if connection_name:
            pane.connection_combo.set(connection_name)
            pane.on_connection_changed()

        self._panes[tab_id] = pane
        self._tab_numbers[tab_id] = tab_num
        self._select_tab(tab_id)
        self._create_strip_tab(tab_id, label)

    def _create_strip_tab(self, tab_id: str, label: str) -> None:
        assert self._tab_strip is not None and self._plus_btn is not None

        chrome = tk.Frame(
            self._tab_strip,
            bg=self._TAB_INACTIVE_BG,
            highlightbackground=self._TAB_BORDER,
            highlightthickness=1,
        )
        chrome.pack(side=tk.LEFT, padx=(0, 2), before=self._plus_btn)

        title_lbl = tk.Label(
            chrome,
            text=label,
            bg=self._TAB_INACTIVE_BG,
            fg=ColorTheme.TEXT_PRIMARY,
            padx=8,
            pady=3,
            cursor="hand2",
        )
        title_lbl.pack(side=tk.LEFT, padx=(0, 12))
        title_lbl.bind("<Button-1>", lambda _e, tid=tab_id: self._select_tab(tid))

        close_btn = tk.Label(
            chrome,
            text="×",
            font=self._TAB_CLOSE_FONT,
            bg=self._TAB_INACTIVE_BG,
            fg=self._TAB_CLOSE_FG,
            padx=4,
            pady=0,
            cursor="hand2",
        )
        close_btn.place(relx=1.0, rely=0.0, anchor="ne", x=1, y=0)

        def _close(_event=None, tid=tab_id):
            self.close_tab(tid)
            return "break"

        close_btn.bind("<Button-1>", _close)
        close_btn.bind("<Enter>", lambda _e: close_btn.config(fg=ColorTheme.ERROR))
        close_btn.bind("<Leave>", lambda _e: close_btn.config(fg=self._TAB_CLOSE_FG))

        chrome.bind("<Button-1>", lambda _e, tid=tab_id: self._select_tab(tid))

        self._strip_tabs[tab_id] = {
            "chrome": chrome,
            "title_lbl": title_lbl,
            "close_btn": close_btn,
        }

    def _select_tab(self, tab_id: str) -> None:
        if tab_id not in self._panes or not self._content_host:
            return
        self._current_tab_id = tab_id
        for tid, pane in self._panes.items():
            if tid == tab_id:
                pane.parent.pack(fill=tk.BOTH, expand=True)
            else:
                pane.parent.pack_forget()
        self._update_tab_styles()
        self._on_tab_changed()

    def _update_tab_styles(self) -> None:
        for tab_id, widgets in self._strip_tabs.items():
            selected = tab_id == self._current_tab_id
            bg = self._TAB_ACTIVE_BG if selected else self._TAB_INACTIVE_BG
            chrome = widgets["chrome"]
            title_lbl = widgets["title_lbl"]
            close_btn = widgets["close_btn"]
            chrome.config(bg=bg)
            title_lbl.config(bg=bg)
            close_btn.config(bg=bg)

    def close_tab(self, tab_id: str) -> None:
        if tab_id not in self._panes:
            return
        if len(self._panes) <= 1:
            messagebox.showinfo("Close Tab", "At least one tab must remain open.")
            return

        was_current = tab_id == self._current_tab_id
        pane = self._panes.pop(tab_id)
        self._tab_numbers.pop(tab_id, None)
        strip = self._strip_tabs.pop(tab_id, None)
        if strip:
            strip["chrome"].destroy()
        # Close this tab's private DB session before tearing down the widgets.
        try:
            pane.dispose()
        except Exception:
            pass
        pane.parent.destroy()

        self._renumber_tabs()

        if was_current:
            remaining = next(iter(self._panes.keys()), None)
            if remaining:
                self._select_tab(remaining)

    def close_current_tab(self) -> None:
        if self._current_tab_id:
            self.close_tab(self._current_tab_id)

    def _current_pane(self) -> Optional[SQLEditorPane]:
        if self._current_tab_id and self._current_tab_id in self._panes:
            return self._panes[self._current_tab_id]
        return next(iter(self._panes.values()), None)

    def _tab_label(self, tab_num: int, connection_name: str) -> str:
        if connection_name:
            return f"Tab {tab_num} · {connection_name}"
        return f"Tab {tab_num}"

    def _refresh_tab_label(self, tab_id: str) -> None:
        if tab_id not in self._panes or tab_id not in self._strip_tabs:
            return
        pane = self._panes[tab_id]
        tab_num = self._tab_numbers.get(tab_id, 0)
        conn = pane.selected_connection_name or ""
        busy = " *" if pane.query_running else ""
        if conn:
            text = f"Tab {tab_num} · {conn}{busy}"
        else:
            text = f"Tab {tab_num}{busy}"
        self._strip_tabs[tab_id]["title_lbl"].config(text=text)

    def _renumber_tabs(self) -> None:
        ordered = sorted(self._tab_numbers.items(), key=lambda x: x[1])
        for i, (tab_id, _) in enumerate(ordered, start=1):
            self._tab_numbers[tab_id] = i
            self._refresh_tab_label(tab_id)
        self._next_tab = len(self._panes) + 1

    def _on_tab_changed(self, _event=None) -> None:
        pane = self._current_pane()
        if pane and pane.selected_connection_name:
            pane._sync_autocommit_from_connection()
            self.status_callback(f"SQL Editor — {pane.selected_connection_name}")

    @property
    def sql_text(self):
        pane = self._current_pane()
        if pane is None:
            raise RuntimeError("No SQL editor tab is open")
        return pane.sql_text

    @property
    def query_running(self) -> bool:
        return any(p.query_running for p in self._panes.values())

    def refresh_connections(self) -> None:
        for tab_id, pane in self._panes.items():
            pane.refresh_connections()
            self._refresh_tab_label(tab_id)

    def apply_default_autocommit(self) -> None:
        """Apply the saved autocommit default to every open SQL editor pane."""
        for pane in self._panes.values():
            pane.apply_default_autocommit()

    def dispose_all(self) -> None:
        """Close every tab's private DB session (call on app shutdown)."""
        for pane in self._panes.values():
            try:
                pane.dispose()
            except Exception:
                pass

    def _apply_query_mode(self, db_type: str) -> None:
        pane = self._current_pane()
        if pane:
            pane._apply_query_mode(db_type)

    def execute_at_cursor(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.execute_at_cursor()

    def execute_selected(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.execute_selected()

    def execute_all(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.execute_all()

    def load_query(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.load_query()

    def save_query(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.save_query()

    def commit_transaction(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.commit_transaction()

    def rollback_transaction(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.rollback_transaction()

    def show_history(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.show_history()

    def export_results(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.export_results()

    def clear_results(self) -> None:
        pane = self._current_pane()
        if pane:
            pane.clear_results()

    def get_dashboard_snapshot(self) -> dict[str, Any]:
        pane = self._current_pane()
        if pane is None:
            return {
                "initialized": True,
                "query_running": False,
                "connection": "",
                "last_query_preview": "",
                "last_query_time": "",
                "history_count": 0,
                "tab_count": 0,
                "overview": "Ready — open a SQL editor tab",
            }
        snap = pane.get_dashboard_snapshot()
        snap["tab_count"] = len(self._panes)
        if len(self._panes) > 1:
            snap["overview"] = f"{snap.get('overview', '')} ({len(self._panes)} tabs)"
        return snap
