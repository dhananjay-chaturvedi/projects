#!/usr/bin/env python3
"""Subprocess helper for Tk widget-interaction tests.

Pytest can abort on some Tk paths when UI widgets are built inside the test
worker process. Run the real UI in this short-lived subprocess instead, invoke
stable callbacks directly, print a compact success marker, then use os._exit()
so lingering Tk/monitoring callbacks cannot hang test teardown.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    import tkinter as tk

    import common.ui.tk.master_shell as ms
    from common.ui.tk.master_shell import UnifiedDBManagerUI

    root = tk.Tk()
    root.withdraw()
    app = UnifiedDBManagerUI(root, feature_module=None)
    root.update_idletasks()

    # 1) Tab switching is the main UI routing interaction. It must lazy-load
    # each stable screen and update the status bar without crashing.
    expected_tabs = {
        "Welcome",
        "Connections",
        "Dashboard",
        "Database Objects",
        "SQL Editor",
        "Data Migration",
        "AI Query Assistant",
        "Monitor",
        "Settings",
        "Clear Cache",
    }
    tab_names = {app.notebook.tab(tab_id, "text") for tab_id in app.notebook.tabs()}
    _assert(expected_tabs.issubset(tab_names), f"missing tabs: {expected_tabs - tab_names}")

    # Avoid opening the Clear Cache pseudo-tab here; it is exercised directly
    # below with patched messageboxes.
    for tab_id in app.notebook.tabs():
        name = app.notebook.tab(tab_id, "text")
        if name == "Clear Cache":
            continue
        app.notebook.select(tab_id)
        app.on_tab_changed()
        root.update_idletasks()

    for key in (
        "connections",
        "objects",
        "sql_editor",
        "conversion",
        "ai_query",
        "monitor",
        "settings",
    ):
        _assert(app.tabs_initialized.get(key) is True, f"{key} tab was not initialized")

    # 2) Status bar if/elif branches: success, error, warning, default/info.
    for status_type, expected_text in (
        ("info", "Info status"),
        ("success", "Success status"),
        ("error", "Error status"),
        ("warning", "Warning status"),
        ("unknown", "Default status"),
    ):
        app.update_status(expected_text, status_type)
        _assert(app.status_bar.cget("text") == expected_text, f"status {status_type} text")
        _assert(app.status_bar.cget("background"), f"status {status_type} background")

    # 3) Clear Cache negative path: user cancels; no caches are touched.
    message_calls: list[tuple[str, str, str]] = []
    ask_values = [False, True]

    def fake_askyesno(title, message, **_kw):
        message_calls.append(("ask", title, message))
        return ask_values.pop(0)

    def fake_showinfo(title, message, **_kw):
        message_calls.append(("info", title, message))

    def fake_showerror(title, message, **_kw):
        message_calls.append(("error", title, message))

    ms.messagebox.askyesno = fake_askyesno
    ms.messagebox.showinfo = fake_showinfo
    ms.messagebox.showerror = fake_showerror

    fake_ai = SimpleNamespace(
        invalidated=0,
        cleared=0,
        get_cache_info=lambda: {"local_mariadb": {"tables": 3}},
        invalidate_cache=lambda: setattr(fake_ai, "invalidated", fake_ai.invalidated + 1),
        clear_conversation=lambda: setattr(fake_ai, "cleared", fake_ai.cleared + 1),
    )
    app.ai_agent = fake_ai
    app.active_connections["kept_conn"] = SimpleNamespace(disconnect=lambda: None)
    before_active = dict(app.active_connections)

    app.clear_all_caches()
    _assert(fake_ai.invalidated == 0, "cancel path should not invalidate AI cache")
    _assert(fake_ai.cleared == 0, "cancel path should not clear AI conversation")
    _assert(app.active_connections == before_active, "cancel path should preserve active connections")

    # 4) Clear Cache positive path: clears caches, reloads managers, preserves
    # active DB connections, shows success, and updates status.
    app.connection_manager = SimpleNamespace(
        connections={"old": {}},
        get_all_connections=lambda: {"old": {}},
        load_connections=lambda: {"reloaded": {}},
    )
    app.monitor_connection_manager = SimpleNamespace(
        connections={"mon": {}},
        get_all_connections=lambda: {"mon": {}},
        load_connections=lambda: {"mon_reloaded": {}},
    )
    app.clear_all_caches()
    _assert(fake_ai.invalidated == 1, "success path invalidates AI cache")
    _assert(fake_ai.cleared == 1, "success path clears AI conversation")
    _assert("kept_conn" in app.active_connections, "success path preserves active connections")
    _assert(app.status_bar.cget("text") == "Cache cleared", "success path updates status")
    _assert(any(kind == "info" and title == "Cache Cleared" for kind, title, _ in message_calls),
            "success path shows info dialog")
    _assert(not any(kind == "error" for kind, _, _ in message_calls), "no error dialogs expected")

    print("UI_WIDGET_INTERACTIONS_OK")
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"UI_WIDGET_INTERACTIONS_FAIL: {exc}", file=sys.stderr)
        sys.stderr.flush()
        os._exit(1)
