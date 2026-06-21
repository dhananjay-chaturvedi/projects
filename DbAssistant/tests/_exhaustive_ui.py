#!/usr/bin/env python3
"""Headless UI construction exerciser.

Tk works in a plain subprocess here (real display present). For each launch
mode we build the actual main window, pump the event loop so every tab/screen
constructs its widgets, walk the full widget tree, enumerate notebook tabs,
then tear down. Any exception during construction = FAIL.

Modes:
  * full     -> UnifiedDBManagerUI(feature_module=None)  (all module tabs)
  * migrator -> Data Migration standalone
  * ai       -> AI Query Assistant standalone
  * monitor  -> Monitoring standalone
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import tkinter as tk
from tkinter import ttk


def _walk(widget):
    n = 1
    for child in widget.winfo_children():
        n += _walk(child)
    return n


def _enumerate_tabs(widget, found):
    if isinstance(widget, ttk.Notebook):
        for tab_id in widget.tabs():
            try:
                found.append(widget.tab(tab_id, "text"))
            except Exception:
                pass
    for child in widget.winfo_children():
        _enumerate_tabs(child, found)


def exercise(mode):
    from common.ui.tk.master_shell import UnifiedDBManagerUI

    feature = None if mode == "full" else mode
    print(f"  [step] creating root ({mode})", flush=True)
    root = tk.Tk()
    root.withdraw()
    print("  [step] constructing UnifiedDBManagerUI", flush=True)
    app = UnifiedDBManagerUI(root, feature_module=feature)
    print("  [step] pumping event loop", flush=True)
    for _ in range(5):
        root.update_idletasks()
        root.update()
    print("  [step] walking widget tree", flush=True)
    widget_count = _walk(root)
    tabs = []
    _enumerate_tabs(root, tabs)
    print(f"  [step] tabs={tabs}", flush=True)
    # Try switching to each top-level notebook tab to force lazy tab builds.
    switched = []
    for child in root.winfo_children():
        if isinstance(child, ttk.Notebook):
            for tab_id in child.tabs():
                try:
                    child.select(tab_id)
                    root.update_idletasks()
                    switched.append(child.tab(tab_id, "text"))
                except Exception as exc:  # noqa
                    switched.append(f"<ERR {child.tab(tab_id,'text')}: {exc}>")
    print(f"  [step] switched={switched}", flush=True)
    return widget_count, tabs, switched


def _watchdog(seconds=35):
    import os
    import threading
    import time

    def _kill():
        time.sleep(seconds)
        print("UI_WATCHDOG timeout — forcing exit")
        os._exit(2)

    t = threading.Thread(target=_kill, daemon=True)
    t.start()


def main():
    _watchdog()
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"
    try:
        wc, tabs, switched = exercise(mode)
        print(f"UI_OK mode={mode} widgets={wc}")
        print(f"  tabs={tabs}")
        print(f"  switched_ok={switched}")
        return 0
    except Exception:
        print(f"UI_FAIL mode={mode}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import os

    rc = main()
    sys.stdout.flush()
    os._exit(rc)  # avoid hanging on lingering daemon/UI threads
