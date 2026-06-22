"""Shim — implementation lives in common.ui.tk.migrator.schema_converter_ui."""

from __future__ import annotations


def __getattr__(name: str):
    from common.ui.tk.migrator import schema_converter_ui as _mod

    return getattr(_mod, name)


def launch_ui(**context):
    from common.ui.tk.migrator.schema_converter_ui import launch_ui as _go

    return _go(**context)
