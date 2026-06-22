"""Shim — implementation lives in common.ui.tk.ai.ai_query_ui."""

from __future__ import annotations


def __getattr__(name: str):
    from common.ui.tk.ai import ai_query_ui as _mod

    return getattr(_mod, name)


def launch_ui(**context):
    from common.ui.tk.ai.ai_query_ui import launch_ui as _go

    return _go(**context)
