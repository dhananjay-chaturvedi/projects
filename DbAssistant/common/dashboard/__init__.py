"""Operational dashboard (core — module-aware, headless-safe)."""

from common.dashboard.service import DashboardService
from common.dashboard import layout_store
from common.dashboard import monitor_adapter

__all__ = ["DashboardService", "layout_store", "monitor_adapter"]


def __getattr__(name: str):
    """Lazy import for dashboard Tk UI."""
    if name == "DashboardUI":
        from common.ui.tk.dashboard_ui import DashboardUI

        return DashboardUI
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
