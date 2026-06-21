"""Desktop UI launcher shim — delegates to common.ui.tk via the registry."""

from __future__ import annotations

from typing import Optional


def launch_desktop_ui(feature_module: Optional[str] = None) -> None:
    from common.core.ui_registry import launch_tk_ui

    launch_tk_ui(feature_module=feature_module)


__all__ = ["launch_desktop_ui"]
