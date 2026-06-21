"""Tkinter desktop UI package (optional — delete this folder without breaking CLI/API)."""

from common.ui.tk.theme import ColorTheme, default_ui_font, default_ui_mono
from common.ui.tk.widgets import (
    bind_canvas_mousewheel,
    create_horizontal_scrollable,
    disable_combobox_mousewheel,
    make_collapsible_section,
    make_scrollable,
)
from common.ui.tk.launcher import launch_desktop_ui

__all__ = [
    "ColorTheme",
    "default_ui_font",
    "default_ui_mono",
    "bind_canvas_mousewheel",
    "create_horizontal_scrollable",
    "disable_combobox_mousewheel",
    "make_collapsible_section",
    "make_scrollable",
    "launch_desktop_ui",
]
