"""Module-local UI helpers — re-export shared ``common.ui`` (no duplication)."""

from common.ui.tk import (
    ColorTheme,
    bind_canvas_mousewheel,
    create_horizontal_scrollable,
    default_ui_font,
    default_ui_mono,
    make_collapsible_section,
    make_scrollable,
)

__all__ = [
    "ColorTheme",
    "default_ui_font",
    "default_ui_mono",
    "bind_canvas_mousewheel",
    "create_horizontal_scrollable",
    "make_collapsible_section",
    "make_scrollable",
]
