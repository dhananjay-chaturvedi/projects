"""
Shared UI properties — the single source of truth every UI reads.

This package is intentionally dependency-light: it imports only
``common.branding`` and ``common.config_loader`` (both headless-safe). It must
NEVER import tkinter, textual or fastapi, so any UI (or even the API) can read
the shared properties without pulling a UI toolkit.

What lives here:

* :mod:`common.ui.shared.properties` — app title, theme palette, fonts,
  default web host/port, window sizes.
* :mod:`common.ui.shared.tabs` — the canonical tab spec (id, label, order,
  scope) plus :func:`visible_tabs` which encodes which tabs each UI shows.
* :mod:`common.ui.shared.specs` — the declarative "common objects" (connection
  form fields, SQL-editor actions, migration options, AI settings/actions,
  cloud providers, keyboard shortcuts) that every UI renders identically.

Because Tk, Textual and Web all read these definitions, changing a tab label,
adding a tab, or tweaking the palette here is reflected across all three UIs
while each still renders natively. The Tk UI remains the visual/functional
source of truth; this module is how that truth is shared.
"""

from __future__ import annotations

from common.ui.shared.properties import (
    UITheme,
    app_title,
    default_web_host,
    default_web_port,
    fonts,
    theme,
    window_size,
)
from common.ui.shared.tabs import TABS, TabSpec, tab_by_id, visible_tabs
from common.ui.shared import specs

def advanced_modules_available() -> bool:
    from common.editions import advanced_modules_installed

    return advanced_modules_installed()


__all__ = [
    "UITheme",
    "app_title",
    "default_web_host",
    "default_web_port",
    "fonts",
    "theme",
    "window_size",
    "TABS",
    "TabSpec",
    "tab_by_id",
    "visible_tabs",
    "specs",
    "advanced_modules_available",
]
