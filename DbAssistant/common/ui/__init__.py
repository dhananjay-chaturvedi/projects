"""
Unified UI home for DbManagementTool.

All three UIs live side by side here and are each independently deletable and
shippable:

* ``common.ui.tk``      — Tkinter desktop UI (the visual/functional source of truth)
* ``common.ui.textual`` — Textual terminal UI
* ``common.ui.web``     — standalone HTML/CSS/JS web UI (own server, reads the
                          core service directly — never the REST API module)

``common.ui.shared`` holds the common UI properties (app title, tab order and
labels, theme palette, default web host/port) that every UI reads, so a change
there is reflected across all three while each UI still renders natively.

Isolation contract: nothing in this package is imported at module-load time by
core/CLI/API. Importing ``common.ui`` must never require tkinter, textual or
fastapi. Deleting any one sub-package leaves the others (and core/CLI/API)
working.
"""

from __future__ import annotations

__all__ = ["shared"]
