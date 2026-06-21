"""Fixed product identity.

The tool's display name is **universal and not user-configurable** — it is a
constant rather than a ``config.ini`` value so every surface (UI window title,
about screens, etc.) stays in lock-step. Internal codenames used by the CLI
(``dbtool``) and the package/repo (``DbManagementTool``) are intentionally
separate from this user-facing brand.
"""

from __future__ import annotations

# User-facing product name shown in the main window title.
APP_NAME = "Database Assistant - Multi-DB Tool"

# Short brand used as a prefix for single-module / standalone window titles.
APP_SHORT_NAME = "Database Assistant"
