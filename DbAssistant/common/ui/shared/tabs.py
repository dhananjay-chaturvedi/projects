"""Canonical tab specification — the source of truth for tab order and labels.

Every UI builds its navigation from :data:`TABS` / :func:`visible_tabs`, so a
change here (rename a tab, reorder, add one) is reflected in the Tk, Textual and
Web UIs alike. The order and labels mirror ``ui_tk.master_shell`` exactly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# scope:
#   "full"   -> only shown in the full combined tool (not standalone module UIs)
#   "core"   -> always present
#   "module" -> present when that module is installed / selected
SCOPE_FULL = "full"
SCOPE_CORE = "core"
SCOPE_MODULE = "module"


@dataclass(frozen=True)
class TabSpec:
    id: str
    label: str
    scope: str
    module: Optional[str] = None  # set when scope == "module"


# Order is authoritative and matches the Tk notebook.
TABS: tuple[TabSpec, ...] = (
    TabSpec("welcome", "Welcome", SCOPE_FULL),
    TabSpec("connections", "Connections", SCOPE_CORE),
    TabSpec("dashboard", "Dashboard", SCOPE_CORE),
    TabSpec("objects", "Database Objects", SCOPE_CORE),
    TabSpec("sql_editor", "SQL Editor", SCOPE_CORE),
    TabSpec("migrator", "Data Migration", SCOPE_MODULE, module="migrator"),
    TabSpec("ai", "AI Query Assistant", SCOPE_MODULE, module="ai"),
    TabSpec("monitor", "Monitor", SCOPE_MODULE, module="monitor"),
    TabSpec("settings", "Settings", SCOPE_CORE),
    TabSpec("clear_cache", "Clear Cache", SCOPE_FULL),
)

_BY_ID = {t.id: t for t in TABS}


def tab_by_id(tab_id: str) -> Optional[TabSpec]:
    return _BY_ID.get(tab_id)


def _installed_modules() -> set[str]:
    """Best-effort discovery of installed module keys (migrator/ai/monitor)."""
    try:
        from common.core import modules as _modules

        status = _modules.status()
        return {k for k, v in status.items() if isinstance(v, dict) and v.get("installed")}
    except Exception:
        return set()


def visible_tabs(
    feature_module: Optional[str] = None,
    *,
    installed: Optional[set[str]] = None,
) -> list[TabSpec]:
    """Return the ordered tabs a UI should show.

    * ``feature_module`` set  -> standalone module UI: core tabs + that one
      module tab + Settings (no Welcome / Clear Cache), mirroring the Tk shell.
    * ``feature_module`` None -> full tool: Welcome + core + every installed
      module + Settings + Clear Cache.
    """
    standalone = feature_module is not None
    if installed is None:
        installed = {feature_module} if standalone else _installed_modules()

    out: list[TabSpec] = []
    for t in TABS:
        if t.scope == SCOPE_FULL:
            if not standalone:
                out.append(t)
        elif t.scope == SCOPE_CORE:
            out.append(t)
        elif t.scope == SCOPE_MODULE:
            if standalone:
                if t.module == feature_module:
                    out.append(t)
            elif t.module in installed:
                out.append(t)
    return out
