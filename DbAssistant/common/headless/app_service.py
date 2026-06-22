"""
App-level helpers for CLI/API parity (Phase 7).

These are utilities the dashboard/master shell exposes in the UI but the CLI
and headless API previously had no way to reach: clearing in-process caches,
resetting the dashboard grid layout, and surfacing the keyboard-shortcut
reference. The functions are designed to be safe to call from a short-lived
CLI process and a long-running API server alike.

No new persistent files are introduced; the dashboard layout already lives in
``<DBASSISTANT_HOME>/session/dashboard_layout.json`` via ``common.dashboard.layout_store``.
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Cache clearing
# ---------------------------------------------------------------------------

def clear_all_caches(*services: Any) -> dict:
    """Clear every cache we know how to clear across the installed modules.

    Each passed-in service may carry one or more of the supported clear hooks:

    * ``clear_ai_cache(connection=None)`` — AI schema/context cache.
    * ``close_all_connections()`` — close all open DB sessions (optional).

    We only clear caches; we explicitly do **not** close open connections by
    default — those should be left up to the user. The returned dict reports
    what we actually did so the caller can render a useful summary.
    """
    cleared: list[str] = []
    skipped: list[dict] = []
    errors: list[dict] = []

    for svc in services:
        if svc is None:
            continue
        if hasattr(svc, "clear_ai_cache"):
            try:
                r = svc.clear_ai_cache(None)
                if r and r.get("ok"):
                    cleared.append("ai_schema_cache")
                else:
                    skipped.append({
                        "cache": "ai_schema_cache",
                        "message": (r or {}).get("message", ""),
                    })
            except Exception as exc:
                errors.append({"cache": "ai_schema_cache", "message": str(exc)})

    return {
        "ok": not errors,
        "cleared": cleared,
        "skipped": skipped,
        "errors": errors,
        "summary": (
            f"Cleared {len(cleared)} cache(s); "
            f"skipped {len(skipped)}; errors {len(errors)}."
        ),
    }


# ---------------------------------------------------------------------------
# Dashboard layout (thin pass-through to layout_store)
# ---------------------------------------------------------------------------

def get_dashboard_layout() -> dict:
    """Return the current dashboard layout grid + the default for comparison."""
    from common.dashboard import layout_store as ls

    return {
        "rows": ls.load_layout(),
        "default_rows": [list(row) for row in ls.DEFAULT_LAYOUT],
        "panel_ids": sorted(ls.ALL_PANEL_IDS),
        "path": str(ls._layout_file()),
    }


def reset_dashboard_layout() -> dict:
    """Reset the persisted dashboard layout to the default 2-column grid."""
    from common.dashboard import layout_store as ls

    layout = ls.reset_layout()
    return {
        "ok": True,
        "rows": layout,
        "path": str(ls._layout_file()),
        "message": "Dashboard layout reset to default.",
    }


def save_dashboard_layout(rows: list) -> dict:
    """Persist a new dashboard layout grid. Validates panel ids."""
    from common.dashboard import layout_store as ls

    if not isinstance(rows, list) or not rows:
        return {"ok": False, "message": "Layout rows must be a non-empty list."}
    cleaned: list[list[str | None]] = []
    for row in rows:
        if not isinstance(row, list):
            return {"ok": False, "message": "Each row must be a list."}
        cells: list[str | None] = []
        for cell in row[:2]:
            if cell in (None, ""):
                cells.append(None)
            elif isinstance(cell, str) and cell in ls.ALL_PANEL_IDS:
                cells.append(cell)
            else:
                return {
                    "ok": False,
                    "message": f"Unknown panel id '{cell}'. "
                               f"Allowed: {sorted(ls.ALL_PANEL_IDS)}",
                }
        while len(cells) < 2:
            cells.append(None)
        cleaned.append(cells)
    ls.save_layout(cleaned)
    return {
        "ok": True,
        "rows": cleaned,
        "path": str(ls._layout_file()),
        "message": "Dashboard layout saved.",
    }


# ---------------------------------------------------------------------------
# Keyboard shortcut reference
# ---------------------------------------------------------------------------

# Curated reference. Keep this in sync with what the UI actually binds; the
# strings come from the dashboard / SQL editor / monitor / AI panes.
_SHORTCUTS = [
    {"section": "Global", "shortcut": "Ctrl+Q / Cmd+Q",
     "action": "Quit the application (closes all connections gracefully)"},
    {"section": "Global", "shortcut": "Esc",
     "action": "Close the active dialog / dropdown"},
    {"section": "Global", "shortcut": "F1",
     "action": "Show the keyboard shortcuts panel"},

    {"section": "SQL Editor", "shortcut": "Ctrl+Enter / Cmd+Enter",
     "action": "Run the current SQL statement"},
    {"section": "SQL Editor", "shortcut": "Ctrl+/ / Cmd+/",
     "action": "Toggle SQL comment on the selected lines"},
    {"section": "SQL Editor", "shortcut": "Ctrl+Space",
     "action": "Autocomplete table/column suggestions"},
    {"section": "SQL Editor", "shortcut": "Ctrl+Z / Cmd+Z",
     "action": "Undo last editor change"},
    {"section": "SQL Editor", "shortcut": "Ctrl+Shift+Z / Cmd+Shift+Z",
     "action": "Redo last editor change"},

    {"section": "Connections", "shortcut": "Ctrl+N / Cmd+N",
     "action": "Open the 'Add database connection' dialog"},
    {"section": "Connections", "shortcut": "Delete",
     "action": "Remove the selected saved connection"},

    {"section": "Monitoring", "shortcut": "Ctrl+R / Cmd+R",
     "action": "Refresh the current monitoring view"},
    {"section": "Monitoring", "shortcut": "Space",
     "action": "Pause / resume the metrics auto-refresh loop"},

    {"section": "AI", "shortcut": "Ctrl+Enter / Cmd+Enter",
     "action": "Submit the current AI prompt"},
    {"section": "AI", "shortcut": "Ctrl+L / Cmd+L",
     "action": "Clear the AI conversation in the active tab"},
]


def list_shortcuts(section: str | None = None) -> dict:
    """Return the canonical keyboard shortcut reference.

    Optionally filter to one section (``Global``, ``SQL Editor`` ...). The
    UI's F1 panel and the CLI/API consume the same data.
    """
    want = (section or "").strip().lower()
    rows = list(_SHORTCUTS)
    if want:
        rows = [r for r in rows if r["section"].lower() == want]
    return {
        "shortcuts": rows,
        "count": len(rows),
        "sections": sorted({r["section"] for r in _SHORTCUTS}),
    }
