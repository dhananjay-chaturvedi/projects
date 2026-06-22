"""
Standalone / embeddable UI wiring for the Schema Converter.

* ``build_tab(parent, context)`` — embed SchemaConverterUI in a parent frame using the
  context the host provides (active_connections, callbacks, theme, fonts).  Used by the
  combined master UI.
* ``launch_ui()`` — open a standalone window: builds its own connections through core,
  and supplies sensible default callbacks.
"""

from __future__ import annotations


def _default_context(parent, root):
    """Assemble a minimal context for a standalone SchemaConverterUI."""
    from common.ui.tk import ColorTheme, default_ui_font, default_ui_mono

    active_connections: dict = {}

    def get_connections():
        return active_connections

    try:
        from schema_converter.bridge import make_service

        svc = make_service()
        for c in svc.list_connections():
            name = c["name"]
            try:
                active_connections[name] = svc.get_manager(name)
            except Exception:
                pass
    except Exception:
        pass

    def update_status(msg, *_a, **_k):
        try:
            root.title(f"Data Migration — {msg}")
        except Exception:
            pass

    return {
        "get_connections_callback": get_connections,
        "update_status_callback": update_status,
        "theme": ColorTheme,
        "fonts": {"ui": default_ui_font(), "mono": default_ui_mono()},
    }


def build_tab(parent, context: dict | None = None):
    """Create SchemaConverterUI inside *parent*."""
    from common.ui.tk.migrator.schema_converter_ui import SchemaConverterUI

    context = context or {}
    root = context.get("root") or parent.winfo_toplevel()
    defaults = _default_context(parent, root)
    get_connections = context.get("get_connections_callback")
    if get_connections is None and "active_connections" in context:
        active = context["active_connections"]

        def get_connections():
            return active

    merged = {**defaults, **context}
    if get_connections is not None:
        merged["get_connections_callback"] = get_connections
    elif "get_connections_callback" not in merged:
        merged["get_connections_callback"] = defaults["get_connections_callback"]

    ui = SchemaConverterUI(
        parent_frame=parent,
        root=root,
        get_connections_callback=merged["get_connections_callback"],
        update_status_callback=merged["update_status_callback"],
        theme=merged["theme"],
        fonts=merged["fonts"],
    )
    ui.create_ui()
    return ui


def launch_lite_ui(**context):
    """Bash menu UI (no tkinter)."""
    from common.core.standalone_runner import launch_shell_ui

    launch_shell_ui("migrator")


def launch_shell_ui(**context):
    from common.core.standalone_runner import launch_shell_ui as _go

    _go("migrator")


def launch_ui(**context):
    """Full module UI: Connections + Objects + SQL Editor + Schema Conversion."""
    from common.ui.tk.migrator.schema_converter_ui import launch_ui as _go

    return _go(**context)
