"""
Standalone / embeddable UI wiring for the Monitoring module.

* ``build_tab(parent, context)`` — embed ServerMonitorUI in a parent frame using
  the context the host provides.  Used by the combined master UI.
* ``launch_ui()`` — open a standalone monitoring window with its own context.
"""

from __future__ import annotations


def _default_context(parent, root):
    from common.ui.tk import ColorTheme
    from common.connection_manager import ConnectionManager

    active_connections: dict = {}
    cm = ConnectionManager()
    try:
        from monitoring.service import make_service

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
            root.title(f"Monitoring — {msg}")
        except Exception:
            pass

    return {
        "connection_manager": cm,
        "active_connections": active_connections,
        "update_status_callback": update_status,
        "theme": ColorTheme,
    }


def build_tab(parent, context: dict | None = None):
    """Create ServerMonitorUI inside *parent*."""
    from common.ui.tk.monitor.server_monitor import ServerMonitorUI

    context = context or {}
    root = context.get("root") or parent.winfo_toplevel()
    if "connection_manager" not in context:
        context = {**_default_context(parent, root), **context}

    ui = ServerMonitorUI(
        parent_frame=parent,
        root=root,
        connection_manager=context["connection_manager"],
        active_connections=context["active_connections"],
        update_status_callback=context["update_status_callback"],
        theme=context["theme"],
    )
    ui.create_ui()
    return ui


def launch_lite_ui(**context):
    """Bash menu UI (no tkinter)."""
    from common.core.standalone_runner import launch_shell_ui

    launch_shell_ui("monitor")


def launch_shell_ui(**context):
    from common.core.standalone_runner import launch_shell_ui as _go

    _go("monitor")


def launch_ui(**context):
    """Full module UI: Connections + Objects + SQL Editor + Monitor."""
    from common.ui.tk.monitor.monitoring_ui import launch_ui as _go

    return _go(**context)
