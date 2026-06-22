"""
Standalone / embeddable UI wiring for the AI Query Assistant.

* ``build_tab(parent, context)`` — embed AIQueryWorkspace in a parent frame using the
  context the host provides (ai_agent, active_connections, callbacks, theme,
  fonts).  Used by the combined master UI.
* ``launch_ui()`` — open a standalone window: builds its own agent, connects
  saved connections through core, and supplies sensible default callbacks.
"""

from __future__ import annotations


def _default_context(parent, root):
    """Assemble a minimal context for a standalone AIQueryWorkspace."""
    from common.ui.tk import ColorTheme, default_ui_font, default_ui_mono
    from .agent import AIQueryAgent

    active_connections: dict = {}
    try:
        from ai_query.service import make_service

        svc = make_service()
        for c in svc.list_connections():
            name = c["name"]
            try:
                active_connections[name] = svc.get_manager(name)
            except Exception:
                # Skip connections that cannot be established right now.
                pass
    except Exception:
        pass

    def update_status(msg, *_a, **_k):
        try:
            root.title(f"AI Query Assistant — {msg}")
        except Exception:
            pass

    def send_to_editor(_sql):
        pass

    return {
        "ai_agent": AIQueryAgent(),
        "active_connections": active_connections,
        "update_status_callback": update_status,
        "send_to_editor_callback": send_to_editor,
        "theme": ColorTheme,
        "fonts": {"ui": default_ui_font(), "mono": default_ui_mono()},
    }


def build_tab(parent, context: dict | None = None):
    """Create AIQueryWorkspace inside *parent*. *context* must include AI deps."""
    from .ai_query_workspace import AIQueryWorkspace

    context = context or {}
    root = context.get("root") or parent.winfo_toplevel()
    if "ai_agent" not in context:
        context = {**_default_context(parent, root), **context}

    ui = AIQueryWorkspace(
        parent_frame=parent,
        root=root,
        ai_agent=context["ai_agent"],
        active_connections=context["active_connections"],
        update_status_callback=context["update_status_callback"],
        send_to_editor_callback=context["send_to_editor_callback"],
        theme=context["theme"],
        fonts=context["fonts"],
    )
    ui.create_ui()
    return ui


def launch_lite_ui(**context):
    """Bash menu UI (no tkinter). Kept for API compatibility."""
    from common.core.standalone_runner import launch_shell_ui

    launch_shell_ui("ai")


def launch_shell_ui(**context):
    from common.core.standalone_runner import launch_shell_ui as _go

    _go("ai")


def launch_ui(**context):
    """Full module UI: Connections + Objects + SQL Editor + AI Query."""
    from common.ui.tk.ai.ai_query_ui import launch_ui as _go

    return _go(**context)
