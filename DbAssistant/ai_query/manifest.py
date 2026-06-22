"""Manifest for the AI Query Assistant module — discovered by core.modules."""

from __future__ import annotations

import shutil

from common.core.modules import ModuleManifest


def _check_requirements() -> list[str]:
    """At least one AI CLI backend (claude/cursor/codex) should be on PATH."""
    if any(shutil.which(b) for b in ("claude", "cursor-agent", "cursor", "codex")):
        return []
    return ["No AI CLI backend found on PATH (install one of: claude, cursor, codex)."]


def _register_cli(subparsers):
    from .cli import register_cli
    return register_cli(subparsers)


def _dispatch_cli(args):
    from .cli import dispatch_cli
    return dispatch_cli(args)


def _build_router(svc=None):
    from .api import build_router
    return build_router(svc)


def _launch_ui(**ctx):
    from common.ui.tk.ai.ai_query_ui import launch_ui

    launch_ui(**ctx)


def _build_tab(parent, context=None):
    from common.ui.tk.ai.standalone import build_tab

    return build_tab(parent, context)


MANIFEST = ModuleManifest(
    name="ai",
    title="AI Query Assistant",
    description="Convert natural-language questions to SQL using CLI AI backends "
                "(Claude / Cursor / Codex).",
    register_cli=_register_cli,
    dispatch_cli=_dispatch_cli,
    cli_commands=["ai"],
    build_router=_build_router,
    launch_ui=_launch_ui,
    build_tab=_build_tab,
    tab_label="AI Query Assistant",
    config_files=["ai_query/config.ini"],
    check_requirements=_check_requirements,
)
