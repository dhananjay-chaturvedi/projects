"""Manifest for AppBuilderAssistant."""

from __future__ import annotations

from common.core.modules import ModuleManifest


def _register_cli(sub):
    from ai_assistant.app_builder.cli import register_cli
    return register_cli(sub)


def _dispatch_cli(args):
    from ai_assistant.app_builder.cli import dispatch_cli
    return dispatch_cli(args)


def _build_router(svc=None):
    from ai_assistant.app_builder.api import build_router
    return build_router(svc)


MANIFEST = ModuleManifest(
    name="app_builder",
    title="App Builder",
    description="Build apps from scratch, codebase or databases with AiAppEngine governance.",
    register_cli=_register_cli,
    dispatch_cli=_dispatch_cli,
    cli_commands=["app-builder"],
    build_router=_build_router,
    tab_label="App Builder",
)
