"""Manifest for the Data Migration module — discovered by common.core.modules.

The Python package is still ``schema_converter`` (internal), but the module is
surfaced to users as **Data Migration** under the ``migrator`` command: it
converts schemas/DDL, transfers data between databases, and validates the
result (schema + data comparison).
"""

from __future__ import annotations

from common.core.modules import ModuleManifest


def _check_requirements() -> list[str]:
    """Data migration only needs the core DB drivers; nothing extra."""
    return []


def _build_router(svc=None):
    from .api import build_router
    return build_router(svc)


def _launch_ui(**ctx):
    from common.ui.tk.migrator.schema_converter_ui import launch_ui

    launch_ui(**ctx)


def _build_tab(parent, context=None):
    from common.ui.tk.migrator.standalone import build_tab

    return build_tab(parent, context)


def _register_cli(subparsers):
    from .cli import register_cli
    return register_cli(subparsers)


def _dispatch_cli(args):
    from .cli import dispatch_cli
    return dispatch_cli(args)


MANIFEST = ModuleManifest(
    name="migrator",
    title="Data Migration",
    description="Migrate databases across Oracle/MySQL/MariaDB/PostgreSQL/SQLite: "
                "convert schemas/DDL, transfer data, and validate the migration "
                "(schema + data comparison).",
    register_cli=_register_cli,
    dispatch_cli=_dispatch_cli,
    cli_commands=["migrator"],
    build_router=_build_router,
    launch_ui=_launch_ui,
    build_tab=_build_tab,
    tab_label="Data Migration",
    config_files=["schema_converter/config.ini"],
    check_requirements=_check_requirements,
)
