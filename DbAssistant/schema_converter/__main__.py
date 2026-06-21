"""
Standalone entry point for the Data Migration module (package: schema_converter).

Usage:
    python -m schema_converter --lite-ui
    python -m schema_converter migrator convert --source-conn db1 --target-type PostgreSQL --table users
    python -m schema_converter api
    python -m schema_converter --ui          # desktop UI (common/ + tkinter; no app/ required)
"""

from __future__ import annotations

from common.core.standalone_runner import run_standalone_module
from schema_converter.cli import dispatch_cli, register_cli

_MIGRATOR_COMMANDS = {"migrator"}


def main(argv: list[str] | None = None) -> int:
    return run_standalone_module(
        module_key="migrator",
        prog="schema_converter",
        description="Data Migration — common DB layer + schema/data migration & validation.",
        register_cli=register_cli,
        dispatch_cli=dispatch_cli,
        module_commands=_MIGRATOR_COMMANDS,
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
