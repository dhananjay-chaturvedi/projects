"""
Standalone entry point for the AI Query Assistant module.

Usage:
    python -m ai_query --ui
    python -m ai_query connections list
    python -m ai_query query --conn X --sql "SELECT 1"
    python -m ai_query ai --conn X "show all tables"
    python -m ai_query api [--host H] [--port P]
"""

from __future__ import annotations

import sys

from common.core.standalone_runner import run_standalone_module
from .cli import dispatch_cli, inject_oneshot_ask, register_cli

_AI_COMMANDS = {"ai"}


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    argv = inject_oneshot_ask(argv)
    return run_standalone_module(
        module_key="ai",
        prog="ai_query",
        description="AI Query Assistant — core (connections, objects, SQL) + AI queries.",
        register_cli=register_cli,
        dispatch_cli=dispatch_cli,
        module_commands=_AI_COMMANDS,
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
