"""
Standalone entry point for the Monitoring module.

Usage:
    python -m monitoring --ui
    python -m monitoring connections list
    python -m monitoring query --conn X --sql "SELECT 1"
    python -m monitoring monitor --conn X --once
    python -m monitoring api [--host H] [--port P]
"""

from __future__ import annotations

from common.core.standalone_runner import run_standalone_module
from .cli import register_cli, dispatch_cli

_MON_COMMANDS = {
    "monitor", "monitor-connections", "monitor-db", "daemon", "thresholds",
    "os", "cloud", "notify", "alerts", "monitor-config",
}


def main(argv: list[str] | None = None) -> int:
    return run_standalone_module(
        module_key="monitor",
        prog="monitoring",
        description="Monitoring — core (connections, objects, SQL) + metrics/alerts.",
        register_cli=register_cli,
        dispatch_cli=dispatch_cli,
        module_commands=_MON_COMMANDS,
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
