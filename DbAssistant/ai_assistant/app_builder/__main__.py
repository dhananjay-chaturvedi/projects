"""``python -m ai_assistant.app_builder``"""

from __future__ import annotations

from common.core.standalone_runner import run_standalone_module
from ai_assistant.app_builder.cli import dispatch_cli, register_cli

_COMMANDS = {"app-builder"}


def main(argv=None) -> int:
    return run_standalone_module(
        module_key="app_builder",
        prog="app_builder",
        description="App Builder — configuration-first app development with AiAppEngine.",
        register_cli=register_cli,
        dispatch_cli=dispatch_cli,
        module_commands=_COMMANDS,
        argv=argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
