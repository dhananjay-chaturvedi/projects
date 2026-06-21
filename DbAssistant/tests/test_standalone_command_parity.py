"""Guard: every module-owned CLI command registered by a module's
``register_cli`` must be listed in that module's ``module_commands`` set in
``__main__.py``.

Regression test for the ``monitor-config`` dispatch bug where a command was
registered in the parser but missing from ``_MON_COMMANDS``, causing
``python -m monitoring monitor-config ...`` to print top-level help and exit 1.
"""
from __future__ import annotations

import argparse

import pytest

from common.core.cli_handlers import CORE_CLI_COMMANDS


def _registered_top_level_commands(register_cli) -> set[str]:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_cli(sub)
    # choices holds every parser name registered by the module.
    for action in sub._get_subactions():  # noqa: SLF001 - introspection in test
        pass
    return set(sub.choices.keys())


@pytest.mark.parametrize(
    "main_module, cli_module, set_name",
    [
        ("monitoring.__main__", "monitoring.cli", "_MON_COMMANDS"),
        ("schema_converter.__main__", "schema_converter.cli", "_MIGRATOR_COMMANDS"),
        ("ai_query.__main__", "ai_query.cli", "_AI_COMMANDS"),
    ],
)
def test_module_command_set_covers_registered_commands(main_module, cli_module, set_name):
    import importlib

    main_mod = importlib.import_module(main_module)
    cli_mod = importlib.import_module(cli_module)

    declared = getattr(main_mod, set_name)
    registered = _registered_top_level_commands(cli_mod.register_cli)

    # Module-owned commands = everything registered that isn't a core command.
    module_owned = registered - set(CORE_CLI_COMMANDS)

    missing = module_owned - set(declared)
    assert not missing, (
        f"{set_name} is missing registered module commands {sorted(missing)}; "
        f"they will misroute via run_standalone_module()."
    )
