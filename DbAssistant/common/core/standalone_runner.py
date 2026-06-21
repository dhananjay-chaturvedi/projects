"""
Shared runner for per-module ``python -m <package>`` entry points.

Works with ``common/`` + one module only (no ``app/`` required for CLI,
shell UI, module API, core commands, or desktop UI). Desktop UI is launched
via each module's ``*_ui.py`` (see ``_MODULE_UI_ENTRY``).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Callable, Optional, Set

from common.core.cli_handlers import CORE_CLI_COMMANDS, dispatch_core_argv
from common.core import cliutil

_MODULE_TITLES = {
    "migrator": "Data Migration",
    "ai": "AI Query Assistant",
    "monitor": "Monitoring",
    "app_builder": "App Builder",
}

_MODULE_SERVICE_FACTORIES = {
    "migrator": "schema_converter.bridge.make_service",
    "ai": "ai_query.service.make_service",
    "monitor": "monitoring.service.make_service",
    "app_builder": "ai_assistant.app_builder.service.make_service",
}

# Canonical desktop UI entry per module (``<package>.<ui_module>:launch_ui``).
_MODULE_UI_ENTRY = {
    "migrator": "common.ui.tk.migrator.schema_converter_ui:launch_ui",
    "ai": "common.ui.tk.ai.ai_query_ui:launch_ui",
    "monitor": "common.ui.tk.monitor.monitoring_ui:launch_ui",
}

_MODULE_TUI_ENTRY = {
    "migrator": "common.ui.textual.launcher:launch_textual_ui",
    "ai": "common.ui.textual.launcher:launch_textual_ui",
    "monitor": "common.ui.textual.launcher:launch_textual_ui",
}


def _import_factory(dotted: str):
    mod_name, _, fn_name = dotted.rpartition(".")
    import importlib

    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


def module_service(module_key: str):
    """Build core + module composite service for standalone CLI/API."""
    factory_path = _MODULE_SERVICE_FACTORIES.get(module_key)
    if factory_path is None:
        from common.headless.db_service import CoreDBService

        return CoreDBService()
    return _import_factory(factory_path)()


def shell_menu_script(module_key: str) -> Path:
    """Return path to ``shell_menu.sh`` for a module package."""
    from common.core import modules as app_modules

    pkg = app_modules.KNOWN_MODULES[module_key][0]
    import importlib.util

    spec = importlib.util.find_spec(pkg)
    if spec is None or not spec.submodule_search_locations:
        raise FileNotFoundError(f"Module package not found: {pkg}")
    script = Path(spec.submodule_search_locations[0]) / "shell_menu.sh"
    if not script.is_file():
        raise FileNotFoundError(f"Shell menu not found: {script}")
    return script


def launch_shell_ui(module_key: str) -> None:
    """Interactive bash menu for module fundamentals (no tkinter)."""
    script = shell_menu_script(module_key)
    os.execvp("bash", ["bash", str(script)])


def launch_lite_ui(module_key: str) -> None:
    """Alias for :func:`launch_shell_ui` (backward-compatible flag name)."""
    launch_shell_ui(module_key)


def launch_module_ui(module_key: str) -> None:
    """Open desktop UI via the module's canonical ``*_ui.py`` ``launch_ui()``."""
    from common.core.ui_registry import launch_tk_ui

    launch_tk_ui(feature_module=module_key)


def launch_module_tui(module_key: str) -> None:
    """Open Textual TUI for a standalone module."""
    from common.core.ui_registry import launch_textual_ui

    launch_textual_ui(feature_module=module_key)


def launch_module_web(module_key: str, *, host: str = "127.0.0.1", port: int = 8090) -> None:
    """Serve the HTML/JS web UI for a standalone module."""
    from common.core.ui_registry import launch_web_ui

    launch_web_ui(feature_module=module_key, host=host, port=port)


def serve_module_api(module_key: str, host: str, port: int) -> int:
    """Serve core + module REST API (no app/ required)."""
    try:
        import uvicorn
    except ImportError:
        print("FastAPI/uvicorn required: pip install fastapi uvicorn", file=sys.stderr)
        return 1

    from common.headless.app_factory import create_app

    title = _MODULE_TITLES.get(module_key, module_key)
    svc = module_service(module_key)
    app = create_app(module_key=module_key, title=f"{title} API", svc=svc)
    print(f"{title} API (core + module) on http://{host}:{port}/docs")
    uvicorn.run(app, host=host, port=port)
    return 0


def dispatch_core_cli(argv: list[str], module_key: str, *, prog: str) -> bool:
    """Run a core CLI command against the module's composite service."""
    if not argv or argv[0] not in CORE_CLI_COMMANDS:
        return False

    if module_key == "monitor" and argv[0] == "connections":
        if len(argv) > 1 and argv[1] == "add":
            # In the Monitoring standalone CLI, adding a DB connection must write
            # to the Monitor-only DB store (monitor_db.json), not the core db.json
            # used by SQL Editor / Migration / AI Query.
            return _dispatch_monitor_db_add_from_core_args(argv[2:], prog=prog)
        if len(argv) == 1 or (len(argv) > 1 and argv[1] == "list"):
            # Symmetric with the add shim above: if a first-time Monitoring
            # install has only monitor_db.json and no core db.json, `monitoring
            # connections list` should still show the saved Monitoring DB
            # profiles instead of looking empty.
            return _dispatch_monitor_connections_list()

    if argv[0] == "ui":
        if "--module" not in argv:
            launch_module_ui(module_key)
        else:
            launch_module_ui(module_key)
        return True

    if argv[0] == "api":
        host, port = "127.0.0.1", 8000
        args = argv[1:]
        i = 0
        while i < len(args):
            if args[i] == "--host" and i + 1 < len(args):
                host = args[i + 1]
                i += 2
            elif args[i] == "--port" and i + 1 < len(args):
                port = int(args[i + 1])
                i += 2
            elif args[i] == "--reload":
                print(
                    "[WARN] --reload is only supported via dbtool.py api on the full tool.",
                    file=sys.stderr,
                )
                i += 1
            else:
                i += 1
        return serve_module_api(module_key, host, port) == 0

    svc = module_service(module_key)
    return dispatch_core_argv(argv, svc, prog=prog)


def _dispatch_monitor_db_add_from_core_args(argv: list[str], *, prog: str) -> bool:
    """Compatibility shim for ``python -m monitoring connections add ...``.

    Older monitoring shell/CLI flows used the core ``connections add`` command.
    For Monitoring, that command should now target the isolated Monitor DB
    store while accepting the same flags users already know.
    """
    p = argparse.ArgumentParser(
        prog=f"{prog} connections add",
        description=(
            "In the Monitoring CLI this saves a Monitor-only DB connection "
            "(not a core Connections-tab profile)."
        ),
    )
    p.add_argument("--name", required=True)
    p.add_argument("--type", required=True, dest="db_type")
    p.add_argument("--host", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--port", default="")
    p.add_argument("--password", default="")
    p.add_argument("--db", default="")
    p.add_argument("--service", default="")
    args = p.parse_args(argv)

    if not args.password:
        import getpass

        args.password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    svc = module_service("monitor")
    if not hasattr(svc, "add_monitor_db_connection"):
        cliutil.err("Monitoring DB connection store is not available.")
        return True

    from common.connection_params import ConnectionParams

    r = svc.add_monitor_db_connection(
        ConnectionParams.from_mapping({
            "name": args.name,
            "db_type": args.db_type,
            "host": args.host,
            "port": args.port or "",
            "user": args.user,
            "password": args.password or "",
            "database": args.db or "",
            "service": args.service or "",
        })
    )
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    if r["ok"]:
        cliutil.info(
            "Saved to Monitoring only (monitor_db.json); not visible in the "
            "Connections tab."
        )
    return True


def _dispatch_monitor_connections_list() -> bool:
    """Compatibility list for ``python -m monitoring connections list``.

    Show the same monitor-visible connection set as ``monitor-connections
    list``: core DB profiles, Monitor-only DB profiles, Monitor SSH targets and
    cloud profiles. This keeps first-time monitor-only installs intuitive.
    """
    svc = module_service("monitor")
    if not hasattr(svc, "list_all_connections"):
        cliutil.err("Monitoring connection list is not available.")
        return True

    rows = svc.list_all_connections(source="all")
    errs = [r for r in rows if r.get("error")]
    rows = [r for r in rows if not r.get("error")]
    if not rows and not errs:
        cliutil.info("No saved monitoring connections.")
        return True

    headers = ["source", "name", "kind", "host", "port", "database",
               "username", "region", "resource"]
    seen: list[str] = []
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.append(k)
    headers = [h for h in headers if h in seen] + \
              [h for h in seen if h not in headers]
    cliutil.print_table(
        [[r.get(h, "") for h in headers] for r in rows], headers, "table"
    )
    for e in errs:
        cliutil.err(f"[{e.get('source','?')}] {e['error']}")
    return True


def run_standalone_module(
    *,
    module_key: str,
    prog: str,
    description: str,
    register_cli: Callable,
    dispatch_cli: Callable,
    module_commands: Set[str],
    argv: Optional[list[str]] = None,
) -> int:
    """
    Entry-point helper used by ``python -m <module>``.

    * ``--ui`` — desktop UI (Connections + Objects + SQL Editor + module tab; ``common/`` only).
    * ``--lite-ui`` / ``--shell-ui`` — bash menu (no tkinter).
    * Core CLI — ``connections``, ``query``, ``objects``, … via common handlers.
    * ``api`` — core + module REST API via common app factory.
    """
    try:
        return _run_standalone_module_body(
            module_key=module_key,
            prog=prog,
            description=description,
            register_cli=register_cli,
            dispatch_cli=dispatch_cli,
            module_commands=module_commands,
            argv=argv,
        )
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if os.environ.get("DBTOOL_DEBUG"):
            import traceback

            traceback.print_exc()
        return 1


def _run_standalone_module_body(
    *,
    module_key: str,
    prog: str,
    description: str,
    register_cli: Callable,
    dispatch_cli: Callable,
    module_commands: Set[str],
    argv: Optional[list[str]] = None,
) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])

    if "--lite-ui" in argv or "--shell-ui" in argv:
        launch_shell_ui(module_key)
        return 0

    if "--tui" in argv:
        launch_module_tui(module_key)
        return 0

    if "--web-ui" in argv:
        launch_module_web(module_key)
        return 0

    if "--ui" in argv:
        launch_module_ui(module_key)
        return 0

    if argv and dispatch_core_cli(argv, module_key, prog=prog):
        return 0

    parser = argparse.ArgumentParser(prog=prog, description=description)
    parser.add_argument(
        "--ui",
        action="store_true",
        help="Desktop UI: Connections, Objects, SQL Editor + module tab (needs tkinter)",
    )
    parser.add_argument(
        "--lite-ui",
        action="store_true",
        help="Bash menu UI (same as --shell-ui; no tkinter)",
    )
    parser.add_argument(
        "--shell-ui",
        action="store_true",
        help="Bash menu UI for this module (no tkinter)",
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Textual terminal UI (needs textual package)",
    )
    parser.add_argument(
        "--web-ui",
        dest="web_ui",
        action="store_true",
        help="HTML/JS web UI served on FastAPI (needs fastapi+uvicorn)",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    register_cli(sub)

    for cmd in sorted(CORE_CLI_COMMANDS):
        p = sub.add_parser(cmd, help=f"[core] use: {prog} {cmd} ...")
        p.add_argument("_ignored", nargs=argparse.REMAINDER, help=argparse.SUPPRESS)

    args = parser.parse_args(argv)

    if getattr(args, "lite_ui", False) or getattr(args, "shell_ui", False):
        launch_shell_ui(module_key)
        return 0

    if getattr(args, "tui", False):
        launch_module_tui(module_key)
        return 0

    if getattr(args, "web_ui", False):
        launch_module_web(module_key)
        return 0

    if args.ui:
        launch_module_ui(module_key)
        return 0

    if args.command in CORE_CLI_COMMANDS:
        dispatch_core_cli([args.command, *getattr(args, "_ignored", [])], module_key, prog=prog)
        return 0

    if args.command in module_commands:
        return dispatch_cli(args) or 0

    parser.print_help()
    return 1
