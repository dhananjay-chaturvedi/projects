"""
Core CLI handlers shared by ``dbtool.py`` and per-module ``python -m`` entry points.

All handlers operate on a service object exposing the :class:`CoreDBService`
surface (connections, execute, get_objects, …).
"""

from __future__ import annotations

import argparse
import getpass
import json
from pathlib import Path
from typing import Any

from common.core import cliutil

CORE_CLI_COMMANDS = frozenset(
    {"connections", "query", "objects", "databases", "config", "app", "api", "ui"}
)


def _print_result(result: dict, fmt: str = "table") -> None:
    if result.get("error"):
        cliutil.err(result["error"])
        return
    if result.get("multiple_results"):
        for r in result.get("results", []):
            _print_result(r, fmt)
        if result.get("message"):
            cliutil.ok(result["message"])
        return
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    msg = result.get("message")
    if columns:
        cliutil.print_table(rows, columns, fmt)
        cliutil.info(f"{result.get('rowcount', len(rows))} row(s)  |  {result.get('time_ms', 0)} ms")
    elif msg:
        cliutil.ok(f"{msg}  ({result.get('time_ms', 0)} ms)")


def _print_objects(items: list, obj_type: str, fmt: str) -> None:
    if not items:
        cliutil.info(f"No {obj_type} found.")
        return
    if isinstance(items[0], dict) and "error" in items[0]:
        cliutil.err(items[0]["error"])
        return
    if any(isinstance(it, (list, tuple)) for it in items):
        width = max((len(it) if isinstance(it, (list, tuple)) else 1) for it in items)
        headers = [f"col{i + 1}" for i in range(width)]
        rows = []
        for it in items:
            row = list(it) if isinstance(it, (list, tuple)) else [it]
            row += [""] * (width - len(row))
            rows.append(row)
        cliutil.print_table(rows, headers, fmt)
    else:
        cliutil.print_table([[i] for i in items], [obj_type], fmt)


# Help tokens accepted after a hand-rolled core command (e.g. `connections -h`).
# These dispatchers parse their action by hand (not via argparse), so they must
# recognise help flags explicitly instead of mistaking them for an action name.
_HELP_FLAGS = frozenset({"-h", "--help", "help"})


def dispatch_core_argv(argv: list[str], svc: Any, *, prog: str = "dbtool") -> bool:
    """
    Run a core command from *argv* against *svc*. Returns True if handled.
    """
    if not argv or argv[0] not in CORE_CLI_COMMANDS:
        return False

    cmd = argv[0]
    rest = argv[1:]

    if cmd == "connections":
        return _dispatch_connections(rest, svc, prog=prog)
    if cmd == "query":
        return _dispatch_query(rest, svc, prog=prog)
    if cmd == "objects":
        return _dispatch_objects(rest, svc, prog=prog)
    if cmd == "databases":
        return _dispatch_databases(rest, svc, prog=prog)
    if cmd == "config":
        return _dispatch_config(rest, svc, prog=prog)
    if cmd == "app":
        return _dispatch_app(rest, svc, prog=prog)
    return False


def _dispatch_connections(argv: list[str], svc: Any, *, prog: str) -> bool:
    if argv and argv[0] in _HELP_FLAGS:
        cliutil.info(
            f"Usage: {prog} connections <action> [options]\n"
            "Actions:\n"
            "  list                        List saved DB connections\n"
            "  active                      List currently open connections\n"
            "  open <name>                 Open and cache a connection in this process\n"
            "  close <name>                Close a cached connection\n"
            "  close-all                   Close all cached connections\n"
            "  test <name>                 Test connectivity to a saved connection\n"
            "  remove <name>               Delete a saved connection\n"
            "  add --name N --type T --host H --user U\n"
            "      [--port P] [--password PW] [--db D] [--service S]\n"
            "                              Add/overwrite a saved connection"
        )
        return True
    if not argv:
        cliutil.err(
            f"Usage: {prog} connections "
            f"list|add|remove|test|open|close|close-all|active …"
        )
        return True
    action = argv[0]
    if action == "list":
        conns = svc.list_connections()
        if not conns:
            cliutil.info("No saved connections.")
            return True
        headers = ["name", "db_type", "host", "port", "service_or_db", "username"]
        rows = [[c.get(h, "") for h in headers] for c in conns]
        cliutil.print_table(rows, headers, "table")
        return True
    if action == "active":
        rows = svc.list_active_connections() if hasattr(svc, "list_active_connections") else []
        if not rows:
            cliutil.info("No active connections.")
            return True
        headers = ["name", "db_type", "host", "port", "service_or_db",
                   "username", "connected"]
        cliutil.print_table(
            [[r.get(h, "") for h in headers] for r in rows], headers, "table"
        )
        return True
    if action == "open":
        if len(argv) < 2:
            cliutil.err("connections open requires NAME")
            return True
        if not hasattr(svc, "open_connection"):
            cliutil.err("open_connection not supported by this service.")
            return True
        cliutil.info(f"Opening connection '{argv[1]}' …")
        r = svc.open_connection(argv[1])
        if r["ok"]:
            extras = []
            if r.get("db_type"):
                extras.append(f"db_type={r['db_type']}")
            if r.get("host"):
                extras.append(f"host={r['host']}")
            if r.get("version"):
                extras.append(f"version={r['version']}")
            tail = ("  |  " + "  |  ".join(extras)) if extras else ""
            cliutil.ok(f"{r['message']}{tail}")
            cliutil.info(
                "Note: 'connections open' caches the live connection for the "
                "current Python process only. To keep the connection warm "
                f"across calls, start the API server ({prog} api) and hit "
                "POST /api/connections/{name}/open instead."
            )
        else:
            cliutil.err(r["message"])
        return True
    if action == "close":
        if len(argv) < 2:
            cliutil.err("connections close requires NAME")
            return True
        if not hasattr(svc, "close_connection"):
            cliutil.err("close_connection not supported by this service.")
            return True
        r = svc.close_connection(argv[1])
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return True
    if action in ("close-all", "closeall"):
        if not hasattr(svc, "close_all_connections"):
            cliutil.err("close_all_connections not supported by this service.")
            return True
        r = svc.close_all_connections()
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        if r.get("closed"):
            cliutil.info("Closed: " + ", ".join(r["closed"]))
        return True
    if action == "add":
        from common.connection_params import ConnectionParams

        p = argparse.ArgumentParser(prog=f"{prog} connections add")
        p.add_argument("--name", required=True)
        p.add_argument("--type", required=True, dest="db_type")
        p.add_argument("--host", required=True)
        p.add_argument("--user", required=True)
        p.add_argument("--port", default="")
        p.add_argument("--password", default="")
        p.add_argument("--db", default="")
        p.add_argument("--service", default="")
        args = p.parse_args(argv[1:])
        pw = args.password or getpass.getpass(f"Password for {args.user}@{args.host}: ")
        r = svc.add_connection(
            ConnectionParams.from_mapping({
                "name": args.name,
                "db_type": args.db_type,
                "host": args.host,
                "port": args.port or "",
                "user": args.user,
                "password": pw,
                "database": args.db or "",
                "service": args.service or "",
            }),
        )
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return True
    if action == "remove":
        if len(argv) < 2:
            cliutil.err("connections remove requires NAME")
            return True
        r = svc.remove_connection(argv[1])
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return True
    if action == "test":
        if len(argv) < 2:
            cliutil.err("connections test requires NAME")
            return True
        cliutil.info(f"Testing connection '{argv[1]}' …")
        r = svc.test_connection(argv[1])
        if r["ok"]:
            cliutil.ok(f"{r['message']}  |  version: {r['version']}")
        else:
            cliutil.err(r["message"])
        return True
    cliutil.err(f"Unknown connections action: {action!r}")
    return True


def _dispatch_query(argv: list[str], svc: Any, *, prog: str) -> bool:
    # Subcommands for the SQL Editor's auxiliary controls (cancel, autocommit,
    # commit, rollback) sit ahead of the default execute parser.
    if argv and argv[0] == "format":
        rest = argv[1:]
        p = argparse.ArgumentParser(prog=f"{prog} query format")
        p.add_argument("--sql", default="")
        p.add_argument("--file", default="")
        p.add_argument("--output", default="",
                       help="Write formatted SQL to a file instead of stdout")
        args = p.parse_args(rest)
        if not hasattr(svc, "format_sql"):
            cliutil.err("format_sql not supported by this service.")
            return True
        sql = args.sql
        if not sql and args.file:
            try:
                with open(args.file, "r", encoding="utf-8") as fh:
                    sql = fh.read()
            except OSError as exc:
                cliutil.err(str(exc))
                return True
        if not sql:
            cliutil.err("Provide --sql or --file")
            return True
        r = svc.format_sql(sql)
        if not r.get("ok"):
            cliutil.err(r.get("message", "Could not format SQL."))
            return True
        out = r.get("sql", sql)
        if args.output:
            try:
                with open(args.output, "w", encoding="utf-8") as fh:
                    fh.write(out)
                cliutil.ok(f"Formatted SQL written to {args.output}")
            except OSError as exc:
                cliutil.err(str(exc))
        else:
            print(out)
        return True

    if argv and argv[0] in {"cancel", "commit", "rollback", "autocommit"}:
        action = argv[0]
        rest = argv[1:]
        p = argparse.ArgumentParser(prog=f"{prog} query {action}")
        if action == "autocommit":
            p.add_argument("op", choices=["get", "set"])
            p.add_argument("--conn", required=True)
            p.add_argument(
                "value", nargs="?", default=None,
                help="For 'set': on | off | true | false | 1 | 0",
            )
        else:
            p.add_argument("--conn", required=True)
        args = p.parse_args(rest)
        if action == "cancel":
            if not hasattr(svc, "cancel_query"):
                cliutil.err("cancel_query not supported by this service.")
                return True
            r = svc.cancel_query(args.conn)
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True
        if action == "commit":
            if not hasattr(svc, "commit"):
                cliutil.err("commit not supported by this service.")
                return True
            r = svc.commit(args.conn)
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True
        if action == "rollback":
            if not hasattr(svc, "rollback"):
                cliutil.err("rollback not supported by this service.")
                return True
            r = svc.rollback(args.conn)
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True
        if action == "autocommit":
            if args.op == "get":
                if not hasattr(svc, "get_autocommit"):
                    cliutil.err("get_autocommit not supported by this service.")
                    return True
                r = svc.get_autocommit(args.conn)
                if r["ok"]:
                    cliutil.ok(f"autocommit={'on' if r['autocommit'] else 'off'}")
                else:
                    cliutil.err(r["message"])
                return True
            # set
            if not hasattr(svc, "set_autocommit"):
                cliutil.err("set_autocommit not supported by this service.")
                return True
            val = (args.value or "").strip().lower()
            if val in ("on", "true", "1", "yes", "y"):
                enabled = True
            elif val in ("off", "false", "0", "no", "n"):
                enabled = False
            else:
                cliutil.err("query autocommit set requires: on | off")
                return True
            r = svc.set_autocommit(args.conn, enabled)
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True

    p = argparse.ArgumentParser(prog=f"{prog} query")
    p.add_argument("--conn", required=True)
    p.add_argument("--sql", default="")
    p.add_argument("--file", default="")
    p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    p.add_argument(
        "--multi", action="store_true",
        help="Split --sql / --file on ';' and execute statements serially",
    )
    args = p.parse_args(argv)
    sql = args.sql
    if args.file:
        try:
            sql = Path(args.file).read_text(encoding="utf-8")
        except OSError as exc:
            cliutil.err(str(exc))
            return True
    if not sql:
        cliutil.err("Provide --sql or --file")
        return True
    if args.multi:
        if not hasattr(svc, "execute_multi"):
            cliutil.err("execute_multi not supported by this service.")
            return True
        r = svc.execute_multi(args.conn, sql)
        if r.get("error") and not r.get("results"):
            cliutil.err(r["error"])
            return True
        for i, item in enumerate(r.get("results") or [], start=1):
            preview = item["statement"]
            if len(preview) > 80:
                preview = preview[:77] + "…"
            cliutil.info(f"[{i}/{r.get('count')}] {preview}")
            _print_result(item["result"], args.format)
        if r.get("error"):
            cliutil.err(r["error"])
        return True
    _print_result(svc.execute(args.conn, sql), args.format)
    return True


def _dispatch_objects(argv: list[str], svc: Any, *, prog: str) -> bool:
    # Table-tooling subcommands (sample, count, export, import-csv) live
    # ahead of the default object-list parser so they're discoverable.
    if argv and argv[0] in {"sample", "count", "export", "import-csv"}:
        action = argv[0]
        rest = argv[1:]
        if action == "sample":
            p = argparse.ArgumentParser(prog=f"{prog} objects sample")
            p.add_argument("--conn", required=True)
            p.add_argument("--table", required=True)
            p.add_argument("--limit", type=int, default=None,
                           help="Rows to sample (default from config)")
            p.add_argument(
                "--format", choices=["table", "json", "csv"], default="table"
            )
            args = p.parse_args(rest)
            if not hasattr(svc, "sample_table"):
                cliutil.err("sample_table not supported by this service.")
                return True
            r = svc.sample_table(args.conn, args.table, args.limit)
            if r.get("error"):
                cliutil.err(r["error"])
                return True
            _print_result(
                {"error": None, "columns": r.get("columns") or [],
                 "rows": r.get("rows") or [],
                 "rowcount": r.get("rowcount", 0), "time_ms": 0,
                 "message": f"{r.get('rowcount', 0)} sample row(s)"},
                args.format,
            )
            return True
        if action == "count":
            p = argparse.ArgumentParser(prog=f"{prog} objects count")
            p.add_argument("--conn", required=True)
            p.add_argument("--table", required=True)
            args = p.parse_args(rest)
            if not hasattr(svc, "count_table"):
                cliutil.err("count_table not supported by this service.")
                return True
            r = svc.count_table(args.conn, args.table)
            if r.get("error"):
                cliutil.err(r["error"])
                return True
            cliutil.ok(f"{r['table']}: {r['count']:,} row(s)")
            return True
        if action == "export":
            p = argparse.ArgumentParser(prog=f"{prog} objects export")
            p.add_argument("--conn", required=True)
            p.add_argument("--table", required=True)
            p.add_argument("--output", required=True, help="Destination file path")
            p.add_argument(
                "--format", choices=["csv", "json"], default="csv",
                help="File format (default: csv)",
            )
            p.add_argument("--limit", type=int, default=0,
                           help="Cap rows exported (0 = all)")
            args = p.parse_args(rest)
            if not hasattr(svc, "export_table"):
                cliutil.err("export_table not supported by this service.")
                return True
            r = svc.export_table(
                args.conn, args.table, args.output,
                fmt=args.format, limit=args.limit or None,
            )
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True
        if action == "import-csv":
            p = argparse.ArgumentParser(prog=f"{prog} objects import-csv")
            p.add_argument("--conn", required=True)
            p.add_argument("--file", required=True, help="Path to CSV file")
            p.add_argument(
                "--table", default="",
                help="Target table (default: CSV filename stem)",
            )
            p.add_argument(
                "--no-create", action="store_true",
                help="Skip CREATE TABLE IF NOT EXISTS",
            )
            p.add_argument(
                "--chunk-size", type=int, default=None,
                help="executemany batch size (default from config)",
            )
            args = p.parse_args(rest)
            if not hasattr(svc, "import_csv_to_table"):
                cliutil.err("import_csv_to_table not supported by this service.")
                return True
            r = svc.import_csv_to_table(
                args.conn, args.file,
                table=args.table or None,
                create_table=not args.no_create,
                chunk_size=args.chunk_size,
            )
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True

    p = argparse.ArgumentParser(prog=f"{prog} objects")
    p.add_argument("--conn", required=True)
    p.add_argument("--type", default="tables")
    p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    args = p.parse_args(argv)
    _print_objects(svc.get_objects(args.conn, args.type), args.type, args.format)
    return True


def _dispatch_databases(argv: list[str], svc: Any, *, prog: str) -> bool:
    if argv and argv[0] in _HELP_FLAGS:
        cliutil.info(
            f"Usage: {prog} databases <action>\n"
            "Actions:\n"
            "  types                       List supported database types (default)\n"
            "  ops --type T                List operations available for a database type"
        )
        return True
    if not argv or argv[0] == "types":
        rows = [
            [d["db_type"], d.get("display_name", ""), d.get("default_port", "")]
            for d in svc.list_db_types()
        ]
        cliutil.print_table(rows, ["type", "display_name", "default_port"], "table")
        return True
    if argv[0] == "ops":
        p = argparse.ArgumentParser(prog=f"{prog} databases ops")
        p.add_argument("--type", required=True)
        args = p.parse_args(argv[1:])
        rows = [
            [o.get("display_name", ""), o.get("operation", "")]
            for o in svc.list_db_ops(args.type)
        ]
        cliutil.print_table(rows, ["display_name", "operation"], "table")
        return True
    cliutil.err(f"Usage: {prog} databases types|ops …")
    return True


def _dispatch_config(argv: list[str], svc: Any, *, prog: str) -> bool:
    if argv and argv[0] in _HELP_FLAGS:
        cliutil.info(
            f"Usage: {prog} config <list|get|set|describe|restore|show>\n"
            f"  {prog} config list [--group G] [--format table|json]\n"
            f"  {prog} config get <id>\n"
            f"  {prog} config describe [<id>]\n"
            f"  {prog} config set <id> <value>\n"
            f"  {prog} config restore [--target all|config|properties] [--yes]\n"
            f"  {prog} config show [--section S]"
        )
        return True
    action = argv[0] if argv else ""

    # Raw INI dump (legacy / back-compat).
    if action == "show":
        p = argparse.ArgumentParser(prog=f"{prog} config show")
        p.add_argument("--section", default="")
        args = p.parse_args(argv[1:])
        r = svc.show_config(section=args.section or None)
        if r.get("error"):
            cliutil.err(r["error"])
            return True
        print(json.dumps(r.get("sections") or {}, indent=2))
        return True

    from common.config import settings_service as S

    if action == "list":
        p = argparse.ArgumentParser(prog=f"{prog} config list")
        p.add_argument("--group", default="", help="Filter by group name")
        p.add_argument("--format", choices=["table", "json"], default="table")
        args = p.parse_args(argv[1:])
        rows = S.describe_all()
        if args.group:
            rows = [r for r in rows if r["group"].lower() == args.group.lower()]
        if not rows:
            cliutil.info("No matching settings.")
            return True
        if args.format == "json":
            print(json.dumps(rows, indent=2, default=str))
            return True
        table = [[r["group"], r["id"], r["value"], r["default"],
                  "yes" if r["requires_restart"] else ""] for r in rows]
        cliutil.print_table(
            table, ["group", "id", "value", "default", "restart?"], "table"
        )
        cliutil.info(f"{len(rows)} setting(s). Use '{prog} config describe <id>' for details.")
        return True

    if action == "get":
        if len(argv) < 2:
            cliutil.err(f"Usage: {prog} config get <id>")
            return True
        spec = S.find(argv[1])
        if spec is None:
            cliutil.err(f"Unknown setting '{argv[1]}'.")
            return True
        print(json.dumps(S.describe(spec), indent=2, default=str))
        return True

    if action == "describe":
        rows = S.describe_all()
        if len(argv) >= 2:
            rows = [r for r in rows if r["id"] == argv[1]]
            if not rows:
                cliutil.err(f"Unknown setting '{argv[1]}'.")
                return True
        for r in rows:
            cliutil.info(cliutil.bold(r["id"]) + f"  ({r['type']})")
            print(f"    {r['label']}: {r['description']}")
            extra = []
            if r["options"]:
                extra.append("options: " + ", ".join(r["options"]))
            if r["minimum"] is not None or r["maximum"] is not None:
                extra.append(f"range: {r['minimum']}..{r['maximum']}")
            if r["unit"]:
                extra.append(f"unit: {r['unit']}")
            extra.append(f"default: {r['default'] or '(blank)'}")
            extra.append(f"current: {r['value']}")
            if r["requires_restart"]:
                extra.append("requires restart")
            print("    " + " | ".join(extra) + "\n")
        return True

    if action == "set":
        if len(argv) < 3:
            cliutil.err(f"Usage: {prog} config set <id> <value>")
            return True
        spec_id = argv[1]
        value = " ".join(argv[2:])
        r = S.set_value(spec_id, value)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        if r.get("ok") and r.get("requires_restart"):
            cliutil.warn("This setting takes effect after restarting the tool.")
        return True

    if action == "restore":
        p = argparse.ArgumentParser(prog=f"{prog} config restore")
        p.add_argument("--target", choices=["all", "config", "properties"], default="all")
        p.add_argument("--yes", action="store_true", help="Skip confirmation")
        args = p.parse_args(argv[1:])
        if not args.yes:
            import sys as _sys

            if _sys.stdin.isatty():
                resp = input(
                    f"Restore {args.target} settings to shipped defaults? "
                    "This overwrites your edits. [y/N]: "
                ).strip().lower()
                if resp not in ("y", "yes"):
                    cliutil.info("Cancelled.")
                    return True
            else:
                cliutil.err("Refusing to restore without --yes in non-interactive mode.")
                return True
        r = S.restore_defaults(args.target)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return True

    cliutil.err(
        f"Usage: {prog} config <list|get|set|describe|restore|show>\n"
        f"  {prog} config list [--group G] [--format table|json]\n"
        f"  {prog} config get <id>\n"
        f"  {prog} config describe [<id>]\n"
        f"  {prog} config set <id> <value>\n"
        f"  {prog} config restore [--target all|config|properties] [--yes]\n"
        f"  {prog} config show [--section S]"
    )
    return True


def _dispatch_app(argv: list[str], svc: Any, *, prog: str) -> bool:
    """App-level commands (Phase 7): cache clearing, dashboard layout,
    keyboard shortcut reference. Pass-through to
    :mod:`common.headless.app_service`.
    """
    from common.headless import app_service as appsvc

    if argv and argv[0] in _HELP_FLAGS:
        cliutil.info(
            f"Usage: {prog} app <action>\n"
            "Actions:\n"
            "  clear-caches                Clear all in-process caches\n"
            "  dashboard-layout show|reset|save [--rows JSON]\n"
            "  shortcuts [--section S]     Show keyboard shortcut reference"
        )
        return True
    if not argv:
        cliutil.err(
            f"Usage: {prog} app clear-caches | dashboard-layout {{show|reset|save}} | shortcuts [--section S]"
        )
        return True
    action = argv[0]

    if action == "clear-caches":
        r = appsvc.clear_all_caches(svc)
        (cliutil.ok if r["ok"] else cliutil.err)(r["summary"])
        if r["cleared"]:
            cliutil.info("Cleared: " + ", ".join(r["cleared"]))
        for s in r["skipped"]:
            cliutil.warn(f"Skipped {s['cache']}: {s.get('message', '')}")
        for e in r["errors"]:
            cliutil.err(f"{e['cache']}: {e['message']}")
        return True

    if action == "dashboard-layout":
        sub = argv[1] if len(argv) > 1 else "show"
        if sub == "show":
            r = appsvc.get_dashboard_layout()
            print(json.dumps(r, indent=2))
            return True
        if sub == "reset":
            r = appsvc.reset_dashboard_layout()
            cliutil.ok(r["message"])
            cliutil.info(f"Layout file: {r['path']}")
            return True
        if sub == "save":
            p = argparse.ArgumentParser(prog=f"{prog} app dashboard-layout save")
            p.add_argument(
                "--rows", required=True,
                help='JSON list of rows, each a list of panel ids or null. '
                     'Example: [["connections","monitor"],["ai","schema"]]',
            )
            args = p.parse_args(argv[2:])
            try:
                rows = json.loads(args.rows)
            except json.JSONDecodeError as exc:
                cliutil.err(f"Invalid JSON for --rows: {exc}")
                return True
            r = appsvc.save_dashboard_layout(rows)
            (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
            return True
        cliutil.err(
            f"Usage: {prog} app dashboard-layout show|reset|save --rows JSON"
        )
        return True

    if action == "shortcuts":
        p = argparse.ArgumentParser(prog=f"{prog} app shortcuts")
        p.add_argument("--section", default="",
                       help="Filter by section (Global | SQL Editor | …)")
        p.add_argument("--format", choices=["table", "json", "csv"],
                       default="table")
        args = p.parse_args(argv[1:])
        r = appsvc.list_shortcuts(args.section or None)
        rows = [[s["section"], s["shortcut"], s["action"]] for s in r["shortcuts"]]
        cliutil.print_table(rows, ["section", "shortcut", "action"], args.format)
        cliutil.info(
            f"{r['count']} shortcut(s); sections: {', '.join(r['sections'])}"
        )
        return True

    cliutil.err(
        f"Unknown app action '{action}'. Try: clear-caches | dashboard-layout | shortcuts"
    )
    return True
