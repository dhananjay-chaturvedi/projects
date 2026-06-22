#!/usr/bin/env python3
"""
dbtool.py  —  DbManagementTool CLI
====================================
Usage:
  python dbtool.py <command> [options]

Commands
--------
  connections list
  connections add  --name N --type TYPE --host H --user U [--port P] [--db D] [--service S]
  connections remove NAME
  connections test NAME

  query  --conn NAME (--sql "SQL" | --file path.sql) [--format table|json|csv]

  objects --conn NAME [--type TYPE]   # TYPE: tables|views|procs|functions|indexes|
                                      # triggers|sequences|constraints|events|databases|
                                      # users|schemas|tablespaces|engines|charsets|
                                      # processlist|roles|extensions|synonyms|packages|
                                      # types|materializedviews|databaselinks|profiles|
                                      # sessions|activity  (engine-dependent)

  migrator convert --source-conn NAME --target-type TYPE --table TABLE [--output file.sql]
  migrator transfer-data --source-conn NAME --target-conn NAME --table TABLE
  migrator compare-data  --source-conn NAME --target-conn NAME --table TABLE
  migrator show    --conn NAME --table TABLE
  migrator dump    --conn NAME [--table TABLE] [--output FILE.sql]

  ai --conn NAME [--backend NAME] "natural language question"
  ai --list-backends

  monitor --conn N1,N2,... [--interval 30] [--once] [--output metrics.json]

  daemon start  [--interval 30] [--connections N1,N2] [--pid-file PATH]
                [--log-file PATH] [--foreground] [--output PATH]
  daemon stop   [--pid-file PATH]
  daemon status [--pid-file PATH]

  api  [--host 0.0.0.0] [--port 8000] [--reload]

  databases types                       # supported DB engines
  databases ops   --type TYPE           # operations a driver supports

  thresholds list   [--source db|os|aws|azure|gcp]
  thresholds show   --source S --metric M
  thresholds check  --source S --metric M --value V [--instance I]

  config show  [--section S]
  config list  [--group G] [--format table|json]
  config describe [<id>]
  config get   <id>
  config set   <id> <value>
  config restore [--target all|config|properties] [--yes]

  notify send --severity INFO|WARNING|CRITICAL --message "..."

  os metrics  [--disk PATH]

  cloud connections list
  cloud connections add    --name N --provider aws|azure|gcp --json PROFILE.json
  cloud connections remove NAME
  cloud connections test   NAME
  cloud login   --name NAME            # interactive aws/az/gcloud login
  cloud metrics --name NAME
  cloud monitor --name NAME [--interval 30] [--once]

Global flags
------------
  --format   table (default) | json | csv   output format
  --no-color suppress ANSI colour output
"""

from __future__ import annotations

import argparse
import csv as csv_mod
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from common.config_loader import (
    get_api_host, get_api_port,
    get_webui_host, get_webui_port,
    get_tui_web_host, get_tui_web_port,
)

# ── Try tabulate for pretty tables ───────────────────────────────────────────
try:
    from tabulate import tabulate as _tabulate
    _TABULATE = True
except ImportError:
    _TABULATE = False

# ── Colour helpers ────────────────────────────────────────────────────────────
_COLOUR = True

def _c(code, text):
    if not _COLOUR:
        return text
    return f"\033[{code}m{text}\033[0m"

def _green(t):  return _c("0;32", t)
def _red(t):    return _c("0;31", t)
def _yellow(t): return _c("1;33", t)
def _cyan(t):   return _c("0;36", t)
def _bold(t):   return _c("1",    t)

def _ok(msg):   print(_green(f"[OK]  {msg}"))
def _err(msg):  print(_red(f"[ERR] {msg}"), file=sys.stderr)
def _info(msg): print(_cyan(f"[   ] {msg}"))
def _warn(msg): print(_yellow(f"[WARN] {msg}"))


# ── Output helpers ────────────────────────────────────────────────────────────

def _print_result(result: dict, fmt: str = "table"):
    """Render an execute() result dict in the requested format."""
    if result.get("error"):
        _err(result["error"])
        return

    if result.get("multiple_results"):
        for r in result.get("results", []):
            _print_result(r, fmt)
        _ok(result.get("message", ""))
        return

    columns = result.get("columns", [])
    rows    = result.get("rows", [])
    msg     = result.get("message")

    if columns:
        if fmt == "json":
            print(json.dumps([dict(zip(columns, row)) for row in rows], indent=2, default=str))
        elif fmt == "csv":
            w = csv_mod.writer(sys.stdout)
            w.writerow(columns)
            w.writerows(rows)
        else:
            if _TABULATE:
                print(_tabulate(rows, headers=columns, tablefmt="rounded_outline"))
            else:
                print("\t".join(columns))
                print("-" * 60)
                for row in rows:
                    print("\t".join(str(v) for v in row))
        _info(f"{result.get('rowcount', len(rows))} row(s)  |  {result.get('time_ms', 0)} ms")
    elif msg:
        _ok(f"{msg}  ({result.get('time_ms', 0)} ms)")


def _print_table(rows: list, headers: list, fmt: str = "table"):
    if fmt == "json":
        print(json.dumps([dict(zip(headers, r)) for r in rows], indent=2, default=str))
    elif fmt == "csv":
        w = csv_mod.writer(sys.stdout)
        w.writerow(headers)
        w.writerows(rows)
    else:
        if _TABULATE:
            print(_tabulate(rows, headers=headers, tablefmt="rounded_outline"))
        else:
            print("\t".join(headers))
            print("-" * 60)
            for row in rows:
                print("\t".join(str(c) for c in row))


def _prompt_password(prompt: str = "Password: ") -> str:
    import getpass
    return getpass.getpass(prompt)


# ── Service singleton ─────────────────────────────────────────────────────────

_svc = None

def _service():
    global _svc
    if _svc is None:
        # Import here so startup is fast for --help
        sys.path.insert(0, str(Path(__file__).parent))
        from app.headless.db_service import DBService
        _svc = DBService()
    return _svc


# =============================================================================
# Command handlers
# =============================================================================

def cmd_connections_list(args):
    conns = _service().list_connections()
    if not conns:
        _info("No saved connections.")
        return
    headers = ["name", "db_type", "host", "port", "service_or_db", "username"]
    rows = [[c.get(h, "") for h in headers] for c in conns]
    _print_table(rows, headers, args.format)


def cmd_connections_add(args):
    from common.connection_params import ConnectionParams

    pw = args.password or _prompt_password(f"Password for {args.user}@{args.host}: ")
    ssh_tunnel = None
    if getattr(args, "ssh_host", ""):
        ssh_pw = args.ssh_password
        if ssh_pw == "" and not args.ssh_key_file:
            ssh_pw = _prompt_password(
                f"SSH password for {args.ssh_user}@{args.ssh_host} "
                f"(blank to use key file): "
            )
        ssh_tunnel = {
            "ssh_host": args.ssh_host,
            "ssh_user": args.ssh_user or "",
            "ssh_port": args.ssh_port or 22,
            "ssh_password": ssh_pw or "",
            "ssh_key_file": args.ssh_key_file or "",
        }
    result = _service().add_connection(
        ConnectionParams.from_mapping({
            "name": args.name,
            "db_type": args.type,
            "host": args.host,
            "port": args.port or "",
            "user": args.user,
            "password": pw,
            "database": args.db or "",
            "service": args.service or "",
            "ssh_tunnel": ssh_tunnel,
        }),
    )
    (_ok if result["ok"] else _err)(result["message"])


def cmd_connections_remove(args):
    result = _service().remove_connection(args.name)
    (_ok if result["ok"] else _err)(result["message"])


def cmd_connections_test(args):
    _info(f"Testing connection '{args.name}' ...")
    r = _service().test_connection(args.name)
    if r["ok"]:
        _ok(f"{r['message']}  |  version: {r['version']}")
    else:
        _err(r["message"])


def cmd_query(args):
    sql = ""
    if args.sql:
        sql = args.sql
    elif args.file:
        try:
            sql = Path(args.file).read_text()
        except OSError as e:
            _err(str(e)); return
    else:
        _err("Provide --sql or --file"); return

    svc = _service()
    ac = getattr(args, "autocommit", "")
    if ac in ("on", "off"):
        if not hasattr(svc, "set_autocommit"):
            _err("set_autocommit not supported by this service."); return
        try:
            svc.get_manager(args.conn)
        except Exception as e:
            _err(str(e)); return
        r = svc.set_autocommit(args.conn, ac == "on")
        if not r.get("ok"):
            _err(r.get("message", "Could not set autocommit")); return
    result = svc.execute(args.conn, sql)
    _print_result(result, args.format)


def cmd_format_sql(args):
    sql = ""
    if args.sql:
        sql = args.sql
    elif args.file:
        try:
            sql = Path(args.file).read_text()
        except OSError as e:
            _err(str(e)); return
    else:
        _err("Provide --sql or --file"); return
    svc = _service()
    if not hasattr(svc, "format_sql"):
        _err("format_sql not supported by this service."); return
    r = svc.format_sql(sql)
    if not r.get("ok"):
        _err(r.get("message", "Could not format SQL.")); return
    out = r.get("sql", sql)
    if args.output:
        try:
            Path(args.output).write_text(out)
            print(f"Formatted SQL written to {args.output}")
        except OSError as e:
            _err(str(e))
    else:
        print(out)


def cmd_autocommit(args):
    svc = _service()
    if args.autocommit_action == "get":
        if not hasattr(svc, "get_autocommit"):
            _err("get_autocommit not supported by this service."); return
        try:
            svc.get_manager(args.conn)
        except Exception as e:
            _err(str(e)); return
        r = svc.get_autocommit(args.conn)
        if not r.get("ok"):
            _err(r.get("message", "failed")); return
        _ok(f"autocommit={r.get('autocommit')}")
        return
    # set
    if not hasattr(svc, "set_autocommit"):
        _err("set_autocommit not supported by this service."); return
    try:
        svc.get_manager(args.conn)
    except Exception as e:
        _err(str(e)); return
    enabled = str(args.enabled).strip().lower() in ("1", "true", "on", "yes")
    r = svc.set_autocommit(args.conn, enabled)
    (_ok if r.get("ok") else _err)(r.get("message", f"autocommit={enabled}"))


def cmd_objects(args):
    obj_type = args.type or "tables"
    items = _service().get_objects(args.conn, obj_type)
    if not items:
        _info(f"No {obj_type} found.")
        return
    if isinstance(items[0], dict) and "error" in items[0]:
        _err(items[0]["error"]); return

    # Multi-column object types (e.g. processlist, users) come back as
    # tuples/lists — render them as a proper table with generic columns.
    if any(isinstance(it, (list, tuple)) for it in items):
        width = max((len(it) if isinstance(it, (list, tuple)) else 1) for it in items)
        headers = [f"col{i + 1}" for i in range(width)]
        rows = []
        for it in items:
            row = list(it) if isinstance(it, (list, tuple)) else [it]
            row += [""] * (width - len(row))
            rows.append(row)
        _print_table(rows, headers, args.format)
    else:
        _print_table([[i] for i in items], [obj_type], args.format)


def cmd_schema_convert(args):
    _info(f"Converting '{args.table}' from '{args.source_conn}' → {args.target_type} ...")
    r = _service().convert_schema(args.source_conn, args.target_type, args.table)
    if r["error"]:
        _err(r["error"]); return

    ddl_parts = []
    if r["ddl"]:
        ddl_parts.append(r["ddl"])
    for idx in (r.get("indexes_ddl") or []):
        ddl_parts.append(idx)
    full_ddl = "\n\n".join(ddl_parts)

    if args.output:
        Path(args.output).write_text(full_ddl)
        _ok(f"DDL written to {args.output}")
    else:
        print(full_ddl)

    issues = r.get("issues") or []
    if issues:
        print()
        _yellow(f"⚠  {len(issues)} conversion warning(s):")
        for iss in issues:
            print(f"   • {iss}")


def cmd_ai(args):
    if getattr(args, "list_backends", False):
        info = _service().list_ai_backends()
        if not info.get("available"):
            _err(info.get("error") or "AI not available."); return
        rows = []
        ready = set(info.get("ready") or [])
        active = info.get("active") or ""
        for b in (info.get("all") or []):
            status = "ready" if b in ready else "not verified"
            mark = " *" if b == active else ""
            rows.append([b + mark, status])
        _print_table(rows, ["backend", "status"], args.format)
        _info("* = active backend")
        return

    if not args.conn:
        _err("--conn is required to ask a question."); return
    if not args.question:
        _err("Provide a question, or use --list-backends."); return

    question = " ".join(args.question)
    _info(f"Asking AI: {question}")
    r = _service().ai_query(args.conn, question, backend=args.backend or None)
    if r.get("error"):
        _err(r["error"]); return
    if r.get("sql"):
        print(_bold("Generated SQL:"))
        print(r["sql"])
    if r.get("explanation"):
        print()
        print(_bold("Explanation:"))
        print(r["explanation"])


def cmd_monitor(args):
    """Run one or more monitoring cycles (non-daemon, blocking)."""
    names = [n.strip() for n in (args.conn or "").split(",") if n.strip()]
    if not names:
        # Use all saved connections
        names = [c["name"] for c in _service().list_connections()]
    if not names:
        _err("No connections to monitor."); return

    interval = args.interval
    once     = args.once
    out_path = args.output
    metrics_store = {}

    def _poll():
        ts = datetime.now().strftime("%H:%M:%S")
        for name in names:
            _info(f"[{ts}] Collecting metrics for '{name}' ...")
            # monitor_any auto-dispatches across db / cloud / monitor (SSH)
            # so dbtool monitor --conn NAME works for every source listed by
            # `dbtool monitor-connections list`.
            r = _service().monitor_any(name)
            if r.get("error"):
                _err(f"  {r['error']}"); continue
            metrics_store[name] = r

            src = r.get("source") or "?"
            print(f"  {_cyan(f'[source: {src}]')}")
            for section, items in r.get("sections", []):
                print(f"  {_bold(section)}")
                for metric, val in items:
                    print(f"    {metric:<35} {val}")

            # monitor_any already evaluates alerts (DB threshold checks for
            # db, provider-side alerts for cloud). We still forward them to
            # the configured notifier so the side-effect from the old code
            # path is preserved.
            for alert in r.get("alerts", []) or []:
                sev = alert.get("severity", "INFO")
                colour = _red if sev == "CRITICAL" else _yellow if sev == "WARNING" else _cyan
                print(colour(f"  [{sev}] {alert.get('message', '')}"))
                try:
                    _service().send_notification(sev, alert.get("message", ""))
                except Exception:
                    pass

        if out_path:
            try:
                import json as _json
                Path(out_path).write_text(
                    _json.dumps(
                        {k: {"sections": v["sections"], "timestamp": v["timestamp"]}
                         for k, v in metrics_store.items()},
                        indent=2, default=str,
                    )
                )
            except Exception as e:
                _err(f"Could not write output: {e}")

    if once:
        _poll()
    else:
        _info(f"Monitoring {names} every {interval}s. Press Ctrl+C to stop.")
        try:
            while True:
                _poll()
                time.sleep(interval)
        except KeyboardInterrupt:
            _info("Stopped.")


# Daemon commands are owned by the monitoring module — see
# `monitoring/cli.py::_daemon` and `MODULE_CLI_COMMANDS` in
# `common/core/modules.py`. The argparse subcommand is registered by the
# monitoring manifest, so `dbtool daemon start/stop/status` is dispatched
# through `monitoring.cli.dispatch_cli`, not through this file.


def cmd_api(args):
    try:
        import uvicorn
    except ImportError:
        _err("uvicorn not installed. Run: pip install uvicorn fastapi")
        sys.exit(1)
    from common.security import api_keys

    host = str(args.host or "127.0.0.1").strip().lower()
    loopback = host in {"127.0.0.1", "localhost", "::1"}
    has_auth = bool(os.environ.get("DBTOOL_API_KEY", "").strip()) or api_keys.has_any_key()
    if not has_auth and not loopback:
        _err(
            "Refusing to start API on a non-loopback host without an API key. "
            "Run: dbtool apikey create --name admin"
        )
        sys.exit(1)
    if not has_auth:
        _warn("API is running keyless on loopback only. Create a key before LAN exposure.")
    _ok(f"Starting REST API on http://{args.host}:{args.port}")
    _info(f"Interactive docs : http://{args.host}:{args.port}/docs")
    _info(f"Health check     : http://{args.host}:{args.port}/api/health")
    uvicorn.run(
        "app.headless.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        app_dir=str(Path(__file__).resolve().parent.parent),
    )


# ── databases (engine registry) ─────────────────────────────────────────

def cmd_databases_types(args):
    rows = _service().list_db_types()
    if not rows:
        _info("No database types registered.")
        return
    headers = ["db_type", "display_name", "default_port"]
    _print_table([[r[h] for h in headers] for r in rows], headers, args.format)


def cmd_databases_ops(args):
    rows = _service().list_db_ops(args.type)
    if not rows:
        _info(f"No operations registered for '{args.type}'.")
        return
    headers = ["display_name", "operation"]
    _print_table([[r[h] for h in headers] for r in rows], headers, args.format)


# ── config ─────────────────────────────────────────────────────────────

def cmd_config_forward(args):
    """Forward curated-settings subcommands to the shared core dispatcher so
    the UI / CLI / API stay in sync on one schema."""
    from common.core.cli_handlers import dispatch_core_argv

    action = getattr(args, "config_action", "")
    argv = ["config", action]
    if action == "list":
        if args.group:
            argv += ["--group", args.group]
        argv += ["--format", getattr(args, "cfg_format", "table")]
    elif action == "get":
        argv += [args.id]
    elif action == "describe":
        if args.id:
            argv += [args.id]
    elif action == "set":
        argv += [args.id, *args.value]
    elif action == "restore":
        argv += ["--target", args.target]
        if args.yes:
            argv += ["--yes"]
    dispatch_core_argv(argv, _service(), prog="dbtool")


def cmd_config_show(args):
    r = _service().show_config(section=args.section or None)
    if r.get("error"):
        _err(r["error"])
        return
    sections = r.get("sections") or {}
    if not sections:
        _info("No matching config section(s).")
        return
    if args.format == "json":
        print(json.dumps(sections, indent=2, default=str))
        return
    for sect, kv in sections.items():
        print(_bold(f"[{sect}]"))
        for k, v in kv.items():
            print(f"  {k} = {v}")
        print()


# ── API keys ───────────────────────────────────────────────────────────

def cmd_apikey(args):
    from common.security import api_keys

    action = getattr(args, "apikey_action", "")
    fmt = getattr(args, "format", "table")
    if action == "create":
        r = api_keys.create_key(getattr(args, "name", "") or "")
        if fmt == "json":
            print(json.dumps(r, indent=2))
        else:
            _ok("API key created. Save the token now; the secret is shown only once.")
            _print_table([[r["key_id"], r.get("name", ""), r["token"]]],
                         ["key_id", "name", "token"], "table")
        return
    if action == "list":
        keys = api_keys.list_keys()
        if fmt == "json":
            print(json.dumps(keys, indent=2))
        else:
            rows = [
                [r.get("key_id", ""), r.get("name", ""), r.get("created_at", ""),
                 r.get("last_used_at", "") or "-", r.get("revoked_at", "") or "-"]
                for r in keys
            ]
            _print_table(rows, ["key_id", "name", "created_at", "last_used", "revoked"],
                         "table")
        return
    if action == "revoke":
        r = api_keys.revoke_key(args.key_id)
        if fmt == "json":
            print(json.dumps(r, indent=2))
        else:
            (_ok if r.get("ok") else _err)(r.get("error") or f"Revoked {args.key_id}")
        if not r.get("ok"):
            sys.exit(1)
        return
    if action == "regenerate":
        r = api_keys.regenerate_key(args.key_id)
        if fmt == "json":
            print(json.dumps(r, indent=2))
        else:
            if not r.get("ok"):
                _err(r.get("error") or "Regenerate failed.")
                sys.exit(1)
            _ok("API key regenerated. Save the token now; the secret is shown only once.")
            _print_table([[r["key_id"], r.get("name", ""), r["token"]]],
                         ["key_id", "name", "token"], "table")
        return
    _err("Unknown apikey action.")
    sys.exit(1)


# ── app (Phase 7) ──────────────────────────────────────────────────────

def cmd_app(args):
    """Forward to the shared dispatch_core_argv so CLI/API/UI stay in sync."""
    from common.core.cli_handlers import dispatch_core_argv

    if not dispatch_core_argv(["app", *getattr(args, "app_args", [])],
                              _service(), prog="dbtool"):
        _err("Unknown 'app' subcommand. See: dbtool app")
        sys.exit(1)


# ── notify ─────────────────────────────────────────────────────────────

def cmd_notify_send(args):
    r = _service().send_notification(args.severity, args.message)
    (_ok if r["ok"] else _err)(r["message"])


# ── os metrics ─────────────────────────────────────────────────────────

def cmd_os_metrics(args):
    r = _service().get_os_metrics(disk_path=args.disk or "/")
    if r["error"]:
        _err(r["error"])
        return
    metrics = r["metrics"] or {}
    headers = ["metric", "value"]
    rows = [[k, v] for k, v in metrics.items()]
    _print_table(rows, headers, args.format)


# ── cloud ──────────────────────────────────────────────────────────────

def cmd_cloud_connections_list(args):
    rows = _service().list_cloud_connections()
    if not rows:
        _info("No saved cloud connections.")
        return
    if rows and "error" in rows[0]:
        _err(rows[0]["error"])
        return
    keys_seen: list[str] = []
    for row in rows:
        for k in row.keys():
            if k not in keys_seen:
                keys_seen.append(k)
    # Promote common keys to the front for readability
    preferred = ["name", "provider", "region", "resource_id", "subscription_id",
                 "project_id", "instance_id"]
    headers = [k for k in preferred if k in keys_seen] + [
        k for k in keys_seen if k not in preferred
    ]
    data = [[r.get(h, "") for h in headers] for r in rows]
    _print_table(data, headers, args.format)


def cmd_cloud_connections_add(args):
    try:
        profile = json.loads(Path(args.json).read_text())
    except Exception as exc:
        _err(f"Could not parse JSON file: {exc}")
        return
    profile.setdefault("provider", args.provider)
    r = _service().add_cloud_connection(args.name, profile)
    (_ok if r["ok"] else _err)(r["message"])


def cmd_cloud_connections_remove(args):
    r = _service().remove_cloud_connection(args.name)
    (_ok if r["ok"] else _err)(r["message"])


def cmd_cloud_connections_test(args):
    _info(f"Testing cloud connection '{args.name}' ...")
    r = _service().test_cloud_connection(args.name)
    (_ok if r["ok"] else _err)(r["message"])


def cmd_cloud_login(args):
    _info(f"Starting interactive login for cloud connection '{args.name}' ...")
    r = _service().cloud_login(args.name)
    (_ok if r["ok"] else _err)(r["message"])


def cmd_cloud_metrics(args):
    r = _service().get_cloud_metrics(args.name)
    if r.get("error"):
        _err(r["error"])
        return
    text = r.get("text", "")
    if text:
        print(text)
    for a in (r.get("alerts") or []):
        sev = a["severity"]
        colour = _red if sev == "CRITICAL" else _yellow if sev == "WARNING" else _cyan
        print(colour(f"  [{sev}] {a['message']}"))


def cmd_cloud_monitor(args):
    """Live polling of a single cloud connection (blocking, Ctrl+C to stop)."""
    interval = args.interval
    once = args.once
    name = args.name

    def _poll():
        ts = datetime.now().strftime("%H:%M:%S")
        _info(f"[{ts}] Fetching cloud metrics for '{name}' ...")
        r = _service().get_cloud_metrics(name)
        if r.get("error"):
            _err(f"  {r['error']}")
            return
        if r.get("text"):
            print(r["text"])
        for a in (r.get("alerts") or []):
            sev = a["severity"]
            colour = _red if sev == "CRITICAL" else _yellow if sev == "WARNING" else _cyan
            print(colour(f"  [{sev}] {a['message']}"))

    if once:
        _poll()
        return
    _info(f"Monitoring cloud '{name}' every {interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            _poll()
            time.sleep(interval)
    except KeyboardInterrupt:
        _info("Stopped.")


# =============================================================================
# Argument parser
# =============================================================================

def build_parser():
    parser = argparse.ArgumentParser(
        prog="dbtool",
        description="DbManagementTool CLI — headless database management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--format", choices=["table", "json", "csv"], default="table",
                        help="Output format (default: table)")
    parser.add_argument("--no-color", action="store_true", help="Disable colour output")
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # ── connections ──────────────────────────────────────────────────────────
    conn_p = sub.add_parser("connections", help="Manage saved connection profiles")
    conn_sub = conn_p.add_subparsers(dest="action", metavar="<action>")
    conn_sub.required = True

    conn_sub.add_parser("list", help="List all saved connections")

    add_p = conn_sub.add_parser("add", help="Add a new connection")
    add_p.add_argument("--name",    required=True)
    add_p.add_argument("--type",    required=True, metavar="TYPE",
                       help="MySQL | MariaDB | PostgreSQL | Oracle | SQLite")
    add_p.add_argument("--host",    required=True)
    add_p.add_argument("--user",    required=True)
    add_p.add_argument("--port",    default="")
    add_p.add_argument("--db",      default="", metavar="DATABASE")
    add_p.add_argument("--service", default="", metavar="SERVICE",
                       help="Oracle service name")
    add_p.add_argument("--password", default="",
                       help="Omit to be prompted (recommended)")
    # Remote connections over an SSH tunnel: host/port above are the database
    # endpoint *as seen from the SSH host* (often localhost).
    add_p.add_argument("--ssh-host", dest="ssh_host", default="", metavar="HOST",
                       help="Bastion/SSH host to tunnel through (enables a remote connection)")
    add_p.add_argument("--ssh-user", dest="ssh_user", default="", metavar="USER",
                       help="SSH username for the tunnel")
    add_p.add_argument("--ssh-port", dest="ssh_port", type=int, default=22,
                       metavar="PORT", help="SSH port (default 22)")
    add_p.add_argument("--ssh-password", dest="ssh_password", default="",
                       help="SSH password (needs sshpass); omit to be prompted")
    add_p.add_argument("--ssh-key-file", dest="ssh_key_file", default="",
                       metavar="PATH", help="SSH private key file for the tunnel")

    rm_p = conn_sub.add_parser("remove", help="Remove a connection")
    rm_p.add_argument("name")

    test_p = conn_sub.add_parser("test", help="Test a connection")
    test_p.add_argument("name")

    # ── query ────────────────────────────────────────────────────────────────
    q_p = sub.add_parser("query", help="Execute SQL")
    q_p.add_argument("--conn",   required=True, metavar="NAME")
    q_p.add_argument("--sql",    default="", metavar="SQL")
    q_p.add_argument("--file",   default="", metavar="FILE.sql")
    q_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    q_p.add_argument(
        "--autocommit", choices=["on", "off"], default="",
        help="Override autocommit for this connection before running the SQL "
             "(default: keep config database.connection.default_autocommit)",
    )

    fmt_p = sub.add_parser("format-sql", help="Pretty-print SQL (keyword case + reindent)")
    fmt_p.add_argument("--sql",    default="", metavar="SQL")
    fmt_p.add_argument("--file",   default="", metavar="FILE.sql")
    fmt_p.add_argument("--output", default="", metavar="FILE.sql",
                       help="Write formatted SQL to a file instead of stdout")

    ac_p = sub.add_parser(
        "autocommit", help="Get or set autocommit on a live connection",
    )
    ac_sub = ac_p.add_subparsers(dest="autocommit_action", metavar="<action>")
    ac_sub.required = True
    ac_get = ac_sub.add_parser("get", help="Show the live autocommit state")
    ac_get.add_argument("--conn", required=True, metavar="NAME")
    ac_set = ac_sub.add_parser("set", help="Set autocommit on a connection")
    ac_set.add_argument("--conn", required=True, metavar="NAME")
    ac_set.add_argument("enabled", help="true/false (on/off)")

    # ── objects ──────────────────────────────────────────────────────────────
    obj_p = sub.add_parser("objects", help="List database objects")
    obj_p.add_argument("--conn",  required=True, metavar="NAME")
    obj_p.add_argument("--type",  default="tables", metavar="TYPE",
                       help="tables | views | procs | functions | indexes | triggers | "
                            "sequences | constraints | events | databases | users | "
                            "schemas | tablespaces | engines | charsets | processlist | "
                            "roles | extensions | synonyms | packages | types | "
                            "materializedviews | databaselinks | profiles | sessions | "
                            "activity  (engine-dependent)")
    obj_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    # ── modules (migrator / ai / monitor) ─────────────────────────────────────
    # Module-owned commands are contributed by installed module manifests.
    # For a known-but-missing module we add lightweight stubs so the command
    # still shows in help and reports a clear "module not installed" message.
    _register_module_commands(sub)

    # ── api ──────────────────────────────────────────────────────────────────
    api_p = sub.add_parser("api", help="Start the REST API server")
    api_p.add_argument("--host",   default=get_api_host())
    api_p.add_argument("--port",   type=int, default=get_api_port())
    api_p.add_argument("--reload", action="store_true",
                       help="Auto-reload on code changes (dev mode)")

    # ── API keys ─────────────────────────────────────────────────────────────
    key_p = sub.add_parser("apikey", help="Manage local API access keys")
    key_sub = key_p.add_subparsers(dest="apikey_action", metavar="<action>")
    key_sub.required = True
    key_create = key_sub.add_parser("create", help="Create an API key")
    key_create.add_argument("--name", default="", help="Friendly key name")
    key_create.add_argument("--format", choices=["table", "json"], default="table")
    key_list = key_sub.add_parser("list", help="List API keys (secrets are never shown)")
    key_list.add_argument("--format", choices=["table", "json"], default="table")
    key_revoke = key_sub.add_parser("revoke", help="Revoke an API key")
    key_revoke.add_argument("key_id")
    key_revoke.add_argument("--format", choices=["table", "json"], default="table")
    key_regen = key_sub.add_parser("regenerate", help="Regenerate an API key secret")
    key_regen.add_argument("key_id")
    key_regen.add_argument("--format", choices=["table", "json"], default="table")

    # ── databases (registry) ─────────────────────────────────────────────────
    db_p = sub.add_parser("databases", help="Inspect supported DB engines")
    db_sub = db_p.add_subparsers(dest="databases_action", metavar="<action>")
    db_sub.required = True

    types_p = db_sub.add_parser("types", help="List supported database engines")
    types_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    ops_p = db_sub.add_parser("ops", help="List operations available for a DB engine")
    ops_p.add_argument("--type", required=True,
                       help="MySQL | MariaDB | PostgreSQL | Oracle | SQLite")
    ops_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    # ── config ───────────────────────────────────────────────────────────────
    cfg_p = sub.add_parser("config", help="Inspect and edit runtime configuration")
    cfg_sub = cfg_p.add_subparsers(dest="config_action", metavar="<action>")
    cfg_sub.required = True
    cfg_show_p = cfg_sub.add_parser("show", help="Show raw config values")
    cfg_show_p.add_argument("--section", default="",
                            help="Limit to one section (default: all)")
    cfg_show_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    # Curated, self-describing settings. Handlers reconstruct an argv and
    # forward to dispatch_core_argv so the UI / CLI / API share one schema.
    cfg_list_p = cfg_sub.add_parser("list", help="List curated settings with current values")
    cfg_list_p.add_argument("--group", default="", help="Filter by group name")
    cfg_list_p.add_argument("--cfg-format", dest="cfg_format",
                            choices=["table", "json"], default="table")
    cfg_get_p = cfg_sub.add_parser("get", help="Show one setting as JSON: config get <id>")
    cfg_get_p.add_argument("id", help="Setting id, e.g. config.project.debug_mode")
    cfg_desc_p = cfg_sub.add_parser("describe", help="Explain setting(s): config describe [<id>]")
    cfg_desc_p.add_argument("id", nargs="?", default="", help="Optional setting id")
    cfg_set_p = cfg_sub.add_parser("set", help="Set & save a setting: config set <id> <value>")
    cfg_set_p.add_argument("id", help="Setting id, e.g. properties.notifications.enabled")
    cfg_set_p.add_argument("value", nargs="+", help="New value (may contain spaces)")
    cfg_restore_p = cfg_sub.add_parser("restore", help="Restore defaults from *.ini.example")
    cfg_restore_p.add_argument("--target", choices=["all", "config", "properties"], default="all")
    cfg_restore_p.add_argument("--yes", action="store_true", help="Skip confirmation")

    # ── modules introspection ─────────────────────────────────────────────────
    mods_p = sub.add_parser("modules", help="List installed / available modules")
    mods_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    # ── ui ─────────────────────────────────────────────────────────────────────
    ui_p = sub.add_parser("ui", help="Launch the desktop UI (full tool or one module)")
    ui_p.add_argument("--module", choices=["migrator", "ai", "monitor"], default="",
                      help="Launch a single module's standalone UI (default: full combined UI)")

    # ── tui ────────────────────────────────────────────────────────────────────
    tui_p = sub.add_parser("tui", help="Launch the Textual terminal UI (full tool or one module)")
    tui_p.add_argument("--module", choices=["migrator", "ai", "monitor"], default="",
                       help="Launch a single module's standalone TUI (default: full combined TUI)")
    tui_p.add_argument("--web", action="store_true",
                       help="Serve Textual UI in browser via textual serve")
    tui_p.add_argument("--host", default=get_tui_web_host(), help="Web bind host (with --web)")
    tui_p.add_argument("--port", type=int, default=get_tui_web_port(), help="Web bind port (with --web)")

    # ── webui ────────────────────────────────────────────────────────────────
    webui_p = sub.add_parser("webui", help="Launch the HTML/JS web UI (REST API + SPA)")
    webui_p.add_argument("--module", choices=["migrator", "ai", "monitor"], default="",
                         help="Serve a single module's web UI (default: full combined UI)")
    webui_p.add_argument("--host", default=get_webui_host(), help="Bind host")
    webui_p.add_argument("--port", type=int, default=get_webui_port(), help="Bind port")

    # ── app (Phase 7: cross-cutting app-level commands) ───────────────────────
    # Subcommands are forwarded verbatim to dispatch_core_argv so we don't
    # duplicate argparse trees. ``--help`` for sub-actions is handled there.
    app_p = sub.add_parser(
        "app",
        help="App-level: clear-caches | dashboard-layout {show|reset|save} | shortcuts",
    )
    app_p.add_argument(
        "app_args", nargs=argparse.REMAINDER,
        help="clear-caches | dashboard-layout {show|reset|save --rows JSON} | "
             "shortcuts [--section S] [--format table|json|csv]",
    )

    return parser


def _register_module_commands(sub) -> None:
    """Add each module's CLI commands (installed → real parser; missing → stub)."""
    from common.core import modules as _modules

    installed = _modules.discover()
    for mkey, (_pkg, title) in _modules.KNOWN_MODULES.items():
        manifest = installed.get(mkey)
        if manifest is not None and manifest.register_cli:
            manifest.register_cli(sub)
        else:
            for cmd in _modules.MODULE_CLI_COMMANDS.get(mkey, []):
                stub = sub.add_parser(cmd, help=f"[{title}] — module not installed",
                                      add_help=False)
                # Swallow any trailing args so we can show a clean "not installed"
                # message instead of an argparse "unrecognized arguments" error.
                stub.add_argument("_ignored", nargs=argparse.REMAINDER,
                                  help=argparse.SUPPRESS)
                stub.set_defaults(_missing_module=mkey)


# =============================================================================
# Dispatch
# =============================================================================

_DISPATCH = {
    # Core commands (always available — Objects + SQL editor + admin).
    ("connections", "list"):    cmd_connections_list,
    ("connections", "add"):     cmd_connections_add,
    ("connections", "remove"):  cmd_connections_remove,
    ("connections", "test"):    cmd_connections_test,
    ("query",       None):      cmd_query,
    ("format-sql",  None):      cmd_format_sql,
    ("autocommit",  None):      cmd_autocommit,
    ("objects",     None):      cmd_objects,
    ("api",         None):      cmd_api,
    ("apikey",      "create"):  cmd_apikey,
    ("apikey",      "list"):    cmd_apikey,
    ("apikey",      "revoke"):  cmd_apikey,
    ("apikey",      "regenerate"): cmd_apikey,
    ("databases",   "types"):   cmd_databases_types,
    ("databases",   "ops"):     cmd_databases_ops,
    ("config",      "show"):    cmd_config_show,
    ("config",      "list"):     cmd_config_forward,
    ("config",      "get"):      cmd_config_forward,
    ("config",      "describe"): cmd_config_forward,
    ("config",      "set"):      cmd_config_forward,
    ("config",      "restore"):  cmd_config_forward,
    ("app",         None):      cmd_app,
}


def cmd_modules(args):
    """List which modules are installed / available in this build."""
    from common.core import modules as _modules

    snap = _modules.status()
    rows = []
    for cmd, info in snap.items():
        rows.append([
            cmd,
            info["title"],
            "yes" if info["installed"] else "no",
            "yes" if info["ready"] else "no",
            "; ".join(info["missing_requirements"]) or "-",
        ])
    _print_table(rows, ["module", "title", "installed", "ready", "missing_requirements"],
                 args.format)


def main():
    parser = build_parser()
    # Route a free-text ``ai`` one-shot question through its dedicated ``ask``
    # subcommand before argparse runs (see ai_query.cli.inject_oneshot_ask).
    argv = sys.argv[1:]
    try:
        from ai_query.cli import inject_oneshot_ask

        argv = inject_oneshot_ask(argv)
    except Exception:
        pass
    args = parser.parse_args(argv)

    global _COLOUR
    if args.no_color:
        _COLOUR = False

    # Ensure the v1 storage layout exists and any pending migration has
    # run before any command touches connections, keys, or runtime state.
    try:
        from common import paths as _paths

        _paths.bootstrap()
    except Exception:
        # bootstrap() already swallows failures and ensures the layout
        # exists. The bare except here is defensive against the rare case
        # where importing the module itself fails (e.g. partial install).
        pass

    from common.core import modules as _modules

    command = args.command

    # 1) Module-owned command? Route to the module (or report it missing).
    module_key = _modules.module_for_command(command)
    if module_key is not None:
        if getattr(args, "_missing_module", None) or not _modules.is_installed(module_key):
            _err(str(_modules.ModuleNotInstalled(command)))
            sys.exit(1)
        manifest = _modules.get(module_key)
        try:
            rc = manifest.dispatch_cli(args)
        except KeyboardInterrupt:
            print("\nAborted.")
            sys.exit(0)
        except Exception as exc:
            _err(str(exc))
            if os.environ.get("DBTOOL_DEBUG"):
                import traceback
                traceback.print_exc()
            sys.exit(1)
        sys.exit(rc or 0)

    # 2) Built-in introspection command.
    if command == "modules":
        cmd_modules(args)
        return

    # 2b) UI launcher (full combined UI, or core + one module tab).
    if command == "ui":
        from common.core.ui_registry import launch_tk_ui

        if args.module:
            if not _modules.is_installed(args.module):
                _err(str(_modules.ModuleNotInstalled(args.module)))
                sys.exit(1)
            launch_tk_ui(feature_module=args.module)
        else:
            launch_tk_ui()
        return

    # 2c) Textual TUI launcher.
    if command == "tui":
        from common.core.ui_registry import launch_textual_ui

        mod = getattr(args, "module", "") or None
        if mod and not _modules.is_installed(mod):
            _err(str(_modules.ModuleNotInstalled(mod)))
            sys.exit(1)
        launch_textual_ui(
            feature_module=mod,
            web=getattr(args, "web", False),
            host=getattr(args, "host", "127.0.0.1"),
            port=getattr(args, "port", 8080),
        )
        return

    # 2d) Web UI launcher (HTML/JS SPA on FastAPI).
    if command == "webui":
        from common.core.ui_registry import launch_web_ui

        mod = getattr(args, "module", "") or None
        if mod and not _modules.is_installed(mod):
            _err(str(_modules.ModuleNotInstalled(mod)))
            sys.exit(1)
        launch_web_ui(
            feature_module=mod,
            host=getattr(args, "host", "127.0.0.1"),
            port=getattr(args, "port", 8090),
        )
        return

    # 3) Core command dispatch.
    action = getattr(args, "action", None)
    databases = getattr(args, "databases_action", None)
    config_act = getattr(args, "config_action", None)
    apikey_act = getattr(args, "apikey_action", None)
    secondary = action or databases or config_act or apikey_act

    handler = _DISPATCH.get((command, secondary)) or _DISPATCH.get((command, None))
    if handler is None:
        parser.print_help()
        sys.exit(1)

    try:
        handler(args)
    except KeyboardInterrupt:
        print("\nAborted.")
        sys.exit(0)
    except Exception as exc:
        _err(str(exc))
        if os.environ.get("DBTOOL_DEBUG"):
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
