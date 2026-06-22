"""
CLI surface for the Monitoring module.

Owns the ``monitor``, ``daemon``, ``thresholds``, ``os``, ``cloud`` and
``notify`` commands.  Wired into the master CLI through the manifest; runnable
standalone via ``python -m monitoring``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

# Convenience: allow running this file directly (``python monitoring/cli.py``)
# in addition to the canonical ``python -m monitoring``. When executed as a
# script, the project root is not on sys.path, so the absolute ``common`` /
# ``monitoring`` imports below would fail with ModuleNotFoundError. We insert
# the project root *before* importing them. This branch is skipped entirely for
# normal package imports (``python -m monitoring`` / ``import monitoring.cli``),
# where ``__package__`` is ``"monitoring"``, so it changes no existing flow.
if not __package__:
    import os

    _PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _PROJECT_ROOT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT)

from common.core import cliutil


def _service():
    from monitoring.service import make_service

    return make_service()


def _alert(sev: str, msg: str) -> None:
    print(f"  [{sev}] {msg}")


# ---------------------------------------------------------------------------
# Parser registration
# ---------------------------------------------------------------------------
def register_cli(subparsers) -> None:
    from common import paths as _paths
    from monitoring import monitor_config as _mcfg

    _DEFAULT_PID = str(_paths.daemon_pid_path())
    _DEFAULT_LOG = str(_paths.daemon_log_path())
    _DEFAULT_OUT = str(_paths.metrics_snapshot_path())
    _DEF_INTERVAL = _mcfg.get_int("monitoring", "default_poll_interval", default=30)
    _DEF_DISK = _mcfg.get("monitoring", "default_disk_path", default="/") or "/"
    _DEF_ALERTS_LIMIT = _mcfg.get_int(
        "monitoring.limits", "alerts_default_limit", default=50)

    # monitor
    mon_p = subparsers.add_parser("monitor", help="Collect and display metrics (blocking)")
    mon_p.add_argument("--conn", default="", metavar="N1,N2,...")
    mon_p.add_argument("--interval", type=int, default=_DEF_INTERVAL, metavar="SECS")
    mon_p.add_argument("--once", action="store_true", help="Run a single poll then exit")
    mon_p.add_argument("--output", default="", metavar="metrics.json")
    mon_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    # monitor-connections (unified view across all three saved-connection stores)
    mc_p = subparsers.add_parser(
        "monitor-connections",
        help="List monitor-eligible saved connections (DB + monitor + cloud)",
    )
    mc_sub = mc_p.add_subparsers(dest="mc_action", metavar="<action>")
    mc_sub.required = True
    mc_list_p = mc_sub.add_parser(
        "list",
        help="List saved connections from Connections tab, Monitor tab and Cloud",
    )
    mc_list_p.add_argument(
        "--source",
        choices=["all", "db", "monitor-db", "monitor", "cloud"],
        default="all",
        help="Filter by source store (default: all)",
    )
    mc_list_p.add_argument(
        "--format", choices=["table", "json", "csv"], default="table"
    )
    mc_names_p = mc_sub.add_parser(
        "names",
        help="Emit '<source>\\t<name>' lines (bash-friendly picker source)",
    )
    mc_names_p.add_argument(
        "--source",
        choices=["all", "db", "monitor-db", "monitor", "cloud"],
        default="all",
    )
    mc_test_p = mc_sub.add_parser(
        "test",
        help="Test a saved connection (auto-dispatches by source)",
    )
    mc_test_p.add_argument(
        "--source",
        choices=["db", "monitor-db", "monitor", "cloud"],
        required=True,
        help="Which store the name lives in",
    )
    mc_test_p.add_argument("name")

    # Monitor-tab SSH target CRUD (Phase 5 parity with UI's Monitor tab)
    mc_add_p = mc_sub.add_parser(
        "add", help="Save a new Monitor-tab SSH/host target",
    )
    mc_add_p.add_argument("--name", required=True)
    mc_add_p.add_argument("--host", required=True)
    mc_add_p.add_argument("--username", required=True)
    mc_add_p.add_argument(
        "--password", default="",
        help="Optional password (stored encrypted; omit to use SSH keys)",
    )
    mc_add_p.add_argument(
        "--target-type", default="vm", dest="target_type",
        choices=["vm", "db_server", "service"],
    )

    mc_upd_p = mc_sub.add_parser(
        "update", help="Update an existing Monitor-tab SSH/host target",
    )
    mc_upd_p.add_argument("--name", required=True,
                          help="Current connection name (also new name)")
    mc_upd_p.add_argument("--host", required=True)
    mc_upd_p.add_argument("--username", required=True)
    mc_upd_p.add_argument("--password", default="")
    mc_upd_p.add_argument(
        "--target-type", default="", dest="target_type",
        choices=["", "vm", "db_server", "service"],
        help="Omit to preserve the previous value",
    )

    mc_rm_p = mc_sub.add_parser(
        "remove", help="Delete a saved Monitor-tab SSH/host target",
    )
    mc_rm_p.add_argument("name")

    # monitor-db — Monitor-tab-only DB connections (isolated from Connections tab)
    mdb_p = subparsers.add_parser(
        "monitor-db",
        help="Manage Monitor-tab-only DB connections (isolated; not visible to "
             "the SQL Editor / Data Migration / AI Query tabs)",
    )
    mdb_sub = mdb_p.add_subparsers(dest="mdb_action", metavar="<action>")
    mdb_sub.required = True

    mdb_list_p = mdb_sub.add_parser("list", help="List Monitor-only DB connections")
    mdb_list_p.add_argument(
        "--format", choices=["table", "json", "csv"], default="table"
    )

    mdb_add_p = mdb_sub.add_parser("add", help="Save a new Monitor-only DB connection")
    mdb_add_p.add_argument("--name", required=True)
    mdb_add_p.add_argument("--db-type", required=True, dest="db_type")
    mdb_add_p.add_argument("--host", required=True)
    mdb_add_p.add_argument("--port", default="")
    mdb_add_p.add_argument(
        "--database", default="",
        help="Database name (or Oracle service name — see --service)",
    )
    mdb_add_p.add_argument("--service", default="", help="Oracle service name")
    mdb_add_p.add_argument("--username", default="")
    mdb_add_p.add_argument(
        "--password", default="",
        help="Stored encrypted; omit to save without a password",
    )
    mdb_add_p.add_argument(
        "--ssh-host", default="", dest="ssh_host",
        help="SSH jump host to reach a remote database (enables SSH tunnel)",
    )
    mdb_add_p.add_argument("--ssh-user", default="", dest="ssh_user",
                           help="SSH username for the tunnel")
    mdb_add_p.add_argument("--ssh-port", default="", dest="ssh_port",
                           help="SSH port (default 22)")
    mdb_add_p.add_argument(
        "--ssh-password", default="", dest="ssh_password",
        help="SSH password (stored encrypted; or use --ssh-key-file)",
    )
    mdb_add_p.add_argument("--ssh-key-file", default="", dest="ssh_key_file",
                           help="Path to an SSH private key for the tunnel")

    mdb_rm_p = mdb_sub.add_parser("remove", help="Delete a Monitor-only DB connection")
    mdb_rm_p.add_argument("name")

    mdb_test_p = mdb_sub.add_parser("test", help="Test a Monitor-only DB connection")
    mdb_test_p.add_argument("name")

    # daemon
    d_p = subparsers.add_parser("daemon", help="Manage the background monitoring daemon")
    d_sub = d_p.add_subparsers(dest="daemon_action", metavar="start|stop|status")
    d_sub.required = True
    st_p = d_sub.add_parser("start", help="Start the daemon")
    st_p.add_argument("--connections", default="", metavar="N1,N2,...",
                      help="Comma-separated names to monitor (default: "
                           "interactive picker, or all when running headless)")
    st_p.add_argument("--interval", type=int, default=_DEF_INTERVAL, metavar="SECS")
    st_p.add_argument("--pid-file", default=_DEFAULT_PID, dest="pid_file")
    st_p.add_argument("--log-file", default=_DEFAULT_LOG, dest="log_file")
    st_p.add_argument("--output", default=_DEFAULT_OUT, metavar="metrics.json")
    st_p.add_argument("--foreground", action="store_true",
                      help="Run in foreground (for containers / systemd)")
    st_p.add_argument("--non-interactive", action="store_true",
                      dest="non_interactive",
                      help="Skip the picker even on a TTY (for scripts / CI). "
                           "Requires --connections, otherwise polls every "
                           "saved DB connection.")
    st_p.add_argument("--no-preflight", action="store_true",
                      dest="no_preflight",
                      help="Skip the per-connection reachability check before "
                           "starting the daemon (NOT recommended)")
    sp_p = d_sub.add_parser("stop", help="Stop the daemon")
    sp_p.add_argument("--pid-file", default=_DEFAULT_PID, dest="pid_file")
    ss_p = d_sub.add_parser("status", help="Show daemon status")
    ss_p.add_argument("--pid-file", default=_DEFAULT_PID, dest="pid_file")

    # thresholds
    th_p = subparsers.add_parser("thresholds", help="Inspect and exercise alert rules")
    th_sub = th_p.add_subparsers(dest="thresholds_action", metavar="<action>")
    th_sub.required = True
    th_list_p = th_sub.add_parser("list", help="List threshold rules")
    th_list_p.add_argument("--source", default="", help="db | os | aws | azure | gcp")
    th_list_p.add_argument("--api", default="", help="cloudwatch | pi | azuremonitor | cloudmonitoring")
    th_list_p.add_argument("--path", default="", help="Dot-joined rule path, e.g. cloudwatch.RDS")
    th_list_p.add_argument("--all", action="store_true", help="Include disabled rules")
    th_list_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    th_show_p = th_sub.add_parser("show", help="Show a specific rule")
    th_show_p.add_argument("--source", required=True)
    th_show_p.add_argument("--metric", required=True)
    th_show_p.add_argument("--path", default="", help="Dot-joined rule path, e.g. cloudwatch.RDS")
    th_show_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    th_check_p = th_sub.add_parser("check", help="Evaluate a value against a rule")
    th_check_p.add_argument("--source", required=True)
    th_check_p.add_argument("--metric", required=True)
    th_check_p.add_argument("--value", required=True, type=float)
    th_check_p.add_argument("--instance", default="manual")
    th_check_p.add_argument("--path", default="", help="Dot-joined rule path, e.g. cloudwatch.RDS")
    th_set_p = th_sub.add_parser("set", help="Edit a threshold rule's values")
    th_set_p.add_argument("--source", required=True)
    th_set_p.add_argument("--metric", required=True)
    th_set_p.add_argument("--path", default="", help="Dot-joined rule path, e.g. cloudwatch.RDS")
    th_set_p.add_argument("--critical", default=None, help="Critical threshold (blank to clear)")
    th_set_p.add_argument("--warning", default=None, help="Warning threshold (blank to clear)")
    th_set_p.add_argument("--info", default=None, help="Info threshold (blank to clear)")
    th_set_p.add_argument("--operator", default=None,
                          choices=[">", ">=", "<", "<=", "==", "!="])
    th_set_p.add_argument("--window", default=None, type=int,
                          help="Consecutive breaches before alerting")
    th_set_p.add_argument("--description", default=None)
    for _act, _help in (("enable", "Enable a threshold rule"),
                        ("disable", "Disable a threshold rule")):
        _p = th_sub.add_parser(_act, help=_help)
        _p.add_argument("--source", required=True)
        _p.add_argument("--metric", required=True)
        _p.add_argument("--path", default="", help="Dot-joined rule path")

    # os
    os_p = subparsers.add_parser("os", help="Host OS metrics (cpu, memory, disk, network)")
    os_sub = os_p.add_subparsers(dest="os_action", metavar="<action>")
    os_sub.required = True
    os_m_p = os_sub.add_parser("metrics", help="Show OS metrics for this machine")
    os_m_p.add_argument("--disk", default=_DEF_DISK, help="Disk usage mount point")
    os_m_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    os_r_p = os_sub.add_parser(
        "remote", help="OS metrics from a remote host over SSH (Monitor target)",
    )
    os_r_p.add_argument("--name", required=True,
                        help="Saved Monitor-tab connection name")
    os_r_p.add_argument("--disk", default=_DEF_DISK,
                        help="Disk usage mount point on the remote host")
    os_r_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    # alerts
    al_p = subparsers.add_parser(
        "alerts", help="Inspect and manage the persistent alerts log",
    )
    al_sub = al_p.add_subparsers(dest="alerts_action", metavar="<action>")
    al_sub.required = True
    al_list_p = al_sub.add_parser("list", help="List recorded alerts (newest first)")
    al_list_p.add_argument("--limit", type=int, default=_DEF_ALERTS_LIMIT)
    al_list_p.add_argument(
        "--severity", default="", choices=["", "INFO", "WARNING", "CRITICAL"],
    )
    al_list_p.add_argument("--source", default="")
    al_list_p.add_argument("--instance", default="")
    al_list_p.add_argument(
        "--format", choices=["table", "json", "csv"], default="table",
    )
    al_log_p = al_sub.add_parser("log", help="Append one alert record to the log")
    al_log_p.add_argument(
        "--severity", required=True, choices=["INFO", "WARNING", "CRITICAL"],
    )
    al_log_p.add_argument("--message", required=True)
    al_log_p.add_argument("--source", default="")
    al_log_p.add_argument("--instance", default="")
    al_clear_p = al_sub.add_parser(
        "clear", help="Remove matching entries (no filters = clear all)",
    )
    al_clear_p.add_argument(
        "--severity", default="", choices=["", "INFO", "WARNING", "CRITICAL"],
    )
    al_clear_p.add_argument("--source", default="")
    al_clear_p.add_argument("--instance", default="")

    # notify
    note_p = subparsers.add_parser("notify", help="Send a notification through the configured channel")
    note_sub = note_p.add_subparsers(dest="notify_action", metavar="<action>")
    note_sub.required = True
    note_send_p = note_sub.add_parser("send", help="Send a notification")
    note_send_p.add_argument("--severity", required=True, choices=["INFO", "WARNING", "CRITICAL"])
    note_send_p.add_argument("--message", required=True)
    note_cfg_p = note_sub.add_parser(
        "config", help="Show or edit notification settings (monitor_config.ini)",
    )
    note_cfg_sub = note_cfg_p.add_subparsers(dest="notify_cfg_action", metavar="<action>")
    note_cfg_sub.required = True
    note_cfg_show_p = note_cfg_sub.add_parser("show", help="Show notification config + secret status")
    note_cfg_show_p.add_argument("--format", choices=["table", "json"], default="table")
    note_cfg_set_p = note_cfg_sub.add_parser("set", help="Set a notification key or secret")
    note_cfg_set_p.add_argument("key", help="Setting key (e.g. enabled, smtp_host, teams_webhook_url)")
    note_cfg_set_p.add_argument("value", nargs="?", default="",
                               help="New value (empty clears secrets)")

    # module-owned configuration (monitoring/monitor_config.ini)
    mcfg_p = subparsers.add_parser(
        "monitor-config",
        help="View or edit monitoring/monitor_config.ini (refresh, keepalive, SSH, lookback)",
    )
    mcfg_sub = mcfg_p.add_subparsers(dest="config_action", metavar="<action>")
    mcfg_sub.required = True
    mcfg_show_p = mcfg_sub.add_parser("show", help="Show all monitor_config.ini values")
    mcfg_show_p.add_argument("--format", choices=["table", "json"], default="table")
    mcfg_show_p.add_argument("--section", default="", help="Filter to one section")
    mcfg_set_p = mcfg_sub.add_parser("set", help="Set one monitor_config.ini value")
    mcfg_set_p.add_argument("section", help="INI section (e.g. monitoring, cloud.lookback)")
    mcfg_set_p.add_argument("key", help="Key name")
    mcfg_set_p.add_argument("value", help="New value")
    mcfg_sub.add_parser(
        "restore", help="Restore monitor_config.ini from monitor_config.ini.example",
    )

    # cloud
    cl_p = subparsers.add_parser("cloud", help="Manage cloud (AWS/Azure/GCP) DB monitoring")
    cl_sub = cl_p.add_subparsers(dest="cloud_action", metavar="<action>")
    cl_sub.required = True
    cl_c_p = cl_sub.add_parser("connections", help="Manage cloud connection profiles")
    cl_c_sub = cl_c_p.add_subparsers(dest="cloud_conn_action", metavar="<action>")
    cl_c_sub.required = True
    cl_c_list_p = cl_c_sub.add_parser("list", help="List saved cloud connections")
    cl_c_list_p.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default=argparse.SUPPRESS,
    )
    cl_c_add_p = cl_c_sub.add_parser(
        "add",
        help="Add a cloud connection (interactive wizard, or --json/--field for scripts)",
    )
    cl_c_add_p.add_argument(
        "--name", default="",
        help="Stored connection name (defaults to the Display Name field)",
    )
    cl_c_add_p.add_argument(
        "--provider", default="", choices=["aws", "azure", "gcp", "other"],
        help="Cloud provider (prompted if omitted in interactive mode)",
    )
    cl_c_add_p.add_argument(
        "--json", default="",
        help="Path to a profile JSON file used to seed the connection",
    )
    cl_c_add_p.add_argument(
        "--field", action="append", default=[], metavar="key=value",
        help="Set/override a profile field (repeatable), "
             "e.g. --field region=us-east-1",
    )
    cl_c_add_p.add_argument(
        "--target-kind", dest="target_kind", default="",
        choices=["", "cloud_db", "vm", "cloud_service"],
        help="Monitoring target kind (default: cloud_db)",
    )
    cl_c_add_p.add_argument(
        "--auth-mode", dest="auth_mode", default="",
        choices=["", "keys", "pwd", "sso"],
        help="Authentication mode (prompted if omitted in interactive mode)",
    )
    cl_c_add_p.add_argument(
        "-i", "--interactive", action="store_true",
        help="Force the interactive wizard even when --json/--field are given",
    )
    cl_c_add_p.add_argument(
        "--no-test", dest="no_test", action="store_true",
        help="Skip the post-save connection test",
    )
    cl_c_rm_p = cl_c_sub.add_parser("remove", help="Remove a cloud connection")
    cl_c_rm_p.add_argument("name")
    cl_c_test_p = cl_c_sub.add_parser("test", help="Probe a cloud connection (check_health)")
    cl_c_test_p.add_argument("name")
    cl_login_p = cl_sub.add_parser("login", help="Interactive login (aws/az/gcloud) for a cloud connection")
    cl_login_p.add_argument("--name", required=True)
    cl_m_p = cl_sub.add_parser("metrics", help="Fetch metrics once from a cloud connection")
    cl_m_p.add_argument("--name", required=True)
    cl_mon_p = cl_sub.add_parser("monitor", help="Live-poll cloud metrics until stopped")
    cl_mon_p.add_argument("--name", required=True)
    cl_mon_p.add_argument("--interval", type=int, default=_DEF_INTERVAL, metavar="SECS")
    cl_mon_p.add_argument("--once", action="store_true", help="Run a single poll and exit")
    cl_rds_p = cl_sub.add_parser(
        "rds-endpoint",
        help="Resolve the SQL endpoint (host/port/engine) for an AWS RDS profile",
    )
    cl_rds_p.add_argument("--name", required=True)
    cl_rds_p.add_argument("--format", choices=["table", "json"], default="table")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
def dispatch_cli(args) -> int:
    cmd = args.command
    if cmd == "monitor":
        return _monitor(args)
    if cmd == "monitor-connections":
        return _monitor_connections(args)
    if cmd == "monitor-db":
        return _monitor_db_conns(args)
    if cmd == "daemon":
        return _daemon(args)
    if cmd == "thresholds":
        return _thresholds(args)
    if cmd == "os":
        return _os_metrics(args)
    if cmd == "notify":
        return _notify(args)
    if cmd == "monitor-config":
        return _monitor_config_cli(args)
    if cmd == "cloud":
        return _cloud(args)
    if cmd == "alerts":
        return _alerts(args)
    cliutil.err(f"Unknown monitoring command '{cmd}'.")
    return 2


def _monitor_connections(args) -> int:
    action = args.mc_action
    if action == "list":
        return _monitor_connections_list(args)
    if action == "names":
        return _monitor_connections_names(args)
    if action == "test":
        return _monitor_connections_test(args)
    if action == "add":
        return _monitor_connections_add(args)
    if action == "update":
        return _monitor_connections_update(args)
    if action == "remove":
        return _monitor_connections_remove(args)
    cliutil.err("Unknown monitor-connections action.")
    return 2


def _monitor_db_conns(args) -> int:
    action = args.mdb_action
    if action == "list":
        return _monitor_db_list(args)
    if action == "add":
        return _monitor_db_add(args)
    if action == "remove":
        return _monitor_db_remove(args)
    if action == "test":
        return _monitor_db_test(args)
    cliutil.err("Unknown monitor-db action.")
    return 2


def _monitor_db_list(args) -> int:
    rows = _service().list_monitor_db_connections()
    errs = [r for r in rows if r.get("error")]
    rows = [r for r in rows if not r.get("error")]
    if not rows and not errs:
        cliutil.info("No Monitor-only DB connections.")
        return 0
    headers = ["name", "db_type", "host", "port", "service_or_db", "username"]
    cliutil.print_table(
        [[r.get(h, "") for h in headers] for r in rows], headers, args.format
    )
    for e in errs:
        cliutil.err(e["error"])
    return 0


def _monitor_db_add(args) -> int:
    from monitoring import monitor_config as _mcfg
    _def_ssh_port = _mcfg.get_int("ssh.connection", "default_ssh_port", default=22)
    ssh_tunnel = None
    if getattr(args, "ssh_host", ""):
        ssh_tunnel = {
            "ssh_host": args.ssh_host,
            "ssh_user": getattr(args, "ssh_user", "") or "",
            "ssh_port": getattr(args, "ssh_port", "") or _def_ssh_port,
            "ssh_password": getattr(args, "ssh_password", "") or "",
            "ssh_key_file": getattr(args, "ssh_key_file", "") or "",
        }
    from common.connection_params import ConnectionParams

    r = _service().add_monitor_db_connection(
        ConnectionParams.from_mapping({
            "name": args.name,
            "db_type": args.db_type,
            "host": args.host,
            "port": args.port,
            "user": args.username,
            "password": args.password or "",
            "database": args.database or "",
            "service": args.service or "",
            "ssh_tunnel": ssh_tunnel,
        })
    )
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_db_remove(args) -> int:
    r = _service().remove_monitor_db_connection(args.name)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_db_test(args) -> int:
    r = _service().test_monitor_db_connection(args.name)
    if r.get("ok"):
        cliutil.ok(
            f"{r.get('message', 'OK')}  |  version: {r.get('version', 'unknown')}"
        )
        return 0
    cliutil.err(r.get("message", "failed"))
    return 1


def _monitor_connections_add(args) -> int:
    r = _service().add_monitor_connection(
        args.name, args.host, args.username,
        password=args.password or "",
        target_type=args.target_type or "vm",
    )
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_connections_update(args) -> int:
    r = _service().update_monitor_connection(
        args.name, args.name, args.host, args.username,
        password=args.password or "",
        target_type=args.target_type or None,
    )
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_connections_remove(args) -> int:
    r = _service().remove_monitor_connection(args.name)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_connections_list(args) -> int:
    rows = _service().list_all_connections(source=args.source)
    errs = [r for r in rows if r.get("error")]
    rows = [r for r in rows if not r.get("error")]
    if not rows and not errs:
        cliutil.info("No saved connections.")
        return 0
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
        [[r.get(h, "") for h in headers] for r in rows], headers, args.format
    )
    for e in errs:
        cliutil.err(f"[{e.get('source','?')}] {e['error']}")
    return 0


def _monitor_connections_names(args) -> int:
    """Emit '<source>\\t<name>' lines. Used by the bash menu picker."""
    rows = _service().list_all_connections(source=args.source)
    for r in rows:
        if r.get("error"):
            continue
        src = r.get("source", "") or ""
        name = r.get("name", "") or ""
        if name:
            print(f"{src}\t{name}")
    return 0


def _monitor_connections_test(args) -> int:
    svc = _service()
    src = args.source
    name = args.name
    cliutil.info(f"Testing [{src}] connection '{name}' ...")
    if src == "db":
        r = svc.test_connection(name)
        if r.get("ok"):
            cliutil.ok(
                f"{r.get('message', 'OK')}  |  version: {r.get('version', 'unknown')}"
            )
            return 0
        cliutil.err(r.get("message", "failed"))
        return 1
    if src == "cloud":
        r = svc.test_cloud_connection(name)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if src == "monitor-db":
        r = svc.test_monitor_db_connection(name)
        if r.get("ok"):
            cliutil.ok(
                f"{r.get('message', 'OK')}  |  version: {r.get('version', 'unknown')}"
            )
            return 0
        cliutil.err(r.get("message", "failed"))
        return 1
    if src == "monitor":
        r = svc.test_monitor_ssh(name)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    cliutil.err(f"Unknown source '{src}'.")
    return 2


def _monitor(args) -> int:
    svc = _service()
    names = [n.strip() for n in (args.conn or "").split(",") if n.strip()]
    if not names:
        names = [c["name"] for c in svc.list_connections()]
    if not names:
        cliutil.err("No connections to monitor.")
        return 1
    store: dict = {}

    def poll():
        ts = datetime.now().strftime("%H:%M:%S")
        for name in names:
            cliutil.info(f"[{ts}] Collecting metrics for '{name}' ...")
            # monitor_any picks the right backend (db / cloud / monitor SSH)
            # so this command stays unified with `monitor-connections list`.
            r = svc.monitor_any(name)
            if r.get("error"):
                cliutil.err(f"  {r['error']}")
                continue
            store[name] = r
            cliutil.info(f"  [source: {r.get('source') or '?'}]")
            for section, items in r.get("sections", []):
                print(f"  {cliutil.bold(section)}")
                for metric, val in items:
                    print(f"    {metric:<35} {val}")
            for a in r.get("alerts", []) or []:
                _alert(a.get("severity", "INFO"), a.get("message", ""))
                try:
                    svc.send_notification(
                        a.get("severity", "INFO"), a.get("message", "")
                    )
                except Exception:
                    pass
        if args.output:
            try:
                Path(args.output).write_text(json.dumps(
                    {k: {"sections": v["sections"], "timestamp": v["timestamp"]}
                     for k, v in store.items()}, indent=2, default=str))
            except Exception as e:
                cliutil.err(f"Could not write output: {e}")

    if args.once:
        poll()
        return 0
    cliutil.info(f"Monitoring {names} every {args.interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            poll()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        cliutil.info("Stopped.")
    return 0


def _is_tty() -> bool:
    import sys
    return sys.stdin.isatty() and sys.stdout.isatty()


def _pick_daemon_targets(svc) -> list[tuple[str, str]]:
    """Show every monitor-eligible saved connection and let the user
    multi-select. Returns a list of ``(source, name)`` pairs.

    Interactive use only — callers must check ``_is_tty()`` first.
    """
    rows = [r for r in svc.list_all_connections("all") if not r.get("error")]
    if not rows:
        cliutil.err(
            "No saved connections found. Add one with "
            "`dbtool connections add` or via the UI before starting the daemon."
        )
        return []

    print()
    print("Saved connections the daemon can monitor:")
    print("-" * 78)
    print(f"{'#':>3}  {'source':<8} {'name':<28} {'kind':<14} {'host'}")
    print("-" * 78)
    for idx, r in enumerate(rows, 1):
        print(
            f"{idx:>3}  {r.get('source', ''):<8} {r.get('name', ''):<28} "
            f"{r.get('kind', ''):<14} {r.get('host', '')}"
        )
    print("-" * 78)
    print(
        "Tip: the daemon is headless — it uses the credentials saved with each "
        "profile. Anything that needs an interactive login (e.g. cloud entries "
        "that require `aws sso login` / `az login`) must already be logged in "
        "on this machine."
    )
    print()

    raw = input(
        "Pick by number: comma list (e.g. '1,3,5'), ranges (e.g. '2-4'), "
        "'all', or blank to cancel: "
    ).strip()
    if not raw:
        return []
    if raw.lower() == "all":
        return [(r["source"], r["name"]) for r in rows]

    chosen: set[int] = set()
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if "-" in tok:
            try:
                lo, hi = (int(x) for x in tok.split("-", 1))
                if lo > hi:
                    lo, hi = hi, lo
                chosen.update(range(lo, hi + 1))
            except ValueError:
                cliutil.warn(f"Skipping invalid range '{tok}'")
        else:
            try:
                chosen.add(int(tok))
            except ValueError:
                cliutil.warn(f"Skipping invalid index '{tok}'")

    out: list[tuple[str, str]] = []
    for i in sorted(chosen):
        if 1 <= i <= len(rows):
            r = rows[i - 1]
            out.append((r["source"], r["name"]))
        else:
            cliutil.warn(f"Index {i} is out of range, skipping")
    return out


def _resolve_pair(svc, name: str) -> tuple[str, str] | None:
    """Find ``(source, name)`` for a free-form ``name``. Used when the user
    passes ``--connections`` explicitly so we don't need to ask them which
    store the name lives in."""
    if not hasattr(svc, "resolve_connection_source"):
        return ("db", name)  # backward compat
    source = svc.resolve_connection_source(name)
    return (source, name) if source else None


def _preflight_pairs(svc, pairs: list[tuple[str, str]]) -> tuple[list[str], list[tuple[str, str, str]]]:
    """Test each ``(source, name)`` pair before handing it to the daemon.

    Returns ``(ok_names, failures)`` where ``failures`` is a list of
    ``(source, name, reason)`` tuples so the caller can render a useful
    summary. Calls the same per-source test functions the unified
    ``monitor-connections test`` command uses.
    """
    ok_names: list[str] = []
    failures: list[tuple[str, str, str]] = []
    for source, name in pairs:
        cliutil.info(f"  pre-flight [{source}] {name} ...")
        try:
            if source == "db":
                r = svc.test_connection(name)
            elif source == "cloud":
                r = svc.test_cloud_connection(name)
            elif source == "monitor":
                r = svc.test_monitor_ssh(name)
            else:
                failures.append((source, name, f"unknown source '{source}'"))
                continue
            if r.get("ok"):
                ok_names.append(name)
            else:
                failures.append((source, name, r.get("message", "failed")))
        except Exception as exc:
            failures.append((source, name, str(exc)))
    return ok_names, failures


def _daemon(args) -> int:
    from monitoring.daemon import MonitorDaemon
    act = args.daemon_action
    if act == "start":
        return _daemon_start(args, MonitorDaemon)
    if act == "stop":
        r = MonitorDaemon.stop_daemon(args.pid_file)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "status":
        r = MonitorDaemon.daemon_status(args.pid_file)
        (cliutil.ok if r["running"] else cliutil.info)(r["message"])
        return 0
    cliutil.err("Unknown daemon action.")
    return 2


def _daemon_start(args, MonitorDaemon) -> int:
    """Start path with picker + pre-flight (see flags in register_cli)."""
    svc = _service()
    explicit = [n.strip() for n in (args.connections or "").split(",") if n.strip()]
    non_interactive = bool(getattr(args, "non_interactive", False))
    no_preflight = bool(getattr(args, "no_preflight", False))

    # Resolve the working set as (source, name) pairs.
    if explicit:
        pairs: list[tuple[str, str]] = []
        unknown: list[str] = []
        for n in explicit:
            p = _resolve_pair(svc, n)
            if p is None:
                unknown.append(n)
            else:
                pairs.append(p)
        if unknown:
            cliutil.err(
                "These names are not saved anywhere: " + ", ".join(unknown)
            )
            cliutil.info(
                "List available targets with: "
                "`dbtool monitor-connections list`"
            )
            return 1
    elif non_interactive or not _is_tty():
        # Headless invocation with no explicit list: keep the old default
        # (poll every saved DB connection) so existing systemd units stay
        # backward compatible.
        if non_interactive:
            cliutil.info(
                "Non-interactive mode and no --connections: defaulting "
                "to all saved DB connections."
            )
        pairs = [("db", c["name"]) for c in svc.list_connections()]
        if not pairs:
            cliutil.err("No saved connections to monitor.")
            return 1
    else:
        pairs = _pick_daemon_targets(svc)
        if not pairs:
            cliutil.info("Nothing selected. Daemon not started.")
            return 1

    # Pre-flight unless explicitly disabled. We always pre-flight when the
    # user picked interactively because that's the most common surface for
    # the "why didn't it start?" question; for --connections runs, the user
    # may have already validated, so we still pre-flight but never block
    # silently.
    if no_preflight:
        ok_names = [n for _, n in pairs]
        failures: list[tuple[str, str, str]] = []
    else:
        cliutil.info("Pre-flight: testing each selected connection ...")
        ok_names, failures = _preflight_pairs(svc, pairs)

    if failures:
        cliutil.warn(f"{len(failures)} connection(s) failed pre-flight:")
        for source, name, reason in failures:
            cliutil.warn(f"  - [{source}] {name}: {reason}")
        cliutil.info(
            "Daemon runs headless and cannot prompt for missing credentials. "
            "Fix each failing profile with `dbtool connections add` / "
            "`connections remove` / the UI, then retry."
        )
        if not ok_names:
            cliutil.err("Nothing reachable. Daemon not started.")
            return 1
        if _is_tty() and not non_interactive:
            cont = input(
                f"Continue with the {len(ok_names)} reachable one(s)? [Y/n]: "
            ).strip().lower()
            if cont in ("n", "no"):
                cliutil.info("Cancelled.")
                return 1
        else:
            cliutil.info(
                f"Continuing with the {len(ok_names)} reachable connection(s)."
            )

    cliutil.ok(f"Starting daemon for: {', '.join(ok_names)}")
    daemon = MonitorDaemon(
        connections=ok_names,
        interval=args.interval,
        pid_file=args.pid_file,
        log_file=args.log_file,
        metrics_file=args.output,
    )
    if args.foreground:
        cliutil.info("Starting daemon in foreground (Ctrl+C to stop)...")
        daemon.run_foreground()
    else:
        daemon.start()
        cliutil.ok(
            f"Daemon started (PID {daemon.pid}). Log: "
            f"{args.log_file or 'stderr'}"
        )
    return 0


def _parse_path(raw: str) -> tuple[str, ...] | None:
    """Convert a dot-joined --path CLI value into the tuple form check_many
    expects. Returns ``None`` when no path was supplied so the legacy
    path-less lookup (db/os) keeps working unchanged."""
    raw = (raw or "").strip()
    if not raw:
        return None
    return tuple(p for p in raw.split(".") if p)


def _thresholds(args) -> int:
    svc = _service()
    act = args.thresholds_action
    if act == "list":
        path = _parse_path(getattr(args, "path", ""))
        rules = svc.list_thresholds(
            source=args.source or None,
            path=path,
            api=getattr(args, "api", "") or None,
            enabled_only=not getattr(args, "all", False),
        )
        if not rules:
            cliutil.info("No threshold rules.")
            return 0
        if args.format == "json":
            print(json.dumps(rules, indent=2, default=str))
            return 0
        # Compose a flat "path" string for display so the table stays readable.
        for r in rules:
            r["path_str"] = ".".join(r.get("path") or [])
        headers = ["source", "api", "path_str", "metric", "metric_name",
                   "operator", "critical", "warning", "info", "unit", "window", "enabled"]
        rows = [[r.get(h, "") if r.get(h) is not None else "" for h in headers] for r in rules]
        cliutil.print_table(rows, headers, args.format)
        return 0
    if act == "show":
        path = _parse_path(getattr(args, "path", ""))
        rule = svc.show_threshold(args.source, args.metric, path=path)
        if rule is None:
            cliutil.err(f"No rule for ({args.source}, {args.metric}).")
            return 1
        if args.format == "json":
            print(json.dumps(rule, indent=2, default=str))
        else:
            for k, v in rule.items():
                print(f"  {k:<18} {v}")
        return 0
    if act == "check":
        path = _parse_path(getattr(args, "path", ""))
        alerts = svc.check_threshold(args.source, args.metric, args.value,
                                     instance_id=args.instance or "manual",
                                     path=path)
        if not alerts:
            cliutil.ok(f"No alert: {args.metric}={args.value} is within thresholds.")
            return 0
        for a in alerts:
            _alert(a["severity"], a["message"])
        return 0
    if act == "set":
        path = _parse_path(getattr(args, "path", ""))
        changes: dict = {}
        for field in ("critical", "warning", "info", "operator", "description"):
            val = getattr(args, field, None)
            if val is not None:
                changes[field] = val
        if getattr(args, "window", None) is not None:
            changes["window"] = args.window
        if not changes:
            cliutil.err("Nothing to change — pass at least one of "
                        "--critical/--warning/--info/--operator/--window/--description.")
            return 2
        r = svc.update_threshold(args.source, args.metric, changes, path=path)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act in ("enable", "disable"):
        path = _parse_path(getattr(args, "path", ""))
        r = svc.set_threshold_enabled(
            args.source, args.metric, act == "enable", path=path
        )
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    cliutil.err("Unknown thresholds action.")
    return 2


def _print_os_alerts(svc, metrics: dict, instance_id: str) -> int:
    """Evaluate OS thresholds against *metrics* and print any breaches.

    Also persists each alert to ``alerts.jsonl`` so the alerts log captures
    one-off CLI polls the same way the daemon does. Returns the alert count.
    """
    if not metrics:
        return 0
    alerts = []
    try:
        alerts = svc.check_os_alerts(metrics, instance_id=instance_id)
    except Exception as exc:
        cliutil.warn(f"OS alert evaluation failed: {exc}")
        return 0
    if not alerts:
        return 0
    cliutil.warn(f"{len(alerts)} OS alert(s):")
    for a in alerts:
        _alert(a.get("severity", "INFO"), a.get("message", ""))
        try:
            svc.log_alert(
                a.get("severity", "INFO"),
                a.get("message", ""),
                source="os",
                instance=instance_id,
            )
        except Exception:
            pass
    return len(alerts)


def _os_metrics(args) -> int:
    act = getattr(args, "os_action", "metrics")
    svc = _service()
    if act == "remote":
        r = svc.get_remote_os_metrics(args.name, disk_path=args.disk or "/")
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Failed to collect remote OS metrics.")
            return 1
        metrics = r.get("metrics") or {}
        if not metrics:
            cliutil.info("Remote host returned no metrics (kernel/perms?).")
            return 0
        cliutil.print_table(
            [[k, v] for k, v in metrics.items()],
            ["metric", "value"],
            args.format,
        )
        _print_os_alerts(svc, metrics, instance_id=args.name)
        return 0
    r = svc.get_os_metrics(disk_path=args.disk or "/")
    if r["error"]:
        cliutil.err(r["error"])
        return 1
    metrics = r["metrics"] or {}
    cliutil.print_table([[k, v] for k, v in metrics.items()], ["metric", "value"], args.format)
    _print_os_alerts(svc, metrics, instance_id="local")
    return 0


def _alerts(args) -> int:
    svc = _service()
    act = args.alerts_action
    if act == "list":
        r = svc.list_alerts(
            limit=args.limit, severity=args.severity or None,
            source=args.source or None, instance=args.instance or None,
        )
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        alerts = r.get("alerts") or []
        if not alerts:
            cliutil.info(f"No alerts in log ({r.get('path', '?')}).")
            return 0
        headers = ["time", "severity", "source", "instance", "message"]
        rows = [[a.get(h, "") for h in headers] for a in alerts]
        cliutil.print_table(rows, headers, args.format)
        cliutil.info(f"{len(alerts)} alert(s) listed from {r.get('path', '?')}.")
        return 0
    if act == "log":
        r = svc.log_alert(
            args.severity, args.message,
            source=args.source or "", instance=args.instance or "",
        )
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "clear":
        r = svc.clear_alerts(
            severity=args.severity or None,
            source=args.source or None, instance=args.instance or None,
        )
        if not r.get("ok"):
            cliutil.err(r.get("message", "Could not clear alerts."))
            return 1
        cliutil.ok(
            f"Removed {r.get('removed', 0)} alert(s); "
            f"kept {r.get('kept', 0)} in {r.get('path', '?')}."
        )
        return 0
    cliutil.err("Unknown alerts action.")
    return 2


def _notify(args) -> int:
    action = getattr(args, "notify_action", "send")
    if action == "config":
        cfg_act = getattr(args, "notify_cfg_action", "show")
        if cfg_act == "set":
            return _notify_config_set(args)
        return _notify_config(args)
    r = _service().send_notification(args.severity, args.message)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _notify_config(args) -> int:
    """Show resolved notification config + which secrets are set."""
    data = _service().get_notification_config()
    if getattr(args, "format", "table") == "json":
        print(json.dumps(data, indent=2, default=str))
        return 0
    rows = [[k, v] for k, v in data.items() if k != "ok"]
    cliutil.print_table(rows, ["setting", "value"], "table")
    cliutil.info(
        "Edit via: monitor notify config set <key> <value>  "
        "(secrets: monitor notify config set teams_webhook_url <url>)"
    )
    return 0


def _notify_config_set(args) -> int:
    key = args.key
    value = args.value or ""
    svc = _service()
    if key in ("teams_webhook_url", "smtp_password"):
        r = svc.set_notification_secret(key, value)
    else:
        r = svc.set_notification_config(key, value)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    return 0 if r["ok"] else 1


def _monitor_config_cli(args) -> int:
    action = args.config_action
    svc = _service()
    if action == "show":
        r = svc.get_monitor_config()
        if not r.get("ok"):
            cliutil.err(r.get("message", "Failed to read monitor config."))
            return 1
        section_filter = (getattr(args, "section", "") or "").strip()
        if getattr(args, "format", "table") == "json":
            out = r
            if section_filter:
                out = {**r, "config": {section_filter: r["config"].get(section_filter, {})}}
            print(json.dumps(out, indent=2, default=str))
            return 0
        cliutil.info(f"Config file: {r.get('path')} (live={r.get('live')})")
        for sec, keys in sorted(r.get("config", {}).items()):
            if section_filter and sec != section_filter:
                continue
            cliutil.info(f"[{sec}]")
            cliutil.print_table(
                [[k, v] for k, v in sorted(keys.items())],
                ["key", "value"], "table",
            )
        cliutil.info("Edit: monitor monitor-config set <section> <key> <value>")
        return 0
    if action == "set":
        r = svc.set_monitor_config(args.section, args.key, args.value)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if action == "restore":
        r = svc.restore_monitor_config()
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    cliutil.err("Unknown config action.")
    return 2


def _cloud(args) -> int:
    svc = _service()
    act = args.cloud_action
    if act == "connections":
        return _cloud_connections(args, svc)
    if act == "login":
        cliutil.info(f"Starting interactive login for cloud connection '{args.name}' ...")
        r = svc.cloud_login(args.name)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "metrics":
        r = svc.get_cloud_metrics(args.name)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        if r.get("text"):
            print(r["text"])
        for a in (r.get("alerts") or []):
            _alert(a["severity"], a["message"])
        return 0
    if act == "monitor":
        return _cloud_monitor(args, svc)
    if act == "rds-endpoint":
        r = svc.resolve_rds_endpoint(args.name)
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0 if r["ok"] else 1
        if not r["ok"]:
            cliutil.err(r["message"])
            return 1
        cliutil.print_table(
            [[r["host"], r["port"], r["db_type"]]],
            ["host", "port", "db_type"], "table",
        )
        cliutil.ok(r["message"])
        return 0
    cliutil.err("Unknown cloud action.")
    return 2


def _cloud_connections(args, svc) -> int:
    act = args.cloud_conn_action
    if act == "list":
        rows = svc.list_cloud_connections()
        if not rows:
            cliutil.info("No saved cloud connections.")
            return 0
        if "error" in rows[0]:
            cliutil.err(rows[0]["error"])
            return 1
        keys_seen: list[str] = []
        for row in rows:
            for k in row.keys():
                if k not in keys_seen:
                    keys_seen.append(k)
        preferred = ["name", "provider", "region", "resource_id", "subscription_id",
                     "project_id", "instance_id"]
        headers = [k for k in preferred if k in keys_seen] + \
                  [k for k in keys_seen if k not in preferred]
        cliutil.print_table(
            [[r.get(h, "") for h in headers] for r in rows],
            headers,
            getattr(args, "format", "table"),
        )
        return 0
    if act == "add":
        return _cloud_connections_add(args, svc)
    if act == "remove":
        r = svc.remove_cloud_connection(args.name)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "test":
        cliutil.info(f"Testing cloud connection '{args.name}' ...")
        r = svc.test_cloud_connection(args.name)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    cliutil.err("Unknown cloud connections action.")
    return 2


# ---------------------------------------------------------------------------
# Cloud "add" — schema-driven wizard (matches the desktop Connections/Monitor
# tab forms) plus a non-interactive --json/--field path for scripts and the
# REST API. Field definitions are the SAME shared schema the UI renders, so
# the three surfaces stay in lock-step.
# ---------------------------------------------------------------------------
_CLOUD_PROVIDER_CANON = {
    "aws": "AWS", "azure": "Azure", "gcp": "GCP", "other": "Other",
}


def _canon_provider(value: str) -> str:
    """Normalise an aws/azure/gcp token to the schema's capitalised key."""
    raw = (value or "").strip()
    return _CLOUD_PROVIDER_CANON.get(raw.lower(), raw)


def _cli_prompt(label: str, default: str = "", secret: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    try:
        if secret:
            import getpass
            val = getpass.getpass(f"  {label}{suffix}: ")
        else:
            val = input(f"  {label}{suffix}: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return default
    return val or default


def _cli_choose(label, values, labels=None, default=None) -> str:
    print(f"\n{label}:")
    for i, v in enumerate(values, 1):
        text = labels.get(v, v) if labels else v
        marker = "  <- default" if default == v else ""
        print(f"  {i}) {text}{marker}")
    while True:
        try:
            raw = input(
                f"Select [1-{len(values)}]"
                + (" (Enter = default)" if default else "") + ": "
            ).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return default if default is not None else values[0]
        if not raw and default is not None:
            return default
        if raw.isdigit() and 1 <= int(raw) <= len(values):
            return values[int(raw) - 1]
        cliutil.err("Invalid selection — enter a number from the list.")


def _cli_prompt_field(field) -> str:
    """Prompt for one schema field tuple: (label, key, marker, help[, options])."""
    label = field[0]
    marker = field[2] if len(field) > 2 else ""
    help_text = field[3] if len(field) > 3 else ""
    options = field[4] if len(field) > 4 else None
    clean_label = label.rstrip(" *")
    if help_text:
        print(f"    {help_text.splitlines()[0]}")
    if options:
        return _cli_choose(clean_label, list(options), default=options[0])
    return _cli_prompt(clean_label, secret=(marker == "*"))


def _build_cloud_profile_interactive(base: dict, name_hint: str):
    """Walk the shared schema, prompting only for values not already in *base*.

    Returns ``(store_name, profile_dict, schema)``.
    """
    from common.cloud import CLOUD_PROVIDER_SCHEMAS, MONITOR_TARGET_KINDS
    from common.cloud.profiles import PURPOSE_MONITOR, TARGET_CLOUD_DB
    from common.cloud.schemas import resource_fields_for

    data = dict(base or {})

    provider = _canon_provider(data.get("provider", ""))
    if provider not in CLOUD_PROVIDER_SCHEMAS:
        provider = _cli_choose(
            "Cloud provider", list(CLOUD_PROVIDER_SCHEMAS.keys()), default="AWS",
        )
    data["provider"] = provider
    schema = CLOUD_PROVIDER_SCHEMAS[provider]

    target_kind = data.get("target_kind", "")
    if target_kind not in MONITOR_TARGET_KINDS:
        target_kind = _cli_choose(
            "Monitoring target kind",
            list(MONITOR_TARGET_KINDS.keys()),
            labels=MONITOR_TARGET_KINDS,
            default=TARGET_CLOUD_DB,
        )
    data["target_kind"] = target_kind

    if name_hint:
        data.setdefault("display_name", name_hint)

    print("\n-- Resource identification --")
    for f in resource_fields_for(provider, target_kind):
        if data.get(f[1]):
            continue
        data[f[1]] = _cli_prompt_field(f)

    auth_mode = data.get("auth_mode", "")
    if auth_mode not in ("keys", "pwd", "sso"):
        sso_label = schema.get("sso_auth", {}).get("tab_label", "SSO / CLI login")
        auth_mode = _cli_choose(
            "Authentication mode",
            ["keys", "pwd", "sso"],
            labels={
                "keys": "Access keys / tokens",
                "pwd": "Username / password",
                "sso": sso_label,
            },
            default="keys",
        )
    data["auth_mode"] = auth_mode

    if auth_mode == "keys":
        auth_fields = schema.get("keys_auth", [])
    elif auth_mode == "pwd":
        auth_fields = schema.get("pwd_auth", [])
    else:
        auth_fields = schema.get("sso_auth", {}).get("fields", [])
    if auth_fields:
        print("\n-- Credentials --")
    for f in auth_fields:
        if data.get(f[1]):
            continue
        data[f[1]] = _cli_prompt_field(f)

    # Persist blanks for every other schema field too, so the saved profile
    # has the exact same shape the desktop UI produces.
    for grp in ("resource", "resource_vm", "resource_cloud_service",
                "keys_auth", "pwd_auth"):
        for f in schema.get(grp, []):
            data.setdefault(f[1], "")
    for f in schema.get("sso_auth", {}).get("fields", []):
        data.setdefault(f[1], "")

    data.setdefault("mfa_enabled", False)
    data.setdefault("mfa_type", "")
    data.setdefault("monitoring", False)
    data.setdefault("purpose", PURPOSE_MONITOR)

    store_name = name_hint or data.get("display_name") or ""
    if not store_name:
        store_name = _cli_prompt("Stored connection name")
    data.setdefault("display_name", store_name)
    return store_name or data["display_name"], data, schema


def _build_cloud_profile_noninteractive(args):
    """Build a profile dict from --json + --field + flags (no prompts)."""
    from common.cloud.profiles import PURPOSE_MONITOR

    profile: dict = {}
    if getattr(args, "json", ""):
        profile = json.loads(Path(args.json).read_text())
    for kv in (getattr(args, "field", None) or []):
        if "=" not in kv:
            raise ValueError(f"invalid --field '{kv}', expected key=value")
        key, _, val = kv.partition("=")
        profile[key.strip()] = val
    if getattr(args, "provider", ""):
        profile["provider"] = _canon_provider(args.provider)
    elif profile.get("provider"):
        profile["provider"] = _canon_provider(profile["provider"])
    if getattr(args, "target_kind", ""):
        profile["target_kind"] = args.target_kind
    if getattr(args, "auth_mode", ""):
        profile["auth_mode"] = args.auth_mode
    profile.setdefault("auth_mode", "keys")
    profile.setdefault("target_kind", "cloud_db")
    profile.setdefault("monitoring", False)
    profile.setdefault("purpose", PURPOSE_MONITOR)
    name = getattr(args, "name", "") or profile.get("display_name", "")
    if name:
        profile.setdefault("display_name", name)
    return name, profile


def _validate_and_save_cloud(svc, name, profile, *, run_test) -> int:
    from common.cloud import CLOUD_PROVIDER_SCHEMAS
    from common.cloud.profiles import TARGET_CLOUD_DB
    from common.cloud.validation import validate_cloud_profile

    provider = profile.get("provider", "")
    schema = CLOUD_PROVIDER_SCHEMAS.get(provider)
    if schema:
        err = validate_cloud_profile(
            profile, provider, schema,
            require_db_identifier=(profile.get("target_kind") == TARGET_CLOUD_DB),
            target_kind=profile.get("target_kind", TARGET_CLOUD_DB),
        )
        if err:
            cliutil.err(err)
            return 1
    r = svc.add_cloud_connection(name, profile)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    if not r["ok"]:
        return 1
    if run_test:
        cliutil.info(f"Testing cloud connection '{name}' ...")
        tr = svc.test_cloud_connection(name)
        (cliutil.ok if tr["ok"] else cliutil.warn)(tr["message"])
    return 0


def _cloud_connections_add(args, svc) -> int:
    has_seed = bool(getattr(args, "json", "")) or bool(getattr(args, "field", None))
    interactive = getattr(args, "interactive", False) or not has_seed
    run_test = not getattr(args, "no_test", False)

    if interactive:
        base: dict = {}
        name_hint = getattr(args, "name", "")
        if has_seed:
            try:
                seeded_name, base = _build_cloud_profile_noninteractive(args)
            except Exception as exc:
                cliutil.err(f"Could not parse seed profile: {exc}")
                return 1
            name_hint = name_hint or seeded_name
        else:
            if getattr(args, "provider", ""):
                base["provider"] = _canon_provider(args.provider)
            if getattr(args, "target_kind", ""):
                base["target_kind"] = args.target_kind
            if getattr(args, "auth_mode", ""):
                base["auth_mode"] = args.auth_mode
        if not sys.stdin.isatty():
            cliutil.err(
                "Interactive cloud add needs a terminal. For scripts/CI use "
                "'--json FILE' and/or repeated '--field key=value'."
            )
            return 2
        try:
            name, profile, _schema = _build_cloud_profile_interactive(base, name_hint)
        except (EOFError, KeyboardInterrupt):
            cliutil.info("\nCancelled.")
            return 1
        if not name:
            cliutil.err("A connection name (Display Name) is required.")
            return 2
        return _validate_and_save_cloud(svc, name, profile, run_test=run_test)

    try:
        name, profile = _build_cloud_profile_noninteractive(args)
    except Exception as exc:
        cliutil.err(f"Could not build profile: {exc}")
        return 1
    if not name:
        cliutil.err("--name is required (or provide display_name via --field/--json).")
        return 2
    return _validate_and_save_cloud(svc, name, profile, run_test=run_test)


def _cloud_monitor(args, svc) -> int:
    def poll():
        ts = datetime.now().strftime("%H:%M:%S")
        cliutil.info(f"[{ts}] Fetching cloud metrics for '{args.name}' ...")
        r = svc.get_cloud_metrics(args.name)
        if r.get("error"):
            cliutil.err(f"  {r['error']}")
            return
        if r.get("text"):
            print(r["text"])
        for a in (r.get("alerts") or []):
            _alert(a["severity"], a["message"])

    if args.once:
        poll()
        return 0
    cliutil.info(f"Monitoring cloud '{args.name}' every {args.interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            poll()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        cliutil.info("Stopped.")
    return 0


if __name__ == "__main__":
    # Direct execution (``python monitoring/cli.py``) delegates to the same
    # standalone runner used by ``python -m monitoring`` so behaviour, command
    # set, and help text stay identical (single source of truth).
    from monitoring.__main__ import main

    raise SystemExit(main())
