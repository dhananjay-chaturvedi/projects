"""
Operational dashboard — in-tool activity and status across tabs.

Reads runtime snapshots supplied by the UI shell. Does not poll databases,
refresh Monitor metrics, or trigger work in other tabs.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from common import paths as _paths
from common.core import modules as app_modules


def _metrics_file() -> Path:
    return _paths.metrics_snapshot_path()


def _daemon_pid() -> Path:
    return _paths.daemon_pid_path()


def _severity_rank(severity: str) -> int:
    return {"CRITICAL": 3, "WARNING": 2, "INFO": 1}.get(str(severity).upper(), 0)


@dataclass(frozen=True)
class DashboardRuntime:
    get_active_connections: Callable[[], dict] | None = None
    get_saved_connections: Callable[[], list] | None = None
    get_monitor_runtime: Callable[[], dict] | None = None
    get_ai_runtime: Callable[[], dict] | None = None
    get_schema_runtime: Callable[[], dict] | None = None
    get_objects_runtime: Callable[[], dict] | None = None
    get_sql_runtime: Callable[[], dict] | None = None
    get_connections_runtime: Callable[[], dict] | None = None


@dataclass(frozen=True)
class DashboardCapabilities:
    feature_module: str | None = None
    has_schema: bool = False
    has_ai: bool = False
    has_monitor: bool = False


@dataclass(frozen=True)
class DashboardPanelInputs:
    """Runtime slices needed to build dashboard panels."""

    active_list: list
    alerts: list
    monitor: dict
    ai: dict
    schema: dict
    objects: dict
    sql: dict
    conn_rt: dict
    saved_count: int


class DashboardService:
    """Collect dashboard data from in-memory / on-disk snapshots only."""

    def __init__(
        self,
        runtime: DashboardRuntime | None = None,
        capabilities: DashboardCapabilities | None = None,
    ):
        runtime = runtime or DashboardRuntime()
        capabilities = capabilities or DashboardCapabilities()
        self._get_active = runtime.get_active_connections or (lambda: {})
        self._get_saved = runtime.get_saved_connections or (lambda: [])
        self._get_monitor_runtime = runtime.get_monitor_runtime or (lambda: {})
        self._get_ai_runtime = runtime.get_ai_runtime or (lambda: {})
        self._get_schema_runtime = runtime.get_schema_runtime or (lambda: {})
        self._get_objects_runtime = runtime.get_objects_runtime or (lambda: {})
        self._get_sql_runtime = runtime.get_sql_runtime or (lambda: {})
        self._get_connections_runtime = runtime.get_connections_runtime or (lambda: {})
        self._feature_module = capabilities.feature_module
        self._has_schema = capabilities.has_schema
        self._has_ai = capabilities.has_ai
        self._has_monitor = capabilities.has_monitor

    def collect(self) -> dict[str, Any]:
        """Build a JSON-serializable dashboard snapshot (no external polling)."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        active = self._get_active() or {}
        saved = self._get_saved() or []
        conn_rt = self._get_connections_runtime() or {}

        active_list = []
        for name, mgr in active.items():
            active_list.append(
                {
                    "name": name,
                    "db_type": getattr(mgr, "db_type", "?"),
                    "connected": bool(getattr(mgr, "conn", None)),
                }
            )

        monitor = self._monitor_section()
        alerts = self._collect_alerts(monitor)
        ai = self._ai_section()
        schema = self._schema_section()
        objects = self._objects_section()
        sql = self._sql_section()
        overall = self._overall_status(active_list, alerts, monitor, ai, schema, sql)

        snapshot: dict[str, Any] = {
            "timestamp": now,
            "overall_status": overall,
            "overall_label": _overall_label(overall),
            "standalone_module": self._feature_module,
            "core": {
                "saved_connections_count": len(saved),
                "active_connections_count": len(active_list),
                "active_connections": active_list,
                "saved_by_type": _count_by_key(saved, "db_type"),
                "connections_runtime": conn_rt,
            },
            "modules": app_modules.status(),
            "alerts": alerts,
            "alert_summary": _summarize_alerts(alerts),
            "monitor": monitor,
            "ai": ai,
            "schema": schema,
            "objects": objects,
            "sql_editor": sql,
            "panels": self._build_panels(
                DashboardPanelInputs(
                    active_list=active_list,
                    alerts=alerts,
                    monitor=monitor,
                    ai=ai,
                    schema=schema,
                    objects=objects,
                    sql=sql,
                    conn_rt=conn_rt,
                    saved_count=len(saved),
                )
            ),
        }
        return snapshot

    def _overall_status(
        self,
        active_list: list,
        alerts: list,
        monitor: dict,
        ai: dict,
        schema: dict,
        sql: dict,
    ) -> str:
        if any(a.get("severity") == "CRITICAL" for a in alerts):
            return "critical"
        if any(a.get("severity") == "WARNING" for a in alerts):
            return "warning"
        if monitor.get("health") in ("degraded", "critical"):
            return "warning" if monitor.get("health") == "degraded" else "critical"
        if ai.get("ui_busy") or ai.get("running_sessions", 0) > 0:
            return "busy"
        if schema.get("running"):
            return "busy"
        if sql.get("query_running"):
            return "busy"
        if monitor.get("card_status") == "monitoring" or monitor.get("activity") == "active":
            return "healthy"
        if active_list:
            return "healthy"
        return "idle"

    def _collect_alerts(self, monitor: dict) -> list[dict[str, Any]]:
        """Alerts already recorded by Monitor tab or background daemon — read only."""
        alerts: list[dict[str, Any]] = []

        for item in monitor.get("recent_alerts") or []:
            if isinstance(item, dict) and item.get("message"):
                alerts.append(
                    {
                        "connection": item.get("connection") or "monitor",
                        "severity": item.get("severity", "INFO"),
                        "message": item.get("message", ""),
                        "source": item.get("source") or "monitor_tab",
                    }
                )

        if self._has_monitor:
            alerts.extend(_alerts_from_metrics_file())

        return _sort_alerts(alerts)

    def _monitor_section(self) -> dict[str, Any]:
        if not self._has_monitor:
            return _not_installed_section("monitor", "Performance Monitor")

        runtime = self._get_monitor_runtime() or {}
        daemon = _daemon_status()
        metrics_meta = _metrics_file_meta()

        ssh_count = int(runtime.get("ssh_hosts") or 0)
        cloud_saved = int(runtime.get("cloud_databases_saved") or 0)
        os_active = int(runtime.get("os_servers_active") or 0)
        local_db_active = int(runtime.get("local_databases_active") or 0)
        cloud_active = int(runtime.get("cloud_databases_active") or 0)
        actively = int(runtime.get("actively_monitoring") or 0)
        if actively <= 0:
            actively = os_active + local_db_active + cloud_active
        resources = actively if actively > 0 else (ssh_count + cloud_saved + local_db_active)
        polling = bool(runtime.get("polling_active"))
        tab_open = bool(runtime.get("tab_initialized"))

        recent = runtime.get("recent_alerts") or []
        card_status = _monitor_card_status(recent, actively, resources)

        if polling or actively > 0:
            activity = "active"
            parts = []
            if os_active:
                parts.append(f"{os_active} OS")
            if local_db_active:
                parts.append(f"{local_db_active} DB")
            if cloud_active:
                parts.append(f"{cloud_active} cloud")
            breakdown = ", ".join(parts) if parts else f"{actively} total"
            activity_label = f"Monitoring {actively} resource(s) ({breakdown})"
        elif tab_open:
            activity_label = "Monitor tab open — no resources polling"
            activity = "idle"
        elif daemon.get("running"):
            activity_label = "Background daemon running (Monitor tab not open)"
            activity = "daemon"
        else:
            activity_label = "No monitoring activity in the tool"
            activity = "idle"

        return {
            "installed": True,
            "ready": app_modules.status().get("monitor", {}).get("ready", False),
            "title": "Performance Monitor",
            "health": card_status,
            "card_status": card_status,
            "activity": activity,
            "health_label": {
                "monitoring": "Live monitoring in progress",
                "healthy": "Monitoring active — no alerts in session log",
                "degraded": "Alerts recorded in Monitor tab this session",
                "critical": "Critical alerts in Monitor session log",
                "ready": "Targets configured — start monitoring in Monitor tab",
                "idle": "No monitoring targets configured",
            }.get(card_status, card_status),
            "activity_label": activity_label,
            "tab_initialized": tab_open,
            "daemon_running": daemon.get("running", False),
            "daemon_pid": daemon.get("pid"),
            "polling_active": polling,
            "resources_monitored": resources,
            "actively_monitoring": actively,
            "os_servers_active": os_active,
            "local_databases_active": local_db_active,
            "cloud_databases_active": cloud_active,
            "ssh_hosts": ssh_count,
            "cloud_databases_saved": cloud_saved,
            "db_monitoring_targets": local_db_active,
            "unread_alerts_os": int(runtime.get("unread_os") or 0),
            "unread_alerts_db": int(runtime.get("unread_db") or 0),
            "metrics_snapshot": metrics_meta,
            "alert_count": len(recent),
            "alert_metric_tone": _alert_metric_tone(recent),
            "recent_alerts": recent[:15],
            "selected_connection": runtime.get("selected_connection") or "",
        }

    def _ai_section(self) -> dict[str, Any]:
        if not self._has_ai:
            return _not_installed_section("ai", "AI Query Assistant")

        runtime = self._get_ai_runtime() or {}
        if runtime.get("installed") is False:
            runtime = {}

        if runtime:
            busy = runtime.get("ui_busy") or runtime.get("running_sessions", 0) > 0
            return {
                "installed": True,
                "ready": app_modules.status().get("ai", {}).get("ready", True),
                "title": "AI Query Assistant",
                "tab_count": runtime.get("tab_count", 0),
                "running_sessions": runtime.get("running_sessions", 0),
                "ui_busy": runtime.get("ui_busy", False),
                "active_backend": runtime.get("active_backend", ""),
                "working_on": runtime.get("working_on", "—"),
                "sessions": runtime.get("sessions", []),
                "status": "running" if busy else "idle",
            }

        return {
            "installed": True,
            "ready": True,
            "title": "AI Query Assistant",
            "tab_count": 0,
            "running_sessions": 0,
            "ui_busy": False,
            "working_on": "AI Query tab not opened yet",
            "sessions": [],
            "status": "idle",
        }

    def _schema_section(self) -> dict[str, Any]:
        if not self._has_schema:
            return _not_installed_section("migrator", "Data Migration")

        runtime = self._get_schema_runtime() or {}
        if runtime:
            return {
                "installed": True,
                "ready": app_modules.status().get("migrator", {}).get("ready", True),
                "title": "Data Migration",
                "running": bool(runtime.get("running")),
                "status_text": runtime.get("status_text", "Idle"),
                "source_connection": runtime.get("source_connection", ""),
                "target_connection": runtime.get("target_connection", ""),
                "selected_tables": runtime.get("selected_tables", 0),
                "overview": runtime.get("overview", ""),
                "status": "running" if runtime.get("running") else "idle",
            }

        return {
            "installed": True,
            "ready": True,
            "title": "Data Migration",
            "running": False,
            "status_text": "Idle",
            "overview": "Data Migration tab not opened yet",
            "status": "idle",
        }

    def _objects_section(self) -> dict[str, Any]:
        runtime = self._get_objects_runtime() or {}
        initialized = runtime.get("initialized", False)
        conn = runtime.get("connection") or ""
        return {
            "title": "Database Objects",
            "initialized": initialized,
            "connection": conn,
            "db_type": runtime.get("db_type") or "",
            "operations_available": runtime.get("operations_available", 0),
            "overview": runtime.get("overview") or "Database Objects tab not opened yet",
            "status": "active" if conn else "idle",
        }

    def _sql_section(self) -> dict[str, Any]:
        runtime = self._get_sql_runtime() or {}
        running = bool(runtime.get("query_running"))
        return {
            "title": "SQL Editor",
            "initialized": runtime.get("initialized", False),
            "query_running": running,
            "connection": runtime.get("connection") or "",
            "last_query_preview": runtime.get("last_query_preview") or "",
            "last_query_time": runtime.get("last_query_time") or "",
            "history_count": runtime.get("history_count", 0),
            "overview": runtime.get("overview") or "SQL Editor tab not opened yet",
            "status": "running" if running else "idle",
        }

    def _build_panels(self, inputs: DashboardPanelInputs) -> list[dict[str, Any]]:
        """Ordered panels — module activity first, then core tabs."""
        active_list = inputs.active_list
        monitor = inputs.monitor
        ai = inputs.ai
        schema = inputs.schema
        objects = inputs.objects
        sql = inputs.sql
        conn_rt = inputs.conn_rt
        saved_count = inputs.saved_count
        panels: list[dict[str, Any]] = []

        panels.append(self._monitor_panel(monitor))
        panels.append(self._ai_panel(ai))
        panels.append(self._schema_panel(schema))

        sql_status = "running" if sql.get("query_running") else sql.get("status", "idle")
        panels.append(
            {
                "id": "sql_editor",
                "priority": 4,
                "title": sql.get("title", "SQL Editor"),
                "installed": True,
                "status": sql_status,
                "headline": sql.get("overview", ""),
                "metrics": [
                    {"label": "Connection", "value": sql.get("connection") or "—"},
                    {"label": "History", "value": str(sql.get("history_count", 0))},
                ],
                "detail_lines": _sql_detail_lines(sql),
                "navigate": "sql_editor",
            }
        )

        obj_status = objects.get("status", "idle")
        panels.append(
            {
                "id": "objects",
                "priority": 5,
                "title": objects.get("title", "Database Objects"),
                "installed": True,
                "status": obj_status,
                "headline": objects.get("overview", ""),
                "metrics": [
                    {"label": "Connection", "value": objects.get("connection") or "—"},
                    {"label": "Engine", "value": objects.get("db_type") or "—"},
                ],
                "detail_lines": _objects_detail_lines(objects),
                "navigate": "objects",
            }
        )

        active_count = conn_rt.get("active_count", len(active_list))
        panels.append(
            {
                "id": "connections",
                "priority": 6,
                "title": "Connections",
                "installed": True,
                "status": "active" if active_list else "idle",
                "headline": conn_rt.get("overview")
                or (
                    f"{active_count} active, {saved_count} saved profile(s)"
                    if active_list or saved_count
                    else "No connections yet"
                ),
                "metrics": [
                    {"label": "Active", "value": str(active_count)},
                    {"label": "Saved", "value": str(saved_count)},
                ],
                "detail_lines": [
                    f"{c['name']} ({c['db_type']})" for c in active_list[:6]
                ] or ["Connect from the Connections tab"],
                "navigate": "connections",
            }
        )

        panels.sort(key=lambda p: p.get("priority", 99))
        return panels

    def _monitor_panel(self, monitor: dict) -> dict[str, Any]:
        if not monitor.get("installed", False):
            return _not_installed_panel("monitor", monitor, priority=1)

        mon_status = monitor.get("card_status") or monitor.get("health", "missing")
        alert_count = monitor.get("alert_count", 0)
        return {
            "id": "monitor",
            "priority": 1,
            "title": monitor.get("title", "Monitor"),
            "installed": True,
            "status": mon_status,
            "headline": monitor.get("activity_label") or monitor.get("health_label", ""),
            "metrics": [
                {
                    "label": "Active resources",
                    "value": str(monitor.get("actively_monitoring", 0)),
                    "tone": "ok" if monitor.get("actively_monitoring") else "normal",
                },
                {
                    "label": "OS / DB / Cloud",
                    "value": (
                        f"{monitor.get('os_servers_active', 0)} / "
                        f"{monitor.get('local_databases_active', 0)} / "
                        f"{monitor.get('cloud_databases_active', 0)}"
                    ),
                    "tone": "normal",
                },
                {
                    "label": "Session alerts",
                    "value": str(alert_count),
                    "tone": monitor.get("alert_metric_tone", "normal"),
                },
                {
                    "label": "Polling",
                    "value": "Yes" if monitor.get("polling_active") else "No",
                    "tone": "ok" if monitor.get("polling_active") else "normal",
                },
            ],
            "detail_lines": _monitor_detail_lines(monitor),
            "navigate": "monitor",
        }

    def _ai_panel(self, ai: dict) -> dict[str, Any]:
        if not ai.get("installed", False):
            return _not_installed_panel("ai", ai, priority=2)

        status = "running" if ai.get("ui_busy") or ai.get("running_sessions") else ai.get("status", "idle")
        return {
            "id": "ai",
            "priority": 2,
            "title": ai.get("title", "AI Query"),
            "installed": True,
            "status": status,
            "headline": ai.get("working_on") or ai.get("message", ""),
            "metrics": [
                {"label": "Tabs", "value": str(ai.get("tab_count", 0))},
                {"label": "Running", "value": str(ai.get("running_sessions", 0))},
                {"label": "Backend", "value": ai.get("active_backend") or "auto"},
            ],
            "detail_lines": _ai_detail_lines(ai),
            "navigate": "ai_query",
        }

    def _schema_panel(self, schema: dict) -> dict[str, Any]:
        if not schema.get("installed", False):
            return _not_installed_panel("schema", schema, priority=3)

        status = "running" if schema.get("running") else schema.get("status", "idle")
        return {
            "id": "schema",
            "priority": 3,
            "title": schema.get("title", "Data Migration"),
            "installed": True,
            "status": status,
            "headline": schema.get("overview") or schema.get("message", ""),
            "metrics": [
                {"label": "Status", "value": schema.get("status_text", "Idle")},
                {"label": "Tables", "value": str(schema.get("selected_tables", 0))},
            ],
            "detail_lines": _schema_detail_lines(schema),
            "navigate": "conversion",
        }


def _not_installed_section(command: str, title: str) -> dict[str, Any]:
    pkg = _module_package_name(command)
    return {
        "installed": False,
        "title": title,
        "message": f"{title} is not installed in this build.",
        "package": pkg,
        "install_hint": _module_install_hint(command),
    }


def _not_installed_panel(
    panel_id: str, section: dict[str, Any], *, priority: int
) -> dict[str, Any]:
    pkg = section.get("package") or panel_id
    return {
        "id": panel_id,
        "priority": priority,
        "title": section.get("title", panel_id),
        "installed": False,
        "status": "missing",
        "headline": section.get("message", "Module not installed in this build."),
        "metrics": [
            {"label": "Package", "value": f"{pkg}/", "tone": "normal"},
            {"label": "In this build", "value": "Not shipped", "tone": "normal"},
        ],
        "detail_lines": section.get("install_hint") or _module_install_hint(panel_id),
        "navigate": None,
    }


def _module_package_name(command: str) -> str:
    try:
        from common.core.modules import KNOWN_MODULES

        pkg, _title = KNOWN_MODULES.get(command, (command, command))
        return pkg.replace(".", "/").split("/")[0]
    except Exception:
        return {
            "monitor": "monitoring",
            "ai": "ai_query",
            "migrator": "schema_converter",
        }.get(command, command)


def _module_install_hint(command: str) -> list[str]:
    pkg = _module_package_name(command)
    lines = [
        f"Copy the «{pkg}/» folder next to «common/», or install the full tool.",
        f"Install dependencies: pip install -r {pkg}/requirements.txt",
        "Verify with: dbtool.py modules — then restart the app.",
    ]
    try:
        mod_status = app_modules.status().get(command, {})
        missing = mod_status.get("missing_requirements") or []
        if mod_status.get("installed") and missing:
            lines.insert(0, "Package found but dependencies are missing:")
            for item in missing[:4]:
                lines.append(f"  • {item}")
    except Exception:
        pass
    return lines


def _monitor_card_status(
    recent: list[dict], actively: int, resources: int
) -> str:
    if recent:
        if any(a.get("severity") == "CRITICAL" for a in recent):
            return "critical"
        return "degraded"
    if actively > 0:
        return "monitoring"
    if resources > 0:
        return "ready"
    return "idle"


def _alert_metric_tone(recent: list[dict]) -> str:
    if not recent:
        return "ok"
    if any(a.get("severity") == "CRITICAL" for a in recent):
        return "critical"
    if any(a.get("severity") == "WARNING" for a in recent):
        return "warning"
    return "normal"


def _overall_label(status: str) -> str:
    return {
        "critical": "Critical — review Monitor alerts",
        "warning": "Warning — alerts recorded in the tool",
        "busy": "Work in progress — query, AI, or data migration running",
        "healthy": "Operational — connections or monitoring active",
        "idle": "Idle — open a tab or connect to get started",
    }.get(status, status)


def _monitor_detail_lines(mon: dict) -> list[str]:
    lines = []
    actively = mon.get("actively_monitoring") or 0
    if actively:
        lines.append(
            f"{actively} resource(s) actively monitored — "
            f"OS {mon.get('os_servers_active', 0)}, "
            f"DB {mon.get('local_databases_active', 0)}, "
            f"cloud {mon.get('cloud_databases_active', 0)}"
        )
    if mon.get("selected_connection"):
        lines.append(f"Selected in Monitor tab: {mon['selected_connection']}")
    unread = (mon.get("unread_alerts_os") or 0) + (mon.get("unread_alerts_db") or 0)
    if unread:
        lines.append(f"{unread} unread alert(s) in Monitor tab")
    meta = mon.get("metrics_snapshot") or {}
    if meta.get("updated"):
        lines.append(f"Last daemon snapshot on disk: {meta['updated']}")
    if not lines:
        lines.append(mon.get("message") or "Open Monitor tab to configure targets")
    return lines


def _ai_detail_lines(ai: dict) -> list[str]:
    lines = []
    for s in ai.get("sessions") or []:
        if s.get("status") not in ("idle", ""):
            lines.append(
                f"Tab {s.get('tab')}: {s.get('connection')} — {s.get('status')} "
                f"({s.get('backend', 'auto')})"
            )
    if not lines and ai.get("sessions"):
        lines.append(f"{len(ai['sessions'])} session tab(s) open")
    return lines[:5]


def _schema_detail_lines(schema: dict) -> list[str]:
    lines = []
    if schema.get("source_connection"):
        lines.append(f"Source: {schema['source_connection']}")
    if schema.get("target_connection"):
        lines.append(f"Target: {schema['target_connection']}")
    if schema.get("running"):
        lines.append(schema.get("status_text") or "Operation running…")
    return lines or [schema.get("overview") or "Ready"]


def _sql_detail_lines(sql: dict) -> list[str]:
    lines = []
    if sql.get("query_running"):
        lines.append(f"Query running on {sql.get('connection') or '?'}")
    elif sql.get("last_query_preview"):
        preview = sql["last_query_preview"].replace("\n", " ")[:100]
        when = sql.get("last_query_time") or ""
        lines.append(f"Last query{f' ({when})' if when else ''}: {preview}")
    return lines or [sql.get("overview") or "Ready"]


def _objects_detail_lines(objects: dict) -> list[str]:
    lines = []
    if objects.get("connection"):
        lines.append(f"Viewing: {objects['connection']} ({objects.get('db_type') or '?'})")
    ops = objects.get("operations_available") or 0
    if ops:
        lines.append(f"{ops} object operation(s) available for this engine")
    return lines or [objects.get("overview") or "Ready"]


def _count_by_key(rows: list, key: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        val = row.get(key, "unknown")
        out[val] = out.get(val, 0) + 1
    return out


def _sort_alerts(alerts: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for a in sorted(alerts, key=lambda x: -_severity_rank(x.get("severity", ""))):
        key = (a.get("connection"), a.get("severity"), a.get("message"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(a)
    return unique


def _summarize_alerts(alerts: list[dict]) -> dict[str, int]:
    summary = {"CRITICAL": 0, "WARNING": 0, "INFO": 0, "total": len(alerts)}
    for a in alerts:
        sev = str(a.get("severity", "INFO")).upper()
        if sev in summary:
            summary[sev] += 1
    return summary


def _daemon_status() -> dict[str, Any]:
    pid_path = _daemon_pid()
    if not pid_path.exists():
        return {"running": False, "pid": None}
    try:
        pid = int(pid_path.read_text().strip())
        try:
            os.kill(pid, 0)
            running = True
        except OSError:
            running = False
        return {"running": running, "pid": pid}
    except Exception:
        return {"running": False, "pid": None}


def _metrics_file_meta() -> dict[str, Any]:
    mfile = _metrics_file()
    if not mfile.exists():
        return {"exists": False}
    try:
        data = json.loads(mfile.read_text())
        return {
            "exists": True,
            "updated": data.get("timestamp") or data.get("updated"),
            "connections": list((data.get("connections") or data).keys())
            if isinstance(data.get("connections"), dict)
            else [],
        }
    except Exception:
        return {"exists": True, "updated": None, "connections": []}


def _alerts_from_metrics_file() -> list[dict[str, Any]]:
    """Passive read of daemon-written metrics file — no DB or Monitor tab polling."""
    mfile = _metrics_file()
    if not mfile.exists():
        return []
    try:
        data = json.loads(mfile.read_text())
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    connections = data.get("connections")
    if isinstance(connections, dict):
        for conn_name, payload in connections.items():
            for alert in payload.get("alerts") or []:
                if isinstance(alert, dict):
                    out.append(
                        {
                            "connection": conn_name,
                            "severity": alert.get("severity", "INFO"),
                            "message": alert.get("message", ""),
                            "source": "daemon",
                        }
                    )
    for alert in data.get("alerts") or []:
        if isinstance(alert, dict):
            out.append(
                {
                    "connection": alert.get("connection", ""),
                    "severity": alert.get("severity", "INFO"),
                    "message": alert.get("message", ""),
                    "source": "daemon",
                }
            )
    return out
