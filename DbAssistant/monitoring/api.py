"""
REST API surface for the Monitoring module.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from monitoring import monitor_config as _mcfg

_DEF_SSH_PORT = _mcfg.get_int("ssh.connection", "default_ssh_port", default=22)
_DEF_ALERTS_LIMIT = _mcfg.get_int("monitoring.limits", "alerts_default_limit", default=50)


class MonitorSSHTunnelSpec(BaseModel):
    ssh_host: str = Field(..., min_length=1, max_length=512,
                          examples=["bastion.example.com"])
    ssh_user: str = Field("", max_length=256, examples=["ubuntu"])
    ssh_port: int = Field(_DEF_SSH_PORT, ge=1, le=65535, examples=[22])
    ssh_password: str = Field("", max_length=4096)
    ssh_key_file: str = Field("", max_length=1024, examples=["/home/me/.ssh/id_rsa"])


class ThresholdCheckRequest(BaseModel):
    source: str = Field(..., examples=["db"])
    metric: str = Field(..., examples=["cpu_percent"])
    value: float = Field(..., examples=[95.0])
    instance: str = Field("manual", examples=["manual"])
    path: list[str] | None = Field(
        None,
        examples=[["cloudwatch", "RDS"]],
        description=(
            "Rule path segments — required when the rule lives under a "
            "specific API/namespace (e.g. ['cloudwatch','RDS'] for AWS "
            "CloudWatch RDS rules). Omit for db/os rules."
        ),
    )


class ThresholdUpdateRequest(BaseModel):
    critical: float | None = Field(None, examples=[95.0])
    warning: float | None = Field(None, examples=[85.0])
    info: float | None = Field(None, examples=[70.0])
    operator: str | None = Field(None, examples=[">="])
    window: str | None = Field(None, examples=["5m"])
    enabled: bool | None = Field(None, examples=[True])
    description: str | None = Field(None, examples=["High CPU usage"])
    path: list[str] | None = Field(
        None,
        examples=[["cloudwatch", "RDS"]],
        description="Rule path segments for cloud/nested threshold rules.",
    )


class ThresholdEnableRequest(BaseModel):
    enabled: bool = Field(..., examples=[True])
    path: list[str] | None = Field(None, examples=[["cloudwatch", "RDS"]])


class NotifyRequest(BaseModel):
    severity: str = Field(..., examples=["WARNING"])
    message: str = Field(..., examples=["Disk almost full"])


class CloudConnectionCreate(BaseModel):
    name: str = Field(..., examples=["prod-rds"])
    profile: dict = Field(..., description="Provider profile (include a 'provider' key)")


class MonitorConnectionCreate(BaseModel):
    name: str = Field(..., examples=["db-host-01"])
    host: str = Field(..., examples=["10.0.0.5"])
    username: str = Field(..., examples=["ec2-user"])
    password: str = Field("", description="Empty for SSH-key/agent based auth")
    target_type: str = Field(
        "vm", examples=["vm", "db_server", "service"],
        description="vm | db_server | service",
    )


class MonitorConnectionUpdate(BaseModel):
    host: str = Field(..., examples=["10.0.0.5"])
    username: str = Field(..., examples=["ec2-user"])
    password: str = Field("", description="Empty preserves existing password")
    target_type: str = Field(
        "", examples=["vm", "db_server", "service"],
        description="Empty preserves previous target_type",
    )


class MonitorDBConnectionCreate(BaseModel):
    name: str = Field(..., examples=["metrics-mariadb"])
    db_type: str = Field(..., examples=["MariaDB", "MySQL", "PostgreSQL", "Oracle"])
    host: str = Field(..., examples=["localhost"])
    port: str = Field("", examples=["3306"])
    database: str = Field("", description="Database name (non-Oracle)")
    service: str = Field("", description="Oracle service name")
    username: str = Field("", examples=["dbuser"])
    password: str = Field("", description="Stored encrypted; empty to omit")
    ssh_tunnel: Optional[MonitorSSHTunnelSpec] = Field(
        None,
        description="Reach a remote database through an SSH tunnel. host/port "
                    "above are the DB endpoint as seen from the SSH host "
                    "(often localhost).",
    )


class AlertLogRequest(BaseModel):
    severity: str = Field(..., examples=["WARNING"])
    message: str = Field(..., examples=["High CPU on prod-db"])
    source: str = Field("", examples=["os"])
    instance: str = Field("", examples=["prod-db"])


class MonitorConfigSet(BaseModel):
    section: str = Field(..., examples=["monitoring", "cloud.lookback", "notifications"])
    key: str = Field(..., examples=["metrics_refresh_interval"])
    value: str = Field(..., examples=["5000"])


class NotificationConfigSet(BaseModel):
    key: str = Field(..., examples=["enabled", "smtp_host", "min_severity"])
    value: str = Field(..., examples=["true"])


class NotificationSecretSet(BaseModel):
    key: str = Field(..., examples=["teams_webhook_url", "smtp_password"])
    value: str = Field("", description="Empty clears the secret")


def build_router(svc=None):
    from fastapi import APIRouter, HTTPException, Query

    if svc is None:
        from monitoring.service import make_service

        svc = make_service()

    router = APIRouter()

    def _error(detail: str, status: int = 400):
        raise HTTPException(status_code=status, detail=detail)

    @router.get("/api/monitor/connections", tags=["Monitoring"])
    def list_monitor_connections(
        source: str = Query(
            "all",
            description="Filter: all | db | monitor | cloud",
        ),
    ):
        """Unified view of every monitor-eligible saved connection.

        Combines Connections-tab DB profiles (``source=db``), Monitor-tab
        SSH/host targets (``source=monitor``) and saved Cloud DB profiles
        (``source=cloud``). Use the ``source`` query parameter to filter.
        """
        normalised = (source or "all").strip().lower()
        if normalised not in ("all", "db", "monitor-db", "monitor", "cloud"):
            _error(
                f"Invalid source '{source}'. Expected one of: all, db, "
                "monitor-db, monitor, cloud."
            )
        rows = svc.list_all_connections(source=normalised)
        return {"connections": rows, "count": len(rows)}

    @router.get("/api/metrics", tags=["Monitoring"])
    def get_all_metrics():
        """Latest metrics for all connections (daemon file if present, else live)."""
        import json

        from common import paths as _paths

        metrics_file = _paths.metrics_snapshot_path()
        if metrics_file.exists():
            try:
                return json.loads(metrics_file.read_text())
            except Exception:
                pass
        result = {}
        for conn in svc.list_connections():
            name = conn["name"]
            r = svc.get_metrics(name)
            if not r.get("error"):
                result[name] = {
                    "sections": r["sections"],
                    "timestamp": r["timestamp"],
                    "os_note": r["os_note"],
                }
        return result

    @router.get("/api/metrics/{connection}", tags=["Monitoring"])
    def get_connection_metrics(connection: str):
        """Collect and return metrics + alerts for any monitor-eligible target."""
        if hasattr(svc, "monitor_any"):
            r = svc.monitor_any(connection)
            if r.get("error"):
                _error(r["error"])
            return {
                "connection": connection,
                "source": r.get("source", ""),
                "timestamp": r.get("timestamp"),
                "os_note": r.get("os_note", ""),
                "sections": r.get("sections", []),
                "raw_floats": r.get("raw_floats", {}),
                "alerts": r.get("alerts", []),
            }
        r = svc.get_metrics(connection)
        if r.get("error"):
            _error(r["error"])
        alerts = svc.check_alerts(connection, r.get("raw_floats", {}))
        return {
            "connection": connection,
            "source": "db",
            "timestamp": r["timestamp"],
            "os_note": r["os_note"],
            "sections": r["sections"],
            "raw_floats": r.get("raw_floats", {}),
            "alerts": alerts,
        }

    @router.get("/api/thresholds", tags=["Thresholds"])
    def thresholds_list(
        source: str = Query("", description="db | os | aws | azure | gcp"),
        api: str = Query("", description="cloudwatch | pi | azuremonitor | cloudmonitoring"),
        path: str = Query(
            "",
            description=(
                "Dot-joined rule path, e.g. cloudwatch.RDS or "
                "azuremonitor.DBforMySQL.flexibleServers."
            ),
        ),
        all: bool = Query(False, description="Include disabled rules"),
    ):
        parsed_path = tuple(p for p in (path or "").split(".") if p) or None
        return svc.list_thresholds(
            source=source or None,
            path=parsed_path,
            api=api or None,
            enabled_only=not all,
        )

    @router.get("/api/thresholds/{source}/{metric}", tags=["Thresholds"])
    def thresholds_show(
        source: str,
        metric: str,
        path: str = Query("", description="Dot-joined rule path"),
    ):
        parsed_path = tuple(p for p in (path or "").split(".") if p) or None
        rule = svc.show_threshold(source, metric, path=parsed_path)
        if rule is None:
            _error(f"No rule for ({source}, {metric}).", 404)
        return rule

    @router.post("/api/thresholds/check", tags=["Thresholds"])
    def thresholds_check(req: ThresholdCheckRequest):
        alerts = svc.check_threshold(
            req.source, req.metric, req.value,
            instance_id=req.instance or "manual",
            path=tuple(req.path) if req.path else None,
        )
        return {"alerts": alerts, "count": len(alerts)}

    @router.patch("/api/thresholds/{source}/{metric}", tags=["Thresholds"])
    def thresholds_update(source: str, metric: str, req: ThresholdUpdateRequest):
        changes = {
            k: v for k, v in req.model_dump(exclude={"path"}, exclude_none=True).items()
        }
        if not changes:
            _error("No threshold changes supplied.")
        if not hasattr(svc, "update_threshold"):
            _error("update_threshold not supported.", 501)
        r = svc.update_threshold(
            source, metric, changes,
            path=tuple(req.path) if req.path else None,
        )
        if not r.get("ok"):
            _error(r.get("message", "Threshold update failed."))
        return r

    @router.post("/api/thresholds/{source}/{metric}/enabled", tags=["Thresholds"])
    def thresholds_set_enabled(source: str, metric: str, req: ThresholdEnableRequest):
        if not hasattr(svc, "set_threshold_enabled"):
            _error("set_threshold_enabled not supported.", 501)
        r = svc.set_threshold_enabled(
            source, metric, req.enabled,
            path=tuple(req.path) if req.path else None,
        )
        if not r.get("ok"):
            _error(r.get("message", "Threshold update failed."))
        return r

    @router.get("/api/os/metrics", tags=["Monitoring"])
    def os_metrics(disk: str = Query("/", description="Disk usage mount point")):
        r = svc.get_os_metrics(disk_path=disk)
        if r.get("error"):
            _error(r["error"])
        if hasattr(svc, "check_os_alerts"):
            metrics = r.get("metrics") or {}
            r["alerts"] = svc.check_os_alerts(metrics, instance_id="local")
            r["alert_count"] = len(r["alerts"])
        return r

    @router.post("/api/notify", tags=["Notify"])
    def notify(req: NotifyRequest):
        r = svc.send_notification(req.severity, req.message)
        if not r["ok"]:
            _error(r["message"])
        return r

    # ------------------------------------------------------------------
    # Module-owned config (monitoring/monitor_config.ini) + notifications.
    # Writes here are intentionally allowed (unlike the read-only core
    # /api/config/*) because this is the module's own settings surface.
    # ------------------------------------------------------------------
    def _mon_svc():
        """Config/notify routes always use the monitoring service (not CoreDBService)."""
        from monitoring.service import make_service
        return make_service()

    @router.get("/api/monitor/config", tags=["Config"])
    def monitor_config_get():
        """All monitor_config.ini sections/values (notification secrets excluded)."""
        return _mon_svc().get_monitor_config()

    @router.post("/api/monitor/config", tags=["Config"])
    def monitor_config_set(req: MonitorConfigSet):
        r = _mon_svc().set_monitor_config(req.section, req.key, req.value)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.post("/api/monitor/config/restore", tags=["Config"])
    def monitor_config_restore():
        r = _mon_svc().restore_monitor_config()
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.get("/api/monitor/notifications", tags=["Notify"])
    def notifications_get():
        """Notification config + which secrets are set (never the values)."""
        return _mon_svc().get_notification_config()

    @router.post("/api/monitor/notifications", tags=["Notify"])
    def notifications_set(req: NotificationConfigSet):
        r = _mon_svc().set_notification_config(req.key, req.value)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.post("/api/monitor/notifications/secret", tags=["Notify"])
    def notifications_secret_set(req: NotificationSecretSet):
        r = _mon_svc().set_notification_secret(req.key, req.value)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.get("/api/monitor/cloud/providers/schema", tags=["Cloud"])
    def cloud_provider_schema(provider: str = Query("", examples=["aws"])):
        """Field metadata for building a cloud "add connection" form.

        Returns the same per-provider/auth-mode/target-kind field definitions
        the desktop UI renders and the CLI wizard prompts for, so API clients
        can offer identical add-connection forms. Pass ``?provider=aws`` to get
        a single provider, or omit it for all providers.
        """
        from common.cloud import CLOUD_PROVIDER_SCHEMAS, MONITOR_TARGET_KINDS

        canon = {"aws": "AWS", "azure": "Azure", "gcp": "GCP", "other": "Other"}
        out = {
            "target_kinds": MONITOR_TARGET_KINDS,
            "auth_modes": ["keys", "pwd", "sso"],
            "providers": {},
        }
        wanted = canon.get(provider.lower(), provider) if provider else ""
        for key, schema in CLOUD_PROVIDER_SCHEMAS.items():
            if wanted and key != wanted:
                continue
            out["providers"][key] = schema
        if wanted and not out["providers"]:
            _error(f"Unknown provider '{provider}'.", 404)
        return out

    @router.get("/api/monitor/cloud/connections", tags=["Cloud"])
    def cloud_connections_list():
        rows = svc.list_cloud_connections()
        if rows and isinstance(rows[0], dict) and "error" in rows[0]:
            _error(rows[0]["error"])
        return rows

    @router.post("/api/monitor/cloud/connections", tags=["Cloud"], status_code=201)
    def cloud_connections_add(req: CloudConnectionCreate):
        r = svc.add_cloud_connection(req.name, req.profile)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.delete("/api/monitor/cloud/connections/{name}", tags=["Cloud"])
    def cloud_connections_remove(name: str):
        r = svc.remove_cloud_connection(name)
        if not r["ok"]:
            _error(r["message"], 404)
        return r

    @router.post("/api/monitor/cloud/connections/{name}/test", tags=["Cloud"])
    def cloud_connections_test(name: str):
        r = svc.test_cloud_connection(name)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.post("/api/monitor/cloud/connections/{name}/login", tags=["Cloud"])
    def cloud_connections_login(name: str):
        r = svc.cloud_login(name)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.get("/api/monitor/cloud/metrics/{name}", tags=["Cloud"])
    def cloud_metrics(name: str):
        r = svc.get_cloud_metrics(name)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/daemon/status", tags=["Monitoring"])
    def daemon_status():
        from monitoring.daemon import MonitorDaemon

        return MonitorDaemon.daemon_status()

    # ------------------------------------------------------------------
    # Parity additions (Phase 5) — Monitor SSH CRUD, remote OS, alerts,
    # RDS endpoint.
    # ------------------------------------------------------------------

    @router.get("/api/monitor/connections/saved", tags=["Monitoring"])
    def monitor_connections_saved():
        """Saved Monitor-tab SSH/host profiles (scrubbed)."""
        if not hasattr(svc, "list_monitor_connections"):
            _error("list_monitor_connections not supported.", 501)
        rows = svc.list_monitor_connections()
        if rows and isinstance(rows[0], dict) and rows[0].get("error"):
            _error(rows[0]["error"])
        return {"connections": rows, "count": len(rows)}

    @router.get(
        "/api/monitor/connections/saved/{name}", tags=["Monitoring"],
    )
    def monitor_connection_show(name: str):
        if not hasattr(svc, "get_monitor_connection"):
            _error("get_monitor_connection not supported.", 501)
        c = svc.get_monitor_connection(name)
        if c is None:
            _error(f"Monitor connection '{name}' not found.", 404)
        if isinstance(c, dict) and c.get("error"):
            _error(c["error"])
        return c

    @router.post(
        "/api/monitor/connections/saved", tags=["Monitoring"], status_code=201,
    )
    def monitor_connection_add(req: MonitorConnectionCreate):
        if not hasattr(svc, "add_monitor_connection"):
            _error("add_monitor_connection not supported.", 501)
        r = svc.add_monitor_connection(
            req.name, req.host, req.username,
            password=req.password or "",
            target_type=req.target_type or "vm",
        )
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.put(
        "/api/monitor/connections/saved/{name}", tags=["Monitoring"],
    )
    def monitor_connection_update(name: str, req: MonitorConnectionUpdate):
        if not hasattr(svc, "update_monitor_connection"):
            _error("update_monitor_connection not supported.", 501)
        r = svc.update_monitor_connection(
            name, name, req.host, req.username,
            password=req.password or "",
            target_type=req.target_type or None,
        )
        if not r["ok"]:
            _error(r["message"], 404 if "not found" in r["message"].lower() else 400)
        return r

    @router.delete(
        "/api/monitor/connections/saved/{name}", tags=["Monitoring"],
    )
    def monitor_connection_remove(name: str):
        if not hasattr(svc, "remove_monitor_connection"):
            _error("remove_monitor_connection not supported.", 501)
        r = svc.remove_monitor_connection(name)
        if not r["ok"]:
            _error(r["message"], 404)
        return r

    # --- Monitor-tab-only DB connections (isolated from the Connections tab) ---
    @router.get("/api/monitor/db-connections", tags=["Monitoring"])
    def monitor_db_connections_list():
        """Saved Monitor-tab-only DB profiles (passwords omitted).

        These are isolated to Monitoring: they are stored separately from the
        Connections-tab DB profiles and are never returned to / usable by the
        SQL Editor, Data Migration or AI Query surfaces.
        """
        if not hasattr(svc, "list_monitor_db_connections"):
            _error("list_monitor_db_connections not supported.", 501)
        rows = svc.list_monitor_db_connections()
        if rows and isinstance(rows[0], dict) and rows[0].get("error"):
            _error(rows[0]["error"])
        return {"connections": rows, "count": len(rows)}

    @router.post(
        "/api/monitor/db-connections", tags=["Monitoring"], status_code=201,
    )
    def monitor_db_connection_add(req: MonitorDBConnectionCreate):
        if not hasattr(svc, "add_monitor_db_connection"):
            _error("add_monitor_db_connection not supported.", 501)
        from common.connection_params import ConnectionParams

        r = svc.add_monitor_db_connection(
            ConnectionParams.from_mapping({
                "name": req.name,
                "db_type": req.db_type,
                "host": req.host,
                "port": req.port,
                "user": req.username,
                "password": req.password or "",
                "database": req.database or "",
                "service": req.service or "",
                "ssh_tunnel": req.ssh_tunnel.model_dump() if req.ssh_tunnel else None,
            })
        )
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.delete(
        "/api/monitor/db-connections/{name}", tags=["Monitoring"],
    )
    def monitor_db_connection_remove(name: str):
        if not hasattr(svc, "remove_monitor_db_connection"):
            _error("remove_monitor_db_connection not supported.", 501)
        r = svc.remove_monitor_db_connection(name)
        if not r["ok"]:
            _error(r["message"], 404)
        return r

    @router.post(
        "/api/monitor/db-connections/{name}/test", tags=["Monitoring"],
    )
    def monitor_db_connection_test(name: str):
        if not hasattr(svc, "test_monitor_db_connection"):
            _error("test_monitor_db_connection not supported.", 501)
        return svc.test_monitor_db_connection(name)

    @router.get(
        "/api/monitor/db-connections/{name}/metrics", tags=["Monitoring"],
    )
    def monitor_db_connection_metrics(name: str):
        if not hasattr(svc, "get_metrics_monitor_db"):
            _error("get_metrics_monitor_db not supported.", 501)
        r = svc.get_metrics_monitor_db(name)
        if r.get("error"):
            _error(r["error"], 404 if "not found" in r["error"].lower() else 400)
        return r

    @router.get(
        "/api/monitor/connections/saved/{name}/os-metrics", tags=["Monitoring"],
    )
    def monitor_remote_os(name: str, disk: str = Query("/", description="Remote mount point")):
        if not hasattr(svc, "get_remote_os_metrics"):
            _error("get_remote_os_metrics not supported.", 501)
        if hasattr(svc, "monitor_any"):
            r_any = svc.monitor_any(name, disk_path=disk or "/")
            if r_any.get("error"):
                _error(r_any.get("error") or "Remote OS metrics failed.")
            return {
                "ok": True,
                "source": r_any.get("source", ""),
                "metrics": r_any.get("raw_floats", {}),
                "sections": r_any.get("sections", []),
                "alerts": r_any.get("alerts", []),
                "alert_count": len(r_any.get("alerts", [])),
                "timestamp": r_any.get("timestamp"),
            }
        r = svc.get_remote_os_metrics(name, disk_path=disk or "/")
        if not r.get("ok"):
            _error(r.get("error") or "Remote OS metrics failed.")
        if hasattr(svc, "check_os_alerts"):
            metrics = r.get("metrics") or {}
            r["alerts"] = svc.check_os_alerts(metrics, instance_id=name)
            r["alert_count"] = len(r["alerts"])
        return r

    @router.get("/api/alerts", tags=["Alerts"])
    def alerts_list(
        limit: int = Query(_DEF_ALERTS_LIMIT, ge=1, le=1000),
        severity: str = Query("", description="INFO|WARNING|CRITICAL"),
        source: str = Query("", description="Optional source filter"),
        instance: str = Query("", description="Optional instance filter"),
    ):
        if not hasattr(svc, "list_alerts"):
            _error("list_alerts not supported.", 501)
        if severity and severity.upper() not in ("INFO", "WARNING", "CRITICAL"):
            _error("Invalid severity. Expected INFO|WARNING|CRITICAL.")
        r = svc.list_alerts(
            limit=limit, severity=severity or None,
            source=source or None, instance=instance or None,
        )
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/alerts", tags=["Alerts"], status_code=201)
    def alerts_log(req: AlertLogRequest):
        if not hasattr(svc, "log_alert"):
            _error("log_alert not supported.", 501)
        r = svc.log_alert(
            req.severity, req.message,
            source=req.source or "", instance=req.instance or "",
        )
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.delete("/api/alerts", tags=["Alerts"])
    def alerts_clear(
        severity: str = Query("", description="INFO|WARNING|CRITICAL"),
        source: str = Query(""),
        instance: str = Query(""),
    ):
        if not hasattr(svc, "clear_alerts"):
            _error("clear_alerts not supported.", 501)
        if severity and severity.upper() not in ("INFO", "WARNING", "CRITICAL"):
            _error("Invalid severity. Expected INFO|WARNING|CRITICAL.")
        r = svc.clear_alerts(
            severity=severity or None,
            source=source or None, instance=instance or None,
        )
        if not r["ok"]:
            _error(r.get("message", "Could not clear alerts."))
        return r

    @router.get(
        "/api/monitor/cloud/connections/{name}/rds-endpoint", tags=["Cloud"],
    )
    def cloud_rds_endpoint(name: str):
        if not hasattr(svc, "resolve_rds_endpoint"):
            _error("resolve_rds_endpoint not supported.", 501)
        r = svc.resolve_rds_endpoint(name)
        if not r["ok"]:
            status = 404 if "not found" in r["message"].lower() else 400
            _error(r["message"], status)
        return r

    return router
