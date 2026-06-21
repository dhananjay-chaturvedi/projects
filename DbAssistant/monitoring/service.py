"""
Monitoring service layer — metrics, thresholds, cloud, OS, notifications.

Uses a :class:`CoreDBService` (or compatible object) for DB connections.
Shipped with the ``monitoring/`` module only.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from collections import deque
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from common.headless.db_service import CoreDBService

try:
    from monitoring.db_metric_config import collect_metrics
    from monitoring.threshold_checker import ThresholdChecker
    _MONITORING_OK = True
except Exception:
    _MONITORING_OK = False
    collect_metrics = None
    ThresholdChecker = None


def make_service(core: Optional["CoreDBService"] = None):
    """Core + monitoring composite for module-only CLI/API."""
    from common.headless.composite import composite_service
    from common.headless.db_service import CoreDBService

    core = core or CoreDBService()
    return composite_service(core, MonitorService(core))


def _ssh_common_options(timeout: int) -> list[str]:
    """Return SSH options that preserve host-key verification by default."""
    from monitoring import monitor_config

    policy = (
        monitor_config.get(
            "ssh.connection", "strict_host_key", default="accept-new")
        or "accept-new"
    ).strip()
    if policy.lower() in {"yes", "true"}:
        policy = "yes"
    elif policy.lower() in {"no", "false"}:
        # Retained only as an explicit operator escape hatch for legacy labs.
        policy = "no"
    else:
        policy = "accept-new"
    try:
        from common import paths as _paths

        known_hosts = _paths.dbassistant_home() / "ssh" / "known_hosts"
    except Exception:
        known_hosts = Path.home() / ".dbassistant" / "ssh" / "known_hosts"
    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    return [
        "-o", f"StrictHostKeyChecking={policy}",
        "-o", f"UserKnownHostsFile={known_hosts}",
        "-o", f"ConnectTimeout={timeout}",
    ]


class MonitorService:
    """Headless monitoring API backed by a core connection service."""

    def __init__(self, core: Any, thresholds_path: str | Path | None = None):
        self._core = core
        try:
            self._checker = ThresholdChecker(config_path=thresholds_path)
        except Exception:
            self._checker = None

    def get_metrics(self, name: str) -> dict:
        return self._collect_db_metrics(self._core, name)

    def _collect_db_metrics(self, core, name: str) -> dict:
        """Collect DB metrics for *name* through *core* (the source service).

        Shared by :meth:`get_metrics` (core Connections store) and
        :meth:`get_metrics_monitor_db` (isolated Monitor-tab store).
        """
        if not _MONITORING_OK or collect_metrics is None:
            return {
                "error": "Monitoring module is not installed.",
                "sections": [],
                "raw_floats": {},
                "os_note": "",
                "timestamp": "",
            }
        profile = core.get_connection_profile(name)
        if not profile:
            return {
                "error": f"Connection '{name}' not found.",
                "sections": [],
                "raw_floats": {},
                "os_note": "",
                "timestamp": "",
            }
        host = profile.get("host", "")
        try:
            mgr = core.get_manager(name, profile)
        except Exception as exc:
            return {
                "error": str(exc),
                "sections": [],
                "raw_floats": {},
                "os_note": "",
                "timestamp": "",
            }

        with core.connection_lock(name):
            sections, raw_floats, os_note = collect_metrics(
                mgr, host=host, checker=self._checker
            )

        return {
            "error": None,
            "sections": sections,
            "raw_floats": raw_floats,
            "os_note": os_note,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def resolve_connection_source(self, name: str) -> str | None:
        """Return ``"db" | "cloud" | "monitor"`` for *name*, or ``None``.

        Lookup order matches what users expect from a unified ``monitor``
        command: a Connections-tab DB profile wins over a same-named cloud
        entry (because DB profiles drive ``get_metrics``), cloud wins over
        Monitor-tab SSH entries (because cloud entries usually carry richer
        provider-side telemetry).
        """
        try:
            if self._core.get_connection_profile(name):
                return "db"
        except Exception:
            pass
        try:
            data = self._cloud_mgr().load_cloud_databases()
            if name in data:
                return "cloud"
        except Exception:
            pass
        try:
            if self._monitor_db_core().get_connection_profile(name):
                return "monitor-db"
        except Exception:
            pass
        try:
            if self._monitor_mgr().get_connection(name):
                return "monitor"
        except Exception:
            pass
        return None

    def monitor_any(self, name: str, disk_path: str = "") -> dict:
        """Source-aware metrics dispatch — mirrors the UI's "Monitor" tab.

        Resolves *name* across DB profiles, cloud profiles, and Monitor-tab
        SSH targets, then calls the matching backend. Returns a uniform shape
        the CLI can render without caring which source it came from::

            {
                "error":      str | None,
                "source":     "db" | "cloud" | "monitor" | None,
                "sections":   [(section_name, [(metric, value), ...]), ...],
                "raw_floats": {metric: float, ...},   # populated for db only
                "alerts":     [{"severity", "message"}, ...],
                "timestamp":  "YYYY-MM-DD HH:MM:SS",
                "text":       str,                    # cloud's rendered block
            }
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        empty: dict = {
            "error": None,
            "source": None,
            "sections": [],
            "raw_floats": {},
            "alerts": [],
            "timestamp": ts,
            "text": "",
        }

        source = self.resolve_connection_source(name)
        if source is None:
            return {**empty, "error": f"Connection '{name}' not found."}

        if source in ("db", "monitor-db"):
            if source == "monitor-db":
                r = self.get_metrics_monitor_db(name)
                profile = self._monitor_db_core().get_connection_profile(name) or {}
            else:
                r = self.get_metrics(name)
                profile = self._core.get_connection_profile(name) or {}
            raw = r.get("raw_floats", {}) or {}
            db_alerts = [
                {**a, "source": "db"}
                for a in self.check_alerts(name, raw, profile=profile)
            ]
            return {
                **empty,
                "source": source,
                "error": r.get("error"),
                "sections": r.get("sections", []),
                "raw_floats": raw,
                "timestamp": r.get("timestamp") or ts,
                "alerts": db_alerts,
            }

        if source == "cloud":
            r = self.get_cloud_metrics(name)
            # Provider monitors already emit their own alerts (per-provider
            # source key set inside the provider). We just tag a fallback
            # ``cloud`` source on any alert that didn't already carry one.
            cloud_alerts = []
            for a in (r.get("alerts") or []):
                tagged = dict(a)
                tagged.setdefault("source", "cloud")
                cloud_alerts.append(tagged)
            return {
                **empty,
                "source": "cloud",
                "error": r.get("error"),
                "sections": r.get("sections", []),
                "alerts": cloud_alerts,
                "text": r.get("text", "") or "",
                "timestamp": ts,
            }

        # Monitor-tab SSH target: OS metrics only — feed them straight into
        # the os-source threshold checker.
        r = self.get_remote_os_metrics(name, disk_path=disk_path)
        metrics = r.get("metrics") or {}
        os_alerts = [
            {**a, "source": "os"}
            for a in self.check_os_alerts(metrics, instance_id=name)
        ]
        return {
            **empty,
            "source": "monitor",
            "error": (None if r.get("ok") else (r.get("error") or "")),
            "sections": [("OS (remote)", list(metrics.items()))] if metrics else [],
            "raw_floats": {k: v for k, v in metrics.items() if isinstance(v, (int, float))},
            "alerts": os_alerts,
            "timestamp": ts,
        }

    def _split_raw_by_source(
        self, db_type: str, raw_floats: dict
    ) -> dict[str, dict[str, float]]:
        """Re-key DB-poll ``raw_floats`` (display-name → value) into the
        metric-key buckets the threshold checker expects.

        The collector in :mod:`monitoring.db_metric_config` indexes its
        output by *display name* (``"CPU Utilization"``), but
        ``monitor_thresholds.ini`` uses the *ini metric key*
        (``cpu_utilization``). ``METRIC_SPECS`` carries both, so we walk
        the per-db-type spec list and emit ``{source: {metric_key: value}}``
        for every display name that actually has a numeric value in
        *raw_floats*. Display names not registered in the spec list are
        ignored — same as before.
        """
        out: dict[str, dict[str, float]] = {}
        if not raw_floats:
            return out
        try:
            from monitoring.db_metric_config import METRIC_SPECS  # local import
        except Exception:
            return out
        specs = METRIC_SPECS.get(db_type or "", []) or []
        for spec in specs:
            try:
                src, metric_key = spec["ini"]
                display = spec["display"]
            except Exception:
                continue
            value = raw_floats.get(display)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            out.setdefault(src, {})[metric_key] = float(value)
        return out

    def _resolve_db_profile(self, name: str, profile: dict | None = None) -> dict:
        """Return a connection profile from core or monitor-db stores."""
        if profile:
            return profile
        found = self._core.get_connection_profile(name)
        if found:
            return found
        try:
            return self._monitor_db_core().get_connection_profile(name) or {}
        except Exception:
            return {}

    def check_alerts(
        self,
        name: str,
        raw_floats: dict,
        *,
        profile: dict | None = None,
    ) -> list[dict]:
        """Evaluate ``[metric.db.*]`` rules against a DB-poll raw_floats dict.

        Translates display-name keys to ini metric keys via
        :meth:`_split_raw_by_source` so the checker actually sees its rules.
        Per-engine sections (``[metric.db.mysql.*]``) are tried first with
        fallback to generic ``[metric.db.*]`` defaults.
        """
        if not self._checker or not raw_floats:
            return []
        from monitoring.db_metric_config import db_type_path

        db_type = self._resolve_db_profile(name, profile).get("db_type", "")
        buckets = self._split_raw_by_source(db_type, raw_floats)
        db_bucket = buckets.get("db", {})
        if not db_bucket:
            return []
        results = self._checker.check_many(
            "db",
            db_bucket,
            instance_id=name,
            path=db_type_path(db_type),
            fallback_to_empty=True,
        )
        return [{"severity": r.severity, "message": r.message} for r in results]

    def check_os_alerts(
        self,
        metrics: dict,
        instance_id: str = "",
        *,
        db_type: str | None = None,
    ) -> list[dict]:
        """Run every ``[metric.os.*]`` threshold against *metrics*.

        Two input shapes are supported:

        * Remote-OS / local-OS metrics — already keyed by ini metric key
          (``cpu_utilization``, ``free_memory_mb``, …). Pass *db_type* as
          ``None`` (the default) and we feed them straight through.
        * DB-poll raw_floats — keyed by display name. Pass *db_type* so
          :meth:`_split_raw_by_source` translates display names to
          ``[metric.os.*]`` metric keys before evaluation.

        Non-numeric values in *metrics* are silently skipped by
        ``check_many``.
        """
        if not self._checker or not metrics:
            return []
        if db_type:
            metrics_for_check = self._split_raw_by_source(db_type, metrics).get("os", {})
        else:
            metrics_for_check = metrics
        if not metrics_for_check:
            return []
        results = self._checker.check_many(
            "os", metrics_for_check, instance_id=instance_id or ""
        )
        return [{"severity": r.severity, "message": r.message} for r in results]

    @staticmethod
    def _rule_to_dict(r) -> dict:
        return {
            "source": r.source,
            "api": r.api,
            "path": list(r.path),
            "section": r.section_id,
            "metric": r.metric,
            "metric_name": r.metric_name,
            "namespace": r.namespace,
            "service_type": r.service_type,
            "resource_provider": r.resource_provider,
            "resource_type": r.resource_type,
            "operator": r.operator,
            "unit": r.unit,
            "window": r.window,
            "critical": r.critical,
            "warning": r.warning,
            "info": r.info,
            "enabled": r.enabled,
            "description": r.description,
        }

    def list_thresholds(
        self,
        source: str | None = None,
        *,
        path: list | tuple | None = None,
        api: str | None = None,
        enabled_only: bool = True,
    ) -> list[dict]:
        if not self._checker:
            return []
        rules = self._checker.list_rules(
            source=source, path=path, api=api, enabled_only=enabled_only,
        )
        return [self._rule_to_dict(r) for r in rules]

    def show_threshold(
        self,
        source: str,
        metric: str,
        *,
        path: list | tuple | None = None,
    ) -> dict | None:
        if not self._checker:
            return None
        r = self._checker.get_rule(source, metric, path=path)
        if r is None:
            return None
        return self._rule_to_dict(r)

    def check_threshold(
        self,
        source: str,
        metric: str,
        value: float,
        instance_id: str = "manual",
        *,
        path: list | tuple | None = None,
    ) -> list[dict]:
        if not self._checker:
            return []
        # A manual "evaluate this value" check is stateless: there is no live
        # poll history, so use immediate (window=1) semantics. The sustained
        # N-consecutive-breaches window only applies to the live daemon poll
        # loop (check_db_alerts / check_os_alerts), which omit the override.
        alerts = self._checker.check_many(
            source, {metric: value}, instance_id=instance_id, path=path,
            window_override=1,
        )
        return [{"severity": a.severity, "message": a.message} for a in alerts]

    def update_threshold(
        self,
        source: str,
        metric: str,
        changes: dict,
        *,
        path: list | tuple | None = None,
    ) -> dict:
        """Persist edits to a threshold rule (critical/warning/info/operator/
        window/enabled/description). Comment-preserving write."""
        if not self._checker:
            return {"ok": False, "message": "Monitoring module is not installed."}
        return self._checker.update_rule(source, metric, changes, path=path)

    def set_threshold_enabled(
        self,
        source: str,
        metric: str,
        enabled: bool,
        *,
        path: list | tuple | None = None,
    ) -> dict:
        if not self._checker:
            return {"ok": False, "message": "Monitoring module is not installed."}
        return self._checker.set_enabled(source, metric, enabled, path=path)

    def get_os_metrics(self, disk_path: str = "") -> dict:
        from monitoring.db_os_collector import get_host_metrics
        from monitoring import monitor_config

        if not disk_path:
            disk_path = monitor_config.get(
                "monitoring", "default_disk_path", default="/") or "/"
        try:
            return {"error": None, "metrics": get_host_metrics(disk_path=disk_path)}
        except Exception as exc:
            return {"error": str(exc), "metrics": {}}

    def send_notification(self, severity: str, message: str) -> dict:
        """Manually dispatch a notification to all enabled channels.

        Routed through :func:`common.notifications.dispatch_alert` with
        ``force=True`` so a manual/test send is attempted regardless of the
        master enable switch and severity gate (channel toggles still apply).
        """
        try:
            from common.notifications import dispatch_alert

            result = dispatch_alert(
                f"[{severity.upper()}] {message}", severity=severity, force=True
            )
            if result.get("delivered"):
                return {"ok": True,
                        "message": "Notification delivered via "
                                   + ", ".join(result["delivered"]) + "."}
            if result.get("skipped"):
                return {"ok": False, "message": f"Not sent: {result['skipped']}."}
            # Surface the first channel error if any.
            errs = [r.get("message", "") for r in result.get("results", [])
                    if not r.get("ok")]
            return {"ok": bool(result.get("ok")),
                    "message": "; ".join(errs) or "Notification dispatched."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    # ------------------------------------------------------------------
    # Module-owned configuration (monitoring/monitor_config.ini)
    # ------------------------------------------------------------------
    def get_monitor_config(self) -> dict:
        """Return all monitor_config.ini sections/keys with current values."""
        from monitoring import monitor_config

        out: dict[str, dict] = {}
        for section, keys in monitor_config.DEFAULTS.items():
            out[section] = {k: monitor_config.get(section, k, default=v)
                            for k, v in keys.items()}
        return {
            "ok": True,
            "config": out,
            "path": str(monitor_config.config_path() or monitor_config.live_path()),
            "live": monitor_config.live_path().exists(),
        }

    def set_monitor_config(self, section: str, key: str, value: str) -> dict:
        """Validate + persist one monitor_config.ini value. Notification keys are
        routed through the notification validator."""
        from monitoring import monitor_config

        if section == "notifications":
            from common.notifications import set_config_value
            return set_config_value(key, value)

        defaults = monitor_config.DEFAULTS.get(section)
        if defaults is None or key not in defaults:
            return {"ok": False,
                    "message": f"Unknown monitor setting '{section}.{key}'."}
        # Numeric keys must parse as int (all current non-notification keys are ints).
        raw = "" if value is None else str(value).strip()
        try:
            int(raw)
        except ValueError:
            return {"ok": False,
                    "message": f"{section}.{key} must be an integer."}
        try:
            monitor_config.set_value(section, key, raw)
        except Exception as exc:
            return {"ok": False, "message": f"Failed to save {section}.{key}: {exc}"}
        return {"ok": True, "message": f"{section}.{key} saved."}

    def restore_monitor_config(self) -> dict:
        from monitoring import monitor_config

        try:
            monitor_config.restore_defaults()
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
        return {"ok": True, "message": "monitor_config.ini restored to defaults."}

    def get_notification_config(self) -> dict:
        """Notification config + which secrets are set (never the values)."""
        from common.notifications import status_dict

        return {"ok": True, **status_dict()}

    def set_notification_config(self, key: str, value: str) -> dict:
        from common.notifications import set_config_value

        return set_config_value(key, value)

    def set_notification_secret(self, key: str, value: str) -> dict:
        from common.notifications import NotificationSecretStore

        if key not in ("teams_webhook_url", "smtp_password"):
            return {"ok": False, "message": f"Unknown notification secret '{key}'."}
        ok = NotificationSecretStore().set(key, "" if value is None else str(value))
        if not ok:
            return {"ok": False, "message": f"Failed to store secret '{key}'."}
        action = "cleared" if not value else "stored (encrypted)"
        return {"ok": True, "message": f"{key} {action}."}

    def _cloud_mgr(self):
        from common.cloud.connection_manager import CloudConnectionManager

        return CloudConnectionManager()

    def _monitor_mgr(self):
        """SSH/host monitor connections saved from the Monitor tab."""
        from monitoring.monitor_connection_manager import MonitorConnectionManager

        return MonitorConnectionManager()

    def _monitor_db_core(self):
        """A CoreDBService backed by the isolated Monitor-tab DB store.

        Reuses all of CoreDBService's connection/test/metrics logic against
        ``monitor_db.json`` so Monitor-tab DB profiles get the same treatment
        as core ones — without duplicating any of it. Cached per service.
        """
        core = getattr(self, "_mdb_core", None)
        if core is None:
            from common.headless.db_service import CoreDBService
            from monitoring.monitor_db_connection_manager import (
                MonitorDBConnectionManager,
            )

            core = CoreDBService(connection_manager=MonitorDBConnectionManager())
            self._mdb_core = core
        return core

    # ------------------------------------------------------------------ #
    # Monitor-tab-only DB connections (isolated from the Connections tab)
    # ------------------------------------------------------------------ #
    def list_monitor_db_connections(self) -> list[dict]:
        """Saved DB profiles owned by the Monitor tab (passwords omitted)."""
        try:
            return self._monitor_db_core().list_connections()
        except Exception as exc:
            return [{"error": str(exc)}]

    def add_monitor_db_connection(
        self,
        params=None,
        **legacy_fields,
    ) -> dict:
        """Save a Monitor-tab-only DB connection. Returns ``{ok, message}``.

        Pass *ssh_tunnel* (``ssh_host``/``ssh_user`` plus optional
        ``ssh_port``/``ssh_password``/``ssh_key_file``) to monitor a remote
        database reached through an SSH tunnel, mirroring the Connections tab.
        """
        try:
            from common.connection_params import ConnectionParams

            if not isinstance(params, ConnectionParams):
                legacy_fields = dict(legacy_fields)
                if params is not None:
                    legacy_fields.setdefault("name", params)
                params = ConnectionParams.from_mapping(legacy_fields)
            return self._monitor_db_core().add_connection(
                params,
            )
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def remove_monitor_db_connection(self, name: str) -> dict:
        """Delete a Monitor-tab-only DB connection. Returns ``{ok, message}``."""
        try:
            return self._monitor_db_core().remove_connection(name)
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def test_monitor_db_connection(self, name: str) -> dict:
        """Test a Monitor-tab-only DB connection."""
        try:
            return self._monitor_db_core().test_connection(name)
        except Exception as exc:
            return {"ok": False, "latency_ms": None, "version": None,
                    "message": str(exc)}

    def get_metrics_monitor_db(self, name: str) -> dict:
        """DB metrics for a Monitor-tab-only connection (same shape as
        :meth:`get_metrics`)."""
        return self._collect_db_metrics(self._monitor_db_core(), name)

    def test_monitor_ssh(self, name: str) -> dict:
        """SSH-ping a saved Monitor-tab host. Returns ``{ok, message}``.

        Uses ``BatchMode=yes`` when no password is stored (key/agent based),
        falling back to ``sshpass`` when a password is set. Always honours
        ``ssh.connection.ssh_test_timeout`` from monitor_config.ini.
        """
        import shutil
        import subprocess
        from monitoring import monitor_config

        try:
            profile = self._monitor_mgr().get_connection(name)
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
        if not profile:
            return {"ok": False, "message": f"Monitor connection '{name}' not found."}

        host = profile.get("host", "")
        user = profile.get("username", "")
        pwd = profile.get("password") or ""
        if not host or not user:
            return {
                "ok": False,
                "message": f"Monitor connection '{name}' is missing host or username.",
            }

        timeout = monitor_config.get_int("ssh.connection", "ssh_test_timeout", default=5)
        target = f"{user}@{host}"
        common_opts = _ssh_common_options(timeout)
        env = None
        if pwd:
            if not shutil.which("sshpass"):
                return {
                    "ok": False,
                    "message": (
                        "Password-based SSH test requires 'sshpass' "
                        "(brew install sshpass / apt-get install sshpass)."
                    ),
                }
            env = {**os.environ, "SSHPASS": pwd}
            cmd = ["sshpass", "-e", "ssh", *common_opts, target, "true"]
        else:
            cmd = ["ssh", "-o", "BatchMode=yes", *common_opts, target, "true"]

        _pad = monitor_config.get_int(
            "ssh.connection", "ssh_test_timeout_padding", default=5)
        try:
            r = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout + _pad,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "message": f"SSH timed out after {timeout}s."}
        except FileNotFoundError as exc:
            return {"ok": False, "message": str(exc)}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

        if r.returncode == 0:
            return {"ok": True, "message": f"SSH reachable: {target}"}
        tail = ((r.stderr or r.stdout) or "").strip().splitlines()
        head = tail[-1] if tail else f"exit {r.returncode}"
        return {"ok": False, "message": f"SSH failed: {head}"}

    def list_all_connections(self, source: str = "all") -> list[dict]:
        """Unified view of every monitor-eligible saved connection.

        ``source`` selects ``all`` (default), ``db`` (Connections-tab DB
        profiles — usable for ``get_metrics``), ``monitor`` (SSH / host targets
        saved from the Monitor tab — usable for OS metrics over SSH) or
        ``cloud`` (cloud DB profiles — usable for ``get_cloud_metrics``).

        Each row carries a ``source`` field so callers / the CLI can tell the
        three lists apart and route follow-up actions to the right endpoint.
        Sensitive fields are never returned.
        """
        wanted = (source or "all").strip().lower()
        rows: list[dict] = []

        if wanted in ("all", "db"):
            try:
                for c in self._core.list_connections():
                    rows.append({
                        "source": "db",
                        "name": c.get("name", ""),
                        "kind": c.get("db_type", ""),
                        "host": c.get("host", ""),
                        "port": c.get("port", ""),
                        "database": c.get("service_or_db", ""),
                        "username": c.get("username", ""),
                    })
            except Exception as exc:
                rows.append({"source": "db", "error": str(exc)})

        if wanted in ("all", "monitor-db"):
            try:
                for c in self._monitor_db_core().list_connections():
                    rows.append({
                        "source": "monitor-db",
                        "name": c.get("name", ""),
                        "kind": c.get("db_type", ""),
                        "host": c.get("host", ""),
                        "port": c.get("port", ""),
                        "database": c.get("service_or_db", ""),
                        "username": c.get("username", ""),
                    })
            except Exception as exc:
                rows.append({"source": "monitor-db", "error": str(exc)})

        if wanted in ("all", "monitor"):
            try:
                for c in self._monitor_mgr().get_all_connections():
                    rows.append({
                        "source": "monitor",
                        "name": c.get("name", ""),
                        "kind": c.get("target_type") or "vm",
                        "host": c.get("host", ""),
                        "username": c.get("username", ""),
                    })
            except Exception as exc:
                rows.append({"source": "monitor", "error": str(exc)})

        if wanted in ("all", "cloud"):
            try:
                from common.cloud.connection_manager import _SENSITIVE_FIELDS

                data = self._cloud_mgr().load_cloud_databases()
                for name, profile in data.items():
                    sql = (profile or {}).get("sql_connection") or {}
                    rows.append({
                        "source": "cloud",
                        "name": name,
                        "kind": profile.get("provider", ""),
                        "host": sql.get("host", ""),
                        "port": sql.get("port", ""),
                        "database": sql.get("service_or_db", ""),
                        "username": sql.get("username", ""),
                        "region": profile.get("region", ""),
                        "resource": profile.get("resource_name", "")
                                    or profile.get("instance_id", ""),
                    })
                _ = _SENSITIVE_FIELDS  # imported only to verify cloud module is present
            except Exception as exc:
                rows.append({"source": "cloud", "error": str(exc)})

        return rows

    def list_cloud_connections(self) -> list[dict]:
        try:
            data = self._cloud_mgr().load_cloud_databases()
        except Exception as exc:
            return [{"error": str(exc)}]
        from common.cloud.connection_manager import _SENSITIVE_FIELDS

        def _scrub(value):
            if isinstance(value, dict):
                return {
                    k: ("***" if k in _SENSITIVE_FIELDS and v else _scrub(v))
                    for k, v in value.items()
                }
            if isinstance(value, list):
                return [_scrub(item) for item in value]
            return value

        out: list[dict] = []
        for name, profile in data.items():
            safe = {"name": name}
            for k, v in profile.items():
                safe[k] = "***" if k in _SENSITIVE_FIELDS and v else _scrub(v)
            out.append(safe)
        return out

    def add_cloud_connection(self, name: str, profile: dict) -> dict:
        try:
            cm = self._cloud_mgr()
            data = cm.load_cloud_databases()
            if name in data:
                return {
                    "ok": False,
                    "message": f"Cloud connection '{name}' already exists.",
                }
            data[name] = dict(profile)
            if cm.save_cloud_databases(data):
                return {"ok": True, "message": f"Cloud connection '{name}' saved."}
            return {"ok": False, "message": f"Failed to save cloud connection '{name}'."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def remove_cloud_connection(self, name: str) -> dict:
        try:
            cm = self._cloud_mgr()
            data = cm.load_cloud_databases()
            if name not in data:
                return {"ok": False, "message": f"Cloud connection '{name}' not found."}
            del data[name]
            if cm.save_cloud_databases(data):
                return {"ok": True, "message": f"Cloud connection '{name}' removed."}
            return {"ok": False, "message": f"Failed to remove cloud connection '{name}'."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def test_cloud_connection(self, name: str) -> dict:
        try:
            from monitoring.cloud_provider_registry import CloudProviderRegistry

            data = self._cloud_mgr().load_cloud_databases()
            if name not in data:
                return {"ok": False, "message": f"Cloud connection '{name}' not found."}
            entry = data[name]
            monitor, err = CloudProviderRegistry.build_monitor(entry)
            if err:
                return {"ok": False, "message": err}
            errors = monitor.check_health() if monitor else ["No monitor"]
            if errors:
                return {"ok": False, "message": "; ".join(errors)}
            return {"ok": True, "message": f"Cloud connection '{name}' healthy."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    @staticmethod
    def _format_cloud_sections(name: str, entry: dict, sections: list) -> str:
        width = 64
        sep = "=" * width
        lines = [sep, f" Cloud DB  : {name}"]
        provider = entry.get("provider", "")
        resource = entry.get("resource_name", "") or entry.get("instance_id", "")
        if provider:
            lines.append(f" Provider  : {provider}")
        if resource:
            lines.append(f" Resource  : {resource}")
        lines.append(sep)
        for title, items in sections or []:
            if not items:
                continue
            lines.append(f"\n  - {title} " + "-" * max(0, width - len(title) - 5))
            for metric, val in items:
                lines.append(f"    {str(metric):<32}  {val}")
        if len(lines) <= 4:
            lines.append("\n  (no metric data returned)")
        return "\n".join(lines)

    def cloud_login(self, name: str) -> dict:
        try:
            from monitoring.cloud_provider_registry import CloudProviderRegistry

            data = self._cloud_mgr().load_cloud_databases()
            if name not in data:
                return {"ok": False, "message": f"Cloud connection '{name}' not found."}
            ok, msg = CloudProviderRegistry.login(data[name])
            return {"ok": bool(ok), "message": msg}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    # ------------------------------------------------------------------
    # Parity additions (Phase 5) — Monitor SSH CRUD, remote OS over SSH,
    # alerts log persistence, RDS endpoint resolver.
    # ------------------------------------------------------------------

    @staticmethod
    def _scrub_monitor_profile(profile: dict) -> dict:
        out = {k: v for k, v in (profile or {}).items() if k != "password"}
        out["has_password"] = bool((profile or {}).get("password"))
        return out

    def list_monitor_connections(self) -> list[dict]:
        """List Monitor-tab SSH targets, with passwords scrubbed."""
        try:
            return [self._scrub_monitor_profile(c)
                    for c in self._monitor_mgr().get_all_connections()]
        except Exception as exc:
            return [{"error": str(exc)}]

    def get_monitor_connection(self, name: str) -> dict | None:
        """One Monitor-tab SSH target, scrubbed."""
        try:
            c = self._monitor_mgr().get_connection(name)
            return self._scrub_monitor_profile(c) if c else None
        except Exception as exc:
            return {"error": str(exc)}

    def add_monitor_connection(
        self,
        name: str,
        host: str,
        username: str,
        password: str = "",
        target_type: str = "vm",
    ) -> dict:
        try:
            ok, msg = self._monitor_mgr().add_connection(
                name=name, host=host, username=username,
                password=password or None, target_type=target_type or "vm",
            )
            return {"ok": bool(ok), "message": msg}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def update_monitor_connection(
        self,
        old_name: str,
        name: str,
        host: str,
        username: str,
        password: str = "",
        target_type: str | None = None,
    ) -> dict:
        try:
            ok, msg = self._monitor_mgr().update_connection(
                old_name=old_name, name=name, host=host, username=username,
                password=password or None, target_type=target_type,
            )
            return {"ok": bool(ok), "message": msg}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def remove_monitor_connection(self, name: str) -> dict:
        try:
            ok, msg = self._monitor_mgr().delete_connection(name)
            return {"ok": bool(ok), "message": msg}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    # -- Remote OS over SSH ---------------------------------------------

    @staticmethod
    def _build_remote_os_script(disk_path: str) -> str:
        """Cross-platform shell script that emits ``key=value`` lines.

        Designed to be piped to ``bash -s`` on the remote host (NOT passed as
        a quoted argument), so embedded awk patterns survive without
        multi-layer escaping. Linux uses /proc; macOS/BSD falls back to
        ``vm_stat`` / ``sysctl`` / ``uptime``. Both branches emit identical
        metric keys so the caller doesn't have to special-case the platform.
        """
        # POSIX single-quote escape for the disk arg embedded into the script.
        disk = (disk_path or "/").replace("'", "'\\''")
        return f"""set -u
uname_s=$(uname -s 2>/dev/null || echo Unknown)
uname_a=$(uname -a 2>/dev/null || true)
[ -n "$uname_a" ] && printf 'uname=%s\\n' "$uname_a"
printf 'uname_s=%s\\n' "$uname_s"

if [ -r /proc/stat ] && [ -r /proc/meminfo ]; then
  # ----- Linux -----
  cpu_idle=$(awk '/^cpu /{{print $5/($2+$3+$4+$5+$6+$7+$8+$9+$10)*100}}' /proc/stat 2>/dev/null || echo '')
  if [ -n "$cpu_idle" ]; then
    printf 'cpu_idle_percent=%s\\n' "$cpu_idle"
    awk -v i="$cpu_idle" 'BEGIN{{printf "cpu_utilization=%.2f\\n", 100-i}}'
  fi
  mem_total=$(awk '/^MemTotal:/ {{print $2}}' /proc/meminfo 2>/dev/null)
  mem_avail=$(awk '/^MemAvailable:/ {{print $2}}' /proc/meminfo 2>/dev/null)
  if [ -n "$mem_total" ] && [ -n "$mem_avail" ]; then
    awk -v t="$mem_total" -v a="$mem_avail" 'BEGIN{{
      printf "free_memory_mb=%.2f\\n", a/1024
      printf "memory_utilization=%.2f\\n", (t-a)/t*100
    }}'
  fi
  if [ -r /proc/loadavg ]; then
    awk '{{print "load_avg_1m="$1; print "load_avg_5m="$2; print "load_avg_15m="$3}}' /proc/loadavg
  fi
elif [ "$uname_s" = "Darwin" ] || [ "$uname_s" = "FreeBSD" ]; then
  # ----- macOS / BSD -----
  cpu_idle=$(top -l 1 -n 0 2>/dev/null | awk '/CPU usage/{{
    for (i=1;i<=NF;i++) if ($i ~ /idle/) {{ v=$(i-1); gsub("%","",v); print v }}
  }}')
  if [ -n "$cpu_idle" ]; then
    printf 'cpu_idle_percent=%s\\n' "$cpu_idle"
    awk -v i="$cpu_idle" 'BEGIN{{printf "cpu_utilization=%.2f\\n", 100-i}}'
  fi
  page=$(sysctl -n hw.pagesize 2>/dev/null || echo 4096)
  mem_total=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
  vm=$(vm_stat 2>/dev/null || true)
  if [ -n "$mem_total" ] && [ "$mem_total" -gt 0 ] && [ -n "$vm" ]; then
    free_pg=$(printf '%s\\n' "$vm" | awk -F'[ .]+' '/Pages free/ {{print $3}}')
    inactive_pg=$(printf '%s\\n' "$vm" | awk -F'[ .]+' '/Pages inactive/ {{print $3}}')
    spec_pg=$(printf '%s\\n' "$vm" | awk -F'[ .]+' '/Pages speculative/ {{print $3}}')
    [ -z "$free_pg" ] && free_pg=0
    [ -z "$inactive_pg" ] && inactive_pg=0
    [ -z "$spec_pg" ] && spec_pg=0
    awk -v f="$free_pg" -v ina="$inactive_pg" -v spc="$spec_pg" \\
        -v ps="$page" -v t="$mem_total" 'BEGIN{{
      avail=(f+ina+spc)*ps
      printf "free_memory_mb=%.2f\\n", avail/1024/1024
      printf "memory_utilization=%.2f\\n", (t-avail)/t*100
    }}'
  fi
  ld=$(uptime 2>/dev/null | sed -E 's/.*load averages?:[[:space:]]*//' | tr ',' ' ' || true)
  if [ -n "$ld" ]; then
    set -- $ld
    [ -n "${{1:-}}" ] && printf 'load_avg_1m=%s\\n' "$1"
    [ -n "${{2:-}}" ] && printf 'load_avg_5m=%s\\n' "$2"
    [ -n "${{3:-}}" ] && printf 'load_avg_15m=%s\\n' "$3"
  fi
fi

# ----- Disk usage (POSIX df is portable) -----
df -P -k '{disk}' 2>/dev/null | awk 'NR==2{{
  used=$5; gsub("%","",used)
  printf "free_disk_gb=%.2f\\n", $4/1024/1024
  printf "disk_utilization=%.2f\\n", used
}}'
"""

    @staticmethod
    def _parse_remote_os_output(blob: str) -> dict[str, float | str]:
        """Convert the structured shell output emitted by
        :meth:`get_remote_os_metrics` into a flat dict of numeric metrics.

        The remote script prints ``key=value`` lines. Numeric values are
        coerced to float; everything else is preserved as a string.
        """
        out: dict[str, float | str] = {}
        for line in (blob or "").splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
        return out

    def get_remote_os_metrics(self, name: str, disk_path: str = "") -> dict:
        """SSH into a saved Monitor-tab target and collect cpu/mem/disk/load.

        Mirrors the UI's "OS over SSH" metrics view: small portable shell
        snippet that works on Linux hosts. Returns ``{ok, error, metrics}``.
        Reuses :meth:`test_monitor_ssh`'s connection-string/timeout choices.
        """
        import shutil
        import subprocess
        from monitoring import monitor_config

        try:
            profile = self._monitor_mgr().get_connection(name)
        except Exception as exc:
            return {"ok": False, "error": str(exc), "metrics": {}}
        if not profile:
            return {
                "ok": False,
                "error": f"Monitor connection '{name}' not found.",
                "metrics": {},
            }

        host = profile.get("host", "")
        user = profile.get("username", "")
        pwd = profile.get("password") or ""
        if not host or not user:
            return {
                "ok": False,
                "error": f"Monitor connection '{name}' is missing host or username.",
                "metrics": {},
            }

        timeout = monitor_config.get_int("ssh.connection", "ssh_test_timeout", default=5)
        target = f"{user}@{host}"
        common_opts = _ssh_common_options(timeout)
        env = None
        _default_disk = monitor_config.get(
            "monitoring", "default_disk_path", default="/") or "/"
        remote_script = self._build_remote_os_script(disk_path or _default_disk)

        # We pipe the script over stdin to `bash -s` on the remote so embedded
        # awk patterns aren't mangled by another layer of shell quoting.
        if pwd:
            if not shutil.which("sshpass"):
                return {
                    "ok": False,
                    "error": (
                        "Password-based SSH requires 'sshpass' "
                        "(brew install sshpass / apt-get install sshpass)."
                    ),
                    "metrics": {},
                }
            env = {**os.environ, "SSHPASS": pwd}
            cmd = ["sshpass", "-e", "ssh", *common_opts, target, "bash -s"]
        else:
            cmd = ["ssh", "-o", "BatchMode=yes", *common_opts, target, "bash -s"]

        _pad = monitor_config.get_int(
            "ssh.connection", "ssh_monitoring_timeout_padding", default=10)
        try:
            r = subprocess.run(
                cmd,
                input=remote_script,
                capture_output=True,
                text=True,
                timeout=timeout + _pad,
                env=env,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "error": f"SSH timed out after {timeout}s.",
                "metrics": {},
            }
        except Exception as exc:
            return {"ok": False, "error": str(exc), "metrics": {}}

        if r.returncode != 0:
            tail = ((r.stderr or r.stdout) or "").strip().splitlines()
            head = tail[-1] if tail else f"exit {r.returncode}"
            return {"ok": False, "error": f"SSH failed: {head}", "metrics": {}}

        metrics = self._parse_remote_os_output(r.stdout or "")
        return {"ok": True, "error": None, "metrics": metrics}

    # -- Alerts log -----------------------------------------------------

    def _alerts_log_path(self):
        """Resolve the persistent alerts log path.

        Honours ``paths.alerts_log_file`` when set; otherwise falls back
        to the resolver in :mod:`common.paths` (default
        ``<DBASSISTANT_HOME>/runtime/alerts.jsonl``, overridable via
        ``[paths] runtime_dir`` in ``config.ini``).
        """
        from common import paths as _paths
        from common.config_loader import config as _cfg

        try:
            base = _cfg.get_path_or_none("paths", "alerts_log_file")
            if base is not None:
                return base
        except Exception:
            pass
        return _paths.alerts_log_path()

    def log_alert(
        self,
        severity: str,
        message: str,
        *,
        source: str = "",
        instance: str = "",
    ) -> dict:
        """Append one alert record to the persistent alerts log."""
        import json
        from datetime import datetime

        sev = (severity or "INFO").upper()
        if sev not in ("INFO", "WARNING", "CRITICAL"):
            return {"ok": False, "message": f"Invalid severity '{severity}'."}
        record = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "severity": sev,
            "source": source or "",
            "instance": instance or "",
            "message": message or "",
        }
        try:
            path = self._alerts_log_path()
            from common.concurrency import append_jsonl_locked

            append_jsonl_locked(path, [record])
            return {"ok": True, "message": "Alert recorded.", "record": record}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def list_alerts(
        self,
        *,
        limit: int | None = None,
        severity: str | None = None,
        source: str | None = None,
        instance: str | None = None,
    ) -> dict:
        """Read the alerts log (newest first). Optional filters narrow down."""
        import json
        from monitoring import monitor_config

        if limit is None:
            limit = monitor_config.get_int(
                "monitoring.limits", "alerts_service_default_limit", default=100)
        path = self._alerts_log_path()
        if not path.exists():
            return {"alerts": [], "total": 0, "path": str(path)}
        sev_filter = (severity or "").upper().strip()
        src_filter = (source or "").strip().lower()
        inst_filter = (instance or "").strip().lower()
        max_items = max(0, int(limit or 0)) if limit is not None else 0
        out = deque(maxlen=max_items or None)
        try:
            from common.concurrency import file_lock

            with file_lock(path, shared=True):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except Exception:
                            continue
                        if sev_filter and rec.get("severity", "").upper() != sev_filter:
                            continue
                        if src_filter and (rec.get("source") or "").lower() != src_filter:
                            continue
                        if inst_filter and (rec.get("instance") or "").lower() != inst_filter:
                            continue
                        out.append(rec)
        except Exception as exc:
            return {"alerts": [], "total": 0, "error": str(exc), "path": str(path)}
        alerts = list(out)
        alerts.reverse()
        return {"alerts": alerts, "total": len(alerts), "path": str(path)}

    def clear_alerts(
        self,
        *,
        severity: str | None = None,
        source: str | None = None,
        instance: str | None = None,
    ) -> dict:
        """Remove matching alerts from the log. With no filter, truncate all."""
        import json

        path = self._alerts_log_path()
        if not path.exists():
            return {"ok": True, "removed": 0, "kept": 0, "path": str(path)}
        sev_filter = (severity or "").upper().strip()
        src_filter = (source or "").strip().lower()
        inst_filter = (instance or "").strip().lower()
        keep: list[dict] = []
        removed = 0
        try:
            from common.concurrency import atomic_write_text, file_lock

            # Hold the sidecar lock across read→filter→atomic replace so
            # concurrent appenders never write to a soon-to-be-replaced inode.
            with file_lock(path):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except Exception:
                            continue
                        # Decide whether THIS record matches the deletion filter.
                        match = True
                        if sev_filter and rec.get("severity", "").upper() != sev_filter:
                            match = False
                        if src_filter and (rec.get("source") or "").lower() != src_filter:
                            match = False
                        if inst_filter and (rec.get("instance") or "").lower() != inst_filter:
                            match = False
                        if match:
                            removed += 1
                        else:
                            keep.append(rec)
                text = "".join(json.dumps(rec) + "\n" for rec in keep)
                atomic_write_text(path, text, lock=False)
            return {
                "ok": True, "removed": removed, "kept": len(keep), "path": str(path),
            }
        except Exception as exc:
            return {"ok": False, "removed": 0, "kept": 0, "message": str(exc)}

    # -- RDS endpoint resolver ------------------------------------------

    def resolve_rds_endpoint(self, name: str) -> dict:
        """Resolve an AWS RDS endpoint for a saved cloud connection.

        Returns ``{ok, host, port, db_type, message}``. Wraps the existing
        :func:`common.cloud.sql_bridge.resolve_aws_rds_sql_endpoint` so callers
        from CLI/API don't need to import the boto3 plumbing.
        """
        try:
            data = self._cloud_mgr().load_cloud_databases()
        except Exception as exc:
            return {"ok": False, "host": "", "port": "", "db_type": "",
                    "message": str(exc)}
        entry = data.get(name)
        if not entry:
            return {"ok": False, "host": "", "port": "", "db_type": "",
                    "message": f"Cloud connection '{name}' not found."}
        provider = (entry.get("provider") or "").upper()
        if provider != "AWS":
            return {"ok": False, "host": "", "port": "", "db_type": "",
                    "message": f"Provider '{provider or 'unknown'}' is not AWS; "
                               "RDS endpoint resolution is AWS-only."}
        try:
            from common.cloud.sql_bridge import resolve_aws_rds_sql_endpoint

            resolved = resolve_aws_rds_sql_endpoint(entry)
        except Exception as exc:
            return {"ok": False, "host": "", "port": "", "db_type": "",
                    "message": str(exc)}
        if not resolved:
            return {"ok": False, "host": "", "port": "", "db_type": "",
                    "message": (
                        f"Could not resolve RDS endpoint for '{name}'. "
                        "Verify the resource_name (DBInstanceIdentifier), region "
                        "and AWS credentials in the profile."
                    )}
        return {
            "ok": True,
            "host": resolved.get("host", ""),
            "port": resolved.get("port", ""),
            "db_type": resolved.get("db_type", ""),
            "message": "Resolved.",
        }

    def get_cloud_metrics(self, name: str) -> dict:
        try:
            from monitoring.cloud_provider_registry import CloudProviderRegistry

            data = self._cloud_mgr().load_cloud_databases()
            if name not in data:
                return {
                    "error": f"Cloud connection '{name}' not found.",
                    "text": "",
                    "sections": [],
                    "graphs": {},
                    "alerts": [],
                }
            entry = data[name]
            monitor, err = CloudProviderRegistry.build_monitor(entry)
            if err:
                return {
                    "error": err,
                    "text": "",
                    "sections": [],
                    "graphs": {},
                    "alerts": [],
                }
            sections, graphs, alerts = CloudProviderRegistry.fetch_metrics(
                name, entry, monitor, threshold_checker=self._checker
            )
            return {
                "error": None,
                "text": self._format_cloud_sections(name, entry, sections),
                "sections": sections,
                "graphs": graphs,
                "alerts": [
                    {
                        "severity": getattr(a, "severity", "INFO"),
                        "message": getattr(a, "message", str(a)),
                    }
                    for a in alerts or []
                ],
            }
        except Exception as exc:
            return {
                "error": str(exc),
                "text": "",
                "sections": [],
                "graphs": {},
                "alerts": [],
            }
