import os
import time
from .cloud_monitor_base import CloudDBMonitor  # noqa: F401
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# GCP Monitoring
from googleapiclient import discovery
from google.cloud import monitoring_v3

load_dotenv()

from .send_notification import send_alert, setup_logger
from .threshold_checker import ThresholdChecker
from . import monitor_config


def _standalone_poll_interval() -> int:
    return max(
        1,
        monitor_config.get_int("monitoring", "standalone_poll_interval", default=10),
    )


def _summarise_gcp_api_error(exc: Exception) -> str:
    """Turn a googleapiclient/google-cloud exception into a short, actionable
    status string.  Used by the monitoring + logging helpers so the UI can
    show "Logs unavailable: Cloud Logging API needs roles/logging.viewer"
    instead of dumping a raw stack trace."""
    msg = str(exc)
    lower = msg.lower()

    # googleapiclient.errors.HttpError carries a .resp.status integer.
    status = getattr(getattr(exc, "resp", None), "status", None)
    reason = ""
    try:
        import json as _json
        content = getattr(exc, "content", b"")
        if content:
            data = _json.loads(content.decode("utf-8", "ignore"))
            reason = (
                data.get("error", {}).get("status")
                or data.get("error", {}).get("message")
                or ""
            )
    except Exception:
        reason = ""

    if status == 403 or "permission_denied" in lower or "permission denied" in lower:
        return (
            "Permission denied (HTTP 403). Grant the calling identity the "
            "required IAM role on the project (e.g. roles/logging.viewer for "
            "logs, roles/monitoring.viewer for metrics, roles/cloudsql.viewer "
            "for instance details)."
        )
    if status == 404 or "not found" in lower:
        return "Not found (HTTP 404). Verify the project and resource name."
    if (
        status == 400
        and ("api has not been used" in lower or "service_disabled" in lower)
    ) or "has not been used in project" in lower:
        return (
            "Required Google API is disabled for this project. Enable it from "
            "GCP Console → APIs & Services → Library."
        )
    if status == 401 or "unauthenticated" in lower:
        return (
            "Authentication failed (HTTP 401). Refresh your credentials and "
            "re-authenticate."
        )

    short = reason or msg
    return short[:220]


# ==========================================
# 2. GCP MONITOR
# ==========================================
class GCPMonitor(CloudDBMonitor):
    def __init__(self, project_id):
        self.project_id = project_id
        # SQL Admin API used for instance enumeration
        self.service = discovery.build("sqladmin", "v1beta4")
        # Monitoring client for metrics
        self.metric_client = monitoring_v3.MetricServiceClient()

    def check_health(self):
        errors = []
        try:
            request = self.service.instances().list(project=self.project_id)
            response = request.execute()
            for instance in response.get("items", []):
                state = instance.get("state")
                if state != "RUNNABLE":
                    errors.append(f"[GCP] Instance '{instance.get('name')}' is {state}")
        except Exception as e:
            errors.append(f"[GCP] Connection Error: {str(e)}")
        return errors

    def _project_name(self):
        return f"projects/{self.project_id}"

    def get_instance_summary(self, resource_id: str) -> dict:
        """Return basic Cloud SQL instance details via Cloud SQL Admin API."""
        if not resource_id:
            return {}
        request = self.service.instances().get(
            project=self.project_id,
            instance=resource_id,
        )
        # Silence googleapiclient.http's automatic WARNING on non-2xx; we
        # convert the exception to a structured error in the caller.
        import logging as _logging
        _http_logger = _logging.getLogger("googleapiclient.http")
        _prev_level = _http_logger.level
        _http_logger.setLevel(_logging.ERROR)
        try:
            inst = request.execute()
        finally:
            _http_logger.setLevel(_prev_level)
        settings = inst.get("settings", {}) or {}
        ip_addresses = inst.get("ipAddresses", []) or []
        return {
            "name": inst.get("name", resource_id),
            "state": inst.get("state", ""),
            "database_version": inst.get("databaseVersion", ""),
            "region": inst.get("region", ""),
            "tier": settings.get("tier", ""),
            "availability_type": settings.get("availabilityType", ""),
            "disk_size_gb": settings.get("dataDiskSizeGb", ""),
            "ip_addresses": ", ".join(
                ip.get("ipAddress", "") for ip in ip_addresses if ip.get("ipAddress")
            ),
        }

    # Cloud SQL System Insights metric types exposed through Cloud Monitoring.
    # Mapping kept here for callers that still pass friendly names; new callers
    # should drive the fetch from monitor_thresholds.ini ``metric_name`` fields
    # and call :meth:`get_metrics_by_type` directly.
    GCP_METRIC_MAP: dict[str, str] = {
        "cpu_utilization": "cloudsql.googleapis.com/database/cpu/utilization",
        "cpu_reserved_cores": "cloudsql.googleapis.com/database/cpu/reserved_cores",
        "memory_utilization": "cloudsql.googleapis.com/database/memory/utilization",
        "memory_usage": "cloudsql.googleapis.com/database/memory/usage",
        "memory_quota": "cloudsql.googleapis.com/database/memory/quota",
        "disk_utilization": "cloudsql.googleapis.com/database/disk/utilization",
        "disk_bytes_used": "cloudsql.googleapis.com/database/disk/bytes_used",
        "disk_quota": "cloudsql.googleapis.com/database/disk/quota",
        "database_connections": "cloudsql.googleapis.com/database/network/connections",
        "io_read_ops": "cloudsql.googleapis.com/database/disk/read_ops_count",
        "io_write_ops": "cloudsql.googleapis.com/database/disk/write_ops_count",
        "disk_read_bytes": "cloudsql.googleapis.com/database/disk/read_bytes_count",
        "disk_write_bytes": "cloudsql.googleapis.com/database/disk/write_bytes_count",
        "network_receive_bytes": "cloudsql.googleapis.com/database/network/received_bytes_count",
        "network_transmit_bytes": "cloudsql.googleapis.com/database/network/sent_bytes_count",
        "replica_lag_seconds": "cloudsql.googleapis.com/database/replica_lag",
        "replication_lag": "cloudsql.googleapis.com/database/replication/replica_lag",
        "transaction_count": "cloudsql.googleapis.com/database/transaction_count",
        "deadlock_count": "cloudsql.googleapis.com/database/deadlock_count",
    }

    def get_metrics_by_type(
        self,
        resource_id: str,
        rule_to_metric_type: dict[str, str],
        minutes_back: int = 15,
        resource_type: str = "cloudsql_database",
    ):
        """Fetch Cloud Monitoring metrics using arbitrary metric type URIs.

        Parameters
        ----------
        resource_id:
            Cloud SQL instance name (used to match the ``database_id`` label).
        rule_to_metric_type:
            ``{rule_id: metric_type}`` where ``rule_id`` is the friendly
            identifier the caller uses to key the result dict and
            ``metric_type`` is the verbatim Cloud Monitoring metric type URI
            (e.g. ``cloudsql.googleapis.com/database/cpu/utilization``).
        minutes_back:
            Lookback window in minutes.
        resource_type:
            Monitored-resource type filter. Defaults to ``cloudsql_database``.

        Returns
        -------
        dict
            ``{rule_id: [{"time": iso, "value": v}], ...}`` with one entry per
            rule that had data. Failures are surfaced via the ``__error__``
            key for the caller to display.
        """
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=minutes_back)
        interval = monitoring_v3.TimeInterval({"end_time": now, "start_time": start})

        results: dict[str, list | str] = {}

        for rule_id, metric_type in rule_to_metric_type.items():
            if not metric_type:
                continue

            filters = [
                f'metric.type="{metric_type}"',
                f'resource.type="{resource_type}"',
            ]
            if resource_id:
                label_filters = [
                    f'resource.labels.database_id="{self.project_id}:{resource_id}"',
                    f'resource.labels.database_id="{resource_id}"',
                ]
                filters.append("(" + " OR ".join(label_filters) + ")")
            filter_str = " AND ".join([f for f in filters if f])

            try:
                iterator = self.metric_client.list_time_series(
                    request={
                        "name": self._project_name(),
                        "filter": filter_str,
                        "interval": interval,
                        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                        "page_size": monitor_config.get_int(
                            "cloud.gcp", "time_series_page_size", default=5),
                    }
                )
                latest_value = None
                latest_time = None
                for ts in iterator:
                    if not ts.points:
                        continue
                    pt = ts.points[0]
                    active_value = None
                    try:
                        active_value = pt.value._pb.WhichOneof("value")
                    except Exception:
                        pass
                    if active_value in ("double_value", "int64_value", "float_value"):
                        val = getattr(pt.value, active_value)
                    else:
                        continue
                    ts_time = pt.interval.end_time
                    if hasattr(ts_time, "ToDatetime"):
                        ts_dt = ts_time.ToDatetime()
                    elif isinstance(ts_time, datetime):
                        ts_dt = ts_time
                    else:
                        continue
                    if latest_time is None or ts_dt > latest_time:
                        latest_time = ts_dt
                        latest_value = val

                if latest_value is not None:
                    results[rule_id] = [
                        {"time": latest_time.isoformat(), "value": latest_value}
                    ]
                else:
                    results[rule_id] = []
            except Exception as exc:
                if "__error__" not in results:
                    results["__error__"] = str(exc)
                results[rule_id] = []

        return results

    def get_metrics(
        self, resource_id: str, metric_names: list[str], minutes_back: int = 15
    ):
        """Fetch metrics from Cloud Monitoring for the given project/resource.
        Returns dict of metric_name -> [{'time': iso, 'value': v}, ...].

        Backward-compatible wrapper around :meth:`get_metrics_by_type`. New
        callers (driven by INI rules) should prefer ``get_metrics_by_type``
        because it takes the full metric type URI directly instead of relying
        on the local ``GCP_METRIC_MAP`` table.
        """
        rule_to_type = {
            name: self.GCP_METRIC_MAP[name]
            for name in metric_names
            if name in self.GCP_METRIC_MAP
        }
        return self.get_metrics_by_type(resource_id, rule_to_type, minutes_back)

    def get_recent_logs(self, resource_id: str, limit: int | None = None) -> dict:
        """Fetch recent Cloud SQL log entries via Cloud Logging API.

        Requires ``logging_service`` to be attached by the provider wrapper and
        roles/logging.viewer on the project.

        Returns a dict ``{"entries": [...], "error": str | None}``.  The error
        string carries actionable text (e.g. permission-denied, API disabled);
        the caller decides how to surface it in the UI.
        """
        if limit is None:
            limit = monitor_config.get_int("cloud.gcp", "recent_logs_limit", default=5)
        service = getattr(self, "logging_service", None)
        if service is None:
            return {"entries": [], "error": "Logging API client unavailable."}

        filters = ['resource.type="cloudsql_database"']
        if resource_id:
            filters.append(
                "("
                f'resource.labels.database_id="{self.project_id}:{resource_id}" '
                "OR "
                f'resource.labels.database_id="{resource_id}"'
                ")"
            )
        filter_str = " AND ".join(filters)

        body = {
            "resourceNames": [self._project_name()],
            "filter": filter_str,
            "orderBy": "timestamp desc",
            "pageSize": limit,
        }

        # googleapiclient.http logs WARNING on any non-2xx response before
        # raising — silence it for this call because we surface the failure
        # ourselves through the returned ``error`` field.
        import logging as _logging
        _http_logger = _logging.getLogger("googleapiclient.http")
        _prev_level = _http_logger.level
        _http_logger.setLevel(_logging.ERROR)
        try:
            try:
                resp = service.entries().list(body=body).execute()
            except Exception as exc:
                return {"entries": [], "error": _summarise_gcp_api_error(exc)}
        finally:
            _http_logger.setLevel(_prev_level)

        entries = []
        for item in resp.get("entries", [])[:limit]:
            payload = (
                item.get("textPayload")
                or item.get("jsonPayload")
                or item.get("protoPayload")
                or ""
            )
            if isinstance(payload, dict):
                payload = payload.get("message") or str(payload)
            entries.append(
                {
                    "timestamp": item.get("timestamp", ""),
                    "severity": item.get("severity", "DEFAULT"),
                    "message": str(payload).replace("\n", " ")[:180],
                }
            )
        return {"entries": entries, "error": None}


# Default metric set used by callers when no threshold rules are loaded.
GCP_METRICS = [
    "cpu_utilization",
    "memory_utilization",
    "disk_utilization",
    "disk_bytes_used",
    "database_connections",
    "io_read_ops",
    "io_write_ops",
    "network_receive_bytes",
    "network_transmit_bytes",
    "replica_lag_seconds",
]


# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def run_once(gcp, log, resource_id, checker=None):
    errors = gcp.check_health()
    if errors:
        for e in errors:
            log.error(e)
        send_alert("\n".join(errors))
    else:
        log.info("Health OK — all GCP databases available.")

    # Metrics
    if resource_id:
        log.info(f"[Metrics] GCP Cloud SQL: {resource_id}")

        # Drive the fetch from INI rules when available; fall back to GCP_METRICS
        # so the daemon still works before any rules are seeded.
        gcp_path = ("cloudmonitoring", "cloudsql", "database")
        rule_to_type: dict[str, str] = {}
        if checker is not None:
            for rule in checker.list_rules(source="gcp", path=gcp_path):
                metric_type = rule.metric_name or gcp.GCP_METRIC_MAP.get(rule.metric, "")
                if metric_type:
                    rule_to_type[rule.metric] = metric_type
        if not rule_to_type:
            rule_to_type = {
                name: gcp.GCP_METRIC_MAP[name]
                for name in GCP_METRICS
                if name in gcp.GCP_METRIC_MAP
            }

        metrics = gcp.get_metrics_by_type(resource_id, rule_to_type, minutes_back=15)
        metrics.pop("__error__", None)

        flat: dict[str, float] = {}
        for name, datapoints in metrics.items():
            latest = datapoints[-1] if datapoints else None
            if not latest:
                log.debug(f"  [Metric] {name}: no data")
                continue
            try:
                v = float(latest["value"])
            except (KeyError, TypeError, ValueError):
                continue
            log.info(f"  [Metric] {name}: {v} at {latest['time']}")
            flat[name] = v

        if checker is not None and flat:
            metric_alerts = checker.check_many(
                "gcp", flat, instance_id=resource_id, path=gcp_path,
            )
            if metric_alerts:
                for alert in metric_alerts:
                    log.warning(alert.message)
                send_alert("\n".join(a.message for a in metric_alerts))
    else:
        log.debug("Skipping metrics: GCP_RESOURCE_ID not set.")


def main():
    log = setup_logger("monitor_gcp")
    _log_dir = monitor_config.get("monitoring", "standalone_log_dir", default="logs") or "logs"
    os.makedirs(_log_dir, exist_ok=True)
    with open(os.path.join(_log_dir, "gcp.pid"), "w") as f:
        f.write(str(os.getpid()))

    project_id = os.environ.get("GCP_PROJECT_ID", "")
    resource_id = os.environ.get("GCP_SQL_RESOURCE_ID", "")
    if not project_id:
        log.error("GCP_PROJECT_ID not set in .env — exiting.")
        return

    poll_interval = _standalone_poll_interval()
    log.info(f"Starting GCP monitor (interval={poll_interval}s)")
    gcp = GCPMonitor(project_id=project_id)
    try:
        checker = ThresholdChecker()
    except Exception as exc:
        log.warning(f"ThresholdChecker unavailable, alerts disabled: {exc}")
        checker = None

    while True:
        try:
            log.info("--- poll ---")
            run_once(gcp, log, resource_id, checker=checker)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            send_alert(f"[GCP] Unexpected monitor error: {e}")
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
