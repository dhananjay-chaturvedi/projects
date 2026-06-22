import os
import time
from datetime import datetime, timezone, timedelta
from .cloud_monitor_base import CloudDBMonitor  # noqa: F401
from dotenv import load_dotenv

load_dotenv()

from azure.identity import (
    ClientSecretCredential,
    AzureCliCredential,
    ManagedIdentityCredential,
    ChainedTokenCredential,
)
from typing import cast
from azure.mgmt.sql import SqlManagementClient
from azure.mgmt.sql.models import Server
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.mysqlflexibleservers import MySQLManagementClient

from .send_notification import send_alert, setup_logger
from .threshold_checker import ThresholdChecker
from . import monitor_config


def _standalone_poll_interval() -> int:
    return max(
        1,
        monitor_config.get_int("monitoring", "standalone_poll_interval", default=10),
    )


# Map an Azure aggregation type (as declared by metric definitions) to the
# attribute that carries its value on an Azure ``MetricValue`` datapoint.
_AGG_TO_FIELD = {
    "average": "average",
    "total":   "total",
    "maximum": "maximum",
    "minimum": "minimum",
    "count":   "count",
}

# Fallback order used ONLY when Azure did not declare a primary aggregation
# for a metric. ``count`` is intentionally last because it is the number of
# samples, not the metric's value — we never want to surface it as the value
# unless nothing else is available.
_VALUE_FIELD_PRIORITY = ("average", "total", "maximum", "minimum", "count")

# Short display tags for the aggregation actually used (surfaced in the UI).
_AGG_DISPLAY_TAG = {
    "average": "avg",
    "total":   "total",
    "maximum": "max",
    "minimum": "min",
    "count":   "count",
}


def _read_metric_value(dp, preferred_field):
    """Return ``(value, field_used)`` for an Azure datapoint without computing.

    Prefers the field matching the metric's platform-declared primary
    aggregation; otherwise takes the first aggregation Azure actually
    populated. Returns ``(None, None)`` when the datapoint carries no value.
    """
    if preferred_field:
        v = getattr(dp, preferred_field, None)
        if v is not None:
            return float(v), preferred_field
    for field in _VALUE_FIELD_PRIORITY:
        v = getattr(dp, field, None)
        if v is not None:
            return float(v), field
    return None, None


# ==========================================
# 2. AZURE MONITOR
# ==========================================
class AzureMonitor(CloudDBMonitor):
    def __init__(self, subscription_id=None):
        subscription_id = subscription_id or os.environ["AZURE_SUBSCRIPTION_ID"]
        tenant = os.environ.get("AZURE_TENANT_ID", "").strip()
        client = os.environ.get("AZURE_CLIENT_ID", "").strip()
        secret = os.environ.get("AZURE_CLIENT_SECRET", "").strip()
        mi_id = os.environ.get("AZURE_LOG_ANALYTICS_ROLE_ID", "").strip()
        if tenant and client and secret:
            credential = ClientSecretCredential(
                tenant_id=tenant,
                client_id=client,
                client_secret=secret,
            )
            print("[Azure] Authenticating with service principal.")
        elif mi_id:
            credential = ChainedTokenCredential(
                ManagedIdentityCredential(client_id=mi_id),
                AzureCliCredential(),
            )
            print(
                "[Azure] Trying managed identity, falling back to az login if unavailable."
            )
        else:
            credential = AzureCliCredential()
            print("[Azure] Falling back to az login.")
        self.sql_client = SqlManagementClient(credential, subscription_id)
        self.monitor_client = MonitorManagementClient(credential, subscription_id)
        self.mysql_client = MySQLManagementClient(credential, subscription_id)
        self.subscription_id = subscription_id

    def check_health(self):
        errors = []
        try:
            for server in cast(list[Server], list(self.sql_client.servers.list())):
                if server.state != "Ready":
                    errors.append(f"[Azure] Server '{server.name}' is {server.state}")
        except Exception as e:
            errors.append(f"[Azure] Connection Error: {str(e)}")
        return errors

    # -- Azure Monitor Metrics --

    def get_primary_aggregations(self, resource_id, metric_names=None):
        """Return ``{metric_name: aggregation}`` as declared by Azure itself.

        Azure Monitor stores every metric pre-aggregated and exposes, per
        metric, a *primary aggregation type* (Average / Total / Maximum /
        Minimum / Count). We read that from the metric-definitions API so the
        platform — not this tool — decides which statistic is canonical for
        each metric. Result is cached per resource.

        On failure (e.g. missing ``metricDefinitions/read`` permission) an
        empty map is returned and callers fall back to a safe request set.
        """
        cache = getattr(self, "_agg_cache", None)
        if cache is None:
            cache = self._agg_cache = {}
        if resource_id in cache:
            return cache[resource_id]

        mapping: dict[str, str] = {}
        try:
            for d in self.monitor_client.metric_definitions.list(resource_id):
                nm = getattr(getattr(d, "name", None), "value", None)
                agg = getattr(d, "primary_aggregation_type", None)
                agg = getattr(agg, "value", agg)  # enum -> str
                if nm and agg:
                    mapping[nm] = str(agg)
        except Exception as e:  # noqa: BLE001 — degrade gracefully
            print(f"[Azure] metric definitions unavailable ({resource_id}): {e}")

        cache[resource_id] = mapping
        return mapping

    def get_metrics(self, resource_id, metric_names, minutes_back=15):
        """Fetch Azure Monitor metrics for a resource — values verbatim.

        The tool performs **no aggregation or computation** of its own. Azure
        Monitor is an aggregation API: every value it stores is already rolled
        up per time bucket, and the API requires an ``aggregation`` and an
        ``interval``. We therefore ask for each metric under the *primary
        aggregation Azure itself declares* and at the finest 1-minute interval,
        then return the platform-computed value as-is.

        Raises on API/auth errors so callers can surface the real reason.
        Returns ``{metric_name: [{"time", "value", "aggregation"}]}`` — only
        metrics Azure actually returned data for.
        """
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=minutes_back)
        fmt = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        results = {}

        primary = self.get_primary_aggregations(resource_id, metric_names)

        # Azure Monitor API accepts at most 20 metric names per call
        chunk_size = monitor_config.get_int("cloud.azure", "metric_chunk_size", default=20)
        for i in range(0, len(metric_names), chunk_size):
            chunk = metric_names[i : i + chunk_size]

            # Ask only for the platform-native aggregations needed by this
            # chunk. When Azure didn't tell us (no definition / no perms),
            # request a safe spread so at least one supported statistic comes
            # back for every metric. "Count" is never used as a *value* (it is
            # the sample count), only as a last-resort field read.
            needed = {primary.get(n, "") for n in chunk}
            needed = {a for a in needed if a and a != "None"}
            aggregation = ",".join(sorted(needed)) if needed else \
                "Average,Total,Maximum,Minimum"

            try:
                response = self.monitor_client.metrics.list(
                    resource_uri=resource_id,
                    timespan=f"{fmt(start)}/{fmt(now)}",
                    interval=monitor_config.get(
                        "cloud.azure", "metric_interval", default="PT1M"),
                    metricnames=",".join(chunk),
                    aggregation=aggregation,
                )
            except Exception:
                # A single metric rejecting the aggregation set must not blank
                # the whole chunk — retry with the broad safe set.
                response = self.monitor_client.metrics.list(
                    resource_uri=resource_id,
                    timespan=f"{fmt(start)}/{fmt(now)}",
                    interval=monitor_config.get(
                        "cloud.azure", "metric_interval", default="PT1M"),
                    metricnames=",".join(chunk),
                    aggregation="Average,Total,Maximum,Minimum",
                )

            for metric in response.value:
                name = metric.name.value
                preferred = _AGG_TO_FIELD.get((primary.get(name) or "").lower())
                datapoints = []
                for ts in metric.timeseries:
                    for dp in ts.data or []:
                        value, used = _read_metric_value(dp, preferred)
                        if value is not None:
                            datapoints.append(
                                {
                                    "time": dp.time_stamp.isoformat(),
                                    "value": value,
                                    "aggregation": used,
                                }
                            )
                if datapoints:
                    results[name] = datapoints
        return results

    # -- Azure MySQL Flexible Server Logs --

    def get_mysql_server_logs(self, resource_group, server_name):
        """List slow-query / audit log files available on an Azure MySQL Flexible Server."""
        try:
            logs = self.mysql_client.log_files.list_by_server(
                resource_group, server_name
            )
            return [
                {
                    "name": log.name,
                    "size_kb": log.size_in_kb,
                    "last_modified": str(log.last_modified_time),
                    "url": log.url,
                }
                for log in logs
            ]
        except Exception as e:
            print(f"[Azure] MySQL server logs error for '{server_name}': {e}")
            return []


# Default metric list kept for backward-compat (MySQL Flexible Server)
AZURE_METRICS = [
    "cpu_percent",
    "memory_percent",
    "active_connections",
    "io_consumption_percent",
    "storage_percent",
    "storage_used",
    "network_bytes_egress",
    "network_bytes_ingress",
    "slow_queries",
    "aborted_connections",
    "total_connections",
    "Queries",
    "Innodb_buffer_pool_reads",
    "Innodb_buffer_pool_read_requests",
    "Innodb_row_lock_waits",
    "Innodb_row_lock_time",
    "backup_storage_used",
    "serverlog_storage_percent",
    "serverlog_storage_usage",
]

# Per-service-type metric sets — only metrics that actually exist on each service
AZURE_METRICS_BY_SERVICE = {
    "Microsoft.Sql/servers": [
        "cpu_percent",
        "dtu_consumption_percent",
        "storage_percent",
        "connection_successful",
        "connection_failed",
        "blocked_by_firewall",
        "deadlock",
        "storage",
        "dtu_used",
        "dwu_used",
        "sessions_percent",
        "workers_percent",
    ],
    "Microsoft.DBforPostgreSQL/servers": [
        "cpu_percent",
        "memory_percent",
        "io_consumption_percent",
        "storage_percent",
        "storage_used",
        "active_connections",
        "network_bytes_egress",
        "network_bytes_ingress",
        "backup_storage_used",
    ],
    "Microsoft.DBforPostgreSQL/flexibleServers": [
        "cpu_percent",
        "memory_percent",
        "iops",
        "storage_percent",
        "storage_used",
        "active_connections",
        "network_bytes_egress",
        "network_bytes_ingress",
        "read_iops",
        "write_iops",
        "read_throughput",
        "write_throughput",
    ],
    "Microsoft.DBforMySQL/servers": [
        "cpu_percent",
        "memory_percent",
        "io_consumption_percent",
        "storage_percent",
        "storage_used",
        "active_connections",
        "network_bytes_egress",
        "network_bytes_ingress",
        "backup_storage_used",
        "serverlog_storage_percent",
    ],
    "Microsoft.DBforMySQL/flexibleServers": [
        "cpu_percent",
        "memory_percent",
        "io_consumption_percent",
        "storage_percent",
        "storage_used",
        "active_connections",
        "network_bytes_egress",
        "network_bytes_ingress",
        "slow_queries",
        "aborted_connections",
        "total_connections",
        "Queries",
        "Innodb_buffer_pool_reads",
        "Innodb_buffer_pool_read_requests",
        "Innodb_row_lock_waits",
        "Innodb_row_lock_time",
        "backup_storage_used",
        "serverlog_storage_percent",
        "serverlog_storage_usage",
        "replication_lag",
    ],
    "Microsoft.DBforMariaDB/servers": [
        "cpu_percent",
        "memory_percent",
        "io_consumption_percent",
        "storage_percent",
        "storage_used",
        "active_connections",
        "network_bytes_egress",
        "network_bytes_ingress",
        "backup_storage_used",
    ],
    "Microsoft.DocumentDB/databaseAccounts": [
        "TotalRequests",
        "TotalRequestUnits",
        "AvailableStorage",
        "DataUsage",
        "IndexUsage",
        "DocumentCount",
        "ReplicationLatency",
        "ServerSideLatency",
        "NormalizedRUConsumption",
    ],
    "Microsoft.Cache/Redis": [
        "cachehits",
        "cachemisses",
        "cachemissrate",
        "cacheRead",
        "cacheWrite",
        "connectedclients",
        "totalcommandsprocessed",
        "operationsPerSecond",
        "percentProcessorTime",
        "usedmemory",
        "usedmemorypercentage",
        "serverLoad",
    ],
}

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def run_once(azure, log, azure_resource_id, az_rg, az_server, checker=None):
    errors = azure.check_health()
    if errors:
        for e in errors:
            log.error(e)
        send_alert("\n".join(errors))
    else:
        log.info("Health OK — all Azure databases available.")

    # -- Metrics --
    if azure_resource_id:
        log.info("[Metrics] Azure MySQL Flexible Server")
        # AZURE_METRICS is the legacy default catalog; the daemon stays on the
        # MySQL Flexible Server profile and lets the ThresholdChecker decide
        # which of those metrics actually have a rule.
        metrics = azure.get_metrics(
            resource_id=azure_resource_id,
            metric_names=AZURE_METRICS,
            minutes_back=15,
        )

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
            agg = latest.get("aggregation", "")
            log.info(f"  [Metric] {name} ({agg}): {v:.2f} at {latest['time']}")
            flat[name] = v

        if checker is not None and flat:
            metric_alerts = checker.check_many(
                "azure", flat, instance_id=az_server or azure_resource_id,
                path=("azuremonitor", "DBforMySQL", "flexibleServers"),
            )
            if metric_alerts:
                for alert in metric_alerts:
                    log.warning(alert.message)
                send_alert("\n".join(a.message for a in metric_alerts))
    else:
        log.debug("Skipping metrics: AZURE_MYSQL_RESOURCE_ID not set.")

    # -- Server Logs --
    if az_rg and az_server:
        log.info(f"Azure MySQL Server Logs: {az_server}")
        server_logs = azure.get_mysql_server_logs(az_rg, az_server)
        for entry in server_logs:
            log.info(
                f"  [Log] {entry['name']}  {entry['size_kb']} KB  modified: {entry['last_modified']}"
            )
    else:
        log.debug(
            "Skipping server logs: AZURE_RESOURCE_GROUP or AZURE_MYSQL_SERVER_NAME not set."
        )


def main():
    log = setup_logger("monitor_azure")
    _log_dir = monitor_config.get("monitoring", "standalone_log_dir", default="logs") or "logs"
    os.makedirs(_log_dir, exist_ok=True)
    with open(os.path.join(_log_dir, "azure.pid"), "w") as f:
        f.write(str(os.getpid()))

    if not os.environ.get("AZURE_SUBSCRIPTION_ID"):
        log.error("AZURE_SUBSCRIPTION_ID not set in .env — exiting.")
        return

    azure_resource_id = os.environ.get("AZURE_MYSQL_RESOURCE_ID", "")
    az_rg = os.environ.get("AZURE_RESOURCE_GROUP", "")
    az_server = os.environ.get("AZURE_MYSQL_SERVER_NAME", "")

    poll_interval = _standalone_poll_interval()
    log.info(f"Starting Azure monitor (interval={poll_interval}s)")
    azure = AzureMonitor()
    try:
        checker = ThresholdChecker()
    except Exception as exc:
        log.warning(f"ThresholdChecker unavailable, alerts disabled: {exc}")
        checker = None

    while True:
        try:
            log.info("--- poll ---")
            run_once(azure, log, azure_resource_id, az_rg, az_server, checker=checker)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            send_alert(f"[Azure] Unexpected monitor error: {e}")
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
