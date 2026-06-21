import os
import time
import boto3
from datetime import datetime, timezone, timedelta
from .cloud_monitor_base import CloudDBMonitor  # noqa: F401 – re-exported for subclasses
from dotenv import load_dotenv

load_dotenv()

from .send_notification import send_alert, setup_logger
from .threshold_checker import ThresholdChecker
from . import monitor_config


def _default_region() -> str:
    return (
        monitor_config.get("cloud.aws", "default_region", "us-east-1")
        or "us-east-1"
    ).strip()


def _standalone_poll_interval() -> int:
    return max(
        1,
        monitor_config.get_int("monitoring", "standalone_poll_interval", default=10),
    )


# ==========================================
# 1. ABSTRACT BASE CLASS
# ==========================================
# ==========================================
# 2. AWS MONITOR
# ==========================================
class AWSMonitor(CloudDBMonitor):
    def __init__(self, region=None, profile=None):
        region = region or os.environ.get("AWS_REGION") or _default_region()
        profile = profile or os.environ.get("AWS_PROFILE")
        session = boto3.Session(profile_name=profile, region_name=region)
        self.rds = session.client("rds")
        self.logs = session.client("logs")
        self.pi = session.client("pi")
        self.cw = session.client("cloudwatch")

    def check_health(self):
        errors = []
        try:
            instances = self.rds.describe_db_instances()
            for db in instances["DBInstances"]:
                status = db["DBInstanceStatus"]
                if status != "available":
                    errors.append(
                        f"[AWS] DB '{db['DBInstanceIdentifier']}' is {status}"
                    )
        except Exception as e:
            errors.append(f"[AWS] Connection Error: {str(e)}")
        return errors

    # -- CloudWatch Logs --

    def get_cloudwatch_logs(self, log_group_name, minutes_back=15, filter_pattern=None):
        """Fetch recent log events from a CloudWatch log group."""
        start_ms = int((time.time() - minutes_back * 60) * 1000)
        kwargs = {
            "logGroupName": log_group_name,
            "startTime": start_ms,
            "limit": monitor_config.get_int(
                "cloud.aws", "cloudwatch_logs_limit", default=100),
        }
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern
        try:
            response = self.logs.filter_log_events(**kwargs)
            return response.get("events", [])
        except Exception as e:
            print(f"[AWS] CloudWatch error for '{log_group_name}': {e}")
            return []

    def stream_cloudwatch_logs(self, log_group_name, filter_pattern=None):
        """Live-tail a CloudWatch log group (blocking, Ctrl+C to stop).
        Uses the ARN-based StartLiveTail API."""
        try:
            response = self.logs.describe_log_groups(
                logGroupNamePrefix=log_group_name, limit=1
            )
            groups = response.get("logGroups", [])
            if not groups or groups[0]["logGroupName"] != log_group_name:
                print(f"[AWS] Log group '{log_group_name}' not found.")
                return
            arn = groups[0]["arn"].rstrip(":*")
        except Exception as e:
            print(f"[AWS] Could not resolve log group ARN: {e}")
            return

        kwargs = {"logGroupIdentifiers": [arn]}
        if filter_pattern:
            kwargs["logEventFilterPattern"] = filter_pattern

        print(f"[AWS] Live-tailing {log_group_name} (Ctrl+C to stop)...")
        try:
            response = self.logs.start_live_tail(**kwargs)
            for event in response["responseStream"]:
                if "sessionUpdate" in event:
                    for log in event["sessionUpdate"].get("sessionResults", []):
                        print(log.get("message", "").strip())
        except KeyboardInterrupt:
            print("\n[AWS] Live tail stopped.")
        except Exception as e:
            print(f"[AWS] Live tail error: {e}")

    # -- RDS Log Files --

    def get_rds_log_files(self, db_instance_identifier):
        """List log files sitting on the RDS instance disk."""
        try:
            response = self.rds.describe_db_log_files(
                DBInstanceIdentifier=db_instance_identifier
            )
            return response.get("DescribeDBLogFiles", [])
        except Exception as e:
            print(f"[AWS] RDS log file list error for '{db_instance_identifier}': {e}")
            return []

    def download_rds_log_file(
        self, db_instance_identifier, log_file_name, max_lines=None
    ):
        """Download the tail of a specific RDS log file."""
        if max_lines is None:
            max_lines = monitor_config.get_int(
                "cloud.aws", "rds_log_tail_max_lines", default=200)
        try:
            response = self.rds.download_db_log_file_portion(
                DBInstanceIdentifier=db_instance_identifier,
                LogFileName=log_file_name,
                NumberOfLines=max_lines,
            )
            return response.get("LogFileData", "")
        except Exception as e:
            print(f"[AWS] RDS log download error for '{log_file_name}': {e}")
            return ""

    # -- CloudWatch RDS Metrics --

    # Canonical RDS metric list. We intentionally do NOT pin a Unit on the
    # query: CloudWatch treats Unit as a *filter*, so a mismatch between the
    # requested unit and the unit the datapoint was stored with makes the
    # whole series come back empty (which previously rendered as 0 / "no
    # data"). Letting CloudWatch pick the unit returns the real values.
    _RDS_CW_METRICS = [
        # Compute
        ("CPUUtilization", "Average"),
        # Memory
        ("FreeableMemory", "Average"),
        ("SwapUsage", "Average"),
        # Connections
        ("DatabaseConnections", "Average"),
        # Storage
        ("FreeStorageSpace", "Average"),
        ("DiskQueueDepth", "Average"),
        # IOPS & throughput
        ("ReadIOPS", "Average"),
        ("WriteIOPS", "Average"),
        ("ReadThroughput", "Average"),
        ("WriteThroughput", "Average"),
        # Latency
        ("ReadLatency", "Average"),
        ("WriteLatency", "Average"),
        # Replication (0 if not a replica)
        ("ReplicaLag", "Average"),
        # Network
        ("NetworkReceiveThroughput", "Average"),
        ("NetworkTransmitThroughput", "Average"),
    ]

    def get_rds_cloudwatch_metrics(self, db_instance_identifier, minutes_back=10):
        """Fetch key RDS CloudWatch metrics for a DB instance.

        Returns ``{MetricName: {"value": float, "time": iso, "source":
        "CloudWatch"}}`` containing only the metrics CloudWatch actually has
        datapoints for.
        """
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=minutes_back)
        results = {}
        queries = [
            {
                "Id": f"m{i}",
                "Label": name,
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/RDS",
                        "MetricName": name,
                        "Dimensions": [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": db_instance_identifier,
                            }
                        ],
                    },
                    "Period": monitor_config.get_int(
                        "cloud.aws", "metric_period_seconds", default=60),
                    "Stat": stat,
                },
                "ReturnData": True,
            }
            for i, (name, stat) in enumerate(self._RDS_CW_METRICS)
        ]
        # Map query Id → metric name so we never rely on response ordering.
        id_to_name = {f"m{i}": name for i, (name, _) in enumerate(self._RDS_CW_METRICS)}
        try:
            resp = self.cw.get_metric_data(
                MetricDataQueries=queries,
                StartTime=start,
                EndTime=now,
                ScanBy="TimestampDescending",  # newest datapoint first
            )
            for item in resp.get("MetricDataResults", []):
                name = id_to_name.get(item.get("Id"), item.get("Label"))
                values = item.get("Values", [])
                timestamps = item.get("Timestamps", [])
                if name and values:
                    # Newest non-null datapoint (ScanBy=Descending → index 0).
                    results[name] = {
                        "value": values[0],
                        "time": timestamps[0].isoformat() if timestamps else "",
                        "source": "CloudWatch",
                    }
        except Exception as e:
            print(f"[AWS] CloudWatch RDS metrics error: {e}")
        return results

    # -- Instance metadata --

    def get_instance_info(self, db_instance_identifier):
        """Resolve metadata needed for Performance Insights fallback.

        Returns a dict with DbiResourceId, whether PI is enabled, engine, and
        status — or an empty dict on failure.
        """
        try:
            resp = self.rds.describe_db_instances(
                DBInstanceIdentifier=db_instance_identifier
            )
            inst = resp["DBInstances"][0]
            return {
                "dbi_resource_id": inst.get("DbiResourceId"),
                "pi_enabled": inst.get("PerformanceInsightsEnabled", False),
                "engine": inst.get("Engine", ""),
                "status": inst.get("DBInstanceStatus", ""),
            }
        except Exception as e:
            print(f"[AWS] describe_db_instances error for '{db_instance_identifier}': {e}")
            return {}

    # -- Performance Insights fallback for core metrics --

    # PI metric → (canonical CloudWatch-style name, multiplier to match the
    # unit CloudWatch uses). PI OS memory counters are reported in kilobytes,
    # so we scale to bytes to match FreeableMemory.
    _PI_FALLBACK_MAP = {
        "os.cpuUtilization.total.avg": ("CPUUtilization", 1.0),
        "os.memory.free.avg": ("FreeableMemory", 1024.0),
        "os.diskIO.avgQueueLen.avg": ("DiskQueueDepth", 1.0),
    }

    def get_pi_fallback_metrics(self, dbi_resource_id, wanted_names, minutes_back=10):
        """Fetch a subset of metrics from Performance Insights OS counters.

        Only the metrics in *wanted_names* that PI can serve are queried.
        Returns ``{CanonicalName: {"value": float, "time": iso, "source":
        "PerformanceInsights"}}``.
        """
        results = {}
        if not dbi_resource_id:
            return results

        # Which PI counters map to a metric we still need?
        pi_queries = []
        pi_to_canonical = {}
        for pi_metric, (canonical, mult) in self._PI_FALLBACK_MAP.items():
            if canonical in wanted_names:
                pi_queries.append({"Metric": pi_metric})
                pi_to_canonical[pi_metric] = (canonical, mult)
        if not pi_queries:
            return results

        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=max(minutes_back, 5))
        try:
            resp = self.pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=dbi_resource_id,
                StartTime=start,
                EndTime=now,
                PeriodInSeconds=monitor_config.get_int(
                    "cloud.aws", "metric_period_seconds", default=60),
                MetricQueries=pi_queries,
            )
            for m in resp.get("MetricList", []):
                pi_metric = m.get("Key", {}).get("Metric", "")
                mapping = pi_to_canonical.get(pi_metric)
                if not mapping:
                    continue
                canonical, mult = mapping
                # Newest datapoint with a non-null value.
                points = [
                    dp for dp in m.get("DataPoints", [])
                    if dp.get("Value") is not None
                ]
                if not points:
                    continue
                latest = points[-1]
                results[canonical] = {
                    "value": latest["Value"] * mult,
                    "time": latest["Timestamp"].isoformat(),
                    "source": "PerformanceInsights",
                }
        except Exception as e:
            print(f"[AWS] Performance Insights fallback error: {e}")
        return results

    def get_pi_explicit_metrics(
        self,
        dbi_resource_id: str,
        rule_to_pi_metric: dict[str, str],
        minutes_back: int = 10,
    ) -> dict[str, dict]:
        """Fetch arbitrary Performance Insights metrics, keyed by rule id.

        Used by the ``[metric.aws.pi.RDS.*]`` rules path so callers can declare
        a rule like ``os.memory.free.avg`` or ``db.load.avg`` in the INI and
        have it fetched directly from Performance Insights (bypassing the
        CloudWatch-first canonical mapping in :meth:`get_rds_metrics`).

        Parameters
        ----------
        dbi_resource_id:
            Performance Insights identifier (``DbiResourceId``).
        rule_to_pi_metric:
            Mapping of ``{rule_id: pi_metric_name}``. ``rule_id`` is the INI
            section's last segment; ``pi_metric_name`` is the verbatim PI
            metric string (e.g. ``os.cpuUtilization.total.avg``).
        minutes_back:
            Look-back window in minutes.

        Returns
        -------
        dict
            ``{rule_id: {"value": float, "time": iso, "source":
            "PerformanceInsights"}}`` — one entry per rule that PI returned
            data for. Rules with no data are simply omitted.
        """
        results: dict[str, dict] = {}
        if not dbi_resource_id or not rule_to_pi_metric:
            return results

        # Build PI query list. Keep a reverse map for re-keying the response.
        queries = [{"Metric": metric} for metric in rule_to_pi_metric.values()]
        pi_to_rule = {metric: rule for rule, metric in rule_to_pi_metric.items()}

        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=max(minutes_back, 5))
        try:
            resp = self.pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=dbi_resource_id,
                StartTime=start,
                EndTime=now,
                PeriodInSeconds=monitor_config.get_int(
                    "cloud.aws", "metric_period_seconds", default=60),
                MetricQueries=queries,
            )
            for m in resp.get("MetricList", []):
                pi_metric = m.get("Key", {}).get("Metric", "")
                rule_id = pi_to_rule.get(pi_metric)
                if not rule_id:
                    continue
                points = [
                    dp for dp in m.get("DataPoints", [])
                    if dp.get("Value") is not None
                ]
                if not points:
                    continue
                latest = points[-1]
                results[rule_id] = {
                    "value": latest["Value"],
                    "time": latest["Timestamp"].isoformat(),
                    "source": "PerformanceInsights",
                }
        except Exception as e:
            print(f"[AWS] PI explicit-rule fetch error: {e}")
        return results

    # Per-component OS CPU counters from Performance Insights. Their sum
    # approximates the aggregate CPU shown by CloudWatch CPUUtilization and the
    # RDS console's "CPU Utilization" Performance Insights breakdown.
    _PI_CPU_COMPONENTS = (
        "os.cpuUtilization.user.avg",
        "os.cpuUtilization.system.avg",
        "os.cpuUtilization.wait.avg",
        "os.cpuUtilization.steal.avg",
        "os.cpuUtilization.nice.avg",
        "os.cpuUtilization.irq.avg",
        "os.cpuUtilization.guest.avg",
    )

    def get_pi_cpu_breakdown(self, dbi_resource_id, minutes_back=10):
        """Fetch the per-component OS CPU breakdown from Performance Insights.

        Returns ``{component: {"value": float, "time": iso}}`` keyed by the
        short component name (``user``, ``system``, ``wait``, ``steal``,
        ``nice``, ``irq``, ``guest``). Each value is the **average** of the
        cloud-provided ``.avg`` datapoints across the lookback window (not a
        single peak/most-recent point), matching the averaged CPU the RDS
        console shows. The sum of the components approximates the total CPU
        utilization. Returns an empty dict if PI is unavailable or has no data.
        """
        results: dict[str, dict] = {}
        if not dbi_resource_id or not getattr(self, "pi", None):
            return results

        queries = [{"Metric": m} for m in self._PI_CPU_COMPONENTS]
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=max(minutes_back, 5))
        try:
            resp = self.pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=dbi_resource_id,
                StartTime=start,
                EndTime=now,
                PeriodInSeconds=monitor_config.get_int(
                    "cloud.aws", "metric_period_seconds", default=60),
                MetricQueries=queries,
            )
            for m in resp.get("MetricList", []):
                metric = m.get("Key", {}).get("Metric", "")
                # os.cpuUtilization.user.avg -> "user"
                parts = metric.split(".")
                short = parts[-2] if len(parts) >= 2 else metric
                points = [
                    dp for dp in m.get("DataPoints", [])
                    if dp.get("Value") is not None
                ]
                if not points:
                    continue
                # Mean of the cloud-provided per-period averages over the
                # window — a smoothed value rather than the last/peak point.
                values = [dp["Value"] for dp in points]
                avg_val = sum(values) / len(values)
                results[short] = {
                    "value": avg_val,
                    "time": points[-1]["Timestamp"].isoformat(),
                }
        except Exception as e:
            print(f"[AWS] PI CPU breakdown fetch error: {e}")
        return results

    def get_rds_metrics(self, db_instance_identifier, minutes_back=10):
        """CloudWatch-first metric fetch with Performance Insights fallback.

        For every canonical RDS metric, the value comes from CloudWatch when
        available; any metric CloudWatch does not return is filled in from
        Performance Insights OS counters (when PI is enabled and PI exposes an
        equivalent counter).

        Returns ``{MetricName: {"value", "time", "source"}}``.
        """
        merged = self.get_rds_cloudwatch_metrics(
            db_instance_identifier, minutes_back=minutes_back
        )

        all_names = {name for name, _ in self._RDS_CW_METRICS}
        missing = [n for n in all_names if n not in merged]
        # Only metrics PI can actually serve are worth a PI call.
        pi_serviceable = {c for (c, _) in self._PI_FALLBACK_MAP.values()}
        if missing and pi_serviceable.intersection(missing):
            info = self.get_instance_info(db_instance_identifier)
            if info.get("pi_enabled") and info.get("dbi_resource_id"):
                fallback = self.get_pi_fallback_metrics(
                    info["dbi_resource_id"], set(missing), minutes_back=minutes_back
                )
                for name, payload in fallback.items():
                    merged.setdefault(name, payload)
        return merged

    # -- Performance Insights --

    def get_performance_insights(self, dbi_resource_id, minutes_back=None):
        """Fetch db.load.avg and top SQL from Performance Insights."""
        if minutes_back is None:
            minutes_back = monitor_config.get_int(
                "cloud.aws", "performance_insights_lookback_minutes", default=60)
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=minutes_back)
        pi_period = monitor_config.get_int(
            "cloud.aws", "performance_insights_period_seconds", default=300)
        result = {}

        try:
            metrics_resp = self.pi.get_resource_metrics(
                ServiceType="RDS",
                Identifier=dbi_resource_id,
                StartTime=start,
                EndTime=now,
                PeriodInSeconds=pi_period,
                MetricQueries=[
                    {"Metric": "db.load.avg"},
                    {"Metric": "db.sampledload.avg"},
                ],
            )
            for m in metrics_resp.get("MetricList", []):
                key = m["Key"]["Metric"]
                result[key] = [
                    {"time": dp["Timestamp"].isoformat(), "value": dp.get("Value")}
                    for dp in m.get("DataPoints", [])
                ]
        except Exception as e:
            print(f"[AWS] Performance Insights metrics error: {e}")

        for group, key, label in [
            ("db.sql", "db.sql.statement", "top_sql"),
            ("db.wait_event", "db.wait_event.name", "top_waits"),
            ("db.user", "db.user.name", "top_users"),
        ]:
            try:
                dim_resp = self.pi.describe_dimension_keys(
                    ServiceType="RDS",
                    Identifier=dbi_resource_id,
                    StartTime=start,
                    EndTime=now,
                    PeriodInSeconds=pi_period,
                    Metric="db.load.avg",
                    GroupBy={"Group": group, "Limit": 5},
                )
                result[label] = [
                    {
                        "name": k.get("Dimensions", {}).get(key, ""),
                        "load": k.get("Total"),
                    }
                    for k in dim_resp.get("Keys", [])
                ]
            except Exception as e:
                print(f"[AWS] Performance Insights {label} error: {e}")

        return result


# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def run_once(aws, log, rds_id, cw_log_group, dbi_resource_id, checker=None):
    errors = aws.check_health()
    if errors:
        for e in errors:
            log.error(e)
        send_alert("\n".join(errors))
    else:
        log.info("Health OK — all AWS databases available.")

    # -- RDS Metrics (CloudWatch first, Performance Insights fallback) --
    log.info(f"[Metrics] RDS CloudWatch/PI: {rds_id}")
    cw_metrics = aws.get_rds_metrics(rds_id, minutes_back=10)
    for name, data in cw_metrics.items():
        v = data["value"]
        source = data.get("source", "CloudWatch")
        if "Bytes" in name and "IOPS" not in name and "Throughput" not in name:
            display = f"{v / (1024**3):.2f} GB"
        elif "Latency" in name:
            display = f"{v * 1000:.2f} ms"
        elif "Percent" in name or "Utilization" in name:
            display = f"{v:.1f}%"
        else:
            display = f"{v:.2f}"
        log.info(f"  [{source}] {name}: {display} at {data['time']}")

    # All threshold evaluation goes through ThresholdChecker — single source
    # of truth, no duplicated thresholds in this module.
    if checker is not None and cw_metrics:
        metric_alerts = checker.check_many(
            "aws", cw_metrics, instance_id=rds_id, path=("cloudwatch", "RDS"),
        )
        if metric_alerts:
            for alert in metric_alerts:
                log.warning(alert.message)
            send_alert("\n".join(a.message for a in metric_alerts))

    # -- CloudWatch Logs --
    log.info(f"[CW-Logs] {cw_log_group}")
    cw_events = aws.get_cloudwatch_logs(cw_log_group, minutes_back=1)
    if cw_events:
        for evt in cw_events:
            log.info(f"  [CW] {evt.get('message', '').strip()}")
    else:
        log.info("  [CW] No new log events.")

    # -- RDS Log Files --
    log.info(f"[RDS-Logs] {rds_id}")
    log_files = aws.get_rds_log_files(rds_id)
    for f in log_files:
        log.info(
            f"  {f['LogFileName']}  {f.get('Size', 0)} bytes  last written: {f.get('LastWritten')}"
        )
    if log_files:
        latest = max(log_files, key=lambda x: x.get("LastWritten", 0))
        tail = aws.download_rds_log_file(rds_id, latest["LogFileName"])
        if tail:
            for line in tail.splitlines()[-10:]:
                log.info(f"  [RDS] {line}")

    # -- Performance Insights --
    if dbi_resource_id:
        log.info(f"[PI] Performance Insights: {rds_id}")
        pi_data = aws.get_performance_insights(dbi_resource_id, minutes_back=10)
        for metric, datapoints in pi_data.items():
            if metric in ("top_sql", "top_waits", "top_users"):
                for row in datapoints:
                    log.info(
                        f"  [PI] {metric}  load={row['load']:.4f}  {row['name'][:120]}"
                    )
            else:
                latest_dp = datapoints[-1] if datapoints else None
                if latest_dp:
                    log.info(
                        f"  [PI] {metric}: {latest_dp['value']} at {latest_dp['time']}"
                    )
    else:
        log.debug("Skipping Performance Insights: AWS_DBI_RESOURCE_ID not set.")


def main():
    log = setup_logger("monitor_aws")
    _log_dir = monitor_config.get("monitoring", "standalone_log_dir", default="logs") or "logs"
    os.makedirs(_log_dir, exist_ok=True)
    with open(os.path.join(_log_dir, "aws.pid"), "w") as f:
        f.write(str(os.getpid()))

    rds_id = os.environ.get("AWS_RDS_INSTANCE_ID", "").strip()
    if not rds_id:
        log.error("AWS_RDS_INSTANCE_ID not set — exiting.")
        return
    cw_log_group = os.environ.get("AWS_CLOUDWATCH_LOG_GROUP", "").strip()
    dbi_resource_id = os.environ.get("AWS_DBI_RESOURCE_ID", "")

    poll_interval = _standalone_poll_interval()
    log.info(f"Starting AWS monitor (interval={poll_interval}s)")
    aws = AWSMonitor()
    try:
        checker = ThresholdChecker()
    except Exception as exc:
        log.warning(f"ThresholdChecker unavailable, alerts disabled: {exc}")
        checker = None

    while True:
        try:
            log.info("--- poll ---")
            run_once(aws, log, rds_id, cw_log_group, dbi_resource_id, checker=checker)
        except Exception as e:
            log.exception(f"Unexpected error: {e}")
            send_alert(f"[AWS] Unexpected monitor error: {e}")
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
