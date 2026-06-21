"""
cloud_providers/aws_provider.py
================================
AWS provider spec: build_monitor + fetch_metrics for Amazon RDS / Aurora.
fetch_metrics returns (sections, graph_data, alerts) — the standard tuple used
by _fetch_cloud_metrics so all providers share the same formatted display.
"""

from __future__ import annotations

import math

from common.cloud.profiles import TARGET_CLOUD_DB, TARGET_CLOUD_SERVICE, TARGET_VM
from monitoring.cloud_monitor_base import CloudProviderSpec, DiscoveryResult
from monitoring import monitor_config
from monitoring.monitor_config import get_lookback_minutes


def _aws_default_region() -> str:
    return (monitor_config.get("cloud.aws", "default_region", "us-east-1") or "us-east-1").strip()


# ---------------------------------------------------------------------------
# Build monitor
# ---------------------------------------------------------------------------

def build_monitor(entry: dict, sso_callback=None):
    """
    Return (AWSMonitor, None) on success or (None, error_string) on failure.

    Supports two credential styles:
      • Static keys (auth_mode 'keys') — explicit Access Key / Secret / token.
      • Default credential chain — used when no static keys are present, e.g.
        a named profile authenticated via `aws login`, IAM Identity Center
        (`aws sso login`), environment variables, or an instance role.
    """
    try:
        import boto3
        from monitoring.monitor_aws import AWSMonitor
    except ImportError as exc:
        return None, f"Missing library for AWS monitoring: {exc}"

    auth_mode = entry.get("auth_mode", "keys")
    region = (entry.get("region", "") or "").strip()
    access_key = entry.get("access_key_id", "")
    secret_key = entry.get("secret_access_key", "")
    session_tok = entry.get("session_token", "") or None
    profile = (entry.get("sso_profile", "") or "").strip() or None

    use_default_chain = (
        auth_mode in ("env", "sso")
        or not access_key
        or not secret_key
    )

    # Environment / SSO / missing static keys → boto3 default credential chain
    # (instance role, env vars, shared credentials, optional named profile).
    if use_default_chain:
        try:
            monitor = AWSMonitor(region=region or None, profile=profile)
            return monitor, None
        except Exception as exc:
            hint = (
                "Ensure an EC2 instance profile, environment variables, or "
                "`aws login` session is available."
                if auth_mode == "env"
                else "Provide Access Keys, or authenticate the selected profile "
                "first by running `aws login` (or `aws sso login`) in a terminal."
            )
            return None, f"AWS authentication failed. {hint} ({exc})"

    try:
        class _AWSMonitorWithCreds(AWSMonitor):
            """AWSMonitor initialised with explicit credentials instead of profile/env."""

            def __init__(self, region, access_key, secret_key, session_tok):
                session = boto3.Session(
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=session_tok,
                    region_name=region,
                )
                self.rds = session.client("rds")
                self.logs = session.client("logs")
                self.cw = session.client("cloudwatch")
                try:
                    self.pi = session.client("pi")
                except Exception:
                    self.pi = None

        monitor = _AWSMonitorWithCreds(
            region or _aws_default_region(),
            access_key,
            secret_key,
            session_tok,
        )
        return monitor, None

    except Exception as exc:
        return None, f"Failed to initialise AWS monitor: {exc}"


def refresh_monitor(entry: dict, monitor, sso_callback=None):
    """Validate AWS clients; rebuild on failure for static/STS credentials."""
    try:
        errors = monitor.check_health() or []
        if errors:
            rebuilt, err = build_monitor(entry)
            if err:
                return monitor, errors[0]
            return rebuilt, None
        return monitor, None
    except Exception as exc:
        rebuilt, err = build_monitor(entry)
        if err:
            return monitor, f"AWS credential refresh failed: {exc}"
        return rebuilt, None


# ---------------------------------------------------------------------------
# Metric formatting helpers
# ---------------------------------------------------------------------------

# Metrics whose raw value is in bytes
_BYTE_METRICS = frozenset({
    "FreeableMemory", "SwapUsage", "FreeStorageSpace",
    "ReadThroughput", "WriteThroughput",
    "NetworkReceiveThroughput", "NetworkTransmitThroughput",
})
# Metrics whose raw value is in seconds (displayed as ms)
_LATENCY_METRICS = frozenset({"ReadLatency", "WriteLatency", "ReplicaLag"})


def _fv(v, unit=""):
    """Format a numeric value with unit."""
    if v is None:
        return "—"
    if unit == "%":
        return f"{v:>12.1f} %"
    if unit == "GB":
        return f"{v / (1024**3):>10.2f} GB"
    if unit == "MB":
        return f"{v / (1024**2):>10.1f} MB"
    if unit == "ms":
        return f"{v * 1000:>10.2f} ms"
    if unit == "/s":
        return f"{v:>10.1f} /s"
    return f"{v:>14.2f}"


# ---------------------------------------------------------------------------
# Fetch metrics — returns (sections, graph_data, alerts)
# ---------------------------------------------------------------------------

def fetch_metrics(display_name: str, entry: dict, monitor, threshold_checker=None):
    """
    Return (sections, graph_data, alerts).
    sections = [(section_title, [(metric_name, value_str)]), ...]
    """
    graph_data: dict[str, float] = {}
    alerts: list = []
    sections: list = []

    instance_id = entry.get("resource_name", "")
    if not instance_id:
        return sections, graph_data, alerts

    lookback = get_lookback_minutes("aws")

    # CloudWatch-first, Performance Insights fallback (get_rds_metrics). Fall
    # back to the plain CloudWatch call for older monitor objects.
    if hasattr(monitor, "get_rds_metrics"):
        raw = monitor.get_rds_metrics(instance_id, minutes_back=lookback)
    elif hasattr(monitor, "get_rds_cloudwatch_metrics"):
        raw = monitor.get_rds_cloudwatch_metrics(instance_id, minutes_back=lookback)
    else:
        return sections, graph_data, alerts

    if not raw:
        sections.append(("CloudWatch", [
            ("Status", "No data — check instance ID / region, "
                       "and that the instance is running")
        ]))
        return sections, graph_data, alerts

    used_pi = any(
        isinstance(d, dict) and d.get("source") == "PerformanceInsights"
        for d in raw.values()
    )

    perf, mem, storage, io_sec, latency, net = [], [], [], [], [], []

    for name, data in raw.items():
        try:
            v = float(data["value"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        # Append a marker so PI-sourced fallback values are distinguishable.
        src = data.get("source") if isinstance(data, dict) else None
        sfx = "  (PI)" if src == "PerformanceInsights" else ""

        # Graph value
        if name in _BYTE_METRICS:
            graph_val = v / (1024 ** 3)
        elif name in _LATENCY_METRICS:
            graph_val = v * 1000
        else:
            graph_val = v
        graph_data[f"{display_name}_{name}"] = graph_val

        # Section bucketing
        if name == "CPUUtilization":
            perf.append(   ("CPU Utilization",      _fv(v, "%") + sfx))
        elif name == "DatabaseConnections":
            perf.append(   ("Connections",           f"{int(v):>14,}" + sfx))
        elif name == "ReadIOPS":
            io_sec.append( ("Read IOPS",             _fv(v, "/s") + sfx))
        elif name == "WriteIOPS":
            io_sec.append( ("Write IOPS",            _fv(v, "/s") + sfx))
        elif name == "ReadThroughput":
            io_sec.append( ("Read Throughput",       _fv(v, "GB") + sfx))
        elif name == "WriteThroughput":
            io_sec.append( ("Write Throughput",      _fv(v, "GB") + sfx))
        elif name == "ReadLatency":
            latency.append(("Read Latency",          _fv(v, "ms") + sfx))
        elif name == "WriteLatency":
            latency.append(("Write Latency",         _fv(v, "ms") + sfx))
        elif name == "ReplicaLag":
            latency.append(("Replica Lag",           _fv(v, "ms") + sfx))
        elif name == "FreeableMemory":
            mem.append(    ("Freeable Memory",       _fv(v, "GB") + sfx))
        elif name == "SwapUsage":
            mem.append(    ("Swap Usage",            _fv(v, "MB") + sfx))
        elif name == "FreeStorageSpace":
            storage.append(("Free Storage Space",    _fv(v, "GB") + sfx))
        elif name == "DiskQueueDepth":
            io_sec.append( ("Disk Queue Depth",      _fv(v) + sfx))
        elif name == "NetworkReceiveThroughput":
            net.append(    ("Receive Throughput",    _fv(v, "MB") + sfx))
        elif name == "NetworkTransmitThroughput":
            net.append(    ("Transmit Throughput",   _fv(v, "MB") + sfx))
        else:
            perf.append((name, f"{v:>14.2f}" + sfx))

    src_label = "CloudWatch + PI fallback" if used_pi else "CloudWatch"
    if perf:     sections.append((f"Performance  ({src_label}, last {lookback} min)", perf))
    if mem:      sections.append(("Memory",   mem))
    if storage:  sections.append(("Storage",  storage))
    if io_sec:   sections.append(("I/O",      io_sec))
    if latency:  sections.append(("Latency",  latency))
    if net:      sections.append(("Network",  net))

    # RDS log files
    if hasattr(monitor, "get_rds_log_files"):
        log_files = monitor.get_rds_log_files(instance_id)
        if log_files:
            log_items = [
                (lf["LogFileName"], f"{lf.get('Size', 0):>10,} bytes")
                for lf in log_files[:5]
            ]
            sections.append((f"RDS Log Files  ({len(log_files)} total)", log_items))

    if threshold_checker:
        # CloudWatch rules: [metric.aws.cloudwatch.RDS.<MetricName>].
        # AWS PI in-code fallback (FreeableMemory/CPUUtilization/DiskQueueDepth) keeps
        # filling the same CloudWatch rule when CW has no data — those datapoints
        # come back tagged "PerformanceInsights" but are still keyed by the CW
        # metric name, so the cloudwatch rule still fires.
        alerts = threshold_checker.check_many(
            "aws", raw, instance_id=instance_id, path=("cloudwatch", "RDS"),
        )

        # Optional: explicit AWS Performance Insights rules
        # ([metric.aws.pi.RDS.*]). When the user enables any of these, fetch the
        # corresponding PI metrics independently and evaluate them against the
        # PI rules. This is additive — does not interfere with the CW fallback.
        pi_rules = threshold_checker.list_rules(
            source="aws", path=("pi", "RDS"),
        )
        if pi_rules and hasattr(monitor, "get_pi_explicit_metrics"):
            info = monitor.get_instance_info(instance_id) if hasattr(monitor, "get_instance_info") else {}
            dbi = info.get("dbi_resource_id") if isinstance(info, dict) else None
            pi_enabled = info.get("pi_enabled") if isinstance(info, dict) else False
            if dbi and pi_enabled:
                # Build {rule_id: pi_metric_name} from enabled rules so the
                # AWS monitor can map PI responses back to rule ids.
                pi_query_map = {r.metric: r.metric_name for r in pi_rules}
                pi_dict = monitor.get_pi_explicit_metrics(dbi, pi_query_map, minutes_back=lookback)
                if pi_dict:
                    pi_alerts = threshold_checker.check_many(
                        "aws", pi_dict, instance_id=instance_id, path=("pi", "RDS"),
                    )
                    alerts = list(alerts) + list(pi_alerts)

    # Per-component Performance Insights CPU breakdown, rendered as a single
    # multi-series graph ("CPU Utilization (PI)") plus a text section. This is
    # additive and independent of the CloudWatch CPUUtilization line above:
    # the sum of the components approximates the aggregate CPU. Gated by
    # [cloud.aws] pi_cpu_breakdown (default on) and only attempted when the
    # instance has Performance Insights enabled.
    if (
        monitor_config.get_bool("cloud.aws", "pi_cpu_breakdown", default=True)
        and hasattr(monitor, "get_pi_cpu_breakdown")
    ):
        try:
            info = (
                monitor.get_instance_info(instance_id)
                if hasattr(monitor, "get_instance_info")
                else {}
            )
        except Exception:
            info = {}
        if (
            isinstance(info, dict)
            and info.get("pi_enabled")
            and info.get("dbi_resource_id")
        ):
            breakdown = monitor.get_pi_cpu_breakdown(
                info["dbi_resource_id"], minutes_back=lookback
            )
            if isinstance(breakdown, dict) and breakdown:
                # Stable component ordering for consistent colors/legend.
                _order = [
                    "user", "system", "wait", "steal", "nice", "irq", "guest",
                ]
                series: dict[str, float] = {}
                for comp in _order:
                    payload = breakdown.get(comp)
                    if payload is None:
                        continue
                    try:
                        cv = float(
                            payload["value"]
                            if isinstance(payload, dict)
                            else payload
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
                    if math.isfinite(cv):
                        series[comp] = cv
                # Include any extra components PI returned but not in _order.
                for comp, payload in breakdown.items():
                    if comp in series:
                        continue
                    try:
                        cv = float(
                            payload["value"]
                            if isinstance(payload, dict)
                            else payload
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
                    if math.isfinite(cv):
                        series[comp] = cv
                if series:
                    graph_data[f"{display_name}_CPU Utilization (PI)"] = series
                    total = sum(series.values())
                    # PI breakdown values are shown to 2 decimals (finer than
                    # the 1-decimal CloudWatch metrics) since the components are
                    # small fractions of total CPU.
                    rows = [("total (sum)", f"{total:>12.2f} %")]
                    rows += [
                        (f"  {comp}", f"{val:>12.2f} %")
                        for comp, val in series.items()
                    ]
                    sections.append(
                        (f"CPU Breakdown  (Performance Insights, last {lookback} min)",
                         rows)
                    )

    return sections, graph_data, alerts


# ---------------------------------------------------------------------------
# Environment discovery
# ---------------------------------------------------------------------------

def _aws_session(entry: dict):
    import boto3

    profile = (entry.get("sso_profile", "") or "").strip() or None
    region = (entry.get("region", "") or "").strip() or None
    return boto3.Session(profile_name=profile, region_name=region)


def discover(entry: dict, target_kind: str, sso_callback=None) -> DiscoveryResult:
    """List regions, profiles, and resources visible to the ambient AWS identity."""
    try:
        import boto3
    except ImportError as exc:
        return DiscoveryResult(error=f"boto3 is required for AWS discovery: {exc}")

    warnings: list[str] = []
    result = DiscoveryResult(warnings=warnings)

    try:
        session = _aws_session(entry)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        account_id = identity.get("Account", "")
        arn = identity.get("Arn", "")
        result.accounts = [
            {
                "id": account_id,
                "label": f"{account_id} — {arn}" if arn else account_id,
            }
        ]
        result.detected["account_id"] = account_id
    except Exception as exc:
        return DiscoveryResult(
            error=(
                "Could not resolve AWS credentials. On EC2 use an instance profile; "
                "otherwise set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or run `aws login`. "
                f"({exc})"
            )
        )

    try:
        profiles = boto3.Session().available_profiles
        if profiles:
            result.detected["profiles"] = profiles
            result.detected["sso_profile"] = (
                (entry.get("sso_profile", "") or "").strip()
                or profiles[0]
            )
    except Exception as exc:
        warnings.append(f"Could not list AWS profiles: {exc}")

    region = (entry.get("region", "") or "").strip()
    if not region:
        region = session.region_name or None
    if not region:
        try:
            import urllib.request

            with urllib.request.urlopen(
                "http://169.254.169.254/latest/meta-data/placement/region",
                timeout=monitor_config.get_int(
                    "cloud.aws", "metadata_timeout_seconds", default=2),
            ) as resp:
                region = resp.read().decode().strip()
        except Exception:
            pass
    if not region:
        region = _aws_default_region()
    result.detected["region"] = region

    try:
        ec2 = session.client("ec2", region_name=region)
        resp = ec2.describe_regions(AllRegions=False)
        result.regions = sorted(
            r["RegionName"]
            for r in resp.get("Regions", [])
            if r.get("RegionName")
        )
    except Exception as exc:
        warnings.append(f"Could not list AWS regions: {exc}")
        result.regions = [region] if region else []

    resources: list[dict] = []

    def _add(label: str, fields: dict):
        resources.append({"label": label, "fields": fields})

    if target_kind == TARGET_CLOUD_DB:
        try:
            rds = session.client("rds", region_name=region)
            paginator = rds.get_paginator("describe_db_instances")
            for page in paginator.paginate():
                for inst in page.get("DBInstances", []):
                    db_id = inst.get("DBInstanceIdentifier", "")
                    eng = inst.get("Engine", "")
                    inst_region = inst.get("AvailabilityZone", "")[:-1] if inst.get("AvailabilityZone") else region
                    _add(
                        f"{db_id} ({eng}, {inst.get('DBInstanceStatus', '')})",
                        {
                            "resource_name": db_id,
                            "region": inst_region or region,
                            "db_engine": eng,
                        },
                    )
        except Exception as exc:
            warnings.append(f"RDS listing failed in {region}: {exc}")

    elif target_kind == TARGET_VM:
        try:
            ec2 = session.client("ec2", region_name=region)
            paginator = ec2.get_paginator("describe_instances")
            for page in paginator.paginate():
                for resv in page.get("Reservations", []):
                    for inst in resv.get("Instances", []):
                        iid = inst.get("InstanceId", "")
                        name = ""
                        for tag in inst.get("Tags", []) or []:
                            if tag.get("Key") == "Name":
                                name = tag.get("Value", "")
                                break
                        state = inst.get("State", {}).get("Name", "")
                        label = f"{iid} ({name or 'unnamed'}, {state})" if iid else ""
                        if label:
                            _add(
                                label,
                                {
                                    "resource_name": iid,
                                    "region": region,
                                    "host": inst.get("PrivateIpAddress")
                                    or inst.get("PublicIpAddress")
                                    or "",
                                },
                            )
        except Exception as exc:
            warnings.append(f"EC2 listing failed in {region}: {exc}")

    elif target_kind == TARGET_CLOUD_SERVICE:
        try:
            elbv2 = session.client("elbv2", region_name=region)
            for lb in elbv2.describe_load_balancers().get("LoadBalancers", []):
                name = lb.get("LoadBalancerName", "")
                _add(
                    f"ELB {name}",
                    {
                        "resource_name": name,
                        "region": region,
                        "cloud_service_type": "AWS/ApplicationELB",
                    },
                )
        except Exception as exc:
            warnings.append(f"ELB listing failed: {exc}")
        try:
            cache = session.client("elasticache", region_name=region)
            for cluster in cache.describe_cache_clusters().get("CacheClusters", []):
                cid = cluster.get("CacheClusterId", "")
                _add(
                    f"ElastiCache {cid}",
                    {
                        "resource_name": cid,
                        "region": region,
                        "cloud_service_type": "AWS/ElastiCache",
                    },
                )
        except Exception as exc:
            warnings.append(f"ElastiCache listing failed: {exc}")
        try:
            ecs = session.client("ecs", region_name=region)
            for cluster_arn in ecs.list_clusters().get("clusterArns", []):
                short = cluster_arn.split("/")[-1]
                _add(
                    f"ECS {short}",
                    {
                        "resource_name": short,
                        "region": region,
                        "cloud_service_type": "AWS/ECS",
                    },
                )
        except Exception as exc:
            warnings.append(f"ECS listing failed: {exc}")

    result.resources = resources
    if not resources and not warnings:
        warnings.append(
            f"No {target_kind} resources found in {region} for this identity."
        )
    return result


# ---------------------------------------------------------------------------
# Headless interactive login
# ---------------------------------------------------------------------------

def cli_login(entry: dict):
    """Authenticate AWS for this connection via the AWS CLI.

    If a Start URL is present the IAM Identity Center flow (`aws sso login`)
    is used; otherwise the modern `aws login` console-credentials flow is run.
    Both open a browser and store credentials that boto3's default chain
    consumes. Returns (ok, message).
    """
    import subprocess

    profile = (entry.get("sso_profile", "") or "").strip()
    start_url = (entry.get("sso_start_url", "") or "").strip()
    region = entry.get("region", "") or entry.get("sso_region", "")

    if start_url:
        cmd = ["aws", "sso", "login"]
    else:
        cmd = ["aws", "login"]
    if profile:
        cmd += ["--profile", profile]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=monitor_config.get_int(
                "cloud.aws", "login_timeout_seconds", default=300))
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            return False, f"{' '.join(cmd)} failed: {err[:300]}"
    except FileNotFoundError:
        return False, ("'aws' command not found. Install AWS CLI v2: "
                       "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html")
    except subprocess.TimeoutExpired:
        return False, f"{' '.join(cmd)} timed out after 5 minutes."
    except Exception as exc:
        return False, f"AWS login error: {exc}"

    # Verify the resulting credentials work.
    try:
        import boto3
        session = boto3.Session(profile_name=profile or None,
                                region_name=region or None)
        identity = session.client("sts").get_caller_identity()
        return True, f"Authenticated as {identity.get('Arn')}"
    except Exception as exc:
        return False, f"Login completed but credential verification failed: {exc}"


# ---------------------------------------------------------------------------
# Provider spec
# ---------------------------------------------------------------------------

SPEC = CloudProviderSpec(
    name="AWS",
    display_name="Amazon Web Services",
    auth_modes=["keys", "env", "sso"],
    build_monitor=build_monitor,
    fetch_metrics=fetch_metrics,
    refresh_monitor=refresh_monitor,
    discover=discover,
)
