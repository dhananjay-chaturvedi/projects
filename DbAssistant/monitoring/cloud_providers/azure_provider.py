"""
cloud_providers/azure_provider.py
==================================
Azure provider spec: build_monitor + fetch_metrics for Azure SQL / MySQL Flex.

Azure SSO (browser login) requires an interactive UI callback.  The caller
(server_monitor_ui) supplies ``sso_callback(az_cmd) -> subprocess.CompletedProcess``
so the dialog can be shown on the main tkinter thread while this function runs
in a background thread.
"""

from __future__ import annotations

import math

from common.cloud.profiles import TARGET_CLOUD_DB, TARGET_CLOUD_SERVICE, TARGET_VM
from monitoring.cloud_monitor_base import CloudProviderSpec, DiscoveryResult
from monitoring import monitor_config
from monitoring.monitor_config import get_lookback_minutes


def _resource_group_from_id(resource_id: str) -> str:
    parts = (resource_id or "").split("/")
    try:
        idx = parts.index("resourceGroups")
        return parts[idx + 1]
    except (ValueError, IndexError):
        return ""


# ---------------------------------------------------------------------------
# Build monitor
# ---------------------------------------------------------------------------

def build_monitor(entry: dict, sso_callback=None):
    """
    Return (AzureMonitor, None) on success or (None, error_string) on failure.

    sso_callback : callable | None
        Required for auth_mode == "sso".
        Signature: ``sso_callback(az_cmd: list[str]) -> subprocess.CompletedProcess``
        The UI provides this so a waiting dialog can be shown while az login runs.
    """
    sub_id = entry.get("subscription_id", "")
    tenant_id = entry.get("tenant_id", "")
    client_id = entry.get("client_id", "")
    client_sec = entry.get("client_secret", "")
    username = entry.get("username", "")
    password = entry.get("password", "")
    auth_mode = entry.get("auth_mode", "keys")

    if not sub_id:
        return None, "Azure Subscription ID is required."

    try:
        from monitoring.monitor_azure import AzureMonitor
        from azure.identity import (
            ClientSecretCredential,
            DefaultAzureCredential,
            UsernamePasswordCredential,
            AzureCliCredential,
        )
        from azure.mgmt.monitor import MonitorManagementClient
        from azure.mgmt.sql import SqlManagementClient
    except ImportError as exc:
        return None, f"Missing library for Azure monitoring: {exc}"

    _AZURE_CLI_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"

    try:
        if auth_mode == "keys" and tenant_id and client_id and client_sec:
            credential = ClientSecretCredential(tenant_id, client_id, client_sec)

        elif auth_mode == "sso":
            az_cmd = ["az", "login"]
            if tenant_id:
                az_cmd += ["--tenant", tenant_id]

            if sso_callback is None:
                return None, "Azure SSO requires a UI callback (sso_callback not provided)."

            result = sso_callback(az_cmd)
            if result.returncode != 0:
                err = (result.stderr or result.stdout or "").strip()
                return None, f"az login failed: {err[:300]}"

            credential = AzureCliCredential()

        elif auth_mode == "pwd" and tenant_id and username and password:
            eff_client = client_id or _AZURE_CLI_CLIENT_ID
            try:
                credential = UsernamePasswordCredential(
                    eff_client, username, password, tenant_id=tenant_id
                )
                credential.get_token("https://management.azure.com/.default")
            except Exception as pwd_exc:
                msg = str(pwd_exc)
                _ropc_disabled = (
                    "wst:FailedAuthentication" in msg
                    or "WsTrust" in msg
                    or "AADSTS7000218" in msg
                    or "AADSTS50053" in msg
                )
                _mfa_required = (
                    "AADSTS50076" in msg
                    or "AADSTS50079" in msg
                    or "multi-factor" in msg.lower()
                    or "conditional access" in msg.lower()
                )
                if _ropc_disabled:
                    return (
                        None,
                        "Azure Username/Password login was rejected by the tenant.\n\n"
                        "Reason: The tenant has disabled the legacy ROPC/WsTrust "
                        "authentication flow for security reasons. Federated tenants "
                        "(ADFS, Azure AD B2B) and tenants with Conditional Access "
                        "policies commonly block this flow.\n\n"
                        "Fix: Edit this connection and switch to the "
                        "'Azure AD Device Code (SSO)' tab.\n"
                        "A browser window will open for interactive login — "
                        "this works with MFA, Conditional Access, and federated accounts.",
                    )
                if _mfa_required:
                    return (
                        None,
                        "Azure MFA required — this account has Conditional Access policies "
                        "that enforce multi-factor authentication.\n\n"
                        "Username/Password (ROPC) flow cannot satisfy MFA challenges.\n\n"
                        "Fix: Edit this connection and switch to the "
                        "'Azure AD Device Code (SSO)' tab. A browser window will open for "
                        "interactive login with full MFA support.",
                    )
                return None, f"Azure authentication failed: {msg[:300]}"
        elif auth_mode == "env":
            credential = DefaultAzureCredential()
            credential.get_token("https://management.azure.com/.default")
        else:
            credential = AzureCliCredential()

        class _AzureMonitorWithCreds(AzureMonitor):
            def __init__(self, credential, subscription_id):
                self.credential = credential
                self.sql_client = SqlManagementClient(credential, subscription_id)
                self.monitor_client = MonitorManagementClient(credential, subscription_id)
                self.subscription_id = subscription_id
                try:
                    from azure.mgmt.mysqlflexibleservers import MySQLManagementClient
                    self.mysql_client = MySQLManagementClient(credential, subscription_id)
                except Exception:
                    self.mysql_client = None

        monitor = _AzureMonitorWithCreds(credential, sub_id)
        return monitor, None

    except Exception as exc:
        return None, f"Failed to initialise Azure monitor: {exc}"


def refresh_monitor(entry: dict, monitor, sso_callback=None):
    """Refresh/validate Azure credentials without forcing browser re-auth."""
    try:
        credential = getattr(monitor, "credential", None)
        if credential is None:
            return build_monitor(entry, sso_callback=None)
        tok = credential.get_token("https://management.azure.com/.default")
        monitor._token_expires_on = tok.expires_on
        errors = monitor.check_health() or []
        if errors:
            return monitor, errors[0]
        return monitor, None
    except Exception as exc:
        return monitor, f"Azure credential refresh failed: {exc}"


# ---------------------------------------------------------------------------
# Metric category mapping for Azure metric names
# ---------------------------------------------------------------------------

_PERF_METRICS  = {"cpu_percent", "connections_failed", "active_connections",
                   "connection_successful", "dtu_consumption_percent", "vcores",
                   "workers_percent", "sessions_percent"}
_MEM_METRICS   = {"memory_percent"}
_STORAGE_METRICS = {"storage_percent", "storage", "storage_used",
                    "backup_storage_used", "allocated_data_storage"}
_IO_METRICS    = {"io_consumption_percent", "physical_data_read_percent",
                  "log_write_percent", "blocked_by_firewall"}
_NET_METRICS   = {"network_bytes_egress", "network_bytes_ingress"}

# Short tags for the platform aggregation surfaced next to each value.
_AGG_SHORT = {
    "average": "avg",
    "total":   "total",
    "maximum": "max",
    "minimum": "min",
    "count":   "count",
}


def _azure_section_for(name: str) -> str:
    lname = name.lower()
    if any(k in lname for k in ("cpu", "connection", "dtu", "worker", "session", "vcore")):
        return "Performance"
    if any(k in lname for k in ("memory",)):
        return "Memory"
    if any(k in lname for k in ("storage", "backup")):
        return "Storage"
    if any(k in lname for k in ("io", "read", "log_write", "iops")):
        return "I/O"
    if any(k in lname for k in ("network", "bytes_egress", "bytes_ingress")):
        return "Network"
    return "Other"


# ---------------------------------------------------------------------------
# Fetch metrics — returns (sections, graph_data, alerts)
# ---------------------------------------------------------------------------

def _svc_type_to_path(svc_type: str) -> tuple[str, ...]:
    """Convert a Microsoft.<RP>/<resourceType> string into the path tuple used
    by threshold rules: ``("azuremonitor", "<RP>", "<resourceType>")``.

    Returns an empty tuple when *svc_type* doesn't match the expected shape.
    """
    if not svc_type or "/" not in svc_type:
        return ()
    head, _, tail = svc_type.partition("/")
    if "." in head:
        rp = head.split(".", 1)[1]
    else:
        rp = head
    if not rp or not tail:
        return ()
    return ("azuremonitor", rp, tail)


def fetch_metrics(display_name: str, entry: dict, monitor, threshold_checker=None):
    """Return (sections, graph_data, alerts)."""
    try:
        from monitoring.monitor_azure import AZURE_METRICS_BY_SERVICE, AZURE_METRICS
    except ImportError:
        return [("Error", [("Status", "Azure library not installed")])], {}, []

    graph_data: dict[str, float] = {}
    alerts: list = []
    sections: list = []

    sub_id = entry.get("subscription_id", "")
    rg = entry.get("resource_group", "")
    resource_name = entry.get("resource_name", display_name)
    svc_type = (entry.get("db_service_type") or "Microsoft.Sql/servers").strip()

    if not (sub_id and rg and resource_name and hasattr(monitor, "get_metrics")):
        return sections, graph_data, alerts

    db_name = entry.get("database_name", "").strip()
    if svc_type == "Microsoft.Sql/servers" and db_name:
        resource_uri = (
            f"/subscriptions/{sub_id}/resourceGroups/{rg}"
            f"/providers/Microsoft.Sql/servers/{resource_name}"
            f"/databases/{db_name}"
        )
    elif svc_type == "Microsoft.Sql/servers" and not db_name:
        sections.append(("Configuration", [
            ("Status", "'Database Name' is required for Microsoft.Sql/servers — "
                       "edit this connection.")
        ]))
        return sections, graph_data, alerts
    else:
        resource_uri = (
            f"/subscriptions/{sub_id}/resourceGroups/{rg}"
            f"/providers/{svc_type}/{resource_name}"
        )

    # Drive the fetch list from INI rules first (enabled rules for this RP),
    # falling back to the legacy static catalog for service types that don't
    # have any rules seeded yet.
    rule_path = _svc_type_to_path(svc_type)
    metric_names: list[str] = []
    if threshold_checker and rule_path:
        for rule in threshold_checker.list_rules(source="azure", path=rule_path):
            api_name = rule.metric_name or rule.metric
            if api_name not in metric_names:
                metric_names.append(api_name)
    if not metric_names:
        metric_names = list(AZURE_METRICS_BY_SERVICE.get(svc_type, AZURE_METRICS))

    lookback = get_lookback_minutes("azure")
    try:
        raw = monitor.get_metrics(resource_uri, metric_names, minutes_back=lookback)
    except Exception as exc:
        sections.append(("Error", [("Azure Monitor error", str(exc)[:80])]))
        return sections, graph_data, alerts

    if not raw:
        sections.append(("Azure Monitor", [
            ("Status", f"No data returned for {svc_type}/{resource_name}")
        ]))
        return sections, graph_data, alerts

    # Group raw metrics into sections
    buckets: dict[str, list] = {}
    _azure_flat: dict = {}

    # Pre-compute a tiny lookup so the UI can mark breaches with an arrow
    # without re-running threshold logic. Uses the same ThresholdChecker rules
    # as the alerts evaluation below, so they can never drift.
    _breach_thresholds: dict[str, tuple[str, float]] = {}
    if threshold_checker and rule_path:
        for rule in threshold_checker.list_rules(source="azure", path=rule_path):
            for thr in (rule.critical, rule.warning):
                if thr is not None:
                    _breach_thresholds[rule.metric] = (rule.operator, float(thr))
                    break

    for name, datapoints in raw.items():
        latest = datapoints[-1] if datapoints else None
        if latest is None:
            continue
        try:
            v = float(latest["value"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        graph_data[f"{display_name}_{name}"] = v
        _azure_flat[name] = v

        # Tag the platform statistic Azure actually returned (avg/total/...),
        # so it's always clear which pre-aggregated value is shown.
        agg = (latest.get("aggregation") or "").lower()
        agg_tag = f"  [{_AGG_SHORT.get(agg, agg)}]" if agg else ""

        alert_flag = ""
        op_thr = _breach_thresholds.get(name)
        if op_thr:
            op, thr = op_thr
            if (op == ">" and v > thr) or (op == "<" and v < thr):
                alert_flag = "  ⚠"

        section = _azure_section_for(name)
        # Format value based on metric name
        lname = name.lower()
        if "percent" in lname or lname.endswith("_pct"):
            val_str = f"{v:>11.1f} %{agg_tag}{alert_flag}"
        elif "bytes" in lname:
            val_str = f"{v / (1024**2):>10.2f} MB{agg_tag}{alert_flag}"
        elif "count" in lname or "connection" in lname or "worker" in lname or "session" in lname:
            val_str = f"{int(v):>14,}{agg_tag}{alert_flag}"
        else:
            val_str = f"{v:>14.2f}{agg_tag}{alert_flag}"

        buckets.setdefault(section, []).append((name, val_str))

    # Emit sections in a fixed preferred order
    for sec in ("Performance", "Memory", "Storage", "I/O", "Network", "Other"):
        if sec in buckets:
            label = f"{sec}  (Azure Monitor, last 1 min)" if sec == "Performance" else sec
            sections.append((label, buckets[sec]))

    if _azure_flat and threshold_checker and rule_path:
        alerts = threshold_checker.check_many(
            "azure", _azure_flat, instance_id=display_name, path=rule_path,
        )

    # MySQL server log files
    if rg and resource_name and hasattr(monitor, "get_mysql_server_logs") and monitor.mysql_client:
        log_files = monitor.get_mysql_server_logs(rg, resource_name)
        if log_files:
            log_items = [
                (lf["name"],
                 f"{lf.get('size_kb', 0):>8,} KB  {lf.get('last_modified', '')}")
                for lf in log_files[:5]
            ]
            sections.append((f"Server Log Files  ({len(log_files)} total)", log_items))

    return sections, graph_data, alerts


# ---------------------------------------------------------------------------
# Environment discovery
# ---------------------------------------------------------------------------

def discover(entry: dict, target_kind: str, sso_callback=None) -> DiscoveryResult:
    """List subscriptions, locations, and resources for the ambient Azure identity."""
    warnings: list[str] = []
    result = DiscoveryResult(warnings=warnings)

    try:
        from azure.identity import DefaultAzureCredential
    except ImportError as exc:
        return DiscoveryResult(error=f"azure-identity is required: {exc}")

    try:
        credential = DefaultAzureCredential()
        credential.get_token("https://management.azure.com/.default")
    except Exception as exc:
        return DiscoveryResult(
            error=(
                "Could not resolve Azure credentials. Use Managed Identity on an Azure VM, "
                "set AZURE_CLIENT_ID/AZURE_CLIENT_SECRET, or run `az login`. "
                f"({exc})"
            )
        )

    subs: list = []
    try:
        from azure.mgmt.subscription import SubscriptionClient

        sub_client = SubscriptionClient(credential)
        subs = list(sub_client.subscriptions.list())
        result.accounts = [
            {
                "id": s.subscription_id,
                "label": f"{s.display_name or s.subscription_id} ({s.subscription_id})",
                "tenant_id": getattr(s, "tenant_id", "") or "",
            }
            for s in subs
            if s.subscription_id
        ]
    except ImportError:
        warnings.append(
            "Install azure-mgmt-subscription for subscription discovery: "
            "pip install azure-mgmt-subscription"
        )
    except Exception as exc:
        warnings.append(f"Subscription listing failed: {exc}")

    sub_id = (entry.get("subscription_id", "") or "").strip()
    if not sub_id and result.accounts:
        sub_id = result.accounts[0]["id"]
    if sub_id:
        result.detected["subscription_id"] = sub_id

    tenant_id = (entry.get("tenant_id", "") or "").strip()
    if not tenant_id:
        for acct in result.accounts:
            if acct.get("id") == sub_id and acct.get("tenant_id"):
                tenant_id = acct["tenant_id"]
                break
        if not tenant_id and subs:
            tenant_id = getattr(subs[0], "tenant_id", "") or ""
    if tenant_id:
        result.detected["tenant_id"] = tenant_id

    if sub_id:
        try:
            from azure.mgmt.subscription import SubscriptionClient

            sub_client = SubscriptionClient(credential)
            result.regions = sorted(
                loc.name
                for loc in sub_client.subscriptions.list_locations(sub_id)
                if getattr(loc, "name", None)
            )
            if result.regions:
                result.detected["region"] = result.regions[0]
        except Exception as exc:
            warnings.append(f"Location listing failed: {exc}")

    if not sub_id:
        result.resources = []
        if not result.accounts:
            result.error = "No Azure subscriptions visible to this identity."
        return result

    resources: list[dict] = []

    def _add(label: str, fields: dict):
        base = {
            "subscription_id": sub_id,
            "tenant_id": tenant_id,
        }
        base.update(fields)
        resources.append({"label": label, "fields": base})

    if target_kind == TARGET_CLOUD_DB:
        try:
            from azure.mgmt.sql import SqlManagementClient

            sql = SqlManagementClient(credential, sub_id)
            for server in sql.servers.list():
                rg = _resource_group_from_id(getattr(server, "id", "") or "")
                name = server.name or ""
                _add(
                    f"SQL {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "db_service_type": "Microsoft.Sql/servers",
                        "region": getattr(server, "location", "") or "",
                    },
                )
        except Exception as exc:
            warnings.append(f"Azure SQL listing failed: {exc}")
        try:
            from azure.mgmt.mysqlflexibleservers import MySQLManagementClient

            mysql = MySQLManagementClient(credential, sub_id)
            for server in mysql.servers.list():
                rg = _resource_group_from_id(getattr(server, "id", "") or "")
                name = server.name or ""
                _add(
                    f"MySQL Flex {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "db_service_type": "Microsoft.DBforMySQL/flexibleServers",
                        "region": getattr(server, "location", "") or "",
                    },
                )
        except ImportError:
            warnings.append(
                "Install azure-mgmt-mysqlflexibleservers for MySQL discovery."
            )
        except Exception as exc:
            warnings.append(f"MySQL flexible listing failed: {exc}")
        try:
            from azure.mgmt.postgresqlflexibleservers import PostgreSQLManagementClient

            pg = PostgreSQLManagementClient(credential, sub_id)
            for server in pg.servers.list():
                rg = _resource_group_from_id(getattr(server, "id", "") or "")
                name = server.name or ""
                _add(
                    f"PostgreSQL Flex {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "db_service_type": "Microsoft.DBforPostgreSQL/flexibleServers",
                        "region": getattr(server, "location", "") or "",
                    },
                )
        except ImportError:
            warnings.append(
                "Install azure-mgmt-postgresqlflexibleservers for PostgreSQL discovery."
            )
        except Exception as exc:
            warnings.append(f"PostgreSQL flexible listing failed: {exc}")

    elif target_kind == TARGET_VM:
        try:
            from azure.mgmt.compute import ComputeManagementClient

            compute = ComputeManagementClient(credential, sub_id)
            for vm in compute.virtual_machines.list_all():
                rg = _resource_group_from_id(getattr(vm, "id", "") or "")
                name = vm.name or ""
                _add(
                    f"VM {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "region": getattr(vm, "location", "") or "",
                    },
                )
        except ImportError:
            warnings.append(
                "Install azure-mgmt-compute for VM discovery: pip install azure-mgmt-compute"
            )
        except Exception as exc:
            warnings.append(f"VM listing failed: {exc}")

    elif target_kind == TARGET_CLOUD_SERVICE:
        try:
            from azure.mgmt.network import NetworkManagementClient

            net = NetworkManagementClient(credential, sub_id)
            for lb in net.load_balancers.list_all():
                rg = _resource_group_from_id(getattr(lb, "id", "") or "")
                name = lb.name or ""
                _add(
                    f"Load Balancer {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "db_service_type": "Microsoft.Network/loadBalancers",
                        "region": getattr(lb, "location", "") or "",
                    },
                )
        except ImportError:
            warnings.append("Install azure-mgmt-network for load balancer discovery.")
        except Exception as exc:
            warnings.append(f"Load balancer listing failed: {exc}")
        try:
            from azure.mgmt.web import WebSiteManagementClient

            web = WebSiteManagementClient(credential, sub_id)
            for site in web.web_apps.list():
                rg = _resource_group_from_id(getattr(site, "id", "") or "")
                name = site.name or ""
                _add(
                    f"Web App {name} ({rg})",
                    {
                        "resource_name": name,
                        "resource_group": rg,
                        "db_service_type": "Microsoft.Web/sites",
                        "region": getattr(site, "location", "") or "",
                    },
                )
        except ImportError:
            warnings.append("Install azure-mgmt-web for App Service discovery.")
        except Exception as exc:
            warnings.append(f"Web app listing failed: {exc}")

    result.resources = resources
    if not resources and not warnings:
        warnings.append(
            f"No {target_kind} resources found in subscription {sub_id}."
        )
    return result


# ---------------------------------------------------------------------------
# Headless interactive login
# ---------------------------------------------------------------------------

def cli_login(entry: dict):
    """Authenticate Azure for this connection via `az login` (opens a browser).

    Returns (ok, message).
    """
    import subprocess

    tenant_id = (entry.get("tenant_id", "") or "").strip()
    az_cmd = ["az", "login"]
    if tenant_id:
        az_cmd += ["--tenant", tenant_id]
    try:
        result = subprocess.run(
            az_cmd, capture_output=True, text=True,
            timeout=monitor_config.get_int(
                "cloud.azure", "login_timeout_seconds", default=300))
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            return False, f"az login failed: {err[:300]}"
    except FileNotFoundError:
        return False, ("'az' command not found. Install Azure CLI: "
                       "https://aka.ms/installazurecli")
    except subprocess.TimeoutExpired:
        return False, "az login timed out after 5 minutes."
    except Exception as exc:
        return False, f"Azure login error: {exc}"

    try:
        from azure.identity import AzureCliCredential
        AzureCliCredential().get_token("https://management.azure.com/.default")
        return True, "Azure CLI login completed and token verified."
    except Exception as exc:
        return False, f"Login completed but token verification failed: {exc}"


# ---------------------------------------------------------------------------
# Provider spec
# ---------------------------------------------------------------------------

SPEC = CloudProviderSpec(
    name="Azure",
    display_name="Microsoft Azure",
    auth_modes=["keys", "env", "sso", "pwd"],
    build_monitor=build_monitor,
    fetch_metrics=fetch_metrics,
    refresh_monitor=refresh_monitor,
    discover=discover,
)
