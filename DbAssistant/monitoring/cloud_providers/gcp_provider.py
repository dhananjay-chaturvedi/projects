"""
cloud_providers/gcp_provider.py
================================
GCP provider spec: build_monitor + fetch_metrics for Google Cloud SQL.

Three independent authentication modes are supported, mirroring the UI tabs:

  keys  – Access Keys / Tokens
          One of the following, in priority order:
            * Google auth JSON pasted into ``sa_key_json``
              (service_account or authorized_user)
            * Google auth JSON file path in ``sa_key_path``
              (service_account or authorized_user)
            * OAuth client ID + client secret + refresh token
            * Short-lived OAuth2 access token in ``oauth_token``

  pwd   – Username / Password
          GCP APIs do not accept a Google account password directly, but the
          tab is repurposed for non-interactive service-account activation:
            * username = service-account email (informational)
            * password = path to a JSON key file *or* the JSON content itself
          This mirrors ``gcloud auth activate-service-account`` and works
          without a browser.

  sso   – Workforce Identity / gcloud
          Runs ``gcloud auth application-default login --no-launch-browser``
          through the UI-provided ``sso_callback`` which displays the
          verification URL, lets the user open it in a browser, and feeds
          back the authorization code that gcloud prints in the browser
          after sign-in.  After a successful login the call falls back to
          Application Default Credentials.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import os

from common.cloud.profiles import TARGET_CLOUD_DB, TARGET_CLOUD_SERVICE, TARGET_VM
from monitoring.cloud_monitor_base import CloudProviderSpec, DiscoveryResult
from monitoring import monitor_config
from monitoring.monitor_config import get_lookback_minutes


# Required keys on a GCP service-account key JSON file. We validate up-front
# so the user gets a useful error rather than the cryptic
# "Service account info was not in the expected format, missing fields ...".
_REQUIRED_SA_FIELDS = (
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
)

_REQUIRED_AUTH_USER_FIELDS = (
    "type",
    "client_id",
    "client_secret",
    "refresh_token",
)

# Cloud SQL System Insights metrics are exposed as cloudsql.googleapis.com
# Cloud Monitoring time series, and logs are exposed through Cloud Logging.
# Use cloud-platform as the single OAuth scope because several ADC /
# Workforce plugins reject ad hoc multi-scope refreshes with invalid_scope.
_GCP_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]


# ---------------------------------------------------------------------------
# Credential building helpers (public — also used by the UI test button)
# ---------------------------------------------------------------------------

def _validate_sa_info(info) -> str | None:
    """Return a human-readable error message if *info* is not a valid GCP
    service-account key dict; return None on success."""
    if not isinstance(info, dict):
        return "Service-account JSON must be a JSON object."
    if info.get("type") != "service_account":
        return (
            "JSON is not a service-account key — expected 'type' field to be "
            f"'service_account' (got '{info.get('type')}').\n"
            "Download the correct file from GCP Console → IAM & Admin → "
            "Service Accounts → <your SA> → Keys → Add Key → JSON."
        )
    missing = [f for f in _REQUIRED_SA_FIELDS if not info.get(f)]
    if missing:
        return (
            "Service-account JSON is missing required field(s): "
            f"{', '.join(missing)}.\n"
            "The file you supplied is not a valid GCP service-account key. "
            "Re-download from GCP Console → IAM & Admin → Service Accounts."
        )
    return None


def _validate_authorized_user_info(info) -> str | None:
    """Validate a gcloud Application Default Credentials user JSON file."""
    if not isinstance(info, dict):
        return "Authorized-user JSON must be a JSON object."
    if info.get("type") != "authorized_user":
        return (
            "JSON is not an authorized-user credential — expected 'type' field "
            f"to be 'authorized_user' (got '{info.get('type')}')."
        )
    missing = [f for f in _REQUIRED_AUTH_USER_FIELDS if not info.get(f)]
    if missing:
        return (
            "Authorized-user JSON is missing required field(s): "
            f"{', '.join(missing)}.\n"
            "Use a valid ADC file from `gcloud auth application-default login`, "
            "or enter OAuth client ID, client secret, and refresh token directly."
        )
    return None


def _validate_google_auth_info(info) -> str | None:
    """Validate supported Google credential JSON types."""
    if not isinstance(info, dict):
        return "Google auth JSON must be a JSON object."
    cred_type = info.get("type")
    if cred_type == "service_account":
        return _validate_sa_info(info)
    if cred_type == "authorized_user":
        return _validate_authorized_user_info(info)
    return (
        "Unsupported Google credential JSON type "
        f"'{cred_type}'. Supported types are 'service_account' and "
        "'authorized_user'.\n"
        "Use a service-account key JSON, a gcloud ADC authorized_user JSON, "
        "or enter OAuth client ID/client secret/refresh token directly."
    )


def _load_google_auth_info_from_path(path: str) -> tuple[dict | None, str | None]:
    """Read and validate a supported Google auth JSON file."""
    if not path:
        return None, "Google auth JSON file path is empty."
    if not os.path.isfile(path):
        return None, f"Google auth JSON file not found: {path}"
    try:
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    except json.JSONDecodeError as exc:
        return None, f"Google auth JSON file is not valid JSON: {exc}"
    except Exception as exc:
        return None, f"Failed to read Google auth JSON file: {exc}"
    err = _validate_google_auth_info(info)
    if err:
        return None, err
    return info, None


def _load_google_auth_info_from_json(blob: str) -> tuple[dict | None, str | None]:
    """Parse and validate a pasted Google auth JSON blob."""
    if not blob:
        return None, "Pasted JSON is empty."
    try:
        info = json.loads(blob)
    except json.JSONDecodeError as exc:
        return None, f"Pasted text is not valid JSON: {exc}"
    err = _validate_google_auth_info(info)
    if err:
        return None, err
    return info, None


def _credentials_from_google_auth_info(info: dict, _svc, _gc):
    """Build credentials from supported Google credential JSON."""
    if info.get("type") == "service_account":
        return _svc.Credentials.from_service_account_info(info, scopes=_GCP_SCOPES)
    if info.get("type") == "authorized_user":
        return _gc.Credentials.from_authorized_user_info(info, scopes=_GCP_SCOPES)
    raise ValueError(f"Unsupported credential type: {info.get('type')}")


def _credentials_from_oauth_client(
    client_id: str, client_secret: str, refresh_token: str, _gc
):
    """Build credentials from direct OAuth user credential fields."""
    return _gc.Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=_GCP_SCOPES,
    )


def build_credentials(entry: dict, sso_callback=None):
    """
    Build Google credentials for *entry*.

    Returns ``(credentials, None)`` on success or ``(None, error_string)``
    on failure.  Used by both ``build_monitor`` and the UI test button so
    auth logic stays consistent across both code paths.
    """
    auth_mode = entry.get("auth_mode", "keys")

    try:
        from google.oauth2 import service_account as _svc
        from google.oauth2 import credentials as _gc
        import google.auth
    except ImportError as exc:
        return None, (
            "google-auth libraries are not installed. "
            f"Install with: pip install google-auth google-cloud-monitoring  ({exc})"
        )

    if auth_mode == "keys":
        sa_key_json = (entry.get("sa_key_json", "") or "").strip()
        sa_key_path = (entry.get("sa_key_path", "") or "").strip()
        oauth_token = (entry.get("oauth_token", "") or "").strip()
        oauth_client_id = (entry.get("oauth_client_id", "") or "").strip()
        oauth_client_secret = (entry.get("oauth_client_secret", "") or "").strip()
        oauth_refresh_token = (entry.get("oauth_refresh_token", "") or "").strip()

        if sa_key_json:
            info, err = _load_google_auth_info_from_json(sa_key_json)
            if err:
                return None, err
            try:
                creds = _credentials_from_google_auth_info(info, _svc, _gc)
            except Exception as exc:
                return None, f"Failed to load pasted Google credential JSON: {exc}"
            return creds, None

        if sa_key_path:
            info, err = _load_google_auth_info_from_path(sa_key_path)
            if err:
                return None, err
            try:
                creds = _credentials_from_google_auth_info(info, _svc, _gc)
            except Exception as exc:
                return None, f"Failed to load Google credential JSON file: {exc}"
            return creds, None

        if oauth_client_id or oauth_client_secret or oauth_refresh_token:
            missing = []
            if not oauth_client_id:
                missing.append("OAuth Client ID")
            if not oauth_client_secret:
                missing.append("OAuth Client Secret")
            if not oauth_refresh_token:
                missing.append("OAuth Refresh Token")
            if missing:
                return None, (
                    "Direct OAuth user credentials are incomplete. Missing: "
                    f"{', '.join(missing)}."
                )
            try:
                return _credentials_from_oauth_client(
                    oauth_client_id,
                    oauth_client_secret,
                    oauth_refresh_token,
                    _gc,
                ), None
            except Exception as exc:
                return None, f"Failed to build OAuth user credentials: {exc}"

        if oauth_token:
            return _gc.Credentials(oauth_token, scopes=_GCP_SCOPES), None

        return None, (
            "Provide one of: Google auth JSON file path, pasted Google auth "
            "JSON, OAuth client ID/client secret/refresh token, or an OAuth2 "
            "access token."
        )

    if auth_mode == "pwd":
        username = (entry.get("username", "") or "").strip()
        password = (entry.get("password", "") or "").strip()

        if not password:
            return None, (
                "GCP APIs cannot authenticate a Google account password directly.\n"
                "Put the *path* to a Google auth JSON file (or paste the JSON "
                "itself) into the 'Password / Key' field."
            )

        # Decide whether 'password' is a path or a JSON blob.
        if password.lstrip().startswith("{"):
            info, err = _load_google_auth_info_from_json(password)
        elif os.path.isfile(password):
            info, err = _load_google_auth_info_from_path(password)
        else:
            return None, (
                "GCP cannot use a plain Google account password via API.\n"
                "Treat the 'Password / Key' field as either a path to a "
                "Google auth JSON file or the JSON content itself.\n"
                f"'{password[:80]}' is neither a readable file nor a JSON blob."
            )
        if err:
            return None, err

        # Username is informational here.  If supplied it should match the
        # service-account email; warn (but do not fail) on mismatch.
        client_email = info.get("client_email", "")
        if username and client_email and username.lower() != client_email.lower():
            # The mismatch is surfaced via the help text rather than blocking
            # the build — using the key file's own client_email is always safe.
            pass

        try:
            creds = _credentials_from_google_auth_info(info, _svc, _gc)
        except Exception as exc:
            return None, f"Failed to load Google credential JSON: {exc}"
        return creds, None

    if auth_mode in ("sso", "env"):
        if auth_mode == "sso" and sso_callback is not None:
            try:
                ok, msg = sso_callback()
            except Exception as exc:
                return None, f"gcloud sign-in callback raised: {exc}"
            if not ok:
                return None, msg or "gcloud sign-in was cancelled or failed."

        try:
            creds, _ = google.auth.default(scopes=_GCP_SCOPES)
            return creds, None
        except Exception as exc:
            hint = (
                "On GCE/GKE use the metadata service account, or set "
                "GOOGLE_APPLICATION_CREDENTIALS, or run "
                "`gcloud auth application-default login`."
                if auth_mode == "env"
                else "Run the SSO sign-in or `gcloud auth application-default login` first."
            )
            return None, f"Application Default Credentials are not available. {hint} ({exc})"

    return None, f"Unknown auth_mode '{auth_mode}' for GCP."


# ---------------------------------------------------------------------------
# Build monitor
# ---------------------------------------------------------------------------

def build_monitor(entry: dict, sso_callback=None):
    """
    Return ``(GCPMonitor, None)`` on success or ``(None, error_string)``
    on failure.  ``sso_callback`` is required only when ``auth_mode == 'sso'``.
    """
    project_id = (entry.get("project_id", "") or "").strip()
    if not project_id:
        return None, "GCP Project ID is required."

    try:
        from monitoring.monitor_gcp import GCPMonitor
    except ImportError as exc:
        return None, f"Missing library for GCP monitoring: {exc}"

    creds, err = build_credentials(entry, sso_callback=sso_callback)
    if err:
        return None, f"Failed to initialise GCP monitor: {err}"

    try:
        import googleapiclient.discovery as _disc
        from google.cloud import monitoring_v3

        class _GCPMonitorWithCreds(GCPMonitor):
            def __init__(self, project_id, credentials):
                self.project_id = project_id
                self.credentials = credentials
                self.service = _disc.build(
                    "sqladmin",
                    "v1beta4",
                    credentials=credentials,
                    cache_discovery=False,
                )
                self.metric_client = monitoring_v3.MetricServiceClient(
                    credentials=credentials
                )
                try:
                    self.logging_service = _disc.build(
                        "logging",
                        "v2",
                        credentials=credentials,
                        cache_discovery=False,
                    )
                except Exception:
                    self.logging_service = None

        return _GCPMonitorWithCreds(project_id, creds), None

    except Exception as exc:
        return None, f"Failed to initialise GCP monitor: {exc}"


def refresh_monitor(entry: dict, monitor, sso_callback=None):
    """Refresh cached Google credentials in-place, then health-check.

    ADC / authorized_user / service-account credentials all know how to refresh
    from their cached refresh material without another browser prompt.  If the
    monitor is from an older in-memory build without a credentials attribute,
    rebuild it once using the saved entry.
    """
    credentials = getattr(monitor, "credentials", None)
    if credentials is None:
        return build_monitor(entry, sso_callback=None)

    try:
        from google.auth.transport.requests import Request

        expiry = getattr(credentials, "expiry", None)
        needs_refresh = not getattr(credentials, "valid", False)
        if expiry is not None:
            now = datetime.now(timezone.utc)
            # google-auth historically stores a naive UTC datetime for expiry;
            # normalise to timezone-aware UTC for safe arithmetic.
            if getattr(expiry, "tzinfo", None) is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            needs_refresh = needs_refresh or expiry <= now + timedelta(minutes=5)

        if needs_refresh and hasattr(credentials, "refresh"):
            credentials.refresh(Request())

        errors = monitor.check_health() or []
        if errors:
            return monitor, errors[0]
        return monitor, None
    except Exception as exc:
        return monitor, f"GCP credential refresh failed: {exc}"


# ---------------------------------------------------------------------------
# GCP metric helpers
# ---------------------------------------------------------------------------

_DEFAULT_METRICS = [
    "cpu_utilization",
    "cpu_reserved_cores",
    "memory_utilization",
    "memory_usage",
    "memory_quota",
    "disk_utilization",
    "disk_bytes_used",
    "disk_quota",
    "database_connections",
    "io_read_ops",
    "io_write_ops",
    "disk_read_bytes",
    "disk_write_bytes",
    "network_receive_bytes",
    "network_transmit_bytes",
    "replica_lag_seconds",
    "replication_lag",
    "transaction_count",
    "deadlock_count",
]

_GCP_BYTE_METRICS = frozenset({
    "disk_bytes_used",
    "disk_quota",
    "disk_read_bytes",
    "disk_write_bytes",
    "memory_usage",
    "memory_quota",
    "network_receive_bytes",
    "network_transmit_bytes",
})
_GCP_PERCENT_METRICS = frozenset({
    "cpu_utilization",
    "memory_utilization",
    "disk_utilization",
})


def _gcp_normalise(name: str, value: float) -> tuple[float, str]:
    """Return (graph_value, formatted_display_string)."""
    if name in _GCP_PERCENT_METRICS:
        pct = value * 100
        return pct, f"{pct:>11.1f} %"
    if name in _GCP_BYTE_METRICS:
        gb = value / (1024 ** 3)
        return gb, f"{gb:>10.2f} GB"
    if "connections" in name:
        return value, f"{int(value):>14,}"
    if "count" in name:
        return value, f"{int(value):>14,}"
    if "cores" in name:
        return value, f"{value:>10.2f}"
    if "ops" in name:
        return value, f"{value:>10.1f} /s"
    if "lag" in name:
        return value, f"{value:>11.2f} s"
    return value, f"{value:>14.2f}"


def _gcp_section(name: str) -> str:
    if "cpu" in name:
        return "Performance"
    if "memory" in name:
        return "Memory"
    if "disk" in name:
        if "ops" in name or "bytes" in name:
            return "I/O"
        return "Storage"
    if "connection" in name:
        return "Performance"
    if "io" in name or "ops" in name:
        return "I/O"
    if "network" in name:
        return "Network"
    if "lag" in name:
        return "Latency"
    if "deadlock" in name:
        return "Errors"
    return "Other"


def _gcp_metric_status(resource_id: str, metric_error: str | None) -> str:
    """User-facing reason when Cloud SQL System Insights metrics are absent."""
    if metric_error:
        lower = metric_error.lower()
        if (
            "service_disabled" in lower
            or "monitoring.googleapis.com" in lower
            or "cloud monitoring api has not been used" in lower
            or "api has not been used" in lower
            or "disabled" in lower
        ):
            return (
                "Cloud SQL connection is OK, but System Insights metrics are "
                "served by the Cloud Monitoring API (monitoring.googleapis.com), "
                "which is disabled for this project. Enable that API to read "
                "Cloud SQL System Insights programmatically."
            )
        if "permission" in lower or "403" in lower or "denied" in lower:
            return (
                "Cloud SQL connection is OK, but metrics were denied. Grant "
                "roles/monitoring.viewer on the project to read Cloud SQL "
                "System Insights metrics."
            )
        return (
            "Cloud SQL connection is OK, but System Insights metrics could not "
            f"be read: {metric_error[:180]}"
        )
    return (
        f"No System Insights data for resource '{resource_id}' — verify the "
        "Cloud SQL instance name, project, and roles/monitoring.viewer."
    )


# ---------------------------------------------------------------------------
# Fetch metrics — returns (sections, graph_data, alerts)
# ---------------------------------------------------------------------------

def fetch_metrics(display_name: str, entry: dict, monitor, threshold_checker=None):
    """Return (sections, graph_data, alerts)."""
    graph_data: dict[str, float] = {}
    alerts: list = []
    sections: list = []

    resource_id = entry.get("resource_name", "")

    if hasattr(monitor, "get_instance_summary"):
        try:
            info = monitor.get_instance_summary(resource_id)
        except Exception as exc:
            info = {"error": str(exc)[:120]}
        if info:
            if "error" in info:
                sections.append(("Cloud SQL Instance", [
                    ("Status", f"Cloud SQL Admin API error: {info['error']}")
                ]))
            else:
                items = [
                    ("Name", info.get("name", "")),
                    ("State", info.get("state", "")),
                    ("Database Version", info.get("database_version", "")),
                    ("Region", info.get("region", "")),
                    ("Machine Tier", info.get("tier", "")),
                    ("Availability", info.get("availability_type", "")),
                    ("Disk Size", f"{info.get('disk_size_gb', '')} GB"),
                ]
                if info.get("ip_addresses"):
                    items.append(("IP Addresses", info["ip_addresses"]))
                sections.append(("Cloud SQL Instance  (Admin API)", items))

    if not hasattr(monitor, "get_metrics") or monitor.metric_client is None:
        sections.append(("Cloud Monitoring", [
            ("Status", "Client unavailable — install google-cloud-monitoring")
        ]))
        return sections, graph_data, alerts

    # Build the fetch list from INI rules first (enabled GCP rules whose path
    # matches Cloud SQL). Fall back to the static catalog when no rules apply.
    gcp_path = ("cloudmonitoring", "cloudsql", "database")
    rule_to_type: dict[str, str] = {}
    if threshold_checker:
        for rule in threshold_checker.list_rules(source="gcp", path=gcp_path):
            metric_type = rule.metric_name or monitor.GCP_METRIC_MAP.get(rule.metric, "")
            if metric_type:
                rule_to_type[rule.metric] = metric_type
    if not rule_to_type:
        rule_to_type = {
            name: monitor.GCP_METRIC_MAP[name]
            for name in _DEFAULT_METRICS
            if name in monitor.GCP_METRIC_MAP
        }

    lookback = get_lookback_minutes("gcp")
    try:
        raw = monitor.get_metrics_by_type(
            resource_id, rule_to_type, minutes_back=lookback,
        )
    except Exception as exc:
        sections.append(("Error", [("GCP Cloud Monitoring error", str(exc)[:80])]))
        return sections, graph_data, alerts

    metric_error = raw.pop("__error__", None) if isinstance(raw, dict) else None

    buckets: dict[str, list] = {}
    flat_for_threshold: dict[str, float] = {}

    for name, datapoints in raw.items():
        if not datapoints:
            continue
        try:
            v = float(datapoints[-1]["value"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        graph_val, display_val = _gcp_normalise(name, v)
        graph_data[f"{display_name}_{name}"] = graph_val
        flat_for_threshold[name] = graph_val

        sec = _gcp_section(name)
        label = name.replace("_", " ").title()
        buckets.setdefault(sec, []).append((label, display_val))

    if not buckets:
        sections.append(("Cloud Monitoring", [
            ("Status",
             _gcp_metric_status(resource_id, metric_error))
        ]))
    else:
        for sec in ("Performance", "Memory", "Storage", "I/O", "Network", "Latency", "Errors", "Other"):
            if sec in buckets:
                label = f"{sec}  (Cloud SQL System Insights, last {lookback} min)" if sec == "Performance" else sec
                sections.append((label, buckets[sec]))

    if threshold_checker and flat_for_threshold:
        alerts = threshold_checker.check_many(
            "gcp", flat_for_threshold, instance_id=display_name, path=gcp_path,
        )

    if hasattr(monitor, "get_recent_logs"):
        try:
            logs_result = monitor.get_recent_logs(resource_id, limit=5)
        except Exception as exc:
            logs_result = {"entries": [], "error": str(exc)[:200]}

        # Tolerate the older list-only return type during in-place upgrades.
        if isinstance(logs_result, list):
            logs_result = {"entries": logs_result, "error": None}

        log_entries = logs_result.get("entries") or []
        log_error = logs_result.get("error")

        if log_entries:
            sections.append((
                "Recent Logs  (Cloud Logging)",
                [
                    (
                        log.get("severity", "DEFAULT"),
                        f"{log.get('timestamp', '')}  {log.get('message', '')}",
                    )
                    for log in log_entries
                ],
            ))
        elif log_error:
            sections.append((
                "Recent Logs  (Cloud Logging)",
                [("Status", f"Logs unavailable: {log_error}")],
            ))

    return sections, graph_data, alerts


# ---------------------------------------------------------------------------
# Environment discovery
# ---------------------------------------------------------------------------

def _gcp_default_project() -> str:
    import os
    import urllib.request

    for key in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"):
        val = (os.environ.get(key) or "").strip()
        if val:
            return val
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(
            req,
            timeout=monitor_config.get_int(
                "cloud.gcp", "metadata_timeout_seconds", default=2),
        ) as resp:
            return resp.read().decode().strip()
    except Exception:
        return ""


def discover(entry: dict, target_kind: str, sso_callback=None) -> DiscoveryResult:
    """List projects, regions, and resources visible to ADC / metadata credentials."""
    warnings: list[str] = []
    result = DiscoveryResult(warnings=warnings)

    try:
        import google.auth
        from googleapiclient import discovery
    except ImportError as exc:
        return DiscoveryResult(
            error=f"google-auth and google-api-python-client required: {exc}"
        )

    try:
        creds, adc_project = google.auth.default(scopes=_GCP_SCOPES)
    except Exception as exc:
        return DiscoveryResult(
            error=(
                "Could not resolve GCP Application Default Credentials. On GCE/GKE "
                "use the attached service account; otherwise set "
                "GOOGLE_APPLICATION_CREDENTIALS or run "
                f"`gcloud auth application-default login`. ({exc})"
            )
        )

    project_id = (entry.get("project_id", "") or "").strip()
    if not project_id:
        project_id = adc_project or _gcp_default_project()
    if project_id:
        result.detected["project_id"] = project_id

    try:
        crm = discovery.build(
            "cloudresourcemanager",
            "v1",
            credentials=creds,
            cache_discovery=False,
        )
        projects: list[dict] = []
        req = crm.projects().list()
        while req is not None:
            resp = req.execute()
            for proj in resp.get("projects", []):
                if proj.get("lifecycleState") == "ACTIVE":
                    pid = proj.get("projectId", "")
                    projects.append(
                        {
                            "id": pid,
                            "label": f"{proj.get('name', pid)} ({pid})",
                        }
                    )
            req = crm.projects().list_next(previous_request=req, previous_response=resp)
        result.accounts = projects
        if not project_id and projects:
            project_id = projects[0]["id"]
            result.detected["project_id"] = project_id
    except Exception as exc:
        warnings.append(f"Project listing failed: {exc}")
        if project_id:
            result.accounts = [{"id": project_id, "label": project_id}]

    if not project_id:
        result.error = "No GCP project ID detected. Set project_id or grant resourcemanager.projects.list."
        return result

    try:
        compute = discovery.build(
            "compute", "v1", credentials=creds, cache_discovery=False
        )
        regions_resp = compute.regions().list(project=project_id).execute()
        result.regions = sorted(
            r.get("name", "")
            for r in regions_resp.get("items", [])
            if r.get("name")
        )
        if result.regions:
            result.detected["region"] = result.regions[0]
    except Exception as exc:
        warnings.append(f"Region listing failed: {exc}")

    resources: list[dict] = []

    def _add(label: str, fields: dict):
        base = {"project_id": project_id}
        base.update(fields)
        resources.append({"label": label, "fields": base})

    if target_kind == TARGET_CLOUD_DB:
        try:
            sqladmin = discovery.build(
                "sqladmin", "v1beta4", credentials=creds, cache_discovery=False
            )
            resp = sqladmin.instances().list(project=project_id).execute()
            for inst in resp.get("items", []) or []:
                name = inst.get("name", "")
                region = (inst.get("region", "") or "").replace("regions/", "")
                _add(
                    f"Cloud SQL {name} ({inst.get('databaseVersion', '')}, {inst.get('state', '')})",
                    {
                        "resource_name": name,
                        "region": region,
                    },
                )
        except Exception as exc:
            warnings.append(f"Cloud SQL listing failed: {exc}")

    elif target_kind == TARGET_VM:
        try:
            compute = discovery.build(
                "compute", "v1", credentials=creds, cache_discovery=False
            )
            req = compute.instances().aggregatedList(project=project_id)
            while req is not None:
                resp = req.execute()
                for _zone, data in (resp.get("items") or {}).items():
                    for inst in data.get("instances") or []:
                        name = inst.get("name", "")
                        zone = (inst.get("zone", "") or "").split("/")[-1]
                        status = inst.get("status", "")
                        _add(
                            f"GCE {name} ({zone}, {status})",
                            {
                                "resource_name": name,
                                "region": zone,
                            },
                        )
                req = compute.instances().aggregatedList_next(
                    previous_request=req, previous_response=resp
                )
        except Exception as exc:
            warnings.append(f"Compute Engine listing failed: {exc}")

    elif target_kind == TARGET_CLOUD_SERVICE:
        try:
            redis = discovery.build(
                "redis", "v1", credentials=creds, cache_discovery=False
            )
            parent = f"projects/{project_id}/locations/-"
            resp = redis.projects().locations().instances().list(parent=parent).execute()
            for inst in resp.get("instances", []) or []:
                name = (inst.get("name", "") or "").split("/")[-1]
                loc = (inst.get("name", "") or "").split("/")[3] if "/" in (inst.get("name") or "") else ""
                _add(
                    f"Memorystore Redis {name}",
                    {
                        "resource_name": name,
                        "region": loc,
                        "cloud_service_type": "redis_instance",
                    },
                )
        except Exception as exc:
            warnings.append(f"Redis listing failed: {exc}")

    result.resources = resources
    if not resources and not warnings:
        warnings.append(
            f"No {target_kind} resources found in project {project_id}."
        )
    return result


# ---------------------------------------------------------------------------
# Headless interactive login
# ---------------------------------------------------------------------------

def cli_login(entry: dict):
    """Authenticate GCP via `gcloud auth application-default login`.

    Sets up Application Default Credentials (ADC) that the GCP client
    libraries consume. Returns (ok, message).
    """
    import subprocess

    project_id = (entry.get("project_id", "") or "").strip()
    cmd = ["gcloud", "auth", "application-default", "login"]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=monitor_config.get_int(
                "cloud.gcp", "login_timeout_seconds", default=300))
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            return False, f"gcloud login failed: {err[:300]}"
    except FileNotFoundError:
        return False, ("'gcloud' command not found. Install the Google Cloud SDK: "
                       "https://cloud.google.com/sdk/docs/install")
    except subprocess.TimeoutExpired:
        return False, "gcloud login timed out after 5 minutes."
    except Exception as exc:
        return False, f"GCP login error: {exc}"

    try:
        import google.auth
        google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        msg = "gcloud ADC login completed and credentials verified."
        if project_id:
            msg += f" (project: {project_id})"
        return True, msg
    except Exception as exc:
        return False, f"Login completed but credential verification failed: {exc}"


# ---------------------------------------------------------------------------
# Provider spec
# ---------------------------------------------------------------------------

SPEC = CloudProviderSpec(
    name="GCP",
    display_name="Google Cloud Platform",
    auth_modes=["keys", "env", "pwd", "sso"],
    build_monitor=build_monitor,
    fetch_metrics=fetch_metrics,
    refresh_monitor=refresh_monitor,
    discover=discover,
)
