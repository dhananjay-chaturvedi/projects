"""
monitor_config.py
=================
Loader + writer for ``monitor_config.ini`` — the Monitoring module's own
configuration file, owned by the module and shipped independently of the
shared ``config.ini`` / ``properties.ini``.

It holds everything the monitor needs: UI refresh cadence, keepalive
intervals, SSH timeouts, graph dimensions, per-provider cloud-metrics
lookback, and alert-notification routing (non-secret keys only — secrets stay
in the encrypted store).

Resolution order for reads:

    monitor_config.ini  ->  monitor_config.ini.example  ->  built-in defaults

The ``.example`` is the shipped default; a live ``monitor_config.ini`` is
created (from the example, preserving its comments) the first time a value is
saved via the Monitor settings UI, ``dbtool monitor config set`` or the API.
The file is re-read automatically when its modification time changes, so edits
take effect on the next poll without a restart.
"""

from __future__ import annotations

import configparser
import shutil
import threading
from pathlib import Path

_DIR = Path(__file__).resolve().parent
_LIVE = _DIR / "monitor_config.ini"
_EXAMPLE = _DIR / "monitor_config.ini.example"

# Built-in defaults — the final fallback when neither file exists and the
# per-key fallback for any missing/invalid value. Mirrors the shipped example.
DEFAULTS: dict[str, dict[str, str]] = {
    "monitoring": {
        "metrics_refresh_interval": "5000",
        "max_graph_data_points": "60",
        "cloud_keepalive_interval": "300",
        "db_keepalive_interval": "120",
        "db_keepalive_skip_if_polled_within": "60",
        "ssh_keepalive_interval": "240",
        "db_metric_skip_ping_if_used_within": "0",
        "cloud_health_skip_if_used_within": "0",
        "ssh_keepalive_skip_if_used_within": "0",
        "cloud_force_refresh_interval": "1800",
        "standalone_poll_interval": "10",
        # Default poll interval (seconds) for the CLI monitor/daemon commands.
        "default_poll_interval": "30",
        # Default disk mount path used for host/remote OS disk metrics.
        "default_disk_path": "/",
        # Directory for standalone cloud-runner log/PID files.
        "standalone_log_dir": "logs",
        # Sustained-breach counter retention TTL (seconds; default 24h).
        "sustained_breach_ttl_seconds": "86400",
        # Sustained-breach in-memory cleanup cadence (seconds).
        "sustained_breach_gc_interval_seconds": "300",
    },
    "ssh.connection": {
        "ssh_timeout": "30",
        "ssh_test_timeout": "5",
        "ssh_control_persist": "600",
        "ssh_os_detection_timeout": "15",
        "ssh_monitoring_timeout": "30",
        # Default SSH port when a connection profile does not specify one.
        "default_ssh_port": "22",
        # Extra seconds added to ssh_test_timeout for the wrapping subprocess.
        "ssh_test_timeout_padding": "5",
        # Extra seconds added to ssh_monitoring_timeout for the subprocess.
        "ssh_monitoring_timeout_padding": "10",
    },
    "monitoring.graphs": {
        "metric_graph_width": "250",
        "metric_graph_height": "70",
    },
    "monitoring.limits": {
        "max_data_points": "60",
        # Default alert-history rows returned by the CLI/API alert listing.
        "alerts_default_limit": "50",
        # Default alert-history rows returned by the service layer.
        "alerts_service_default_limit": "100",
    },
    "cloud.lookback": {
        "aws_lookback_minutes": "10",
        "azure_lookback_minutes": "15",
        "gcp_lookback_minutes": "15",
    },
    "cloud.aws": {
        "default_region": "us-east-1",
        "pi_cpu_breakdown": "true",
        # AWS CLI SSO/login subprocess timeout (seconds).
        "login_timeout_seconds": "300",
        # EC2 instance-metadata (region) request timeout (seconds).
        "metadata_timeout_seconds": "2",
        # CloudWatch metric aggregation period (seconds).
        "metric_period_seconds": "60",
        # Max CloudWatch log events fetched per poll.
        "cloudwatch_logs_limit": "100",
        # Max RDS log lines tailed per request.
        "rds_log_tail_max_lines": "200",
        # Performance Insights deep-dive lookback window (minutes).
        "performance_insights_lookback_minutes": "60",
        # Performance Insights metric granularity (seconds; PI supports 1/60/300/3600).
        "performance_insights_period_seconds": "300",
    },
    "cloud.azure": {
        # Azure CLI login subprocess timeout (seconds).
        "login_timeout_seconds": "300",
        # Azure Monitor metric request batch size.
        "metric_chunk_size": "20",
        # Azure Monitor metric granularity (ISO-8601 duration).
        "metric_interval": "PT1M",
    },
    "cloud.gcp": {
        # GCP CLI login subprocess timeout (seconds).
        "login_timeout_seconds": "300",
        # GCE metadata-server request timeout (seconds).
        "metadata_timeout_seconds": "2",
        # Max recent Cloud SQL log entries fetched.
        "recent_logs_limit": "5",
        # Cloud Monitoring time-series page size.
        "time_series_page_size": "5",
    },
    "notifications": {
        "enabled": "false",
        "min_severity": "WARNING",
        # Max characters in a delivered Teams/email alert payload.
        "max_message_chars": "20000",
        "teams_timeout_seconds": "15",
        "teams_max_attempts": "2",
        "teams_max_backoff_seconds": "5",
        "teams_enabled": "false",
        "email_enabled": "false",
        "smtp_host": "",
        "smtp_port": "587",
        "smtp_use_tls": "true",
        "smtp_username": "",
        "email_from": "",
        "email_to": "",
    },
}

_LOOKBACK_MIN = 1
_LOOKBACK_MAX = 1440  # 24h

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}

_lock = threading.RLock()
_parser: configparser.ConfigParser | None = None
_loaded_from: Path | None = None
_loaded_mtime: float | None = None


# --------------------------------------------------------------------------- #
# Internal loading
# --------------------------------------------------------------------------- #
def _config_path() -> Path | None:
    if _LIVE.exists():
        return _LIVE
    if _EXAMPLE.exists():
        return _EXAMPLE
    return None


def _load(force: bool = False) -> configparser.ConfigParser:
    """Return a parser for monitor_config.ini, re-reading when the file's path
    or modification time has changed since the last load.
    """
    global _parser, _loaded_from, _loaded_mtime
    with _lock:
        path = _config_path()
        try:
            mtime = path.stat().st_mtime if path is not None else None
        except OSError:
            mtime = None

        if (
            _parser is not None
            and not force
            and path == _loaded_from
            and mtime == _loaded_mtime
        ):
            return _parser

        parser = configparser.ConfigParser(inline_comment_prefixes=("#", ";"))
        if path is not None:
            try:
                parser.read(path, encoding="utf-8")
            except (OSError, configparser.Error):
                pass  # fall back to DEFAULTS via empty parser

        _parser = parser
        _loaded_from = path
        _loaded_mtime = mtime
        return parser


def reload() -> None:
    """Force a re-read of monitor_config.ini on the next access."""
    _load(force=True)


def _default(section: str, key: str) -> str | None:
    sec = DEFAULTS.get(section)
    if sec is None:
        return None
    return sec.get(key)


# --------------------------------------------------------------------------- #
# Typed reads
# --------------------------------------------------------------------------- #
def get(section: str, key: str, default: str | None = None) -> str | None:
    parser = _load()
    raw = parser.get(section, key, fallback=None)
    if raw is None:
        raw = _default(section, key)
    if raw is None:
        return default
    return raw


def get_int(section: str, key: str, default: int = 0) -> int:
    raw = get(section, key, None)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default


def get_float(section: str, key: str, default: float = 0.0) -> float:
    raw = get(section, key, None)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return default


def get_bool(section: str, key: str, default: bool = False) -> bool:
    raw = get(section, key, None)
    if raw is None:
        return default
    val = str(raw).strip().lower()
    if val in _TRUE:
        return True
    if val in _FALSE:
        return False
    return default


def get_lookback_minutes(provider: str) -> int:
    """Cloud-metrics lookback window (minutes) for ``provider`` (aws/azure/gcp),
    clamped to ``[1, 1440]``; falls back to the built-in default.
    """
    key = (provider or "").strip().lower()
    default = int(DEFAULTS["cloud.lookback"].get(f"{key}_lookback_minutes", "10"))
    val = get_int("cloud.lookback", f"{key}_lookback_minutes", default)
    return max(_LOOKBACK_MIN, min(_LOOKBACK_MAX, val))


# --------------------------------------------------------------------------- #
# Writes (comment-preserving) to the live monitor_config.ini
# --------------------------------------------------------------------------- #
def _ensure_live_file() -> Path:
    """Make sure a live monitor_config.ini exists, seeding it from the example
    (to preserve the documented comments) the first time it is written.
    """
    if not _LIVE.exists() and _EXAMPLE.exists():
        shutil.copy2(_EXAMPLE, _LIVE)
    return _LIVE


def set_value(section: str, key: str, value: str) -> None:
    """Persist ``[section] key = value`` to the live monitor_config.ini,
    preserving comments and layout. Creates the live file from the example on
    first write.
    """
    from common.config.ini_writer import set_ini_value

    with _lock:
        path = _ensure_live_file()
        set_ini_value(path, section, key, "" if value is None else str(value))
        reload()


def restore_defaults() -> None:
    """Reset monitor_config.ini to the shipped example (defaults)."""
    with _lock:
        if _EXAMPLE.exists():
            shutil.copy2(_EXAMPLE, _LIVE)
        elif _LIVE.exists():
            _LIVE.unlink()
        reload()


def config_path() -> Path | None:
    """Path the loader is currently reading from (live, else example, else None)."""
    return _config_path()


def live_path() -> Path:
    """Path of the live monitor_config.ini (whether or not it exists yet)."""
    return _LIVE
