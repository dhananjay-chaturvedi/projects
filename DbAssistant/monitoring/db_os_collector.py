"""
db_os_collector.py
==================
Collects OS-level metrics (CPU, memory, disk, load, swap) using psutil.

Used by the SSH / Server Monitoring path and local ``dbtool monitor os`` CLI
to surface host CPU, memory, disk, load, and network metrics.

DB connection monitoring does **not** call this module — OS metrics belong
exclusively to SSH server monitoring.

Usage
-----
    from db_os_collector import get_host_metrics

    metrics = get_host_metrics()
    # metrics = {
    #   "cpu_utilization":    float (%),
    #   "free_memory_mb":     float (MB),
    #   "memory_utilization": float (%),
    #   "free_disk_gb":       float (GB),
    #   "disk_utilization":   float (%),
    #   "load_avg_1m":        float,
    #   "swap_used_mb":       float (MB),
    # }
"""

from __future__ import annotations

import os

try:
    import psutil
    _PSUTIL_OK = True
except ImportError:
    _PSUTIL_OK = False

# Hosts that mean "this machine" (empty/unknown host is NOT localhost)
_LOCALHOST_NAMES = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}


def is_localhost(host: str | None) -> bool:
    """Return True when *host* explicitly refers to the local machine."""
    h = (host or "").lower().strip()
    if not h:
        return False
    return h in _LOCALHOST_NAMES


def get_host_metrics(disk_path: str = "") -> dict[str, float]:
    """
    Return a dict of OS metric values collected via psutil.

    Keys match the metric names used in monitor_thresholds.ini [metric.os.*].

    Returns an empty dict if psutil is not installed or any collection fails.
    """
    if not _PSUTIL_OK:
        return {}
    if not disk_path:
        try:
            from monitoring import monitor_config
            disk_path = monitor_config.get(
                "monitoring", "default_disk_path", default="/") or "/"
        except Exception:
            disk_path = "/"

    result: dict[str, float] = {}

    try:
        result["cpu_utilization"] = psutil.cpu_percent(interval=None)
    except Exception:
        pass

    try:
        vm = psutil.virtual_memory()
        result["free_memory_mb"]     = vm.available / (1024 ** 2)
        result["memory_utilization"] = vm.percent
    except Exception:
        pass

    try:
        du = psutil.disk_usage(disk_path)
        result["free_disk_gb"]     = du.free / (1024 ** 3)
        result["disk_utilization"] = du.percent
    except Exception:
        pass

    try:
        # getloadavg is Unix-only; silently skip on Windows
        la = os.getloadavg() if hasattr(os, "getloadavg") else psutil.getloadavg()
        result["load_avg_1m"]  = la[0]
        result["load_avg_5m"]  = la[1]
        result["load_avg_15m"] = la[2]
    except Exception:
        pass

    try:
        sm = psutil.swap_memory()
        result["swap_used_mb"] = sm.used / (1024 ** 2)
    except Exception:
        pass

    try:
        # Aggregate all interface counters
        net = psutil.net_io_counters()
        result["net_bytes_recv"] = float(net.bytes_recv)
        result["net_bytes_sent"] = float(net.bytes_sent)
        result["net_errors"]     = float(net.errin + net.errout)
    except Exception:
        pass

    return result
