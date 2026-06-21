"""Unit tests for AzureMonitor.get_metrics — verifies the tool fetches
platform-native aggregations at 1-minute granularity and performs NO local
computation on the values.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from monitoring.monitor_azure import AzureMonitor, _read_metric_value


def _dp(ts, *, average=None, total=None, maximum=None, minimum=None, count=None):
    return SimpleNamespace(
        time_stamp=ts,
        average=average,
        total=total,
        maximum=maximum,
        minimum=minimum,
        count=count,
    )


def _metric(name, datapoints):
    return SimpleNamespace(
        name=SimpleNamespace(value=name),
        timeseries=[SimpleNamespace(data=datapoints)],
    )


def _definition(name, primary):
    return SimpleNamespace(
        name=SimpleNamespace(value=name),
        primary_aggregation_type=primary,
    )


def _make_monitor():
    """An AzureMonitor with a mocked monitor_client (no real auth)."""
    mon = AzureMonitor.__new__(AzureMonitor)  # bypass __init__/auth
    mon.monitor_client = MagicMock()
    return mon


def test_read_metric_value_prefers_primary_field():
    ts = datetime.now(timezone.utc)
    dp = _dp(ts, average=10.0, total=600.0)
    # Primary = total → read total, not average
    assert _read_metric_value(dp, "total") == (600.0, "total")
    # Primary = average → read average
    assert _read_metric_value(dp, "average") == (10.0, "average")


def test_read_metric_value_falls_back_when_primary_missing():
    ts = datetime.now(timezone.utc)
    dp = _dp(ts, total=42.0)  # only total populated
    # Preferred is average (None) → fall back to first populated field
    assert _read_metric_value(dp, "average") == (42.0, "total")


def test_read_metric_value_none_when_empty():
    ts = datetime.now(timezone.utc)
    assert _read_metric_value(_dp(ts), "average") == (None, None)


def test_get_metrics_uses_one_minute_interval_and_native_aggregation():
    mon = _make_monitor()
    now = datetime.now(timezone.utc)

    # Azure declares the canonical aggregation per metric.
    mon.monitor_client.metric_definitions.list.return_value = [
        _definition("cpu_percent", "Average"),
        _definition("connection_failed", "Total"),
    ]

    # Azure returns BOTH aggregation fields; the tool must pick each metric's
    # own primary field verbatim (no averaging/summing locally).
    mon.monitor_client.metrics.list.return_value = SimpleNamespace(
        value=[
            _metric("cpu_percent", [_dp(now, average=37.5, total=9999.0)]),
            _metric("connection_failed", [_dp(now, average=1.0, total=12.0)]),
        ]
    )

    out = mon.get_metrics("res-uri", ["cpu_percent", "connection_failed"])

    # 1-minute interval, never PT15M
    _, kwargs = mon.monitor_client.metrics.list.call_args
    assert kwargs["interval"] == "PT1M"
    # Requested aggregations are the union of platform primaries
    assert set(kwargs["aggregation"].split(",")) == {"Average", "Total"}

    # cpu uses Average (37.5), NOT total; connection_failed uses Total (12.0)
    assert out["cpu_percent"][-1]["value"] == 37.5
    assert out["cpu_percent"][-1]["aggregation"] == "average"
    assert out["connection_failed"][-1]["value"] == 12.0
    assert out["connection_failed"][-1]["aggregation"] == "total"


def test_get_metrics_falls_back_when_definitions_unavailable():
    mon = _make_monitor()
    now = datetime.now(timezone.utc)

    # No definitions (e.g. missing permission) → empty primary map
    mon.monitor_client.metric_definitions.list.side_effect = Exception("denied")
    mon.monitor_client.metrics.list.return_value = SimpleNamespace(
        value=[_metric("storage_percent", [_dp(now, average=55.0)])]
    )

    out = mon.get_metrics("res-uri", ["storage_percent"])

    _, kwargs = mon.monitor_client.metrics.list.call_args
    assert kwargs["interval"] == "PT1M"
    # Safe broad request set when the platform didn't declare primaries
    assert set(kwargs["aggregation"].split(",")) == {
        "Average", "Total", "Maximum", "Minimum"
    }
    assert out["storage_percent"][-1]["value"] == 55.0


def test_get_metrics_caches_definitions_per_resource():
    mon = _make_monitor()
    now = datetime.now(timezone.utc)
    mon.monitor_client.metric_definitions.list.return_value = [
        _definition("cpu_percent", "Average"),
    ]
    mon.monitor_client.metrics.list.return_value = SimpleNamespace(
        value=[_metric("cpu_percent", [_dp(now, average=10.0)])]
    )

    mon.get_metrics("res-uri", ["cpu_percent"])
    mon.get_metrics("res-uri", ["cpu_percent"])

    # Definitions fetched once, then served from cache
    assert mon.monitor_client.metric_definitions.list.call_count == 1
