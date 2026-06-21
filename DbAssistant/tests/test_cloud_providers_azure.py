"""Azure cloud provider tests (mocked)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from monitoring.cloud_providers import azure_provider as az


def test_build_monitor_missing_subscription():
    mon, err = az.build_monitor({"provider": "Azure"})
    assert mon is None
    assert err


def test_refresh_monitor_sets_token_expiry():
    monitor = MagicMock()
    monitor.credential = MagicMock()
    monitor.credential.get_token.return_value = MagicMock(expires_on=9999999999)
    monitor.check_health.return_value = []
    with patch.object(az, "build_monitor", return_value=(monitor, None)):
        mon, err = az.refresh_monitor({}, monitor)
    assert err is None
    assert getattr(monitor, "_token_expires_on", None) == 9999999999


def test_fetch_metrics_skips_bad_azure_points():
    monitor = MagicMock()
    monitor.get_metrics.return_value = {
        "cpu_percent": [{"value": 81.5, "aggregation": "average"}],
        "memory_percent": [{"bad": 1}],
        "storage_percent": [{"value": float("nan"), "aggregation": "average"}],
        "active_connections": [{"value": "not-number", "aggregation": "total"}],
    }
    monitor.mysql_client = None
    sections, graphs, alerts = az.fetch_metrics(
        "az-prod",
        {
            "subscription_id": "sub",
            "resource_group": "rg",
            "resource_name": "srv",
            "database_name": "db",
            "db_service_type": "Microsoft.Sql/servers",
        },
        monitor,
        threshold_checker=None,
    )
    assert graphs == {"az-prod_cpu_percent": 81.5}
    assert alerts == []
    assert any("Performance" in title for title, _ in sections)


def test_fetch_metrics_surfaces_aggregation_tag():
    """The platform statistic Azure returned must be shown in the row."""
    monitor = MagicMock()
    monitor.get_metrics.return_value = {
        "cpu_percent": [{"value": 42.0, "aggregation": "average"}],
        "connection_failed": [{"value": 7.0, "aggregation": "total"}],
    }
    monitor.mysql_client = None
    sections, graphs, alerts = az.fetch_metrics(
        "az-prod",
        {
            "subscription_id": "sub",
            "resource_group": "rg",
            "resource_name": "srv",
            "database_name": "db",
            "db_service_type": "Microsoft.Sql/servers",
        },
        monitor,
        threshold_checker=None,
    )
    rows = [row for _title, body in sections for row in body]
    rendered = {name: val for name, val in rows}
    assert "[avg]" in rendered.get("cpu_percent", "")
    assert "[total]" in rendered.get("connection_failed", "")
