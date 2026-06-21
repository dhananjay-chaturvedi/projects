"""GCP cloud provider tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from monitoring.cloud_providers import gcp_provider as gcp


def test_validate_sa_info_missing_type():
    err = gcp._validate_sa_info({"project_id": "p"})
    assert err and "service_account" in err


def test_validate_authorized_user_missing_client_id():
    err = gcp._validate_authorized_user_info({"type": "authorized_user"})
    assert err


def test_refresh_monitor_refreshes_near_expiry(monkeypatch):
    creds = MagicMock()
    # Naive UTC mirrors what google-auth historically stores in `expiry`.
    creds.expiry = (datetime.now(timezone.utc) + timedelta(minutes=2)).replace(tzinfo=None)
    creds.valid = True
    monitor = MagicMock()
    monitor.credentials = creds
    monitor.check_health.return_value = []
    monkeypatch.setattr(
        "google.auth.transport.requests.Request", lambda: MagicMock()
    )
    mon, err = gcp.refresh_monitor({}, monitor)
    assert err is None
    assert creds.refresh.called or creds.valid


def test_fetch_metrics_skips_bad_gcp_points():
    monitor = MagicMock()
    monitor.metric_client = object()
    # gcp_provider.fetch_metrics calls monitor.get_metrics_by_type when no
    # threshold_checker is supplied, falling back to the static catalog
    # keyed by friendly rule_id.
    monitor.get_metrics_by_type.return_value = {
        "cpu_utilization": [{"value": 0.91}],
        "memory_utilization": [{"bad": 1}],
        "disk_utilization": [{"value": float("inf")}],
        "database_connections": [{"value": "not-number"}],
    }
    sections, graphs, alerts = gcp.fetch_metrics(
        "gcp-prod",
        {"resource_name": "dbs-prod"},
        monitor,
        threshold_checker=None,
    )
    assert graphs == {"gcp-prod_cpu_utilization": 91.0}
    assert alerts == []
    assert any("Performance" in title for title, _ in sections)


@pytest.mark.integration
def test_gcp_adc_credentials_load(gcp_adc_available):
    from google.auth import default

    creds, project = default()
    assert creds is not None
