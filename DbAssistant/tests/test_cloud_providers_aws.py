"""AWS cloud provider tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from monitoring.cloud_providers import aws_provider as aws


def test_build_monitor_without_keys_uses_default_chain(monkeypatch):
    """Without static keys, build_monitor falls back to the default credential
    chain (named profile / `aws login` / SSO / env / instance role)."""
    import monitoring.monitor_aws as maws

    created = {}

    class FakeMonitor:
        def __init__(self, region=None, profile=None):
            created["region"] = region
            created["profile"] = profile

    monkeypatch.setattr(maws, "AWSMonitor", FakeMonitor)
    mon, err = aws.build_monitor({"region": "us-east-1", "sso_profile": "myprof"})
    assert err is None
    assert isinstance(mon, FakeMonitor)
    assert created == {"region": "us-east-1", "profile": "myprof"}


def test_build_monitor_default_chain_failure_message(monkeypatch):
    """If the default credential chain cannot authenticate, the error guides
    the user to run `aws login`."""
    import monitoring.monitor_aws as maws

    class FailingMonitor:
        def __init__(self, region=None, profile=None):
            raise RuntimeError("no credentials")

    monkeypatch.setattr(maws, "AWSMonitor", FailingMonitor)
    mon, err = aws.build_monitor({"region": "us-east-1"})
    assert mon is None
    assert err and "aws login" in err.lower()


def test_refresh_monitor_healthy():
    monitor = MagicMock()
    monitor.check_health.return_value = []
    mon, err = aws.refresh_monitor({}, monitor)
    assert err is None
    assert mon is monitor


def test_refresh_monitor_rebuild_on_health_error(monkeypatch):
    monitor = MagicMock()
    monitor.check_health.return_value = ["RDS unreachable"]
    rebuilt = MagicMock()
    monkeypatch.setattr(aws, "build_monitor", lambda e: (rebuilt, None))
    mon, err = aws.refresh_monitor({"access_key_id": "a", "secret_access_key": "b"}, monitor)
    assert err is None
    assert mon is rebuilt


def test_fetch_metrics_skips_malformed_and_non_finite_datapoints():
    monitor = MagicMock()
    monitor.get_rds_metrics.return_value = {
        "CPUUtilization": {"value": 92.5},
        "Broken": {},
        "NaNMetric": {"value": float("nan")},
        "TextMetric": {"value": "not-number"},
    }
    sections, graphs, alerts = aws.fetch_metrics(
        "prod",
        {"resource_name": "db-prod"},
        monitor,
        threshold_checker=None,
    )
    assert any("Performance" in title for title, _ in sections)
    assert graphs == {"prod_CPUUtilization": 92.5}
    assert alerts == []


@pytest.mark.integration
def test_aws_live_caller_identity(aws_available):
    import boto3

    ident = boto3.client("sts").get_caller_identity()
    assert "Account" in ident
