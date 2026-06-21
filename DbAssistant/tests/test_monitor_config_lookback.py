"""Tests for monitor_config.ini — the module-owned monitoring config that
carries the per-provider cloud-metrics lookback window, and the provider
wiring that consumes it.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import monitoring.monitor_config as mc
from monitoring.cloud_providers import aws_provider as aws
from monitoring.cloud_providers import azure_provider as azure
from monitoring.cloud_providers import gcp_provider as gcp


@pytest.fixture
def lookback_ini(tmp_path, monkeypatch):
    """Point the loader at a temp monitor_config.ini and reload it.

    Yields a writer ``set(text)`` so each test controls the file contents;
    the loader's mtime check means we bump mtime via write_text each time.
    """
    live = tmp_path / "monitor_config.ini"
    monkeypatch.setattr(mc, "_LIVE", live)
    monkeypatch.setattr(mc, "_EXAMPLE", tmp_path / "monitor_config.ini.example")

    def _set(text: str) -> None:
        live.write_text(text, encoding="utf-8")
        mc.reload()

    try:
        yield _set
    finally:
        mc.reload()  # restore real file on the next access


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def test_defaults_from_shipped_file():
    """The repo's live monitor_config.ini ships the documented defaults."""
    mc.reload()
    assert mc.get_lookback_minutes("aws") == 10
    assert mc.get_lookback_minutes("azure") == 15
    assert mc.get_lookback_minutes("gcp") == 15


def test_override_values(lookback_ini):
    lookback_ini(
        "[cloud.lookback]\n"
        "aws_lookback_minutes = 5\n"
        "azure_lookback_minutes = 20\n"
        "gcp_lookback_minutes = 30\n"
    )
    assert mc.get_lookback_minutes("aws") == 5
    assert mc.get_lookback_minutes("azure") == 20
    assert mc.get_lookback_minutes("gcp") == 30


def test_clamping_and_invalid(lookback_ini):
    lookback_ini(
        "[cloud.lookback]\n"
        "aws_lookback_minutes = 0\n"        # below min -> 1
        "azure_lookback_minutes = 99999\n"  # above max -> 1440
        "gcp_lookback_minutes = banana\n"   # invalid -> default 15
    )
    assert mc.get_lookback_minutes("aws") == mc._LOOKBACK_MIN == 1
    assert mc.get_lookback_minutes("azure") == mc._LOOKBACK_MAX == 1440
    assert mc.get_lookback_minutes("gcp") == 15


def test_missing_key_falls_back_to_default(lookback_ini):
    lookback_ini("[cloud.lookback]\naws_lookback_minutes = 7\n")
    assert mc.get_lookback_minutes("aws") == 7
    # azure/gcp keys absent -> built-in defaults
    assert mc.get_lookback_minutes("azure") == 15
    assert mc.get_lookback_minutes("gcp") == 15


def test_unknown_provider_defaults_to_ten(lookback_ini):
    lookback_ini("[cloud.lookback]\n")
    assert mc.get_lookback_minutes("oracle-cloud") == 10


def test_mtime_autoreload(lookback_ini):
    lookback_ini("[cloud.lookback]\naws_lookback_minutes = 5\n")
    assert mc.get_lookback_minutes("aws") == 5
    # Rewriting the file (new mtime) is picked up without an explicit reload.
    lookback_ini("[cloud.lookback]\naws_lookback_minutes = 12\n")
    assert mc.get_lookback_minutes("aws") == 12


# ---------------------------------------------------------------------------
# Provider wiring — each provider passes the configured lookback through.
# ---------------------------------------------------------------------------

def test_gcp_provider_uses_configured_lookback(lookback_ini):
    lookback_ini("[cloud.lookback]\ngcp_lookback_minutes = 22\n")
    monitor = MagicMock()
    monitor.metric_client = object()
    monitor.get_metrics_by_type.return_value = {"cpu_utilization": [{"value": 1.0}]}
    gcp.fetch_metrics("gcp-prod", {"resource_name": "dbs-prod"}, monitor,
                      threshold_checker=None)
    assert monitor.get_metrics_by_type.call_args.kwargs["minutes_back"] == 22


def test_azure_provider_uses_configured_lookback(lookback_ini):
    lookback_ini("[cloud.lookback]\nazure_lookback_minutes = 18\n")
    monitor = MagicMock()
    monitor.get_metrics.return_value = {}
    entry = {
        "subscription_id": "s", "resource_group": "rg",
        "resource_name": "srv", "db_service_type": "Microsoft.DBforMySQL/flexibleServers",
    }
    azure.fetch_metrics("az-prod", entry, monitor, threshold_checker=None)
    assert monitor.get_metrics.call_args.kwargs["minutes_back"] == 18


def test_aws_provider_uses_configured_lookback(lookback_ini):
    lookback_ini("[cloud.lookback]\naws_lookback_minutes = 8\n")
    monitor = MagicMock()
    monitor.get_rds_metrics.return_value = {"CPUUtilization": {"value": 12.0}}
    aws.fetch_metrics("aws-prod", {"resource_name": "rds-prod"}, monitor,
                      threshold_checker=None)
    assert monitor.get_rds_metrics.call_args.kwargs["minutes_back"] == 8
