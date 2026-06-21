"""Azure monitor unit tests (mocked)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_azure_monitor_check_health_no_clients():
    try:
        from monitoring.monitor_azure import AzureMonitor
    except ImportError:
        pytest.skip("azure SDK not available")

    mon = AzureMonitor.__new__(AzureMonitor)
    mon.sql_client = MagicMock()
    mon.monitor_client = MagicMock()
    mon.mysql_client = None
    errors = mon.check_health()
    assert isinstance(errors, list)
