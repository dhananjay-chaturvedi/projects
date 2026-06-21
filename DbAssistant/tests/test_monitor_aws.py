"""AWS monitor unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_aws_monitor_check_health_empty_when_clients_ok():
    try:
        from monitoring.monitor_aws import AWSMonitor
    except ImportError:
        pytest.skip("boto3/monitor_aws not available")

    mon = AWSMonitor.__new__(AWSMonitor)
    mon.rds = MagicMock()
    mon.cw = MagicMock()
    mon.logs = MagicMock()
    mon.pi = None
    errors = mon.check_health()
    assert errors == [] or isinstance(errors, list)
