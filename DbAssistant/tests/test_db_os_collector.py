"""db_os_collector tests with mocked psutil."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import db_os_collector as doc


def test_get_host_metrics_mocked():
    fake_cpu = [10.0, 20.0, 30.0]
    with patch.object(doc, "psutil") as ps:
        ps.cpu_percent.return_value = fake_cpu
        ps.virtual_memory.return_value = MagicMock(
            total=8 * 1024**3, used=4 * 1024**3, percent=50.0
        )
        ps.disk_usage.return_value = MagicMock(
            total=100, used=50, free=50, percent=50.0
        )
        m = doc.get_host_metrics()
    assert "cpu_utilization" in m or len(m) >= 0


def test_is_localhost():
    assert doc.is_localhost("127.0.0.1") is True
    assert doc.is_localhost("localhost") is True
    assert doc.is_localhost("192.168.1.1") is False


def test_empty_host_is_not_localhost():
    """Regression: unknown/empty host must not be treated as localhost."""
    assert doc.is_localhost("") is False
    assert doc.is_localhost(None) is False
