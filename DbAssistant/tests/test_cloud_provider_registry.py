"""CloudProviderRegistry dispatch tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from monitoring.cloud_provider_registry import CloudProviderRegistry


def test_refresh_monitor_delegates():
    monitor = MagicMock()
    entry = {"provider": "AWS"}
    spec = MagicMock()
    spec.refresh_monitor = MagicMock(return_value=(monitor, None))
    with patch.object(CloudProviderRegistry, "get", return_value=spec):
        mon, err = CloudProviderRegistry.refresh_monitor(entry, monitor)
    assert err is None


def test_refresh_monitor_fallback_build():
    monitor = MagicMock()
    entry = {"provider": "AWS"}
    spec = MagicMock()
    spec.refresh_monitor = None
    spec.build_monitor = MagicMock(return_value=(monitor, None))
    with patch.object(CloudProviderRegistry, "get", return_value=spec):
        mon, err = CloudProviderRegistry.refresh_monitor(entry, monitor)
    assert err is None


def test_get_provider_is_case_insensitive():
    assert CloudProviderRegistry.get("aws") is CloudProviderRegistry.get("AWS")


def test_build_monitor_catches_provider_exception():
    spec = MagicMock()
    spec.build_monitor.side_effect = RuntimeError("boom")
    with patch.object(CloudProviderRegistry, "get", return_value=spec):
        mon, err = CloudProviderRegistry.build_monitor({"provider": "AWS"})
    assert mon is None
    assert "boom" in err


def test_fetch_metrics_catches_provider_exception():
    spec = MagicMock()
    spec.fetch_metrics.side_effect = RuntimeError("api down")
    with patch.object(CloudProviderRegistry, "get", return_value=spec):
        sections, graphs, alerts = CloudProviderRegistry.fetch_metrics(
            "x", {"provider": "AWS"}, MagicMock()
        )
    assert graphs == {}
    assert alerts == []
    assert "api down" in sections[0][1][0][1]


def test_login_catches_provider_exception():
    spec = MagicMock()
    spec.login = MagicMock(side_effect=RuntimeError("login down"))
    with patch.object(CloudProviderRegistry, "get", return_value=spec):
        ok, msg = CloudProviderRegistry.login({"provider": "AWS"})
    assert ok is False
    assert "login down" in msg
