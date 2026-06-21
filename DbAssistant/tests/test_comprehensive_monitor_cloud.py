"""
Comprehensive monitoring and cloud tests — metrics, thresholds, cloud profiles.

Run:
    pytest tests/test_comprehensive_monitor_cloud.py -v -m comprehensive
"""

from __future__ import annotations

import pytest

from tests.integration_helpers import skip_if_tunnel

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


@pytest.fixture
def monitor_svc():
    from monitoring.service import make_service

    return make_service()


class TestDbMonitoring:
    @pytest.mark.parametrize("conn_name", ["local_mariadb"], ids=str)
    def test_get_metrics_saved_conn(
        self, monitor_svc, saved_db_connection_names, conn_name
    ):
        if conn_name not in saved_db_connection_names:
            pytest.skip(f"{conn_name} not in saved list")
        result = monitor_svc.get_metrics(conn_name)
        if result.get("error"):
            skip_if_tunnel(conn_name, result["error"])
            pytest.fail(result["error"])
        assert "sections" in result
        assert "timestamp" in result

    def test_check_alerts_with_sample_floats(self, monitor_svc):
        raw = {"cpu_percent": 95.0, "memory_percent": 50.0}
        alerts = monitor_svc.check_alerts("manual_test", raw)
        assert isinstance(alerts, list)

    def test_os_metrics(self, monitor_svc):
        result = monitor_svc.get_os_metrics()
        assert isinstance(result, dict)
        assert result.get("error") is None or "cpu" in str(result).lower()


class TestThresholds:
    def test_list_thresholds(self, monitor_svc):
        rows = monitor_svc.list_thresholds()
        assert isinstance(rows, list)

    def test_show_threshold_detail(self, monitor_svc):
        detail = monitor_svc.show_threshold("db", "cpu_percent")
        assert detail is None or isinstance(detail, dict)

    @pytest.mark.parametrize(
        "source,metric,value",
        [
            ("db", "cpu_percent", 99.0),
            ("db", "cpu_percent", 10.0),
            ("os", "disk_percent", 99.0),
        ],
    )
    def test_threshold_check_values(self, monitor_svc, source, metric, value):
        result = monitor_svc.check_threshold(source, metric, value)
        assert isinstance(result, list)

    def test_threshold_check_fires_immediately_on_breach(self, monitor_svc):
        """The shared one-shot check (used by CLI/API/UI) must fire on a
        single breaching sample — it is stateless and cannot rely on the
        sustained-window counter accumulating across separate processes.

        Regression for the default os.cpu_utilization rule (critical > 90).
        """
        breach = monitor_svc.check_threshold("os", "cpu_utilization", 99.0)
        if not breach:
            pytest.skip("os.cpu_utilization rule not configured in this env")
        assert breach[0]["severity"] == "CRITICAL"
        safe = monitor_svc.check_threshold("os", "cpu_utilization", 5.0)
        assert safe == []


class TestCloudProfiles:
    def test_list_cloud_connections(self, monitor_svc):
        profiles = monitor_svc.list_cloud_connections()
        assert isinstance(profiles, list)


def pytest_generate_tests(metafunc):
    if "cloud_name" in metafunc.fixturenames:
        from tests.integration_helpers import load_saved_cloud_connection_names

        names = load_saved_cloud_connection_names()
        if not names:
            names = ["__no_cloud_profiles__"]
        metafunc.parametrize("cloud_name", names, ids=str)


@pytest.mark.slow
def test_cloud_connection_test(monitor_svc, cloud_name):
    if cloud_name == "__no_cloud_profiles__":
        pytest.skip("No cloud profiles configured")
    result = monitor_svc.test_cloud_connection(cloud_name)
    if not result.get("ok"):
        pytest.skip(result.get("message", "cloud test failed"))
    assert result["ok"]


@pytest.mark.slow
def test_cloud_metrics(monitor_svc, cloud_name):
    if cloud_name == "__no_cloud_profiles__":
        pytest.skip("No cloud profiles configured")
    result = monitor_svc.get_cloud_metrics(cloud_name)
    if result.get("error"):
        pytest.skip(result["error"])
    assert isinstance(result, dict)


class TestMonitoringUtils:
    def test_sustained_breach_tracking(self):
        import monitoring.monitoring_utils as mu

        mu._store.clear()
        key = "db:cpu_percent:inst1"
        assert not mu.sustained_breach(key, 99.0, ">", 90.0, window=3)
        mu.sustained_breach(key, 99.0, ">", 90.0, window=3)
        mu.sustained_breach(key, 99.0, ">", 90.0, window=3)
        assert mu.sustained_breach(key, 99.0, ">", 90.0, window=3)
        mu._store.clear()

    def test_db_metric_config_sections_defined(self):
        from monitoring.db_metric_config import SECTION_ORDER

        assert isinstance(SECTION_ORDER, list) and SECTION_ORDER


class TestDashboardService:
    def test_dashboard_collect(self):
        from common.dashboard.service import (
            DashboardCapabilities,
            DashboardRuntime,
            DashboardService,
        )
        from common.headless.db_service import CoreDBService

        svc = CoreDBService()
        dash = DashboardService(
            DashboardRuntime(
                get_active_connections=lambda: {},
                get_saved_connections=lambda: svc.list_connections(),
            ),
            DashboardCapabilities(has_schema=True, has_ai=True, has_monitor=True),
        )
        snapshot = dash.collect()
        assert isinstance(snapshot, dict)
        assert "timestamp" in snapshot
        assert "core" in snapshot
