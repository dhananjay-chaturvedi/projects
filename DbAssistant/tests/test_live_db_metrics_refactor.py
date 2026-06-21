"""Live integration: DB metrics refactor against real saved connections.

Uses local MariaDB (default test creds) and optional saved profiles:
  DBTOOL_TEST_CONNS=local_mariadb,my_gcp_postgres
"""

from __future__ import annotations

import os

import pytest

MYSQL_HOST = os.environ.get("MYSQL_TEST_HOST", "localhost")
MYSQL_PORT = os.environ.get("MYSQL_TEST_PORT", "3306")
MYSQL_USER = os.environ.get("MYSQL_TEST_USER", "dheeru")
MYSQL_PASS = os.environ.get("MYSQL_TEST_PASS", "dheeru")
MYSQL_DB = os.environ.get("MYSQL_TEST_DB", "test")


def _section_titles(sections):
    return {t for t, _ in (sections or [])}


@pytest.mark.integration
def test_live_mariadb_db_metrics_no_os_section(dbassistant_home, mysql_available):
    from monitoring.service import make_service

    svc = make_service()
    add = svc.add_monitor_db_connection(
        name="live_mariadb_metrics",
        db_type="MariaDB",
        host=MYSQL_HOST,
        port=str(MYSQL_PORT),
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
    )
    assert add["ok"], add

    metrics = svc.get_metrics_monitor_db("live_mariadb_metrics")
    assert metrics.get("error") is None, metrics
    assert _section_titles(metrics.get("sections")) >= {"Connections"}
    assert "Host / OS" not in _section_titles(metrics.get("sections"))
    assert "Buffer Pool Used" in metrics.get("raw_floats", {}) or metrics.get("sections")

    # Engine path threshold: MariaDB resolves to its own namespace
    from monitoring.threshold_checker import ThresholdChecker
    from monitoring.db_metric_config import db_type_path

    checker = ThresholdChecker()
    rule = checker.get_rule(
        "db", "buffer_pool_usage_pct",
        path=db_type_path("MariaDB"), fallback_to_empty=True,
    )
    assert rule is not None
    assert "mariadb" in rule.section_id


@pytest.mark.integration
@pytest.mark.slow
def test_saved_local_mariadb_db_metrics_no_os():
    """Live DB metrics for saved core profile ``local_mariadb`` (real home dir)."""
    from monitoring.service import make_service

    svc = make_service()
    name = "local_mariadb"
    if svc.resolve_connection_source(name) != "db":
        pytest.skip(f"{name} not in core db store")

    test = svc._core.test_connection(name)
    if not test.get("ok"):
        pytest.skip(f"{name} unreachable: {test.get('message')}")

    metrics = svc.get_metrics(name)
    assert metrics.get("error") is None, f"{name}: {metrics}"
    titles = _section_titles(metrics.get("sections"))
    assert "Host / OS" not in titles, f"{name} leaked OS section"
    assert "Connections" in titles or titles, f"{name}: expected DB metric sections"


@pytest.mark.integration
@pytest.mark.slow
def test_saved_gcp_postgres_cloud_metrics_smoke():
    """Smoke: ``my_gcp_postgres`` cloud profile resolves and returns metrics text."""
    from monitoring.service import make_service

    svc = make_service()
    name = "my_gcp_postgres"
    if svc.resolve_connection_source(name) != "cloud":
        pytest.skip(f"{name} not in cloud store")

    result = svc.get_cloud_metrics(name)
    if result.get("error"):
        pytest.skip(f"{name} cloud metrics unavailable: {result.get('error')}")
    assert result.get("sections") or result.get("text"), f"{name}: empty cloud metrics"
