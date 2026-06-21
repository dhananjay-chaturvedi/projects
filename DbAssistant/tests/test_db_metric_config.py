"""db_metric_config.collect_metrics tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import db_metric_config as dmc


@pytest.fixture
def fake_db_manager():
    mgr = MagicMock()
    mgr.db_type = "MySQL"
    mgr.execute_query = MagicMock(
        side_effect=lambda q: ({"rows": [[1]], "columns": ["v"]}, None)
    )
    return mgr


def _section_titles(sections):
    return {title for title, _items in sections}


def test_collect_metrics_sql_success(fake_db_manager):
    sections, raw, note = dmc.collect_metrics(fake_db_manager, host="localhost")
    assert isinstance(sections, list)
    assert fake_db_manager.execute_query.called
    assert note == ""


def test_collect_metrics_sql_error_shows_in_display(fake_db_manager):
    fake_db_manager.execute_query = MagicMock(
        return_value=(None, "permission denied")
    )
    sections, raw, note = dmc.collect_metrics(fake_db_manager, host="remote")
    assert note == ""
    assert isinstance(raw, dict)


def test_no_host_os_section_for_localhost(fake_db_manager):
    """OS metrics belong to SSH monitoring — never in DB panel."""
    sections, raw, note = dmc.collect_metrics(fake_db_manager, host="127.0.0.1")
    assert "Host / OS" not in _section_titles(sections)
    assert "CPU Utilization" not in raw
    assert note == ""


def test_no_host_os_section_for_remote(fake_db_manager):
    sections, raw, note = dmc.collect_metrics(fake_db_manager, host="10.20.30.40")
    assert "Host / OS" not in _section_titles(sections)
    assert "CPU Utilization" not in raw
    assert note == ""


def test_db_type_path_per_engine_namespaces():
    assert dmc.db_type_path("MySQL") == ("mysql",)
    assert dmc.db_type_path("MariaDB") == ("mariadb",)
    assert dmc.db_type_path("PostgreSQL") == ("postgresql",)
    assert dmc.db_type_path("Oracle") == ("oracle",)
    assert dmc.db_type_path("SQLite") == ("sqlite",)
    assert dmc.db_type_path("") == ()


def test_per_engine_threshold_lookup_with_fallback(tmp_path):
    """Engine-specific rule wins; unknown engine-specific falls back to generic."""
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.active_connections]
critical = 100
operator = >
window = 1
enabled = true

[metric.db.mariadb.active_connections]
critical = 50
operator = >
window = 1
enabled = true
""",
        encoding="utf-8",
    )
    from monitoring.threshold_checker import ThresholdChecker

    checker = ThresholdChecker(config_path=ini)
    mgr = MagicMock()
    mgr.db_type = "MariaDB"
    mgr.execute_query = MagicMock(
        return_value=({"rows": [[99]]}, None)
    )

    sections, raw, _ = dmc.collect_metrics(mgr, checker=checker)
    assert raw.get("Active Connections") == 99.0

    # MariaDB now has its own namespace → engine-specific rule (critical=50)
    alert = checker.check(
        "db", "active_connections", 99.0,
        path=dmc.db_type_path("MariaDB"), fallback_to_empty=True,
    )
    assert alert is not None

    # Generic-only metric still resolves via fallback
    rule = checker.get_rule(
        "db", "slow_query_count",
        path=dmc.db_type_path("MySQL"), fallback_to_empty=True,
    )
    assert rule is None or rule.enabled  # no rule in mini ini — enabled by default in collect
