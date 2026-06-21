"""Per-engine DB threshold path with generic fallback."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from monitoring.threshold_checker import ThresholdChecker


@pytest.fixture
def engine_ini(tmp_path: Path) -> ThresholdChecker:
    ini = tmp_path / "thresholds.ini"
    ini.write_text(textwrap.dedent("""\
        [metric.db.cache_hit_ratio]
        critical = 80
        operator = <
        window = 1
        enabled = true

        [metric.db.mysql.cache_hit_ratio]
        critical = 90
        operator = <
        window = 1
        enabled = true

        [metric.db.postgresql.database_size_mb]
        critical = 1000
        operator = >
        window = 1
        enabled = true
    """), encoding="utf-8")
    return ThresholdChecker(config_path=ini)


def test_engine_specific_rule_wins(engine_ini):
    rule = engine_ini.get_rule(
        "db", "cache_hit_ratio", path=("mysql",), fallback_to_empty=True
    )
    assert rule is not None
    assert rule.critical == 90


def test_fallback_to_generic_when_no_engine_rule(engine_ini):
    rule = engine_ini.get_rule(
        "db", "cache_hit_ratio", path=("oracle",), fallback_to_empty=True
    )
    assert rule is not None
    assert rule.critical == 80


def test_exact_path_required_without_fallback(engine_ini):
    assert engine_ini.get_rule("db", "cache_hit_ratio", path=("oracle",)) is None
    assert engine_ini.get_rule("db", "cache_hit_ratio", path=("mysql",)).critical == 90


def test_check_many_uses_fallback(engine_ini):
    alerts = engine_ini.check_many(
        "db", {"cache_hit_ratio": 85.0},
        instance_id="t",
        path=("mysql",),
        fallback_to_empty=True,
    )
    assert len(alerts) == 1
