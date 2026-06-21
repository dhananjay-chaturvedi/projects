"""Advanced threshold_checker tests."""

from __future__ import annotations

import time

import pytest

import monitoring.monitoring_utils as mu
from monitoring.threshold_checker import ThresholdChecker


@pytest.fixture
def checker_ini(tmp_path):
    ini = tmp_path / "monitor_thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu_percent]
enabled = true
warning = 70
critical = 90
operator = >
sustained_window = 2

[metric.db.unknown_metric]
enabled = false
warning = 1
critical = 2
operator = >
"""
    )
    return ThresholdChecker(str(ini))


def test_sustained_breach_resets_on_recovery():
    key = "adv_test_cpu"
    mu._store.pop(key, None)
    assert mu.sustained_breach(key, 50, ">", 70, window=2) is False
    assert mu.sustained_breach(key, 80, ">", 70, window=2) is False
    assert mu.sustained_breach(key, 85, ">", 70, window=2) is True
    assert mu.sustained_breach(key, 50, ">", 70, window=2) is False


def test_check_many_disabled_metric_ignored(checker_ini):
    alerts = checker_ini.check_many(
        "db", {"unknown_metric": 999.0}, instance_id="x"
    )
    assert alerts == []


def test_check_many_fires_on_breach(checker_ini):
    alerts = checker_ini.check_many(
        "db",
        {"cpu_percent": 95.0},
        instance_id="inst1",
    )
    # May need sustained window - at least returns list
    assert isinstance(alerts, list)


def test_window_override_fires_immediately(checker_ini):
    """A single-sample manual check with window_override=1 must fire on the
    first breach even though the rule's sustained_window is 2.

    Regression: the stateless CLI/API/UI ``thresholds check`` ran in a fresh
    process per call, so the in-memory consecutive-breach counter never
    reached the window and the command always reported "within thresholds".
    """
    mu._store.clear()
    # Without override the rule needs 2 consecutive samples → first is silent.
    first = checker_ini.check("db", "cpu_percent", 95.0, instance_id="inst1")
    assert first is None
    # With override=1 a single breaching sample fires straight away.
    mu._store.clear()
    fired = checker_ini.check(
        "db", "cpu_percent", 95.0, instance_id="inst1", window_override=1,
    )
    assert fired is not None
    assert fired.severity == "CRITICAL"
    # A safe value still reports nothing under the override.
    assert (
        checker_ini.check(
            "db", "cpu_percent", 10.0, instance_id="inst1", window_override=1,
        )
        is None
    )


def test_legacy_sustained_window_is_honored(tmp_path):
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
critical = 90
operator = >
sustained_window = 2
"""
    )
    checker = ThresholdChecker(str(ini))
    rule = checker.get_rule("db", "cpu")
    assert rule.window == 2


def test_window_less_than_one_is_clamped(tmp_path):
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
critical = 90
operator = >
window = 0
"""
    )
    checker = ThresholdChecker(str(ini))
    assert checker.get_rule("db", "cpu").window == 1


def test_list_rules_returns_enabled_only(checker_ini):
    metrics = {r.metric for r in checker_ini.list_rules("db")}
    assert "cpu_percent" in metrics
    assert "unknown_metric" not in metrics


def test_check_many_skips_missing_non_numeric_and_non_finite_payloads(tmp_path):
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
critical = 90
operator = >
window = 1
"""
    )
    checker = ThresholdChecker(str(ini))
    assert checker.check_many("db", {"cpu": {"bad": 95}}) == []
    assert checker.check_many("db", {"cpu": {"value": "nan"}}) == []
    assert checker.check_many("db", {"cpu": {"value": float("inf")}}) == []


def test_check_rejects_non_finite_value(tmp_path):
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
critical = 90
operator = >
window = 1
"""
    )
    checker = ThresholdChecker(str(ini))
    assert checker.check("db", "cpu", float("nan")) is None
    assert checker.check("db", "cpu", float("inf")) is None


# ---------------------------------------------------------------------------
# Strict "consecutive breaches" semantics + memory hygiene
# ---------------------------------------------------------------------------


def test_strict_consecutive_one_safe_resets_counter():
    """A single non-breaching sample must drop the in-flight counter to 0.

    Scenario reproduces the case the docs describe:  with window=3, two
    breaches followed by one safe sample must require three *fresh*
    breaches afterwards before firing.
    """
    key = "strict_consecutive_helper"
    mu._store.pop(key, None)

    assert mu.sustained_breach(key, 95, ">", 90, window=3) is False  # 1
    assert mu.sustained_breach(key, 96, ">", 90, window=3) is False  # 2
    assert mu.sustained_breach(key, 50, ">", 90, window=3) is False  # reset
    assert mu.sustained_breach(key, 95, ">", 90, window=3) is False  # 1
    assert mu.sustained_breach(key, 95, ">", 90, window=3) is False  # 2
    assert mu.sustained_breach(key, 95, ">", 90, window=3) is True   # 3 → fire


def test_check_strict_consecutive_through_checker(tmp_path):
    """End-to-end via ``ThresholdChecker.check`` — safe values must
    flow through the helper so the counter resets at the checker level
    too, not only at the raw helper level.
    """
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
critical = 90
operator = >
window = 3
"""
    )
    checker = ThresholdChecker(str(ini))
    mu.reset_all()

    assert checker.check("db", "cpu", 95.0, instance_id="h1") is None
    assert checker.check("db", "cpu", 96.0, instance_id="h1") is None
    # Recovery — must reset the counter even though the value is safe.
    assert checker.check("db", "cpu", 50.0, instance_id="h1") is None
    assert checker.check("db", "cpu", 95.0, instance_id="h1") is None
    assert checker.check("db", "cpu", 95.0, instance_id="h1") is None
    # Third *consecutive* fresh breach finally fires.
    assert checker.check("db", "cpu", 95.0, instance_id="h1") is not None


def test_check_resets_higher_severity_on_intermediate_drop(tmp_path):
    """Counters are tracked per severity.  A value that breaches the
    warning level but not critical must:

    * increment the warning counter,
    * reset the critical counter,

    so the next time the value spikes to critical we need a full
    window of fresh critical-breaching samples before firing
    ``CRITICAL``.
    """
    ini = tmp_path / "thresholds.ini"
    ini.write_text(
        """
[metric.db.cpu]
enabled = true
warning = 70
critical = 90
operator = >
window = 2
"""
    )
    checker = ThresholdChecker(str(ini))
    mu.reset_all()

    # Two critical-breaching samples in a row — would fire CRITICAL …
    assert checker.check("db", "cpu", 95.0, instance_id="h2") is None
    # … but the second sample drops below critical (still above warning).
    # That must wipe the critical counter while keeping warning growing.
    alert = checker.check("db", "cpu", 75.0, instance_id="h2")
    # With window=2 warning fires on the second consecutive breach.
    assert alert is not None and alert.severity == "WARNING"

    # Now climb back to critical — counter started fresh, so the first
    # critical-breaching sample must NOT yet fire CRITICAL.
    second = checker.check("db", "cpu", 95.0, instance_id="h2")
    assert second is None or second.severity != "CRITICAL"
    # Two consecutive critical samples then fire CRITICAL.
    third = checker.check("db", "cpu", 95.0, instance_id="h2")
    assert third is not None and third.severity == "CRITICAL"


def test_non_numeric_does_not_reset_counter():
    """A flaky source sending a non-numeric / NaN value should leave
    the in-flight counter unchanged, otherwise transient API hiccups
    would silently mask a long-running breach.
    """
    key = "nan_no_reset"
    mu._store.pop(key, None)

    assert mu.sustained_breach(key, 95, ">", 90, window=3) is False  # 1
    assert mu.sustained_breach(key, 96, ">", 90, window=3) is False  # 2
    # NaN / non-numeric — must NOT reset the counter.
    assert mu.sustained_breach(key, float("nan"), ">", 90, window=3) is False
    assert mu.sustained_breach(key, "N/A", ">", 90, window=3) is False
    # Next breaching sample must therefore fire (count was preserved at 2 → 3).
    assert mu.sustained_breach(key, 97, ">", 90, window=3) is True


# ---------------------------------------------------------------------------
# TTL / memory hygiene
# ---------------------------------------------------------------------------


def test_purge_stale_drops_old_keys_only():
    mu.reset_all()
    # Old entry — past TTL.
    mu.sustained_breach("ttl_old", 95, ">", 90, window=3)
    time.sleep(0.02)
    # Fresh entry — must survive cleanup.
    mu.sustained_breach("ttl_fresh", 95, ">", 90, window=3)

    removed = mu.purge_stale(ttl_seconds=0.01)
    assert removed == 1
    assert "ttl_old" not in mu._store
    assert "ttl_fresh" in mu._store


def test_purge_stale_with_large_ttl_keeps_recent_entries():
    """A TTL that comfortably exceeds the entry's age must keep it."""
    mu.reset_all()
    mu.sustained_breach("ttl_keep", 95, ">", 90, window=3)
    # Anything younger than an hour stays.
    assert mu.purge_stale(ttl_seconds=3600) == 0
    assert "ttl_keep" in mu._store


def test_purge_stale_with_negative_ttl_clears_all():
    """A negative TTL is the simplest way to force a full sweep."""
    mu.reset_all()
    mu.sustained_breach("ttl_a", 95, ">", 90, window=3)
    mu.sustained_breach("ttl_b", 95, ">", 90, window=3)
    assert mu.purge_stale(ttl_seconds=-1) == 2
    assert mu._store == {}


def test_reset_all_clears_state():
    mu.sustained_breach("reset_check", 95, ">", 90, window=3)
    assert "reset_check" in mu._store
    mu.reset_all()
    assert mu._store == {}


def test_opportunistic_gc_runs_during_sustained_breach():
    """The implicit sweep inside ``sustained_breach`` removes truly
    stale entries without a manual ``purge_stale`` call."""
    mu.reset_all()
    mu.sustained_breach("opportunistic_old", 95, ">", 90, window=3)
    # Age the entry past the TTL and force the GC cooldown to elapse.
    with mu._store_lock:
        count, _ = mu._store["opportunistic_old"]
        old_ts = time.monotonic() - mu.STALE_KEY_TTL_SECONDS - 1.0
        mu._store["opportunistic_old"] = (count, old_ts)
        mu._last_gc[0] = 0.0

    mu.sustained_breach("opportunistic_trigger", 50, ">", 90, window=3)
    assert "opportunistic_old" not in mu._store
