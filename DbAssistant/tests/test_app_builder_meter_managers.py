"""Tests for the App Builder meter-managers package."""

from __future__ import annotations

from ai_assistant.meters.base import Measurement

from ai_assistant.app_builder.meter_managers import (
    MeterManagerRegistry,
    Severity,
)
from ai_assistant.app_builder.meter_managers.base import MeterManager
from ai_assistant.app_builder.meters import AppMeterRegistry


def _m(meter, score, threshold=0.7, **kw):
    return Measurement(meter=meter, score=score, threshold=threshold, **kw)


def test_manager_ok_when_meter_passes():
    mgr = MeterManagerRegistry()
    sig = mgr.manage(_m("schema_design_meter", 0.9, 0.7))
    assert sig is None


def test_manager_warning_band():
    mgr = MeterManagerRegistry()
    # 0.65 is within 0.1 of threshold 0.7 → WARNING, not FAIL.
    sig = mgr.manage(_m("schema_design_meter", 0.65, 0.7))
    assert sig is not None
    assert sig.severity is Severity.WARNING


def test_manager_fail_with_reason_and_deficit():
    mgr = MeterManagerRegistry()
    sig = mgr.manage(_m("backend_logic_meter", 0.2, 0.7,
                        issues=["no HTTP routes/handlers found"]))
    assert sig.severity is Severity.FAIL
    assert round(sig.deficit, 2) == 0.5
    assert "no HTTP routes" in sig.reason


def test_functional_correctness_manager_no_tests_message():
    mgr = MeterManagerRegistry()
    sig = mgr.manage(_m("functional_correctness_meter", 0.0, 0.9,
                        evidence={"executed": 0}))
    assert sig.severity is Severity.FAIL
    assert "No tests executed" in sig.suggestion


def test_manage_all_orders_worst_first():
    mgr = MeterManagerRegistry()
    measurements = [
        _m("code_hygiene_meter", 0.68, 0.7),          # small warning
        _m("functional_correctness_meter", 0.0, 0.9),  # big fail
        _m("schema_design_meter", 0.4, 0.7),           # mid fail
    ]
    signals = mgr.manage_all(measurements)
    # FAIL signals come before WARNING; within FAIL, larger deficit first.
    assert signals[0].meter == "functional_correctness_meter"
    assert signals[-1].severity is Severity.WARNING


def test_summarize_groups_by_severity():
    mgr = MeterManagerRegistry()
    signals = mgr.manage_all([
        _m("functional_correctness_meter", 0.0, 0.9),
        _m("code_hygiene_meter", 0.68, 0.7),
    ])
    summ = mgr.summarize(signals)
    assert "functional_correctness_meter" in summ["failing"]
    assert "code_hygiene_meter" in summ["warning"]


def test_unknown_meter_uses_default_manager():
    mgr = MeterManagerRegistry()
    sig = mgr.manage(_m("some_future_meter", 0.1, 0.7, issues=["x"]))
    assert sig is not None
    assert sig.severity is Severity.FAIL


def test_managers_drive_from_quality_battery():
    reg = AppMeterRegistry()
    mgr = MeterManagerRegistry()
    poor = {"app.py": "x = 1\n"}
    measurements = reg.quality_measurements(
        poor, description="store with orders", features=["checkout"],
        entities=["order"], test_outcome={"summary": "no tests"})
    signals = mgr.manage_all(measurements.values())
    metabolite = {s.meter for s in signals}
    assert "functional_correctness_meter" in metabolite
    assert all(s.severity in (Severity.FAIL, Severity.WARNING) for s in signals)


def test_registry_extensible():
    mgr = MeterManagerRegistry()

    class CustomManager(MeterManager):
        meter_name = "custom_meter"

    mgr.register(CustomManager())
    assert "custom_meter" in mgr.names()
