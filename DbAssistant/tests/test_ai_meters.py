"""Tests for the deterministic ai_assistant.meters subsystem.

Uses real SQL, a real schema dict and real Python source so each meter is
exercised on genuine good/bad inputs (not mocks).
"""

from __future__ import annotations

from ai_assistant.meters import MeterSuite
from ai_assistant.meters.accuracy_meter import AccuracyMeter
from ai_assistant.meters.build_accuracy_meter import BuildAccuracyMeter
from ai_assistant.meters.build_design_accuracy_meter import BuildDesignAccuracyMeter
from ai_assistant.meters.code_accuracy_meter import CodeAccuracyMeter
from ai_assistant.meters.code_adaptability_meter import CodeAdaptabilityMeter
from ai_assistant.meters.code_design_management_system import CodeDesignManagementSystem
from ai_assistant.meters.code_efficiency_management_system import (
    CodeEfficiencyManagementSystem,
)
from ai_assistant.meters.error_meter import ErrorMeter
from ai_assistant.meters.input_reliability_management_system import (
    InputReliabilityManagementSystem,
    InputSignal,
)
from ai_assistant.meters.output_accuracy_management_system import (
    OutputAccuracyManagementSystem,
)
from ai_assistant.meters.understanding_meter import UnderstandingMeter

SCHEMA = {
    "customers": ["customer_id", "name", "email", "created_at"],
    "orders": ["order_id", "customer_id", "amount", "status"],
}


# --------------------------- accuracy_meter ------------------------------- #
def test_accuracy_high_for_valid_grounded_sql():
    m = AccuracyMeter().measure(
        "How many customers are there?",
        "SELECT COUNT(*) FROM customers",
        schema=SCHEMA,
        execution={"ok": True, "rowcount": 1},
    )
    assert m.passed and m.score >= 0.8
    assert m.evidence["unknown_tables"] == []


def test_accuracy_low_for_unknown_table():
    m = AccuracyMeter().measure(
        "list invoices", "SELECT * FROM invoices", schema=SCHEMA,
    )
    assert not m.passed
    assert "invoices" in m.evidence["unknown_tables"]


def test_accuracy_zero_for_non_sql():
    m = AccuracyMeter().measure("show", "I am not able to answer that", schema=SCHEMA)
    assert m.components["sql_parses"] == 0.0


def test_accuracy_penalizes_execution_failure():
    ok = AccuracyMeter().measure("q", "SELECT name FROM customers", schema=SCHEMA,
                                 execution={"ok": True, "rowcount": 5})
    bad = AccuracyMeter().measure("q", "SELECT name FROM customers", schema=SCHEMA,
                                  execution={"ok": False, "error": "deadlock"})
    assert ok.score > bad.score


# ----------------------------- error_meter -------------------------------- #
def test_error_meter_clean_vs_dirty():
    clean = ErrorMeter().measure("SELECT COUNT(*) FROM orders LIMIT 1", schema=SCHEMA,
                                 execution={"ok": True})
    dirty = ErrorMeter().measure("SELECT * FROM ghost", schema=SCHEMA,
                                 execution={"ok": False, "error": "no such table"})
    assert clean.score > dirty.score
    assert dirty.evidence["error_count"] >= 2


# -------------------------- understanding_meter --------------------------- #
def test_understanding_intent_match():
    good = UnderstandingMeter().measure(
        "how many orders are there?", "SELECT COUNT(*) FROM orders"
    )
    bad = UnderstandingMeter().measure(
        "how many orders are there?", "DELETE FROM orders"
    )
    assert good.score > bad.score


def test_understanding_followup_continuity():
    cont = UnderstandingMeter().measure(
        "only the paid ones", "SELECT * FROM orders WHERE status='paid'",
        previous_sql="SELECT * FROM orders", is_followup=True,
    )
    assert cont.components["followup_continuity"] == 1.0


# ----------------- input_reliability_management_system -------------------- #
def test_input_reliability_allows_good_input():
    m = InputReliabilityManagementSystem().measure(InputSignal(
        connection_ok=True, schema_loaded=True, schema_table_count=2,
        question="How many customers?", context_completeness=0.9,
    ))
    assert m.passed and m.evidence["allow"] is True


def test_input_reliability_blocks_without_connection():
    m = InputReliabilityManagementSystem().measure(InputSignal(
        connection_ok=False, schema_loaded=True, question="x?",
    ))
    assert m.evidence["allow"] is False


def test_input_reliability_requires_pii_mask():
    m = InputReliabilityManagementSystem().measure(InputSignal(
        connection_ok=True, schema_loaded=True, question="who is John Smith?",
        pii_required=True, pii_masked=False, context_completeness=1.0,
    ))
    assert m.evidence["allow"] is False


# ---------------- output_accuracy_management_system ----------------------- #
def test_output_system_accepts_good_and_rejects_bad():
    oams = OutputAccuracyManagementSystem()
    good = oams.evaluate("how many customers?", "SELECT COUNT(*) FROM customers",
                         schema=SCHEMA, execution={"ok": True, "rowcount": 1})
    bad = oams.evaluate("list", "SELECT * FROM nope", schema=SCHEMA,
                        execution={"ok": False, "error": "missing table"})
    assert good.accepted and not bad.accepted
    assert oams.stats()["count"] == 2
    assert 0.0 <= oams.stats()["accept_rate"] <= 1.0


# ------------------------- build_accuracy_meter --------------------------- #
def test_build_accuracy_full_match():
    m = BuildAccuracyMeter().measure(
        expected_files=["src/app.py", "tests/test_app.py", "README.md"],
        produced_files=["src/app.py", "tests/test_app.py", "README.md"],
        required_services=["notification", "docs"],
        present_services=["notification", "docs"],
    )
    assert m.passed and m.score >= 0.9


def test_build_accuracy_missing_service():
    m = BuildAccuracyMeter().measure(
        expected_files=["src/app.py"], produced_files=["src/app.py"],
        required_services=["notification", "monitoring"],
        present_services=["notification"],
    )
    assert "monitoring" in m.evidence["missing_services"]
    assert not m.passed


# ---------------------- build_design_accuracy_meter ----------------------- #
def test_build_design_full_blueprint():
    files = [
        "src/app.py", "tests/test_app.py", "README.md",
        ".github/workflows/ci.yml", "Dockerfile", "requirements.txt",
    ]
    m = BuildDesignAccuracyMeter().measure(produced_files=files)
    assert m.passed and m.score >= 0.9


def test_build_design_missing_rules():
    m = BuildDesignAccuracyMeter().measure(produced_files=["main.py"])
    assert not m.passed
    assert m.issues


# --------------------------- code_accuracy_meter -------------------------- #
GOOD_CODE = '''
"""A tiny module."""


def add(a: int, b: int) -> int:
    """Return the sum of two integers."""
    return a + b
'''

BAD_CODE = "def broken(:\n    return"


def test_code_accuracy_valid_vs_syntax_error():
    good = CodeAccuracyMeter().measure(GOOD_CODE, tests={"passed": 3, "total": 3})
    bad = CodeAccuracyMeter().measure(BAD_CODE)
    assert good.passed and good.score >= 0.9
    assert not bad.passed and bad.components["parses"] == 0.0


def test_code_accuracy_test_ratio_matters():
    full = CodeAccuracyMeter().measure(GOOD_CODE, tests={"passed": 10, "total": 10})
    half = CodeAccuracyMeter().measure(GOOD_CODE, tests={"passed": 5, "total": 10})
    assert full.score > half.score


# ------------------------ code_adaptability_meter ------------------------- #
COMPLEX_CODE = '''
def f(x):
    total = 0
    for i in range(x):
        if i % 2 == 0:
            if i % 3 == 0:
                for j in range(i):
                    if j > 5:
                        while j > 0:
                            j -= 1
                            total += j
                        total += j
                    elif j < 2:
                        total -= 1
    return total
'''


def test_adaptability_clean_beats_complex():
    clean = CodeAdaptabilityMeter().measure(GOOD_CODE)
    messy = CodeAdaptabilityMeter().measure(COMPLEX_CODE)
    assert clean.score > messy.score


# ------------------ code_efficiency_management_system --------------------- #
NPLUS1 = '''
def load(conn, ids):
    rows = []
    for i in ids:
        rows.append(conn.execute("SELECT * FROM t WHERE id=%s", (i,)))
    return rows
'''


def test_efficiency_flags_n_plus_one():
    eff = CodeEfficiencyManagementSystem().measure(NPLUS1)
    assert eff.issues  # at least one recommendation
    assert any("N+1" in i or "loop" in i for i in eff.issues)
    clean = CodeEfficiencyManagementSystem().measure(GOOD_CODE)
    assert clean.score >= eff.score


def test_efficiency_runtime_budget():
    over = CodeEfficiencyManagementSystem().measure(
        GOOD_CODE, runtime={"ms": 500, "budget_ms": 100}
    )
    under = CodeEfficiencyManagementSystem().measure(
        GOOD_CODE, runtime={"ms": 50, "budget_ms": 100}
    )
    assert under.score > over.score


# ------------------ code_design_management_system ------------------------- #
BAD_DESIGN = '''
class badName:
    def m(self):
        try:
            return 1
        except:
            return None
'''


def test_code_design_flags_violations():
    m = CodeDesignManagementSystem().measure(BAD_DESIGN)
    assert m.issues
    joined = " ".join(m.issues)
    assert "PascalCase" in joined or "bare except" in joined
    good = CodeDesignManagementSystem().measure(GOOD_CODE)
    assert good.score > m.score


# ------------------------------- suite ------------------------------------ #
def test_suite_evaluate_code_artifact():
    suite = MeterSuite()
    verdict = suite.evaluate_code_artifact(GOOD_CODE, tests={"passed": 4, "total": 4})
    assert verdict["accepted"] is True
    assert set(verdict["measurements"]) == {
        "code_accuracy_meter", "code_adaptability_meter",
        "code_efficiency_management_system", "code_design_management_system",
    }


def test_suite_evaluate_build():
    suite = MeterSuite()
    verdict = suite.evaluate_build(
        expected_files=["src/app.py", "tests/test_app.py", "README.md",
                        ".github/workflows/ci.yml", "Dockerfile", "requirements.txt"],
        produced_files=["src/app.py", "tests/test_app.py", "README.md",
                        ".github/workflows/ci.yml", "Dockerfile", "requirements.txt"],
        required_services=["docs", "ci_cd"], present_services=["docs", "ci_cd"],
    )
    assert verdict["accepted"] is True


def test_suite_owns_all_meters():
    meters = MeterSuite().all_meters()
    assert len(meters) == 15
    for name in (
        "requirement_coverage_meter", "requirement_fidelity_meter",
        "data_understanding_meter", "process_adherence_meter",
    ):
        assert name in meters
