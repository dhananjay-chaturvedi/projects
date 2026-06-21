"""Tests for auto-execute orchestrator and SATISFIED parsing."""

from ai_query.auto_execute_orchestrator import AutoExecuteOrchestrator
from ai_query.response_parser import parse_structured_ai_response, build_agent_result
from ai_query.sql_execution_service import execute_sql_after_gate


class _FakeAgent:
    def run_auto_refine(self, *args, **kwargs):
        return {"satisfied": True, "explanation": "Done."}


def test_orchestrator_should_continue_until_satisfied():
    orch = AutoExecuteOrchestrator(_FakeAgent(), max_iterations=3)
    assert orch.should_continue({"satisfied": False}, 1, lambda: False) is True
    assert orch.should_continue({"satisfied": True}, 1, lambda: False) is False


def test_orchestrator_stops_on_cancel():
    orch = AutoExecuteOrchestrator(_FakeAgent(), max_iterations=5)
    assert orch.should_continue({"satisfied": False}, 1, lambda: True) is False


def test_orchestrator_stops_at_max_iterations():
    orch = AutoExecuteOrchestrator(_FakeAgent(), max_iterations=2)
    assert orch.should_continue({"satisfied": False}, 2, lambda: False) is False


def test_orchestrator_build_panel_context():
    orch = AutoExecuteOrchestrator(_FakeAgent(), max_iterations=5)
    ctx = orch.build_panel_context(
        problem_statement="How many tables?",
        summary_sql="SELECT 1",
        explanation="Count tables.",
        query_output="table_count\n5",
        iteration=2,
        sql_mode="summary",
    )
    assert ctx["iteration"] == 2
    assert ctx["sql_mode"] == "summary"
    assert "tables" in ctx["problem_statement"]


def test_parse_satisfied_yes():
    text = """
SATISFIED:
yes

EXPLANATION:
The query output answers the question completely.

SUMMARY_SQL:
NO CHANGE
"""
    parsed = parse_structured_ai_response(text)
    result = build_agent_result(parsed, keep_sql="SELECT 1")
    assert parsed["satisfied"] is True
    assert result["satisfied"] is True


def test_parse_satisfied_no():
    text = """
SATISFIED:
no

SUMMARY_SQL:
SELECT COUNT(*) FROM information_schema.tables;

EXPLANATION:
Try this catalog query instead.
"""
    parsed = parse_structured_ai_response(text)
    assert parsed["satisfied"] is False


def test_execute_sql_after_gate_does_not_run_main_sql_when_explain_fails():
    class DB:
        def __init__(self):
            self.calls = []

        def execute_query(self, sql):
            self.calls.append(sql)
            if sql.startswith("EXPLAIN"):
                return None, "bad plan"
            return {"rows": [(1,)]}, None

    db = DB()
    out = execute_sql_after_gate(
        "SELECT * FROM orders",
        db,
        {"explain_sql": "EXPLAIN SELECT * FROM orders", "explain_note": "must explain"},
    )
    assert out["blocked"] is True
    assert out["allowed"] is False
    assert "EXPLAIN failed" in out["error"]
    assert db.calls == ["EXPLAIN SELECT * FROM orders"]
