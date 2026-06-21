"""Agent-level SQL mode validation tests."""

from ai_query.agent import AIQueryAgent


class _CtxAgent(AIQueryAgent):
    def __init__(self):
        pass


def test_strict_summary_blocks_user_table_sql():
    agent = _CtxAgent()
    result = {
        "summary_sql": "SELECT * FROM orders",
        "explanation": "test",
    }
    context = {
        "database_type": "mysql",
        "schema": {"tables": ["orders", "customers"]},
    }
    out = agent._apply_sql_mode_validation(result, context, "strict_summary")
    assert out.get("summary_mode_blocked") is True


def test_summary_mode_allows_user_table_sql():
    agent = _CtxAgent()
    result = {
        "summary_sql": "SELECT * FROM orders LIMIT 10",
        "explanation": "test",
    }
    context = {
        "database_type": "mysql",
        "schema": {"tables": ["orders"]},
    }
    out = agent._apply_sql_mode_validation(result, context, "summary")
    assert not out.get("summary_mode_blocked")


def test_open_mode_allows_user_table_sql():
    agent = _CtxAgent()
    result = {"summary_sql": "SELECT * FROM orders", "explanation": "test"}
    context = {"database_type": "mysql", "schema": {"tables": ["orders"]}}
    out = agent._apply_sql_mode_validation(result, context, "open")
    assert not out.get("summary_mode_blocked")
