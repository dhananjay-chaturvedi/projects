#!/usr/bin/env python3
"""
End-to-end AI tests for SQL modes using Cursor Agent + local MariaDB.

Run:
  RUN_AI_CURSOR_TESTS=1 .venv/bin/python -m pytest tests/test_sql_modes_ai_cursor.py -v -s

Or the standalone report:
  .venv/bin/python tests/run_sql_modes_ai_cursor_report.py
"""

from __future__ import annotations

import os
import re
import time

import pytest

from ai_query.agent import AIQueryAgent
from ai_query.session_manager import AISessionManager
from ai_query.summary_sql_validator import validate_summary_mode_sql
from common.db_manager import DatabaseManager

pytestmark = [
    pytest.mark.integration,
    pytest.mark.ai,
]

MYSQL = dict(
    host="localhost",
    port=3306,
    username="dheeru",
    password="dheeru",
    database="test",
)

SKIP_REASON = "Set RUN_AI_CURSOR_TESTS=1 to run Cursor Agent live AI tests"


def _skip_unless_enabled():
    if os.environ.get("RUN_AI_CURSOR_TESTS", "").strip() not in ("1", "true", "yes"):
        pytest.skip(SKIP_REASON)


def _sql_lower(result: dict) -> str:
    return (result.get("summary_sql") or result.get("sql") or "").lower()


@pytest.fixture(scope="module")
def cursor_available():
    _skip_unless_enabled()
    from ai_query.backends.cursor_backend import CursorBackend

    backend = CursorBackend()
    if not backend.check_availability(force=True):
        pytest.skip(f"Cursor Agent unavailable: {backend.get_unavailable_reason()}")
    return backend


@pytest.fixture(scope="module")
def db_manager():
    _skip_unless_enabled()
    mgr = DatabaseManager("MariaDB")
    mgr.connect(**MYSQL)
    yield mgr
    mgr.disconnect()


@pytest.fixture(scope="module")
def ai_agent(cursor_available):
    agent = AIQueryAgent()
    ok = agent.set_backend("cursor", verify=True)
    assert ok, "Failed to select Cursor backend"
    return agent


@pytest.fixture
def session(ai_agent):
    sess = ai_agent.sessions.create(
        connection_name="local_mariadb",
        backend="cursor",
    )
    yield sess
    try:
        ai_agent.sessions.delete(sess.session_id)
    except Exception:
        pass


def _ask(agent, session, db_manager, question: str, *, rules: str = "") -> dict:
    session.connection_name = "local_mariadb"
    session.sql_modes_v2 = True
    if rules:
        session.sql_execution_rules = rules
    t0 = time.time()
    result = agent.start_new_conversation(
        question,
        db_manager,
        "local_mariadb",
        session_id=session.session_id,
    )
    elapsed = time.time() - t0
    print(f"\n--- AI ({elapsed:.1f}s) mode={session.sql_mode} ---")
    print(f"Q: {question}")
    print(f"SQL: {result.get('summary_sql') or result.get('sql')}")
    print(f"blocked: {result.get('summary_mode_blocked')}")
    print(f"error: {result.get('error')}")
    if result.get("error"):
        pytest.fail(result["error"])
    return result


class TestCursorStrictSummaryMode:
    def test_metadata_question_uses_catalog(
        self, ai_agent, session, db_manager
    ):
        session.sql_mode = "strict_summary"
        result = _ask(
            ai_agent,
            session,
            db_manager,
            "How many tables are in the current database? "
            "Answer using SUMMARY_SQL with catalog/metadata views only.",
        )
        sql = _sql_lower(result)
        ctx = ai_agent.get_cached_comprehensive_context(
            db_manager, "local_mariadb", "schema"
        )
        user_tables = ai_agent._user_table_names_from_context(ctx)
        violations = validate_summary_mode_sql(
            result.get("summary_sql") or "",
            "MariaDB",
            user_tables,
        )
        assert not violations, f"Strict mode SQL violations: {violations}"
        assert (
            "information_schema" in sql
            or "mysql" in sql
            or "tables" in sql
        )


class TestCursorSummaryMode:
    def test_data_question_uses_user_table(
        self, ai_agent, session, db_manager
    ):
        session.sql_mode = "summary"
        result = _ask(
            ai_agent,
            session,
            db_manager,
            "Show up to 5 employee first names from the EMPLOYEES table.",
        )
        sql = _sql_lower(result)
        assert "employees" in sql
        assert not result.get("summary_mode_blocked")
        assert "limit" in sql or "top" in sql

    def test_prompt_includes_execution_rules(
        self, ai_agent, session, db_manager
    ):
        rules = "Always use LIMIT clause for SELECT on user tables"
        session.sql_mode = "summary"
        session.sql_execution_rules = rules
        _ask(
            ai_agent,
            session,
            db_manager,
            "Count rows in EMPLOYEES (use LIMIT if selecting rows).",
            rules=rules,
        )
        prompt = ai_agent.get_last_prompt_sent()
        assert rules in prompt


class TestCursorOpenMode:
    def test_business_question_uses_user_tables(
        self, ai_agent, session, db_manager
    ):
        session.sql_mode = "open"
        result = _ask(
            ai_agent,
            session,
            db_manager,
            "What is the total number of employees in the EMPLOYEES table?",
        )
        sql = _sql_lower(result)
        assert "employees" in sql
        assert not result.get("summary_mode_blocked")

    def test_generated_sql_executes_on_db(
        self, ai_agent, session, db_manager
    ):
        session.sql_mode = "open"
        result = _ask(
            ai_agent,
            session,
            db_manager,
            "Return the count of rows in EMPLOYEES as a single number.",
        )
        sql = result.get("summary_sql") or result.get("sql")
        assert sql
        exec_result, error = db_manager.execute_query(sql)
        assert error is None, error
        assert exec_result.get("rows") is not None


class TestCursorFollowUp:
    def test_follow_up_refines_query(
        self, ai_agent, session, db_manager
    ):
        session.sql_mode = "open"
        first = _ask(
            ai_agent,
            session,
            db_manager,
            "Select first names from EMPLOYEES limit 3.",
        )
        assert first.get("summary_sql") or first.get("sql")
        t0 = time.time()
        second = ai_agent.send_follow_up(
            "Add LAST_NAME column to the query.",
            db_manager,
            "local_mariadb",
            session_id=session.session_id,
        )
        print(f"\n--- Follow-up ({time.time()-t0:.1f}s) ---")
        print(f"SQL: {second.get('summary_sql') or second.get('sql')}")
        assert not second.get("error"), second.get("error")
        sql = _sql_lower(second)
        assert "last_name" in sql or "no change" in sql
