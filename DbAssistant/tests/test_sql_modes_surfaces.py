"""Tests for SQL modes / execution rules on CLI, API, and DBService surfaces."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai_query.response_parser import response_format_instructions
from ai_query.sql_execution_rules import build_explain_sql
from ai_query.summary_sql_validator import validate_summary_mode_sql


@pytest.mark.parametrize(
    "db_type,catalog_sql",
    [
        ("MySQL", "SELECT COUNT(*) FROM information_schema.tables"),
        ("MariaDB", "SELECT COUNT(*) FROM information_schema.tables"),
        ("PostgreSQL", "SELECT COUNT(*) FROM pg_catalog.pg_tables"),
        ("Oracle", "SELECT COUNT(*) FROM dba_tables"),
        ("SQLServer", "SELECT COUNT(*) FROM sys.tables"),
        ("SQLite", "SELECT name FROM sqlite_master"),
    ],
)
def test_strict_validator_accepts_catalog_per_engine(db_type, catalog_sql):
    assert validate_summary_mode_sql(catalog_sql, db_type) == []


@pytest.mark.parametrize(
    "db_type",
    ["MySQL", "MariaDB", "PostgreSQL", "Oracle", "SQLServer", "SQLite"],
)
def test_prompts_for_all_modes_per_engine(db_type):
    for mode in ("strict_summary", "summary", "open"):
        text = response_format_instructions(db_type, sql_mode=mode)
        assert mode.replace("_", " ") in text or mode in text


@pytest.mark.parametrize(
    "db_type,prefix",
    [
        ("MySQL", "EXPLAIN "),
        ("MariaDB", "EXPLAIN "),
        ("PostgreSQL", "EXPLAIN "),
        ("Oracle", "EXPLAIN PLAN FOR "),
        ("SQLServer", "SET SHOWPLAN_ALL ON"),
        ("SQLite", "EXPLAIN "),
    ],
)
def test_explain_sql_dialects(db_type, prefix):
    assert build_explain_sql("SELECT 1", db_type).startswith(prefix)


def test_db_service_ai_query_passes_sql_mode():
    from ai_query.service import AIService

    svc = AIService.__new__(AIService)
    svc._core = MagicMock()
    mock_ai = MagicMock()
    mock_sess = MagicMock()
    mock_sess.session_id = "sid"
    mock_sess.sql_mode = "summary"
    mock_ai.sessions.create.return_value = mock_sess
    mock_ai.start_new_conversation.return_value = {
        "sql": "SELECT 1",
        "summary_sql": "SELECT 1",
        "explanation": "ok",
        "error": None,
    }
    mock_ai.sessions.delete.return_value = True
    mock_mgr = MagicMock()

    svc._ai = mock_ai
    svc._core.get_manager = MagicMock(return_value=mock_mgr)

    with patch("ai_query.service._AI_AVAILABLE", True):
        r = svc.ai_query("local", "count tables", sql_mode="open")

    assert r["sql"] == "SELECT 1"
    assert mock_sess.sql_mode == "open"
    mock_ai.start_new_conversation.assert_called_once()


def test_db_service_session_update_sql_mode():
    from ai_query.service import AIService

    svc = AIService.__new__(AIService)
    svc._core = MagicMock()
    mock_ai = MagicMock()
    mock_sess = MagicMock()
    mock_sess.session_id = "sid"
    mock_sess.to_dict.return_value = {"sql_mode": "strict_summary"}
    mock_ai.sessions.resolve.return_value = mock_sess
    svc._ai = mock_ai

    with patch("ai_query.service._AI_AVAILABLE", True):
        r = svc.ai_session_update("tab1", sql_mode="strict_summary")

    assert mock_sess.sql_mode == "strict_summary"
    assert r["session"]["sql_mode"] == "strict_summary"


def test_db_service_execute_sql_blocks_without_limit():
    from ai_query.service import AIService

    svc = AIService.__new__(AIService)
    svc._core = MagicMock()
    mock_ai = MagicMock()
    mock_sess = MagicMock()
    mock_sess.connection_name = "local"
    mock_sess.sql_mode = "open"
    mock_sess.sql_execution_rules = (
        "Always use LIMIT clause for SELECT on user tables"
    )
    mock_ai.sessions.resolve.return_value = mock_sess
    mock_ai.get_cached_comprehensive_context.return_value = {
        "schema": {"tables": ["EMPLOYEES"]},
    }
    svc._ai = mock_ai
    svc._core.get_manager = MagicMock(return_value=MagicMock(db_type="MariaDB"))

    with patch("ai_query.service._AI_AVAILABLE", True):
        r = svc.ai_session_execute_sql("tab1", "SELECT * FROM EMPLOYEES")

    assert r.get("blocked") is True
    assert "LIMIT" in (r.get("error") or "")
