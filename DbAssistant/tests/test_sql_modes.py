"""Tests for three-tier SQL modes and execution rules."""

from ai_query.response_parser import response_format_instructions
from ai_query.sql_modes import (
    migrate_stored_sql_mode,
    normalize_sql_mode,
    is_strict_summary,
    execution_rules_apply,
)
from ai_query.sql_execution_rules import evaluate_execution_rules, build_explain_sql


def test_normalize_three_modes():
    assert normalize_sql_mode("strict_summary") == "strict_summary"
    assert normalize_sql_mode("summary") == "summary"
    assert normalize_sql_mode("open") == "open"
    assert normalize_sql_mode("strict") == "strict_summary"


def test_legacy_session_migration():
    assert migrate_stored_sql_mode("summary", sql_modes_v2=False) == "strict_summary"
    assert migrate_stored_sql_mode("open", sql_modes_v2=False) == "summary"
    assert migrate_stored_sql_mode("open", sql_modes_v2=True) == "open"


def test_strict_prompt_mentions_metadata_only():
    text = response_format_instructions("mysql", sql_mode="strict_summary")
    assert "STRICT" in text
    assert "strict_summary" in text


def test_summary_prompt_allows_user_tables():
    text = response_format_instructions("mysql", sql_mode="summary")
    assert "user-schema tables" in text
    assert "summary" in text


def test_open_prompt_unrestricted():
    text = response_format_instructions("mysql", sql_mode="open")
    assert "no artificial catalog-only limit" in text.lower() or "directly answers" in text


def test_execution_rules_apply_only_summary_open():
    assert execution_rules_apply("strict_summary") is False
    assert execution_rules_apply("summary") is True
    assert execution_rules_apply("open") is True


def test_is_strict_summary():
    assert is_strict_summary("strict_summary") is True
    assert is_strict_summary("summary") is False


def test_limit_rule_blocks_user_table_select():
    rules = "Always use LIMIT clause for SELECT on user tables"
    sql = "SELECT * FROM orders WHERE status = 'open'"
    result = evaluate_execution_rules(
        sql, rules, user_table_names=["orders"], db_type="mysql"
    )
    assert result.allowed is False
    assert "LIMIT" in result.blocked_reason


def test_limit_rule_allows_with_limit():
    rules = "Always use LIMIT clause for SELECT on user tables"
    sql = "SELECT * FROM orders LIMIT 10"
    result = evaluate_execution_rules(
        sql, rules, user_table_names=["orders"], db_type="mysql"
    )
    assert result.allowed is True


def test_limit_rule_not_bypassed_by_string_or_comment():
    rules = "Always use LIMIT clause for SELECT on user tables"
    sql = "SELECT * FROM orders WHERE note = 'limit 10' -- LIMIT 1"
    result = evaluate_execution_rules(
        sql, rules, user_table_names=["orders"], db_type="mysql"
    )
    assert result.allowed is False
    assert "LIMIT" in result.blocked_reason


def test_join_count_ignores_strings_and_comments():
    rules = "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
    sql = "SELECT 'join join' AS txt FROM a -- JOIN b\nJOIN c ON a.id = c.id"
    result = evaluate_execution_rules(sql, rules, db_type="mysql")
    assert result.run_explain_first is False


def test_explain_rule_multi_join():
    rules = "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
    sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id"
    result = evaluate_execution_rules(sql, rules, db_type="mysql")
    assert result.allowed is True
    assert result.run_explain_first is True
    assert result.explain_sql.startswith("EXPLAIN")


def test_build_explain_postgres():
    assert build_explain_sql("SELECT 1", "postgresql").startswith("EXPLAIN ")
