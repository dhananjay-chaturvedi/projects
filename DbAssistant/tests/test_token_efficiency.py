"""Tests for AI Query token efficiency, prompt assembly, and determinism fixes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai_query import prompt_assembly as pa
from ai_query import token_meter
from ai_query.auto_execute_orchestrator import AutoExecuteOrchestrator
from ai_query.response_parser import _clean_sql_block, parse_structured_ai_response, response_format_instructions
from ai_query.sql_validation import validate_sql_against_schema


SAMPLE_CONTEXT = {
    "database_type": "postgresql",
    "question_complexity": 0,
    "_analysis": {
        "needs_relationships": False,
        "needs_performance": False,
        "needs_analysis": False,
        "needs_system": True,
        "is_simple": True,
        "complexity_score": 1,
    },
    "schema": {
        "table_count": 2,
        "tables": ["users", "orders"],
        "table_schemas": {
            "users": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "email", "type": "text", "nullable": True},
            ],
            "orders": [
                {"name": "id", "type": "int", "nullable": False},
                {"name": "user_id", "type": "int", "nullable": False},
            ],
        },
    },
    "relationships": {},
    "system": {},
    "performance": {},
    "metadata": {},
}


def test_estimate_tokens_nonempty():
    assert token_meter.estimate_tokens("hello world") >= 1
    assert token_meter.estimate_tokens("") == 0


def test_token_meter_capture_hook():
    captured = []
    token_meter.register_capture_hook(lambda r: captured.append(r))
    try:
        token_meter.record_prompt(path="test", prompt="SELECT 1", backend="claude", tier=1)
    finally:
        token_meter.clear_capture_hooks()
    assert captured
    assert captured[0]["path"] == "test"
    assert captured[0]["prompt_tokens_est"] >= 1


def test_compact_schema_smaller_than_verbose():
    table_schemas = SAMPLE_CONTEXT["schema"]["table_schemas"]
    compact = pa.format_table_schemas(table_schemas, compact=True, max_tables=5)
    verbose = pa.format_table_schemas(table_schemas, compact=False, max_tables=5)
    assert len(compact) < len(verbose)
    assert "users" in compact and "email" in compact
    assert "┏" not in compact


def test_schema_digest_preserves_referenced_tables():
    digest = pa.build_schema_digest(
        SAMPLE_CONTEXT,
        {"users"},
        compact=True,
    )
    assert "users" in digest
    assert "email" in digest
    assert "SCHEMA DIGEST" in digest


def test_dedupe_peer_bundles_same_connection():
    b1 = {
        "tab_number": 1,
        "connection_name": "db1",
        "db_type": "postgresql",
        "schema_context": "SCHEMA_A",
    }
    b2 = {
        "tab_number": 3,
        "connection_name": "db1",
        "db_type": "postgresql",
        "schema_context": "SCHEMA_B",
    }
    merged = pa.dedupe_peer_bundles([b1, b2])
    assert len(merged) == 1
    assert merged[0]["tab_numbers"] == [1, 3]


def test_cache_covers_needs_category_aware():
    cached = {
        "_analysis": {
            "needs_system": True,
            "needs_performance": False,
        }
    }
    assert pa.cache_covers_needs(cached, {"needs_system": True})
    assert not pa.cache_covers_needs(cached, {"needs_performance": True})


def test_looks_like_sql_guard():
    assert pa.looks_like_sql("SELECT id FROM users", "postgresql")
    assert not pa.looks_like_sql(
        "Here is an explanation of your database schema without any query.",
        "postgresql",
    )


def test_response_format_auto_omits_detail_for_simple():
    brief = response_format_instructions(
        "postgresql",
        "conn",
        sql_mode="summary",
        complexity=0,
        is_simple=True,
        full_format=False,
    )
    assert "DETAIL_SQL" not in brief
    assert "SUMMARY_SQL" in brief


def test_tilde_fence_cleaning():
    sql = _clean_sql_block("~~~\nSELECT 1\n~~~")
    assert sql == "SELECT 1"


def test_sql_validation_ast_or_regex():
    ctx = SAMPLE_CONTEXT
    ok_sql = "SELECT id, email FROM users"
    bad_sql = "SELECT mystery_col FROM users"
    warnings_ok = validate_sql_against_schema(ok_sql, ctx, db_type="postgresql")
    warnings_bad = validate_sql_against_schema(bad_sql, ctx, db_type="postgresql")
    assert warnings_ok == []
    assert warnings_bad


def test_auto_execute_oscillation_detection():
    agent = MagicMock()
    orch = AutoExecuteOrchestrator(agent, max_iterations=5)
    assert orch.should_continue({"satisfied": False, "sql": "SELECT 1"}, 1, lambda: False)
    assert not orch.should_continue({"satisfied": False, "sql": "SELECT 1"}, 2, lambda: False)


def test_needs_escalation_on_empty_sql():
    assert pa.needs_escalation({"is_clarification": False}, None, [])
    assert not pa.needs_escalation({"is_clarification": True}, None, [])


def test_build_intelligent_context_compact(monkeypatch):
    monkeypatch.setattr(
        pa,
        "prompt_flags",
        lambda: {
            "compact_schema": True,
            "consolidate_instructions": True,
            "dedup_followup_schema": True,
            "dedup_crosstab_schema": True,
            "progressive_escalation": True,
            "full_format_block": "auto",
            "schema_drift_check": False,
        },
    )
    with patch("ai_query.backends.AIBackendRegistry") as reg_cls:
        reg_cls.return_value = MagicMock(get_default_name=lambda: "", get=lambda: None)
        from ai_query.agent import AIQueryAgent

        agent = AIQueryAgent()
    text = agent._build_intelligent_context(SAMPLE_CONTEXT, "list users", tier=1)
    assert "┏" not in text
    assert "users" in text
    assert "email" in text


def test_call_ai_records_tokens(monkeypatch):
    with patch("ai_query.backends.AIBackendRegistry") as reg_cls:
        reg = MagicMock()
        reg.get_default_name.return_value = "claude"
        backend = MagicMock()
        backend.name = "claude"
        backend.is_available.return_value = True
        backend.supports_resume = False
        backend.call.return_value = {"response": "SUMMARY_SQL:\nSELECT 1", "error": None}
        reg.get.return_value = backend
        reg_cls.return_value = reg
        from ai_query.agent import AIQueryAgent

        agent = AIQueryAgent()
        agent.cli_available = True
        agent._active_backend = backend
        captured = []
        token_meter.register_capture_hook(lambda r: captured.append(r))
        try:
            result = agent._call_ai("SELECT 1 prompt", path="test_ask", tier=1)
        finally:
            token_meter.clear_capture_hooks()
        assert result.get("prompt_tokens_est", 0) >= 1
        assert agent.last_prompt_tokens_est >= 1
        assert captured[0]["path"] == "test_ask"
