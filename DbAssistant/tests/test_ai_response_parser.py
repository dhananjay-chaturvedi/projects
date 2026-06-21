"""Tests for structured AI response parsing (summary SQL pipeline)."""

from ai_query.response_parser import (
    parse_structured_ai_response,
    build_agent_result,
    catalog_view_guidance,
)


SAMPLE = """
CONTEXT:
- Connection: prod
- Database type: mysql

SUMMARY_SQL:
SELECT COUNT(*) AS table_count
FROM information_schema.tables
WHERE table_schema = DATABASE();

EXPLANATION:
This counts tables in the current schema using information_schema.

DETAIL_SQL:
SELECT table_name FROM information_schema.tables LIMIT 10;

INSIGHTS:
- How many rows per table?
- Which tables lack primary keys?
"""


def test_parse_structured_sections():
    parsed = parse_structured_ai_response(SAMPLE)
    assert "information_schema" in parsed["summary_sql"]
    assert parsed["detail_sql"] is not None
    assert "primary keys" in parsed["insights"].lower()
    assert parsed["context"] is not None


def test_legacy_sql_format():
    legacy = "SQL:\nSELECT 1;\n\nEXPLANATION:\nOne row.\n"
    parsed = parse_structured_ai_response(legacy)
    assert parsed["summary_sql"] == "SELECT 1;"


def test_no_change_clarification():
    text = """
SUMMARY_SQL:
NO CHANGE

EXPLANATION:
The query is already correct.

DETAIL_SQL:
NO CHANGE
"""
    parsed = parse_structured_ai_response(text)
    result = build_agent_result(parsed, keep_sql="SELECT 1")
    assert result["is_clarification"] is True
    assert result["summary_sql"] == "SELECT 1"


def test_build_agent_result_merges_sections():
    parsed = parse_structured_ai_response(SAMPLE)
    result = build_agent_result(parsed)
    assert result["sql"] == result["summary_sql"]
    assert "Detail SQL" in result["explanation"]
    assert "Insights" in result["explanation"]


def test_catalog_guidance_mysql():
    assert "information_schema" in catalog_view_guidance("mysql")


def test_parse_satisfied_section():
    text = """
SATISFIED:
yes

EXPLANATION:
All done.
"""
    parsed = parse_structured_ai_response(text)
    assert parsed["satisfied"] is True
