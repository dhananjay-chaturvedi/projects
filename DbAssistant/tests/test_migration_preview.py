"""Tests for shared migration preview formatters."""

from __future__ import annotations

from common.ui.shared.migration_preview import format_sample_data, format_schema_preview


def test_format_schema_preview_includes_table_and_ddl():
    text = format_schema_preview({
        "tables": [{
            "table": "public.users",
            "target_table": "users",
            "ddl": "CREATE TABLE users (id INT);",
            "issues": ["note one"],
        }],
    })
    assert "SCHEMA CONVERSION PREVIEW" in text
    assert "public.users" in text
    assert "CREATE TABLE users" in text
    assert "note one" in text


def test_format_sample_data_includes_rows():
    text = format_sample_data({
        "tables": [{
            "table": "users",
            "columns": ["id", "name"],
            "rows": [{"id": 1, "name": "Ada"}],
        }],
    })
    assert "SAMPLE DATA" in text
    assert "users" in text
    assert "Ada" in text
