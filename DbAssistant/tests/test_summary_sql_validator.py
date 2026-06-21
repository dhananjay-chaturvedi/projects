"""Tests for Summary mode SQL validation (catalog/system views only)."""

from ai_query.summary_sql_validator import validate_summary_mode_sql


def test_allows_information_schema():
    sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()"
    assert validate_summary_mode_sql(sql, "mysql") == []


def test_rejects_user_table_by_name():
    sql = "SELECT * FROM orders WHERE status = 'open'"
    errors = validate_summary_mode_sql(
        sql, "mysql", user_table_names=["orders", "customers"]
    )
    assert any("orders" in e for e in errors)


def test_rejects_unknown_table_in_summary_mode():
    sql = "SELECT * FROM my_custom_view"
    errors = validate_summary_mode_sql(sql, "mysql")
    assert errors


def test_open_mode_not_validated_by_agent_layer():
    """Validator itself always checks; agent skips when sql_mode is open."""
    sql = "SELECT * FROM orders"
    errors = validate_summary_mode_sql(sql, "mysql", user_table_names=["orders"])
    assert errors


def test_postgres_catalog_allowed():
    sql = "SELECT relname FROM pg_catalog.pg_class LIMIT 5"
    assert validate_summary_mode_sql(sql, "postgresql") == []


def test_oracle_dba_views_allowed():
    sql = "SELECT COUNT(*) FROM dba_tables"
    assert validate_summary_mode_sql(sql, "oracle") == []
