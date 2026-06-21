"""Unit tests for per-engine capability metadata."""

from __future__ import annotations

from common.db_capabilities import (
    DEFAULT_SQL_CAPABILITIES,
    DOCUMENTDB_CAPABILITIES,
    MONGODB_CAPABILITIES,
    MYSQL_CAPABILITIES,
    ORACLE_CAPABILITIES,
    POSTGRES_CAPABILITIES,
    SQLSERVER_CAPABILITIES,
    DBCapabilities,
)


def test_default_sql_capabilities():
    caps = DEFAULT_SQL_CAPABILITIES
    assert caps.query_language == "sql"
    assert caps.supports_schema_conversion is True
    assert caps.supports_document_query is False
    assert caps.feature_enabled("schema_conversion") is True


def test_sqlserver_capabilities():
    caps = SQLSERVER_CAPABILITIES
    assert caps.query_language == "sql"
    assert caps.label_for_operation("getSchemas", "Schemas") == "Schemas"
    assert caps.to_dict()["supports_transactions"] is True


def test_mongodb_capabilities():
    caps = MONGODB_CAPABILITIES
    assert caps.query_language == "document"
    assert caps.supports_transactions is False
    assert caps.supports_schema_conversion is False
    assert caps.label_for_operation("getTables", "Tables") == "Collections"
    assert caps.feature_enabled("schema_conversion") is False


def test_documentdb_matches_mongo_document_mode():
    assert DOCUMENTDB_CAPABILITIES.query_language == "document"
    assert DOCUMENTDB_CAPABILITIES.operation_labels["getTables"] == "Collections"


def test_mysql_capabilities_support_ssl():
    caps = MYSQL_CAPABILITIES
    assert caps.supports_ssl is True
    assert "verify_ca" in caps.ssl_mode_options
    assert "ca" in caps.ssl_fields


def test_postgres_capabilities_ssl_modes():
    caps = POSTGRES_CAPABILITIES
    assert "verify-full" in caps.ssl_mode_options


def test_oracle_capabilities_wallet_field():
    assert "wallet" in ORACLE_CAPABILITIES.ssl_fields


def test_feature_enabled_custom_disabled():
    caps = DBCapabilities(disabled_features=("ai_query",))
    assert caps.feature_enabled("ai_query") is False
    assert caps.feature_enabled("schema_conversion") is True
