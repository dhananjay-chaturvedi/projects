"""Registry tests for SQL Server, MongoDB, DocumentDB, and capabilities."""

from __future__ import annotations

import importlib.util

import pytest


@pytest.fixture(autouse=True)
def _reset_registry():
    from common.database_registry import DatabaseRegistry

    DatabaseRegistry._initialized = False
    DatabaseRegistry._registry = {}
    yield
    DatabaseRegistry._initialized = False
    DatabaseRegistry._registry = {}


@pytest.mark.skipif(
    importlib.util.find_spec("pymssql") is None,
    reason="pymssql not installed",
)
def test_sqlserver_registered_with_capabilities():
    from common.database_registry import DatabaseRegistry

    assert "SQLServer" in DatabaseRegistry.get_all_types()
    caps = DatabaseRegistry.get_capabilities("SQLServer")
    assert caps.query_language == "sql"
    assert DatabaseRegistry.get_default_port("SQLServer") == 1433
    assert DatabaseRegistry.supports_operation("SQLServer", "getTables") is True


@pytest.mark.skipif(
    importlib.util.find_spec("pymongo") is None,
    reason="pymongo not installed",
)
def test_mongodb_registered_collections_label():
    from common.database_registry import DatabaseRegistry

    assert "MongoDB" in DatabaseRegistry.get_all_types()
    assert "DocumentDB" in DatabaseRegistry.get_all_types()
    caps = DatabaseRegistry.get_capabilities("MongoDB")
    assert caps.query_language == "document"
    ops = {fn: label for label, fn in DatabaseRegistry.get_available_operations("MongoDB")}
    assert ops.get("getMongoCollections") == "Collections"
    assert DatabaseRegistry.supports_operation("MongoDB", "executeDocumentQuery") is True


def test_db_service_list_db_types_includes_capabilities():
    from common.headless.db_service import CoreDBService

    rows = CoreDBService().list_db_types()
    assert rows
    sample = rows[0]
    assert "capabilities" in sample
    assert "query_language" in sample["capabilities"]


def test_db_service_collections_alias_maps_to_get_tables():
    from common.headless.db_service import CoreDBService

    assert "collections" in CoreDBService.supported_object_types()
    assert CoreDBService._OBJ_OP_MAP["collections"] == "getTables"
