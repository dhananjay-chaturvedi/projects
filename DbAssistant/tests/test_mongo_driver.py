"""Unit tests for MongoDB / DocumentDB driver (pymongo)."""

from __future__ import annotations

import importlib.util
import json
from unittest.mock import MagicMock, patch

import pytest

from common.drivers.conMongo import MongoConnection, executeMongoQuery


@pytest.mark.skipif(
    importlib.util.find_spec("pymongo") is None,
    reason="pymongo not installed",
)
def test_con_mongo_module_imports():
    from common.drivers import conMongo

    assert conMongo.connectMongo is not None
    assert conMongo.connectDocumentDB is not None


def test_get_mongo_collections():
    from common.drivers import conMongo

    conn = MagicMock(spec=MongoConnection)
    conn.db.list_collection_names.return_value = ["b", "a"]
    assert conMongo.getMongoCollections(conn) == ["a", "b"]


def test_execute_mongo_find_query():
    conn = MagicMock(spec=MongoConnection)
    coll = MagicMock()
    conn.db.__getitem__.return_value = coll
    coll.find.return_value.limit.return_value = [
        {"_id": 1, "name": "alice"},
        {"_id": 2, "name": "bob"},
    ]

    payload = json.dumps(
        {"collection": "users", "operation": "find", "filter": {}, "limit": 10}
    )
    result, err = executeMongoQuery(conn, payload)
    assert err is None
    assert result is not None
    assert result["rowcount"] == 2
    assert "_id" in result["columns"]
    assert "name" in result["columns"]


def test_execute_mongo_invalid_json():
    conn = MagicMock(spec=MongoConnection)
    result, err = executeMongoQuery(conn, "{not json")
    assert result is None
    assert "Invalid JSON" in err


def test_execute_mongo_count():
    conn = MagicMock(spec=MongoConnection)
    coll = MagicMock()
    conn.db.__getitem__.return_value = coll
    coll.count_documents.return_value = 42

    payload = json.dumps({"collection": "orders", "operation": "count", "filter": {}})
    result, err = executeMongoQuery(conn, payload)
    assert err is None
    assert result["rows"] == [[42]]


def test_connect_documentdb_enables_tls():
    from common.drivers import conMongo

    with patch.object(conMongo, "connectMongo", return_value="ok") as mock_connect:
        out = conMongo.connectDocumentDB(
            database="app",
            host="cluster.docdb.amazonaws.com",
            user="u",
            password="p",
            tls_ca_file="/tmp/ca.pem",
        )
        assert out == "ok"
        mock_connect.assert_called_once()
        assert mock_connect.call_args.kwargs["tls"] is True


def test_connect_mongo_without_pymongo():
    from common.drivers import conMongo

    with patch.object(conMongo, "MongoClient", None):
        assert conMongo.connectMongo(
            database="db", host="host", user="u", password="p"
        ) is None
