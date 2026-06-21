"""Tests for document migration adapters."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from schema_converter.adapters import (
    migration_pair_kind,
    transfer_document_collection,
    validate_migration_pair,
)


def test_migration_pair_kind():
    assert migration_pair_kind("MongoDB", "DocumentDB") == "document"
    assert migration_pair_kind("PostgreSQL", "MariaDB") == "relational"
    assert migration_pair_kind("MySQL", "MongoDB") == "mixed"


def test_validate_migration_pair_blocks_mixed():
    err = validate_migration_pair("PostgreSQL", "MongoDB")
    assert err is not None
    assert "not supported yet" in err


def test_transfer_document_collection_batches():
    source_mgr = MagicMock()
    target_mgr = MagicMock()
    source_mgr.db_type = "MongoDB"
    target_mgr.db_type = "MongoDB"
    source_mgr.conn = MagicMock()
    target_mgr.conn = MagicMock()

    docs_batch_1 = [{"_id": 1, "name": "alpha"}, {"_id": 2, "name": "beta"}]
    docs_batch_2 = [{"_id": 3, "name": "gamma"}]

    with patch("schema_converter.adapters.DatabaseRegistry.get_operation") as get_op:
        read_fn = MagicMock(side_effect=[docs_batch_1, docs_batch_2, []])
        write_fn = MagicMock(side_effect=[2, 1])
        get_op.side_effect = lambda db_type, op: read_fn if op == "readMongoDocuments" else write_fn

        rows = transfer_document_collection(
            source_mgr,
            target_mgr,
            "users",
            "users_copy",
            batch_size=2,
        )

    assert rows == 3
    assert read_fn.call_count == 2
    assert write_fn.call_count == 2
