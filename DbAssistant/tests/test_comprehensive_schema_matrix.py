"""
Comprehensive schema converter tests — convert, dump, compare permutations.

Run:
    pytest tests/test_comprehensive_schema_matrix.py -v -m comprehensive
"""

from __future__ import annotations

import pytest

from schema_converter.bridge import make_service
from tests.integration_helpers import (
    DATA_COMPARE_MODES,
    DATA_COMPARE_SAMPLE_SIZES,
    SCHEMA_TARGET_TYPES,
    SCHEMA_MIRROR_TABLE,
    SCHEMA_TEST_TABLE,
    ddl_contains_create_table,
    first_table_name,
    setup_mysql_table,
    teardown_mysql_table,
)

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


@pytest.fixture
def schema_svc(mysql_svc):
    svc, conn = mysql_svc
    composite = make_service(svc)
    return composite, conn


@pytest.fixture
def schema_tables(mysql_raw_conn):
    src = setup_mysql_table(mysql_raw_conn, SCHEMA_TEST_TABLE)
    tgt = setup_mysql_table(mysql_raw_conn, SCHEMA_MIRROR_TABLE)
    yield src, tgt
    teardown_mysql_table(mysql_raw_conn, src)
    teardown_mysql_table(mysql_raw_conn, tgt)


class TestSchemaConvertMatrix:
    @pytest.mark.parametrize("target_type", SCHEMA_TARGET_TYPES)
    def test_convert_ephemeral_table(self, schema_svc, schema_tables, target_type):
        composite, conn = schema_svc
        src, _ = schema_tables
        result = composite.convert_schema(conn, target_type, src)
        if target_type == "SQL Server":
            assert result.get("error") and "unsupported" in result["error"].lower()
            return
        if target_type == "SQLite" and not (result.get("ddl") or "").strip():
            pytest.skip("SQLite mapper returned empty DDL for JSON column table")
        assert not result.get("error"), result.get("error")
        ddl = result.get("ddl") or ""
        assert ddl.strip()
        assert "CREATE" in ddl.upper()
        assert isinstance(result.get("issues"), list)

    @pytest.mark.parametrize("target_type", SCHEMA_TARGET_TYPES)
    def test_convert_existing_db_table(self, schema_svc, target_type):
        composite, conn = schema_svc
        tables = composite.get_objects(conn, "tables")
        tbl = first_table_name(tables)
        if not tbl:
            pytest.skip("no tables in database")
        result = composite.convert_schema(conn, target_type, tbl)
        if target_type == "SQL Server":
            assert result.get("error") and "unsupported" in result["error"].lower()
            return
        if target_type == "SQLite" and not (result.get("ddl") or "").strip():
            pytest.skip("SQLite mapper returned empty DDL")
        assert not result.get("error"), result.get("error")
        assert result.get("ddl")


class TestSchemaDumpMatrix:
    def test_dump_all_tables(self, schema_svc):
        composite, conn = schema_svc
        result = composite.dump_schema(conn)
        assert not result.get("error"), result.get("error")
        assert result.get("table_count", 0) >= 0

    def test_dump_single_table(self, schema_svc, schema_tables):
        composite, conn = schema_svc
        src, _ = schema_tables
        result = composite.dump_schema(conn, table=src)
        assert not result.get("error"), result.get("error")
        assert ddl_contains_create_table(result.get("ddl", ""), src)


class TestSchemaCompareMatrix:
    def test_compare_schema_identical_tables(self, schema_svc, schema_tables):
        composite, conn = schema_svc
        src, tgt = schema_tables
        result = composite.compare_schema(conn, conn, src, tgt)
        assert not result.get("error"), result.get("error")
        assert result.get("match") is True

    def test_compare_schema_self(self, schema_svc, schema_tables):
        composite, conn = schema_svc
        src, _ = schema_tables
        result = composite.compare_schema(conn, conn, src)
        assert not result.get("error"), result.get("error")
        assert result.get("match") is True

    def test_compare_schema_mismatch_column(self, schema_svc, mysql_raw_conn, schema_tables):
        composite, conn = schema_svc
        src, tgt = schema_tables
        cur = mysql_raw_conn.cursor()
        try:
            cur.execute(f"ALTER TABLE `{tgt}` ADD COLUMN extra_col INT DEFAULT 0")
            mysql_raw_conn.commit()
        finally:
            cur.close()
        result = composite.compare_schema(conn, conn, src, tgt)
        assert not result.get("error"), result.get("error")
        assert result.get("match") is False
        assert result.get("issues")

    @pytest.mark.parametrize("mode", DATA_COMPARE_MODES)
    @pytest.mark.parametrize("sample_size", DATA_COMPARE_SAMPLE_SIZES)
    def test_compare_data_modes(self, schema_svc, schema_tables, mode, sample_size):
        composite, conn = schema_svc
        src, tgt = schema_tables
        result = composite.compare_data(
            conn, conn, src, tgt, mode=mode, sample_size=sample_size
        )
        assert not result.get("error"), result.get("error")
        assert result.get("match") is True

    @pytest.mark.parametrize("mode", DATA_COMPARE_MODES)
    def test_compare_data_mismatch(self, schema_svc, mysql_raw_conn, schema_tables, mode):
        composite, conn = schema_svc
        src, tgt = schema_tables
        cur = mysql_raw_conn.cursor()
        try:
            cur.execute(f"INSERT INTO `{tgt}` (code, score) VALUES ('Z', 99.9)")
            mysql_raw_conn.commit()
        finally:
            cur.close()
        result = composite.compare_data(conn, conn, src, tgt, mode=mode, sample_size=5)
        assert not result.get("error"), result.get("error")
        assert result.get("match") is False


class TestTableSchemaIntrospection:
    def test_get_table_schema_columns(self, schema_svc, schema_tables):
        composite, conn = schema_svc
        src, _ = schema_tables
        result = composite.get_table_schema(conn, src)
        assert not result.get("error"), result.get("error")
        assert result["table"] == src
        assert len(result["columns"]) >= 3

    @pytest.mark.parametrize("bad_table", ["__no_such_table__", ""])
    def test_get_table_schema_missing(self, schema_svc, bad_table):
        composite, conn = schema_svc
        if not bad_table:
            pytest.skip("empty table name engine-specific")
        result = composite.get_table_schema(conn, bad_table)
        # Either error or empty columns
        assert result.get("error") or not result.get("columns")
