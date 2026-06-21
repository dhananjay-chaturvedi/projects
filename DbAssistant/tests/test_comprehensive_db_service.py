"""
Comprehensive CoreDBService tests — SQL datasets, object types, connections.

Run (live MySQL required):
    pytest tests/test_comprehensive_db_service.py -v -m comprehensive
"""

from __future__ import annotations

import pytest

from tests.integration_helpers import (
    DML_SQL_SEQUENCE,
    INVALID_SQL_CASES,
    MYSQL_OBJECT_TYPES,
    SELECT_SQL_CASES,
    assert_query_result,
    skip_if_tunnel,
)

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


@pytest.mark.parametrize("case", SELECT_SQL_CASES, ids=lambda c: c.label)
def test_execute_select_cases(mysql_svc, case):
    svc, conn = mysql_svc
    result = svc.execute(conn, case.sql)
    assert_query_result(case, result)


@pytest.mark.parametrize("case", INVALID_SQL_CASES, ids=lambda c: c.label)
def test_execute_invalid_sql(mysql_svc, case):
    svc, conn = mysql_svc
    result = svc.execute(conn, case.sql)
    assert_query_result(case, result)


def test_execute_dml_lifecycle(mysql_svc):
    svc, conn = mysql_svc
    table = "pytest_comp_dml_tmp"
    for raw in DML_SQL_SEQUENCE:
        sql = raw.format(table=table)
        result = svc.execute(conn, sql)
        if "SELECT" in sql.upper():
            assert not result.get("error"), result.get("error")
            rows = result.get("rows") or []
            assert len(rows) == 2
            tags = [r[0] for r in rows]
            assert "pytest_a" in tags and "pytest_b" in tags
            amounts = {r[0]: float(r[1]) for r in rows}
            assert amounts["pytest_a"] == 99.9
            assert amounts["pytest_b"] == 20.0
        else:
            assert not result.get("error"), f"{sql}: {result.get('error')}"
    svc.execute(conn, f"DROP TABLE IF EXISTS `{table}`")


def _first_scalar(result: dict) -> str:
    if result.get("multiple_results"):
        for part in result.get("results") or []:
            val = _first_scalar(part)
            if val:
                return val
        return ""
    rows = result.get("rows") or []
    if not rows:
        return ""
    row = rows[0]
    cell = row[0] if isinstance(row, (list, tuple)) else row
    return str(cell)


def test_execute_multi_statement(mysql_svc):
    svc, conn = mysql_svc
    result = svc.execute(conn, "SET @pytest_x := 7; SELECT @pytest_x AS x;")
    assert not result.get("error"), result.get("error")
    assert _first_scalar(result) == "7"


@pytest.mark.parametrize("obj_type", MYSQL_OBJECT_TYPES)
def test_get_objects_mysql_types(mysql_svc, obj_type):
    svc, conn = mysql_svc
    items = svc.get_objects(conn, obj_type)
    assert isinstance(items, list)
    if items and isinstance(items[0], dict) and "error" in items[0]:
        pytest.fail(f"{obj_type}: {items[0]['error']}")
    if obj_type == "tables":
        assert len(items) >= 0


def test_get_table_schema_on_dml_table(mysql_svc):
    svc, conn = mysql_svc
    table = "pytest_comp_schema_introspect"
    svc.execute(
        conn,
        f"CREATE TABLE IF NOT EXISTS `{table}` ("
        f"id INT PRIMARY KEY, label VARCHAR(32))",
    )
    try:
        schema = svc.get_table_schema(conn, table)
        assert not schema.get("error"), schema.get("error")
        assert schema["table"] == table
        col_names = [c.get("name") or c.get("column") or str(c) for c in schema["columns"]]
        assert any("id" in str(n).lower() for n in col_names)
    finally:
        svc.execute(conn, f"DROP TABLE IF EXISTS `{table}`")


def test_connection_crud_roundtrip(tmp_config_dir, mysql_available, mysql_connect_kwargs):
    from common.connection_params import ConnectionParams
    from common.headless.db_service import CoreDBService

    svc = CoreDBService()
    kw = mysql_connect_kwargs
    name = "pytest_crud_conn"
    add = svc.add_connection(
        ConnectionParams.from_mapping({
            "name": name,
            "db_type": "MySQL",
            "host": kw["host"],
            "port": kw["port"],
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }),
    )
    assert add["ok"]
    listed = {c["name"] for c in svc.list_connections()}
    assert name in listed
    test = svc.test_connection(name)
    assert test["ok"], test.get("message")
    removed = svc.remove_connection(name)
    assert removed["ok"]
    assert name not in {c["name"] for c in svc.list_connections()}


def test_list_db_types_and_ops(mysql_svc):
    svc, conn = mysql_svc
    types = svc.list_db_types()
    assert isinstance(types, list) and types
    mysql_row = next((t for t in types if t["db_type"] in ("MySQL", "MariaDB")), None)
    assert mysql_row is not None
    ops = svc.list_db_ops(mysql_row["db_type"])
    assert isinstance(ops, list) and ops
    op_names = {o["operation"] for o in ops}
    assert any("table" in n.lower() for n in op_names)


def test_show_config_sections(mysql_svc):
    svc, _ = mysql_svc
    all_cfg = svc.show_config()
    assert not all_cfg.get("error")
    assert "sections" in all_cfg and all_cfg["sections"]
    paths = svc.show_config(section="paths")
    assert "paths" in paths.get("sections", {})


@pytest.mark.parametrize("conn_name", ["local_mariadb"], ids=str)
def test_saved_connection_live(saved_db_connection_names, conn_name):
    """Exercise a well-known saved profile when present."""
    if conn_name not in saved_db_connection_names:
        pytest.skip(f"{conn_name} not in saved connections")
    from common.headless.db_service import CoreDBService

    svc = CoreDBService()
    test = svc.test_connection(conn_name)
    if not test["ok"]:
        skip_if_tunnel(conn_name, test.get("message", ""))
        pytest.fail(test.get("message"))
    result = svc.execute(conn_name, "SELECT 1 AS n")
    assert not result.get("error"), result.get("error")
    assert result["rows"][0][0] == "1"
    svc.disconnect(conn_name)
