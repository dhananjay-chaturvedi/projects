"""
Comprehensive saved-connection matrix — every profile in ~/.dbmanager.

Exercises test, query, objects, schema, and monitor paths per connection.
Skips tunnel-dependent hosts when SSH is down.

Run:
    pytest tests/test_comprehensive_saved_connections.py -v -m comprehensive
    DBTOOL_TEST_CONNS=local_mariadb,aws_stg_pushdb pytest tests/test_comprehensive_saved_connections.py -v
"""

from __future__ import annotations

import pytest

from tests.integration_helpers import (
    MYSQL_ONLY_SQL_LABELS,
    PORTABLE_SQL_CASES,
    MYSQL_OBJECT_TYPES,
    assert_query_result,
    first_table_name,
    skip_if_tunnel,
)

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


def _core_svc():
    from common.headless.db_service import CoreDBService

    return CoreDBService()


def pytest_generate_tests(metafunc):
    if "conn_name" in metafunc.fixturenames:
        from tests.integration_helpers import load_saved_db_connection_names

        names = load_saved_db_connection_names()
        if not names:
            names = ["__no_saved_conns__"]
        metafunc.parametrize("conn_name", names, ids=str)


_REACHABLE: dict[str, bool | str] = {}


def _ensure_reachable(conn_name: str) -> None:
    if conn_name == "__no_saved_conns__":
        pytest.skip("No saved database connections found")
    if conn_name in _REACHABLE:
        cached = _REACHABLE[conn_name]
        if cached is True:
            return
        pytest.skip(str(cached))
    svc = _core_svc()
    test = svc.test_connection(conn_name)
    if not test.get("ok"):
        msg = test.get("message", "unreachable")
        skip_if_tunnel(conn_name, msg)
        _REACHABLE[conn_name] = f"{conn_name}: {msg}"
        pytest.skip(str(_REACHABLE[conn_name]))
    _REACHABLE[conn_name] = True


@pytest.mark.parametrize("case", PORTABLE_SQL_CASES, ids=lambda c: c.label)
def test_saved_conn_select_queries(conn_name, case):
    _ensure_reachable(conn_name)
    svc = _core_svc()
    profile = svc.get_connection_profile(conn_name) or {}
    db_type = profile.get("db_type", "")
    if case.label in MYSQL_ONLY_SQL_LABELS and db_type not in ("MySQL", "MariaDB"):
        pytest.skip(f"{case.label} is MySQL-specific")
    result = svc.execute(conn_name, case.sql)
    if result.get("error"):
        skip_if_tunnel(conn_name, result["error"])
    assert_query_result(case, result)
    svc.disconnect(conn_name)


@pytest.mark.parametrize("obj_type", MYSQL_OBJECT_TYPES)
def test_saved_conn_object_types(conn_name, obj_type):
    _ensure_reachable(conn_name)
    svc = _core_svc()
    profile = svc.get_connection_profile(conn_name)
    db_type = (profile or {}).get("db_type", "")
    if db_type and db_type not in ("MySQL", "MariaDB") and obj_type in (
        "events",
        "engines",
        "charsets",
    ):
        pytest.skip(f"{obj_type} not applicable to {db_type}")
    items = svc.get_objects(conn_name, obj_type)
    if items and isinstance(items[0], dict) and "error" in items[0]:
        err = items[0]["error"]
        if "not supported" in err.lower():
            return
        skip_if_tunnel(conn_name, err)
        pytest.fail(f"{conn_name} {obj_type}: {err}")
    assert isinstance(items, list)
    svc.disconnect(conn_name)


def test_saved_conn_tables_and_schema(conn_name):
    _ensure_reachable(conn_name)
    svc = _core_svc()
    tables = svc.get_objects(conn_name, "tables")
    if tables and isinstance(tables[0], dict) and "error" in tables[0]:
        skip_if_tunnel(conn_name, tables[0]["error"])
        pytest.fail(tables[0]["error"])
    tbl = first_table_name(tables)
    if tbl:
        schema = svc.get_table_schema(conn_name, tbl)
        if schema.get("error"):
            skip_if_tunnel(conn_name, schema["error"])
        assert schema.get("table") == tbl
        assert isinstance(schema.get("columns"), list)
    svc.disconnect(conn_name)


def test_saved_conn_schema_convert_targets(conn_name):
    _ensure_reachable(conn_name)
    from schema_converter.bridge import make_service

    svc = _core_svc()
    composite = make_service(svc)
    tables = composite.get_objects(conn_name, "tables")
    tbl = first_table_name(tables)
    if not tbl:
        pytest.skip(f"{conn_name}: no tables")
    for target in ("MySQL", "PostgreSQL", "MariaDB"):
        result = composite.convert_schema(conn_name, target, tbl)
        if result.get("error"):
            skip_if_tunnel(conn_name, result["error"])
            pytest.fail(f"{target}: {result['error']}")
        assert result.get("ddl")
    svc.disconnect(conn_name)


@pytest.mark.comprehensive
def test_saved_conn_compare_schema_self(conn_name):
    _ensure_reachable(conn_name)
    from schema_converter.bridge import make_service

    svc = _core_svc()
    composite = make_service(svc)
    tbl = first_table_name(composite.get_objects(conn_name, "tables"))
    if not tbl:
        pytest.skip(f"{conn_name}: no tables")
    result = composite.compare_schema(conn_name, conn_name, tbl)
    if result.get("error"):
        skip_if_tunnel(conn_name, result["error"])
    assert result.get("match") is True or not result.get("issues")
    svc.disconnect(conn_name)
