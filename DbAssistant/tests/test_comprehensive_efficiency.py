"""
Efficiency assertions for core query paths — ensures responses stay within bounds.

Run:
    pytest tests/test_comprehensive_efficiency.py -v -m comprehensive
"""

from __future__ import annotations

import time

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


SIMPLE_QUERY_BUDGET_MS = 15_000.0
OBJECTS_BUDGET_MS = 30_000.0
SCHEMA_BUDGET_MS = 45_000.0


def test_query_latency_budget(mysql_svc):
    svc, conn = mysql_svc
    start = time.perf_counter()
    result = svc.execute(conn, "SELECT 1 AS n")
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert not result.get("error")
    reported = float(result.get("time_ms") or 0)
    assert reported <= SIMPLE_QUERY_BUDGET_MS
    assert elapsed_ms <= SIMPLE_QUERY_BUDGET_MS + 500


def test_objects_tables_latency(mysql_svc):
    svc, conn = mysql_svc
    start = time.perf_counter()
    items = svc.get_objects(conn, "tables")
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert isinstance(items, list)
    if items and isinstance(items[0], dict) and "error" in items[0]:
        pytest.fail(items[0]["error"])
    assert elapsed_ms <= OBJECTS_BUDGET_MS


def test_schema_convert_latency(mysql_svc):
    from schema_converter.bridge import make_service
    from tests.integration_helpers import first_table_name

    svc, conn = mysql_svc
    composite = make_service(svc)
    tbl = first_table_name(composite.get_objects(conn, "tables"))
    if not tbl:
        pytest.skip("no tables")
    start = time.perf_counter()
    result = composite.convert_schema(conn, "PostgreSQL", tbl)
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert not result.get("error"), result.get("error")
    assert elapsed_ms <= SCHEMA_BUDGET_MS


@pytest.mark.parametrize("iterations", [3, 5])
def test_repeated_query_stable(mysql_svc, iterations):
    svc, conn = mysql_svc
    times = []
    for _ in range(iterations):
        result = svc.execute(conn, "SELECT SLEEP(0) AS ok, 1 AS n")
        assert not result.get("error")
        times.append(float(result.get("time_ms") or 0))
    assert max(times) <= SIMPLE_QUERY_BUDGET_MS
    # Later runs should not be dramatically slower than first (no connection leak)
    assert max(times) <= min(times) * 5 + 1000
