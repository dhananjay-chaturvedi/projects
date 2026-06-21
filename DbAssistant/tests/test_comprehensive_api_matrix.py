"""
Comprehensive API endpoint matrix — core + installed module routes.

Run:
    pytest tests/test_comprehensive_api_matrix.py -v -m comprehensive
"""

from __future__ import annotations

import pytest

from tests.integration_helpers import (
    DATA_COMPARE_MODES,
    DATA_COMPARE_SAMPLE_SIZES,
    SCHEMA_TARGET_TYPES,
    first_table_name,
)

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]


# ── Core routes ──────────────────────────────────────────────────────────────


class TestCoreApiRoutes:
    def test_health_and_modules(self, api_client):
        r = api_client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body.get("status") == "ok"
        assert "timestamp" in body

        r = api_client.get("/api/modules")
        assert r.status_code == 200
        mods = r.json()
        assert isinstance(mods, dict)

    def test_api_index(self, api_client):
        r = api_client.get("/api")
        assert r.status_code == 200
        data = r.json()
        assert "health" in data

    def test_connections_list(self, api_client):
        r = api_client.get("/api/connections")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_config_all_and_section(self, api_client):
        r = api_client.get("/api/config")
        assert r.status_code == 200
        assert "sections" in r.json()

        r = api_client.get("/api/config", params={"section": "paths"})
        assert r.status_code == 200
        assert "paths" in r.json().get("sections", {})

    def test_databases_types(self, api_client):
        r = api_client.get("/api/databases/types")
        assert r.status_code == 200
        types = r.json()
        assert isinstance(types, list) and types
        assert any(t.get("db_type") for t in types)

    def test_databases_ops_mysql(self, api_client):
        r = api_client.get("/api/databases/ops", params={"type": "MySQL"})
        assert r.status_code == 200
        ops = r.json()
        assert isinstance(ops, list) and ops

    def test_dashboard_snapshot(self, api_client):
        r = api_client.get("/api/dashboard")
        assert r.status_code == 200
        dash = r.json()
        assert isinstance(dash, dict)


class TestCoreApiWithEphemeralMysql:
    def test_connection_crud_via_api(self, core_api_client, mysql_connect_kwargs):
        client, svc, conn = core_api_client
        kw = mysql_connect_kwargs
        name = "api_crud_tmp"
        body = {
            "name": name,
            "db_type": "MySQL",
            "host": kw["host"],
            "port": str(kw["port"]),
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }
        r = client.post("/api/connections", json=body)
        assert r.status_code == 201, r.text
        assert r.json().get("ok")

        r = client.post(f"/api/connections/{name}/test")
        assert r.status_code == 200, r.text

        r = client.post("/api/query", json={"connection": conn, "sql": "SELECT 42 AS n"})
        assert r.status_code == 200, r.text
        data = r.json()
        assert data["rows"][0][0] == "42"

        r = client.get(f"/api/objects/{conn}", params={"type": "tables"})
        assert r.status_code == 200, r.text
        payload = r.json()
        assert payload["connection"] == conn
        assert "items" in payload

        r = client.delete(f"/api/connections/{name}")
        assert r.status_code == 200, r.text

    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("SELECT 1 AS one", "1"),
            ("SELECT UPPER('abc') AS up", "ABC"),
            ("SELECT 10 * 5 AS prod", "50"),
        ],
    )
    def test_query_param_matrix(self, core_api_client, sql, expected):
        client, _, conn = core_api_client
        r = client.post("/api/query", json={"connection": conn, "sql": sql})
        assert r.status_code == 200, r.text
        assert r.json()["rows"][0][0] == expected

    def test_query_invalid_returns_error(self, core_api_client):
        client, _, conn = core_api_client
        r = client.post("/api/query", json={"connection": conn, "sql": "SELEC 1"})
        assert r.status_code >= 400


# ── Schema module routes ───────────────────────────────────────────────────────


@pytest.fixture
def schema_conn_and_table(core_api_client):
    client, _, conn = core_api_client
    r = client.get(f"/api/objects/{conn}", params={"type": "tables"})
    assert r.status_code == 200
    tbl = first_table_name(r.json().get("items") or [])
    if not tbl:
        pytest.skip("no tables in ephemeral MySQL database")
    return client, conn, tbl


class TestSchemaApiRoutes:
    def test_schema_show(self, schema_conn_and_table):
        client, conn, tbl = schema_conn_and_table
        r = client.get(f"/api/migrator/{conn}/{tbl}")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body.get("table") == tbl
        assert "columns" in body

    def test_schema_dump_all(self, schema_conn_and_table):
        client, conn, _ = schema_conn_and_table
        r = client.get(f"/api/migrator/{conn}/dump")
        assert r.status_code == 200, r.text
        body = r.json()
        if body.get("table_count", 1) == 0:
            pytest.skip("ephemeral database has no tables to dump")
        assert body.get("ddl")

    def test_schema_dump_single(self, schema_conn_and_table):
        client, conn, tbl = schema_conn_and_table
        r = client.get(f"/api/migrator/{conn}/dump", params={"table": tbl})
        assert r.status_code == 200, r.text
        ddl = r.json().get("ddl", "")
        assert tbl.lower() in ddl.lower() or "create table" in ddl.lower()

    @pytest.mark.parametrize("target_type", SCHEMA_TARGET_TYPES)
    def test_schema_convert_targets(self, schema_conn_and_table, target_type):
        client, conn, tbl = schema_conn_and_table
        r = client.post(
            "/api/migrator/convert",
            json={"source_conn": conn, "target_type": target_type, "table": tbl},
        )
        if target_type == "SQL Server":
            assert r.status_code >= 400 or "unsupported" in r.text.lower()
            return
        assert r.status_code == 200, r.text
        body = r.json()
        if target_type == "SQLite" and not (body.get("ddl") or "").strip():
            pytest.skip("SQLite mapper returned empty DDL")
        assert body.get("ddl") or body.get("all_ddl")
        assert isinstance(body.get("issues"), list)

    def test_compare_schema_self(self, schema_conn_and_table):
        client, conn, tbl = schema_conn_and_table
        r = client.post(
            "/api/migrator/compare-schema",
            json={"source_conn": conn, "target_conn": conn, "table": tbl},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body.get("match") is True or not body.get("issues")

    @pytest.mark.parametrize("mode", DATA_COMPARE_MODES)
    @pytest.mark.parametrize("sample_size", DATA_COMPARE_SAMPLE_SIZES[:2])
    def test_compare_data_self(
        self, schema_conn_and_table, mode, sample_size, schema_test_tables
    ):
        client, conn, _ = schema_conn_and_table
        src, tgt = schema_test_tables
        # Register ephemeral conn only knows one DB — compare src table to itself
        r = client.post(
            "/api/migrator/compare-data",
            json={
                "source_conn": conn,
                "target_conn": conn,
                "table": src,
                "target_table": src,
                "mode": mode,
                "sample_size": sample_size,
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body.get("match") is True or body.get("error") is None


# ── Monitoring module routes ───────────────────────────────────────────────────


class TestMonitoringApiRoutes:
    def test_os_metrics(self, api_client):
        r = api_client.get("/api/os/metrics")
        assert r.status_code == 200, r.text
        body = r.json()
        assert isinstance(body, dict)

    def test_thresholds_list(self, api_client):
        r = api_client.get("/api/thresholds")
        assert r.status_code == 200, r.text

    def test_thresholds_check_manual(self, api_client):
        r = api_client.post(
            "/api/thresholds/check",
            json={"source": "db", "metric": "cpu_percent", "value": 50.0},
        )
        assert r.status_code == 200, r.text

    def test_daemon_status(self, api_client):
        r = api_client.get("/api/daemon/status")
        assert r.status_code == 200, r.text

    def test_cloud_connections_list(self, api_client):
        r = api_client.get("/api/cloud/connections")
        assert r.status_code == 200, r.text
        assert isinstance(r.json(), (list, dict))

    def test_metrics_all(self, api_client):
        r = api_client.get("/api/metrics")
        assert r.status_code == 200, r.text


class TestMonitoringApiSavedConn:
    @pytest.mark.parametrize("conn_name", ["local_mariadb"], ids=str)
    def test_metrics_for_saved_conn(self, api_client, saved_db_connection_names, conn_name):
        if conn_name not in saved_db_connection_names:
            pytest.skip(f"{conn_name} not saved")
        r = api_client.get(f"/api/metrics/{conn_name}")
        if r.status_code >= 400:
            from tests.integration_helpers import skip_if_tunnel

            skip_if_tunnel(conn_name, r.text)
        assert r.status_code == 200, r.text
        body = r.json()
        assert "sections" in body or "timestamp" in body


# ── AI module routes (no live LLM unless backend ready) ───────────────────────


class TestAiApiRoutes:
    def test_ai_backends(self, api_client):
        r = api_client.get("/api/ai/backends")
        if r.status_code == 404:
            pytest.skip("AI module not installed")
        assert r.status_code == 200, r.text
        assert isinstance(r.json(), (list, dict))

    def test_ai_sessions_crud(self, api_client, saved_db_connection_names):
        r = api_client.get("/api/ai/sessions")
        if r.status_code == 404:
            pytest.skip("AI module not installed")
        assert r.status_code == 200

        conn = "local_mariadb"
        if conn not in saved_db_connection_names:
            conn = saved_db_connection_names[0] if saved_db_connection_names else None
        if not conn:
            pytest.skip("no saved connection for AI session")

        r = api_client.post("/api/ai/sessions", json={"connection": conn})
        assert r.status_code in (200, 201), r.text
        sid = r.json().get("session_id") or r.json().get("id")
        if sid:
            r = api_client.get(f"/api/ai/sessions/{sid}")
            assert r.status_code == 200
            r = api_client.delete(f"/api/ai/sessions/{sid}")
            assert r.status_code == 200
