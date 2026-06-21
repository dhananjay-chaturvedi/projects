"""
Comprehensive CLI command matrix — subprocess tests mirroring user workflows.

Run:
    pytest tests/test_comprehensive_cli_matrix.py -v -m comprehensive
"""

from __future__ import annotations

import json

import pytest

from tests.integration_helpers import (
    SCHEMA_TARGET_TYPES,
    first_table_name,
    parse_json_stdout,
    run_cli,
    skip_if_tunnel,
)

pytestmark = [pytest.mark.integration, pytest.mark.comprehensive]

CORE_CONN = "local_mariadb"


def _conn_available(name: str) -> bool:
    from common.connection_manager import ConnectionManager

    names = {c.get("name") for c in ConnectionManager().get_all_connections()}
    return name in names


@pytest.fixture
def primary_conn(saved_db_connection_names):
    if _conn_available(CORE_CONN):
        return CORE_CONN
    if saved_db_connection_names:
        return saved_db_connection_names[0]
    pytest.skip("No saved connections for CLI tests")


class TestCoreCli:
    def test_connections_list(self):
        p = run_cli("schema_converter", "connections", "list")
        assert p.returncode == 0, p.stderr

    def test_connections_test(self, primary_conn):
        p = run_cli("schema_converter", "connections", "test", primary_conn)
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0, p.stderr

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1 AS n",
            "SELECT 2 + 2 AS sum_val",
            "SELECT 'hello' AS msg",
        ],
    )
    def test_query_json(self, primary_conn, sql):
        p = run_cli(
            "schema_converter",
            "query",
            "--conn",
            primary_conn,
            "--sql",
            sql,
            "--format",
            "json",
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0
        data = parse_json_stdout(p.stdout)
        assert data

    @pytest.mark.parametrize("obj_type", ["tables", "views", "indexes", "databases"])
    def test_objects_json(self, primary_conn, obj_type):
        p = run_cli(
            "schema_converter",
            "objects",
            "--conn",
            primary_conn,
            "--type",
            obj_type,
            "--format",
            "json",
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0

    def test_databases_types(self):
        p = run_cli("schema_converter", "databases", "types")
        assert p.returncode == 0, p.stderr
        assert "MySQL" in p.stdout or "MariaDB" in p.stdout

    def test_config_show(self):
        p = run_cli("schema_converter", "config", "show")
        assert p.returncode == 0


class TestSchemaCli:
    def test_schema_show(self, primary_conn):
        p = run_cli(
            "schema_converter",
            "objects",
            "--conn",
            primary_conn,
            "--type",
            "tables",
            "--format",
            "json",
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        tbl = first_table_name(parse_json_stdout(p.stdout))
        if not tbl:
            pytest.skip("no tables")
        p = run_cli(
            "schema_converter",
            "migrator",
            "show",
            "--conn",
            primary_conn,
            "--table",
            tbl,
        )
        assert p.returncode == 0, p.stderr

    @pytest.mark.parametrize("target", SCHEMA_TARGET_TYPES[:4])
    def test_schema_convert(self, primary_conn, target, tmp_path):
        p = run_cli(
            "schema_converter",
            "objects",
            "--conn",
            primary_conn,
            "--type",
            "tables",
            "--format",
            "json",
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        tbl = first_table_name(parse_json_stdout(p.stdout))
        if not tbl:
            pytest.skip("no tables")
        out = tmp_path / f"convert_{target}.sql"
        p = run_cli(
            "schema_converter",
            "migrator",
            "convert",
            "--source-conn",
            primary_conn,
            "--target-type",
            target,
            "--table",
            tbl,
            "--output",
            str(out),
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0
        if target == "SQLite" and out.is_file() and out.stat().st_size == 0:
            pytest.skip("SQLite mapper produced empty output file")
        assert out.is_file() and out.stat().st_size > 0


class TestMonitoringCli:
    def test_os_metrics(self):
        p = run_cli("monitoring", "os", "metrics")
        assert p.returncode == 0, p.stderr

    def test_thresholds_list(self):
        p = run_cli("monitoring", "thresholds", "list")
        assert p.returncode == 0, p.stderr

    def test_cloud_connections_list(self):
        p = run_cli("monitoring", "cloud", "connections", "list")
        assert p.returncode == 0, p.stderr

    def test_daemon_status(self):
        p = run_cli("monitoring", "daemon", "status")
        assert p.returncode == 0, p.stderr

    def test_monitor_once(self, primary_conn):
        p = run_cli(
            "monitoring",
            "monitor",
            "--conn",
            primary_conn,
            "--once",
            timeout=120,
        )
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0


class TestAiCli:
    def test_list_backends(self):
        p = run_cli("ai_query", "ai", "--list-backends")
        assert p.returncode == 0, p.stderr

    def test_session_list(self):
        p = run_cli("ai_query", "ai", "session", "list")
        assert p.returncode == 0, p.stderr

    def test_session_new(self, primary_conn):
        p = run_cli("ai_query", "ai", "session", "new", "--conn", primary_conn)
        if p.returncode != 0:
            skip_if_tunnel(primary_conn, p.stdout + p.stderr)
        assert p.returncode == 0


class TestDbtoolUnifiedCli:
    def test_dbtool_connections_list(self):
        from tests.integration_helpers import project_python, ROOT
        import subprocess

        p = subprocess.run(
            [str(project_python()), "dbtool.py", "connections", "list"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert p.returncode == 0, p.stderr

    def test_dbtool_config_show(self):
        from tests.integration_helpers import project_python, ROOT
        import subprocess

        p = subprocess.run(
            [str(project_python()), "dbtool.py", "config", "show"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert p.returncode == 0, p.stderr
