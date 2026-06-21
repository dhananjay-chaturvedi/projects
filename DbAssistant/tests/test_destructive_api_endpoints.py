"""Destructive/state-changing API coverage with throwaway resources.

These tests intentionally exercise real write/delete paths, but only against
temporary config homes, ephemeral saved profiles, temp files, and tables whose
names are generated for the test and dropped in cleanup.
"""

from __future__ import annotations

import csv
import uuid
from pathlib import Path

import pytest


pytestmark = [pytest.mark.integration]


@pytest.fixture
def full_api_client(tmp_config_dir, mysql_available, mysql_connect_kwargs, monkeypatch):
    """Full composite API client backed by an isolated config home.

    This differs from ``core_api_client`` in ``conftest.py``: that fixture wraps
    the core service with only the schema bridge, which is perfect for schema
    tests but intentionally does not expose monitoring-side service methods.
    """
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from common.headless.app_factory import create_app
    from common.connection_params import ConnectionParams
    from common.headless.db_service import CoreDBService

    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    core = CoreDBService()
    kw = mysql_connect_kwargs
    core.add_connection(
        ConnectionParams.from_mapping({
            "name": "api_mysql",
            "db_type": "MySQL",
            "host": kw["host"],
            "port": kw["port"],
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }),
    )
    client = TestClient(create_app(svc=core))
    yield client, "api_mysql"
    try:
        core.disconnect_all()
    except Exception:
        pass


def _tmp_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _drop_table(mysql_raw_conn, table: str) -> None:
    cur = mysql_raw_conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        mysql_raw_conn.commit()
    finally:
        cur.close()


def _create_table(mysql_raw_conn, table: str, ddl_tail: str) -> None:
    cur = mysql_raw_conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(f"CREATE TABLE `{table}` ({ddl_tail})")
        mysql_raw_conn.commit()
    finally:
        cur.close()


class TestConnectionDestructiveApi:
    def test_create_duplicate_delete_and_delete_missing_connection(
        self, core_api_client, mysql_connect_kwargs
    ):
        client, _, _ = core_api_client
        kw = mysql_connect_kwargs
        name = _tmp_name("api_conn")
        body = {
            "name": name,
            "db_type": "MySQL",
            "host": kw["host"],
            "port": str(kw["port"]),
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }

        created = client.post("/api/connections", json=body)
        assert created.status_code == 201, created.text
        assert created.json()["ok"] is True

        duplicate = client.post("/api/connections", json=body)
        assert duplicate.status_code == 400
        assert "already" in duplicate.text.lower() or "exists" in duplicate.text.lower()

        tested = client.post(f"/api/connections/{name}/test")
        assert tested.status_code == 200, tested.text

        deleted = client.delete(f"/api/connections/{name}")
        assert deleted.status_code == 200, deleted.text
        assert deleted.json()["ok"] is True

        tested_after_delete = client.post(f"/api/connections/{name}/test")
        assert tested_after_delete.status_code == 400

        deleted_again = client.delete(f"/api/connections/{name}")
        assert deleted_again.status_code == 404

    def test_create_connection_validation_rejects_blank_required_fields(
        self, core_api_client
    ):
        client, _, _ = core_api_client
        bad = client.post(
            "/api/connections",
            json={
                "name": "",
                "db_type": "",
                "host": "",
                "user": "",
                "password": "",
            },
        )
        assert bad.status_code == 422


class TestDashboardAndCacheDestructiveApi:
    def test_dashboard_layout_save_invalid_and_reset(self, core_api_client):
        client, _, _ = core_api_client
        original = client.get("/api/dashboard/layout")
        assert original.status_code == 200, original.text
        default_rows = original.json()["default_rows"]

        valid_rows = [["connections", "monitor"], ["sql_editor", None]]
        saved = client.put("/api/dashboard/layout", json={"rows": valid_rows})
        assert saved.status_code == 200, saved.text
        assert saved.json()["ok"] is True
        assert saved.json()["rows"] == valid_rows

        reloaded = client.get("/api/dashboard/layout")
        assert reloaded.status_code == 200, reloaded.text
        assert reloaded.json()["rows"] == valid_rows

        invalid_panel = client.put(
            "/api/dashboard/layout", json={"rows": [["not_a_panel", None]]}
        )
        assert invalid_panel.status_code == 400
        assert "unknown panel" in invalid_panel.text.lower()

        invalid_shape = client.put("/api/dashboard/layout", json={"rows": []})
        assert invalid_shape.status_code == 422

        reset = client.post("/api/dashboard/layout/reset")
        assert reset.status_code == 200, reset.text
        assert reset.json()["ok"] is True
        assert reset.json()["rows"] == default_rows

    def test_clear_caches_endpoint_is_repeat_safe(self, core_api_client):
        client, _, _ = core_api_client

        first = client.post("/api/app/clear-caches")
        assert first.status_code == 200, first.text
        assert first.json()["ok"] in (True, False)
        assert "summary" in first.json()

        second = client.post("/api/app/clear-caches")
        assert second.status_code == 200, second.text
        assert "summary" in second.json()


class TestSqlTransactionDestructiveApi:
    def test_autocommit_rollback_and_commit_against_throwaway_table(
        self, core_api_client, mysql_raw_conn
    ):
        client, _, conn = core_api_client
        table = _tmp_name("zz_api_tx")
        _create_table(mysql_raw_conn, table, "id INT PRIMARY KEY, note VARCHAR(50)")

        try:
            opened = client.post(f"/api/connections/{conn}/open")
            assert opened.status_code == 200, opened.text

            disabled = client.put(
                f"/api/query/{conn}/autocommit", json={"enabled": False}
            )
            assert disabled.status_code == 200, disabled.text
            assert disabled.json()["ok"] is True

            inserted = client.post(
                "/api/query",
                json={
                    "connection": conn,
                    "sql": f"INSERT INTO `{table}` (id, note) VALUES (1, 'rollback')",
                },
            )
            assert inserted.status_code == 200, inserted.text

            rolled_back = client.post(f"/api/query/{conn}/rollback")
            assert rolled_back.status_code == 200, rolled_back.text

            count_after_rollback = client.get(
                f"/api/objects/{conn}/count", params={"table": table}
            )
            assert count_after_rollback.status_code == 200, count_after_rollback.text
            assert count_after_rollback.json()["count"] == 0

            inserted_commit = client.post(
                "/api/query",
                json={
                    "connection": conn,
                    "sql": f"INSERT INTO `{table}` (id, note) VALUES (2, 'commit')",
                },
            )
            assert inserted_commit.status_code == 200, inserted_commit.text

            committed = client.post(f"/api/query/{conn}/commit")
            assert committed.status_code == 200, committed.text

            count_after_commit = client.get(
                f"/api/objects/{conn}/count", params={"table": table}
            )
            assert count_after_commit.status_code == 200, count_after_commit.text
            assert count_after_commit.json()["count"] == 1
        finally:
            client.put(f"/api/query/{conn}/autocommit", json={"enabled": True})
            _drop_table(mysql_raw_conn, table)

    def test_transaction_endpoints_fail_for_inactive_connection(self, core_api_client):
        client, _, _ = core_api_client
        assert client.post("/api/query/no_such_conn/commit").status_code == 404
        assert client.post("/api/query/no_such_conn/rollback").status_code == 404
        assert client.put(
            "/api/query/no_such_conn/autocommit", json={"enabled": True}
        ).status_code == 400


class TestObjectImportExportDestructiveApi:
    def test_import_export_and_invalid_import_paths(
        self, core_api_client, mysql_raw_conn, tmp_path, monkeypatch
    ):
        from common import paths as app_paths

        exports = tmp_path / "exports"
        exports.mkdir(parents=True)
        monkeypatch.setattr(app_paths, "exports_dir", lambda: exports)

        client, _, conn = core_api_client
        table = _tmp_name("zz_api_import")
        csv_path = exports / "import.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["id", "name", "price"])
            writer.writerow([1, "widget", "12.50"])
            writer.writerow([2, "gadget", "8.25"])

        try:
            imported = client.post(
                f"/api/objects/{conn}/import-csv",
                json={
                    "file_path": str(csv_path),
                    "table": table,
                    "create_table": True,
                    "chunk_size": 1,
                },
            )
            assert imported.status_code == 200, imported.text
            assert imported.json()["ok"] is True
            assert imported.json()["rows_inserted"] == 2

            count = client.get(f"/api/objects/{conn}/count", params={"table": table})
            assert count.status_code == 200, count.text
            assert count.json()["count"] == 2

            sample = client.get(
                f"/api/objects/{conn}/sample", params={"table": table, "limit": 1}
            )
            assert sample.status_code == 200, sample.text
            assert sample.json()["rowcount"] == 1

            export_csv = exports / "export.csv"
            exported = client.post(
                f"/api/objects/{conn}/export",
                json={"table": table, "output_path": str(export_csv), "format": "csv"},
            )
            assert exported.status_code == 200, exported.text
            assert exported.json()["ok"] is True
            assert export_csv.exists()

            export_json = exports / "export.json"
            exported_json = client.post(
                f"/api/objects/{conn}/export",
                json={"table": table, "output_path": str(export_json), "format": "json"},
            )
            assert exported_json.status_code == 200, exported_json.text
            assert exported_json.json()["ok"] is True
            assert export_json.exists()

            bad_format = client.post(
                f"/api/objects/{conn}/export",
                json={"table": table, "output_path": str(exports / "bad.txt"), "format": "xml"},
            )
            assert bad_format.status_code == 400
            assert "unsupported format" in bad_format.text.lower()

            missing_file = client.post(
                f"/api/objects/{conn}/import-csv",
                json={"file_path": str(exports / "missing.csv"), "table": _tmp_name("zz_missing")},
            )
            assert missing_file.status_code == 400
            assert "not found" in missing_file.text.lower()

            outside_exports = client.post(
                f"/api/objects/{conn}/import-csv",
                json={"file_path": str(tmp_path / "outside.csv"), "table": _tmp_name("zz_outside")},
            )
            assert outside_exports.status_code == 400
            assert "allowed directory" in outside_exports.text.lower()

            invalid_table = client.post(
                f"/api/objects/{conn}/import-csv",
                json={"file_path": str(csv_path), "table": "bad-name"},
            )
            assert invalid_table.status_code == 400
            assert "invalid table name" in invalid_table.text.lower()
        finally:
            _drop_table(mysql_raw_conn, table)


class TestMonitorDestructiveApi:
    def test_monitor_connection_and_db_connection_crud_are_isolated(
        self, full_api_client, mysql_connect_kwargs
    ):
        client, _ = full_api_client
        ssh_name = _tmp_name("api_ssh")
        db_name = _tmp_name("api_mon_db")
        kw = mysql_connect_kwargs

        added_ssh = client.post(
            "/api/monitor/connections/saved",
            json={
                "name": ssh_name,
                "host": "127.0.0.1",
                "username": "nobody",
                "password": "",
                "target_type": "vm",
            },
        )
        assert added_ssh.status_code == 201, added_ssh.text

        updated_ssh = client.put(
            f"/api/monitor/connections/saved/{ssh_name}",
            json={
                "host": "localhost",
                "username": "nobody",
                "password": "",
                "target_type": "db_server",
            },
        )
        assert updated_ssh.status_code == 200, updated_ssh.text

        deleted_ssh = client.delete(f"/api/monitor/connections/saved/{ssh_name}")
        assert deleted_ssh.status_code == 200, deleted_ssh.text
        assert client.delete(f"/api/monitor/connections/saved/{ssh_name}").status_code == 404

        added_db = client.post(
            "/api/monitor/db-connections",
            json={
                "name": db_name,
                "db_type": "MariaDB",
                "host": kw["host"],
                "port": str(kw["port"]),
                "database": kw["database"],
                "username": kw["user"],
                "password": kw["password"],
            },
        )
        assert added_db.status_code == 201, added_db.text

        # Monitor DB profiles must not leak into the core Connections API.
        core_names = {
            row.get("name")
            for row in client.get("/api/connections").json()
            if isinstance(row, dict)
        }
        assert db_name not in core_names

        tested_db = client.post(f"/api/monitor/db-connections/{db_name}/test")
        assert tested_db.status_code == 200, tested_db.text

        deleted_db = client.delete(f"/api/monitor/db-connections/{db_name}")
        assert deleted_db.status_code == 200, deleted_db.text
        assert client.delete(f"/api/monitor/db-connections/{db_name}").status_code == 404

    def test_alert_log_and_filtered_clear(self, full_api_client):
        client, _ = full_api_client
        instance = _tmp_name("api_alert")

        invalid = client.post(
            "/api/alerts",
            json={"severity": "BAD", "message": "bad", "source": "test", "instance": instance},
        )
        assert invalid.status_code == 400

        created = client.post(
            "/api/alerts",
            json={
                "severity": "WARNING",
                "message": "throwaway warning",
                "source": "pytest",
                "instance": instance,
            },
        )
        assert created.status_code == 201, created.text

        listed = client.get("/api/alerts", params={"instance": instance})
        assert listed.status_code == 200, listed.text
        assert listed.json().get("total", 0) >= 1

        cleared = client.delete(
            "/api/alerts", params={"source": "pytest", "instance": instance}
        )
        assert cleared.status_code == 200, cleared.text
        assert cleared.json()["ok"] is True

        invalid_clear = client.delete("/api/alerts", params={"severity": "BAD"})
        assert invalid_clear.status_code == 400
