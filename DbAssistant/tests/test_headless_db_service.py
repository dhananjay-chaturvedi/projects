"""DBService smoke tests."""

from __future__ import annotations

from unittest.mock import patch
from unittest.mock import MagicMock

from app.headless.db_service import DBService
from common.headless.db_service import CoreDBService


def test_db_service_list_connections_empty():
    svc = DBService()
    with patch.object(svc._cm, "get_all_connections", return_value=[]):
        out = svc.list_connections()
    assert isinstance(out, list)


def test_db_service_has_connection_manager():
    svc = DBService()
    assert svc._cm is not None


def test_core_add_connection_propagates_duplicate_error():
    from common.connection_params import ConnectionParams

    svc = CoreDBService()
    with patch.object(
        svc._cm,
        "add_connection",
        return_value=(False, "Connection name already exists"),
    ):
        out = svc.add_connection(
            ConnectionParams.from_mapping({
                "name": "dup",
                "db_type": "MySQL",
                "host": "localhost",
                "port": 3306,
                "user": "u",
                "password": "p",
                "database": "test",
            }),
        )
    assert out == {"ok": False, "message": "Connection name already exists"}


def test_core_execute_preserves_truncation_metadata():
    svc = CoreDBService()
    mgr = MagicMock()
    mgr.execute_query.return_value = (
        {
            "columns": ["id"],
            "rows": [(1,), (2,)],
            "rowcount": 2,
            "time": 0.001,
            "truncated": True,
            "max_rows": 2,
        },
        None,
    )
    with patch.object(svc, "_get_or_connect", return_value=mgr):
        out = svc.execute("c", "SELECT * FROM big_table")
    assert out["error"] is None
    assert out["rows"] == [["1"], ["2"]]
    assert out["truncated"] is True
    assert out["max_rows"] == 2


def test_core_split_sql_handles_strings_comments_and_dollar_quotes():
    sql = (
        "SELECT 'a;b'; # ignored; comment\n"
        "DO $$ BEGIN RAISE NOTICE 'x;y'; END $$; "
        "SELECT 'it''s still one; string';"
    )
    assert CoreDBService._split_sql_statements(sql) == [
        "SELECT 'a;b'",
        "# ignored; comment\nDO $$ BEGIN RAISE NOTICE 'x;y'; END $$",
        "SELECT 'it''s still one; string'",
    ]


def test_quote_table_escapes_identifier_parts():
    assert CoreDBService._quote_table_for("MySQL", "sch.we`ird") == "`sch`.`we``ird`"
    assert CoreDBService._quote_table_for("SQLServer", "dbo.we]ird") == "[dbo].[we]]ird]"
    assert CoreDBService._quote_table_for("PostgreSQL", 'public.we"ird') == '"public"."we""ird"'


def test_get_table_schema_passes_database_for_qualified_mysql_table():
    svc = CoreDBService()
    mgr = MagicMock()
    mgr.db_type = "MariaDB"
    mgr.conn = object()
    schema = [{"name": "PRODUCT_ID", "type": "int", "nullable": False, "default": None}]

    with (
        patch.object(svc, "_get_or_connect", return_value=mgr),
        patch(
            "common.headless.db_service.DatabaseRegistry.supports_operation",
            return_value=True,
        ),
        patch(
            "common.headless.db_service.DatabaseRegistry.execute_operation",
            return_value=schema,
        ) as exec_op,
    ):
        out = svc.get_table_schema("local_mariadb", "test.PRODUCTS")

    assert out["error"] is None
    assert out["columns"] == schema
    exec_op.assert_any_call(
        "MariaDB", "getTableSchema", mgr.conn, "PRODUCTS", database="test"
    )


def test_import_csv_duplicate_header_fails_before_connect(tmp_path, monkeypatch):
    from common import paths as app_paths

    monkeypatch.setattr(app_paths, "exports_dir", lambda: tmp_path)
    csv_file = tmp_path / "dup.csv"
    csv_file.write_text("id,id\n1,2\n", encoding="utf-8")
    svc = CoreDBService()
    with patch.object(svc, "_get_or_connect") as get_conn:
        out = svc.import_csv_to_table("c", str(csv_file), table="t")
    assert out["ok"] is False
    assert "duplicate" in out["message"].lower()
    get_conn.assert_not_called()


def test_import_csv_streams_rows_in_chunks(tmp_path, monkeypatch):
    from common import paths as app_paths

    monkeypatch.setattr(app_paths, "exports_dir", lambda: tmp_path)
    csv_file = tmp_path / "rows.csv"
    csv_file.write_text("id,name\n1,a\n2,b\n3,c\n", encoding="utf-8")
    svc = CoreDBService()
    mgr = MagicMock()
    mgr.db_type = "MySQL"
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    mgr.conn = conn

    with patch.object(svc, "_get_or_connect", return_value=mgr):
        out = svc.import_csv_to_table(
            "c",
            str(csv_file),
            table="target",
            create_table=False,
            chunk_size=2,
        )

    assert out["ok"] is True
    assert out["rows_inserted"] == 3
    assert cur.executemany.call_count == 2
    first_batch = cur.executemany.call_args_list[0].args[1]
    second_batch = cur.executemany.call_args_list[1].args[1]
    assert first_batch == [["1", "a"], ["2", "b"]]
    assert second_batch == [["3", "c"]]
    cur.close.assert_called_once()
    conn.commit.assert_called_once()


def test_show_config_masks_secret_like_keys(monkeypatch):
    svc = CoreDBService()

    class DummyParser:
        def sections(self):
            return ["secrets"]

        def items(self, section):
            assert section == "secrets"
            return [("password", "plain"), ("api_key", "k"), ("safe", "value")]

    class DummyConfig:
        parser = DummyParser()

    monkeypatch.setattr(
        "common.config_loader.get_config",
        lambda: DummyConfig(),
    )
    out = svc.show_config()
    assert out["sections"]["secrets"]["password"] == "***"
    assert out["sections"]["secrets"]["api_key"] == "***"
    assert out["sections"]["secrets"]["safe"] == "value"
