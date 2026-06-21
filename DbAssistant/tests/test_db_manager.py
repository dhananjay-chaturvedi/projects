"""DatabaseManager unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from common.db_manager import DatabaseManager


@pytest.fixture
def mysql_mgr():
    return DatabaseManager("MySQL")


def test_ping_or_reconnect_ping_ok(mysql_mgr):
    mysql_mgr.conn = object()
    with patch("common.db_manager.DatabaseRegistry.get_operation") as get_op:
        get_op.return_value = lambda c: True
        assert mysql_mgr.ping_or_reconnect() is True


def test_ping_or_reconnect_reconnect_path(mysql_mgr):
    mysql_mgr._last_connect_params = {
        "database": "test",
        "host": "localhost",
        "username": "u",
        "password": "p",
        "port": 3306,
    }
    mysql_mgr.conn = object()

    ping = MagicMock(return_value=False)
    reconnect = MagicMock(return_value=object())

    def get_op(db_type, op):
        if op == "ping":
            return ping
        if op == "reconnect":
            return reconnect
        return None

    with patch("common.db_manager.DatabaseRegistry.get_operation", side_effect=get_op):
        assert mysql_mgr.ping_or_reconnect() is True
    reconnect.assert_called_once()


def test_reconnect_without_saved_params(mysql_mgr):
    mysql_mgr.conn = object()
    with patch("common.db_manager.DatabaseRegistry.get_operation") as get_op:
        get_op.return_value = lambda c: False
        assert mysql_mgr.ping_or_reconnect() is False


def test_execute_query_not_connected(mysql_mgr):
    result, err = mysql_mgr.execute_query("SELECT 1")
    assert result is None
    assert "Not connected" in err


def test_execute_query_single_select(mysql_mgr):
    cur = MagicMock()
    cur.description = [("x",)]
    cur.fetchmany.return_value = [(1,)]
    cur.fetchall.return_value = [(99,)]
    cur.rowcount = 1
    mysql_mgr.conn = MagicMock()
    mysql_mgr.conn.cursor.return_value = cur
    result, err = mysql_mgr.execute_query("SELECT 1")
    assert err is None
    assert result["columns"] == ["x"]
    assert result["rows"] == [(1,)]
    assert result["truncated"] is False
    cur.close.assert_called_once()


def test_execute_query_rejects_empty_sql(mysql_mgr):
    mysql_mgr.conn = MagicMock()
    result, err = mysql_mgr.execute_query("   ")
    assert result is None
    assert "empty" in err.lower()
    mysql_mgr.conn.cursor.assert_not_called()


def test_execute_query_rejects_non_string_sql(mysql_mgr):
    mysql_mgr.conn = MagicMock()
    result, err = mysql_mgr.execute_query(None)
    assert result is None
    assert "string" in err.lower()
    mysql_mgr.conn.cursor.assert_not_called()


def test_execute_query_comment_only(mysql_mgr):
    mysql_mgr.conn = MagicMock()
    result, err = mysql_mgr.execute_query("-- only a comment")
    assert err is None
    assert "Comment-only" in result["message"]
    mysql_mgr.conn.cursor.assert_not_called()


def test_split_sql_preserves_semicolon_inside_strings(mysql_mgr):
    sql = "SELECT 'a;b'; SELECT 'it''s ok; still string';"
    assert mysql_mgr._split_sql_statements(sql) == [
        "SELECT 'a;b'",
        "SELECT 'it''s ok; still string'",
    ]


def test_split_sql_handles_hash_and_dash_comments(mysql_mgr):
    sql = "SELECT 1; # comment; ignored\nSELECT 2; -- another; ignored\nSELECT 3"
    assert mysql_mgr._split_sql_statements(sql) == [
        "SELECT 1",
        "# comment; ignored\nSELECT 2",
        "-- another; ignored\nSELECT 3",
    ]


def test_split_sql_keeps_postgres_dollar_quote_body(mysql_mgr):
    sql = "DO $$ BEGIN RAISE NOTICE 'a;b'; END $$; SELECT 1;"
    assert mysql_mgr._split_sql_statements(sql) == [
        "DO $$ BEGIN RAISE NOTICE 'a;b'; END $$",
        "SELECT 1",
    ]


def test_split_sql_keeps_procedural_block_as_single_statement(mysql_mgr):
    mysql_mgr.db_type = "Oracle"
    sql = "BEGIN\n  x := 1;\n  y := 2;\nEND;"
    assert mysql_mgr._split_sql_statements(sql) == ["BEGIN\n  x := 1;\n  y := 2;\nEND"]


def test_strip_comments_preserves_comment_like_text_in_string(mysql_mgr):
    sql = "SELECT '--not comment', '/*not comment*/', 1 -- real comment"
    stripped = mysql_mgr._strip_sql_comments(sql)
    assert "'--not comment'" in stripped
    assert "'/*not comment*/'" in stripped
    assert "real comment" not in stripped


def test_execute_query_caps_rows_to_avoid_unbounded_memory(mysql_mgr):
    cur = MagicMock()
    cur.description = [("x",)]
    cur.fetchmany.return_value = [(1,), (2,), (3,)]
    mysql_mgr.conn = MagicMock()
    mysql_mgr.conn.cursor.return_value = cur

    with patch("common.db_manager.properties.get_int", return_value=2):
        result, err = mysql_mgr.execute_query("SELECT * FROM huge_table")

    assert err is None
    assert result["rows"] == [(1,), (2,)]
    assert result["rowcount"] == 2
    assert result["truncated"] is True
    assert result["max_rows"] == 2
    cur.fetchmany.assert_called_once_with(3)
    cur.close.assert_called_once()


def test_execute_query_unlimited_fetch_uses_fetchall(mysql_mgr):
    cur = MagicMock()
    cur.description = [("x",)]
    cur.fetchall.return_value = [(1,), (2,), (3,)]
    mysql_mgr.conn = MagicMock()
    mysql_mgr.conn.cursor.return_value = cur

    with patch("common.db_manager.properties.get_int", return_value=0):
        result, err = mysql_mgr.execute_query("SELECT * FROM t")

    assert err is None
    assert result["rows"] == [(1,), (2,), (3,)]
    assert result["truncated"] is False
    cur.fetchall.assert_called_once()
    cur.close.assert_called_once()


def test_execute_query_closes_cursor_on_execute_error(mysql_mgr):
    cur = MagicMock()
    cur.execute.side_effect = RuntimeError("boom")
    mysql_mgr.conn = MagicMock()
    mysql_mgr.conn.cursor.return_value = cur

    result, err = mysql_mgr.execute_query("SELECT broken")

    assert result is None
    assert "boom" in err
    cur.close.assert_called_once()


def test_disconnect_always_drops_handle(mysql_mgr):
    conn = object()
    mysql_mgr.conn = conn
    with patch("common.db_manager.DatabaseRegistry.get_operation", return_value=MagicMock(side_effect=RuntimeError("close failed"))):
        with pytest.raises(RuntimeError):
            mysql_mgr.disconnect()
    assert mysql_mgr.conn is None


def test_mysql_cancel_closes_both_cursors(mysql_mgr):
    id_cursor = MagicMock()
    id_cursor.fetchone.return_value = (123,)
    kill_cursor = MagicMock()
    mysql_mgr.conn = MagicMock()
    mysql_mgr.conn.cursor.side_effect = [id_cursor, kill_cursor]

    assert mysql_mgr.cancel_query() is True
    id_cursor.close.assert_called_once()
    kill_cursor.close.assert_called_once()


def test_postgres_cancel_closes_cursor_on_error():
    mgr = DatabaseManager("PostgreSQL")
    pid_cursor = MagicMock()
    pid_cursor.fetchone.return_value = ("bad-pid",)
    mgr.conn = MagicMock()
    mgr.conn.cursor.return_value = pid_cursor

    assert mgr.cancel_query() is False
    pid_cursor.close.assert_called_once()
