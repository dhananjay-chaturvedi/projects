import pytest
import types

from common.drivers import conMysql


class DummyCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def execute(self, query, params=None):
        self._last_query = query

    def fetchone(self):
        return self._rows[0] if self._rows else (None,)

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class DummyConn:
    def __init__(self, rows=None, connected=True):
        self._rows = rows or []
        self._connected = connected

    def is_connected(self):
        return self._connected

    def cursor(self, buffered=True):
        return DummyCursor(rows=self._rows)

    def ping(self, reconnect=False, attempts=1, delay=0):
        if not self._connected:
            raise Exception("not connected")

    def close(self):
        self._connected = False


def test_connectMysql_success(monkeypatch):
    # Simulate mysql.connector.connect returning a connected object
    dummy_conn = DummyConn(connected=True)

    class DummyModule:
        class Error(conMysql.Error):
            pass

        @staticmethod
        def connect(**kwargs):
            return dummy_conn

    monkeypatch.setattr(conMysql.mysql.connector, "connect", DummyModule.connect)

    conn = conMysql.connectMysql(
        database="db", host="host", user="user", password="pw", port=3306
    )
    assert conn is dummy_conn


def test_connectMysql_failure(monkeypatch):
    # Simulate mysql.connector.connect raising Error
    class DummyModule:
        class Error(conMysql.Error):
            pass

        @staticmethod
        def connect(**kwargs):
            raise DummyModule.Error("failed")

    monkeypatch.setattr(conMysql.mysql.connector, "connect", DummyModule.connect)

    conn = conMysql.connectMysql(
        database="db", host="host", user="user", password="pw", port=3306
    )
    assert conn is None


def test_getMysqlTables_with_db(monkeypatch):
    rows = [(b"table1",), (b"table2",)]
    dummy_conn = DummyConn(rows=rows, connected=True)
    tables = conMysql.getMysqlTables(dummy_conn, database="mydb")
    assert "table1" in tables and "table2" in tables


def test_getMysqlTables_no_db(monkeypatch):
    # When no current database, simulate information_schema result
    rows = [(b"table_a",), (b"table_b",)]
    dummy_conn = DummyConn(rows=rows, connected=True)
    # Monkeypatch cursor behavior indirectly by creating DummyConn with rows
    tables = conMysql.getMysqlTables(dummy_conn, database=None)
    assert isinstance(tables, list)
