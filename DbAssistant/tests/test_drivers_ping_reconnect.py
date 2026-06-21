"""Per-driver ping/reconnect unit tests."""

from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

import importlib.util

from common.drivers import conMysql, conMariadb, conPostgres, conSQLite

if not (importlib.util.find_spec("oracledb") or importlib.util.find_spec("cx_Oracle")):
    pytest.skip("Oracle driver not installed (pip install oracledb)", allow_module_level=True)

from common.drivers import conOracle  # noqa: E402


class DummyMysqlConn:
    def __init__(self, connected=True):
        self._connected = connected

    def is_connected(self):
        return self._connected

    def ping(self, reconnect=False, attempts=1, delay=0):
        if not self._connected:
            raise Exception("down")

    def close(self):
        self._connected = False


def test_ping_mysql_alive():
    assert conMysql.pingMysql(DummyMysqlConn(True)) is True


def test_ping_mysql_dead():
    assert conMysql.pingMysql(DummyMysqlConn(False)) is False


def test_ping_mariadb_alive():
    conn = DummyMysqlConn(True)
    assert conMariadb.pingMariadb(conn) is True


def test_ping_postgres_select1():
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.closed = 0
    conn.cursor.return_value = cur
    assert conPostgres.pingPostgres(conn) is True
    cur.execute.assert_called_with("SELECT 1")
    conn.rollback.assert_not_called()


def test_ping_postgres_closed():
    conn = MagicMock()
    conn.closed = 1
    assert conPostgres.pingPostgres(conn) is False


def test_ping_oracle_delegates():
    conn = MagicMock()
    with patch.object(conOracle, "validate_connection", return_value=True):
        assert conOracle.pingOracle(conn) is True


def test_ping_sqlite():
    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    assert conSQLite.pingSQLite(conn) is True


def test_reconnect_mysql(monkeypatch):
    import common.drivers.conMysql as mysql_mod

    calls = []

    def fake_disconnect(c):
        calls.append("disconnect")

    def fake_connect(**kw):
        calls.append("connect")
        return DummyMysqlConn(True)

    monkeypatch.setattr(mysql_mod, "disconnectMysql", fake_disconnect)
    monkeypatch.setattr(mysql_mod, "connectMysql", fake_connect)
    out = mysql_mod.reconnectMysql(
        DummyMysqlConn(False),
        database="db",
        host="h",
        user="u",
        password="p",
    )
    assert out is not None
    assert "disconnect" in calls and "connect" in calls


def test_database_registry_unknown_ping():
    from common.database_registry import DatabaseRegistry

    assert DatabaseRegistry.get_operation("NoSuchDB", "ping") is None
