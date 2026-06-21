"""Unit tests for SQL Server driver (pymssql)."""

from __future__ import annotations

import importlib.util
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.skipif(
    importlib.util.find_spec("pymssql") is None,
    reason="pymssql not installed",
)
def test_con_sqlserver_module_imports():
    from common.drivers import conSqlServer

    assert conSqlServer.connectSqlServer is not None


def test_ping_sqlserver_select1():
    from common.drivers import conSqlServer

    cur = MagicMock()
    cur.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.cursor.return_value = cur
    assert conSqlServer.pingSqlServer(conn) is True
    cur.execute.assert_called_with("SELECT 1")


def test_ping_sqlserver_dead():
    from common.drivers import conSqlServer

    conn = MagicMock()
    conn.cursor.side_effect = Exception("down")
    assert conSqlServer.pingSqlServer(conn) is False


def test_get_sqlserver_tables():
    from common.drivers import conSqlServer

    cur = MagicMock()
    cur.fetchall.return_value = [("dbo.users",), ("dbo.orders",)]
    conn = MagicMock()
    conn.cursor.return_value = cur
    tables = conSqlServer.getSqlServerTables(conn)
    assert tables == ["dbo.users", "dbo.orders"]


def test_connect_sqlserver_without_pymssql():
    from common.drivers import conSqlServer

    with patch.object(conSqlServer, "pymssql", None):
        assert conSqlServer.connectSqlServer(
            database="db", host="host", user="u", password="p"
        ) is None
