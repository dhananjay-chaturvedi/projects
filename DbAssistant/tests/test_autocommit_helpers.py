from __future__ import annotations

from unittest.mock import MagicMock

from common.autocommit import get_autocommit, set_autocommit
from common.headless.db_service import CoreDBService


class _PostgresConn:
    def __init__(self):
        self.autocommit = False
        self.rollback_count = 0

    def rollback(self):
        self.rollback_count += 1


class _SqlServerConn:
    def __init__(self):
        self._state = False

    @property
    def autocommit_state(self):
        return self._state

    def autocommit(self, status):
        self._state = bool(status)


class _SQLiteConn:
    def __init__(self):
        self.isolation_level = "DEFERRED"


def test_postgres_enabling_autocommit_rolls_back_open_transaction_first():
    conn = _PostgresConn()

    set_autocommit(conn, "PostgreSQL", True)

    assert conn.rollback_count == 1
    assert conn.autocommit is True
    assert get_autocommit(conn, "PostgreSQL") is True


def test_sqlserver_autocommit_uses_method_and_readable_state():
    conn = _SqlServerConn()

    set_autocommit(conn, "SQLServer", True)

    assert callable(conn.autocommit)
    assert get_autocommit(conn, "SQLServer") is True


def test_sqlite_autocommit_maps_to_isolation_level():
    conn = _SQLiteConn()

    set_autocommit(conn, "SQLite", True)
    assert conn.isolation_level is None
    assert get_autocommit(conn, "SQLite") is True

    set_autocommit(conn, "SQLite", False)
    assert conn.isolation_level == "DEFERRED"
    assert get_autocommit(conn, "SQLite") is False


def test_core_service_autocommit_preserves_sqlserver_method_api():
    svc = CoreDBService(connection_manager=MagicMock())
    mgr = MagicMock()
    mgr.db_type = "SQLServer"
    mgr.conn = _SqlServerConn()
    mgr.capabilities.supports_transactions = True
    svc._active["sqlsrv"] = mgr

    out = svc.set_autocommit("sqlsrv", True)
    readback = svc.get_autocommit("sqlsrv")

    assert out["ok"] is True
    assert readback == {"ok": True, "autocommit": True, "message": ""}
    assert callable(mgr.conn.autocommit)
