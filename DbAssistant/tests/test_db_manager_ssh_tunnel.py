"""DatabaseManager SSH-tunnel wiring (tunnel + driver mocked)."""

from __future__ import annotations

import types

import pytest

from common.db_manager import DatabaseManager
from common.database_registry import DatabaseRegistry


class _FakeTunnel:
    def __init__(self):
        self.local_host = "127.0.0.1"
        self.local_port = 55432
        self.is_open = True
        self.closed = False

    def close(self):
        self.closed = True
        self.is_open = False


@pytest.fixture
def captured(monkeypatch):
    """Capture the host/port the driver connect func receives."""
    seen = {}

    def _fake_connect(**kwargs):
        seen.clear()
        seen.update(kwargs)
        return types.SimpleNamespace(name="fake-conn")

    def _fake_disconnect(conn):
        seen["disconnected"] = True

    def _get_operation(db_type, op):
        if op == "connect":
            return _fake_connect
        if op == "disconnect":
            return _fake_disconnect
        return None

    monkeypatch.setattr(DatabaseRegistry, "get_operation", staticmethod(_get_operation))
    return seen


def test_connect_without_tunnel_uses_original_host(captured, monkeypatch):
    mgr = DatabaseManager("PostgreSQL")
    mgr.connect(host="db.example.com", port=5432, username="u", password="p", database="d")
    assert captured["host"] == "db.example.com"
    assert captured["port"] == 5432
    assert mgr._ssh_tunnel is None


def test_connect_with_tunnel_rewrites_host_port(captured, monkeypatch):
    fake = _FakeTunnel()
    monkeypatch.setattr(
        "common.ssh_tunnel.open_tunnel_from_config",
        lambda cfg, host, port: fake,
    )
    mgr = DatabaseManager("PostgreSQL")
    mgr.connect(
        host="db.internal", port=5432, username="u", password="p", database="d",
        ssh_tunnel={"ssh_host": "bastion", "ssh_user": "me"},
    )
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 55432
    assert mgr._ssh_tunnel is fake


def test_disconnect_closes_tunnel(captured, monkeypatch):
    fake = _FakeTunnel()
    monkeypatch.setattr(
        "common.ssh_tunnel.open_tunnel_from_config",
        lambda cfg, host, port: fake,
    )
    mgr = DatabaseManager("MySQL")
    mgr.connect(
        host="h", port=3306, username="u", password="p", database="d",
        ssh_tunnel={"ssh_host": "bastion", "ssh_user": "me"},
    )
    mgr.disconnect()
    assert fake.closed is True
    assert mgr._ssh_tunnel is None


def test_open_tunnel_reused_on_second_connect(captured, monkeypatch):
    fake = _FakeTunnel()
    calls = {"n": 0}

    def _open(cfg, host, port):
        calls["n"] += 1
        return fake

    monkeypatch.setattr("common.ssh_tunnel.open_tunnel_from_config", _open)
    mgr = DatabaseManager("MySQL")
    cfg = {"ssh_host": "bastion", "ssh_user": "me"}
    mgr.connect(host="h", port=3306, username="u", password="p", database="d", ssh_tunnel=cfg)
    # simulate a reconnect through the same manager
    mgr._close_db_handle()
    mgr.connect(host="h", port=3306, username="u", password="p", database="d", ssh_tunnel=cfg)
    assert calls["n"] == 1  # tunnel opened once, reused on reconnect
    assert captured["port"] == 55432
