"""Monitor connection manager tests (parity with cloud manager)."""

from __future__ import annotations

import pytest

from monitoring.monitor_connection_manager import MonitorConnectionManager


@pytest.fixture
def isolated_monitor_mgr(tmp_path):
    cfg = tmp_path / "config"
    cfg.mkdir()
    mgr = MonitorConnectionManager.__new__(MonitorConnectionManager)
    mgr.config_dir = cfg
    mgr.config_file = cfg / "monitor_connections.json"
    mgr.key_file = cfg / ".monitor_key"
    mgr.cipher = MonitorConnectionManager._init_cipher(mgr)
    return mgr


def test_encrypt_decrypt(isolated_monitor_mgr):
    enc = isolated_monitor_mgr._encrypt_password("pw")
    assert isolated_monitor_mgr._decrypt_password(enc) == "pw"


def test_save_load(isolated_monitor_mgr):
    isolated_monitor_mgr.connections = [
        {"name": "srv1", "host": "h", "username": "u", "password": "p"}
    ]
    assert isolated_monitor_mgr.save_connections() is True
    loaded = isolated_monitor_mgr.load_connections()
    assert loaded[0]["password"] == "p"
