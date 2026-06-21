"""CloudConnectionManager persistence tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.cloud.connection_manager import CloudConnectionManager, _SENSITIVE_FIELDS


@pytest.fixture
def isolated_cloud_mgr(tmp_path, monkeypatch):
    cfg = tmp_path / "config"
    cfg.mkdir()
    monkeypatch.chdir(tmp_path)
    # Point config at tmp via writing config.ini
    ini = tmp_path / "config.ini"
    ini.write_text(
        f"""
[paths]
config_dir = {cfg}
cloud_connections_file = cloud_connections.json
db_key_file = .db_key
"""
    )
    monkeypatch.setenv("DBTOOL_INI", str(ini))
    # Reload config is heavy; construct manager with patched paths instead
    mgr = CloudConnectionManager.__new__(CloudConnectionManager)
    mgr.config_dir = cfg
    mgr.config_file = cfg / "cloud_connections.json"
    mgr.key_file = cfg / ".db_key"
    mgr.cipher = CloudConnectionManager._init_cipher(mgr)
    return mgr


def test_sensitive_fields_include_gcp_oauth():
    assert "sa_key_json" in _SENSITIVE_FIELDS
    assert "oauth_refresh_token" in _SENSITIVE_FIELDS


def test_encrypt_decrypt_roundtrip(isolated_cloud_mgr):
    enc = isolated_cloud_mgr._encrypt("secret-value")
    assert enc
    assert isolated_cloud_mgr._decrypt(enc) == "secret-value"


def test_save_load_roundtrip(isolated_cloud_mgr):
    data = {
        "prod": {
            "provider": "AWS",
            "secret_access_key": "sk",
            "access_key_id": "ak",
        }
    }
    assert isolated_cloud_mgr.save_cloud_databases(data) is True
    loaded = isolated_cloud_mgr.load_cloud_databases()
    assert loaded["prod"]["secret_access_key"] == "sk"


def test_corrupt_json_returns_empty(isolated_cloud_mgr):
    isolated_cloud_mgr.config_file.write_text("{not json")
    assert isolated_cloud_mgr.load_cloud_databases() == {}
