"""ConnectionManager persistence for remote (SSH-tunnel) connections."""

from __future__ import annotations

import json

import pytest

from common import paths as _paths
from common.connection_params import ConnectionParams
from common.connection_manager import ConnectionManager, _DB_SENSITIVE_FIELDS


@pytest.fixture
def isolated_cm(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    _paths.bootstrap(force=True)
    return ConnectionManager()


def _tunnel(**over):
    base = {
        "ssh_host": "bastion.example.com",
        "ssh_user": "ubuntu",
        "ssh_port": 2222,
        "ssh_password": "sshsecret",
        "ssh_key_file": "",
    }
    base.update(over)
    return base


def _params(**over):
    base = {
        "name": "r",
        "db_type": "MySQL",
        "host": "localhost",
        "port": "3306",
        "service_or_db": "db",
        "username": "u",
        "password": "pw",
    }
    base.update(over)
    return ConnectionParams.from_mapping(base)


def test_ssh_password_is_a_sensitive_field():
    assert "ssh_password" in _DB_SENSITIVE_FIELDS


def test_add_remote_connection_stores_tunnel(isolated_cm):
    ok, _ = isolated_cm.add_connection(
        _params(name="remote_pg", db_type="PostgreSQL", port="5432",
                service_or_db="appdb", username="app", password="dbpw",
                save_password=True, ssh_tunnel=_tunnel()),
    )
    assert ok
    prof = isolated_cm.get_connection("remote_pg")
    tun = prof["ssh_tunnel"]
    assert tun["ssh_host"] == "bastion.example.com"
    assert tun["ssh_user"] == "ubuntu"
    assert tun["ssh_port"] == 2222
    assert tun["ssh_password"] == "sshsecret"


def test_ssh_password_encrypted_on_disk(isolated_cm):
    isolated_cm.add_connection(
        _params(name="r", password="dbpw", save_password=True,
                ssh_tunnel=_tunnel()),
    )
    raw = json.loads(_paths.db_connections_path().read_text())
    stored = raw[0]["ssh_tunnel"]["ssh_password"]
    assert stored != "sshsecret"  # encrypted at rest
    # and round-trips back to plaintext on load
    reloaded = ConnectionManager()
    assert reloaded.get_connection("r")["ssh_tunnel"]["ssh_password"] == "sshsecret"


def test_save_password_false_drops_ssh_password(isolated_cm):
    isolated_cm.add_connection(
        _params(name="r2", password="dbpw", save_password=False,
                ssh_tunnel=_tunnel()),
    )
    tun = isolated_cm.get_connection("r2")["ssh_tunnel"]
    assert "ssh_password" not in tun
    assert tun["ssh_host"] == "bastion.example.com"


def test_key_file_auth_persists(isolated_cm):
    isolated_cm.add_connection(
        _params(name="r3", db_type="PostgreSQL", host="10.0.0.5", port="5432",
                password="dbpw", save_password=True,
                ssh_tunnel=_tunnel(ssh_password="", ssh_key_file="/home/me/.ssh/id_rsa")),
    )
    tun = isolated_cm.get_connection("r3")["ssh_tunnel"]
    assert tun["ssh_key_file"] == "/home/me/.ssh/id_rsa"
    assert "ssh_password" not in tun


def test_no_tunnel_block_when_no_ssh_host(isolated_cm):
    isolated_cm.add_connection(
        _params(name="plain", save_password=True, ssh_tunnel={"ssh_user": "x"}),
    )
    assert "ssh_tunnel" not in isolated_cm.get_connection("plain")


def test_update_connection_preserves_tunnel(isolated_cm):
    isolated_cm.add_connection(
        _params(name="r4", save_password=True, ssh_tunnel=_tunnel()),
    )
    ok, _ = isolated_cm.update_connection(
        "r4",
        _params(name="r4", port="3307", service_or_db="db2",
                save_password=True, ssh_tunnel=_tunnel(ssh_host="new.bastion")),
    )
    assert ok
    prof = isolated_cm.get_connection("r4")
    assert prof["port"] == "3307"
    assert prof["ssh_tunnel"]["ssh_host"] == "new.bastion"
