"""Unit tests for common.ssh_tunnel (no real SSH; subprocess/socket mocked)."""

from __future__ import annotations

import subprocess
import types

import pytest

from common import ssh_tunnel as st
from common.ssh_tunnel import (
    SSHTunnel,
    SSHTunnelError,
    normalize_tunnel_config,
    open_tunnel_from_config,
)


# --------------------------------------------------------------------------
# normalize_tunnel_config
# --------------------------------------------------------------------------

def test_normalize_returns_none_without_host():
    assert normalize_tunnel_config(None) is None
    assert normalize_tunnel_config({}) is None
    assert normalize_tunnel_config({"ssh_user": "x"}) is None
    assert normalize_tunnel_config({"ssh_host": "   "}) is None


def test_normalize_cleans_and_keeps_defined_fields():
    out = normalize_tunnel_config(
        {
            "ssh_host": " bastion ",
            "ssh_user": " me ",
            "ssh_port": "2222",
            "ssh_password": "pw",
            "ssh_key_file": "",
        }
    )
    assert out == {
        "ssh_host": "bastion",
        "ssh_user": "me",
        "ssh_port": 2222,
        "ssh_password": "pw",
    }


def test_normalize_defaults_port_22():
    out = normalize_tunnel_config({"ssh_host": "h", "ssh_user": "u"})
    assert out["ssh_port"] == 22


# --------------------------------------------------------------------------
# SSHTunnel construction + command building
# --------------------------------------------------------------------------

def _tunnel(**over):
    kw = dict(ssh_host="bastion", ssh_user="me", remote_host="db.internal", remote_port=5432)
    kw.update(over)
    return SSHTunnel(**kw)


def test_constructor_requires_host_and_user():
    with pytest.raises(SSHTunnelError):
        SSHTunnel(ssh_host="", ssh_user="me", remote_host="h", remote_port=1)
    with pytest.raises(SSHTunnelError):
        SSHTunnel(ssh_host="h", ssh_user="", remote_host="h", remote_port=1)


def test_build_command_key_auth(monkeypatch, tmp_path):
    key = tmp_path / "id_rsa"
    key.write_text("KEY")
    t = _tunnel(ssh_key_file=str(key))
    t.local_port = 15432
    cmd = t._build_command()
    assert cmd[0] == "ssh"
    assert "-L" in cmd
    fwd_idx = cmd.index("-L") + 1
    assert cmd[fwd_idx] == "127.0.0.1:15432:db.internal:5432"
    assert "BatchMode=yes" in cmd
    assert "-i" in cmd and str(key) in cmd
    assert cmd[-1] == "me@bastion"


def test_build_command_password_requires_sshpass(monkeypatch):
    monkeypatch.setattr(st.shutil, "which", lambda _x: None)
    t = _tunnel(ssh_password="secret")
    t.local_port = 10000
    with pytest.raises(SSHTunnelError):
        t._build_command()


def test_build_command_password_uses_sshpass(monkeypatch):
    monkeypatch.setattr(st.shutil, "which", lambda _x: "/usr/bin/sshpass")
    t = _tunnel(ssh_password="secret")
    t.local_port = 10001
    cmd = t._build_command()
    assert cmd[:3] == ["sshpass", "-p", "secret"]
    assert "BatchMode=yes" not in cmd


def test_build_command_missing_key_file_raises(tmp_path):
    t = _tunnel(ssh_key_file=str(tmp_path / "nope"))
    t.local_port = 10002
    with pytest.raises(SSHTunnelError):
        t._build_command()


# --------------------------------------------------------------------------
# open() / close()
# --------------------------------------------------------------------------

def _fake_run(returncode=0, stderr=""):
    def _run(cmd, **kw):
        return types.SimpleNamespace(returncode=returncode, stdout="", stderr=stderr)
    return _run


def test_open_success(monkeypatch, tmp_path):
    key = tmp_path / "k"
    key.write_text("k")
    monkeypatch.setattr(st.shutil, "which", lambda _x: "/usr/bin/ssh")
    monkeypatch.setattr(st.subprocess, "run", _fake_run(0))
    monkeypatch.setattr(st, "_free_local_port", lambda: 19999)
    monkeypatch.setattr(st, "_wait_for_port", lambda *a, **k: True)
    t = _tunnel(ssh_key_file=str(key))
    t.open()
    assert t.is_open is True
    assert t.local_port == 19999
    assert t.local_host == "127.0.0.1"


def test_open_ssh_failure_raises(monkeypatch, tmp_path):
    key = tmp_path / "k"
    key.write_text("k")
    monkeypatch.setattr(st.shutil, "which", lambda _x: "/usr/bin/ssh")
    monkeypatch.setattr(st.subprocess, "run", _fake_run(255, "permission denied"))
    monkeypatch.setattr(st, "_free_local_port", lambda: 20000)
    t = _tunnel(ssh_key_file=str(key))
    with pytest.raises(SSHTunnelError) as exc:
        t.open()
    assert "permission denied" in str(exc.value)
    assert t.is_open is False


def test_open_forward_never_ready_closes_and_raises(monkeypatch, tmp_path):
    key = tmp_path / "k"
    key.write_text("k")
    monkeypatch.setattr(st.shutil, "which", lambda _x: "/usr/bin/ssh")
    monkeypatch.setattr(st.subprocess, "run", _fake_run(0))
    monkeypatch.setattr(st, "_free_local_port", lambda: 20001)
    monkeypatch.setattr(st, "_wait_for_port", lambda *a, **k: False)
    closed = {"v": False}
    t = _tunnel(ssh_key_file=str(key))
    monkeypatch.setattr(t, "close", lambda: closed.__setitem__("v", True))
    with pytest.raises(SSHTunnelError):
        t.open()
    assert closed["v"] is True


def test_open_timeout_raises(monkeypatch, tmp_path):
    key = tmp_path / "k"
    key.write_text("k")
    monkeypatch.setattr(st.shutil, "which", lambda _x: "/usr/bin/ssh")

    def _boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="ssh", timeout=1)

    monkeypatch.setattr(st.subprocess, "run", _boom)
    monkeypatch.setattr(st, "_free_local_port", lambda: 20002)
    t = _tunnel(ssh_key_file=str(key))
    with pytest.raises(SSHTunnelError):
        t.open()


def test_open_tunnel_from_config_requires_host():
    with pytest.raises(SSHTunnelError):
        open_tunnel_from_config({}, "h", 1)
