"""Opt-in live SSH tunnel test.

Requires a reachable SSH server on localhost with key auth working
(``ssh localhost true`` must succeed non-interactively) and a TCP service to
forward to. Enable with::

    DBTOOL_TEST_SSH_LOCALHOST=1 \
    DBTOOL_TEST_SSH_USER=$USER \
    DBTOOL_TEST_FWD_PORT=3306 \
    pytest tests/test_ssh_tunnel_live.py -v

Skipped by default so CI / sandboxed runs stay deterministic.
"""

from __future__ import annotations

import os
import subprocess

import pytest

from common.ssh_tunnel import SSHTunnel, _port_is_open

pytestmark = pytest.mark.integration


def _localhost_ssh_ok() -> bool:
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3",
             "localhost", "true"],
            capture_output=True, timeout=8,
        )
        return r.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False


@pytest.mark.skipif(
    os.environ.get("DBTOOL_TEST_SSH_LOCALHOST") != "1",
    reason="set DBTOOL_TEST_SSH_LOCALHOST=1 to run the live tunnel test",
)
def test_live_localhost_tunnel():
    if not _localhost_ssh_ok():
        pytest.skip("passwordless ssh to localhost is not available")
    fwd_port = int(os.environ.get("DBTOOL_TEST_FWD_PORT", "3306"))
    user = os.environ.get("DBTOOL_TEST_SSH_USER", os.environ.get("USER", ""))
    tunnel = SSHTunnel(
        ssh_host="localhost", ssh_user=user,
        remote_host="127.0.0.1", remote_port=fwd_port,
    )
    try:
        tunnel.open()
        assert tunnel.is_open
        assert _port_is_open(tunnel.local_host, tunnel.local_port)
    finally:
        tunnel.close()
    assert tunnel.is_open is False
