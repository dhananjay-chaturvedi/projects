# ---------------------------------------------------------------------
# description: SSH local port-forward tunnels for remote DB connections
# ---------------------------------------------------------------------

"""SSH tunnels (local port forwarding) for reaching databases that are only
accessible through a bastion / jump host.

Design
------
* Uses the **system OpenSSH client** via ``subprocess`` — the same approach the
  Monitoring module already uses — so there is **no new Python dependency**.
* Password auth requires ``sshpass`` on ``PATH`` (already documented for the
  Monitoring module). Key-file auth needs nothing extra.
* A tunnel forwards ``127.0.0.1:<local_port>`` to ``<remote_host>:<remote_port>``
  *as seen from the SSH server*. The DB driver then connects to the local end.
* Tunnels use an OpenSSH ControlMaster socket so teardown is a clean
  ``ssh -O exit`` and the forward dies with it.

The connection-profile ``ssh_tunnel`` block looks like::

    {
        "ssh_host": "bastion.example.com",
        "ssh_port": 22,                 # optional, default 22
        "ssh_user": "ubuntu",
        "ssh_password": "",             # optional (password auth, needs sshpass)
        "ssh_key_file": "/path/key.pem" # optional (key auth)
    }
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from common.config_loader import config, console_debug, console_print


class SSHTunnelError(RuntimeError):
    """Raised when an SSH tunnel cannot be established or torn down."""


# Hard floor / ceiling so misconfigured values can't wedge a connect attempt.
_MIN_CONNECT_TIMEOUT = 3
_DEFAULT_CONTROL_PERSIST = 3600  # seconds the master socket lingers when idle
_PORT_WAIT_TIMEOUT = 12.0        # seconds to wait for the local forward to open


@dataclass(frozen=True)
class SSHTunnelConfig:
    """Configuration for an OpenSSH local-port-forward tunnel."""

    ssh_host: str
    ssh_user: str
    remote_host: str
    remote_port: int
    ssh_port: int = 22
    ssh_password: str = ""
    ssh_key_file: str = ""
    local_port: Optional[int] = None
    connect_timeout: Optional[int] = None
    control_persist: Optional[int] = None

    @classmethod
    def from_call(
        cls,
        first: "SSHTunnelConfig | str | None",
        legacy_args: tuple,
        values: dict,
    ) -> "SSHTunnelConfig":
        """Coerce config-object or legacy constructor calls."""
        if isinstance(first, cls):
            src = {**first.__dict__, **values}
        else:
            src = dict(values)
            names = ("ssh_host", "ssh_user", "remote_host", "remote_port")
            positional = legacy_args if first is None else (first, *legacy_args)
            for name, value in zip(names, positional):
                src.setdefault(name, value)
        return cls(
            ssh_host=src.get("ssh_host", ""),
            ssh_user=src.get("ssh_user", ""),
            remote_host=src.get("remote_host", ""),
            remote_port=int(src.get("remote_port")),
            ssh_port=int(src.get("ssh_port", 22) or 22),
            ssh_password=src.get("ssh_password", "") or "",
            ssh_key_file=src.get("ssh_key_file", "") or "",
            local_port=src.get("local_port"),
            connect_timeout=src.get("connect_timeout"),
            control_persist=src.get("control_persist"),
        )


def _free_local_port() -> int:
    """Return an unused TCP port on the loopback interface.

    Binding to port 0 lets the OS choose; we immediately release it and hand
    the number to ssh. There's a tiny race window, but OpenSSH will fail fast
    (``ExitOnForwardFailure``) and the caller surfaces that error.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        return sock.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        sock.close()


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _port_is_open(host, port):
            return True
        time.sleep(0.2)
    return False


class SSHTunnel:
    """An OpenSSH local-port-forward tunnel managed as a context object."""

    def __init__(self, tunnel: SSHTunnelConfig | str | None = None, *legacy_args, **kwargs) -> None:
        tunnel = SSHTunnelConfig.from_call(tunnel, legacy_args, kwargs)
        ssh_host = tunnel.ssh_host
        ssh_user = tunnel.ssh_user
        remote_host = tunnel.remote_host
        remote_port = tunnel.remote_port
        ssh_port = tunnel.ssh_port
        ssh_password = tunnel.ssh_password
        ssh_key_file = tunnel.ssh_key_file
        local_port = tunnel.local_port
        connect_timeout = tunnel.connect_timeout
        control_persist = tunnel.control_persist
        if not ssh_host:
            raise SSHTunnelError("SSH host is required for a remote connection.")
        if not ssh_user:
            raise SSHTunnelError("SSH username is required for a remote connection.")

        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_port = int(ssh_port or 22)
        self.ssh_password = ssh_password or ""
        self.ssh_key_file = (ssh_key_file or "").strip()
        self.remote_host = remote_host or "127.0.0.1"
        self.remote_port = int(remote_port)

        self.local_host = "127.0.0.1"
        self.local_port = local_port

        default_timeout = int(
            config.get_float("database.connection", "connection_timeout", default=30.0)
        )
        self.connect_timeout = max(
            _MIN_CONNECT_TIMEOUT, int(connect_timeout or default_timeout)
        )
        self.control_persist = int(control_persist or _DEFAULT_CONTROL_PERSIST)
        self.control_path = os.path.join(
            tempfile.gettempdir(), f"dbassistant_tunnel_{uuid.uuid4().hex[:12]}"
        )
        self._open = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        return self._open

    def _ssh_target(self) -> str:
        return f"{self.ssh_user}@{self.ssh_host}"

    def _build_command(self) -> list[str]:
        """Construct the OpenSSH master command for this tunnel."""
        forward = (
            f"{self.local_host}:{self.local_port}:"
            f"{self.remote_host}:{self.remote_port}"
        )
        base = [
            "ssh",
            "-p", str(self.ssh_port),
            "-M", "-N", "-f",
            "-L", forward,
            "-o", "ControlMaster=yes",
            "-o", f"ControlPath={self.control_path}",
            "-o", f"ControlPersist={self.control_persist}",
            "-o", "ExitOnForwardFailure=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"ConnectTimeout={self.connect_timeout}",
        ]

        if self.ssh_password:
            if not shutil.which("sshpass"):
                raise SSHTunnelError(
                    "Password authentication needs 'sshpass' on PATH.\n"
                    "Install it (macOS: brew install sshpass, "
                    "Linux: apt-get install sshpass) or use an SSH key file."
                )
            cmd = ["sshpass", "-p", self.ssh_password] + base
        else:
            # No password supplied → never block on an interactive prompt.
            base += ["-o", "BatchMode=yes"]
            if self.ssh_key_file:
                if not os.path.isfile(os.path.expanduser(self.ssh_key_file)):
                    raise SSHTunnelError(
                        f"SSH key file not found: {self.ssh_key_file}"
                    )
                base += [
                    "-o", "IdentitiesOnly=yes",
                    "-i", os.path.expanduser(self.ssh_key_file),
                ]
            cmd = base

        cmd.append(self._ssh_target())
        return cmd

    def open(self) -> "SSHTunnel":
        """Establish the tunnel; returns self so callers can chain."""
        if self._open:
            return self
        if not shutil.which("ssh"):
            raise SSHTunnelError(
                "OpenSSH client ('ssh') not found on PATH; cannot open a tunnel."
            )
        if self.local_port is None:
            self.local_port = _free_local_port()

        cmd = self._build_command()
        console_debug(
            f"Opening SSH tunnel 127.0.0.1:{self.local_port} -> "
            f"{self.remote_host}:{self.remote_port} via {self._ssh_target()}"
        )
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=self.connect_timeout + 5,
            )
        except subprocess.TimeoutExpired as exc:
            raise SSHTunnelError(
                f"Timed out establishing SSH tunnel to {self.ssh_host}."
            ) from exc
        except FileNotFoundError as exc:
            raise SSHTunnelError(f"SSH command failed: {exc}") from exc

        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise SSHTunnelError(
                f"Failed to open SSH tunnel to {self.ssh_host}: "
                f"{detail or 'ssh exited with status ' + str(result.returncode)}"
            )

        if not _wait_for_port(self.local_host, self.local_port, _PORT_WAIT_TIMEOUT):
            self.close()
            raise SSHTunnelError(
                "SSH tunnel opened but the local forward never became reachable. "
                "Check that the database is reachable from the SSH host."
            )

        self._open = True
        console_print(
            f"SSH tunnel ready on 127.0.0.1:{self.local_port} "
            f"(-> {self.remote_host}:{self.remote_port})"
        )
        return self

    def close(self) -> None:
        """Tear down the tunnel's ControlMaster socket (best effort)."""
        if self.control_path and os.path.exists(self.control_path):
            try:
                subprocess.run(
                    [
                        "ssh",
                        "-O", "exit",
                        "-o", f"ControlPath={self.control_path}",
                        self._ssh_target(),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=self.connect_timeout,
                )
            except (subprocess.SubprocessError, OSError) as exc:
                console_debug(f"SSH tunnel teardown error (ignored): {exc}")
            finally:
                try:
                    if os.path.exists(self.control_path):
                        os.unlink(self.control_path)
                except OSError:
                    pass
        self._open = False

    def __enter__(self) -> "SSHTunnel":
        return self.open()

    def __exit__(self, *_exc) -> None:
        self.close()


def normalize_tunnel_config(cfg: Optional[dict]) -> Optional[dict]:
    """Return a cleaned ssh_tunnel dict, or ``None`` when no tunnel is defined.

    A tunnel is considered "defined" only when an ``ssh_host`` is present, so
    callers can pass through an empty/partial block harmlessly.
    """
    if not cfg or not isinstance(cfg, dict):
        return None
    ssh_host = (cfg.get("ssh_host") or "").strip()
    if not ssh_host:
        return None
    out = {
        "ssh_host": ssh_host,
        "ssh_user": (cfg.get("ssh_user") or "").strip(),
        "ssh_port": int(cfg.get("ssh_port") or 22),
    }
    for key in ("ssh_password", "ssh_key_file"):
        value = cfg.get(key)
        if value not in (None, ""):
            out[key] = value
    return out


def open_tunnel_from_config(
    cfg: dict, remote_host: str, remote_port: int
) -> SSHTunnel:
    """Build and open an :class:`SSHTunnel` from a normalized config dict."""
    clean = normalize_tunnel_config(cfg)
    if clean is None:
        raise SSHTunnelError("No SSH host configured for this remote connection.")
    tunnel = SSHTunnel(
        ssh_host=clean["ssh_host"],
        ssh_user=clean.get("ssh_user", ""),
        remote_host=remote_host,
        remote_port=remote_port,
        ssh_port=clean.get("ssh_port", 22),
        ssh_password=clean.get("ssh_password", ""),
        ssh_key_file=clean.get("ssh_key_file", ""),
    )
    return tunnel.open()


__all__ = [
    "SSHTunnel",
    "SSHTunnelError",
    "normalize_tunnel_config",
    "open_tunnel_from_config",
]
