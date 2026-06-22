# ---------------------------------------------------------------------
# description: Connection manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""Saved DB connection persistence.

All on-disk durability + secret handling is delegated to
:mod:`common.secret_store` so this module focuses on the connection-level
data model (add/update/delete/lookup).

Storage layout is owned by :mod:`common.paths`: this manager never
hardcodes paths.
"""

from common import paths as _paths
from common.config_loader import config
from common.connection_params import ConnectionParams
from common.secret_store import (
    atomic_write_json,
    encrypt_value,
    decrypt_value,
    load_or_create_fernet_key,
    safe_read_json,
    walk_decrypt_secrets,
    walk_encrypt_secrets,
)


# Anything walked through the secret store is encrypted on write and decrypted
# on read.  Kept as a small frozenset for cheap membership checks.
_DB_SENSITIVE_FIELDS = frozenset({"password", "ssh_password"})


class ConnectionManager:
    """Manage saved database connections with encrypted passwords."""

    def __init__(self, config_file=None):
        # The new layout is fixed: ``<DBASSISTANT_HOME>/connections/db.json``
        # encrypted under ``<DBASSISTANT_HOME>/keys/db.key``. ``config_file``
        # is preserved as an injection point so tests / advanced callers
        # can still override the connections-file path (the key remains
        # tied to the dbassistant home, otherwise rotation would surprise
        # callers).
        _paths.bootstrap()
        self.config_dir = _paths.connections_dir()
        self.config_dir.mkdir(parents=True, exist_ok=True)

        if config_file is None:
            self.config_file = _paths.db_connections_path()
        else:
            self.config_file = self.config_dir / config_file

        self.key_file = _paths.db_key_path()
        self.key_file.parent.mkdir(parents=True, exist_ok=True)

        self._key_perms = config.get_octal(
            "security", "key_file_permissions", default=0o600
        )
        self._file_perms = config.get_octal(
            "security", "config_file_permissions", default=0o600
        )

        self.cipher = self._init_cipher()
        self.connections = self.load_connections()

    # ------------------------------------------------------------------
    # Crypto + persistence
    # ------------------------------------------------------------------

    def _init_cipher(self):
        return load_or_create_fernet_key(
            self.key_file, perms=getattr(self, "_key_perms", 0o600)
        )

    def _encrypt_password(self, password):
        return encrypt_value(self.cipher, password)

    def _decrypt_password(self, encrypted_password):
        return decrypt_value(self.cipher, encrypted_password)

    def load_connections(self):
        """Load saved connections, returning a list with decrypted secrets."""
        data = safe_read_json(self.config_file)
        if not isinstance(data, list):
            return []
        return walk_decrypt_secrets(data, self.cipher, _DB_SENSITIVE_FIELDS)

    def save_connections(self):
        """Atomically persist the current connection list with encrypted secrets."""
        try:
            payload = walk_encrypt_secrets(
                self.connections, self.cipher, _DB_SENSITIVE_FIELDS
            )
            return atomic_write_json(
                self.config_file, payload,
                perms=getattr(self, "_file_perms", 0o600),
            )
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Data model
    # ------------------------------------------------------------------

    @staticmethod
    def _build_connection(params: ConnectionParams):
        connection = params.to_profile(include_password=params.save_password)
        connection.pop("ssh_tunnel", None)
        normalized_tunnel = ConnectionManager._normalize_ssh_tunnel(
            params.ssh_tunnel, params.save_password
        )
        if normalized_tunnel:
            connection["ssh_tunnel"] = normalized_tunnel
        return connection

    @staticmethod
    def _normalize_ssh_tunnel(ssh_tunnel, save_password):
        """Return a cleaned ssh_tunnel dict, or ``None`` when not a tunnel.

        Only persists the SSH password when ``save_password`` is set, mirroring
        how the primary DB password is handled.
        """
        if not ssh_tunnel or not isinstance(ssh_tunnel, dict):
            return None
        ssh_host = (ssh_tunnel.get("ssh_host") or "").strip()
        if not ssh_host:
            return None
        out = {
            "ssh_host": ssh_host,
            "ssh_user": (ssh_tunnel.get("ssh_user") or "").strip(),
            "ssh_port": int(ssh_tunnel.get("ssh_port") or 22),
        }
        key_file = (ssh_tunnel.get("ssh_key_file") or "").strip()
        if key_file:
            out["ssh_key_file"] = key_file
        ssh_password = ssh_tunnel.get("ssh_password") or ""
        if ssh_password and save_password:
            out["ssh_password"] = ssh_password
        return out

    def add_connection(
        self,
        params: ConnectionParams,
        persist=True,
    ):
        name = params.name
        if any(c.get("name") == name for c in self.connections):
            return False, "Connection name already exists"

        self.connections.append(self._build_connection(params))
        if not persist:
            return True, "Connection saved successfully"
        if self.save_connections():
            return True, "Connection saved successfully"
        self.connections.pop()
        return False, "Failed to save connection"

    def update_connection(
        self,
        old_name,
        params: ConnectionParams,
        persist=True,
    ):
        for i, conn in enumerate(self.connections):
            if conn.get("name") != old_name:
                continue
            previous = self.connections[i]
            self.connections[i] = self._build_connection(params)
            if not persist:
                return True, "Connection updated successfully"
            if self.save_connections():
                return True, "Connection updated successfully"
            self.connections[i] = previous
            return False, "Failed to update connection"
        return False, "Connection not found"

    def delete_connection(self, name):
        for i, conn in enumerate(self.connections):
            if conn.get("name") == name:
                removed = self.connections.pop(i)
                if self.save_connections():
                    return True, "Connection deleted successfully"
                self.connections.insert(i, removed)
                return False, "Failed to delete connection"
        return False, "Connection not found"

    def get_connection(self, name):
        """Return a deep copy of the named connection or ``None``.

        A deep copy is returned so callers cannot mutate the manager's
        in-memory state by accident — including nested dicts like ssh_tunnel.
        """
        import copy
        for conn in self.connections:
            if conn.get("name") == name:
                return copy.deepcopy(conn)
        return None

    def get_all_connections(self):
        return self.connections

    def connection_exists(self, name):
        return any(conn.get("name") == name for conn in self.connections)
