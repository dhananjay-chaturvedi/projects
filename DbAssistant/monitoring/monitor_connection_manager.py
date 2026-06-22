"""Saved Monitoring (SSH/OS-target) connections with encrypted passwords."""

from common import paths as _paths
from common.config_loader import config
from common.secret_store import (
    atomic_write_json,
    encrypt_value,
    decrypt_value,
    load_or_create_fernet_key,
    safe_read_json,
    walk_decrypt_secrets,
    walk_encrypt_secrets,
)


_MONITOR_SENSITIVE_FIELDS = frozenset({"password"})


class MonitorConnectionManager:
    """Manage saved Monitoring connections with encrypted passwords."""

    def __init__(self, config_file=None):
        # New v1 layout: ``<DBASSISTANT_HOME>/connections/monitor.json``
        # encrypted under ``<DBASSISTANT_HOME>/keys/monitor.key``.
        _paths.bootstrap()
        self.config_dir = _paths.connections_dir()
        self.config_dir.mkdir(parents=True, exist_ok=True)

        if config_file is None:
            self.config_file = _paths.monitor_connections_path()
        else:
            self.config_file = self.config_dir / config_file

        self.key_file = _paths.monitor_key_path()
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
        data = safe_read_json(self.config_file)
        if not isinstance(data, list):
            return []
        return walk_decrypt_secrets(data, self.cipher, _MONITOR_SENSITIVE_FIELDS)

    def save_connections(self):
        try:
            payload = walk_encrypt_secrets(
                self.connections, self.cipher, _MONITOR_SENSITIVE_FIELDS
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

    def add_connection(self, name, host, username, password=None, target_type="vm"):
        if any(c.get("name") == name for c in self.connections):
            return False, "Connection name already exists"

        self.connections.append({
            "name": name,
            "host": host,
            "username": username,
            "password": password,
            "target_type": target_type or "vm",
        })
        if self.save_connections():
            return True, "Monitor connection saved successfully"
        return False, "Failed to save monitor connection"

    def update_connection(
        self, old_name, name, host, username, password=None, target_type=None,
    ):
        """Update an existing monitor connection.

        ``target_type`` defaults to the previous value when omitted so callers
        that don't know/care about it (e.g. the UI's edit dialog) don't reset
        the persisted field.
        """
        for i, conn in enumerate(self.connections):
            if conn.get("name") != old_name:
                continue
            prev_target = conn.get("target_type") or "vm"
            self.connections[i] = {
                "name": name,
                "host": host,
                "username": username,
                "password": password,
                "target_type": target_type if target_type else prev_target,
            }
            if self.save_connections():
                return True, "Monitor connection updated successfully"
            return False, "Failed to update monitor connection"
        return False, "Monitor connection not found"

    def delete_connection(self, name):
        for i, conn in enumerate(self.connections):
            if conn.get("name") == name:
                self.connections.pop(i)
                if self.save_connections():
                    return True, "Monitor connection deleted successfully"
                return False, "Failed to delete monitor connection"
        return False, "Monitor connection not found"

    def get_connection(self, name):
        for conn in self.connections:
            if conn.get("name") == name:
                return dict(conn)
        return None

    def get_all_connections(self):
        return self.connections

    def connection_exists(self, name):
        return any(conn.get("name") == name for conn in self.connections)
