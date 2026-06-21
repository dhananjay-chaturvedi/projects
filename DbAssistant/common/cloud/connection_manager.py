# ---------------------------------------------------------------------
# Cloud connection persistence — shared by Connections tab and Monitoring.
# ---------------------------------------------------------------------

"""Cloud connection profiles with at-rest encryption of credentials.

Hardening notes:
    * Top-level *and* nested sensitive fields are encrypted recursively
      (see :data:`_SENSITIVE_FIELDS`). Earlier versions only walked the
      top level, leaving ``sql_connection.password`` in plaintext on disk.
    * On-disk durability comes from :mod:`common.secret_store`
      (atomic temp-file rename, fsync, race-safe key creation).
    * The decrypter is forgiving: legacy plaintext values continue to load
      and are re-encrypted on the next save.
"""

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


_SENSITIVE_FIELDS = frozenset(
    {
        "access_key_id",
        "secret_access_key",
        "session_token",
        "client_secret",
        "password",
        "private_key",
        "api_key",
        "sso_client_secret",
        "sa_key_json",
        "oauth_token",
        "oauth_client_secret",
        "oauth_refresh_token",
        "bearer_token",
    }
)


class CloudConnectionManager:
    """Persist cloud connection profiles with encrypted credentials."""

    def __init__(self):
        # New v1 layout: ``<DBASSISTANT_HOME>/connections/cloud.json``
        # encrypted under a dedicated ``<DBASSISTANT_HOME>/keys/cloud.key``.
        # Pre-v1 profiles (encrypted with the shared ``.db_key``) are
        # re-encrypted during migration.
        _paths.bootstrap()
        self.config_dir = _paths.connections_dir()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = _paths.cloud_connections_path()

        self.key_file = _paths.cloud_key_path()
        self.key_file.parent.mkdir(parents=True, exist_ok=True)

        self._key_perms = config.get_octal(
            "security", "key_file_permissions", default=0o600
        )
        self._file_perms = config.get_octal(
            "security", "config_file_permissions", default=0o600
        )

        self.cipher = self._init_cipher()

    def _init_cipher(self):
        return load_or_create_fernet_key(
            self.key_file, perms=getattr(self, "_key_perms", 0o600)
        )

    def _encrypt(self, value: str) -> str | None:
        return encrypt_value(self.cipher, value)

    def _decrypt(self, token: str) -> str | None:
        return decrypt_value(self.cipher, token)

    def load_cloud_databases(self) -> dict:
        """Load cloud profiles, decrypting every known sensitive field at any depth."""
        data = safe_read_json(self.config_file)
        if not isinstance(data, dict):
            return {}
        return walk_decrypt_secrets(data, self.cipher, _SENSITIVE_FIELDS)

    def save_cloud_databases(self, cloud_databases: dict) -> bool:
        """Atomically persist cloud profiles, recursively encrypting secrets."""
        if not isinstance(cloud_databases, dict):
            return False
        try:
            payload = walk_encrypt_secrets(
                cloud_databases, self.cipher, _SENSITIVE_FIELDS
            )
            return atomic_write_json(
                self.config_file, payload,
                perms=getattr(self, "_file_perms", 0o600),
            )
        except Exception:
            return False
