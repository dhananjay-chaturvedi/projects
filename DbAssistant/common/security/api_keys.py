"""Local API-key management.

Secrets are shown once, stored only as salted hashes, and protected with the
same sidecar lock/atomic-write helpers used by other shared state.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common import paths
from common.concurrency import atomic_write_text, file_lock, read_json

_ITERATIONS = 260_000
_KEY_PREFIX = "dbak_"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_path() -> Path:
    return paths.dbassistant_home() / "keys" / "api_credentials.json"


def _hash_secret(secret: str, salt_hex: str | None = None) -> tuple[str, str]:
    salt = bytes.fromhex(salt_hex) if salt_hex else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", secret.encode("utf-8"), salt, _ITERATIONS)
    return salt.hex(), digest.hex()


def _load_unlocked() -> dict[str, Any]:
    data = read_json(store_path(), {"keys": []})
    if not isinstance(data, dict):
        return {"keys": []}
    keys = data.get("keys")
    if not isinstance(keys, list):
        data["keys"] = []
    return data


def _save_unlocked(data: dict[str, Any]) -> None:
    import json

    atomic_write_text(store_path(), json.dumps(data, indent=2), lock=False)


def _public_record(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "key_id": rec.get("key_id", ""),
        "name": rec.get("name", ""),
        "created_at": rec.get("created_at", ""),
        "updated_at": rec.get("updated_at", ""),
        "last_used_at": rec.get("last_used_at", ""),
        "revoked_at": rec.get("revoked_at", ""),
    }


def create_key(name: str = "", *, expires_at: str | None = None) -> dict[str, Any]:
    """Create a local API credential and return the secret exactly once."""
    key_id = _KEY_PREFIX + secrets.token_urlsafe(9).replace("-", "_")
    secret = secrets.token_urlsafe(32)
    salt, digest = _hash_secret(secret)
    now = _now()
    rec = {
        "key_id": key_id,
        "name": name or key_id,
        "salt": salt,
        "secret_hash": digest,
        "iterations": _ITERATIONS,
        "created_at": now,
        "updated_at": now,
        "last_used_at": "",
        "revoked_at": "",
        "expires_at": expires_at or "",
    }
    with file_lock(store_path()):
        data = _load_unlocked()
        data.setdefault("keys", []).append(rec)
        _save_unlocked(data)
    return {**_public_record(rec), "secret": secret, "token": f"{key_id}.{secret}"}


def list_keys() -> list[dict[str, Any]]:
    with file_lock(store_path(), shared=True):
        data = _load_unlocked()
        return [_public_record(r) for r in data.get("keys", []) if isinstance(r, dict)]


def verify_token(token: str) -> dict[str, Any] | None:
    """Return public key info when *token* is valid and active."""
    if not token or "." not in token:
        return None
    key_id, secret = token.split(".", 1)
    if not key_id or not secret:
        return None
    with file_lock(store_path()):
        data = _load_unlocked()
        changed = False
        for rec in data.get("keys", []):
            if not isinstance(rec, dict):
                continue
            if rec.get("key_id") != key_id or rec.get("revoked_at"):
                continue
            salt = rec.get("salt", "")
            expected = rec.get("secret_hash", "")
            if not salt or not expected:
                continue
            _, candidate = _hash_secret(secret, salt)
            if hmac.compare_digest(candidate, expected):
                rec["last_used_at"] = _now()
                changed = True
                if changed:
                    _save_unlocked(data)
                return _public_record(rec)
        return None


def revoke_key(key_id: str) -> dict[str, Any]:
    with file_lock(store_path()):
        data = _load_unlocked()
        for rec in data.get("keys", []):
            if isinstance(rec, dict) and rec.get("key_id") == key_id:
                rec["revoked_at"] = rec.get("revoked_at") or _now()
                rec["updated_at"] = _now()
                _save_unlocked(data)
                return {"ok": True, "key": _public_record(rec)}
    return {"ok": False, "error": f"API key '{key_id}' not found."}


def regenerate_key(key_id: str) -> dict[str, Any]:
    secret = secrets.token_urlsafe(32)
    salt, digest = _hash_secret(secret)
    with file_lock(store_path()):
        data = _load_unlocked()
        for rec in data.get("keys", []):
            if isinstance(rec, dict) and rec.get("key_id") == key_id:
                rec["salt"] = salt
                rec["secret_hash"] = digest
                rec["iterations"] = _ITERATIONS
                rec["revoked_at"] = ""
                rec["updated_at"] = _now()
                _save_unlocked(data)
                return {
                    "ok": True,
                    **_public_record(rec),
                    "secret": secret,
                    "token": f"{key_id}.{secret}",
                }
    return {"ok": False, "error": f"API key '{key_id}' not found."}


def has_any_key() -> bool:
    return bool([r for r in list_keys() if not r.get("revoked_at")])
