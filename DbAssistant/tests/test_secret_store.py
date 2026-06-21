"""Production-grade tests for :mod:`common.secret_store` and the three
managers that consume it (ConnectionManager, CloudConnectionManager,
MonitorConnectionManager).

These tests cover the failure modes that motivated the hardening pass:

* atomic write durability (no partial JSON on crash);
* nested-secret encryption (cloud profiles with ``sql_connection.password``);
* legacy plaintext on-disk values continue to load and are silently
  re-encrypted on the next save;
* race-free Fernet key creation across concurrent instances;
* corrupt/missing files don't raise (return empty defaults);
* large payloads round-trip;
* permission bits applied to both key and data files.
"""

from __future__ import annotations

import json
import os
import stat
import sys
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from common.secret_store import (
    atomic_write_json,
    decrypt_value,
    encrypt_value,
    load_or_create_fernet_key,
    safe_read_json,
    scrub_for_display,
    walk_decrypt_secrets,
    walk_encrypt_secrets,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _make_cipher(tmp_path: Path) -> Fernet:
    return load_or_create_fernet_key(tmp_path / ".key")


def test_encrypt_decrypt_roundtrip(tmp_path):
    cipher = _make_cipher(tmp_path)
    enc = encrypt_value(cipher, "super-secret")
    assert enc is not None and enc != "super-secret"
    assert decrypt_value(cipher, enc) == "super-secret"


def test_encrypt_empty_returns_none(tmp_path):
    cipher = _make_cipher(tmp_path)
    assert encrypt_value(cipher, "") is None
    assert encrypt_value(cipher, None) is None  # type: ignore[arg-type]


def test_decrypt_legacy_plaintext_returns_none(tmp_path):
    cipher = _make_cipher(tmp_path)
    # Anything that isn't valid base64+Fernet must come back as None so the
    # walker can fall back to the original (legacy plaintext).
    assert decrypt_value(cipher, "loN2ny@hv") is None
    assert decrypt_value(cipher, "not-base64-!!") is None


def test_decrypt_wrong_key_returns_none(tmp_path):
    cipher_a = load_or_create_fernet_key(tmp_path / "a.key")
    cipher_b = load_or_create_fernet_key(tmp_path / "b.key")
    token = encrypt_value(cipher_a, "x")
    assert token is not None
    assert decrypt_value(cipher_b, token) is None


def test_walk_encrypts_nested_dict(tmp_path):
    cipher = _make_cipher(tmp_path)
    obj = {
        "gcp": {
            "name": "g",
            "sql_connection": {"password": "plain", "username": "dheeru"},
        }
    }
    enc = walk_encrypt_secrets(obj, cipher, {"password"})
    inner = enc["gcp"]["sql_connection"]
    assert inner["username"] == "dheeru"
    assert inner["password"] != "plain"

    dec = walk_decrypt_secrets(enc, cipher, {"password"})
    assert dec["gcp"]["sql_connection"]["password"] == "plain"


def test_walk_skips_non_strings(tmp_path):
    cipher = _make_cipher(tmp_path)
    obj = {"password": None, "secret_access_key": 42, "ok": "x"}
    enc = walk_encrypt_secrets(obj, cipher, {"password", "secret_access_key"})
    assert enc["password"] is None
    assert enc["secret_access_key"] == 42
    assert enc["ok"] == "x"


def test_walk_decrypts_mixed_legacy_and_ciphertext(tmp_path):
    """Legacy plaintext values must continue to load after the migration."""
    cipher = _make_cipher(tmp_path)
    encrypted = encrypt_value(cipher, "encrypted")
    obj = {
        "a": {"password": encrypted},          # already migrated
        "b": {"password": "still-plain"},      # legacy on-disk plaintext
    }
    dec = walk_decrypt_secrets(obj, cipher, {"password"})
    assert dec["a"]["password"] == "encrypted"
    assert dec["b"]["password"] == "still-plain"


def test_scrub_for_display_masks_nested_secrets(tmp_path):
    obj = {
        "aws": {"secret_access_key": "AKIA…", "sql_connection": {"password": "p"}}
    }
    masked = scrub_for_display(obj, {"secret_access_key", "password"})
    assert masked["aws"]["secret_access_key"] == "***"
    assert masked["aws"]["sql_connection"]["password"] == "***"


# ---------------------------------------------------------------------------
# Atomic write properties
# ---------------------------------------------------------------------------


def test_atomic_write_creates_file_with_perms(tmp_path):
    p = tmp_path / "out.json"
    assert atomic_write_json(p, {"a": 1}, perms=0o600) is True
    mode = stat.S_IMODE(p.stat().st_mode)
    assert mode == 0o600
    assert json.loads(p.read_text()) == {"a": 1}


def test_atomic_write_does_not_corrupt_on_serialiser_failure(tmp_path):
    """A bad object must not destroy an existing file."""
    p = tmp_path / "out.json"
    assert atomic_write_json(p, {"a": 1}) is True
    # Sets are not JSON-serialisable; the call must fail without truncating.
    assert atomic_write_json(p, {"bad": {1, 2, 3}}) is False
    assert json.loads(p.read_text()) == {"a": 1}


def test_atomic_write_replaces_existing_file(tmp_path):
    p = tmp_path / "out.json"
    atomic_write_json(p, {"v": 1})
    atomic_write_json(p, {"v": 2})
    assert json.loads(p.read_text()) == {"v": 2}


def test_safe_read_returns_none_for_missing(tmp_path):
    assert safe_read_json(tmp_path / "nope.json") is None


def test_safe_read_returns_none_for_corrupt(tmp_path):
    p = tmp_path / "out.json"
    p.write_text("{not json")
    assert safe_read_json(p) is None


def test_no_stale_temp_files_on_success(tmp_path):
    p = tmp_path / "out.json"
    atomic_write_json(p, {"v": 1})
    leftovers = [
        name for name in os.listdir(tmp_path)
        if name.endswith(".tmp") or ".tmp." in name
    ]
    assert leftovers == [], f"Stray temp files left: {leftovers}"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_concurrent_writers_do_not_lose_writes(tmp_path):
    """Multiple writers serialise via the lock; final state is well-formed."""
    p = tmp_path / "out.json"
    atomic_write_json(p, {"v": 0})
    errors: list[Exception] = []

    def writer(i: int):
        try:
            atomic_write_json(p, {"v": i})
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert not errors
    final = json.loads(p.read_text())
    assert "v" in final
    assert 0 <= final["v"] < 20


def test_concurrent_key_creation_is_race_free(tmp_path):
    """Two threads racing to create the key end up with the same Fernet key."""
    key_path = tmp_path / ".db_key"
    results: list[bytes] = []

    def make():
        load_or_create_fernet_key(key_path)
        results.append(key_path.read_bytes())

    threads = [threading.Thread(target=make) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(set(results)) == 1
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o600


# ---------------------------------------------------------------------------
# Cloud manager: nested encryption + legacy migration
# ---------------------------------------------------------------------------


def _isolated_cloud_mgr(tmp_path: Path):
    from common.cloud.connection_manager import CloudConnectionManager

    cfg = tmp_path / "config"
    cfg.mkdir()
    mgr = CloudConnectionManager.__new__(CloudConnectionManager)
    mgr.config_dir = cfg
    mgr.config_file = cfg / "cloud_connections.json"
    mgr.key_file = cfg / ".db_key"
    mgr.cipher = mgr._init_cipher()
    return mgr


def test_cloud_nested_password_is_encrypted_on_disk(tmp_path):
    """The original production bug: nested secrets were written in plaintext."""
    mgr = _isolated_cloud_mgr(tmp_path)
    profile = {
        "gcp-pg": {
            "provider": "GCP",
            "sql_connection": {
                "host": "1.2.3.4",
                "username": "dheeru",
                "password": "very-secret",
            },
        }
    }
    assert mgr.save_cloud_databases(profile) is True
    raw = json.loads(mgr.config_file.read_text())
    # The nested password must NOT appear in plaintext on disk.
    on_disk = raw["gcp-pg"]["sql_connection"]["password"]
    assert on_disk != "very-secret"
    assert isinstance(on_disk, str) and len(on_disk) > 40

    loaded = mgr.load_cloud_databases()
    assert loaded["gcp-pg"]["sql_connection"]["password"] == "very-secret"


def test_cloud_legacy_plaintext_loads_and_migrates(tmp_path):
    mgr = _isolated_cloud_mgr(tmp_path)
    # Hand-craft a legacy on-disk file with plaintext nested password.
    legacy = {
        "old-profile": {
            "provider": "GCP",
            "sql_connection": {"password": "legacy-plain"},
        }
    }
    mgr.config_file.write_text(json.dumps(legacy))
    loaded = mgr.load_cloud_databases()
    assert loaded["old-profile"]["sql_connection"]["password"] == "legacy-plain"

    # Subsequent save must encrypt that nested secret going forward.
    assert mgr.save_cloud_databases(loaded) is True
    raw = json.loads(mgr.config_file.read_text())
    assert raw["old-profile"]["sql_connection"]["password"] != "legacy-plain"


def test_cloud_save_is_atomic_on_serialiser_failure(tmp_path):
    mgr = _isolated_cloud_mgr(tmp_path)
    mgr.save_cloud_databases({"x": {"name": "x"}})
    # An unserialisable value must not destroy the previous content.
    assert mgr.save_cloud_databases({"x": {"bad": {1, 2}}}) is False
    raw = json.loads(mgr.config_file.read_text())
    assert raw == {"x": {"name": "x"}}


def test_cloud_load_returns_empty_dict_for_corrupt(tmp_path):
    mgr = _isolated_cloud_mgr(tmp_path)
    mgr.config_file.write_text("not json {")
    assert mgr.load_cloud_databases() == {}


# ---------------------------------------------------------------------------
# DB ConnectionManager
# ---------------------------------------------------------------------------


def _isolated_db_mgr(tmp_path: Path):
    from common.connection_manager import ConnectionManager

    cfg = tmp_path / "config"
    cfg.mkdir()
    mgr = ConnectionManager.__new__(ConnectionManager)
    mgr.config_dir = cfg
    mgr.config_file = cfg / "saved_connections.json"
    mgr.key_file = cfg / ".db_key"
    mgr.cipher = mgr._init_cipher()
    mgr.connections = []
    return mgr


def test_db_add_update_delete_lifecycle(tmp_path):
    from common.connection_params import ConnectionParams

    mgr = _isolated_db_mgr(tmp_path)
    def params(**over):
        base = {
            "name": "c1", "db_type": "MySQL", "host": "localhost",
            "port": 3306, "service_or_db": "test",
            "username": "u", "password": "p",
        }
        base.update(over)
        return ConnectionParams.from_mapping(base)

    ok, msg = mgr.add_connection(
        params(save_password=True),
    )
    assert ok, msg
    assert mgr.connection_exists("c1")

    # Duplicate add must fail.
    ok2, _ = mgr.add_connection(
        params(),
    )
    assert ok2 is False

    # Update rotates fields atomically.
    ok3, _ = mgr.update_connection(
        "c1",
        params(name="c1-renamed", port=3307, username="u2",
               password="p2", save_password=True),
    )
    assert ok3
    on_disk = json.loads(mgr.config_file.read_text())
    assert on_disk[0]["name"] == "c1-renamed"
    assert on_disk[0]["password"] != "p2"  # encrypted

    # Delete
    ok4, _ = mgr.delete_connection("c1-renamed")
    assert ok4 and mgr.connections == []


def test_db_get_connection_returns_copy(tmp_path):
    from common.connection_params import ConnectionParams

    mgr = _isolated_db_mgr(tmp_path)
    mgr.add_connection(
        ConnectionParams.from_mapping({
            "name": "c1", "db_type": "MySQL", "host": "h",
            "port": 3306, "service_or_db": "d", "username": "u",
            "password": "p", "save_password": True,
        }),
    )
    c = mgr.get_connection("c1")
    c["host"] = "MUTATED"
    again = mgr.get_connection("c1")
    assert again["host"] != "MUTATED"


def test_db_corrupt_file_returns_empty_list(tmp_path):
    mgr = _isolated_db_mgr(tmp_path)
    mgr.config_file.write_text("[not json")
    assert mgr.load_connections() == []
