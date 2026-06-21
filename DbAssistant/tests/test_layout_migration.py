"""End-to-end tests for the v1 ``~/.dbassistant/`` layout migration.

Each test sets ``DBASSISTANT_HOME`` to a private temp dir and isolates
the legacy ``HOME`` so the migrator never touches the developer's real
files. The fixtures below also reset the in-process bootstrap memo so
every test starts in a clean state.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import pytest

# Loud failure during collection beats cryptic late errors.
pytest.importorskip("cryptography")

from cryptography.fernet import Fernet

from common import paths as P
from common.layout_migration import migrate_if_needed
from common.secret_store import (
    atomic_write_json,
    encrypt_value,
    load_or_create_fernet_key,
    safe_read_json,
    walk_decrypt_secrets,
    walk_encrypt_secrets,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_home(tmp_path, monkeypatch):
    """Quarantine ``HOME`` and ``DBASSISTANT_HOME`` to a tmp tree.

    Returns ``(legacy_home, dbassistant_home)``. The migrator's resolver
    sees ``DBASSISTANT_HOME``; ``Path.home()`` (used by
    :func:`common.paths.legacy_*`) returns ``legacy_home``.
    """
    legacy_home = tmp_path / "home"
    legacy_home.mkdir()
    dbassistant_home = tmp_path / "dbassistant"

    monkeypatch.setenv("HOME", str(legacy_home))
    monkeypatch.setenv("DBASSISTANT_HOME", str(dbassistant_home))
    # Lock pathlib's Path.home() to the quarantine too — pathlib reads
    # HOME on POSIX, but be explicit so the test never reaches real $HOME.
    monkeypatch.setattr(Path, "home", lambda: legacy_home)

    P.reset_bootstrap_state_for_tests()
    yield legacy_home, dbassistant_home


def _write_legacy_db_key(legacy_cfg: Path) -> Fernet:
    legacy_cfg.mkdir(parents=True, exist_ok=True)
    key_path = legacy_cfg / ".db_key"
    key_path.write_bytes(Fernet.generate_key())
    os.chmod(key_path, 0o600)
    return load_or_create_fernet_key(key_path)


def _seed_legacy_layout(legacy_home: Path) -> dict:
    """Create a realistic pre-v1 layout under ``legacy_home``.

    Returns the in-memory plaintext payloads so tests can compare them
    against what migration produces.
    """
    legacy_cfg = legacy_home / ".dbmanager"
    legacy_rt = legacy_home / ".dbtool"
    legacy_cfg.mkdir()
    legacy_rt.mkdir()

    # ---- Keys + DB connections (encrypted with .db_key) -------------------
    db_cipher = _write_legacy_db_key(legacy_cfg)
    db_profiles = [
        {
            "name": "local_mysql",
            "db_type": "MySQL",
            "host": "localhost",
            "port": 3306,
            "service_or_db": "test",
            "username": "u",
            "password": encrypt_value(db_cipher, "db-secret-1"),
            "save_password": True,
        }
    ]
    atomic_write_json(legacy_cfg / "saved_connections.json", db_profiles)

    # ---- Cloud connections (also encrypted with .db_key in pre-v1) -------
    cloud_plain = {
        "aws_prod": {
            "provider": "AWS",
            "access_key_id": "AKIA-EXAMPLE",
            "secret_access_key": "very-secret",
            "sql_connection": {
                "host": "rds-host",
                "username": "appuser",
                "password": "nested-pw",
            },
        }
    }
    from common.cloud.connection_manager import _SENSITIVE_FIELDS as CLOUD_SENS

    encrypted_cloud = walk_encrypt_secrets(cloud_plain, db_cipher, CLOUD_SENS)
    atomic_write_json(legacy_cfg / "cloud_connections.json", encrypted_cloud)

    # ---- Monitor connections (encrypted with .monitor_key) ---------------
    mon_key = legacy_cfg / ".monitor_key"
    mon_key.write_bytes(Fernet.generate_key())
    os.chmod(mon_key, 0o600)
    mon_cipher = load_or_create_fernet_key(mon_key)
    monitor_profiles = [
        {
            "name": "vm1",
            "host": "10.0.0.1",
            "username": "ssh-user",
            "password": encrypt_value(mon_cipher, "ssh-pw"),
            "target_type": "vm",
        }
    ]
    atomic_write_json(
        legacy_cfg / "saved_monitor_connections.json", monitor_profiles
    )

    # ---- Legacy junk we want to see renamed ------------------------------
    (legacy_cfg / ".cloud_key").write_bytes(b"legacy-junk")
    (legacy_cfg / "saved_cloud_connections.json").write_text("{}")

    # ---- Runtime/session files ------------------------------------------
    (legacy_rt / "daemon.log").write_text("old daemon log\n")
    (legacy_rt / "daemon.pid").write_text("99999\n")
    (legacy_rt / "metrics.json").write_text('{"connections": {}}')
    (legacy_rt / "alerts.jsonl").write_text(
        '{"severity":"INFO","message":"hello"}\n'
    )
    (legacy_rt / "ai_state.json").write_text('{"mask_pii": true}')
    (legacy_rt / "dashboard_layout.json").write_text(
        '{"rows": [["connections","monitor"]]}'
    )
    sess_dir = legacy_rt / "ai_sessions"
    sess_dir.mkdir()
    (sess_dir / "sessions.json").write_text("[]")

    return {
        "db_password": "db-secret-1",
        "cloud_plain":  cloud_plain,
        "ssh_password": "ssh-pw",
    }


# ---------------------------------------------------------------------------
# Resolver / env-var tests
# ---------------------------------------------------------------------------


def test_dbassistant_home_uses_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "x"))
    assert P.dbassistant_home() == (tmp_path / "x").resolve()


def test_dbassistant_home_expands_tilde(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DBASSISTANT_HOME", "~/data/dbassistant")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert P.dbassistant_home() == (tmp_path / "data" / "dbassistant").resolve()


def test_dbassistant_home_default_when_env_blank(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DBASSISTANT_HOME", "  ")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert P.dbassistant_home() == (tmp_path / ".dbassistant").resolve()


def test_runtime_session_dirs_are_overridable(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    P.reset_bootstrap_state_for_tests()

    from common.config_loader import config as _cfg

    # Inject overrides via monkey-patching get(); the resolver reads them
    # lazily so this is enough for the duration of the test.
    real_get = _cfg.get
    monkeypatch.setattr(
        _cfg, "get",
        lambda s, k, d="": (str(tmp_path / "rt") if (s == "paths" and k == "runtime_dir")
                            else str(tmp_path / "ses") if (s == "paths" and k == "session_dir")
                            else real_get(s, k, d)),
    )
    assert P.runtime_dir() == (tmp_path / "rt").resolve()
    assert P.session_dir() == (tmp_path / "ses").resolve()


# ---------------------------------------------------------------------------
# Migration behaviour
# ---------------------------------------------------------------------------


def test_fresh_install_creates_layout_and_marks_version(fake_home):
    legacy_home, dbassistant_home = fake_home

    result = migrate_if_needed()
    assert result["ok"], result
    assert result["status"] == "fresh"

    assert dbassistant_home.is_dir()
    for sub in ("keys", "connections", "runtime", "session"):
        assert (dbassistant_home / sub).is_dir(), sub
    assert P.read_layout_version() == P.LAYOUT_VERSION


def test_full_migration_preserves_db_password(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    result = migrate_if_needed()
    assert result["ok"], result
    assert result["status"] == "migrated"

    # The DB connection should decrypt correctly with the new ConnectionManager
    P.reset_bootstrap_state_for_tests()  # force re-bootstrap so the next read sees current env
    from common.connection_manager import ConnectionManager

    mgr = ConnectionManager()
    profile = mgr.get_connection("local_mysql")
    assert profile is not None
    assert profile["password"] == "db-secret-1"


def test_full_migration_preserves_cloud_password_with_new_key(fake_home):
    legacy_home, _ = fake_home
    seed = _seed_legacy_layout(legacy_home)

    result = migrate_if_needed()
    assert result["ok"], result["message"]
    assert result["status"] == "migrated"
    assert result["details"]["connections"]["cloud.json"] == "re-encrypted+verified"

    # The new cloud.key must exist and be distinct from the migrated db.key.
    assert P.cloud_key_path().is_file()
    assert P.cloud_key_path().read_bytes() != P.db_key_path().read_bytes()

    P.reset_bootstrap_state_for_tests()
    from common.cloud.connection_manager import CloudConnectionManager

    cm = CloudConnectionManager()
    loaded = cm.load_cloud_databases()
    assert loaded["aws_prod"]["secret_access_key"] == "very-secret"
    assert (
        loaded["aws_prod"]["sql_connection"]["password"]
        == seed["cloud_plain"]["aws_prod"]["sql_connection"]["password"]
    )


def test_full_migration_preserves_monitor_password(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    result = migrate_if_needed()
    assert result["ok"], result

    P.reset_bootstrap_state_for_tests()
    from monitoring.monitor_connection_manager import MonitorConnectionManager

    mcm = MonitorConnectionManager()
    profile = mcm.get_connection("vm1")
    assert profile is not None
    assert profile["password"] == "ssh-pw"


def test_runtime_state_files_are_copied(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    result = migrate_if_needed()
    assert result["ok"], result

    assert P.daemon_log_path().read_text() == "old daemon log\n"
    assert P.metrics_snapshot_path().is_file()
    assert P.alerts_log_path().read_text().strip()
    assert P.ai_state_path().is_file()
    assert P.dashboard_layout_path().is_file()
    assert (P.ai_sessions_dir() / "sessions.json").is_file()


def test_legacy_directories_are_renamed(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    result = migrate_if_needed()
    assert result["ok"], result

    assert not (legacy_home / ".dbmanager").exists()
    assert not (legacy_home / ".dbtool").exists()
    assert (legacy_home / ".dbmanager.legacy").is_dir()
    assert (legacy_home / ".dbtool.legacy").is_dir()
    # Legacy junk files are renamed inside the *.legacy/ tree.
    assert (legacy_home / ".dbmanager.legacy" / ".cloud_key.legacy").is_file()
    assert (
        legacy_home / ".dbmanager.legacy" / "saved_cloud_connections.json.legacy"
    ).is_file()


def test_migration_is_idempotent(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    r1 = migrate_if_needed()
    assert r1["status"] == "migrated"
    # Capture cloud.key and db.key bytes so we can prove they're untouched.
    cloud_key_after_first = P.cloud_key_path().read_bytes()
    db_key_after_first = P.db_key_path().read_bytes()

    P.reset_bootstrap_state_for_tests()
    r2 = migrate_if_needed()
    assert r2["ok"]
    assert r2["status"] == "current"
    assert P.cloud_key_path().read_bytes() == cloud_key_after_first
    assert P.db_key_path().read_bytes() == db_key_after_first


def test_partial_legacy_only_dbmanager_present(fake_home):
    legacy_home, _ = fake_home
    legacy_cfg = legacy_home / ".dbmanager"
    db_cipher = _write_legacy_db_key(legacy_cfg)
    atomic_write_json(
        legacy_cfg / "saved_connections.json",
        [{"name": "only_db", "db_type": "MySQL", "host": "h", "port": 3306,
          "service_or_db": "d", "username": "u",
          "password": encrypt_value(db_cipher, "pw"), "save_password": True}],
    )

    result = migrate_if_needed()
    assert result["ok"], result
    assert result["status"] == "migrated"

    P.reset_bootstrap_state_for_tests()
    from common.connection_manager import ConnectionManager

    mgr = ConnectionManager()
    assert mgr.get_connection("only_db")["password"] == "pw"


def test_corrupted_cloud_legacy_does_not_destroy_new_layout(fake_home, monkeypatch):
    """If cloud profiles exist but the legacy .db_key is missing, the migrator
    must roll back the partial new layout instead of leaving garbage."""
    legacy_home, dbassistant_home = fake_home
    legacy_cfg = legacy_home / ".dbmanager"
    legacy_cfg.mkdir()
    # cloud_connections.json present, .db_key absent -> migrator must error
    (legacy_cfg / "cloud_connections.json").write_text('{"x": {"provider":"AWS"}}')

    result = migrate_if_needed()
    assert result["ok"] is False
    assert "legacy .db_key is missing" in result["message"]
    # Partial new layout should have been rolled back: no cloud key, no
    # cloud.json. Version marker must NOT have been written.
    assert not P.cloud_key_path().exists()
    assert not P.cloud_connections_path().exists()
    assert P.read_layout_version() is None


def test_bootstrap_caches_result(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    P.reset_bootstrap_state_for_tests()
    r1 = P.bootstrap()
    assert r1["status"] == "migrated"

    # Second call without reset should hit the in-process cache and not
    # re-run the work. The status string from migrate_if_needed when the
    # marker is present is "current"; the cached path returns the original
    # "migrated" result, which is exactly what we want to verify.
    r2 = P.bootstrap()
    assert r2 is r1


def test_bootstrap_force_reruns(fake_home):
    legacy_home, _ = fake_home
    _seed_legacy_layout(legacy_home)

    P.reset_bootstrap_state_for_tests()
    r1 = P.bootstrap()
    assert r1["status"] == "migrated"

    r2 = P.bootstrap(force=True)
    # Second forced run sees the marker already in place.
    assert r2["status"] == "current"
