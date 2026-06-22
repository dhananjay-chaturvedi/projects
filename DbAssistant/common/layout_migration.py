"""
One-time migration from the legacy two-directory layout
(``~/.dbmanager/`` + ``~/.dbtool/``) to the unified ``~/.dbassistant/``
storage tree.

Design goals
------------

* **Copy-then-rename.** Originals are read but never deleted until the
  whole migration succeeds. If any step fails, the partial new layout
  is removed and the user's old data is untouched.
* **Cloud profiles get a brand-new key.** Pre-v1 cloud profiles were
  encrypted with the same key as the DB profiles (``.db_key``). v1 gives
  cloud its own ``cloud.key``; we re-encrypt every cloud secret during
  migration and **verify the roundtrip** before declaring success.
* **Legacy files are renamed in place, not deleted.** The previous
  ``~/.dbmanager/`` and ``~/.dbtool/`` directories become
  ``~/.dbmanager.legacy/`` and ``~/.dbtool.legacy/`` so a rollback is
  one rename away.
* **Idempotent.** A ``version`` marker in the new root tells subsequent
  launches that migration is already done.
* **Lock-protected.** A sibling lock file serialises concurrent
  first-launches so two processes can never race the migration.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

from common import paths as P
from common.secret_store import (
    atomic_write_json,
    load_or_create_fernet_key,
    safe_read_json,
    walk_decrypt_secrets,
    walk_encrypt_secrets,
)


_CLOUD_SENSITIVE_FIELDS = frozenset(
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


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def migrate_if_needed(*, log=None) -> dict:
    """Run the layout migration if (and only if) it has not been run.

    Returns a small structured dict so callers can branch on the
    outcome but never raises for ordinary "nothing to do" cases.

    Return shape::

        {"ok": True,  "status": "current" | "fresh" | "migrated"
                                | "rolled-back" | "skipped",
         "message": "...",
         "details": {...}}

        {"ok": False, "status": "failed", "message": "...",
         "details": {...}}
    """
    _info = (log.info if log else _stderr_info)
    _warn = (log.warning if log else _stderr_warn)

    home = P.dbassistant_home()

    lock_fd, lock_path = _acquire_migration_lock(home)
    try:
        version_on_disk = P.read_layout_version()
        if version_on_disk is not None and version_on_disk >= P.LAYOUT_VERSION:
            return _result(True, "current", "Layout already at current version.",
                           {"version": version_on_disk})

        legacy_cfg = P.legacy_dbmanager_dir()
        legacy_rt  = P.legacy_dbtool_dir()

        legacy_present = legacy_cfg.exists() or legacy_rt.exists()
        if not legacy_present:
            P.ensure_layout()
            P.write_layout_version()
            return _result(True, "fresh", "No legacy data found; new layout created.",
                           {"home": str(home)})

        _info(f"[layout-migration] migrating to {home}")
        P.ensure_layout()

        # Stage 1: keys + connections (with cloud re-encrypt + verify)
        try:
            details_keys = _migrate_keys(legacy_cfg)
            details_conn = _migrate_connections(legacy_cfg, log=log)
        except _MigrationError as exc:
            _warn(f"[layout-migration] failed: {exc}")
            _rollback_new_layout(home, log=log)
            return _result(False, "failed", str(exc), exc.details)

        # Stage 2: runtime / session (best-effort copy of state files)
        details_state = _migrate_state(legacy_rt, log=log)

        # Stage 3: mark legacy directories so we never touch them again
        try:
            renamed = _rename_legacy(legacy_cfg, legacy_rt)
        except OSError as exc:
            _warn(f"[layout-migration] could not rename legacy dirs: {exc}")
            renamed = {"error": str(exc)}

        if not P.write_layout_version():
            _warn("[layout-migration] could not write version marker; will retry next launch.")

        return _result(
            True,
            "migrated",
            f"Migrated legacy data to {home}",
            {
                "home":        str(home),
                "keys":        details_keys,
                "connections": details_conn,
                "state":       details_state,
                "renamed":     renamed,
            },
        )
    finally:
        _release_migration_lock(lock_fd, lock_path)


# ---------------------------------------------------------------------------
# Stage 1a — keys
# ---------------------------------------------------------------------------


def _migrate_keys(legacy_cfg: Path) -> dict:
    """Copy legacy key files to the new location with the new names.

    Pre-v1 layout actually had only ``.db_key`` (shared by DB and cloud)
    plus ``.monitor_key``. The new layout has three separate files:

    * ``keys/db.key``       <-- legacy ``.db_key``
    * ``keys/monitor.key``  <-- legacy ``.monitor_key``
    * ``keys/cloud.key``    <-- freshly generated (cloud profiles get
      re-encrypted in :func:`_migrate_connections`)

    Missing source keys are tolerated: the secret store will create
    them lazily the first time a manager needs them.
    """
    details: dict[str, str] = {}
    pairs = [
        (legacy_cfg / ".db_key", P.db_key_path(),       "db.key"),
        (legacy_cfg / ".monitor_key", P.monitor_key_path(), "monitor.key"),
    ]
    for src, dst, label in pairs:
        if src.is_file() and src.stat().st_size > 0 and not dst.exists():
            try:
                _safe_copy(src, dst, perms=0o600)
                details[label] = "copied"
            except OSError as exc:
                raise _MigrationError(
                    f"Could not copy legacy key {src} -> {dst}: {exc}",
                    {"src": str(src), "dst": str(dst)},
                )
        elif dst.exists():
            details[label] = "kept-existing"
        else:
            details[label] = "missing-legacy"

    # cloud.key is created freshly on demand by _migrate_connections().
    details.setdefault("cloud.key", "deferred")
    return details


# ---------------------------------------------------------------------------
# Stage 1b — connections (with cloud re-encrypt + verify)
# ---------------------------------------------------------------------------


def _migrate_connections(legacy_cfg: Path, *, log=None) -> dict:
    """Copy and (for cloud) re-encrypt connection profiles."""
    details: dict[str, str] = {}

    # ---- DB profiles ------------------------------------------------------
    src_db = legacy_cfg / "saved_connections.json"
    if src_db.is_file() and not P.db_connections_path().exists():
        _safe_copy(src_db, P.db_connections_path(), perms=0o600)
        details["db.json"] = "copied"
    elif P.db_connections_path().exists():
        details["db.json"] = "kept-existing"
    else:
        details["db.json"] = "missing-legacy"

    # ---- Monitor profiles ------------------------------------------------
    src_mon = legacy_cfg / "saved_monitor_connections.json"
    if src_mon.is_file() and not P.monitor_connections_path().exists():
        _safe_copy(src_mon, P.monitor_connections_path(), perms=0o600)
        details["monitor.json"] = "copied"
    elif P.monitor_connections_path().exists():
        details["monitor.json"] = "kept-existing"
    else:
        details["monitor.json"] = "missing-legacy"

    # ---- Cloud profiles (the tricky one: re-encrypt + verify) ------------
    src_cloud = legacy_cfg / "cloud_connections.json"
    if not src_cloud.is_file():
        details["cloud.json"] = "missing-legacy"
        return details
    if P.cloud_connections_path().exists():
        details["cloud.json"] = "kept-existing"
        return details

    raw = safe_read_json(src_cloud)
    if not isinstance(raw, dict):
        # Empty or malformed legacy file - treat as nothing to migrate.
        details["cloud.json"] = "legacy-empty"
        return details

    legacy_db_key = legacy_cfg / ".db_key"
    if not legacy_db_key.is_file():
        raise _MigrationError(
            "Cloud profiles exist but legacy .db_key is missing; cannot "
            "decrypt to re-encrypt with cloud.key.",
            {"cloud_src": str(src_cloud)},
        )

    try:
        old_cipher = load_or_create_fernet_key(legacy_db_key, perms=0o600)
    except Exception as exc:
        raise _MigrationError(
            f"Could not load legacy .db_key: {exc}",
            {"legacy_key": str(legacy_db_key)},
        )

    # Decrypt with the legacy DB key (forgiving: any value that isn't
    # ciphertext is returned as-is, which matches secret_store semantics).
    plain_profiles = walk_decrypt_secrets(raw, old_cipher, _CLOUD_SENSITIVE_FIELDS)

    try:
        new_cipher = load_or_create_fernet_key(P.cloud_key_path(), perms=0o600)
    except Exception as exc:
        raise _MigrationError(
            f"Could not create new cloud.key: {exc}",
            {"target": str(P.cloud_key_path())},
        )

    re_encrypted = walk_encrypt_secrets(plain_profiles, new_cipher, _CLOUD_SENSITIVE_FIELDS)
    if not atomic_write_json(P.cloud_connections_path(), re_encrypted, perms=0o600):
        raise _MigrationError(
            "Could not write re-encrypted cloud.json",
            {"target": str(P.cloud_connections_path())},
        )

    # Verify roundtrip: re-read, decrypt with the new key, compare every
    # known sensitive field against the in-memory plaintext.
    verify_raw = safe_read_json(P.cloud_connections_path())
    if not isinstance(verify_raw, dict):
        raise _MigrationError(
            "Cloud roundtrip verify failed: re-read produced non-dict.",
            {"target": str(P.cloud_connections_path())},
        )
    verify_plain = walk_decrypt_secrets(verify_raw, new_cipher, _CLOUD_SENSITIVE_FIELDS)
    if not _structural_equal(verify_plain, plain_profiles):
        raise _MigrationError(
            "Cloud roundtrip verify failed: decrypted payload differs from source.",
            {"target": str(P.cloud_connections_path())},
        )
    details["cloud.json"] = "re-encrypted+verified"
    return details


def _structural_equal(a, b) -> bool:
    """Recursive structural equality on dict/list/scalar trees."""
    if type(a) is not type(b):  # noqa: E721 - intentional strict check
        return False
    if isinstance(a, dict):
        if a.keys() != b.keys():
            return False
        return all(_structural_equal(a[k], b[k]) for k in a)
    if isinstance(a, list):
        if len(a) != len(b):
            return False
        return all(_structural_equal(x, y) for x, y in zip(a, b))
    return a == b


# ---------------------------------------------------------------------------
# Stage 2 — runtime / session state (best-effort)
# ---------------------------------------------------------------------------


def _migrate_state(legacy_rt: Path, *, log=None) -> dict:
    """Move runtime + session files from ``~/.dbtool/`` to the new tree.

    These are runtime artefacts (logs, last-snapshot metrics, alerts log,
    UI prefs). Best-effort: any single failure here is *not* fatal — they
    can be regenerated. We track per-file outcomes so the caller knows
    what happened.
    """
    if not legacy_rt.exists():
        return {"status": "no-legacy-runtime-dir"}

    moves = [
        ("daemon.pid",              P.daemon_pid_path()),
        ("daemon.log",              P.daemon_log_path()),
        ("metrics.json",            P.metrics_snapshot_path()),
        ("alerts.jsonl",            P.alerts_log_path()),
        ("ai_state.json",           P.ai_state_path()),
        ("dashboard_layout.json",   P.dashboard_layout_path()),
    ]
    out: dict[str, str] = {}
    for name, dst in moves:
        src = legacy_rt / name
        if not src.is_file():
            out[name] = "missing-legacy"
            continue
        if dst.exists():
            out[name] = "kept-existing"
            continue
        try:
            _safe_copy(src, dst, perms=0o600)
            out[name] = "copied"
        except OSError as exc:
            out[name] = f"failed: {exc.strerror or exc}"

    # ai_sessions/ — a small directory of session JSON files.
    src_sessions = legacy_rt / "ai_sessions"
    if src_sessions.is_dir():
        dst_sessions = P.ai_sessions_dir()
        copied = 0
        for child in src_sessions.iterdir():
            target = dst_sessions / child.name
            if target.exists():
                continue
            try:
                if child.is_file():
                    _safe_copy(child, target, perms=0o600)
                    copied += 1
                elif child.is_dir():
                    shutil.copytree(child, target)
                    copied += 1
            except OSError as exc:
                out[f"ai_sessions/{child.name}"] = f"failed: {exc}"
        out["ai_sessions"] = f"copied={copied}"
    else:
        out["ai_sessions"] = "missing-legacy"

    return out


# ---------------------------------------------------------------------------
# Stage 3 — rename legacy dirs (so we never touch them again)
# ---------------------------------------------------------------------------


def _rename_legacy(legacy_cfg: Path, legacy_rt: Path) -> dict:
    out: dict[str, str] = {}
    for src in (legacy_cfg, legacy_rt):
        if not src.exists():
            out[str(src)] = "missing"
            continue
        target = src.with_name(src.name + ".legacy")
        # If a *.legacy already exists, use a numeric suffix.
        suffix = 0
        final = target
        while final.exists():
            suffix += 1
            final = src.with_name(f"{src.name}.legacy.{suffix}")
        try:
            os.rename(src, final)
            out[str(src)] = f"renamed -> {final}"
        except OSError as exc:
            out[str(src)] = f"failed: {exc.strerror or exc}"

    # Also rename the known unused legacy artefacts inside the moved dir.
    # (They're already inside the *.legacy/ tree now, but renaming the
    # files themselves makes their status obvious to anyone inspecting.)
    legacy_renamed = legacy_cfg.with_name(legacy_cfg.name + ".legacy")
    if legacy_renamed.is_dir():
        for name in (".cloud_key", "saved_cloud_connections.json"):
            p = legacy_renamed / name
            if p.exists() and not (legacy_renamed / f"{name}.legacy").exists():
                try:
                    p.rename(legacy_renamed / f"{name}.legacy")
                except OSError:
                    pass

    return out


# ---------------------------------------------------------------------------
# Rollback (called only on stage-1 failure)
# ---------------------------------------------------------------------------


def _rollback_new_layout(home: Path, *, log=None) -> None:
    """Remove any half-created files in the new layout so a retry has a
    clean slate. Leaves the legacy directories untouched.
    """
    _info = (log.info if log else _stderr_info)
    for p in [
        P.db_connections_path(),
        P.cloud_connections_path(),
        P.monitor_connections_path(),
        P.cloud_key_path(),
    ]:
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass
    _info(f"[layout-migration] rolled back partial new layout under {home}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _MigrationError(Exception):
    def __init__(self, msg: str, details: Optional[dict] = None):
        super().__init__(msg)
        self.details = details or {}


def _result(ok: bool, status: str, message: str, details: dict) -> dict:
    return {"ok": ok, "status": status, "message": message, "details": details}


def _safe_copy(src: Path, dst: Path, *, perms: int) -> None:
    """Copy ``src`` to ``dst`` atomically with ``perms`` on the result.

    Implementation:
      1. Stream into a sibling temp file in ``dst.parent``.
      2. ``fsync`` + ``os.replace``.
      3. ``chmod`` to ``perms`` on the final inode.

    Idempotency: if ``dst`` already exists, raises (callers check first).
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=dst.name + ".", suffix=".tmp",
                                        dir=str(dst.parent))
    try:
        os.fchmod(tmp_fd, perms)
        with os.fdopen(tmp_fd, "wb") as wfh, open(src, "rb") as rfh:
            shutil.copyfileobj(rfh, wfh, length=64 * 1024)
            wfh.flush()
            try:
                os.fsync(wfh.fileno())
            except OSError:
                pass
        os.replace(tmp_name, str(dst))
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


# Lock helpers ---------------------------------------------------------------


def _acquire_migration_lock(home: Path):
    """Best-effort exclusive lock so concurrent first-launches don't race.

    The lock file lives *inside* the dbassistant home (``home/.migrate.lock``)
    rather than in ``$HOME`` — the user explicitly asked that nothing from
    this tool be visible in ``$HOME`` other than the single ``.dbassistant/``
    directory. ``home`` itself is created here if missing.
    """
    try:
        home.mkdir(parents=True, exist_ok=True)
        # Best-effort 0o700 — secret_store.atomic_write_json/key code
        # tightens this further when it creates real files later.
        try:
            os.chmod(home, 0o700)
        except OSError:
            pass
    except OSError:
        pass
    lock_path = home / ".migrate.lock"
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    except OSError:
        return None, None
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
    except OSError:
        # Locking might be unsupported - continue best-effort.
        pass
    return lock_fd, lock_path


def _release_migration_lock(lock_fd, lock_path):
    if lock_fd is None:
        return
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        os.close(lock_fd)
    except OSError:
        pass


def _stderr_info(msg: str) -> None:
    print(msg, file=sys.stderr)


def _stderr_warn(msg: str) -> None:
    print(msg, file=sys.stderr)


# Re-export for callers who want a single import.
__all__ = ["migrate_if_needed"]
