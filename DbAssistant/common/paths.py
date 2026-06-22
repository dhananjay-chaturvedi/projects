"""
Central path resolver for the DB Assistant tool.

Single source of truth for **where the tool stores its data on disk.**
The directory layout is intentionally non-configurable via ``config.ini``
so users (and bugs) cannot accidentally fragment the storage. The only
escape hatch is the ``DBASSISTANT_HOME`` environment variable, which
exists for Docker containers, systemd services, and pytest fixtures.

Layout (under :func:`dbassistant_home`)
---------------------------------------

::

    ~/.dbassistant/
        version                          # layout schema marker
        keys/
            db.key                       # encrypts saved DB profiles
            cloud.key                    # encrypts saved cloud profiles
            monitor.key                  # encrypts monitor/SSH profiles
        connections/
            db.json                      # saved DB connection profiles
            cloud.json                   # saved cloud connection profiles
            monitor.json                 # saved monitor/SSH profiles
            monitor_db.json              # Monitor-tab-only DB profiles (isolated)
        runtime/                         # overridable via [paths] runtime_dir
            daemon.log
            daemon.pid
            metrics.json
            alerts.jsonl
        session/                         # overridable via [paths] session_dir
            ai_state.json
            dashboard_layout.json
            ai_sessions/sessions.json

Public surface
--------------

* :func:`dbassistant_home`           - root directory
* :func:`keys_dir` / :func:`db_key_path` / :func:`cloud_key_path` /
  :func:`monitor_key_path`
* :func:`connections_dir` / :func:`db_connections_path` /
  :func:`cloud_connections_path` / :func:`monitor_connections_path`
* :func:`runtime_dir` / :func:`session_dir`        (config-overridable)
* :func:`daemon_pid_path` / :func:`daemon_log_path` /
  :func:`metrics_snapshot_path` / :func:`alerts_log_path` /
  :func:`ai_state_path` / :func:`dashboard_layout_path` /
  :func:`ai_sessions_dir`
* :func:`ensure_layout` - create the directory tree with safe permissions
* :data:`LAYOUT_VERSION` / :func:`read_layout_version` /
  :func:`write_layout_version`
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


LAYOUT_VERSION = 1

_DEFAULT_DIR_NAME = ".dbassistant"
_DIR_PERMS = 0o700
_FILE_PERMS = 0o600


# ---------------------------------------------------------------------------
# Root resolver
# ---------------------------------------------------------------------------


def dbassistant_home() -> Path:
    """Return the root storage directory for the tool.

    Resolution order:

    1. ``$DBASSISTANT_HOME`` (if set and non-empty). Tilde and env-vars
       are expanded. This is the **only** way to relocate the tool's
       storage; it is intended for Docker, systemd, and tests.
    2. ``~/.dbassistant`` (default for normal users).

    The returned path is resolved (symlinks followed) so callers receive
    a stable absolute path.
    """
    override = os.environ.get("DBASSISTANT_HOME", "").strip()
    if override:
        return Path(os.path.expanduser(os.path.expandvars(override))).resolve()
    return (Path.home() / _DEFAULT_DIR_NAME).resolve()


# ---------------------------------------------------------------------------
# Always-hardcoded subdirectories (keys + connections)
# ---------------------------------------------------------------------------


def keys_dir() -> Path:
    return dbassistant_home() / "keys"


def db_key_path() -> Path:
    return keys_dir() / "db.key"


def cloud_key_path() -> Path:
    return keys_dir() / "cloud.key"


def monitor_key_path() -> Path:
    return keys_dir() / "monitor.key"


def notifications_key_path() -> Path:
    """Fernet key encrypting notification secrets (Teams webhook, SMTP pwd)."""
    return keys_dir() / "notifications.key"


def notifications_secrets_path() -> Path:
    """Encrypted store for notification secrets (webhook URL, SMTP password)."""
    return dbassistant_home() / "notifications.json"


def connections_dir() -> Path:
    return dbassistant_home() / "connections"


def db_connections_path() -> Path:
    return connections_dir() / "db.json"


def cloud_connections_path() -> Path:
    return connections_dir() / "cloud.json"


def monitor_connections_path() -> Path:
    return connections_dir() / "monitor.json"


def monitor_db_connections_path() -> Path:
    """Saved DB connection profiles owned by the Monitoring module.

    Stored separately from the core ``db.json`` so connections added from the
    Monitor tab are isolated — visible only inside Monitoring, never to the
    SQL Editor / Data Migration / AI Query tabs (which read ``db.json``).
    """
    return connections_dir() / "monitor_db.json"


# ---------------------------------------------------------------------------
# Runtime + session directories (config-overridable)
# ---------------------------------------------------------------------------


def _config_override(key: str) -> Optional[Path]:
    """Read an optional override from ``config.ini`` ``[paths]`` ``key``.

    Returns ``None`` when the key is missing or empty. Imports lazily so
    this module remains usable from inside ``common.config_loader``.
    """
    try:
        from common.config_loader import config

        raw = config.get("paths", key, "").strip()
        if not raw:
            return None
        return Path(os.path.expanduser(os.path.expandvars(raw))).resolve()
    except Exception:
        return None


def runtime_dir() -> Path:
    """Resolve the runtime/state directory.

    Override via ``[paths] runtime_dir`` in ``config.ini``. Default is
    ``<dbassistant_home>/runtime``.
    """
    override = _config_override("runtime_dir")
    return override if override is not None else dbassistant_home() / "runtime"


def session_dir() -> Path:
    """Resolve the per-session preferences directory.

    Override via ``[paths] session_dir`` in ``config.ini``. Default is
    ``<dbassistant_home>/session``.
    """
    override = _config_override("session_dir")
    return override if override is not None else dbassistant_home() / "session"


def daemon_pid_path() -> Path:
    return runtime_dir() / "daemon.pid"


def daemon_log_path() -> Path:
    return runtime_dir() / "daemon.log"


def metrics_snapshot_path() -> Path:
    return runtime_dir() / "metrics.json"


def alerts_log_path() -> Path:
    return runtime_dir() / "alerts.jsonl"


def ai_state_path() -> Path:
    return session_dir() / "ai_state.json"


def dashboard_layout_path() -> Path:
    return session_dir() / "dashboard_layout.json"


def ai_sessions_dir() -> Path:
    return session_dir() / "ai_sessions"


def ai_assistant_home() -> Path:
    """Root for ai_assistant data (capture, training sets, built apps)."""
    return dbassistant_home() / "ai_assistant"


def ai_capture_dir() -> Path:
    """Isolated per-project/per-db capture store for LLM training."""
    return ai_assistant_home() / "capture"


def app_builder_dir() -> Path:
    """Generated app workspaces and build artifacts."""
    return ai_assistant_home() / "app_builder"


def exports_dir() -> Path:
    """Sandbox for table export/import file paths."""
    return dbassistant_home() / "exports"


# ---------------------------------------------------------------------------
# Layout management
# ---------------------------------------------------------------------------


def _ensure_dir(path: Path) -> None:
    """Create ``path`` (and parents) if missing, with ``0o700`` perms.

    Existing directories are left untouched; we don't tighten perms on
    user-created paths under ``runtime_dir`` / ``session_dir`` that they
    may have chosen to share.
    """
    if path.exists():
        return
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, _DIR_PERMS)
    except OSError:
        # Best-effort; some filesystems (NFS, FAT, mounted volumes) don't
        # support POSIX modes. The atomic-write helpers will still set
        # 0o600 on the actual key/secret files themselves.
        pass


def ensure_layout() -> Path:
    """Create the full directory tree and return :func:`dbassistant_home`.

    Idempotent. Always returns the absolute root path even when called
    repeatedly. Subdirectories under runtime/session that the user has
    redirected to a different volume are created at the configured
    location.
    """
    home = dbassistant_home()
    _ensure_dir(home)
    _ensure_dir(keys_dir())
    _ensure_dir(connections_dir())
    _ensure_dir(runtime_dir())
    _ensure_dir(session_dir())
    _ensure_dir(ai_sessions_dir())
    _ensure_dir(exports_dir())
    return home


def read_layout_version() -> Optional[int]:
    """Return the layout schema version stored on disk, or ``None``."""
    marker = dbassistant_home() / "version"
    if not marker.is_file():
        return None
    try:
        raw = marker.read_text(encoding="utf-8").strip()
        return int(raw)
    except (OSError, ValueError):
        return None


def write_layout_version(version: int = LAYOUT_VERSION) -> bool:
    """Write the layout schema marker. Returns ``True`` on success."""
    marker = dbassistant_home() / "version"
    try:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(f"{int(version)}\n", encoding="utf-8")
        try:
            os.chmod(marker, _FILE_PERMS)
        except OSError:
            pass
        return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Legacy locations (used only by the migrator)
# ---------------------------------------------------------------------------


def legacy_dbmanager_dir() -> Path:
    """Pre-v1 location of saved connections + key files."""
    return Path.home() / ".dbmanager"


def legacy_dbtool_dir() -> Path:
    """Pre-v1 location of runtime/state/session files."""
    return Path.home() / ".dbtool"


# ---------------------------------------------------------------------------
# Bootstrap (lazy, idempotent migration entry point)
# ---------------------------------------------------------------------------


_BOOTSTRAPPED = False
_BOOTSTRAP_RESULT: Optional[dict] = None


def bootstrap(*, force: bool = False) -> dict:
    """Ensure the storage layout exists and any pending migration has run.

    Safe to call from any number of import sites; the work runs at most
    once per Python process (set ``force=True`` to override, e.g. in
    tests after switching ``DBASSISTANT_HOME``).

    Failures here are deliberately *non-fatal*: we log via stderr but
    still return a structured result so the caller can decide. Letting
    a healthy ``ConnectionManager`` instantiate against a partially
    initialised tree is safer than dying at import time.
    """
    global _BOOTSTRAPPED, _BOOTSTRAP_RESULT
    if _BOOTSTRAPPED and not force:
        return _BOOTSTRAP_RESULT or {"ok": True, "status": "cached"}

    try:
        from common.layout_migration import migrate_if_needed

        result = migrate_if_needed()
    except Exception as exc:
        # Never let bootstrap take down the process: keep the new layout
        # in a usable state via ensure_layout() and report the failure.
        try:
            ensure_layout()
        except Exception:
            pass
        result = {
            "ok": False,
            "status": "exception",
            "message": f"bootstrap failed: {exc}",
            "details": {},
        }

    _BOOTSTRAPPED = True
    _BOOTSTRAP_RESULT = result
    return result


def reset_bootstrap_state_for_tests() -> None:
    """Allow tests to re-run :func:`bootstrap` after switching env vars."""
    global _BOOTSTRAPPED, _BOOTSTRAP_RESULT
    _BOOTSTRAPPED = False
    _BOOTSTRAP_RESULT = None
