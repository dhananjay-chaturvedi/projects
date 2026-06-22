"""Cross-process concurrency primitives for shared on-disk state.

Multiple surfaces (Tk / Textual / Web UIs, CLI, FastAPI headless API, and the
monitoring daemon) run as independent threads *and* processes, and several of
them write the same JSON / JSONL / INI files. Plain ``open(..., "w")`` or
``write_text`` from two writers races: torn files and — more often — lost
updates (read-modify-write where the last writer clobbers the other's change).

This module centralises the fix used throughout the tool:

* :func:`file_lock` — a cross-process advisory lock keyed on a sidecar
  ``<target>.lock`` file (the same convention used by
  :func:`common.secret_store.atomic_write_json`, so they interoperate).
* :func:`update_json_locked` — lock + read + mutate + atomic replace, which
  closes the lost-update window for read-modify-write JSON files.
* :func:`append_jsonl_locked` — locked append for shared append-only logs.
* :func:`atomic_write_text` — crash-safe text write (temp + fsync + replace).

The lock is advisory and best-effort: on filesystems without ``flock`` (some
networked mounts) the atomic rename still prevents torn files, and the lock
simply degrades to a no-op rather than failing the operation.
"""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

try:  # POSIX advisory locks; absent on some platforms.
    import fcntl
except ImportError:  # pragma: no cover - Windows / unusual platforms
    fcntl = None  # type: ignore[assignment]


def _lock_path(target: Path) -> Path:
    return target.with_name(target.name + ".lock")


@contextmanager
def file_lock(target: str | Path, *, shared: bool = False) -> Iterator[None]:
    """Hold a cross-process advisory lock for *target* for the block's duration.

    The lock is taken on a sibling ``<target>.lock`` file (matching
    :func:`common.secret_store.atomic_write_json`). Use ``shared=True`` for
    read-only critical sections that may run concurrently with each other but
    must exclude writers.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_file = _lock_path(target)
    fd = os.open(str(lock_file), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
            except OSError:
                pass  # locking unsupported here; proceed best-effort
        yield
    finally:
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
        os.close(fd)


def _atomic_replace_bytes(path: Path, payload: bytes, *, perms: int = 0o600) -> None:
    """Write *payload* to *path* via temp file + fsync + ``os.replace``.

    Does **not** take a lock — callers that need read-modify-write safety must
    already hold :func:`file_lock` for *path*.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        os.fchmod(tmp_fd, perms)
        with os.fdopen(tmp_fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp_name, str(path))
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    try:
        dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
    except OSError:
        dir_fd = None
    if dir_fd is not None:
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)


def atomic_write_text(path: str | Path, text: str, *, perms: int = 0o600,
                      lock: bool = True) -> None:
    """Crash-safe text write. Holds :func:`file_lock` unless ``lock=False``."""
    path = Path(path)
    payload = text.encode("utf-8")
    if lock:
        with file_lock(path):
            _atomic_replace_bytes(path, payload, perms=perms)
    else:
        _atomic_replace_bytes(path, payload, perms=perms)


def read_json(path: str | Path, default: Any = None) -> Any:
    """Best-effort JSON read (no lock). Returns *default* on any failure."""
    path = Path(path)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def update_json_locked(
    path: str | Path,
    mutator: Callable[[Any], Any],
    *,
    default: Any = None,
    perms: int = 0o600,
    indent: int = 2,
) -> Any:
    """Atomically read-modify-write a JSON file under an exclusive lock.

    *mutator* receives the current value (or *default* if the file is missing
    or unreadable) and returns the new value to persist. The whole
    read→mutate→write happens inside one lock, so concurrent callers never lose
    each other's updates. Returns the persisted value.
    """
    path = Path(path)
    with file_lock(path):
        current = read_json(path, default)
        new_value = mutator(current)
        _atomic_replace_bytes(
            path, json.dumps(new_value, indent=indent).encode("utf-8"), perms=perms)
        return new_value


def append_jsonl_locked(path: str | Path, records: Iterable[dict]) -> int:
    """Append JSON records (one per line) under an exclusive lock.

    Returns the number of records written. Safe for many concurrent appenders.
    """
    path = Path(path)
    rows = [r for r in records]
    if not rows:
        return 0
    blob = "".join(json.dumps(r) + "\n" for r in rows)
    with file_lock(path):
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(blob)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
    return len(rows)
