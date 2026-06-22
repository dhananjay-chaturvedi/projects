"""
Hardened persistence + symmetric-encryption helpers shared by:

* :class:`common.connection_manager.ConnectionManager` (DB connections)
* :class:`common.cloud.connection_manager.CloudConnectionManager` (cloud profiles)
* :class:`monitoring.monitor_connection_manager.MonitorConnectionManager` (SSH/OS targets)

Why a shared module?
--------------------
Each manager previously rolled its own truncate-then-write, ad-hoc key creation,
and only encrypted top-level fields.  In production that combination produced
three real-world failure modes:

1. **Data-loss on crash** — ``open(path, "w")`` truncates before any lock is
   held.  Any panic before ``json.dump`` finishes left an empty file and
   permanently lost every saved connection / cloud profile.
2. **Key race** — two processes starting concurrently both saw "no key" and
   each generated a different ``Fernet`` key.  Passwords were then encrypted
   with one and undecryptable with the other.
3. **Plaintext secrets at depth** — cloud profiles have a nested
   ``sql_connection.password`` dict; the old encrypter walked only the top
   level, so nested DB passwords were written to disk **in plaintext**.

This module fixes all three with a small, dependency-light surface area:

* :func:`atomic_write_json` — write to a sibling temp file, fsync, then
  ``os.replace``.  Crash-safe.
* :func:`safe_read_json` — shared-lock + parse, returns ``None`` on
  unreadable / corrupted files (callers decide the empty-default shape).
* :func:`load_or_create_fernet_key` — uses ``os.open(O_CREAT|O_EXCL)`` with
  mode ``0o600`` so the key file never exists with looser permissions and a
  losing-race process re-reads the winner's key.
* :func:`encrypt_value` / :func:`decrypt_value` — single-value Fernet+base64
  helpers used by the recursive walkers.
* :func:`walk_encrypt_secrets` / :func:`walk_decrypt_secrets` — depth-first
  scrubbing for any structure (``dict`` of any nesting, ``list`` of dicts,
  etc.) using a configurable set of sensitive keys.

The decrypter is forgiving: values that don't round-trip as ciphertext are
returned as-is so legacy plaintext entries continue to load.  The next save
will rewrite them in ciphertext.
"""

from __future__ import annotations

import base64
import fcntl
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable

from cryptography.fernet import Fernet, InvalidToken


# ---------------------------------------------------------------------------
# Sensitive-field walking
# ---------------------------------------------------------------------------

def _walk(obj: Any, sensitive: Iterable[str], transform):
    """Mutate-in-place walk that applies ``transform(value)`` to every string
    value whose key matches one of ``sensitive`` (case-sensitive).

    Containers traversed: ``dict``, ``list``, ``tuple``.  Other types are
    returned unchanged.  Returns the (possibly new) value so callers can use
    it for both list elements and root-level values.
    """
    sensitive = set(sensitive)
    return _walk_inner(obj, sensitive, transform)


def _walk_inner(obj, sensitive: set, transform):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(v, (dict, list, tuple)):
                out[k] = _walk_inner(v, sensitive, transform)
            elif k in sensitive and isinstance(v, str) and v:
                out[k] = transform(v)
            else:
                out[k] = v
        return out
    if isinstance(obj, list):
        return [_walk_inner(v, sensitive, transform) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_walk_inner(v, sensitive, transform) for v in obj)
    return obj


def walk_encrypt_secrets(obj: Any, cipher: Fernet, sensitive: Iterable[str]) -> Any:
    """Return a deep-copied ``obj`` with every sensitive string encrypted."""
    def enc(v: str) -> str:
        token = encrypt_value(cipher, v)
        return token if token is not None else v
    return _walk(obj, sensitive, enc)


def walk_decrypt_secrets(obj: Any, cipher: Fernet, sensitive: Iterable[str]) -> Any:
    """Return a deep-copied ``obj`` with every sensitive ciphertext decrypted.

    Legacy plaintext values (values that fail to decrypt) are returned
    unchanged so old saves continue to load.
    """
    def dec(v: str) -> str:
        plain = decrypt_value(cipher, v)
        return plain if plain is not None else v
    return _walk(obj, sensitive, dec)


def scrub_for_display(obj: Any, sensitive: Iterable[str], placeholder: str = "***") -> Any:
    """Return a deep-copied ``obj`` with every sensitive value replaced.

    Useful for logging / API responses where callers should never receive
    secrets even when the underlying storage is encrypted.
    """
    sentinel = placeholder
    return _walk(obj, sensitive, lambda _v: sentinel)


# ---------------------------------------------------------------------------
# Fernet helpers
# ---------------------------------------------------------------------------

def encrypt_value(cipher: Fernet, value: str) -> str | None:
    """Encrypt + base64-encode a single string. Returns ``None`` on failure."""
    if value is None or value == "":
        return None
    try:
        token = cipher.encrypt(value.encode("utf-8"))
        return base64.b64encode(token).decode("utf-8")
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[secret_store] encrypt failed: {exc}", file=sys.stderr)
        return None


def decrypt_value(cipher: Fernet, token: str) -> str | None:
    """Decode + decrypt a base64-encoded Fernet token. Returns ``None`` if
    the input doesn't look like ciphertext from this key (callers should
    treat that as legacy plaintext)."""
    if not token:
        return None
    try:
        raw = base64.b64decode(token.encode("utf-8"))
    except Exception:
        return None
    try:
        return cipher.decrypt(raw).decode("utf-8")
    except (InvalidToken, ValueError):
        return None
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[secret_store] decrypt failed: {exc}", file=sys.stderr)
        return None


def load_or_create_fernet_key(key_path: Path, *, perms: int = 0o600) -> Fernet:
    """Load an existing Fernet key from ``key_path`` or create one atomically.

    Concurrency model:

    * A sibling ``.lock`` file is opened with ``LOCK_EX`` so readers and
      creators never observe a half-written key.
    * Inside the lock, if the key already exists we read it; otherwise we
      generate one, write+fsync it under a sibling temp file, then
      ``os.replace`` it into the final path with ``perms`` already set on
      the temp file's fd via ``os.fchmod``.
    * The atomic rename guarantees concurrent processes that take the lock
      *next* see the final key with the right permissions and full content.
    """
    key_path = Path(key_path)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = key_path.with_name(key_path.name + ".lock")
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except OSError:
            # Locking may be unsupported (some networked FS). The atomic
            # rename below still gives us "all-or-nothing" creation.
            pass

        if key_path.exists() and key_path.stat().st_size > 0:
            return Fernet(_read_key(key_path))

        new_key = Fernet.generate_key()
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=key_path.name + ".",
            suffix=".tmp",
            dir=str(key_path.parent),
        )
        try:
            os.fchmod(tmp_fd, perms)
            with os.fdopen(tmp_fd, "wb") as fh:
                fh.write(new_key)
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass
            os.replace(tmp_name, str(key_path))
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
        return Fernet(new_key)
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


def _read_key(key_path: Path) -> bytes:
    with open(key_path, "rb") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Locked / atomic JSON I/O
# ---------------------------------------------------------------------------

def safe_read_json(path: Path) -> Any:
    """Read a JSON file under a shared (``LOCK_SH``) advisory lock.

    Returns ``None`` if the file does not exist, is unreadable, or is not
    valid JSON.  Callers decide the empty-default shape (``[]`` vs ``{}``).
    """
    path = Path(path)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_SH)
            except OSError:
                # Locking might be unsupported (e.g. some networked FS).
                # Falling through to read is acceptable — we'd rather load
                # than refuse.
                pass
            try:
                return json.load(fh)
            finally:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[secret_store] read failed for {path}: {exc}", file=sys.stderr)
        return None


def atomic_write_json(path: Path, obj: Any, *, perms: int = 0o600, indent: int = 2) -> bool:
    """Crash-safe JSON write.

    Implementation:
      1. Serialise to a UTF-8 byte string up-front (failure here doesn't
         touch the target file).
      2. Create a sibling temp file in the same directory with ``perms``.
      3. ``write`` + ``flush`` + ``fsync`` the temp file.
      4. ``os.replace`` over the target (atomic on POSIX/NTFS).
      5. ``fsync`` the parent directory so the rename hits the disk.

    Concurrent writers acquire ``LOCK_EX`` on the **target** file (creating
    it lazily) to serialise renames; the actual rename itself is atomic.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = json.dumps(obj, indent=indent, sort_keys=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        print(f"[secret_store] could not encode JSON for {path}: {exc}", file=sys.stderr)
        return False

    # Acquire an exclusive lock on a side-car lock file so two processes
    # don't both rename over each other.  We use a stable lock-file name
    # ('<target>.lock') so multiple writers contend on the same inode.
    lock_path = path.with_name(path.name + ".lock")
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    except OSError as exc:
        print(f"[secret_store] could not create lock for {path}: {exc}", file=sys.stderr)
        return False

    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
        except OSError:
            # Continue best-effort; rename is still atomic per-call.
            pass

        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".",
            suffix=".tmp",
            dir=str(path.parent),
        )
        try:
            os.fchmod(tmp_fd, perms)
            with os.fdopen(tmp_fd, "wb") as tmp_fh:
                tmp_fh.write(payload)
                tmp_fh.flush()
                try:
                    os.fsync(tmp_fh.fileno())
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
        return True
    except Exception as exc:
        print(f"[secret_store] atomic write failed for {path}: {exc}", file=sys.stderr)
        return False
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)
