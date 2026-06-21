"""Safe path resolution helpers for user-supplied names and file paths."""

from __future__ import annotations

import re
from pathlib import Path

class PathEscapeError(ValueError):
    """Raised when a user-supplied path would escape its sandbox root."""


_UNSAFE_SEGMENT_RE = re.compile(r"[<>:\"|?*\x00]")


def assert_safe_name(name: str, *, label: str = "name") -> str:
    """Return a single safe path segment (no separators or ``..``)."""
    s = (name or "").strip()
    if not s or s in {".", ".."}:
        raise PathEscapeError(f"Invalid {label}.")
    norm = s.replace("\\", "/")
    if ".." in norm.split("/"):
        raise PathEscapeError(f"Invalid {label}: path traversal is not allowed.")
    if "/" in norm or norm.startswith("~"):
        raise PathEscapeError(f"Invalid {label}: must be a single name segment.")
    if _UNSAFE_SEGMENT_RE.search(s):
        raise PathEscapeError(f"Invalid {label}: contains forbidden characters.")
    return s


def assert_safe_relative_file(path: str, *, label: str = "path") -> str:
    """Return a normalized relative file path safe to join under a workspace root."""
    raw = (path or "").strip().replace("\\", "/").lstrip("/")
    if not raw or raw.startswith("../") or "/../" in raw or raw.endswith("/.."):
        raise PathEscapeError(f"Invalid {label}: path traversal is not allowed.")
    parts = [p for p in raw.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise PathEscapeError(f"Invalid {label}: path traversal is not allowed.")
    if any(_UNSAFE_SEGMENT_RE.search(p) for p in parts):
        raise PathEscapeError(f"Invalid {label}: contains forbidden characters.")
    return "/".join(parts)


def resolve_under(root: Path, *parts: str) -> Path:
    """Resolve *parts* under *root* or raise :class:`PathEscapeError`."""
    base = Path(root).expanduser().resolve()
    candidate = base.joinpath(*parts).resolve() if parts else base
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise PathEscapeError("Path must stay within the allowed directory.") from exc
    return candidate


def resolve_user_path(root: Path, user_path: str) -> Path:
    """Resolve a user path relative to *root* unless already absolute under it."""
    base = Path(root).expanduser().resolve()
    raw = Path((user_path or "").strip())
    candidate = (raw if raw.is_absolute() else base / raw).expanduser().resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise PathEscapeError("Path must stay within the allowed directory.") from exc
    return candidate
