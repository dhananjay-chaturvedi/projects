"""Resolve project / database scope for isolated capture storage."""

from __future__ import annotations

import hashlib
import re
from typing import Any


def _slug(value: str, *, max_len: int = 48) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip())
    s = s.strip("._-") or "default"
    return s[:max_len]


def resolve_project_id(
    *,
    explicit: str | None = None,
    connection_name: str = "",
    host: str = "",
    database: str = "",
) -> str:
    """Return a stable project id.

    Callers (LlmBuilderAssistant) may pass an explicit id. Otherwise we derive
    one from connection metadata so each customer/project gets its own subtree.
    """
    if explicit and explicit.strip():
        return _slug(explicit.strip())
    seed = "|".join([connection_name, host, database]).lower()
    if not seed.strip("|"):
        return "default"
    digest = hashlib.sha256(seed.encode()).hexdigest()[:16]
    prefix = _slug(connection_name or host or "project", max_len=24)
    return f"{prefix}_{digest}"


def resolve_database_name(db_manager: Any, connection_name: str = "") -> str:
    """Best-effort database/schema name for partitioning."""
    for attr in ("database", "db_name", "service_or_db", "schema"):
        val = getattr(db_manager, attr, None)
        if val:
            return _slug(str(val))
    cfg = getattr(db_manager, "config", None) or {}
    if isinstance(cfg, dict):
        for key in ("database", "db", "service_or_db", "service"):
            if cfg.get(key):
                return _slug(str(cfg[key]))
    return _slug(connection_name or "db")


def capture_scope(
    *,
    project_id: str | None,
    connection_name: str,
    db_manager: Any,
) -> tuple[str, str, str]:
    """Return ``(project_id, connection_slug, database_slug)``."""
    host = getattr(db_manager, "host", "") or ""
    db = resolve_database_name(db_manager, connection_name)
    pid = resolve_project_id(
        explicit=project_id,
        connection_name=connection_name,
        host=str(host),
        database=db,
    )
    return pid, _slug(connection_name or "connection"), db
