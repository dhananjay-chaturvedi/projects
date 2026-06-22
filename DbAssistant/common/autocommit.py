"""Shared autocommit helpers for SQL drivers, UI, CLI, and API."""

from __future__ import annotations

from typing import Any

from common.config_loader import config


def default_autocommit() -> bool:
    """Return the configured default autocommit mode for new connections."""
    return config.get_bool("database.connection", "default_autocommit", default=False)


def get_autocommit(conn: Any, db_type: str) -> bool:
    """Return the live autocommit state for *conn*."""
    if db_type == "SQLite":
        return getattr(conn, "isolation_level", "") is None
    if db_type == "SQLServer":
        return bool(getattr(conn, "autocommit_state", False))
    return bool(getattr(conn, "autocommit", False))


def set_autocommit(conn: Any, db_type: str, enabled: bool) -> None:
    """Set autocommit for *conn*, handling driver-specific APIs safely."""
    enabled = bool(enabled)

    if db_type == "SQLite":
        conn.isolation_level = None if enabled else "DEFERRED"
        return

    if db_type == "SQLServer":
        conn.autocommit(enabled)
        return

    if db_type == "PostgreSQL" and enabled:
        # psycopg2 refuses to change autocommit while an implicit transaction is
        # open. Metadata SELECTs can open one before the user touches the toggle.
        try:
            conn.rollback()
        except Exception:
            pass

    conn.autocommit = enabled
