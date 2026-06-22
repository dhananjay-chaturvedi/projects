"""
Shared SSL/TLS connection option helpers for SQL drivers.

Connection profiles and UI use a common shape:
  ssl_mode, ssl_ca, ssl_cert, ssl_key, wallet_location (Oracle)

Each driver maps these to its native parameters. When all are empty/disabled,
connect behaviour is unchanged (plain TCP).
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class SslParams:
    """Unified SSL/TLS connection options shared by SQL drivers.

    Groups the ``ssl_mode``/``ssl_ca``/``ssl_cert``/``ssl_key``/``wallet_location``
    quad into a single value object so driver ``connect``/``reconnect`` helpers
    accept one ``ssl=`` argument instead of four loose parameters. All fields
    default to ``None`` (plain TCP), so passing no SSL info is unchanged.
    """

    ssl_mode: str | None = None
    ssl_ca: str | None = None
    ssl_cert: str | None = None
    ssl_key: str | None = None
    wallet_location: str | None = None

    @classmethod
    def from_source(cls, source: "SslParams | Mapping[str, Any] | None") -> "SslParams":
        """Coerce an existing :class:`SslParams`, a mapping, or ``None``.

        Lets drivers keep accepting SSL fields via leftover ``**kwargs`` (the
        path used by :class:`common.db_manager.DatabaseManager`) while still
        exposing a single typed ``ssl=`` parameter to direct callers.
        """
        if isinstance(source, cls):
            return source
        src: Mapping[str, Any] = source or {}
        return cls(
            ssl_mode=src.get("ssl_mode"),
            ssl_ca=src.get("ssl_ca"),
            ssl_cert=src.get("ssl_cert"),
            ssl_key=src.get("ssl_key"),
            wallet_location=src.get("wallet_location"),
        )


def _clean_path(value: str | None) -> str | None:
    if not value:
        return None
    path = str(value).strip()
    return path if path else None


def ssl_enabled(
    ssl_mode: str | None = None,
    ssl_ca: str | None = None,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
    wallet_location: str | None = None,
) -> bool:
    mode = (ssl_mode or "").strip().lower()
    if mode in ("disable", "disabled", "off", ""):
        return bool(
            _clean_path(ssl_ca)
            or _clean_path(ssl_cert)
            or _clean_path(ssl_key)
            or _clean_path(wallet_location)
        )
    return True


def mysql_ssl_connect_kwargs(
    ssl_mode: str | None = None,
    ssl_ca: str | None = None,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
) -> dict[str, Any]:
    """mysql-connector-python SSL options."""
    if not ssl_enabled(ssl_mode, ssl_ca, ssl_cert, ssl_key):
        return {}

    mode = (ssl_mode or "require").strip().lower()
    kwargs: dict[str, Any] = {"ssl_disabled": False}

    ca = _clean_path(ssl_ca)
    cert = _clean_path(ssl_cert)
    key = _clean_path(ssl_key)

    if ca:
        kwargs["ssl_ca"] = ca
    if cert:
        kwargs["ssl_cert"] = cert
    if key:
        kwargs["ssl_key"] = key

    if mode in ("verify_ca", "verify-ca", "verify_full", "verify-full"):
        kwargs["ssl_verify_cert"] = True
        kwargs["ssl_verify_identity"] = mode.endswith("full")
    elif mode == "require":
        kwargs["ssl_verify_cert"] = bool(ca)

    return kwargs


def postgres_ssl_connect_kwargs(
    ssl_mode: str | None = None,
    ssl_ca: str | None = None,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
) -> dict[str, Any]:
    """psycopg2 libpq SSL options."""
    if not ssl_enabled(ssl_mode, ssl_ca, ssl_cert, ssl_key):
        return {}

    mode = (ssl_mode or "require").strip().lower()
    if mode == "verify_ca":
        mode = "verify-ca"

    kwargs: dict[str, Any] = {"sslmode": mode}

    ca = _clean_path(ssl_ca)
    cert = _clean_path(ssl_cert)
    key = _clean_path(ssl_key)
    if ca:
        kwargs["sslrootcert"] = ca
    if cert:
        kwargs["sslcert"] = cert
    if key:
        kwargs["sslkey"] = key

    return kwargs


def sqlserver_encryption_value(ssl_mode: str | None) -> str | None:
    """Map unified ssl_mode to pymssql encryption= values."""
    mode = (ssl_mode or "").strip().lower()
    mapping = {
        "disable": "off",
        "disabled": "off",
        "off": "off",
        "prefer": "request",
        "request": "request",
        "require": "require",
        "verify_ca": "require",
        "verify-ca": "require",
        "verify_full": "require",
        "verify-full": "require",
    }
    return mapping.get(mode)


def sqlserver_ssl_connect_kwargs(ssl_mode: str | None = None, **_ignored) -> dict[str, Any]:
    """pymssql encryption parameter (CA trust is via OS / FreeTDS config)."""
    enc = sqlserver_encryption_value(ssl_mode)
    if not enc or enc == "off":
        if not ssl_enabled(ssl_mode):
            return {}
    if enc:
        return {"encryption": enc}
    return {"encryption": "request"} if ssl_enabled(ssl_mode) else {}


def oracle_ssl_connect_kwargs(
    host: str,
    port: int | str,
    service_name: str,
    ssl_mode: str | None = None,
    ssl_ca: str | None = None,
    wallet_location: str | None = None,
) -> dict[str, Any]:
    """
    oracledb / cx_Oracle TCPS options.

    Returns extra connect() kwargs and optionally a TCPS DSN override.
    """
    if not ssl_enabled(ssl_mode, ssl_ca, wallet_location=wallet_location):
        return {}

    wallet = _clean_path(wallet_location)
    ca = _clean_path(ssl_ca)
    extra: dict[str, Any] = {}

    if wallet and os.path.isdir(wallet):
        extra["wallet_location"] = wallet
        extra["config_dir"] = wallet

    if ca and os.path.isfile(ca):
        ctx = ssl.create_default_context(cafile=ca)
        extra["ssl_context"] = ctx

    mode = (ssl_mode or "require").strip().lower()
    if mode not in ("disable", "disabled", "off"):
        extra["dsn"] = (
            f"(DESCRIPTION="
            f"(RETRY_COUNT=3)"
            f"(ADDRESS=(PROTOCOL=TCPS)(HOST={host})(PORT={int(port)}))"
            f"(CONNECT_DATA=(SERVICE_NAME={service_name}))"
            f"))"
        )

    return extra
