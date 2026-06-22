"""Shared driver-level connection option objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from common.drivers.ssl_support import SslParams


@dataclass(frozen=True)
class DriverConnectionParams:
    """Core connection identity used by low-level database drivers."""

    database: str | None = None
    host: str | None = None
    user: str | None = None
    password: str | None = None
    port: int | str | None = None
    ssl: SslParams | None = None
    tls: bool | None = None
    tls_ca_file: str | None = None
    auth_source: str | None = None

    @classmethod
    def from_call(
        cls,
        first: "DriverConnectionParams | None" = None,
        values: Mapping[str, Any] | None = None,
    ) -> "DriverConnectionParams":
        """Coerce params-object calls or keyword calls into DriverConnectionParams."""
        if isinstance(first, cls):
            base = first
            src = values or {}
            return cls(
                database=src.get("database", src.get("db", base.database)),
                host=src.get("host", base.host),
                user=src.get("user", base.user),
                password=src.get("password", base.password),
                port=src.get("port", base.port),
                ssl=SslParams.from_source(src.get("ssl", base.ssl)),
                tls=src.get("tls", base.tls),
                tls_ca_file=src.get("tls_ca_file", base.tls_ca_file),
                auth_source=src.get("auth_source", base.auth_source),
            )

        src = dict(values or {})
        src.setdefault("database", src.get("db"))
        return cls(
            database=src.get("database"),
            host=src.get("host"),
            user=src.get("user"),
            password=src.get("password"),
            port=src.get("port"),
            ssl=SslParams.from_source(src.get("ssl", src)),
            tls=src.get("tls"),
            tls_ca_file=src.get("tls_ca_file"),
            auth_source=src.get("auth_source"),
        )
