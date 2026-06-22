"""Shared connection parameter object for UI, CLI, API, and headless services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ConnectionParams:
    """Normalized database connection form values.

    Defaults mirror the previous optional keyword parameters so callers can omit
    empty SSL/TLS/SSH fields instead of passing ``None`` or ``""`` explicitly.
    """

    name: str
    db_type: str
    host: str
    port: int | str
    service_or_db: str = ""
    username: str = ""
    password: str = ""
    save_password: bool | None = None
    ssl_mode: str | None = None
    ssl_ca: str | None = None
    ssl_cert: str | None = None
    ssl_key: str | None = None
    wallet_location: str | None = None
    tls: bool | None = None
    tls_ca_file: str | None = None
    ssh_tunnel: dict | None = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "ConnectionParams":
        """Build params from either UI/API names or saved-profile names."""
        service_or_db = (
            values.get("service_or_db")
            or values.get("service")
            or values.get("database")
            or ""
        )
        username = values.get("username") or values.get("user") or ""
        return cls(
            name=values.get("name", ""),
            db_type=values.get("db_type", ""),
            host=values.get("host", ""),
            port=values.get("port", ""),
            service_or_db=service_or_db,
            username=username,
            password=values.get("password", "") or "",
            save_password=values.get("save_password"),
            ssl_mode=values.get("ssl_mode") or None,
            ssl_ca=values.get("ssl_ca") or None,
            ssl_cert=values.get("ssl_cert") or None,
            ssl_key=values.get("ssl_key") or None,
            wallet_location=values.get("wallet_location") or None,
            tls=values.get("tls"),
            tls_ca_file=values.get("tls_ca_file") or None,
            ssh_tunnel=values.get("ssh_tunnel"),
        )

    def to_profile(self, *, include_password: bool = True) -> dict:
        """Return the saved connection profile shape used by the manager."""
        profile = {
            "name": self.name,
            "db_type": self.db_type,
            "host": self.host,
            "port": str(self.port),
            "service_or_db": self.service_or_db,
            "username": self.username,
            "password": self.password if include_password else "",
            "save_password": bool(self.save_password),
        }
        for key in (
            "ssl_mode",
            "ssl_ca",
            "ssl_cert",
            "ssl_key",
            "wallet_location",
            "tls",
            "tls_ca_file",
        ):
            value = getattr(self, key)
            if value not in (None, ""):
                profile[key] = value
        if self.ssh_tunnel:
            profile["ssh_tunnel"] = self.ssh_tunnel
        return profile
