"""Thin adapter so MonitorService can poll already-active GUI connections."""

from __future__ import annotations

import threading
from typing import Callable


class ActiveConnectionsMonitorAdapter:
    """Minimal CoreDBService surface for dashboard metric polling."""

    def __init__(
        self,
        get_active_connections: Callable[[], dict],
        get_saved_connections: Callable[[], list],
    ):
        self._get_active = get_active_connections
        self._get_saved = get_saved_connections
        self._locks: dict[str, threading.Lock] = {}
        self._meta = threading.Lock()

    def get_connection_profile(self, name: str) -> dict | None:
        for row in self._get_saved() or []:
            if row.get("name") == name:
                return dict(row)
        mgr = (self._get_active() or {}).get(name)
        if not mgr:
            return None
        params = getattr(mgr, "_last_connect_params", None) or {}
        return {
            "name": name,
            "db_type": getattr(mgr, "db_type", ""),
            "host": params.get("host", "localhost"),
            "username": params.get("username", ""),
            "service_or_db": params.get("database") or params.get("service") or "",
        }

    def get_manager(self, name: str, profile: dict | None = None):
        mgr = (self._get_active() or {}).get(name)
        if not mgr or not getattr(mgr, "conn", None):
            raise ConnectionError(f"Connection '{name}' is not active.")
        return mgr

    def connection_lock(self, name: str) -> threading.Lock:
        with self._meta:
            if name not in self._locks:
                self._locks[name] = threading.Lock()
            return self._locks[name]
