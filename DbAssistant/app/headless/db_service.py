"""
app/headless/db_service.py
==========================
Full DB service for the combined tool — core + installed module services.

Single-module builds use ``common/`` + module ``service.py`` instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from common.headless.db_service import CoreDBService

try:
    from monitoring.service import MonitorService

    _MONITORING_OK = True
except Exception:
    _MONITORING_OK = False
    MonitorService = None

try:
    from ai_query.service import AIService

    _AI_OK = True
except Exception:
    _AI_OK = False
    AIService = None

try:
    from schema_converter.bridge import SchemaBridge

    _SCHEMA_OK = True
except Exception:
    _SCHEMA_OK = False
    SchemaBridge = None


class DBService(CoreDBService):
    """Unified service — core methods on self; module methods via delegation."""

    def __init__(self, thresholds_path: str | Path | None = None):
        super().__init__()
        extras: list[Any] = []
        if _MONITORING_OK and MonitorService is not None:
            extras.append(MonitorService(self, thresholds_path))
        if _AI_OK and AIService is not None:
            extras.append(AIService(self))
        if _SCHEMA_OK and SchemaBridge is not None:
            extras.append(SchemaBridge(self))
        self._module_services = tuple(extras)

    def __getattr__(self, name: str):
        for svc in self._module_services:
            if hasattr(svc, name):
                return getattr(svc, name)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
