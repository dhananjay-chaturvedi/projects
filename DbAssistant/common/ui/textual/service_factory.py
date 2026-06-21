"""Build headless service for Textual UI (same contract as CLI/API)."""

from __future__ import annotations

from typing import Any, Optional


def build_service(feature_module: Optional[str] = None) -> Any:
    """
    Return a composite service for the Textual UI.

    * ``feature_module=None`` — full tool (core + all installed modules).
    * ``feature_module='migrator'|'ai'|'monitor'`` — standalone module service.
    """
    if feature_module:
        from common.core.standalone_runner import module_service

        return module_service(feature_module)

    from common.headless.app_factory import _composite_full_service
    from common.headless.db_service import CoreDBService

    return _composite_full_service(CoreDBService())
