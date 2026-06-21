"""Full-tool FastAPI factory — core routes on ``DBService``."""

from __future__ import annotations

from typing import Optional

from common.headless.app_factory import create_app as _create_core_app
from common.headless.app_factory import mount_core_routes, mount_module_routers


def create_app(*, module_key: Optional[str] = None, title: Optional[str] = None, svc=None):
    """Full-tool API: core routes + module routers on ``DBService`` when *svc* omitted."""
    if svc is None:
        from app.headless.db_service import DBService

        svc = DBService()
    return _create_core_app(module_key=module_key, title=title, svc=svc)


__all__ = ["create_app", "mount_core_routes", "mount_module_routers"]
