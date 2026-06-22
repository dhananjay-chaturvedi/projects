"""
Standalone backend for the Web UI.

This is the Web UI's *own* server. It reads the in-process service layer
(:class:`common.headless.db_service.CoreDBService` / a module composite)
**directly** and serves the bundled SPA. It registers routes via the neutral
:mod:`common.headless.core_routes` glue and the modules' own routers — it does
**not** import :mod:`common.headless.app_factory` (the public REST API), so the
Web UI keeps working even if the public API is removed.

A small ``/ui/config`` endpoint exposes the shared UI properties
(:mod:`common.ui.shared`) so the SPA can render the same title, tab order,
labels and colour theme as the Tk desktop UI.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common.config_loader import get_project_version
from common.headless.core_routes import mount_core_routes, mount_module_routers


def build_service(feature_module: Optional[str] = None) -> Any:
    """Construct the in-process service the Web UI reads from.

    * ``feature_module=None`` — core + every installed module (composite).
    * a module key — core + that one module (composite), mirroring the
      standalone module surface.
    """
    if feature_module is None:
        from common.headless.core_routes import _composite_full_service
        from common.headless.db_service import CoreDBService

        return _composite_full_service(CoreDBService())

    # Standalone module: reuse the shared module-service factory.
    from common.core.standalone_runner import module_service

    return module_service(feature_module)


def _ui_config_payload(feature_module: Optional[str]) -> dict:
    """Shared UI properties for the SPA (title, tabs, theme, fonts)."""
    from common.ui import shared

    tabs = [
        {"id": t.id, "label": t.label, "scope": t.scope, "module": t.module}
        for t in shared.visible_tabs(feature_module)
    ]
    return {
        "title": shared.app_title(feature_module),
        "featureModule": feature_module,
        "version": get_project_version(),
        "advancedModules": shared.advanced_modules_available(),
        "tabs": tabs,
        "theme": shared.theme().as_dict(),
        "fonts": shared.fonts(),
        "specs": shared.specs.as_payload(),
    }


def build_web_backend(feature_module: Optional[str] = None) -> FastAPI:
    """Return the Web UI's standalone FastAPI app (own routes + SPA + config)."""
    from common.ui.web.mount import mount_web_ui

    from common.config_loader import get_api_cors_origins
    from common.security.http_guards import (
        install_http_guards,
        install_unhandled_exception_handler,
        webui_public_path_checker,
    )

    svc = build_service(feature_module)

    # This is a UI server, not an API product: disable the Swagger/ReDoc/OpenAPI
    # surface entirely so opening the host shows the app, never API docs.
    app = FastAPI(
        title="DbManagementTool — Web UI",
        description="Standalone Web UI server (reads the core service directly).",
        version=get_project_version(),
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    # Same-origin SPA; restrict to local origins unless explicitly configured.
    origins_raw = os.environ.get("DBTOOL_API_CORS_ORIGINS", get_api_cors_origins()).strip()
    if origins_raw == "*":
        origins = ["*"]
    elif origins_raw:
        origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
    else:
        origins = [
            "http://localhost",
            "http://127.0.0.1",
            "http://localhost:8090",
            "http://127.0.0.1:8090",
        ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    install_http_guards(app, public_path_checker=webui_public_path_checker)
    install_unhandled_exception_handler(app)

    # Same handlers as the public API, but owned by the Web UI and bound to a
    # service we built ourselves — never via app_factory.create_app. The bare
    # host ("/") opens the SPA instead of any API docs.
    mount_core_routes(app, svc, root_redirect="/ui/")
    mount_module_routers(app, svc, module_key=feature_module)

    @app.get("/ui/config", include_in_schema=False)
    def ui_config():
        return _ui_config_payload(feature_module)

    @app.get("/ui/apikeys", include_in_schema=False)
    def ui_apikeys():
        from common.security import api_keys

        return {"keys": api_keys.list_keys()}

    @app.post("/ui/apikeys", include_in_schema=False)
    def ui_apikey_create(body: dict):
        from common.security import api_keys

        return api_keys.create_key(str((body or {}).get("name") or ""))

    @app.post("/ui/apikeys/{key_id}/revoke", include_in_schema=False)
    def ui_apikey_revoke(key_id: str):
        from common.security import api_keys

        return api_keys.revoke_key(key_id)

    @app.post("/ui/apikeys/{key_id}/regenerate", include_in_schema=False)
    def ui_apikey_regenerate(key_id: str):
        from common.security import api_keys

        return api_keys.regenerate_key(key_id)

    # Serve the SPA at /ui.
    mount_web_ui(app, path="/ui")

    return app
