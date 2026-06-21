"""
Assemble the **public REST API** app for module-only or full-tool builds.

The route handlers themselves live in :mod:`common.headless.core_routes` (a
neutral HTTP↔service bridge). This module adds everything that makes it the
*public API product*: the OpenAPI docs, CORS, the optional API-key middleware,
and the root redirect to ``/docs``.

The standalone Web UI (:mod:`common.ui.web`) deliberately does NOT import this
module — it builds its own server from ``core_routes`` directly — so deleting
the public API leaves the Web UI working.
"""

from __future__ import annotations

import hmac
import logging
import os
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from common.config_loader import (
    get_api_cors_origins,
    get_api_max_body_bytes,
    get_project_version,
)
from common.core import modules as _modules

# Route glue lives in core_routes; re-export the public names so existing
# imports (`from common.headless.app_factory import ConnectionCreate`, etc.)
# keep working.
from common.headless.core_routes import (  # noqa: F401
    AutocommitRequest,
    ConnectionCreate,
    CsvImportRequest,
    DashboardLayoutRequest,
    FormatSqlRequest,
    MultiQueryRequest,
    QueryRequest,
    SSHTunnelSpec,
    TableExportRequest,
    _composite_full_service,
    _error,
    mount_core_routes,
    mount_module_routers,
)

try:
    from dotenv import load_dotenv as _load_dotenv

    _load_dotenv()
except Exception:
    pass


# Ensure storage layout / pending migration is in place before any service
# instance is constructed (CoreDBService touches ConnectionManager which
# touches the keys/connections subtree).
try:
    from common import paths as _paths

    _paths.bootstrap()
except Exception:
    pass


def _env_int(name: str, default: int, low: int, high: int) -> int:
    try:
        value = int(os.environ.get(name, ""))
    except ValueError:
        return default
    return max(low, min(high, value))


def _public_path(path: str) -> bool:
    return (
        path in {"/", "/api", "/api/health", "/openapi.json"}
        or path.startswith("/docs")
        or path.startswith("/redoc")
    )


def _install_production_middlewares(app: FastAPI) -> None:
    from common.security.http_guards import install_http_guards

    install_http_guards(app)


def _install_unhandled_exception_handler(app: FastAPI) -> None:
    from common.security.http_guards import install_unhandled_exception_handler

    install_unhandled_exception_handler(app)


def create_app(
    *,
    module_key: Optional[str] = None,
    title: Optional[str] = None,
    svc: Any = None,
) -> FastAPI:
    """
    Build a FastAPI application.

    * ``module_key=None`` — core + every installed module router.
    * ``module_key='migrator'|'ai'|'monitor'`` — core + that module only.
    """
    if module_key is not None:
        manifest = _modules.get(module_key)
        default_title = manifest.title if manifest else module_key
        description = f"Core + {default_title} module"
    else:
        default_title = "DbManagementTool API"
        description = "Headless REST API — core + installed modules."

    if svc is None:
        from common.headless.db_service import CoreDBService

        svc = CoreDBService()

    # When running the full app, layer every installed module's bridge over
    # the bare core service so the core routes (e.g. /api/app/clear-caches,
    # /api/connections/{n}/open) can resolve module-side methods like
    # `clear_ai_cache`. Single-module apps stay on the bare core.
    if module_key is None:
        svc = _composite_full_service(svc)

    app = FastAPI(
        title=title or default_title,
        description=description,
        version=get_project_version(),
        docs_url="/docs",
        redoc_url="/redoc",
    )
    origins_raw = os.environ.get("DBTOOL_API_CORS_ORIGINS", get_api_cors_origins()).strip()
    if origins_raw == "*":
        origins = ["*"]
    elif origins_raw:
        origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
    else:
        # No CORS origins configured — default to localhost only (safe).
        # Set DBTOOL_API_CORS_ORIGINS=* or a comma-separated list to allow other origins.
        origins = ["http://localhost", "http://127.0.0.1",
                   "http://localhost:8080", "http://127.0.0.1:8080"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _install_production_middlewares(app)
    _install_unhandled_exception_handler(app)

    mount_core_routes(app, svc)
    mount_module_routers(app, svc, module_key=module_key)
    return app
