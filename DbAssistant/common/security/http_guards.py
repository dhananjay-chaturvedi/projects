"""Shared HTTP middleware for API and Web UI servers."""

from __future__ import annotations

import hmac
import logging
import os
from collections.abc import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from common.config_loader import get_api_max_body_bytes


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def default_public_path_checker(path: str) -> bool:
    """Paths that skip API-key auth on the public REST API."""
    return (
        path in {"/", "/api", "/api/health", "/openapi.json"}
        or path.startswith("/docs")
        or path.startswith("/redoc")
    )


def webui_public_path_checker(path: str) -> bool:
    """Paths that skip API-key auth on the standalone Web UI server."""
    if path in {"/", "/api/health"}:
        return True
    if path == "/ui/config":
        return True
    if path.startswith("/ui/apikeys"):
        return False
    if path.startswith("/ui/"):
        return True
    return False


def install_http_guards(
    app: FastAPI,
    *,
    public_path_checker: Callable[[str], bool] | None = None,
) -> None:
    """Install body-size limits and optional API-key authentication."""
    api_key = os.environ.get("DBTOOL_API_KEY", "").strip()
    from common.security import api_keys

    api_key_bytes = api_key.encode("utf-8")
    configured_max_body = get_api_max_body_bytes(default=10 * 1024 * 1024)
    max_body = _env_int(
        "DBTOOL_API_MAX_BODY_BYTES",
        configured_max_body,
        1024,
        100 * 1024 * 1024,
    )
    is_public = public_path_checker or default_public_path_checker

    @app.middleware("http")
    async def _guard_request(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > max_body:
                    return JSONResponse(
                        {"detail": f"Request body too large (max {max_body} bytes)."},
                        status_code=413,
                    )
            except ValueError:
                return JSONResponse({"detail": "Invalid Content-Length header."}, status_code=400)
        elif request.method in ("POST", "PUT", "PATCH"):
            body = await request.body()
            if len(body) > max_body:
                return JSONResponse(
                    {"detail": f"Request body too large (max {max_body} bytes)."},
                    status_code=413,
                )

        if (api_key or api_keys.has_any_key()) and not is_public(request.url.path):
            supplied = request.headers.get("x-api-key", "")
            auth = request.headers.get("authorization", "")
            if auth.lower().startswith("bearer "):
                supplied = auth[7:].strip()
            verified_key = api_keys.verify_token(supplied)
            try:
                supplied_bytes = supplied.encode("utf-8")
            except (AttributeError, UnicodeEncodeError):
                supplied_bytes = b""
            legacy_ok = bool(api_key) and hmac.compare_digest(supplied_bytes, api_key_bytes)
            if not (legacy_ok or verified_key):
                return JSONResponse({"detail": "Unauthorized."}, status_code=401)
            if verified_key:
                request.state.api_key = verified_key
        return await call_next(request)


def install_unhandled_exception_handler(app: FastAPI) -> None:
    @app.exception_handler(Exception)
    async def _unhandled_exception(_request: Request, exc: Exception):
        logging.getLogger(__name__).exception("Unhandled HTTP error", exc_info=exc)
        return JSONResponse({"detail": "Internal server error."}, status_code=500)


def require_loopback_or_api_key(host: str) -> tuple[bool, str]:
    """Return ``(ok, message)`` for non-loopback startup without authentication."""
    from common.security import api_keys

    host_norm = str(host or "127.0.0.1").strip().lower()
    loopback = host_norm in {"127.0.0.1", "localhost", "::1"}
    has_auth = bool(os.environ.get("DBTOOL_API_KEY", "").strip()) or api_keys.has_any_key()
    if not has_auth and not loopback:
        return False, (
            "Refusing to bind on a non-loopback host without an API key. "
            "Run: dbtool apikey create --name admin"
        )
    if not has_auth:
        return True, "Running keyless on loopback only. Create a key before LAN exposure."
    return True, ""
