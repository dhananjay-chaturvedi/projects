"""Mount the static web UI onto an existing FastAPI app."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_STATIC_DIR = Path(__file__).resolve().parent / "static"


def static_dir() -> Path:
    """Absolute path to the bundled static assets."""
    return _STATIC_DIR


def mount_web_ui(app: Any, *, path: str = "/ui") -> None:
    """
    Mount the SPA at *path* on an existing FastAPI/Starlette ``app``.

    The API itself is untouched; this only adds a static file route. Safe to
    call multiple times (idempotent on the same mount path).
    """
    from fastapi.staticfiles import StaticFiles

    if not _STATIC_DIR.is_dir():
        raise FileNotFoundError(f"Web UI assets missing: {_STATIC_DIR}")

    # Avoid double-mount if called twice.
    for route in getattr(app, "routes", []):
        if getattr(route, "path", None) == path or getattr(route, "name", "") == "web_ui":
            return

    app.mount(
        path,
        StaticFiles(directory=str(_STATIC_DIR), html=True),
        name="web_ui",
    )
