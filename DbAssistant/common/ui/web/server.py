"""Serve the standalone Web UI (own backend + SPA) via uvicorn.

The Web UI runs its own server that reads the core service directly. It does
not build or mount the public REST API (``app_factory.create_app``).
"""

from __future__ import annotations

import sys
from typing import Optional


def build_web_app(feature_module: Optional[str] = None):
    """Return the Web UI's standalone FastAPI app (SPA mounted at ``/ui``)."""
    from common.ui.web.backend import build_web_backend

    return build_web_backend(feature_module)


def launch_web_ui(
    feature_module: Optional[str] = None,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> None:
    """Run the standalone Web UI server via uvicorn.

    Host/port default to the shared ``[ui.web]`` properties so the Web UI is
    configured in one place alongside the other UIs.
    """
    try:
        import uvicorn
    except ImportError:
        from common.core import cliutil

        cliutil.err("Web UI requires FastAPI + uvicorn: pip install fastapi uvicorn")
        sys.exit(1)

    from common.ui import shared
    from common.security.http_guards import require_loopback_or_api_key

    host = host or shared.default_web_host()
    port = port or shared.default_web_port()
    ok, message = require_loopback_or_api_key(host)
    if not ok:
        from common.core import cliutil

        cliutil.err(message)
        sys.exit(1)
    if message:
        print(f"[WARN] {message}")

    app = build_web_app(feature_module)
    scope = feature_module or "full"
    print(f"Web UI ({scope}) on http://{host}:{port}/ui/")
    uvicorn.run(app, host=host, port=port)
