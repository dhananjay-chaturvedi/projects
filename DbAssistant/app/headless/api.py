"""
headless/api.py
===============
FastAPI REST API for DbManagementTool — modular master API.

Start with:
    python dbtool.py api [--host 127.0.0.1] [--port 8000]
  or directly:
    uvicorn app.headless.api:app --reload

The API is assembled from a small always-on **core** (connections, SQL editor,
object browser, config, database registry) plus the routers of whichever
**modules** are installed (schema converter, AI query assistant, monitoring).
Missing modules simply contribute no routes; ``GET /api/modules`` reports what
is installed.

Per-module standalone APIs (``python -m monitoring api``, etc.) use the same
core routes plus only that module's router — see ``app.headless.app_factory``.
"""

from __future__ import annotations

from app.headless.app_factory import create_app
from common.core import modules as _modules

app = create_app()
MOUNTED_MODULES = list(_modules.discover().keys())
