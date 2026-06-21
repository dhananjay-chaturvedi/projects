"""Guard: OpenAPI schema (and Swagger/ReDoc UIs) must generate for the full
composite app and every per-module app.

Regression test for the bug where request-body Pydantic models were declared as
local classes inside route-builder functions (``_CfgSet``). Pydantic v2 could
not resolve those forward references, so ``GET /openapi.json`` (and therefore
``/docs`` and ``/redoc``) raised at request time.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from common.headless.app_factory import create_app


def _client(**kw):
    return TestClient(create_app(**kw))


def test_composite_openapi_generates(dbassistant_home):
    c = _client()
    assert c.get("/openapi.json").status_code == 200
    assert c.get("/docs").status_code == 200
    assert c.get("/redoc").status_code == 200


@pytest.mark.parametrize("module_key", ["migrator", "ai", "monitor"])
def test_module_openapi_generates(module_key, dbassistant_home):
    c = _client(module_key=module_key)
    r = c.get("/openapi.json")
    assert r.status_code == 200, r.text[:300]
    assert r.json().get("paths"), "OpenAPI paths should be non-empty"
