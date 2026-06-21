"""FastAPI headless API tests."""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from common.headless.app_factory import create_app


class _DummySvc:
    def list_connections(self):
        return []

    def add_connection(self, _params):
        return {"ok": True, "message": "saved"}

    def execute(self, connection, sql):
        return {"error": None, "columns": ["x"], "rows": [["1"]], "rowcount": 1}

    def show_config(self, section=None):
        return {"error": None, "sections": {}}

    def list_db_types(self):
        return []

    def list_db_ops(self, type):
        return []


@pytest.fixture
def client(monkeypatch):
    """Hermetic client: tests must not depend on the dev's ``.env`` state.

    Clears ``DBTOOL_API_KEY`` and builds a fresh app so basic route smoke
    tests don't 401 just because the local environment has a real key
    configured.
    """
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    return TestClient(create_app(svc=_DummySvc()))


def test_health_endpoint(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert "status" in data or "ok" in str(data).lower()


def test_list_connections(client):
    r = client.get("/api/connections")
    assert r.status_code == 200


def test_api_key_required_when_configured(monkeypatch):
    monkeypatch.setenv("DBTOOL_API_KEY", "secret")
    c = TestClient(create_app(svc=_DummySvc()))
    assert c.get("/api/health").status_code == 200
    assert c.get("/api/connections").status_code == 401
    assert c.get("/api/connections", headers={"X-API-Key": "secret"}).status_code == 200
    assert c.get("/api/connections", headers={"Authorization": "Bearer secret"}).status_code == 200


def test_api_key_uses_constant_time_compare(monkeypatch):
    """Wrong keys (incl. length mismatches and empty values) must all 401,
    and the comparison must go through hmac.compare_digest so it's safe
    against timing side-channels."""
    import hmac as _hmac

    calls = {"n": 0}
    real_compare = _hmac.compare_digest

    def _spy(a, b):
        calls["n"] += 1
        # Both arguments must be bytes-likes of the same type — proves we
        # encoded properly before comparing.
        assert isinstance(a, (bytes, bytearray)) and isinstance(b, (bytes, bytearray))
        return real_compare(a, b)

    monkeypatch.setattr("common.headless.app_factory.hmac.compare_digest", _spy)
    monkeypatch.setenv("DBTOOL_API_KEY", "s3cr3t-LONG-key-value")
    c = TestClient(create_app(svc=_DummySvc()))

    assert c.get("/api/connections").status_code == 401
    assert c.get("/api/connections", headers={"X-API-Key": ""}).status_code == 401
    assert c.get("/api/connections", headers={"X-API-Key": "x"}).status_code == 401
    assert c.get("/api/connections", headers={"X-API-Key": "s3cr3t-LONG-key-valuE"}).status_code == 401
    assert c.get("/api/connections", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert c.get(
        "/api/connections", headers={"X-API-Key": "s3cr3t-LONG-key-value"}
    ).status_code == 200
    assert c.get(
        "/api/connections", headers={"Authorization": "Bearer s3cr3t-LONG-key-value"}
    ).status_code == 200

    # /api/health is public and must never invoke the comparison.
    pre = calls["n"]
    assert c.get("/api/health").status_code == 200
    assert calls["n"] == pre
    assert pre >= 7  # one per protected request above


def test_request_body_size_limit(monkeypatch):
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    monkeypatch.setenv("DBTOOL_API_MAX_BODY_BYTES", "10")
    c = TestClient(create_app(svc=_DummySvc()))
    r = c.post(
        "/api/query",
        json={"connection": "c", "sql": "SELECT " + "x" * 2000},
    )
    assert r.status_code == 413


def test_query_payload_validation_rejects_empty_sql(monkeypatch):
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    c = TestClient(create_app(svc=_DummySvc()))
    r = c.post("/api/query", json={"connection": "c", "sql": ""})
    assert r.status_code == 422


class _ExplodingSvc(_DummySvc):
    def execute(self, connection, sql):
        raise RuntimeError("unexpected service failure")


class _NotFoundSvc(_DummySvc):
    def remove_connection(self, name):
        return {"ok": False, "message": "Connection not found"}


def test_unhandled_service_exception_returns_normalized_500(monkeypatch):
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    monkeypatch.delenv("DBTOOL_DEBUG", raising=False)
    c = TestClient(create_app(svc=_ExplodingSvc()), raise_server_exceptions=False)
    r = c.post("/api/query", json={"connection": "c", "sql": "SELECT 1"})
    assert r.status_code == 500
    assert r.json() == {"detail": "Internal server error."}
    assert "Traceback" not in r.text


def test_unhandled_exception_detail_verbose_when_debug_enabled(monkeypatch):
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    monkeypatch.setenv("DBTOOL_DEBUG", "1")
    c = TestClient(create_app(svc=_ExplodingSvc()), raise_server_exceptions=False)
    r = c.post("/api/query", json={"connection": "c", "sql": "SELECT 1"})
    assert r.status_code == 500
    assert r.json() == {"detail": "Internal server error."}
    assert "unexpected service failure" not in r.text


def test_http_exception_still_returns_expected_status(monkeypatch):
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    c = TestClient(create_app(svc=_NotFoundSvc()))
    r = c.delete("/api/connections/ghost")
    assert r.status_code == 404
    assert r.json()["detail"] == "Connection not found"
