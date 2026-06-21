"""Focused tests for Teams alert delivery hardening."""

from __future__ import annotations

import urllib.error
import urllib.request

import pytest

from monitoring import send_notification as sn


@pytest.fixture(autouse=True)
def _clear_non_retryable_cache():
    sn._NON_RETRYABLE_FAILURES.clear()
    yield
    sn._NON_RETRYABLE_FAILURES.clear()


class FakeResp:
    def __init__(self, status=200, body=b"ok"):
        self.status = status
        self._body = body

    def read(self):
        return self._body

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def test_invalid_webhook_url_rejected(monkeypatch):
    monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "not-a-url")
    out = sn.send_alert("hello")
    assert out["ok"] is False
    assert "valid http" in out["message"]


def test_success_uses_timeout_and_post(monkeypatch):
    monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "https://example.test/webhook")
    seen = {}

    def fake_urlopen(req, timeout=None):
        seen["timeout"] = timeout
        seen["method"] = req.get_method()
        seen["content_type"] = req.headers.get("Content-type")
        return FakeResp(200, b"1")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    out = sn.send_alert("ok", timeout=7)
    assert out["ok"] is True
    assert out["status"] == 200
    assert seen["timeout"] == 7
    assert seen["method"] == "POST"
    assert "application/json" in seen["content_type"]


def test_non_retryable_400_returns_immediately(monkeypatch):
    monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "https://example.test/webhook")
    sn._NON_RETRYABLE_FAILURES.clear()
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise urllib.error.HTTPError(
            req.full_url,
            400,
            "Bad Request",
            hdrs=None,
            fp=FakeResp(400, b"bad payload"),
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    out = sn.send_alert("bad", max_attempts=3)
    assert out["ok"] is False
    assert out["status"] == 400
    assert calls["n"] == 1

    second = sn.send_alert("bad again", max_attempts=3)
    assert second["status"] == 400
    assert calls["n"] == 1


def test_retryable_503_retries_until_success(monkeypatch):
    monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "https://example.test/webhook")
    calls = {"n": 0}
    sleeps = []

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResp(503, b"busy")
        return FakeResp(200, b"ok")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(sn.time, "sleep", lambda s: sleeps.append(s))
    out = sn.send_alert("retry", max_attempts=2)
    assert out["ok"] is True
    assert calls["n"] == 2
    assert sleeps == [1]


def test_url_error_returns_connection_failure(monkeypatch):
    monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "https://example.test/webhook")

    def fake_urlopen(req, timeout=None):
        raise urllib.error.URLError("dns failed")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    out = sn.send_alert("network", max_attempts=1)
    assert out["ok"] is False
    assert "dns failed" in out["message"]


def test_payload_truncates_very_large_message():
    payload = sn._teams_payload("x" * (sn._MAX_MESSAGE_CHARS + 25))
    assert b"truncated 25 character" in payload
    assert len(payload) < sn._MAX_MESSAGE_CHARS + 2000
