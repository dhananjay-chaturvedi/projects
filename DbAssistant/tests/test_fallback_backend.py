"""Fallback backend: failover routing, SQL correction, and surface wiring.

Covers the shared agent/service layer that powers the AI Query Assistant's
fallback backend (failover when the primary is unreachable + the corrector that
repairs SQL the primary got wrong) plus CLI/API parity.
"""

from __future__ import annotations

import types


class _StubBackend:
    """Minimal AIBackend stand-in for routing tests."""

    def __init__(self, name, *, available=True, response="ok", error=None,
                 supports_resume=False):
        self.name = name
        self.display_name = name
        self.supports_resume = supports_resume
        self._available = available
        self._response = response
        self._error = error
        self.calls = []

    def is_available(self):
        return self._available

    def check_availability(self, force=False):
        return self._available

    def get_unavailable_reason(self):
        return "" if self._available else "unreachable"

    def call(self, prompt, timeout=120, resume_session_id=None):
        self.calls.append(prompt)
        return {"response": self._response, "error": self._error,
                "backend_session_id": None}


class _FakeRegistry:
    def __init__(self, backends):
        self._backends = backends

    def get(self, name):
        return self._backends.get(name)


def _agent_with(primary, fallback_value="", backends=None):
    from ai_query.agent import AIQueryAgent

    agent = AIQueryAgent()
    agent._active_backend = primary
    agent._fallback_value = fallback_value
    if backends is not None:
        agent._registry = _FakeRegistry(backends)
    return agent


# ── failover ─────────────────────────────────────────────────────────────────

def test_failover_when_primary_unavailable():
    primary = _StubBackend("claude", available=False)
    fb = _StubBackend("codex", available=True, response="from-fallback")
    agent = _agent_with(primary, "codex", {"codex": fb})

    res = agent._call_ai("hi")
    assert res["response"] == "from-fallback"
    assert res.get("used_fallback") is True
    assert res.get("backend_used") == "codex"
    assert fb.calls and not primary.calls


def test_failover_when_primary_call_errors():
    primary = _StubBackend("claude", available=True, response=None,
                            error="timed out")
    fb = _StubBackend("codex", available=True, response="recovered")
    agent = _agent_with(primary, "codex", {"codex": fb})

    res = agent._call_ai("hi")
    assert res["response"] == "recovered"
    assert res.get("used_fallback") is True
    assert primary.calls and fb.calls


def test_no_failover_when_primary_succeeds():
    primary = _StubBackend("claude", available=True, response="primary-ok")
    fb = _StubBackend("codex", available=True, response="should-not-run")
    agent = _agent_with(primary, "codex", {"codex": fb})

    res = agent._call_ai("hi")
    assert res["response"] == "primary-ok"
    assert "used_fallback" not in res
    assert not fb.calls


def test_no_backend_available_without_fallback():
    primary = _StubBackend("claude", available=False)
    agent = _agent_with(primary, "", {})
    res = agent._call_ai("hi")
    assert res["response"] is None
    assert "No AI backend available" in res["error"]


# ── set / get fallback ─────────────────────────────────────────────────────────

def test_set_and_get_fallback_backend():
    fb = _StubBackend("codex", available=True)
    agent = _agent_with(_StubBackend("claude"), backends={"codex": fb})
    assert agent.set_fallback_backend("codex", verify=True) is True
    assert agent.get_fallback_backend_name() == "codex"
    assert agent.get_fallback_backend_value() == "codex"
    assert agent.has_fallback_backend() is True

    # Clearing.
    assert agent.set_fallback_backend("", verify=False) is True
    assert agent.has_fallback_backend() is False


def test_set_fallback_local_model_selection():
    fb = _StubBackend("local-llm", available=True)
    agent = _agent_with(_StubBackend("claude"), backends={"local-llm": fb})
    agent.set_fallback_backend("local-llm::mymodel", verify=False)
    assert agent.get_fallback_backend_name() == "local-llm"
    assert agent.get_fallback_backend_value() == "local-llm::mymodel"


# ── correct_sql ────────────────────────────────────────────────────────────────

_GOOD_RESPONSE = "SUMMARY_SQL:\nSELECT id FROM customer;\n\nEXPLANATION:\nFixed table name.\n"


def test_correct_sql_syntax_uses_fallback():
    fb = _StubBackend("codex", available=True, response=_GOOD_RESPONSE)
    agent = _agent_with(_StubBackend("local-llm"), "codex", {"codex": fb})

    res = agent.correct_sql(
        "list customer ids", "SELECT id FROM custmer",
        db_type="postgresql", error_text="relation custmer does not exist",
        mode="syntax",
    )
    assert res["sql"] and "customer" in res["sql"]
    assert res["backend_used"] == "codex"
    assert res["error"] is None
    # Prompt should mention the execution error for the syntax-repair path.
    assert "relation custmer does not exist" in fb.calls[0]


def test_correct_sql_interpretation_prompt():
    fb = _StubBackend("codex", available=True, response=_GOOD_RESPONSE)
    agent = _agent_with(_StubBackend("local-llm"), "codex", {"codex": fb})

    res = agent.correct_sql(
        "top customers by spend", "SELECT * FROM customer",
        db_type="mysql", mode="interpretation",
    )
    assert res["sql"]
    assert "wrong interpretation" in fb.calls[0].lower()


def test_correct_sql_without_fallback_errors():
    agent = _agent_with(_StubBackend("local-llm"), "", {})
    res = agent.correct_sql("q", "SELECT 1", db_type="sqlite")
    assert res["sql"] is None
    assert "fallback" in res["error"].lower()


def test_call_backend_unknown():
    agent = _agent_with(_StubBackend("claude"), backends={})
    res = agent.call_backend("nope", "prompt")
    assert res["response"] is None
    assert "Unknown backend" in res["error"]


# ── service layer ──────────────────────────────────────────────────────────────

class _FakeAgent:
    def __init__(self):
        self._fb = ""
        self.correct_calls = []

    def set_fallback_backend(self, name, verify=True):
        self._fb = (name or "").strip()
        return bool(self._fb)

    def get_fallback_backend_value(self):
        return self._fb

    def get_fallback_backend_name(self):
        return self._fb.split("::")[0] if self._fb else ""

    def correct_sql(self, question, sql, **kw):
        self.correct_calls.append((question, sql, kw))
        return {"sql": "SELECT 1", "explanation": "fixed",
                "error": None, "backend_used": kw.get("backend_value") or self._fb}


class _FakeCore:
    def get_manager(self, name):
        return types.SimpleNamespace(db_type="postgresql", connection_name=name)


def _service_with_fake_agent():
    from ai_query.service import AIService

    svc = AIService(_FakeCore())
    svc._ai = _FakeAgent()
    return svc


def test_service_configure_fallback_persists():
    svc = _service_with_fake_agent()
    r = svc.configure_ai_fallback_backend("codex", verify=False)
    assert r["ok"] is True
    assert r["fallback"] == "codex"

    # Persisted so a later read sees it.
    from ai_query.service import _read_ai_state
    assert _read_ai_state().get("fallback_backend") == "codex"

    # Clearing.
    r2 = svc.configure_ai_fallback_backend("", verify=False)
    assert r2["fallback_value"] == ""
    assert "cleared" in r2["message"].lower()


def test_service_correct_sql_passes_connection():
    svc = _service_with_fake_agent()
    r = svc.correct_sql("q", "SELECT bad", connection="mydb",
                        error_text="boom", mode="syntax")
    assert r["ok"] is True
    assert r["sql"] == "SELECT 1"
    q, sql, kw = svc._ai.correct_calls[0]
    assert kw["connection_name"] == "mydb"
    assert kw["error_text"] == "boom"
    assert kw["mode"] == "syntax"


# ── CLI / API parity ───────────────────────────────────────────────────────────

def test_cli_exposes_fallback_and_correct():
    import ai_query.cli as cli
    assert "fallback" in cli._AI_SUBCOMMANDS
    assert "correct" in cli._AI_SUBCOMMANDS
    assert hasattr(cli, "_dispatch_fallback")
    assert hasattr(cli, "_dispatch_correct")


def test_api_exposes_fallback_and_correct_routes():
    from fastapi import FastAPI
    from ai_query.api import build_router

    svc = _service_with_fake_agent()
    app = FastAPI()
    app.include_router(build_router(svc))
    paths = {r.path for r in app.routes}
    assert "/api/ai/fallback-backend" in paths
    assert "/api/ai/correct-sql" in paths

