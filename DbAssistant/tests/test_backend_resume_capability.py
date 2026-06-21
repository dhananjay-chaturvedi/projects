"""Per-backend supports_resume capability + agent gating."""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_backend_resume_flags():
    from ai_query.backends.claude_cli import ClaudeCliBackend
    from ai_query.backends.cursor_backend import CursorBackend
    from ai_query.backends.codex_backend import CodexBackend
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    assert ClaudeCliBackend.supports_resume is True
    assert CursorBackend.supports_resume is True
    assert CodexBackend.supports_resume is False
    assert LocalLlmBackend.supports_resume is False


def test_get_info_reports_resume():
    from ai_query.backends.claude_cli import ClaudeCliBackend
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    assert ClaudeCliBackend().get_info().get("resume_supported") is True
    assert LocalLlmBackend().get_info().get("resume_supported") is False


class _Backend:
    def __init__(self, supports_resume):
        self.supports_resume = supports_resume
        self.last_resume = "unset"

    def is_available(self):
        return True

    def call(self, prompt, timeout=120, resume_session_id=None):
        self.last_resume = resume_session_id
        return {"response": "ok", "error": None, "backend_session_id": "new-sid"}


class _Sessions:
    def __init__(self, sess):
        self._sess = sess

    def get(self, sid):
        return self._sess


def _agent_call(backend, sess):
    from ai_query.agent import AIQueryAgent

    self_obj = types.SimpleNamespace(
        default_timeout=10,
        _active_backend=backend,
        _bound_session_id="s1",
        sessions=_Sessions(sess),
        last_prompt_sent="",
        _fallback_value="",
        last_prompt_tokens_est=0,
    )
    self_obj.get_active_backend_name = lambda: "test"
    # _call_ai delegates resume handling to _invoke_backend and consults
    # _fallback_backend_obj; bind both to the stub.
    self_obj._invoke_backend = types.MethodType(
        AIQueryAgent._invoke_backend, self_obj)
    self_obj._fallback_backend_obj = types.MethodType(
        AIQueryAgent._fallback_backend_obj, self_obj)
    return AIQueryAgent._call_ai(self_obj, "hi")


def test_resume_gated_for_supporting_backend():
    backend = _Backend(supports_resume=True)
    sess = types.SimpleNamespace(backend_session_id="prev-sid")
    _agent_call(backend, sess)
    # Prior session id was replayed, and the new one persisted.
    assert backend.last_resume == "prev-sid"
    assert sess.backend_session_id == "new-sid"


def test_resume_skipped_for_stateless_backend():
    backend = _Backend(supports_resume=False)
    sess = types.SimpleNamespace(backend_session_id="prev-sid")
    _agent_call(backend, sess)
    # No resume id sent, and the stale id is NOT overwritten with a fake one.
    assert backend.last_resume is None
    assert sess.backend_session_id == "prev-sid"


def test_list_backend_options_includes_resume_flag():
    from ai_query.agent import AIQueryAgent

    agent = AIQueryAgent()
    opts = agent.list_backend_options()
    assert opts, "expected at least one backend option"
    assert all("resume_supported" in o for o in opts)
    by_backend = {o["backend"]: o["resume_supported"] for o in opts}
    assert by_backend.get("claude") is True
    assert by_backend.get("codex") is False
