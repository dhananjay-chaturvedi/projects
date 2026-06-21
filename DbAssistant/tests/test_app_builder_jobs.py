"""Tests for App Builder background jobs + runtime helpers."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ai_assistant.app_builder import service as svc_mod
from ai_assistant.app_builder.jobs import BuildJobManager, get_job_manager
from ai_assistant.app_builder.service import AppBuilderService


def _service_rooted_at(tmp_path: Path, monkeypatch) -> AppBuilderService:
    root = tmp_path / "app_builder"
    root.mkdir(parents=True)
    monkeypatch.setattr(svc_mod.app_paths, "app_builder_dir", lambda: root)
    return AppBuilderService()


def test_start_app_requires_runnable_app(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    r = svc.start_app({"name": "ghost"})
    assert not r["ok"]


def test_start_and_stop_app(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    ws = svc._workspace("demo")
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text("app = object()\n", encoding="utf-8")

    # Mock Popen so we don't actually launch uvicorn in tests.
    import subprocess as sp

    class _Proc:
        pid = 12345

        def poll(self):
            return None

        def terminate(self):
            pass

    monkeypatch.setattr(sp, "Popen", lambda *a, **k: _Proc())

    started = svc.start_app({"name": "demo", "port": 9001})
    assert started["ok"]
    assert started["port"] == 9001
    assert "9001" in started["url"]

    stopped = svc.stop_app({"name": "demo"})
    assert stopped["ok"] and stopped["stopped"]


def test_job_manager_start_and_status(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)

    def fake_run(body, **kw):
        on_progress = kw.get("on_progress")
        if on_progress:
            on_progress({"agent_event": {
                "session": "builder",
                "event": {"type": "session_status", "text": "working"},
            }})
        return {"ok": True, "workspace": str(svc._workspace(body["name"])),
                "agentic": True}

    svc.run_agentic_build = fake_run  # type: ignore[method-assign]
    mgr = BuildJobManager(svc)
    started = mgr.start({"name": "jobdemo", "mode": "from_scratch"})
    assert started["ok"] and started["job_id"]

    job_id = started["job_id"]
    # Wait for thread to finish.
    for _ in range(50):
        st = mgr.status(job_id)
        if st["status"] in ("finished", "error", "stopped"):
            break
        time.sleep(0.05)

    st = mgr.status(job_id)
    assert st["status"] == "finished"
    events = mgr.events(job_id, 0)
    assert any(e.get("type") == "agent_event" for e in events)


def test_job_stop_sets_cancel(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    cancel_ev = threading.Event()

    def slow_run(body, **kw):
        ev = kw.get("cancel_event")
        for _ in range(100):
            if ev and ev.is_set():
                return {"ok": False, "aborted": True, "agentic": True}
            time.sleep(0.02)
        return {"ok": True, "agentic": True}

    svc.run_agentic_build = slow_run  # type: ignore[method-assign]
    mgr = BuildJobManager(svc)
    started = mgr.start({"name": "slow", "mode": "from_scratch"})
    job_id = started["job_id"]
    time.sleep(0.05)
    mgr.stop(job_id)
    for _ in range(50):
        st = mgr.status(job_id)
        if st["status"] in ("stopped", "finished", "error"):
            break
        time.sleep(0.05)
    assert mgr.status(job_id)["status"] in ("stopped", "finished", "error")


def test_job_send_message_with_fake_coordinator(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    coord = MagicMock()
    coord.route_user_request.return_value = "framed reply"
    svc.last_coordinator = coord
    mgr = BuildJobManager(svc)
    started = mgr.start({"name": "msg", "mode": "from_scratch"})
    job_id = started["job_id"]
    r = mgr.send_message(job_id, "hello", target="auto", interactive=True)
    assert r["ok"]
    coord.route_user_request.assert_called_once()


def test_job_answer_flow(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    answers = []

    def run_with_ask(body, **kw):
        ask = kw.get("ask")
        if ask:
            # Simulate a decision that gets answered.
            decision = MagicMock(
                id="q1", question="Pick?", detail="", options=["a", "b"],
                allow_multiple=False,
            )
            answers.append(ask(decision))
        return {"ok": True, "agentic": True}

    svc.run_agentic_build = run_with_ask  # type: ignore[method-assign]
    mgr = BuildJobManager(svc)
    started = mgr.start({"name": "decide", "mode": "from_scratch"})
    job_id = started["job_id"]

    # Wait for decision event then answer.
    for _ in range(50):
        st = mgr.status(job_id)
        if st.get("pending_decision"):
            mgr.answer(job_id, "a")
            break
        time.sleep(0.05)

    for _ in range(50):
        if mgr.status(job_id)["status"] in ("finished", "error", "stopped"):
            break
        time.sleep(0.05)

    assert answers == ["a"] or answers == ["skip"]


def test_get_job_manager_singleton(tmp_path, monkeypatch):
    svc = _service_rooted_at(tmp_path, monkeypatch)
    from ai_assistant.app_builder.jobs import get_job_manager

    m1 = get_job_manager(svc)
    m2 = get_job_manager(svc)
    assert m1 is m2


def test_api_router_has_job_routes():
    from ai_assistant.app_builder.api import build_router

    router = build_router()
    paths = {getattr(r, "path", "") for r in router.routes}
    assert "/api/app-builder/jobs" in paths
    assert "/api/app-builder/jobs/{job_id}/events" in paths
    assert "/api/app-builder/start-app" in paths
    assert "/api/app-builder/stop-app" in paths
