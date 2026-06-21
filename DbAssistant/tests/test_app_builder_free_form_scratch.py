"""from_scratch builds are free-form — only a minimal runnable contract."""

from __future__ import annotations

import ast
from pathlib import Path

from ai_assistant.app_builder.agent import AgentRequest, DeterministicAgent
from ai_assistant.app_builder.commit_gate import CommitGate, snapshot_workspace
from ai_assistant.app_builder.engine import (
    SCRATCH_CONTRACT,
    AiAppEngine,
    AppBlueprint,
    BuildMode,
)
from ai_assistant.app_builder.spec import AppSpec
from ai_assistant.app_builder.webapp import minimal_scratch_stub


def test_from_scratch_expected_manifest_is_minimal_contract():
    eng = AiAppEngine()
    bp = AppBlueprint(name="app", mode=BuildMode.FROM_SCRATCH)
    assert eng.expected_manifest(bp) == list(SCRATCH_CONTRACT)


def test_minimal_scratch_stub_is_valid_and_has_health():
    files = minimal_scratch_stub(AppSpec(app_name="demo", description="test app"))
    assert "src/app.py" in files
    assert "requirements.txt" in files
    ast.parse(files["src/app.py"])
    assert '@app.get("/health")' in files["src/app.py"]
    assert '@app.get("/", response_class=HTMLResponse)' in files["src/app.py"]


def test_deterministic_from_scratch_uses_stub_not_full_scaffold():
    req = AgentRequest(
        mode=BuildMode.FROM_SCRATCH,
        app_name="demo",
        description="a chatbot for car service",
        entities=["car", "service", "customer"],
    )
    files = {f.path: f.content for f in DeterministicAgent().generate(req).files}
    assert set(files) <= {"src/app.py", "src/__init__.py", "requirements.txt"}
    assert "src/api.py" not in files
    assert "templates/base.html" not in files


def test_from_scratch_gate_accepts_free_structure_without_revert(tmp_path):
    eng = AiAppEngine()
    bp = AppBlueprint(name="free", mode=BuildMode.FROM_SCRATCH)
    gate = CommitGate(eng, bp, target_coverage=0.0, structure_enforced=False)
    ws = tmp_path / "ws"
    ws.mkdir()
    before = snapshot_workspace(ws)
    (ws / "app").mkdir()
    (ws / "app" / "main.py").write_text(
        '"""Custom layout chosen by the agent."""\n'
        "def run():\n"
        '    """Run the app."""\n'
        "    return 'ok'\n",
        encoding="utf-8")
    (ws / "src").mkdir()
    (ws / "src" / "app.py").write_text(
        'from fastapi import FastAPI\napp = FastAPI()\n',
        encoding="utf-8")
    (ws / "requirements.txt").write_text("fastapi\n", encoding="utf-8")
    verdict = gate.gate(ws, before, infra_blocking=False, enforce_coverage=False)
    assert verdict.accepted is True
    assert verdict.reverted is False
    assert (ws / "app" / "main.py").is_file()


def test_service_auto_enables_agentic_for_from_scratch(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    captured: dict = {}

    class _Backend:
        name = "cursor"

    def _fake_resolve(body):
        return _Backend()

    def _fake_run(*args, **kwargs):
        captured["force_agentic"] = kwargs["context"].force_agentic
        class _R:
            def as_dict(self):
                return {"ok": True, "agentic": True}
        return _R()

    monkeypatch.setattr(svc, "_resolve_backend", _fake_resolve)
    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator.run",
        _fake_run,
    )
    monkeypatch.setattr(
        "ai_assistant.app_builder.agent_runner.supports_agentic_write",
        lambda _b: True,
    )
    svc.auto_build(
        {"name": "x", "mode": "from_scratch", "use_ai": True},
        backend=_Backend(),
    )
    assert captured.get("force_agentic") is True


def test_service_auto_enables_agentic_for_from_database_with_aiqa(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    captured: dict = {}

    class _Backend:
        name = "cursor"

    class _DbUnderstanding:
        pass

    db_understanding = _DbUnderstanding()

    def _fake_run(*args, **kwargs):
        ctx = kwargs["context"]
        captured["force_agentic"] = ctx.force_agentic
        captured["db_understanding"] = ctx.db_understanding
        captured["schema"] = ctx.schema

        class _R:
            def as_dict(self):
                return {"ok": True, "agentic": True}

        return _R()

    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator.run",
        _fake_run,
    )
    monkeypatch.setattr(
        "ai_assistant.app_builder.agent_runner.supports_agentic_write",
        lambda _b: True,
    )
    svc.auto_build(
        {
            "name": "dbx",
            "mode": "from_database",
            "use_ai": True,
            "schema": {"customers": ["id", "name"]},
        },
        backend=_Backend(),
        db_understanding=db_understanding,
    )
    assert captured.get("force_agentic") is True
    assert captured.get("db_understanding") is db_understanding
    assert captured.get("schema") == {"customers": ["id", "name"]}
