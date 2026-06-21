"""Tests for AppBuilderAssistant build flows: scratch / database / codebase,
plus the AI-backed agent contract and deterministic fallback."""

from __future__ import annotations

import ast

import pytest

from ai_assistant.app_builder.agent import (
    AgentRequest,
    CliBackendAgent,
    frame_prompt,
    parse_files,
)
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.flows import BuildFlows, analyze_codebase


@pytest.fixture
def home(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    return tmp_path


def _service():
    from ai_assistant.app_builder.service import AppBuilderService

    return AppBuilderService()


# ── from scratch ─────────────────────────────────────────────────────────────
def test_build_from_scratch(home):
    r = _service().build({"name": "scratchapp", "mode": "from_scratch",
                          "description": "A todo API"})
    assert r["ok"] is True
    assert r["verdict"]["score"] == pytest.approx(1.0, abs=0.01)
    assert "src/app.py" in r["files"]
    assert "requirements.txt" in r["files"]
    # Free-form from_scratch: minimal runnable stub only — agent owns structure.
    assert "tests/test_app.py" not in r["files"]
    assert "templates/base.html" not in r["files"]

    from pathlib import Path

    app_py = (Path(r["workspace"]) / "src" / "app.py").read_text()
    ast.parse(app_py)
    assert "app = FastAPI" in app_py
    assert '@app.get("/health")' in app_py
    assert '@app.get("/", response_class=HTMLResponse)' in app_py


def test_scaffold_backcompat(home):
    r = _service().scaffold_from_scratch("legacyapp")
    assert r["ok"] is True
    assert r["files"]


# ── from database ────────────────────────────────────────────────────────────
def test_build_from_database_generates_valid_models(home):
    schema = {"customers": ["id", "name", "email"], "orders": ["id", "customer_id", "total"]}
    r = _service().build({"name": "dbapp", "mode": "from_database", "schema": schema})
    assert r["ok"] is True
    assert "src/models.py" in r["files"]
    assert "src/repository.py" in r["files"]

    from pathlib import Path

    ws = Path(r["workspace"])
    models = (ws / "src/models.py").read_text()
    ast.parse(models)  # must be valid Python
    assert "class Customer" in models
    assert "class Order" in models
    repo = (ws / "src/repository.py").read_text()
    ast.parse(repo)
    assert "?" in repo  # parameterized queries (SQLite placeholders)
    assert "%s" not in repo  # no string-formatted SQL values
    app_py = (ws / "src/app.py").read_text()
    assert "app = FastAPI" in app_py


def test_build_from_database_requires_schema_source(home):
    # No connection and no schema -> blueprint invalid.
    r = _service().build({"name": "bad", "mode": "from_database"})
    assert r["ok"] is False
    assert any("connection" in i for i in r["verdict"]["issues"])


# ── from codebase ────────────────────────────────────────────────────────────
def test_analyze_codebase_real(tmp_path):
    (tmp_path / "mod.py").write_text(
        "def f(x):\n    if x:\n        for i in range(x):\n"
        "            print(i)\n    return x\n"
    )
    (tmp_path / "bad.py").write_text("def g(:\n  pass\n")  # syntax error
    facts = analyze_codebase(str(tmp_path))
    assert facts["files"] == 2
    assert facts["loc"] > 0
    assert any("syntax error" in i for i in facts["issues"])
    assert any("test" in r.lower() for r in facts["recommendations"])


def test_build_from_codebase(home, tmp_path):
    code_dir = tmp_path / "legacy"
    code_dir.mkdir()
    (code_dir / "main.py").write_text('"""m."""\n\n\ndef run():\n    """run."""\n    return 1\n')
    r = _service().build({"name": "cbapp", "mode": "from_codebase",
                          "codebase_path": str(code_dir)})
    assert r["ok"] is True
    assert "docs/ANALYSIS.md" in r["files"]
    assert r["analysis"]["files"] == 1


# ── AI agent contract ────────────────────────────────────────────────────────
def test_frame_prompt_includes_contract():
    req = AgentRequest(mode=BuildMode.FROM_SCRATCH, app_name="x",
                       required_files=["src/app.py"], rules=["no bare except"])
    prompt = frame_prompt(req)
    assert "=== FILE:" in prompt
    assert "no bare except" in prompt
    assert "src/app.py" in prompt


def test_parse_files_strict_contract():
    text = (
        "preamble\n"
        "=== FILE: src/app.py ===\n"
        "```python\nprint('hi')\n```\n"
        "=== END FILE ===\n"
        "=== FILE: README.md ===\n"
        "# Title\n"
        "=== END FILE ===\n"
    )
    files = parse_files(text)
    paths = {f.path: f.content for f in files}
    assert paths["src/app.py"].strip() == "print('hi')"
    assert paths["README.md"].strip() == "# Title"


class _FakeBackend:
    name = "fake"

    def __init__(self, response):
        self._response = response

    def call(self, prompt, timeout=180, resume_session_id=None):
        return {"response": self._response, "error": None}


def test_cli_backend_agent_uses_ai_output():
    resp = (
        "=== FILE: src/app.py ===\n"
        '"""app."""\n\n\ndef main():\n    """run."""\n    return {"status": "ok"}\n'
        "=== END FILE ===\n"
    )
    agent = CliBackendAgent(_FakeBackend(resp))
    out = agent.generate(AgentRequest(mode=BuildMode.FROM_SCRATCH, app_name="x"))
    assert out.backend == "fake"
    assert any(f.path == "src/app.py" for f in out.files)


def test_cli_backend_agent_falls_back_on_empty():
    agent = CliBackendAgent(_FakeBackend("no file blocks here"))
    out = agent.generate(AgentRequest(mode=BuildMode.FROM_SCRATCH, app_name="x"))
    assert "fallback" in out.notes
    assert out.files  # deterministic files present


def test_flow_fills_gaps_when_ai_incomplete(home):
    # AI returns only one file; engine-required files must still be produced.
    resp = "=== FILE: src/app.py ===\ndef main():\n    return {}\n=== END FILE ===\n"
    agent = CliBackendAgent(_FakeBackend(resp))
    bp = AppBlueprint(name="gapapp", mode=BuildMode.FROM_SCRATCH)
    flows = BuildFlows()
    r = flows.build_from_scratch(bp, home / "ws", agent=agent)
    assert "src/app.py" in r["files"]
    assert "requirements.txt" in r["files"]
