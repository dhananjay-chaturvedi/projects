"""Tests for the concise advisor (Session B) and the tiered commit gate.

Covers two requested behaviours:
1. Session B answers are advisory — short, no code, no repeated blocks.
2. The per-commit gate treats infra (docs/hosting/monitoring/CI) as optional
   add-ons (suggestions), enforcing them only at the start/end of a build, while
   meters/managers (code quality + core structure) still gate every commit.
"""

from __future__ import annotations

from ai_assistant.app_builder.agent import AgentRequest, DeterministicAgent
from ai_assistant.app_builder.agent_runner import (
    AgentEvent,
    AgentEventType,
    AgentMode,
)
from ai_assistant.app_builder.build_session import BuilderSession, concise_answer
from ai_assistant.app_builder.commit_gate import CommitGate
from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode


def test_builder_outline_reuses_persistent_session_in_ask_mode():
    """Session A's outline runs on its OWN persistent runner (no 4th session).

    The understanding-phase outline must not spawn a separate agent session; it
    reuses Session A's runner and only forces that single turn into ASK mode so
    the builder cannot write files while drafting the outline.
    """

    class RecordingRunner:
        def __init__(self):
            self.calls: list[tuple[str, AgentMode | None]] = []
            self.session_id = "A-session"

        def run(self, prompt, *, mode=None):
            self.calls.append((prompt, mode))
            return [AgentEvent(AgentEventType.ASSISTANT_TEXT,
                               text="outline: models, routes, features")]

    session = BuilderSession.__new__(BuilderSession)
    session._runner = RecordingRunner()
    session._primed = False
    session.last_events = []

    out = session.prepare_outline("APP: a CRM", brief=None)

    assert "outline" in out
    # Exactly one turn, on the same (persistent) runner, forced to ASK mode.
    assert len(session._runner.calls) == 1
    assert session._runner.calls[0][1] == AgentMode.ASK


# ── Session B: concise advisory answers ──────────────────────────────────────
def test_concise_answer_strips_code_fences():
    text = (
        "Use PostgreSQL for reliability.\n"
        "```python\n"
        "def f():\n    return 1\n"
        "```\n"
        "It scales well."
    )
    out = concise_answer(text)
    assert "def f()" not in out
    assert "code omitted" in out
    assert "PostgreSQL" in out and "scales well" in out


def test_concise_answer_drops_indented_code_and_dedupes():
    text = "Recommendation: cache reads.\n" * 5 + "    x = compute_heavy_thing()\n"
    out = concise_answer(text)
    # Repeated line appears once; indented code line removed.
    assert out.count("Recommendation: cache reads.") == 1
    assert "compute_heavy_thing" not in out


def test_concise_answer_truncates_long_text():
    out = concise_answer("word " * 400)
    assert out.endswith("…")
    assert len(out.split()) <= 130


# ── engine: infra is separable from the core surface ─────────────────────────
def _blueprint() -> AppBlueprint:
    return AppBlueprint(
        name="shop", mode=BuildMode.FROM_DATABASE,
        description="an online store to sell electronics",
        connections=["local"],
        services=["ci_cd", "document", "hosting", "monitoring", "database"],
    )


def test_expected_manifest_excludes_infra_when_requested():
    eng = AiAppEngine()
    bp = _blueprint()
    full = set(eng.expected_manifest(bp, include_infra=True))
    core = set(eng.expected_manifest(bp, include_infra=False))
    infra_only = full - core
    assert core < full
    assert "src/app.py" in core  # core surface kept
    assert any(p.startswith("docs/") for p in infra_only)
    assert "Dockerfile" in infra_only
    assert ".github/workflows/ci.yml" in infra_only
    assert not any(eng._is_infra_file(p) for p in core)


def test_infra_suggestions_lists_missing_addons():
    eng = AiAppEngine()
    bp = _blueprint()
    core_files = eng.expected_manifest(bp, include_infra=False)
    sugg = eng.infra_suggestions(bp, core_files)
    assert any("Dockerfile" in s for s in sugg)
    assert any("monitoring" in s for s in sugg)


# ── gate: infra optional per-commit, enforced at start/end ───────────────────
def _baseline_files(bp: AppBlueprint) -> dict[str, str]:
    req = AgentRequest(
        mode=bp.mode, app_name=bp.name, description=bp.description,
        services=list(bp.services))
    resp = DeterministicAgent().generate(req)
    return {f.path: f.content for f in resp.files}


def test_gate_infra_optional_per_commit_but_strict_at_end():
    eng = AiAppEngine()
    bp = _blueprint()
    # Isolate the infra dimension: never let coverage be the deciding factor.
    gate = CommitGate(eng, bp, target_coverage=0.0)

    baseline = _baseline_files(bp)
    core_only = {p: c for p, c in baseline.items() if not eng._is_infra_file(p)}

    lenient = gate.evaluate_files(
        core_only, infra_blocking=False, enforce_coverage=False)
    strict = gate.evaluate_files(
        core_only, infra_blocking=True, enforce_coverage=True)

    # Per-commit (lenient): the partial-but-valid app is accepted, with the
    # missing infra reported as optional suggestions rather than blocking gaps.
    assert lenient.accepted is True
    assert lenient.suggestions
    # End-of-build (strict): the same core-only files score lower because the
    # infra add-ons are now required.
    assert strict.score <= lenient.score
    assert not strict.suggestions  # strict mode does not emit add-on hints


def test_gate_still_reverts_broken_code(tmp_path):
    from ai_assistant.app_builder.commit_gate import snapshot_workspace

    ws = tmp_path / "ws"
    ws.mkdir()
    eng = AiAppEngine()
    bp = AppBlueprint(
        name="bad", mode=BuildMode.FROM_DATABASE, connections=["local"],
        services=["database"],
    )
    gate = CommitGate(eng, bp, target_coverage=0.0)
    before = snapshot_workspace(ws)
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text(
        "def bad():\n    try:\n        pass\n    except:\n        pass\n",
        encoding="utf-8")
    # Even in the lenient per-round mode, genuinely bad code is reverted.
    verdict = gate.gate(ws, before, infra_blocking=False, enforce_coverage=False)
    assert not verdict.accepted
    assert verdict.reverted is True
    assert "src/app.py" not in snapshot_workspace(ws)


def test_gate_keeps_work_when_revert_disabled(tmp_path):
    """With revert=False the failed commit is flagged but NEVER deleted."""
    from ai_assistant.app_builder.commit_gate import snapshot_workspace

    ws = tmp_path / "ws"
    ws.mkdir()
    eng = AiAppEngine()
    bp = AppBlueprint(
        name="bad", mode=BuildMode.FROM_DATABASE, connections=["local"],
        services=["database"],
    )
    gate = CommitGate(eng, bp, target_coverage=0.0)
    before = snapshot_workspace(ws)
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text(
        "def bad():\n    try:\n        pass\n    except:\n        pass\n",
        encoding="utf-8")
    verdict = gate.gate(ws, before, infra_blocking=False,
                        enforce_coverage=False, revert=False)
    assert not verdict.accepted
    assert verdict.reverted is False
    # The agent's work survives — the per-round loop pauses instead of wiping.
    assert "src/app.py" in snapshot_workspace(ws)


def test_snapshot_includes_github_workflows_but_skips_hidden_junk(tmp_path):
    from ai_assistant.app_builder.commit_gate import snapshot_workspace

    ws = tmp_path / "ws"
    (ws / ".github" / "workflows").mkdir(parents=True)
    (ws / ".github" / "workflows" / "ci.yml").write_text("name: CI\n",
                                                         encoding="utf-8")
    (ws / ".pytest_cache").mkdir(parents=True)
    (ws / ".pytest_cache" / "nodeids").write_text("cache\n", encoding="utf-8")
    snap = snapshot_workspace(ws)
    assert ".github/workflows/ci.yml" in snap
    assert ".pytest_cache/nodeids" not in snap


def test_database_build_rejects_flask_entrypoint():
    eng = AiAppEngine()
    bp = AppBlueprint(
        name="dbapp", mode=BuildMode.FROM_DATABASE, connections=["local"],
        services=["database"],
    )
    files = eng.expected_manifest(bp, include_infra=False)
    verdict = eng.evaluate_build(
        bp,
        files,
        sample_code=(
            "from flask import Flask\n"
            "app = Flask(__name__)\n"
            "@app.route('/health')\n"
            "def health():\n    return 'ok'\n"
        ),
        include_infra=False,
    )
    assert verdict.accepted is False
    assert any("FastAPI ASGI" in issue for issue in verdict.issues)
