"""Tests for the autonomous App Build orchestrator.

The orchestrator is the "smart agent": it produces a runnable baseline, then in
auto mode keeps talking to the AI Query Assistant (here a FakeBridge), gating
every AI file through the meters/managers and never regressing below the safe
baseline. These tests use a deterministic FakeBridge so no real model is needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator


class FakeBridge:
    """Scripted stand-in for the AI Query Assistant channel."""

    def __init__(self, responses: list[str], available: bool = True) -> None:
        self._responses = list(responses)
        self._available = available
        self.calls = 0

    def available(self) -> bool:
        return self._available

    def generate(self, prompt: str) -> str:
        self.calls += 1
        return self._responses.pop(0) if self._responses else ""


_GOOD_FILE = (
    "=== FILE: src/services/extra.py ===\n"
    '"""Extra helper module."""\n'
    "from __future__ import annotations\n\n\n"
    "def compute_total(values: list[int]) -> int:\n"
    '    """Return the sum of values."""\n'
    "    return sum(values)\n"
    "=== END FILE ===\n"
)

_BAD_FILE = (
    "=== FILE: src/services/broken.py ===\n"
    "def oops(:\n    pass\n"  # syntax error -> managers reject
    "=== END FILE ===\n"
)


def _bp(name="auto", mode=BuildMode.FROM_SCRATCH, **kw):
    return AppBlueprint(name=name, mode=mode,
                        services=["ci_cd", "document", "hosting", "database"], **kw)


def _bp_db(name="auto", **kw):
    """Database-mode blueprint — full scaffold + manifest (auto-loop tests)."""
    return AppBlueprint(
        name=name, mode=BuildMode.FROM_DATABASE, connections=["local"],
        services=["ci_cd", "document", "hosting", "database"], **kw)


# ── baseline (no AI) ─────────────────────────────────────────────────────────
def test_baseline_only_without_bridge(tmp_path):
    orch = AppBuildOrchestrator()
    result = orch.run(_bp(), tmp_path / "ws")
    assert result.ok is True
    assert result.used_ai is False
    assert len(result.rounds) == 1
    assert result.rounds[0].phase == "baseline"
    assert "src/app.py" in result.files
    assert (tmp_path / "ws" / "src" / "app.py").is_file()


def test_db_understanding_emits_answerer_profiling_status(tmp_path):
    from ai_assistant.app_builder.db_understanding import DataInsight

    class _FakeDbUnderstanding:
        def available(self) -> bool:
            return True

        def understand(self, schema):
            return DataInsight(
                app_name="Fleet Tracker",
                app_summary="Track fleet vehicles in real time.",
                design_brief="Build a fleet management app with live maps.",
            )

    events: list[dict] = []
    orch = AppBuildOrchestrator()
    orch.run(
        _bp_db(name="fleet"),
        tmp_path / "ws",
        db_understanding=_FakeDbUnderstanding(),
        on_progress=events.append,
    )
    statuses = [
        e["agent_event"]["event"]["text"]
        for e in events
        if isinstance(e, dict)
        and e.get("agent_event", {}).get("session") == "answerer"
        and e.get("agent_event", {}).get("event", {}).get("type") == "session_status"
    ]
    assert any("understanding the database" in t for t in statuses)
    assert any(
        "DB understanding complete" in t and "Fleet Tracker" in t
        for t in statuses
    )


def test_progress_callback_receives_rounds(tmp_path):
    seen = []
    orch = AppBuildOrchestrator()
    orch.run(_bp(), tmp_path / "ws", on_progress=seen.append)
    assert seen and seen[0]["phase"] == "baseline"


# ── auto mode with AI ────────────────────────────────────────────────────────
def test_auto_loop_keeps_good_ai_file_and_converges(tmp_path):
    bridge = FakeBridge([_GOOD_FILE, ""])  # add a file, then converge
    orch = AppBuildOrchestrator(max_rounds=4)
    result = orch.run(_bp_db(), tmp_path / "ws", bridge=bridge)
    assert result.used_ai is True
    assert result.ok is True
    # The good AI file was integrated and written to disk.
    assert "src/services/extra.py" in result.files
    assert (tmp_path / "ws" / "src" / "services" / "extra.py").is_file()
    # Loop ran then converged (baseline + improve + converge).
    notes = [r.note for r in result.rounds]
    assert any("converged" in n for n in notes)


def test_managers_reject_unsafe_ai_file(tmp_path):
    bridge = FakeBridge([_BAD_FILE])  # only a broken file
    orch = AppBuildOrchestrator(max_rounds=2)
    result = orch.run(_bp_db(), tmp_path / "ws", bridge=bridge)
    # Build must not regress: still ok, broken file never written.
    assert result.ok is True
    assert "src/services/broken.py" not in result.files
    assert not (tmp_path / "ws" / "src" / "services" / "broken.py").exists()
    rejected = sum(r.rejected_files for r in result.rounds)
    assert rejected >= 1


def test_mixed_response_accepts_good_rejects_bad(tmp_path):
    mixed = _GOOD_FILE + _BAD_FILE
    bridge = FakeBridge([mixed, ""])
    orch = AppBuildOrchestrator(max_rounds=3)
    result = orch.run(_bp_db(), tmp_path / "ws", bridge=bridge)
    assert "src/services/extra.py" in result.files
    assert "src/services/broken.py" not in result.files


def test_bridge_unavailable_is_baseline_only(tmp_path):
    bridge = FakeBridge([_GOOD_FILE], available=False)
    orch = AppBuildOrchestrator()
    result = orch.run(_bp(), tmp_path / "ws", bridge=bridge)
    assert result.used_ai is False
    assert bridge.calls == 0


def test_blueprint_rejected_short_circuits(tmp_path):
    # from_database with no connection/schema -> invalid blueprint.
    bp = AppBlueprint(name="bad", mode=BuildMode.FROM_DATABASE)
    result = AppBuildOrchestrator().run(bp, tmp_path / "ws")
    assert result.ok is False
    assert result.rounds[0].phase == "blueprint"


# ── through the service ───────────────────────────────────────────────────────
def test_service_auto_build_with_injected_bridge(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from ai_assistant.app_builder.service import AppBuilderService

    bridge = FakeBridge([_GOOD_FILE, ""])
    r = AppBuilderService().auto_build(
        {"name": "svcauto", "mode": "from_database", "connections": ["local"]},
        bridge=bridge,
    )
    assert r["ok"] is True
    assert r["used_ai"] is True
    assert r["agent"] == "orchestrator"
    assert any("converged" in rnd["note"] for rnd in r["rounds"])
    assert "src/services/extra.py" in r["files"]


def test_service_auto_build_database_uses_schema(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from ai_assistant.app_builder.service import AppBuilderService

    r = AppBuilderService().auto_build({
        "name": "dbauto", "mode": "from_database",
        "schema": {"customers": ["id", "name"], "orders": ["id", "total"]},
        "description": "manage customers and orders",
    })
    assert r["ok"] is True
    ws = Path(r["workspace"])
    assert (ws / "src" / "models.py").read_text().count("class ") >= 2


@pytest.mark.parametrize("max_rounds", [1, 2, 5])
def test_auto_loop_respects_max_rounds(tmp_path, max_rounds):
    # Bridge always returns a *new* good file so it would loop forever if unbounded.
    def endless():
        i = 0
        while True:
            i += 1
            yield (
                f"=== FILE: src/services/m{i}.py ===\n"
                f'"""Module {i}."""\n\n\ndef f{i}() -> int:\n'
                f'    """Return {i}."""\n    return {i}\n'
                "=== END FILE ===\n"
            )
    gen = endless()

    class Endless:
        calls = 0

        def available(self):
            return True

        def generate(self, prompt):
            self.calls += 1
            return next(gen)

    bridge = Endless()
    orch = AppBuildOrchestrator(max_rounds=max_rounds)
    orch.run(_bp(), tmp_path / "ws", bridge=bridge)
    assert bridge.calls <= max_rounds
