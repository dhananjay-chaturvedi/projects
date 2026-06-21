"""Interaction control + guarded schema deployment for the App Builder.

Covers the three interaction levels (uninterrupted / auto / interactive), the
opt-in additive schema deployment, and the orchestrator decision points
(confirm plan, per-round apply/stop, confirm + perform deploy). A real
in-memory SQLite manager is used so deployment is genuinely executed.
"""

from __future__ import annotations

import sqlite3

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.interaction import (
    BuildDecider,
    BuildDecision,
    decider_from_options,
)
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from ai_assistant.app_builder.schema_deploy import deploy_schema, extract_ddl


# ── a real sqlite-backed manager that genuinely runs DDL ──────────────────────
class DeployManager:
    db_type = "sqlite"

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.executed: list[str] = []

    def execute_query(self, sql: str):
        self.executed.append(sql)
        try:
            self.conn.execute(sql)
            self.conn.commit()
            return [], None
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)

    def tables(self) -> set[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")
        return {r[0] for r in cur.fetchall()}


# ── BuildDecider levels ───────────────────────────────────────────────────────
def _approve(d: BuildDecision):
    return d.options[0] if d.kind == "choice" else True


def test_uninterrupted_never_asks():
    asked = []
    dec = BuildDecider(level="uninterrupted",
                       ask=lambda d: asked.append(d.id) or True)
    assert dec.decide(BuildDecision("a", "?", default=True)) is True
    assert dec.decide(BuildDecision("b", "?", critical=True, default=True)) is True
    assert asked == []  # nothing was ever asked
    assert all(not e["asked"] for e in dec.log)


def test_auto_asks_only_critical():
    seen = []
    dec = BuildDecider(level="auto", ask=lambda d: seen.append(d.id) or True)
    dec.decide(BuildDecision("normal", "?", default="x"))      # not asked
    dec.decide(BuildDecision("crit", "?", critical=True, default="y"))  # asked
    assert seen == ["crit"]
    assert not dec.interactive


def test_interactive_asks_everything_when_ask_present():
    seen = []
    dec = BuildDecider(level="interactive", ask=lambda d: seen.append(d.id) or True)
    dec.decide(BuildDecision("plan", "?", default=True))
    dec.decide(BuildDecision("apply", "?", kind="choice",
                             options=["apply", "skip", "stop"], default="apply"))
    assert seen == ["plan", "apply"]
    assert dec.interactive


def test_interactive_without_ask_is_silent():
    dec = decider_from_options(interaction="interactive", uninterrupted=False, ask=None)
    assert dec.decide(BuildDecision("x", "?", default=True)) is True
    assert not dec.interactive  # no callback → cannot be interactive
    assert all(not e["asked"] for e in dec.log)


def test_decline_returns_false_and_is_logged():
    dec = BuildDecider(level="interactive", ask=lambda d: False)
    assert dec.approved(BuildDecision("plan", "?", default=True)) is False
    assert dec.log[-1]["answer"] is False and dec.log[-1]["asked"] is True


# ── schema_deploy helper ──────────────────────────────────────────────────────
_FILES = {
    "src/db/schema.sql": (
        "CREATE TABLE IF NOT EXISTS customers (id INTEGER PRIMARY KEY, name TEXT);\n"
        "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total REAL);\n"
    ),
}


def test_extract_ddl_only_create_table():
    risky = {"src/db/schema.sql": _FILES["src/db/schema.sql"]
             + "DROP TABLE customers;\nINSERT INTO orders VALUES (1, 2.0);\n"}
    ddl = extract_ddl(risky)
    assert len(ddl) == 2  # DROP/INSERT are excluded
    assert all(s.upper().startswith("CREATE TABLE IF NOT EXISTS") for s in ddl)


def test_deploy_dry_run_executes_nothing():
    mgr = DeployManager()
    report = deploy_schema(mgr, _FILES, dry_run=True)
    assert report["dry_run"] and report["statements"] == 2
    assert report["executed"] == 0 and not report["deployed"]
    assert mgr.executed == []


def test_deploy_creates_tables():
    mgr = DeployManager()
    report = deploy_schema(mgr, _FILES)
    assert report["deployed"] and report["executed"] == 2
    assert {"customers", "orders"} <= mgr.tables()


def test_deploy_without_manager_reports_error():
    report = deploy_schema(None, _FILES)
    assert not report["deployed"]
    assert any("connection" in e for e in report["errors"])


# ── orchestrator decision points ──────────────────────────────────────────────
def _scratch_bp(name="planapp", **kw):
    return AppBlueprint(
        name=name, mode=BuildMode.FROM_SCRATCH,
        services=["ci_cd", "document", "hosting", "database"],
        description="manage customers and orders", **kw)


def test_plan_cancel_aborts_build(tmp_path):
    dec = BuildDecider(level="interactive", ask=lambda d: False)  # decline plan
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(_scratch_bp(), tmp_path / "ws", decider=dec)
    assert result.ok is False
    assert any("cancelled" in r.note for r in result.rounds)
    assert not result.files


def test_default_no_deploy_when_not_requested(tmp_path):
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(_scratch_bp(), tmp_path / "ws")
    assert result.schema_deploy.get("deployed") is False
    assert result.journal["schema_deployed"] is False


def test_deploy_when_requested_and_uninterrupted(tmp_path):
    mgr = DeployManager()
    bp = _scratch_bp(connections=["local"])
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(bp, tmp_path / "ws", deploy_schema=True, db_manager=mgr,
                      decider=BuildDecider(level="uninterrupted"))
    # Minimal from_scratch stub has no schema.sql — deploy is a no-op, not an error.
    assert result.schema_deploy.get("deployed") is False
    assert result.journal["schema_deployed"] is False
    assert mgr.tables() == set()


def test_deploy_runs_when_schema_sql_present(tmp_path):
    mgr = DeployManager()
    bp = _scratch_bp(connections=["local"])
    ws = tmp_path / "ws"
    AppBuildOrchestrator(max_rounds=0).run(bp, ws)
    schema_path = ws / "src" / "db"
    schema_path.mkdir(parents=True, exist_ok=True)
    (schema_path / "schema.sql").write_text(
        "CREATE TABLE IF NOT EXISTS customers (id INTEGER, name TEXT);\n",
        encoding="utf-8",
    )
    files = {str(p.relative_to(ws)): p.read_text(encoding="utf-8")
             for p in ws.rglob("*") if p.is_file()}
    report = deploy_schema(mgr, files)
    assert report["deployed"] is True
    assert "customers" in mgr.tables()


def test_deploy_declined_in_interactive(tmp_path):
    mgr = DeployManager()
    bp = _scratch_bp(connections=["local"])
    # Approve the plan, decline the (critical) deploy confirmation.
    answers = {"confirm_plan": True, "confirm_deploy_schema": False}
    dec = BuildDecider(level="interactive",
                       ask=lambda d: answers.get(d.id, True))
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(bp, tmp_path / "ws", deploy_schema=True, db_manager=mgr,
                      decider=dec)
    assert result.schema_deploy["deployed"] is False
    assert mgr.executed == []  # nothing run because the user declined


def test_no_deploy_for_from_database(tmp_path):
    """Deploy is from_scratch only — never touches an existing database."""
    mgr = DeployManager()
    bp = AppBlueprint(name="dbapp", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description="manage things")
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(bp, tmp_path / "ws",
                      schema={"customers": ["id", "name"]},
                      deploy_schema=True, db_manager=mgr)
    assert result.schema_deploy.get("deployed") is False
    assert mgr.executed == []


# ── interactive per-round control ─────────────────────────────────────────────
class _Bridge:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def available(self):
        return True

    def generate(self, prompt):
        self.calls += 1
        return self._responses.pop(0) if self._responses else ""


_NEW_FILE = (
    "=== FILE: src/extra.py ===\n"
    '"""Extra module."""\n\n'
    "def helper():\n"
    '    """Return a constant."""\n'
    "    return 1\n"
    "=== END FILE ===\n"
)


def test_interactive_stop_halts_loop(tmp_path):
    bridge = _Bridge([_NEW_FILE, _NEW_FILE, _NEW_FILE])
    answers = {"confirm_plan": True}

    def ask(d):
        if d.id.startswith("apply_round_"):
            return "stop"
        return answers.get(d.id, True)

    dec = BuildDecider(level="interactive", ask=ask)
    orch = AppBuildOrchestrator(max_rounds=3)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=bridge, decider=dec)
    assert any("stopped by user" in r.note for r in result.rounds)
    assert bridge.calls == 1  # stopped after the first AI round


def test_interactive_skip_does_not_apply(tmp_path):
    bridge = _Bridge([_NEW_FILE])
    answers = {"confirm_plan": True}

    def ask(d):
        if d.id.startswith("apply_round_"):
            return "skip"
        return answers.get(d.id, True)

    dec = BuildDecider(level="interactive", ask=ask)
    orch = AppBuildOrchestrator(max_rounds=1)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=bridge, decider=dec)
    assert any("skipped by user" in r.note for r in result.rounds)
    assert "src/extra.py" not in result.files


# ── service wiring ────────────────────────────────────────────────────────────
def test_service_builds_decider_from_body_flags(tmp_path, monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    monkeypatch.setattr(svc, "_workspace", lambda name: tmp_path / name)
    mgr = DeployManager()
    out = svc.auto_build(
        {"name": "svcapp", "mode": "from_scratch",
         "description": "manage customers and orders",
         "services": ["ci_cd", "document", "hosting", "database"],
         "connections": ["local"], "interaction": "uninterrupted",
         "uninterrupted": True, "deploy_schema": True},
        db_manager=mgr,
    )
    assert out["schema_deploy"].get("deployed") is False
    assert out["journal"]["schema_deployed"] is False


# ── agentic path selection ─────────────────────────────────────────────────────
class _FakeBackend:
    name = "cursor"

    def _resolve_executable(self):
        return "cursor"


def test_service_agentic_flag_enables_agentic_path(tmp_path, monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    monkeypatch.setattr(svc, "_workspace", lambda name: tmp_path / name)
    captured = {}

    def fake_run(*args, **kwargs):
        captured.update(kwargs)
        from ai_assistant.app_builder.orchestrator import OrchestrationResult

        return OrchestrationResult(
            ok=True, workspace=str(tmp_path), final_score=1.0,
            files=[], rounds=[], mode="from_scratch", used_ai=True, agentic=True,
        )

    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator.run",
        fake_run,
    )
    out = svc.auto_build(
        {"name": "ag", "mode": "from_scratch", "use_ai": True, "agentic": True},
        backend=_FakeBackend(),
    )
    assert captured["context"].force_agentic is True
    assert captured["context"].backend is not None
    assert out.get("agentic") is True


def test_service_local_llm_disables_agentic(tmp_path, monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    class RagBackend:
        name = "local-llm"

    svc = AppBuilderService()
    monkeypatch.setattr(svc, "_workspace", lambda name: tmp_path / name)
    captured = {}

    def fake_run(*args, **kwargs):
        captured.update(kwargs)
        from ai_assistant.app_builder.orchestrator import OrchestrationResult

        return OrchestrationResult(
            ok=True, workspace=str(tmp_path), final_score=1.0,
            files=[], rounds=[], mode="from_scratch", used_ai=True,
        )

    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator.run",
        fake_run,
    )
    svc.auto_build(
        {"name": "rag", "mode": "from_scratch", "use_ai": True, "agentic": True},
        backend=RagBackend(),
    )
    assert captured["context"].force_agentic is False


def test_service_auto_build_returns_structured_error_when_orchestrator_raises(monkeypatch):
    from ai_assistant.app_builder.service import AppBuilderService

    def boom(self, *args, **kwargs):
        raise RuntimeError("agent workspace unavailable")

    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator.run",
        boom,
    )
    out = AppBuilderService().auto_build({
        "name": "bad",
        "mode": "from_scratch",
        "use_ai": True,
    })
    assert out["ok"] is False
    assert out["aborted"] is True
    assert out["stop_reason"] == "orchestration error"
    assert out["error"] == "agent workspace unavailable"


def test_orchestrator_agentic_loop_records_commits(tmp_path, monkeypatch):
    from ai_assistant.app_builder.commit_gate import CommitVerdict

    class StubBuilder:
        cancelled = False
        last_events: list = []
        last_text = ""

        def __init__(self, *a, **k):
            self.transcript = "Should I use SQLite?"

        def prime(self, brief, **kwargs):
            return []

        def send(self, msg):
            return []

        def plan(self, prompt):
            return "PLAN: build the app."

        def pending_questions(self):
            return []

    class StubAnswerer:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def frame_answer(self, *a, **k):
            return "yes"

        def frame_kickoff(self, description, *, brief=""):
            return f"Build: {description}"

        def frame_confirm_completion(self, *, brief="", context="",
                                     validator_findings="", **kwargs):
            return "Ready for the user to start and verify."

    class StubValidator:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def send(self, msg):
            return []

        def validate(self, *a, **k):
            return "VERDICT: complete"

    class StubCoord:
        progress = None
        validator = None
        _test_plan = ""

        def __init__(self, *a, **k):
            pass

        def start(self):
            pass

        def kickoff(self, description):
            return {"framed": description, "issues": []}

        def route_questions(self, events=None, **k):
            return []

        def relay_validation(self, *a, **k):
            return None

        def deliver_feedback(self, events=None, **k):
            return None

        def deliver_user_messages(self, events=None, **k):
            return None

        def b_progress_check(self, events=None, **k):
            return None

        def finalize_agreement(self, *a, **k):
            return {"complete": True, "issues": [], "statements": {}}

    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.BuilderSession", StubBuilder)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.AnswerSession", StubAnswerer)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.ValidatorSession", StubValidator)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.DualSessionCoordinator",
        StubCoord)

    def fake_gate(self, workspace, before, **kw):
        return CommitVerdict(accepted=True, score=0.95, coverage=0.9)

    monkeypatch.setattr(
        "ai_assistant.app_builder.commit_gate.CommitGate.gate", fake_gate)

    class StubBridge:
        def available(self):
            return True

    orch = AppBuildOrchestrator(max_rounds=1)
    result = orch.run(
        _scratch_bp(), tmp_path / "ws",
        bridge=StubBridge(), backend=_FakeBackend(),
        force_agentic=True,
    )
    assert result.agentic is True
    assert result.used_ai is True
    assert len(result.commits) >= 1
    assert any("agentic" in r.note for r in result.rounds)


def _install_agentic_stubs(monkeypatch, plan_calls=None, sent=None):
    """Patch the dual-session classes + gate; record plan()/send() calls."""
    from ai_assistant.app_builder.commit_gate import CommitVerdict

    class StubBuilder:
        cancelled = False
        last_events: list = []
        last_text = ""

        def __init__(self, *a, **k):
            self.transcript = ""

        def prime(self, brief, **kwargs):
            return []

        def plan(self, prompt):
            if plan_calls is not None:
                plan_calls.append(prompt)
            return "PLAN: build the app"

        def send(self, msg):
            if sent is not None:
                sent.append(msg)
            return []

        def pending_questions(self):
            return []

    class StubAnswerer:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def frame_answer(self, *a, **k):
            return "ok"

        def frame_kickoff(self, description, *, brief=""):
            return f"Build: {description}"

        def frame_confirm_completion(self, *, brief="", context="",
                                     validator_findings="", **kwargs):
            return "Ready for the user to start and verify."

    class StubValidator:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def send(self, msg):
            return []

        def validate(self, *a, **k):
            return "VERDICT: complete"

    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.BuilderSession", StubBuilder)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.AnswerSession", StubAnswerer)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.ValidatorSession", StubValidator)
    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator._run_tests",
        lambda self, ws, paths=None: {"passed": True, "summary": "stubbed",
                                      "paths": paths or []})

    def fake_gate(self, workspace, before, **kw):
        return CommitVerdict(accepted=True, score=0.95, coverage=0.95)

    monkeypatch.setattr(
        "ai_assistant.app_builder.commit_gate.CommitGate.gate", fake_gate)


class _StubBridge:
    def available(self):
        return True


def test_agentic_runs_plan_phase_first(tmp_path, monkeypatch):
    plan_calls: list = []
    sent: list = []
    _install_agentic_stubs(monkeypatch, plan_calls=plan_calls, sent=sent)
    orch = AppBuildOrchestrator(max_rounds=1)
    result = orch.run(
        _scratch_bp(), tmp_path / "ws",
        bridge=_StubBridge(), backend=_FakeBackend(), force_agentic=True,
    )
    assert plan_calls, "the builder must be asked for a plan before building"
    assert "PLAN" in plan_calls[0]
    # Plan is auto-approved and the builder is told to proceed to build.
    assert any("PLAN APPROVED" in m for m in sent)
    assert any(r.phase == "plan" for r in result.rounds)


def test_agentic_cancel_aborts_build(tmp_path, monkeypatch):
    import threading

    _install_agentic_stubs(monkeypatch)
    cancel = threading.Event()
    cancel.set()  # already requested → first loop check aborts
    orch = AppBuildOrchestrator(max_rounds=3)
    result = orch.run(
        _scratch_bp(), tmp_path / "ws",
        bridge=_StubBridge(), backend=_FakeBackend(), force_agentic=True,
        cancel_event=cancel,
    )
    assert result.aborted is True
    assert result.ok is False
    assert any("aborted" in r.note for r in result.rounds)


def _install_loop_stubs(monkeypatch, *, send_events=None, files_changed=None,
                        gate_accepted=True):
    """Stub the dual-session classes + gate with controllable turn output."""
    from ai_assistant.app_builder.commit_gate import CommitVerdict

    class StubBuilder:
        cancelled = False

        def __init__(self, *a, **k):
            self.transcript = ""
            self.last_events = list(send_events or [])

        @property
        def last_text(self):
            return "\n".join(
                getattr(e, "text", "") for e in self.last_events
                if getattr(getattr(e, "type", None), "value", "") == "assistant_text")

        def prime(self, brief, **kwargs):
            return []

        def plan(self, prompt):
            return "PLAN"

        def send(self, msg):
            self.last_events = list(send_events or [])
            return list(send_events or [])

        def pending_questions(self):
            return []

    class StubAnswerer:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def frame_answer(self, *a, **k):
            return "ok"

        def frame_kickoff(self, description, *, brief=""):
            return f"Build: {description}"

        def frame_confirm_completion(self, *, brief="", context="",
                                     validator_findings="", **kwargs):
            return "Ready for the user to start and verify."

    class StubValidator:
        def __init__(self, *a, **k):
            pass

        def prime(self, brief, **kwargs):
            return []

        def send(self, msg):
            return []

        def validate(self, *a, **k):
            return "VERDICT: complete"

    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.BuilderSession", StubBuilder)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.AnswerSession", StubAnswerer)
    monkeypatch.setattr(
        "ai_assistant.app_builder.build_session.ValidatorSession", StubValidator)
    monkeypatch.setattr(
        "ai_assistant.app_builder.orchestrator.AppBuildOrchestrator._run_tests",
        lambda self, ws, paths=None: {"passed": True, "summary": "stubbed",
                                      "paths": paths or []})

    def fake_gate(self, workspace, before, **kw):
        return CommitVerdict(accepted=gate_accepted, score=0.5, coverage=0.5,
                             files_changed=list(files_changed or []))

    monkeypatch.setattr(
        "ai_assistant.app_builder.commit_gate.CommitGate.gate", fake_gate)


def _patch_loop_eval(monkeypatch, orch, *, accepted, gaps, improved):
    from types import SimpleNamespace
    verdict = SimpleNamespace(accepted=accepted, score=0.5, issues=[])
    monkeypatch.setattr(orch, "_evaluate", lambda bp, files: verdict)
    monkeypatch.setattr(orch, "_coverage",
                        lambda files: {"score": 0.5, "gaps": list(gaps)})
    monkeypatch.setattr(orch, "_done", lambda v, c: False)
    monkeypatch.setattr(orch, "_better", lambda *a, **k: improved)


def test_loop_stops_on_no_progress(tmp_path, monkeypatch):
    # Gaps remain (so it keeps trying) but no files change and nothing improves.
    _install_loop_stubs(monkeypatch, send_events=[], files_changed=[])
    orch = AppBuildOrchestrator(max_rounds=10, max_no_progress_rounds=2,
                                repeat_output_limit=99)
    _patch_loop_eval(monkeypatch, orch, accepted=False, gaps=["x"], improved=False)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True)
    assert result.stop_reason == "no progress"
    assert result.ok is False


def test_loop_stops_when_nothing_left(tmp_path, monkeypatch):
    # No gaps, no changes, not idle → nothing warrants another round.
    _install_loop_stubs(monkeypatch, send_events=[], files_changed=[])
    orch = AppBuildOrchestrator(max_rounds=10)
    _patch_loop_eval(monkeypatch, orch, accepted=False, gaps=[], improved=False)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True)
    assert result.stop_reason == "nothing left to build"


def test_loop_honors_agent_done_signal(tmp_path, monkeypatch):
    from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
    _install_loop_stubs(
        monkeypatch,
        send_events=[AgentEvent(AgentEventType.ASSISTANT_TEXT,
                                "The app is complete and all tests pass.")],
        files_changed=["src/app.py"])
    orch = AppBuildOrchestrator(max_rounds=10)
    _patch_loop_eval(monkeypatch, orch, accepted=True, gaps=[], improved=True)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True)
    assert result.stop_reason == "agent reported completion"


def test_loop_stops_on_repeated_output(tmp_path, monkeypatch):
    from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
    _install_loop_stubs(
        monkeypatch,
        send_events=[AgentEvent(AgentEventType.ASSISTANT_TEXT, "thinking...")],
        files_changed=[])
    orch = AppBuildOrchestrator(max_rounds=10, max_no_progress_rounds=99,
                                repeat_output_limit=2)
    _patch_loop_eval(monkeypatch, orch, accepted=False, gaps=["x"], improved=False)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True)
    assert result.stop_reason == "agent repeating output"


def test_decider_take_control_switches_to_interactive():
    asked = []
    dec = decider_from_options(interaction="uninterrupted", uninterrupted=True,
                               ask=lambda d: asked.append(d) or True)
    assert dec.interactive is False  # uninterrupted asks nothing
    dec.take_control()
    assert dec.level == "interactive"
    assert dec.uninterrupted is False
    assert dec.interactive is True  # now the user is consulted


def test_collaboration_pipeline_runs_end_to_end(tmp_path, monkeypatch):
    """With collaboration on, the build runs the understanding phase + meter
    governance and surfaces them in the result, without breaking the loop."""
    from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
    _install_loop_stubs(
        monkeypatch,
        send_events=[AgentEvent(AgentEventType.ASSISTANT_TEXT,
                                "The app is complete and all tests pass.")],
        files_changed=["src/app.py"])
    orch = AppBuildOrchestrator(max_rounds=2, collaboration=True)
    _patch_loop_eval(monkeypatch, orch, accepted=True, gaps=[], improved=True)

    events = []
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True,
                      on_progress=lambda p: events.append(p))

    # The understanding phase ran and is recorded on the result.
    assert "ready" in result.understanding
    assert isinstance(result.understanding.get("similarity"), dict)
    # A session-understanding event was emitted for the UI.
    kinds = [
        (e.get("agent_event", {}).get("event", {}) or {}).get("type")
        for e in events if isinstance(e, dict)
    ]
    assert "session_understanding" in kinds


def test_collaboration_off_by_default(tmp_path, monkeypatch):
    """Default builds keep the legacy flow (no understanding phase)."""
    from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
    _install_loop_stubs(
        monkeypatch,
        send_events=[AgentEvent(AgentEventType.ASSISTANT_TEXT, "done")],
        files_changed=["src/app.py"])
    orch = AppBuildOrchestrator(max_rounds=1)
    _patch_loop_eval(monkeypatch, orch, accepted=True, gaps=[], improved=True)
    result = orch.run(_scratch_bp(), tmp_path / "ws", bridge=_StubBridge(),
                      backend=_FakeBackend(), force_agentic=True)
    assert result.understanding == {}


def test_agentic_baseline_seeds_no_prebuilt_page():
    """Agentic builds must NOT pre-write a generic landing page — Session A
    authors the requirement-specific launch page itself."""
    orch = AppBuildOrchestrator()
    seed = orch._agentic_baseline(orch._request(_scratch_bp(), None))
    assert "src/app.py" not in seed  # no pre-built page
    assert "requirements.txt" in seed  # install/host still works


def test_reconcile_adopts_on_disk_work(tmp_path):
    """Whatever Session A left on disk is adopted into the shipped set, even if
    the per-round gate never reported it as a tracked diff (persist guarantee)."""
    orch = AppBuildOrchestrator()
    ws = tmp_path / "ws"
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text("app = object()\n", encoding="utf-8")
    (ws / "src" / "orders.py").write_text("def total():\n    return 0\n",
                                          encoding="utf-8")
    merged = orch._reconcile_with_workspace(ws, {"requirements.txt": "fastapi\n"})
    assert "src/app.py" in merged
    assert "src/orders.py" in merged
    assert merged["requirements.txt"] == "fastapi\n"  # tracked-only file kept
