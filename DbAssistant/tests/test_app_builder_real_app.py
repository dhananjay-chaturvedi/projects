"""The App Builder should build the REAL app described, not a table-per-word CRUD.

These guard two things:

* the deterministic *fallback* entity mining is conservative (it does not turn
  every word of the prompt into a database table), and
* the AI agent prompts (both the one-shot frame and the orchestrator iteration)
  explicitly steer the model to design/build the complete application and to
  avoid mirroring nouns into CRUD tables.
"""

from __future__ import annotations

from ai_assistant.app_builder.agent import AgentRequest, frame_prompt
from ai_assistant.app_builder.ai_bridge import DirectChatBridge
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from ai_assistant.app_builder.requirements import _candidate_entities, derive_spec


# ── conservative entity mining ────────────────────────────────────────────────
def test_mining_does_not_make_a_table_per_word():
    prompt = ("Build a real proper app that lets users add, edit and delete "
              "blog posts and also view comments on each post.")
    ents = _candidate_entities(prompt)
    # Verbs / UI / filler words must not become entities.
    for bad in ("add", "edit", "delete", "view", "real", "proper", "app",
                "users", "build"):
        assert bad not in ents
    # Capped and deduped (post/posts collapse to one entity).
    assert len(ents) <= 3
    assert sum(1 for e in ents if e.startswith("post")) <= 1


def test_mining_dedupes_singular_plural():
    ents = _candidate_entities("manage customers and a customer profile")
    assert sum(1 for e in ents if e.startswith("customer")) == 1


def test_derive_spec_scratch_stays_small():
    spec = derive_spec(description="a wordy description with many random nouns "
                                   "like invoices clients projects tasks teams")
    assert 1 <= len(spec.entities) <= 3  # not one table per noun


# ── AI prompt steers a real app (from_scratch) ────────────────────────────────
def test_frame_prompt_steers_real_app_for_scratch():
    req = AgentRequest(mode=BuildMode.FROM_SCRATCH, app_name="blog",
                       description="a blogging app with posts and comments")
    prompt = frame_prompt(req)
    assert "table per noun" in prompt
    assert "COMPLETE" in prompt


def test_frame_prompt_database_mode_has_no_scratch_goal():
    req = AgentRequest(mode=BuildMode.FROM_DATABASE, app_name="x",
                       description="x", schema={"t": ["id"]})
    assert "table per noun" not in frame_prompt(req)


class _RecordingBridge:
    def __init__(self):
        self.prompts: list[str] = []

    def available(self):
        return True

    def generate(self, prompt):
        self.prompts.append(prompt)
        return ""  # converge immediately; we only inspect the prompt


def test_orchestrator_iteration_steers_real_app_for_scratch(tmp_path):
    bridge = _RecordingBridge()
    bp = AppBlueprint(name="blog", mode=BuildMode.FROM_SCRATCH,
                      services=["ci_cd", "document", "hosting", "database"],
                      description="a blogging app with posts and comments")
    AppBuildOrchestrator(max_rounds=1).run(bp, tmp_path / "ws", bridge=bridge)
    assert bridge.prompts
    assert "COMPLETE application" in bridge.prompts[0]
    assert "CRUD table per noun" in bridge.prompts[0]
    assert "REQUESTED ENTITIES" not in bridge.prompts[0]


def test_direct_chat_bridge_uses_selected_backend_not_query_conversation():
    class Agent:
        def __init__(self):
            self.prompts = []
            self.started = False

        def is_available(self):
            return True

        def _call_ai(self, prompt, timeout=None):
            self.prompts.append(prompt)
            return {"response": "ok", "error": None}

        def start_new_conversation(self, *args, **kwargs):
            self.started = True
            raise AssertionError("from_scratch must not use AI Query Assistant")

    agent = Agent()
    bridge = DirectChatBridge(agent)
    assert bridge.available() is True
    assert bridge.generate("build a customer support chatbot for car servicing") == "ok"
    assert agent.prompts == ["build a customer support chatbot for car servicing"]
    assert agent.started is False


def test_direct_chat_bridge_uses_active_backend_even_when_agent_flag_false():
    class Backend:
        def __init__(self):
            self.prompts = []

        def is_available(self):
            return True

        def call(self, prompt, timeout=None):
            self.prompts.append(prompt)
            return {"response": "ok", "error": None}

    class Agent:
        def __init__(self):
            self._active_backend = Backend()

        def is_available(self):
            return False  # AIQueryAgent.cli_available may lag selected backend

    agent = Agent()
    bridge = DirectChatBridge(agent)
    assert bridge.available() is True
    assert bridge.generate("build app") == "ok"
    assert agent._active_backend.prompts == ["build app"]


def test_service_from_scratch_use_ai_uses_direct_agent(monkeypatch, tmp_path):
    from ai_assistant.app_builder.service import AppBuilderService
    import ai_query.agent as agent_mod

    calls = {"prompts": [], "conversation": 0}

    class FakeAgent:
        def is_available(self):
            return True

        def _call_ai(self, prompt, timeout=None):
            calls["prompts"].append(prompt)
            return {"response": "", "error": None}

        def start_new_conversation(self, *args, **kwargs):
            calls["conversation"] += 1
            raise AssertionError("from_scratch must not use AI Query Assistant")

    from ai_assistant.app_builder.ai_bridge import DirectChatBridge

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(agent_mod, "AIQueryAgent", FakeAgent)
    monkeypatch.setattr(
        "ai_assistant.app_builder.agent_runner.supports_agentic_write",
        lambda _b: False,
    )
    svc = AppBuilderService()
    monkeypatch.setattr(svc, "_workspace", lambda name: tmp_path / name)
    out = svc.auto_build({
        "name": "supportbot", "mode": "from_scratch", "use_ai": True,
        "description": "build a customer support chatbot for car servicing",
        "max_rounds": 1,
    }, bridge=DirectChatBridge(FakeAgent()))
    assert out["used_ai"] is True
    assert calls["conversation"] == 0
    assert calls["prompts"]
    assert "customer support chatbot for car servicing" in calls["prompts"][0]


class _AvailBridge:
    """Stub bridge so fidelity-driven coverage is active (no code-agent loop)."""

    def available(self):
        return True

    def generate(self, prompt):
        return ""


# ── auto-build is fidelity-driven for from_scratch (not table-per-noun done) ──
def test_scratch_autobuild_not_done_until_requirement_reflected(tmp_path):
    """A rich requirement that the scaffold doesn't reflect must NOT be marked
    complete just because CRUD tables exist — fidelity drives completion."""
    bp = AppBlueprint(
        name="booking", mode=BuildMode.FROM_SCRATCH,
        services=["ci_cd", "document", "hosting", "database"],
        description=("a movie ticket booking app where visitors pick a showtime, "
                     "choose seats and receive a booking confirmation"))
    # With an available AI channel, fidelity (not bare structural CRUD) drives
    # completion — even when max_rounds=0 leaves only the deterministic scaffold.
    result = AppBuildOrchestrator(max_rounds=0).run(
        bp, tmp_path / "ws", bridge=_AvailBridge())
    # The scaffold does not reflect showtime/seat/confirmation intent, so the
    # build is honestly reported as incomplete with the missing requirements.
    assert result.coverage_ok is False
    assert any("requirement not yet reflected" in g for g in result.gaps)


def test_scratch_autobuild_prefers_app_that_reflects_requirement(tmp_path):
    """When the AI returns files that reflect the requirement, they raise the
    completion score and are kept over the bare CRUD scaffold."""
    # An AI round that adds real booking surfaces (routes + template + test).
    ai_files = (
        "=== FILE: src/api.py ===\n"
        '"""Booking API."""\n'
        "from fastapi import APIRouter\n\n"
        "router = APIRouter()\n\n\n"
        '@router.get("/showtimes")\n'
        "def showtimes() -> dict:\n"
        '    """List showtimes."""\n'
        "    return {}\n\n\n"
        '@router.get("/seats")\n'
        "def seats() -> dict:\n"
        '    """List seats for a showtime."""\n'
        "    return {}\n\n\n"
        '@router.post("/booking")\n'
        "def booking() -> dict:\n"
        '    """Create a booking confirmation."""\n'
        "    return {}\n"
        "=== END FILE ===\n"
        "=== FILE: templates/showtime.html ===\n"
        "<html>pick a showtime, choose seats, get your booking confirmation</html>\n"
        "=== END FILE ===\n"
        "=== FILE: tests/test_booking.py ===\n"
        "def test_booking():\n"
        "    assert True\n"
        "=== END FILE ===\n"
    )
    bridge = _RecordingBridge2([ai_files, ""])
    bp = AppBlueprint(
        name="booking", mode=BuildMode.FROM_SCRATCH,
        services=["ci_cd", "document", "hosting", "database"],
        description=("a movie ticket booking app where visitors pick a showtime, "
                     "choose seats and receive a booking confirmation"))
    result = AppBuildOrchestrator(max_rounds=3).run(
        bp, tmp_path / "ws", bridge=bridge)
    assert "src/api.py" in result.files
    assert "templates/showtime.html" in result.files
    # Fidelity to the requirement improved by accepting the AI's real surfaces.
    assert result.fidelity >= 0.7


def test_autobuild_reports_contacting_ai_after_baseline(tmp_path):
    bridge = _RecordingBridge2([""])
    seen = []
    bp = AppBlueprint(
        name="supportbot", mode=BuildMode.FROM_SCRATCH,
        services=["ci_cd", "document", "hosting", "database"],
        description="build a customer support chatbot for car servicing")
    AppBuildOrchestrator(max_rounds=1).run(
        bp, tmp_path / "ws", bridge=bridge, on_progress=seen.append)
    notes = [e["note"] for e in seen if isinstance(e, dict) and "note" in e]
    assert any("deterministic baseline" in n for n in notes)
    assert any("contacting AI backend" in n for n in notes)


class _RecordingBridge2:
    def __init__(self, responses):
        self._responses = list(responses)
        self.prompts: list[str] = []

    def available(self):
        return True

    def generate(self, prompt):
        self.prompts.append(prompt)
        return self._responses.pop(0) if self._responses else ""


# ── from_database: build the app the DATA implies, not a schema mirror ────────
class _Insight:
    """Minimal DataInsight-like object the orchestrator can consume."""

    def __init__(self, summary, flow):
        self.app_summary = summary
        self.data_flow = flow
        self.tables = [object()]  # non-empty so journal marks verified_with_data

    def prompt_block(self):
        return f"DATABASE UNDERSTANDING\nApp: {self.app_summary}\nFlow: {self.data_flow}"

    def as_dict(self):
        return {"app_summary": self.app_summary, "data_flow": self.data_flow,
                "tables": [{"name": "loans"}]}


class _Understanding:
    def __init__(self, insight):
        self._insight = insight

    def available(self):
        return True

    def understand(self, schema):
        return self._insight


def test_database_build_targets_data_intent_not_schema(tmp_path):
    """With no user prompt, the build is judged against the app the DATA implies
    (from the AI Query Assistant understanding), not a CRUD mirror of tables."""
    bridge = _RecordingBridge2([""])  # converge; just inspect prompt + scoring
    insight = _Insight("a library lending app for members to borrow books",
                       "members borrow books -> loans -> returns")
    bp = AppBlueprint(name="lib", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description="")  # user left describe empty
    result = AppBuildOrchestrator(max_rounds=1).run(
        bp, tmp_path / "ws",
        schema={"books": ["id", "title"], "members": ["id", "name"],
                "loans": ["id", "book_id", "member_id"]},
        bridge=bridge, db_understanding=_Understanding(insight))
    # The agent is told to build the implied app, not mirror the schema.
    assert "do not just expose" in bridge.prompts[0].lower()
    # Completion is measured against the data's implied intent (library/lending),
    # which the bare schema-mirror baseline does not reflect → not done.
    assert result.coverage_ok is False
    assert any("requirement not yet reflected" in g for g in result.gaps)


def test_database_build_matching_description_stays_complete(tmp_path):
    """When the description matches the table data, the mirror is acceptable and
    the build is not blocked (back-compat for simple management apps)."""
    r = AppBuildOrchestrator(max_rounds=0).run(
        AppBlueprint(name="dbm", mode=BuildMode.FROM_DATABASE,
                     connections=["local"], services=["database"],
                     description="manage customers and orders"),
        tmp_path / "ws",
        schema={"customers": ["id", "name"], "orders": ["id", "total"]})
    assert r.coverage_ok is True
