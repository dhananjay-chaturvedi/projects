"""Kickoff-via-B flow and the START!/DONE! inter-session handoff markers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.agent_runner import (
    AgentEvent,
    AgentEventType,
    classify_ask_intent,
    detect_phase_done,
    extract_marked_asks,
)
from ai_assistant.app_builder.build_session import (
    AnswerSession,
    BuilderSession,
    DualSessionCoordinator,
    ValidatorSession,
    extract_payload,
)
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.interaction import BuildDecider
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator


def _backend():
    b = MagicMock()
    b.name = "cursor"
    return b


def _brief():
    return make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                                   description="ecommerce store"))


def _events(text):
    return [AgentEvent(AgentEventType.ASSISTANT_TEXT, text)]


# ── structured ASK / PHASE-DONE markers ──────────────────────────────────────
def test_extract_marked_asks_parses_intent():
    text = (
        "ASK: Which payment provider should we use?\n"
        "CONFIRM: Is SQLite acceptable for this app?\n"
        "APPROVE: add a new orders table"
    )
    asks = extract_marked_asks(text)
    assert len(asks) == 3
    assert asks[0][1].startswith("Which payment")
    assert asks[0][0] in ("decide", "open")
    assert asks[1][0] == "confirm"
    assert asks[2][0] == "approve"


def test_classify_ask_intent_fallback():
    assert classify_ask_intent("May I add a new table?") == "approve"
    assert classify_ask_intent("Is this the right approach?") == "confirm"
    assert classify_ask_intent("Which framework should you use?") == "decide"


def test_detect_phase_done_components():
    text = "Built routes.\nPHASE-DONE: api\nPHASE-DONE: web"
    assert detect_phase_done(text) == ["api", "web"]
    assert detect_phase_done("still building") == []


# ── marker extraction ────────────────────────────────────────────────────────
def test_extract_payload_between_markers():
    assert extract_payload("blah START! use sqlite DONE! trailing") == "use sqlite"
    assert extract_payload("no markers here") == "no markers here"
    assert extract_payload("START! only start, no done") == "only start, no done"
    assert extract_payload("") == ""


def test_frame_answer_strips_markers(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=_events(
                   "Sure! START! Use SQLite for simplicity. DONE! (extra chatter)")):
        out = AnswerSession(_backend(), tmp_path).frame_answer(
            "Which DB?", brief=_brief())
    assert "Use SQLite for simplicity." in out
    assert "START!" not in out and "DONE!" not in out


def test_validate_strips_markers(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=_events("START! VERDICT: incomplete\n- add tests DONE!")):
        out = ValidatorSession(_backend(), tmp_path).validate("digest", brief=_brief())
    assert "incomplete" in out.lower()
    assert "DONE!" not in out


# ── kickoff: first user prompt routed through B, then handed to A ────────────
def test_frame_kickoff_returns_actionable_brief(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=_events(
                   "START! Build a store to sell electronics with cart and "
                   "checkout; entities: product, order. DONE!")):
        out = AnswerSession(_backend(), tmp_path).frame_kickoff(
            "sell electronics online", brief=_brief())
    assert "checkout" in out.lower()
    assert "START!" not in out


def test_frame_kickoff_preserves_user_domain(tmp_path):
    # B's reframe drops the niche ("grocery") — the safety net restores it so the
    # domain is never generalized away on the path to Session A.
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=_events(
                   "START! Build an ecommerce app with cart and checkout. "
                   "DONE!")):
        out = AnswerSession(_backend(), tmp_path).frame_kickoff(
            "build a grocery ecommerce app", brief=_brief())
    assert "grocery" in out.lower()


def test_kickoff_hands_a_verbatim_user_request(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        backend = _backend()
        builder = BuilderSession(backend, tmp_path)
        answerer = AnswerSession(backend, tmp_path)
        coord = DualSessionCoordinator(
            builder, answerer, _brief(), BuildDecider(uninterrupted=True))
        with patch.object(AnswerSession, "frame_kickoff",
                          return_value="An ecommerce app."), \
                patch.object(BuilderSession, "send") as send:
            coord.kickoff("build a grocery ecommerce app")
    # A is not sent during kickoff; the exact user words are queued for A's
    # first write-capable build turn.
    send.assert_not_called()
    assert "USER REQUEST (verbatim" in coord.builder_instruction
    assert "grocery ecommerce app" in coord.builder_instruction


def test_kickoff_routes_user_prompt_through_b_to_a(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        backend = _backend()
        builder = BuilderSession(backend, tmp_path)
        answerer = AnswerSession(backend, tmp_path)
        validator = ValidatorSession(backend, tmp_path)
        coord = DualSessionCoordinator(
            builder, answerer, _brief(), BuildDecider(uninterrupted=True),
            validator=validator)
        with patch.object(AnswerSession, "frame_kickoff",
                          return_value="Build X with cart and checkout.") as fk, \
                patch.object(BuilderSession, "send") as send, \
                patch.object(ValidatorSession, "send") as vsend:
            rec = coord.kickoff("describe: build X")
    fk.assert_called_once()
    assert rec["framed"] == "Build X with cart and checkout."
    assert coord._framed_brief == "Build X with cart and checkout."
    # Session A receives nothing during kickoff; the framed brief is queued for
    # its first write-capable build turn.
    send.assert_not_called()
    assert "BUILD BRIEF" in coord.builder_instruction
    assert "Build X" in coord.builder_instruction
    assert "openable webpage" in coord.builder_instruction
    # Session C also receives the framed brief for validation context.
    assert any("BUILD BRIEF" in c.args[0] for c in vsend.call_args_list)


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
def test_from_database_frame_first_instruction_ordered_sections(_run, tmp_path):
    """DB builds: B delivers ordered instruction (intent, brief+data, action) to A/C."""
    from ai_assistant.app_builder.agent_runner import AgentMode

    backend = _backend()
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    validator = ValidatorSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(
        name="fleet", mode=BuildMode.FROM_DATABASE, connections=["local"]))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        validator=validator)
    design = (
        "PREDICTED APP: Fleet Tracker\n"
        "USER-FACING FEATURES: live map, vehicle list, maintenance alerts")
    coord.design_brief = design
    coord.db_context = (
        "DATABASE UNDERSTANDING (phased profile + interpretation):\n"
        "  - vehicles(id, name)  ~10 rows\n"
        "      sample: {'id': 1, 'name': 'Van 12'}")
    phrase = "Build exactly this kind of app: a fleet management dashboard."
    with patch.object(AnswerSession, "frame_db_intent", return_value=phrase) as fdi, \
            patch.object(AnswerSession, "frame_kickoff") as fk, \
            patch.object(BuilderSession, "send") as send, \
            patch.object(ValidatorSession, "send") as vsend:
        coord.frame_first_instruction("optional user hint")
    fdi.assert_called_once()
    fk.assert_not_called()
    # Session A is NEVER given a read-only turn here — the instruction is
    # stashed and delivered on A's first WRITE turn instead.
    send.assert_not_called()
    a_msg = coord.builder_instruction
    c_msg = " ".join(c.args[0] for c in vsend.call_args_list)
    # Ordered sections: intent before design brief before action.
    assert a_msg.index("1. WHAT TO BUILD:") < a_msg.index("2. DESIGN BRIEF AND DATA:")
    assert a_msg.index("2. DESIGN BRIEF AND DATA:") < a_msg.index("3. ACTION:")
    assert phrase in a_msg
    assert "DESIGN BRIEF:" in a_msg
    assert design in a_msg
    assert "sample:" in a_msg
    assert "USER REQUEST (verbatim" not in a_msg
    assert "BUILD BRIEF (the advisor's elaboration" not in a_msg
    assert phrase in c_msg
    assert design in c_msg
    assert "sample:" in c_msg
    # The read-only validator still receives its instruction in ask mode.
    assert vsend.call_args.kwargs.get("mode") == AgentMode.ASK
    assert coord.builder_instruction
    assert coord.validator_instruction


def test_kickoff_noop_when_advisor_cannot_frame(tmp_path):
    # A stub answerer without frame_kickoff must not crash the build.
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        backend = _backend()
        builder = BuilderSession(backend, tmp_path)

        class _Stub:
            def prime(self, brief, **kwargs):
                return []

        coord = DualSessionCoordinator(
            builder, _Stub(), _brief(), BuildDecider(uninterrupted=True))
        with patch.object(BuilderSession, "send") as send:
            rec = coord.kickoff("anything")
    assert rec["issues"]
    send.assert_not_called()


def test_plan_prompt_does_not_include_raw_user_description():
    raw = "very unique hidden prompt: build a saffron drone marketplace"
    bp = AppBlueprint(name="drone_shop", mode=BuildMode.FROM_SCRATCH,
                      description=raw)
    prompt = AppBuildOrchestrator()._frame_plan(bp, None)
    assert raw not in prompt
    assert "BUILD BRIEF already handed to you" in prompt
