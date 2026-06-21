"""Tests for dual-session App Builder coordination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
from ai_assistant.app_builder.build_session import (
    AnswerSession,
    BuilderSession,
    DualSessionCoordinator,
    ValidatorSession,
)
from ai_assistant.app_builder.decision import build_requirement_model
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.interaction import BuildDecider, UNINTERRUPTED, INTERACTIVE
from ai_assistant.app_builder.mediation import BuildProgress, ContextMediator


def _mock_backend():
    b = MagicMock()
    b.name = "cursor"
    return b


def test_governance_brief_rendered_for_both_roles():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="ecommerce store")
    brief = make_brief(bp)
    assert "GOVERNANCE BRIEF" in brief.render(role="builder")
    assert "YOUR ROLE" in brief.render(role="answerer")
    # The user requirement must reach EVERY role — especially the builder, so
    # Session A is never primed blind (only seeing the app name).
    assert "ecommerce store" in brief.render(role="builder")
    assert "ecommerce store" in brief.render(role="answerer")
    assert "ecommerce store" in brief.render(role="validator")


def test_builder_brief_includes_user_requirement_at_prime():
    bp = AppBlueprint(name="myapp", mode=BuildMode.FROM_SCRATCH,
                      description="a grocery mart chain management app")
    text = make_brief(bp).render(role="builder")
    assert "USER REQUIREMENT (what to build): a grocery mart chain" in text
    # With the domain defined, A is told NOT to ask the user to pick a domain.
    assert "do NOT ask the user to pick/define a domain" in text


def test_builder_brief_without_description_waits_for_brief():
    bp = AppBlueprint(name="myapp", mode=BuildMode.FROM_SCRATCH)
    text = make_brief(bp).render(role="builder")
    assert "USER REQUIREMENT: not provided yet" in text
    assert "BUILD BRIEF" in text


def test_validator_brief_is_test_author_not_app_builder():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="ecommerce store")
    text = make_brief(bp).render(role="validator")
    # C authors tests in its folder; it must never be told to build the app.
    assert "build the complete working application" not in text
    assert "VALIDATOR" in text
    assert "validator_generated_tests" in text
    assert "TEST AUTHOR" in text
    assert "USER REQUIREMENT" in text


def test_builder_brief_documents_communication_protocol():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="ecommerce store")
    text = make_brief(bp).render(role="builder")
    assert "PHASE-DONE:" in text
    assert "ASK:" in text and "APPROVE:" in text
    assert "STRUCTURE FREEDOM" in text
    assert "src.app:app" in text


def test_advisor_context_is_intent_first():
    rm = build_requirement_model("ecommerce store", entities=["product"])
    med = ContextMediator(requirement_model=rm)
    ctx = med.advisor_context(
        "Which database should I use?",
        builder_text="Building models now.",
        history="laid skeleton",
        progress=BuildProgress(phase="db", round=2),
        kind="decide",
    )
    ask_pos = ctx.index("ASK INTENT")
    req_pos = ctx.index("USER REQUIREMENT")
    prog_pos = ctx.index("BUILD PROGRESS")
    assert ask_pos < req_pos < prog_pos


def test_builder_brief_from_scratch_requires_session_agreement():
    bp = AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH)
    text = make_brief(bp).render(role="builder")
    assert "Session C validates" in text
    assert "Session B agrees" in text


def test_builder_brief_demands_production_functional_app():
    """The builder is told to ship a working product with sample data, not a mockup."""
    for mode in (BuildMode.FROM_SCRATCH, BuildMode.FROM_DATABASE):
        bp = AppBlueprint(name="shop", mode=mode, description="ecommerce store",
                          connections=["local"])
        text = make_brief(bp).render(role="builder")
        assert "PRODUCTION-FUNCTIONAL CONTRACT" in text
        assert "END TO END" in text
        assert "SAMPLE DATA" in text and "SEED" in text
        assert "RICH UX" in text


def test_reviewer_and_validator_briefs_check_functionality():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="ecommerce store")
    reviewer = make_brief(bp).render(role="answerer")
    assert "PRODUCTION-FUNCTIONAL CONTRACT" in reviewer
    validator = make_brief(bp).render(role="validator")
    assert "REAL functionality" in validator
    assert "sample data is" in validator


def test_builder_brief_from_scratch_allows_free_structure():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="ecommerce store")
    text = make_brief(bp).render(role="builder")
    assert "STRUCTURE FREEDOM" in text
    assert "PROJECT SKELETON" not in text
    assert "create this folder structure FIRST" not in text
    assert "tests/unit_test/" in text  # advisory, optional


def test_minimal_prime_from_database_excludes_requirement_and_schema():
    """FROM_DATABASE: A/C get role-only prime; B still gets the full brief."""
    bp = AppBlueprint(
        name="fleet", mode=BuildMode.FROM_DATABASE,
        description="fleet tracker", connections=["local"],
    )
    brief = make_brief(
        bp, schema={"vehicles": ["id", "name"]},
        data_insight="fleet ops data")
    builder_text = brief.render_minimal(role="builder")
    validator_text = brief.render_minimal(role="validator")
    answerer_text = brief.render(role="answerer")
    assert "MINIMAL" in builder_text
    assert "Session B" in builder_text
    assert "USER REQUIREMENT" not in builder_text
    assert "schema:" not in builder_text
    assert "data understanding:" not in builder_text
    assert "MINIMAL" in validator_text
    assert "Session B" in validator_text
    assert "fleet tracker" in answerer_text or "fleet" in answerer_text


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
def test_from_database_start_primes_a_c_minimally_b_fully(mock_run, tmp_path):
    backend = _mock_backend()
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    validator = ValidatorSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(
        name="fleet", mode=BuildMode.FROM_DATABASE, connections=["local"]))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        validator=validator)
    coord.start()
    assert mock_run.call_count == 3
    texts = [c.args[0] for c in mock_run.call_args_list]
    assert "MINIMAL" in texts[0]   # Session A
    assert "MINIMAL" not in texts[1]  # Session B — full brief
    assert "MINIMAL" in texts[2]   # Session C
    assert "GOVERNANCE BRIEF" in texts[1]


def test_builder_brief_from_database_requires_skeleton_first():
    bp = AppBlueprint(
        name="shop", mode=BuildMode.FROM_DATABASE,
        description="ecommerce store", connections=["local"],
    )
    text = make_brief(bp).render(role="builder")
    assert "PROJECT SKELETON" in text
    for d in ("unit_test", "full_test", "api", "db"):
        assert f"tests/{d}/" in text, d


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
def test_dual_session_starts_with_governance(mock_run, tmp_path):
    backend = _mock_backend()
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(builder, answerer, brief,
                                   BuildDecider(uninterrupted=True))
    coord.start()
    assert mock_run.call_count == 2  # governance brief pushed to A and B


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_uninterrupted_routes_questions_via_answerer(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Use SQLite for simplicity."

    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    decider = BuildDecider(level=UNINTERRUPTED, uninterrupted=True)
    coord = DualSessionCoordinator(builder, answerer, brief, decider)

    # The builder's latest turn contains a genuine question.
    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which database should I use?")]
    routed = coord.route_questions()
    assert routed
    assert routed[0]["answer"] == "Use SQLite for simplicity."
    assert _run.called


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_genuine_question_answered_once_then_deduped(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Use SQLite."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which database should I use?")]
    first = coord.route_questions()
    second = coord.route_questions()  # same question again → not re-answered
    assert len(first) == 1
    assert second == []


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_narration_is_not_treated_as_question(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "x"
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT,
        "I created the model and the routes. Awaiting the next step.")]
    assert coord.route_questions() == []  # no '?' → nothing answered


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_permission_request_auto_approved(frame, _run, tmp_path):
    backend = _mock_backend()
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))

    builder.last_events = [AgentEvent(
        AgentEventType.QUESTION, "permission_request", {"permission": True})]
    routed = coord.route_questions()
    assert len(routed) == 1
    assert routed[0]["kind"] == "permission"
    assert "Approved" in routed[0]["answer"]
    frame.assert_not_called()  # permission auto-approved, no Session B needed


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_cursor_interaction_question_routes_through_session_b(
        frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Use email lookup; it works without accounts."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))

    builder.last_events = [AgentEvent(
        AgentEventType.QUESTION,
        "How should order history work, given there are no user accounts?\n"
        "Options:\n- email: Look up orders by checkout email\n"
        "- session: Track browser session",
        {
            "interaction_question": True,
            "prompt": (
                "How should order history work, given there are no user "
                "accounts?"
            ),
        },
    )]
    with patch.object(BuilderSession, "send") as send:
        routed = coord.route_questions()
    assert len(routed) == 1
    assert routed[0]["kind"] == "decide"
    assert routed[0]["request_id"].startswith("rq_")
    assert routed[0]["status"] == "delivered_to_a"
    assert "email lookup" in routed[0]["answer"]
    frame.assert_called_once()
    assert any("ANSWER [rq_" in c.args[0] and "email lookup" in c.args[0]
               for c in send.call_args_list)


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_interactive_interaction_question_uses_native_decision(
        frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Build a grocery catalog with fresh produce."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH))
    captured: list = []

    def ask(decision):
        captured.append(decision)
        return "email"  # user picked an agent option

    decider = BuildDecider(level=INTERACTIVE, ask=ask)
    coord = DualSessionCoordinator(builder, answerer, brief, decider)

    builder.last_events = [AgentEvent(
        AgentEventType.QUESTION,
        "Pick a domain:\nOptions:\n- email: Email lookup\n- session: Session",
        {
            "interaction_question": True,
            "prompt": "Pick a domain:",
            "option_items": [
                {"id": "email", "label": "Email lookup"},
                {"id": "session", "label": "Session"},
            ],
            "allow_multiple": False,
        },
    )]
    with patch.object(BuilderSession, "send") as send:
        routed = coord.route_questions()
    assert len(routed) == 1
    assert routed[0]["answer"] == "email"
    assert captured[0].kind == "interaction"
    assert captured[0].agent_options[0]["id"] == "email"
    assert captured[0].recommendation == "Build a grocery catalog with fresh produce."
    assert any("email" in c.args[0] for c in send.call_args_list)


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_relay_surfaces_question_and_answer_to_session_b(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Use SQLite for simplicity."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    relays: list[dict] = []
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        on_relay=relays.append)

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which database should I use?")]
    coord.route_questions()
    directions = [r["direction"] for r in relays]
    assert "a_to_b" in directions  # A's question shown going to B
    assert "b_to_a" in directions  # B's framed answer shown going to A
    answer = next(r for r in relays if r["direction"] == "b_to_a")
    assert answer["request_id"].startswith("rq_")
    assert "SQLite" in answer["text"]


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_atomic_question_flow_records_delivery_status(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Choose email lookup for order history."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH))
    relays: list[dict] = []
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        on_relay=relays.append)
    builder.last_events = [AgentEvent(
        AgentEventType.QUESTION,
        "How should order history work?",
        {"interaction_question": True, "prompt": "How should order history work?",
         "toolCallId": "toolu_123"},
    )]
    with patch.object(BuilderSession, "send") as send:
        routed = coord.route_questions()
    rid = routed[0]["request_id"]
    assert rid.startswith("rq_")
    assert routed[0]["status"] == "delivered_to_a"
    assert coord._handoffs[rid].status == "delivered_to_a"
    assert [r["status"] for r in relays if r.get("request_id") == rid] == [
        "received_from_a", "answered_by_b", "delivered_to_a"]
    assert any(f"ANSWER [{rid}]" in c.args[0] for c in send.call_args_list)


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_kickoff")
def test_relay_c_to_b_not_emitted_to_session_b(frame, _run, tmp_path):
    """C-internal findings stay in C; only A-bound traffic goes to B's box."""
    backend = _mock_backend()
    frame.return_value = "Fix checkout tests."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH))
    relays: list[dict] = []
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        validator=ValidatorSession(backend, tmp_path),
        on_relay=relays.append)
    with patch.object(ValidatorSession, "validate",
                      return_value="VERDICT: incomplete\n- add checkout tests"):
        coord.relay_validation("digest")
    directions = [r["direction"] for r in relays]
    assert "c_to_b" not in directions
    assert "b_to_a" in directions


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_kickoff")
def test_relay_surfaces_kickoff_brief_to_session_b(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Build a task manager with tasks and due dates."
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    relays: list[dict] = []
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        on_relay=relays.append)

    coord.kickoff("a to-do app")
    directions = [r["direction"] for r in relays]
    assert "to_b" in directions      # description handed to B for framing
    assert "b_to_a" in directions    # B's framed brief handed to A
    framed = next(r for r in relays if r["direction"] == "b_to_a")
    assert "task manager" in framed["text"]


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_interactive_uses_decider_for_questions(frame, _run, tmp_path):
    backend = _mock_backend()
    frame.return_value = "Proposed: use Postgres."

    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))

    def ask(decision):
        return "send_proposed"

    decider = BuildDecider(level=INTERACTIVE, ask=ask)
    coord = DualSessionCoordinator(builder, answerer, brief, decider)

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which DB should I use?")]
    routed = coord.route_questions()
    assert routed[0]["asked"] is True
    assert "Proposed: use Postgres." in routed[0]["answer"]
