"""Tests for the App Builder Assistant mediating between Session 1 and 2."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
from ai_assistant.app_builder.build_session import (
    AnswerSession,
    BuilderSession,
    DualSessionCoordinator,
)
from ai_assistant.app_builder.decision import build_requirement_model
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.interaction import BuildDecider
from ai_assistant.app_builder.mediation import (
    BuildProgress,
    ContextMediator,
    ReplyReview,
)


def _mediator():
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="a scalable store for millions of users, "
                                  "high traffic")
    model = build_requirement_model(bp.description)
    return ContextMediator(requirement_model=model, brief=make_brief(bp))


def test_advisor_context_includes_focus_progress_and_expectation():
    med = _mediator()
    progress = BuildProgress(phase="coverage", round=2, coverage=0.6,
                             score=0.82, files_built=18,
                             gaps=["entity 'order' missing API"])
    brief = med.advisor_context(
        "Which database should I use, SQLite or PostgreSQL?",
        builder_text="I scaffolded models.\nWhich database should I use, "
                     "SQLite or PostgreSQL?",
        progress=progress)
    assert "mediation brief for the ADVISOR" in brief
    assert "REQUIREMENT FOCUS" in brief and "scalability" in brief
    assert "BUILD PROGRESS: round 2 [coverage]" in brief
    assert "entity 'order' missing API" in brief
    assert "THE BUILDER'S QUESTION:" in brief
    assert "WHAT WE NEED FROM YOU" in brief
    assert "no code" in brief


def test_advisor_context_includes_builder_history():
    med = _mediator()
    brief = med.advisor_context(
        "Which cache should I use?",
        builder_text="Now wiring the product list.\nWhich cache should I use?",
        history="Earlier I created the catalog model and the checkout route.",
        progress=BuildProgress(round=3, coverage=0.7))
    assert "WHAT THE BUILDER HAS DONE SO FAR (history):" in brief
    assert "catalog model and the checkout route" in brief
    assert "WHERE IN THE BUILD (current context):" in brief
    assert "ASK INTENT:" in brief


def test_review_reply_flags_code_and_injects_no_code_rule():
    med = _mediator()
    review = med.review_reply(
        "How should I store orders?",
        "Use this:\n```python\ndef save(): ...\n```",
        progress=BuildProgress(accepted=True))
    assert not review.aligned
    assert any("code" in i for i in review.issues)
    assert any("no code" in r.lower() or "do not write code" in r.lower()
               for r in review.injected_rules)


def test_review_reply_flags_premature_completion_with_open_gaps():
    med = _mediator()
    review = med.review_reply(
        "Anything else?",
        "Looks good — the app is complete and all tests pass.",
        progress=BuildProgress(accepted=False, gaps=["feature 'search' missing"]))
    assert not review.aligned
    assert any("not complete" in r.lower() for r in review.injected_rules)
    # open gaps are surfaced as a directive
    assert any("search" in r for r in review.injected_rules)


def test_review_reply_requires_testing_when_incomplete():
    med = _mediator()
    review = med.review_reply(
        "Which DB?", "Use PostgreSQL for reliability.",
        progress=BuildProgress(accepted=False, gaps=["entity 'order' missing API"]))
    assert any("test" in r.lower() for r in review.injected_rules)


def test_review_reply_aligned_when_clean_and_complete():
    med = _mediator()
    review = med.review_reply(
        "Ready to ship?",
        "Yes — performance and scalability targets are met; tests pass.",
        progress=BuildProgress(accepted=True, gaps=[]))
    assert review.aligned
    assert isinstance(review, ReplyReview)


def test_directives_text_blocks_are_appended_form():
    med = _mediator()
    assert med.directives_text([]) == ""
    out = med.directives_text(["Rule one.", "Rule two."])
    assert "App Builder Assistant directives" in out
    assert "- Rule one." in out and "- Rule two." in out


def test_question_context_extracts_surrounding_lines():
    med = _mediator()
    builder_text = (
        "Created the catalog page.\n"
        "Added the cart model.\n"
        "Which payment provider should I integrate?\n"
        "Then I will wire checkout."
    )
    ctx = med.question_context(
        "Which payment provider should I integrate?", builder_text)
    assert "cart model" in ctx
    assert "Which payment provider" in ctx


def test_targets_line_only_shows_notable_targets():
    med = _mediator()  # high-scale requirement → scale_class=high notable
    line = med._targets_line()
    assert "scale_class=high" in line
    assert "standard" not in line


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
def test_coordinator_passes_mediated_context_to_advisor(_run, tmp_path):
    backend = MagicMock()
    backend.name = "cursor"
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="a scalable store for millions of users")
    brief = make_brief(bp)
    med = ContextMediator(
        requirement_model=build_requirement_model(bp.description), brief=brief)
    progress = BuildProgress(phase="build", round=1, coverage=0.5, score=0.8,
                             files_built=12, gaps=["feature 'search' missing"])
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        mediator=med, progress=progress)

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT,
        "Scaffolded models. Which database should I use?")]

    with patch.object(AnswerSession, "frame_answer",
                      return_value="Use PostgreSQL.") as frame:
        coord.route_questions()

    assert frame.called
    ctx = frame.call_args.kwargs["context"]
    # Session B received the mediation brief, not a raw transcript tail.
    assert "mediation brief for the ADVISOR" in ctx
    assert "feature 'search' missing" in ctx
    assert "BUILD PROGRESS: round 1" in ctx


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
def test_coordinator_reviews_reply_and_injects_directives(_run, tmp_path):
    backend = MagicMock()
    backend.name = "cursor"
    builder = BuilderSession(backend, tmp_path)
    answerer = AnswerSession(backend, tmp_path)
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                      description="a scalable store for millions of users")
    brief = make_brief(bp)
    med = ContextMediator(
        requirement_model=build_requirement_model(bp.description), brief=brief)
    progress = BuildProgress(phase="build", round=1, coverage=0.5, score=0.8,
                             accepted=False, files_built=12,
                             gaps=["feature 'search' missing"])
    reviews: list[dict] = []
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True),
        mediator=med, progress=progress, on_review=reviews.append)

    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which database should I use?")]

    # Advisor reply omits testing while the build is incomplete → re-aligned.
    with patch.object(AnswerSession, "frame_answer",
                      return_value="Use PostgreSQL for reliability."):
        routed = coord.route_questions()

    assert routed
    answer = routed[0]["answer"]
    assert "App Builder Assistant directives" in answer
    assert "search" in answer  # open gap surfaced to the builder
    assert reviews and reviews[0]["injected_rules"]
