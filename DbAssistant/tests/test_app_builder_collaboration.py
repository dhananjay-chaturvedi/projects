"""Tests for App Builder agent collaboration enhancements."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
from ai_assistant.app_builder.build_session import (
    AnswerSession,
    BuilderSession,
    DualSessionCoordinator,
    ValidatorSession,
)
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.interaction import BuildDecider
from ai_assistant.app_builder.mediation import BuildProgress, ContextMediator
from ai_assistant.meters.app_quality_meter import AppQualityMeter
from ai_assistant.meters.quality_manager import QualityManager
from ai_assistant.meters.registry import MeterSuite


def _mock_backend():
    b = MagicMock()
    b.name = "cursor"
    return b


def test_from_scratch_review_reply_omits_mandatory_test_rules():
    med = ContextMediator(structure_enforced=False)
    review = med.review_reply(
        "Which DB?",
        "Use SQLite for the grocery catalog.",
        progress=BuildProgress(gaps=["support"], accepted=False),
    )
    rules_text = " ".join(review.injected_rules).lower()
    assert "meters/managers" not in rules_text
    assert "every change must add or update tests" not in rules_text
    # Gaps are surfaced as verify-first candidates, not hard enforcement.
    assert "verify" in rules_text or "skip any already implemented" in rules_text


def test_structure_enforced_still_requires_tests():
    med = ContextMediator(structure_enforced=True)
    review = med.review_reply(
        "Which DB?",
        "Use SQLite.",
        progress=BuildProgress(gaps=["support"], accepted=False),
    )
    assert any("tests" in r.lower() for r in review.injected_rules)


def test_gap_directive_is_verify_first_not_blind_rerequest():
    """Even when enforcing structure, gaps must be verified, not blindly re-asked."""
    med = ContextMediator(structure_enforced=True)
    review = med.review_reply(
        "Which DB?",
        "Use SQLite.",
        progress=BuildProgress(gaps=["customers", "orders"], accepted=False),
    )
    gap_rules = [r for r in review.injected_rules
                 if "customers" in r and "orders" in r]
    assert gap_rules, "expected a directive that lists the detected gaps"
    text = gap_rules[0].lower()
    assert "still genuinely missing" in text
    assert "already implemented" in text


def test_user_request_context_marks_gaps_as_candidates(tmp_path):
    med = ContextMediator()
    ctx = med.user_request_context(
        "add a refund flow",
        progress=BuildProgress(gaps=["refunds"], accepted=False),
    )
    assert "auto-detected" in ctx.lower()
    assert "do not re-request anything already implemented" in ctx.lower()


def test_app_quality_meter_standard_checks():
    good = {
        "src/app.py": (
            "from fastapi import FastAPI\napp = FastAPI()\n"
            "@app.get('/health')\ndef health(): return {'status': 'ok'}\n"
        ),
        "tests/test_app.py": "def test_health(): assert True\n",
        "tests/sample_data.py": "DATA = {}\n",
        "README.md": "# app\n",
    }
    m = AppQualityMeter().measure(good, description="grocery store")
    assert m.evidence["checks"]["runnable_contract"]
    assert m.evidence["checks"]["health_endpoint"]
    assert m.evidence["checks"]["tests_present"]
    assert m.score >= 0.7


def test_quality_manager_flags_off_topic_reply():
    qm = QualityManager(threshold=0.5)
    low = qm.review_advisor_reply(
        "build a grocery ecommerce app",
        "Build a generic electronics storefront with GPUs.",
    )
    assert not low.aligned
    assert low.nudge


def test_quality_manager_accepts_on_topic_reply():
    qm = QualityManager(threshold=0.3)
    ok = qm.review_advisor_reply(
        "build a grocery ecommerce app",
        "Build a grocery store with fresh produce and checkout.",
    )
    assert ok.aligned


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_user_request")
def test_queue_user_message_and_deliver(frame, _run, tmp_path):
    frame.return_value = "Fix the cart bug in checkout."
    builder = BuilderSession(_mock_backend(), tmp_path)
    answerer = AnswerSession(_mock_backend(), tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))
    item = coord.queue_user_message("cart is broken")
    assert item["framed"]
    assert len(coord._user_queue) == 1
    with patch.object(BuilderSession, "send") as send:
        delivered = coord.deliver_user_messages()
    assert delivered is not None
    assert any("cart is broken" in c.args[0] for c in send.call_args_list)


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_user_request")
def test_user_queue_held_while_a_has_pending_question(frame, _run, tmp_path):
    frame.return_value = "guidance"
    builder = BuilderSession(_mock_backend(), tmp_path)
    answerer = AnswerSession(_mock_backend(), tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))
    coord.queue_user_message("help")
    builder.last_events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT, "Which database should I use?")]
    assert coord.deliver_user_messages() is None
    assert len(coord._user_queue) == 1


@patch("ai_assistant.app_builder.build_session.AgentRunner.run", return_value=[])
@patch.object(AnswerSession, "frame_answer")
def test_b_progress_check_gated_and_capped(frame, _run, tmp_path):
    frame.return_value = "Continue building the checkout flow."
    builder = BuilderSession(_mock_backend(), tmp_path)
    answerer = AnswerSession(_mock_backend(), tmp_path)
    brief = make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH))
    coord = DualSessionCoordinator(
        builder, answerer, brief, BuildDecider(uninterrupted=True))
    coord.max_progress_checks = 1
    assert coord.b_progress_check(no_progress=True, no_progress_streak=1) is None
    n1 = coord.b_progress_check(no_progress=True, no_progress_streak=2)
    assert n1
    assert len(coord._user_queue) == 1
    n2 = coord.b_progress_check(no_progress=True, no_progress_streak=3)
    assert n2 is None


@patch("ai_assistant.app_builder.build_session.AgentRunner.run",
       return_value=[AgentEvent(AgentEventType.ASSISTANT_TEXT,
                                "TEST PLAN:\n- health\n- checkout")])
def test_validator_prepare_test_plan(_run, tmp_path):
    brief = make_brief(AppBlueprint(
        name="shop", mode=BuildMode.FROM_SCRATCH,
        description="grocery store"))
    plan = ValidatorSession(_mock_backend(), tmp_path).prepare_test_plan(
        "grocery store", brief=brief)
    assert "health" in plan.lower() or "TEST PLAN" in plan


def test_evaluate_app_quality_in_suite():
    suite = MeterSuite()
    out = suite.evaluate_app_quality({
        "src/app.py": "from fastapi import FastAPI\napp=FastAPI()\n",
    })
    assert "checks" in out
    assert "score" in out


def test_validator_context_from_scratch_notes_freedom():
    med = ContextMediator(
        structure_enforced=False,
        brief=make_brief(AppBlueprint(name="x", mode=BuildMode.FROM_SCRATCH)),
    )
    ctx = med.validator_context()
    assert "STRUCTURE FREEDOM" in ctx
    assert "pre-decided frameworks" in ctx
