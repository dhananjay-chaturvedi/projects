"""Tests for Session C (validator): the third persistent build session.

Covers the validator session itself, the clean-verdict guard, the C→B→A relay
in the coordinator, the mediator's validation framing, and the code-computed
evidence digest the App Builder Assistant hands to Session C.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.agent_runner import AgentEvent, AgentEventType
from ai_assistant.app_builder.build_session import (
    AnswerSession,
    BuilderSession,
    DualSessionCoordinator,
    ValidatorSession,
    validation_is_clean,
)
from ai_assistant.app_builder.decision import build_requirement_model
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.interaction import BuildDecider
from ai_assistant.app_builder.mediation import BuildProgress, ContextMediator
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator


def _mock_backend():
    b = MagicMock()
    b.name = "cursor"
    return b


def _brief():
    return make_brief(AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH,
                                   description="ecommerce store"))


# ── clean-verdict guard ──────────────────────────────────────────────────────
def test_validation_is_clean_heuristic():
    assert validation_is_clean("VERDICT: complete")
    assert validation_is_clean("verdict: complete\nLooks production ready.")
    assert not validation_is_clean("VERDICT: incomplete\n- add checkout")
    assert not validation_is_clean("VERDICT: complete\n- but add tests first")
    assert not validation_is_clean("")
    assert not validation_is_clean("I think it is mostly fine")  # no verdict


def test_validator_authors_tests_read_only_and_returns_text(tmp_path):
    """C authors tests as TEXT on its read-only (ASK) session — never writable.

    C must never get a write-capable session (it could otherwise touch Session
    A's code, and the plan/similarity turns must stay read-only). It returns the
    file contents; the orchestrator does the writing, sandboxed to its folder.
    """
    from ai_assistant.app_builder.agent_runner import AgentMode

    v = ValidatorSession(_mock_backend(), tmp_path)
    assert v._runner._mode == AgentMode.ASK  # judging session is read-only
    assert not hasattr(v, "_writer")  # no write-capable session exists at all
    events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT,
        "validator_generated_tests/test_smoke.py\n```python\n"
        "def test_health():\n    assert True\n```")]
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=events) as run:
        out = v.author_tests(brief=_brief(), structure="src/app.py", symbols="app")
    # Authoring runs in ASK mode and returns the file text (no disk write here).
    assert run.call_args.kwargs.get("mode") == AgentMode.ASK
    assert "test_health" in out


def test_parse_validator_test_files_scopes_to_folder():
    from ai_assistant.app_builder.build_session import parse_validator_test_files

    text = (
        "validator_generated_tests/test_a.py\n```python\ndef test_a(): pass\n```\n"
        "`validator_generated_tests/test_b.py`:\n```python\ndef test_b(): pass\n```\n"
        "conftest.py\n```python\nimport pytest\n```\n"
        "src/app.py\n```python\nHACK = 1\n```\n"
        "../escape.py\n```python\nHACK = 2\n```\n")
    files = dict(parse_validator_test_files(text, folder="validator_generated_tests"))
    assert "validator_generated_tests/test_a.py" in files
    assert "validator_generated_tests/test_b.py" in files
    # bare filename is placed inside the folder
    assert "validator_generated_tests/conftest.py" in files
    # anything pointing outside the folder is dropped
    assert "src/app.py" not in files
    assert all("escape" not in p for p in files)


def test_orchestrator_writes_validator_tests_only_in_folder(tmp_path):
    from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

    orch = AppBuildOrchestrator()
    files = [
        (f"{VALIDATOR_TEST_DIR}/test_x.py", "def test_x(): pass\n"),
        ("src/app.py", "HACK = 1\n"),  # must be refused by the path guard
    ]
    written = orch._write_validator_tests(tmp_path, files)
    assert written == [f"{VALIDATOR_TEST_DIR}/test_x.py"]
    assert (tmp_path / VALIDATOR_TEST_DIR / "test_x.py").exists()
    assert not (tmp_path / "src" / "app.py").exists()


def test_syntax_check_drops_and_deletes_broken_test_files(tmp_path):
    """A malformed test file is dropped + deleted so it can't break collection."""
    from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

    orch = AppBuildOrchestrator()
    good = f"{VALIDATOR_TEST_DIR}/test_ok.py"
    bad = f"{VALIDATOR_TEST_DIR}/test_bad.py"
    orch._write_validator_tests(tmp_path, [
        (good, "def test_ok():\n    assert True\n"),
        (bad, "def test_bad(:\n    pass\n"),  # syntax error
    ])
    broken = orch._syntax_check_files(tmp_path, [good, bad])
    assert broken == [bad]
    assert (tmp_path / good).exists()  # valid file kept
    assert not (tmp_path / bad).exists()  # malformed file removed


# ── the validator session ────────────────────────────────────────────────────
def test_validator_validate_returns_concise_verdict(tmp_path):
    events = [AgentEvent(AgentEventType.ASSISTANT_TEXT,
                         "VERDICT: incomplete\n- missing checkout tests")]
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=events):
        v = ValidatorSession(_mock_backend(), tmp_path)
        out = v.validate("build score 0.5", brief=_brief())
    assert "incomplete" in out.lower()
    assert "checkout" in out.lower()


# ── C → B → A relay in the coordinator ───────────────────────────────────────
def _coord(tmp_path, *, mediator=None):
    backend = _mock_backend()
    return DualSessionCoordinator(
        BuilderSession(backend, tmp_path),
        AnswerSession(backend, tmp_path),
        _brief(),
        BuildDecider(uninterrupted=True),
        validator=ValidatorSession(backend, tmp_path),
        mediator=mediator,
        progress=BuildProgress(gaps=["checkout"]),
    )


def test_relay_validation_clean_skips_advisor_and_builder(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: complete"), \
                patch.object(AnswerSession, "frame_answer") as frame, \
                patch.object(BuilderSession, "send") as send:
            rec = coord.relay_validation("digest")
    assert rec["clean"] is True
    frame.assert_not_called()   # no tokens spent on B
    send.assert_not_called()    # builder not interrupted


def test_relay_validation_green_queues_proceed_note(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: complete"), \
                patch.object(AnswerSession, "frame_answer",
                             return_value="API verified — proceed to web.") as frame, \
                patch.object(BuilderSession, "send") as send:
            rec = coord.relay_validation(
                "digest", green_relay=True, component="api")
            assert rec["clean"] is True
            assert rec["queued"] is True
            assert rec.get("green") is True
            frame.assert_called_once()
            send.assert_not_called()
            assert len(coord._feedback_queue) == 1
            delivered = coord.deliver_feedback([])
            assert delivered is not None
            assert any("VALIDATION FEEDBACK" in c.args[0]
                       for c in send.call_args_list)


def test_relay_validation_issues_queue_then_deliver_c_to_b_to_a(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: incomplete\n- add checkout tests"), \
                patch.object(AnswerSession, "frame_answer",
                             return_value="Add unit tests for checkout next.") as frame, \
                patch.object(BuilderSession, "send") as send:
            rec = coord.relay_validation("digest")
            # C→B framing happens immediately, but A is NOT interrupted yet —
            # B's advice is queued for ordered delivery.
            assert rec["clean"] is False
            assert rec["queued"] is True
            frame.assert_called_once()
            send.assert_not_called()
            assert len(coord._feedback_queue) == 1

            # A is free (no pending question) → deliver one queued note to A.
            delivered = coord.deliver_feedback()
            assert delivered is not None
            assert len(coord._feedback_queue) == 0
            assert any("VALIDATION FEEDBACK" in c.args[0]
                       for c in send.call_args_list)


def test_deliver_feedback_holds_while_builder_has_pending_question(tmp_path):
    """An unsolicited note must wait while A is waiting for its own answer."""
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: incomplete\n- add checkout tests"), \
                patch.object(AnswerSession, "frame_answer",
                             return_value="Add unit tests for checkout next."), \
                patch.object(BuilderSession, "send") as send:
            coord.relay_validation("digest")
            # A is mid-question (waiting on a bound answer): hold the queue.
            pending = [AgentEvent(AgentEventType.QUESTION,
                                  "Which payment provider should I use?")]
            assert coord.deliver_feedback(pending) is None
            assert len(coord._feedback_queue) == 1
            send.assert_not_called()
            # Once A is free, the queued note is delivered (still in order).
            assert coord.deliver_feedback([]) is not None
            assert len(coord._feedback_queue) == 0


def test_relay_validation_final_does_not_nudge_builder(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: incomplete\n- add tests"), \
                patch.object(AnswerSession, "frame_answer") as frame, \
                patch.object(BuilderSession, "send") as send:
            rec = coord.relay_validation("digest", relay=False)
    assert rec["relayed"] is False
    frame.assert_not_called()
    send.assert_not_called()


def test_start_primes_all_three_sessions(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]) as run:
        coord = _coord(tmp_path)
        coord.start()
    assert run.call_count == 3  # A, B and C all get the governance brief


# ── mediator framing for the validator ───────────────────────────────────────
def test_validator_context_includes_scope_progress_and_expectation():
    rm = build_requirement_model("ecommerce store to sell electronics",
                                 entities=["product"], features=["list"])
    med = ContextMediator(requirement_model=rm)
    prog = BuildProgress(phase="testing", round=2, coverage=0.5, gaps=["checkout"])
    ctx = med.validator_context(
        progress=prog, history="built product models",
        test_scope="entities: product", how_to_test="run the pytest suite",
        framed_brief="Build a store with cart and checkout",
        component="api")
    assert "VALIDATION BRIEF" in ctx
    assert "checkout" in ctx
    assert "TESTING SCOPE" in ctx
    assert "HOW TO TEST" in ctx
    assert "validator" in ctx.lower()
    assert "BUILD BRIEF (from advisor)" in ctx
    assert "COMPONENT UNDER TEST: api" in ctx


def test_validation_to_advice_carries_findings_and_open_gaps():
    med = ContextMediator()
    out = med.validation_to_advice(
        "- add checkout tests", progress=BuildProgress(gaps=["checkout"]))
    assert "VALIDATOR FINDINGS" in out
    assert "checkout" in out
    assert "OPEN REQUIREMENTS" in out


# ── code-computed evidence digest (no tokens spent) ──────────────────────────
def test_validation_digest_summarizes_evidence():
    orch = AppBuildOrchestrator()
    orch._insight = None
    orch._req_entities = ["product"]
    orch._req_features = ["list"]
    verdict = SimpleNamespace(score=0.8, accepted=True, issues=["thin tests"])
    cov = {"score": 0.6, "gaps": ["checkout"]}
    digest = orch._validation_digest(
        {"src/app.py": "code"}, verdict, cov, {"passed": True, "summary": "1 passed"},
        component="api", test_paths=["tests/api"])
    assert "component: api" in digest
    assert "requirement coverage: 0.60" in digest
    assert "checkout" in digest
    assert "PASSED" in digest
    assert "thin tests" in digest
    assert "tests/api" in digest


def test_components_touched_maps_files_and_phase_done():
    orch = AppBuildOrchestrator()
    comps = orch._components_touched(
        ["src/api/routes.py", "templates/index.html"],
        phase_done=["api"])
    assert comps[0] == "api"
    assert "web" in comps


def test_components_touched_infers_free_form_paths():
    orch = AppBuildOrchestrator()
    comps = orch._components_touched(
        ["backend/routes_orders.py", "models/product.py", "test_checkout.py"])
    assert "api" in comps
    assert "db" in comps
    assert "tests" in comps


def test_status_preface_includes_progress_and_gaps(tmp_path):
    coord = _coord(tmp_path)
    coord.progress = BuildProgress(
        phase="api", round=2, coverage=0.5, gaps=["checkout"])
    pre = coord.status_preface()
    assert "BUILD STATUS" in pre
    assert "checkout" in pre


def test_route_user_request_goes_through_b_to_a(tmp_path):
    relays: list[dict] = []
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path, mediator=ContextMediator())
        coord.on_relay = relays.append
        with patch.object(AnswerSession, "frame_user_request",
                          return_value="Add a cart page with checkout.") as frame, \
                patch.object(BuilderSession, "send") as send:
            out = coord.route_user_request("add shopping cart")
    assert "cart" in out
    frame.assert_called_once()
    directions = [r["direction"] for r in relays]
    assert "user_to_b" in directions
    assert "b_to_a" in directions
    assert "c_to_b" not in directions
    # A receives the user's verbatim message AND B's contextual guidance.
    assert any("USER MESSAGE (relayed via advisor)" in c.args[0]
               and "add shopping cart" in c.args[0]
               for c in send.call_args_list)
    assert any("ADVISOR GUIDANCE" in c.args[0] for c in send.call_args_list)


def test_frame_user_request_engages_with_user_content(tmp_path):
    events = [AgentEvent(
        AgentEventType.ASSISTANT_TEXT,
        "START! The /checkout route 500s because the cart total is None — have "
        "the builder guard the empty-cart case and add a test. DONE!")]
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=events):
        b = AnswerSession(_mock_backend(), tmp_path)
        out = b.frame_user_request(
            "the checkout page crashes", brief=_brief())
    assert "checkout" in out.lower()
    assert "requirements are not fulfilled" not in out.lower()


def test_route_user_request_does_not_apply_gap_directives(tmp_path):
    """A user bug report must not be overwritten by generic gap directives."""
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path, mediator=ContextMediator())
        with patch.object(AnswerSession, "frame_user_request",
                          return_value="Guard the empty-cart total in /checkout.") \
                as frame, \
                patch.object(BuilderSession, "send") as send:
            coord.route_user_request("checkout crashes")
        frame.assert_called_once()
        sent = "\n".join(c.args[0] for c in send.call_args_list)
    assert "Guard the empty-cart total" in sent
    assert "close these requirements" not in sent.lower()


def test_finalize_agreement_complete_when_all_agree(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: complete"), \
                patch.object(AnswerSession, "frame_confirm_completion",
                             return_value="Ready for the user to start and verify."):
            agr = coord.finalize_agreement(
                "digest", meters_ok=True, agent_done=True)
    assert agr["complete"] is True
    assert agr["c_clean"] is True
    assert agr["b_confirms"] is True


def test_finalize_agreement_incomplete_when_validator_fails(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        coord = _coord(tmp_path)
        with patch.object(ValidatorSession, "validate",
                          return_value="VERDICT: incomplete\n- missing tests"), \
                patch.object(AnswerSession, "frame_confirm_completion",
                             return_value="Not ready — add tests first."):
            agr = coord.finalize_agreement("digest", meters_ok=True)
    assert agr["complete"] is False
    assert "validator" in agr["issues"][0]


def test_orchestrator_validation_mode_defaults_low_token():
    orch = AppBuildOrchestrator()
    assert orch.validation_mode == "low_token"
    orch2 = AppBuildOrchestrator(validation_mode="thorough")
    assert orch2.validation_mode == "thorough"
    assert orch2.THOROUGH_PHASES == ("api", "db", "web", "tests")


def test_test_paths_for_components_resolves_existing(tmp_path):
    orch = AppBuildOrchestrator()
    (tmp_path / "tests" / "api").mkdir(parents=True)
    (tmp_path / "tests" / "api" / "__init__.py").write_text("")
    paths = orch._test_paths_for_components(tmp_path, ["api", "missing"])
    assert "tests/api" in paths


def test_marked_ask_routed_with_intent(tmp_path):
    with patch("ai_assistant.app_builder.build_session.AgentRunner.run",
               return_value=[]):
        backend = _mock_backend()
        builder = BuilderSession(backend, tmp_path)
        answerer = AnswerSession(backend, tmp_path)
        coord = DualSessionCoordinator(
            builder, answerer, _brief(), BuildDecider(uninterrupted=True))
        builder.last_events = [AgentEvent(
            AgentEventType.ASSISTANT_TEXT,
            "ASK: Which payment provider should we integrate?")]
        with patch.object(AnswerSession, "frame_answer",
                          return_value="Use Stripe.") as frame:
            routed = coord.route_questions()
        assert len(routed) == 1
        assert routed[0]["kind"] in ("decide", "open")
        frame.assert_called_once()
