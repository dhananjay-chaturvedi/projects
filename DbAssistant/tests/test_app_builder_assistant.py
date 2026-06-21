"""Tests for the App Builder Assistant router and the understanding phase."""

from __future__ import annotations

import threading
import time

import pytest

from ai_assistant.app_builder.assistant import (
    AppBuilderAssistant,
    RoutingError,
    Session,
)
from ai_assistant.app_builder.meters.design_plan import DesignPlan
from ai_assistant.app_builder.understanding import (
    _best_of_plans,
    run_understanding_phase,
)


class FakeBuilder:
    def __init__(self):
        self.inbox = []
        self.last_text = ""
        self.transcript = ""
        self.outline_calls = 0

    def prepare_outline(self, prompt, *, brief=None):
        self.outline_calls += 1
        return ("Plan: create src/models.py and src/routes.py. Models: customer, "
                "order. Endpoints /customers /orders. Features create, list.")

    def plan(self, prompt):
        return ("Plan: create src/models.py and src/routes.py. Models: customer, "
                "order. Endpoints /customers /orders. Features create, list.")

    def send(self, text):
        self.inbox.append(text)
        return []


class FakeAdvisor:
    def __init__(self):
        self.seen = []

    def frame_answer(self, question, *, brief=None, context=""):
        self.seen.append(question)
        if "BUSINESS DESIGN" in question:
            return ("Entities: customer and order (models). Components: model, "
                    "router, template. Features: create and list.")
        return "advisor-reply"


class FakeValidator:
    def __init__(self):
        self.inbox = []
        self.outline_calls = 0

    def send(self, text):
        self.inbox.append(text)

    def validate(self, digest, *, brief=None, context=""):
        return "verdict: ok"

    def prepare_outline(self, description, *, brief=None, context=""):
        self.outline_calls += 1
        return ("Validate customer and order models; test create and list flows. "
                "Components: model, router, test.")

    def prepare_test_plan(self, description, *, brief=None, context=""):
        return ("Validate customer and order models; test create and list flows. "
                "Components: model, router, test.")


def _assistant():
    return AppBuilderAssistant(
        builder=FakeBuilder(), advisor=FakeAdvisor(), validator=FakeValidator(),
        brief=None)


# ── routing rules ───────────────────────────────────────────────────────────────
def test_validator_cannot_reach_builder_directly():
    a = _assistant()
    # The C→A direct route is forbidden — C must always reach A via B.
    with pytest.raises(RoutingError):
        a.route(Session.C, Session.A, "do this")


def test_assistant_addresses_sessions_only_via_b():
    a = _assistant()
    with pytest.raises(RoutingError):
        a.route(Session.ASSISTANT, Session.A, "hi")
    with pytest.raises(RoutingError):
        a.route(Session.ASSISTANT, Session.C, "hi")
    # Via B is allowed.
    a.assistant_note("note for the builder", to=Session.A)
    assert a.builder.inbox  # B forwarded to A


def test_route_log_is_recorded():
    a = _assistant()
    a.route(Session.A, Session.B, "How?", intent="question")
    assert any(m.intent == "question" for m in a.log)


# ── measurement ownership ─────────────────────────────────────────────────────
def test_assistant_tracks_quality():
    a = _assistant()
    poor = {"app.py": "x = 1\n"}
    rep = a.evaluate_quality(poor, description="store with orders",
                             features=["checkout"], entities=["order"],
                             test_outcome={"summary": "no tests"})
    assert rep["overall"] < 0.5


# ── understanding phase ─────────────────────────────────────────────────────────
def test_understanding_phase_runs_in_parallel_and_gates():
    calls = []

    class SlowBuilder(FakeBuilder):
        def prepare_outline(self, prompt, *, brief=None):
            calls.append(("A", threading.get_ident()))
            time.sleep(0.05)
            return super().prepare_outline(prompt, brief=brief)

        def plan(self, prompt):
            calls.append(("A", threading.get_ident()))
            time.sleep(0.05)
            return super().plan(prompt)

    class SlowAdvisor(FakeAdvisor):
        def frame_answer(self, q, *, brief=None, context=""):
            calls.append(("B", threading.get_ident()))
            time.sleep(0.05)
            return super().frame_answer(q, brief=brief, context=context)

    class SlowValidator(FakeValidator):
        def prepare_outline(self, d, *, brief=None, context=""):
            calls.append(("C", threading.get_ident()))
            time.sleep(0.05)
            return super().prepare_outline(d, brief=brief, context=context)

    a = AppBuilderAssistant(builder=SlowBuilder(), advisor=SlowAdvisor(),
                            validator=SlowValidator(), brief=None)
    t0 = time.time()
    res = run_understanding_phase(
        a, "a CRM with customers and orders",
        entities=["customer", "order"], features=["create", "list"])
    elapsed = time.time() - t0

    assert res.ready is True
    assert res.similarity["score"] >= 0.75
    assert res.rounds == 1
    # Three distinct worker threads → genuinely concurrent.
    assert len({tid for _, tid in calls}) == 3
    # Concurrent execution is far faster than 3×0.05s serial.
    assert elapsed < 0.12
    # Ask-mode outline path used for builder (not full plan/build turn).
    assert a.builder.outline_calls == 1
    # The agreed design is the best-of-three merge.
    assert "customer" in a.design.entities and "order" in a.design.entities


def test_understanding_phase_single_round_no_reconcile():
    """Default max_reconcile=0: one prep call per session, no second round."""
    prep_calls = {"A": 0, "B": 0, "C": 0}

    class CountingBuilder(FakeBuilder):
        def prepare_outline(self, prompt, *, brief=None):
            prep_calls["A"] += 1
            return "Plan: alpha widget models."

    class CountingAdvisor(FakeAdvisor):
        def frame_answer(self, q, *, brief=None, context=""):
            if "BUSINESS DESIGN" in q or "ADVISOR" in q:
                prep_calls["B"] += 1
                return "Entities: beta gadget. Components: ledger."
            return "noted"

    class CountingValidator(FakeValidator):
        def prepare_outline(self, d, *, brief=None, context=""):
            prep_calls["C"] += 1
            return "Validate gamma sprocket flows."

    a = AppBuilderAssistant(builder=CountingBuilder(), advisor=CountingAdvisor(),
                            validator=CountingValidator(), brief=None)
    res = run_understanding_phase(a, "x")
    assert res.rounds == 1
    assert prep_calls == {"A": 1, "B": 1, "C": 1}
    assert res.agreed_design is not None


def test_best_of_plans_merges_role_contributions():
    plans = {
        "A": DesignPlan(
            role="builder", files={"src/app.py"}, endpoints={"/api"},
            components={"route"}),
        "B": DesignPlan(
            role="advisor", entities={"product"}, features={"create"},
            components={"service"}),
        "C": DesignPlan(
            role="validator", components={"test"}, features={"list"}),
    }
    merged = _best_of_plans(plans)
    assert "src/app.py" in merged.files
    assert "/api" in merged.endpoints
    assert "product" in merged.entities
    assert "create" in merged.features
    assert "test" in merged.components


def test_understanding_phase_from_b_instruction_not_raw_description():
    """FROM_DATABASE: A follows B's instruction (no read-only A turn); C restates."""
    captured: dict[str, str] = {}

    class InstrBuilder(FakeBuilder):
        def prepare_outline(self, prompt, *, brief=None):
            captured["builder_prompt"] = prompt
            return "Outline: fleet map and vehicle list pages."

    class InstrValidator(FakeValidator):
        def prepare_outline(self, d, *, brief=None, context=""):
            captured["validator_context"] = context
            return "Validate fleet flows and sample data."

    b_instr = (
        "SESSION B — BUILD INSTRUCTION:\n\n"
        "1. WHAT TO BUILD:\nBuild exactly this kind of app: fleet tracker.\n\n"
        "2. DESIGN BRIEF AND DATA:\nDESIGN BRIEF:\nFleet app")
    v_instr = b_instr.replace("BUILD", "VALIDATION")
    advisor = "1. WHAT TO BUILD:\nBuild exactly this kind of app: fleet tracker."

    a = AppBuilderAssistant(
        builder=InstrBuilder(), advisor=FakeAdvisor(),
        validator=InstrValidator(), brief=None)
    res = run_understanding_phase(
        a, "raw user description should not reach A",
        builder_instruction=b_instr,
        validator_instruction=v_instr,
        advisor_design=advisor,
        entities=["vehicle"], features=["list"])
    # Session A takes NO read-only outline turn — its understanding IS B's
    # instruction (so its write-capable session is never flipped read-only).
    assert "builder_prompt" not in captured
    assert res.plan_texts.get("A") == b_instr
    # The read-only validator still restates B's instruction.
    assert "SESSION B INSTRUCTION" in captured["validator_context"]
    assert res.plan_texts.get("B") == advisor
    assert res.agreed_design is not None


def test_understanding_phase_not_ready_on_divergent_plans():
    class DivergentAdvisor(FakeAdvisor):
        def frame_answer(self, q, *, brief=None, context=""):
            if "BUSINESS DESIGN" in q:
                return "Entities: invoice and vendor. Components: ledger."
            return "x"

    a = AppBuilderAssistant(builder=FakeBuilder(), advisor=DivergentAdvisor(),
                            validator=FakeValidator(), brief=None)
    res = run_understanding_phase(
        a, "something", entities=None, features=None, max_reconcile=1)
    assert res.ready is False
    # Even when not ready, an agreed (union) design is still produced so the
    # pipeline never deadlocks.
    assert res.agreed_design is not None
