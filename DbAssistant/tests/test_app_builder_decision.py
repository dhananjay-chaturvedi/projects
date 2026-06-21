"""Tests for the App Builder Assistant's balanced decision engine."""

from __future__ import annotations

import math

import pytest

from ai_assistant.app_builder.decision import (
    DIMENSIONS,
    DecisionEngine,
    FitnessMeterSuite,
    build_requirement_model,
)


# ── requirement model: machine-understandable parsing ────────────────────────
def test_priorities_normalize_to_one():
    model = build_requirement_model("a simple lightweight todo app")
    assert math.isclose(sum(model.priorities.values()), 1.0, rel_tol=1e-9)
    assert set(model.priorities) == set(DIMENSIONS)


def test_performance_signal_raises_performance_priority():
    fast = build_requirement_model("a real-time low latency trading dashboard")
    plain = build_requirement_model("a notes app")
    assert fast.priorities["performance"] > plain.priorities["performance"]
    assert fast.targets["latency_class"] == "low"


def test_scale_signal_sets_high_scale_target():
    model = build_requirement_model(
        "a scalable platform for millions of users with high traffic")
    assert model.targets["scale_class"] == "high"
    assert "scalability" in model.top_dimensions(3)


def test_cost_and_resource_signals_set_targets():
    model = build_requirement_model(
        "a cheap lightweight low-cost embedded app with low memory")
    assert model.targets["budget_class"] == "tight"
    assert model.targets["resource_class"] == "constrained"


def test_tokens_carry_assigned_meaning():
    model = build_requirement_model("a secure payment processing service")
    cats = {t.term: t.category for t in model.tokens}
    assert cats.get("secure") == "reliability"
    assert cats.get("payment") == "reliability"
    # domain words still captured but tagged "domain"
    assert any(t.category == "domain" for t in model.tokens)


# ── fitness meter suite: math + hard rules ────────────────────────────────────
def test_high_scale_requirement_fails_sqlite_hard_rule():
    model = build_requirement_model(
        "a scalable platform for millions of users, high traffic, distributed")
    suite = FitnessMeterSuite(model)
    sqlite = suite.score_profile(_profile("SQLite"))
    postgres = suite.score_profile(_profile("PostgreSQL"))
    assert not sqlite.passed
    assert sqlite.violations
    assert postgres.passed
    assert postgres.score > sqlite.score


def test_simple_cheap_requirement_prefers_sqlite():
    model = build_requirement_model("a simple cheap lightweight personal todo app")
    suite = FitnessMeterSuite(model)
    sqlite = suite.score_profile(_profile("SQLite"))
    postgres = suite.score_profile(_profile("PostgreSQL"))
    assert sqlite.score > postgres.score
    assert sqlite.passed


def test_critical_requirement_penalizes_low_reliability():
    model = build_requirement_model(
        "a mission critical banking payment system with high availability")
    suite = FitnessMeterSuite(model)
    cache = suite.score_profile(_profile("in-memory cache"))
    assert not cache.passed
    assert any("reliab" in v.lower() for v in cache.violations)


# ── decision engine: balanced, justified choice ──────────────────────────────
def test_decide_picks_scalable_db_for_high_scale():
    model = build_requirement_model(
        "a scalable store for millions of users with high traffic")
    engine = DecisionEngine(model)
    decision = engine.decide(
        "Which database should I use, SQLite or PostgreSQL?")
    assert decision.chosen == "PostgreSQL"
    assert "PostgreSQL" in decision.answer
    assert decision.rationale


def test_decide_overrides_weak_proposal():
    model = build_requirement_model(
        "a scalable platform for millions of users, high traffic")
    engine = DecisionEngine(model)
    # The agent proposes SQLite but the question also mentions PostgreSQL.
    decision = engine.decide(
        "Should I use SQLite or PostgreSQL for storage?",
        proposed="I suggest we use SQLite to keep it simple.")
    assert decision.chosen == "PostgreSQL"
    assert decision.overrode_proposal is True


def test_decide_generic_when_no_known_option():
    model = build_requirement_model("a fast responsive analytics dashboard")
    engine = DecisionEngine(model)
    decision = engine.decide(
        "What color scheme should the dashboard use?",
        proposed="Use a dark theme with blue accents.")
    assert decision.chosen is None
    assert "dark theme" in decision.answer
    # guidance reflects the requirement priorities
    assert "performance" in decision.answer or "performance" in decision.rationale


def test_decide_is_deterministic():
    model = build_requirement_model("a cheap simple lightweight app")
    engine = DecisionEngine(model)
    q = "Use SQLite or PostgreSQL?"
    assert engine.decide(q).answer == engine.decide(q).answer


def _profile(name: str):
    from ai_assistant.app_builder.decision import _OPTION_INDEX

    return _OPTION_INDEX[name.lower()]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
