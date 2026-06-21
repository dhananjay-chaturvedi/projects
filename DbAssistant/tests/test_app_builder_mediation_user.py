"""Tests for user-request mediation (user → B → A)."""

from __future__ import annotations

from ai_assistant.app_builder.mediation import BuildProgress, ContextMediator
from ai_assistant.app_builder.decision import build_requirement_model


def test_user_request_context_includes_progress_and_request():
    rm = build_requirement_model(
        "ecommerce store", entities=["product"], features=["list"])
    med = ContextMediator(requirement_model=rm)
    ctx = med.user_request_context(
        "add a shopping cart",
        history="built product list page",
        progress=BuildProgress(phase="web", round=3, coverage=0.6,
                               gaps=["checkout"]),
    )
    assert "USER REQUEST" in ctx
    assert "shopping cart" in ctx
    assert "BUILD PROGRESS" in ctx
    assert "checkout" in ctx
    assert "ADVISOR" in ctx
