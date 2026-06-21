"""Tests for bounded post-build finalize repair loop and related config."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.governance import make_brief
from ai_assistant.app_builder.orchestrator import (
    AppBuildOrchestrator,
    FinalBuildState,
    FinalizeContext,
)


def _bp():
    return AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH)


def _final_args(tmp_path, coord, best_cov):
    return (
        FinalizeContext(
            workspace=tmp_path,
            blueprint=_bp(),
            req=None,
            gate=MagicMock(),
            coord=coord,
            last_suggestions=[],
            on_progress=None,
        ),
        FinalBuildState({}, MagicMock(), best_cov),
        lambda: False,
    )


def test_max_finalize_repairs_defaults_zero():
    orch = AppBuildOrchestrator()
    assert orch.max_finalize_repairs == 0


def test_max_finalize_repairs_clamped_non_negative():
    orch = AppBuildOrchestrator(max_finalize_repairs=-3)
    assert orch.max_finalize_repairs == 0
    orch2 = AppBuildOrchestrator(max_finalize_repairs=2)
    assert orch2.max_finalize_repairs == 2


def test_governance_engineering_principles_builder_render():
    text = make_brief(_bp()).render(role="builder")
    assert "ENGINEERING PRINCIPLES" in text
    assert "ROOT CAUSE" in text


def test_governance_engineering_principles_builder_render_minimal():
    text = make_brief(_bp()).render_minimal(role="builder")
    assert "ENGINEERING PRINCIPLES" in text
    assert "ROOT CAUSE" in text


def test_final_repair_instruction_format():
    orch = AppBuildOrchestrator(max_finalize_repairs=2)
    msg = orch._final_repair_instruction("fix /health", attempt=1)
    assert "REPAIR ROUND 1/2" in msg
    assert "ROOT CAUSE" in msg
    assert "PATCH" in msg
    assert "fix /health" in msg


def test_forward_nudge_is_patch_only():
    from ai_assistant.app_builder.commit_gate import CommitVerdict

    orch = AppBuildOrchestrator()
    verdict = CommitVerdict(accepted=False, score=0.5, coverage=0.4,
                            gaps=["api"])
    text = orch._forward_nudge(verdict, {"gaps": ["api"]})
    assert "PATCH" in text
    assert "re-scaffold" in text.lower() or "recreate" in text.lower()


def test_auto_build_wires_max_finalize_repairs_from_config():
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    captured: dict = {}

    class _Orch:
        def __init__(self, engine, **kwargs):
            captured.update(kwargs)

        def run(self, *a, **k):
            from ai_assistant.app_builder.orchestrator import OrchestrationResult
            return OrchestrationResult(
                ok=True, workspace="/tmp", final_score=1.0, files=[],
                rounds=[], mode="from_scratch", used_ai=False)

    with patch("ai_assistant.app_builder.orchestrator.AppBuildOrchestrator", _Orch):
        with patch("ai_assistant.app_builder.service.mc.get_int", return_value=3):
            svc.auto_build({"name": "x", "mode": "from_scratch"})
    assert captured["config"].max_finalize_repairs == 3


def test_auto_build_body_overrides_max_finalize_repairs():
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    captured: dict = {}

    class _Orch:
        def __init__(self, engine, **kwargs):
            captured.update(kwargs)

        def run(self, *a, **k):
            from ai_assistant.app_builder.orchestrator import OrchestrationResult
            return OrchestrationResult(
                ok=True, workspace="/tmp", final_score=1.0, files=[],
                rounds=[], mode="from_scratch", used_ai=False)

    with patch("ai_assistant.app_builder.orchestrator.AppBuildOrchestrator", _Orch):
        with patch("ai_assistant.app_builder.service.mc.get_int", return_value=2):
            svc.auto_build({
                "name": "x",
                "mode": "from_scratch",
                "max_finalize_repairs": 5,
            })
    assert captured["config"].max_finalize_repairs == 5


def test_finalize_with_repairs_stops_on_agreement(tmp_path):
    orch = AppBuildOrchestrator(max_finalize_repairs=2)
    orch._decider = SimpleNamespace(uninterrupted=True)
    builder = MagicMock()
    coord = SimpleNamespace(builder=builder)
    outcomes = [
        {"agreement": {"complete": False, "issues": ["x"],
                       "statements": {"advice": "fix api"}},
         "current": {}, "best": MagicMock(), "best_cov": {"score": 0.9, "gaps": []}},
        {"agreement": {"complete": True, "issues": [], "statements": {}},
         "current": {}, "best": MagicMock(), "best_cov": {"score": 1.0, "gaps": []}},
    ]
    with patch.object(orch, "_final_evaluation_pass", side_effect=outcomes):
        with patch.object(orch, "_read_workspace", return_value={}):
            with patch.object(orch, "_ensure_required", side_effect=lambda *a: a[2]):
                with patch.object(orch, "_evaluate") as ev:
                    with patch.object(orch, "_coverage", return_value={"score": 1, "gaps": []}):
                        with patch.object(orch, "_better", return_value=False):
                            ev.return_value = MagicMock()
                            agreement, *_ = orch._finalize_with_repairs(
                                *_final_args(tmp_path, coord, {"score": 0.9, "gaps": []}))
    assert agreement.get("complete") is True
    assert builder.send.call_count == 1
    assert "REPAIR ROUND 1/2" in builder.send.call_args[0][0]


def test_finalize_with_repairs_budget_exhausted(tmp_path):
    orch = AppBuildOrchestrator(max_finalize_repairs=2)
    orch._decider = SimpleNamespace(uninterrupted=True)
    builder = MagicMock()
    coord = SimpleNamespace(builder=builder)
    incomplete = {
        "agreement": {"complete": False, "issues": ["still broken"],
                     "statements": {"advice": "fix more"}},
        "current": {}, "best": MagicMock(),
        "best_cov": {"score": 0.5, "gaps": ["api"]},
    }
    with patch.object(orch, "_final_evaluation_pass", return_value=incomplete):
        with patch.object(orch, "_read_workspace", return_value={}):
            with patch.object(orch, "_ensure_required", side_effect=lambda *a: a[2]):
                with patch.object(orch, "_evaluate") as ev:
                    with patch.object(orch, "_coverage", return_value={"score": 0.5, "gaps": []}):
                        with patch.object(orch, "_better", return_value=False):
                            ev.return_value = MagicMock()
                            agreement, *_ = orch._finalize_with_repairs(
                                *_final_args(tmp_path, coord, {"score": 0.5, "gaps": []}))
    assert agreement.get("complete") is False
    assert builder.send.call_count == 2
    assert orch._stop_reason == "repair budget exhausted — issues remain"


def test_finalize_with_repairs_skipped_when_zero_budget(tmp_path):
    orch = AppBuildOrchestrator(max_finalize_repairs=0)
    orch._decider = SimpleNamespace(uninterrupted=True)
    builder = MagicMock()
    coord = SimpleNamespace(builder=builder)
    incomplete = {
        "agreement": {"complete": False, "issues": ["x"],
                     "statements": {"advice": "fix"}},
        "current": {}, "best": MagicMock(),
        "best_cov": {"score": 0.5, "gaps": []},
    }
    with patch.object(orch, "_final_evaluation_pass", return_value=incomplete):
        agreement, *_ = orch._finalize_with_repairs(
            *_final_args(tmp_path, coord, {"score": 0.5, "gaps": []}))
    assert agreement.get("complete") is False
    builder.send.assert_not_called()


def test_finalize_with_repairs_skipped_in_interactive_mode(tmp_path):
    orch = AppBuildOrchestrator(max_finalize_repairs=2)
    orch._decider = SimpleNamespace(uninterrupted=False)
    builder = MagicMock()
    coord = SimpleNamespace(builder=builder)
    incomplete = {
        "agreement": {"complete": False, "issues": ["x"],
                     "statements": {"advice": "fix"}},
        "current": {}, "best": MagicMock(),
        "best_cov": {"score": 0.5, "gaps": []},
        "runnable_ok": True,
    }
    with patch.object(orch, "_final_evaluation_pass", return_value=incomplete):
        agreement, *_ = orch._finalize_with_repairs(
            *_final_args(tmp_path, coord, {"score": 0.5, "gaps": []}))
    builder.send.assert_not_called()
    assert agreement.get("complete") is False


def test_finalize_with_repairs_runnable_gate_in_interactive_mode(tmp_path):
    orch = AppBuildOrchestrator(max_finalize_repairs=2)
    orch._decider = SimpleNamespace(uninterrupted=False)
    builder = MagicMock()
    coord = SimpleNamespace(builder=builder)
    not_runnable = {
        "agreement": {"complete": True, "issues": [], "statements": {}},
        "current": {}, "best": MagicMock(),
        "best_cov": {"score": 1.0, "gaps": []},
        "runnable_ok": False,
    }
    still_broken = dict(not_runnable)

    def _eval_side_effect(*_args, **_kwargs):
        return still_broken

    with patch.object(orch, "_preflight", create=True) as pf:
        pf.ok = False
        pf.digest.return_value = "app imports (src.app:app dry-run): False"
        with patch.object(orch, "_boot_check", create=True) as boot:
            boot.ok = False
            boot.digest.return_value = "boot check: FAILED"
            with patch.object(orch, "_http_smoke", create=True) as sm:
                sm.skipped = False
                sm.ok = True
                sm.digest.return_value = "launch smoke: PASSED"
                with patch.object(
                    orch, "_final_evaluation_pass",
                    side_effect=_eval_side_effect,
                ):
                    with patch.object(orch, "_read_workspace", return_value={}):
                        with patch.object(
                            orch, "_ensure_required", side_effect=lambda *a: a[2],
                        ):
                            with patch.object(orch, "_evaluate") as ev:
                                with patch.object(
                                    orch, "_coverage",
                                    return_value={"score": 1, "gaps": []},
                                ):
                                    with patch.object(orch, "_better", return_value=False):
                                        ev.return_value = MagicMock()
                                        agreement, *_ = orch._finalize_with_repairs(
                                            *_final_args(
                                                tmp_path, coord,
                                                {"score": 1.0, "gaps": []},
                                            ))
    assert builder.send.call_count >= 1
    assert "CODE GATE" in builder.send.call_args[0][0]
    assert orch._stop_reason.startswith(
        "build INCOMPLETE — runnability gate failing")
    assert agreement.get("complete") is True
