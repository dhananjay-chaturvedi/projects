"""Regression tests for the auto-execute AI loop wiring."""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _m(name):
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    return getattr(AIQueryUI, name)


def _base(**over):
    calls = {"run_step": 0, "execute": 0, "finish": 0, "callback": None}
    self_obj = types.SimpleNamespace(
        auto_loop_cancelled=False,
        auto_execute_sql=False,
        auto_execute_ai_loop=False,
        _pipeline_callback=None,
        _run_auto_loop_step=lambda: calls.__setitem__("run_step", calls["run_step"] + 1),
        _finish_auto_pipeline=lambda: calls.__setitem__("finish", calls["finish"] + 1),
        execute_ai_query=lambda from_pipeline=False: calls.__setitem__("execute", calls["execute"] + 1),
        _sync_panels_to_session=lambda *a, **k: None,
    )
    self_obj._auto_execute_sql_enabled = types.MethodType(_m("_auto_execute_sql_enabled"), self_obj)
    self_obj._auto_execute_ai_loop_enabled = types.MethodType(_m("_auto_execute_ai_loop_enabled"), self_obj)
    for k, v in over.items():
        setattr(self_obj, k, v)
    return self_obj, calls


def test_continue_runs_loop_when_only_loop_enabled():
    self_obj, calls = _base(auto_execute_ai_loop=True)
    _m("_continue_pipeline_after_ai")(self_obj, {"sql": "SELECT 1"})
    assert calls["run_step"] == 1
    assert calls["execute"] == 0


def test_continue_executes_sql_when_auto_sql_enabled():
    self_obj, calls = _base(auto_execute_sql=True)
    self_obj._pipeline_after_execute = types.MethodType(
        _m("_pipeline_after_execute"), self_obj)
    _m("_continue_pipeline_after_ai")(self_obj, {"summary_sql": "SELECT 1"})
    assert calls["execute"] == 1
    assert self_obj._pipeline_callback is self_obj._pipeline_after_execute


def test_continue_skips_execute_for_clarification():
    self_obj, calls = _base(auto_execute_sql=True)
    _m("_continue_pipeline_after_ai")(
        self_obj, {"summary_sql": "SELECT 1", "is_clarification": True})
    # Clarification must not auto-execute; with no loop it finishes.
    assert calls["execute"] == 0
    assert calls["finish"] == 1


def test_continue_finishes_when_cancelled():
    self_obj, calls = _base(auto_execute_ai_loop=True, auto_loop_cancelled=True)
    _m("_continue_pipeline_after_ai")(self_obj, {"sql": "SELECT 1"})
    assert calls["finish"] == 1
    assert calls["run_step"] == 0


def test_after_execute_continues_loop():
    self_obj, calls = _base(auto_execute_ai_loop=True)
    _m("_pipeline_after_execute")(self_obj)
    assert calls["run_step"] == 1


def test_after_execute_finishes_without_loop():
    self_obj, calls = _base(auto_execute_ai_loop=False)
    _m("_pipeline_after_execute")(self_obj)
    assert calls["finish"] == 1


def test_checkbox_label_clarified():
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "Auto-run AI follow-ups (until satisfied)" in ui
