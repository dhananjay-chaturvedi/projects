"""AI Query UI: batch 'questions from a file' runner.

Drives the real (unbound) batch methods on a stub so no Tk display is needed.
``root.after`` runs callbacks synchronously so the step chain is deterministic.
"""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _batch_stub(auto):
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    stub = types.SimpleNamespace(
        _batch_questions=[], _batch_index=0, _batch_active=False,
        _batch_auto=False, _batch_step_pending=False,
        generated=[], statuses=[], chats=[], nl_text="",
    )
    fake_root = types.SimpleNamespace()
    fake_root.after = lambda ms, fn, *a: fn(*a)
    stub.root = fake_root
    stub._set_question_text = lambda t: setattr(stub, "nl_text", t)
    stub.generate_sql_from_question = (
        lambda: stub.generated.append(stub._batch_questions[stub._batch_index]))
    stub._add_chat_message = lambda r, m: stub.chats.append((r, m))
    stub.update_status = lambda m: stub.statuses.append(m)
    stub._auto_execute_ai_loop_enabled = lambda: auto
    stub._auto_execute_sql_enabled = lambda: False
    for name in ("_start_questions_batch", "_batch_run_current",
                 "_batch_on_step_done", "_batch_finish"):
        setattr(stub, name, types.MethodType(getattr(AIQueryUI, name), stub))
    return stub


def test_batch_auto_runs_each_question_on_completion():
    stub = _batch_stub(auto=True)
    stub._start_questions_batch(["q1", "q2", "q3"])
    # First question loaded + generated immediately.
    assert stub.nl_text == "q1"
    assert stub.generated == ["q1"]
    assert stub._batch_auto is True

    # Auto completion drives the next question.
    stub._batch_on_step_done(auto_only=True)
    assert stub.generated == ["q1", "q2"]
    assert stub.nl_text == "q2"

    stub._batch_on_step_done(auto_only=True)
    assert stub.generated == ["q1", "q2", "q3"]

    # Completing the last one finishes the batch.
    stub._batch_on_step_done(auto_only=True)
    assert stub._batch_active is False
    assert stub.generated == ["q1", "q2", "q3"]


def test_batch_manual_waits_for_execute():
    stub = _batch_stub(auto=False)
    stub._start_questions_batch(["q1", "q2"])
    assert stub.generated == ["q1"]
    assert stub._batch_auto is False

    # An auto-completion signal must NOT advance a manual batch.
    stub._batch_on_step_done(auto_only=True)
    assert stub.generated == ["q1"]

    # Only the manual execute signal advances it.
    stub._batch_on_step_done(manual_only=True)
    assert stub.generated == ["q1", "q2"]
    stub._batch_on_step_done(manual_only=True)
    assert stub._batch_active is False


def test_batch_force_advances_on_generation_error():
    stub = _batch_stub(auto=False)
    stub._start_questions_batch(["q1", "q2"])
    assert stub.generated == ["q1"]
    # A generation error force-advances so the batch never stalls.
    stub._batch_on_step_done(force=True)
    assert stub.generated == ["q1", "q2"]


def test_batch_stop_aborts():
    stub = _batch_stub(auto=True)
    stub._start_questions_batch(["q1", "q2"])
    assert stub.generated == ["q1"]
    # Simulate Stop.
    stub._batch_active = False
    stub._batch_step_pending = False
    stub._batch_on_step_done(auto_only=True)  # ignored
    assert stub.generated == ["q1"]


def test_batch_idempotent_completion_signal():
    stub = _batch_stub(auto=True)
    stub._start_questions_batch(["q1", "q2"])
    stub._batch_on_step_done(auto_only=True)
    assert stub.generated == ["q1", "q2"]
    # A duplicate completion for the same step is ignored (pending already off
    # until the next question starts; this fires after q2 started, advancing it).
    # Re-asserting the guard: calling with no pending step does nothing.
    stub._batch_step_pending = False
    before = list(stub.generated)
    stub._batch_on_step_done(auto_only=True)
    assert stub.generated == before


def test_ui_wires_questions_from_file_button():
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "Questions from file" in ui
    assert "def load_questions_from_file_dialog" in ui
    assert "load_questions_from_file" in ui  # reuses the shared parser
