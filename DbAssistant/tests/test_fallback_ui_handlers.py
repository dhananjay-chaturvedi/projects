"""AI Query UI: fallback dropdown + flag-button branching + auto-correct.

These exercise the Tkinter handler logic by binding the real (unbound) methods
onto a lightweight stub so no display is required.
"""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class _FakeText:
    def __init__(self, text=""):
        self._t = text

    def get(self, *args):
        return self._t

    def config(self, **kw):
        pass

    def delete(self, *a):
        self._t = ""

    def insert(self, _idx, s):
        self._t = (self._t or "") + s


class _FakeCombo:
    def __init__(self, val=""):
        self._v = val

    def get(self):
        return self._v


class _FakeVar:
    def __init__(self, val=""):
        self._v = val

    def get(self):
        return self._v

    def set(self, v):
        self._v = v


class _FakeNotebook:
    def __init__(self):
        self.selected = None

    def select(self, idx):
        self.selected = idx

    def index(self, _):
        return 1

    def tab(self, _idx, _opt):
        return "Chat"


class _FakeAgent:
    def __init__(self, primary="local-llm", primary_value="local-llm::m1",
                 fallback="codex", active_model="m1"):
        self._primary = primary
        self._primary_value = primary_value
        self._fallback = fallback
        self._active_model = active_model

    def get_active_backend_name(self):
        return self._primary

    def get_active_backend_value(self):
        return self._primary_value

    def get_fallback_backend_value(self):
        return self._fallback

    def get_active_local_model(self):
        return self._active_model

    def set_fallback_backend(self, value, verify=True):
        self._fallback = value
        return True


def _ui_stub(agent, *, autofix=False):
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    stub = types.SimpleNamespace(
        ai_agent=agent,
        ai_conn_combo=_FakeCombo("mydb"),
        ai_question_text=_FakeText("list customers"),
        ai_sql_text=_FakeText("SELECT * FROM custmer"),
        ai_results_text=_FakeText(""),
        ai_results_notebook=_FakeNotebook(),
        active_connections={"mydb": types.SimpleNamespace(db_type="postgresql")},
        autofix_train=autofix,
        _autofix_in_progress=False,
        _last_sql_corrected=False,
        _pending_autofix_train=None,
        chat_messages=[],
        statuses=[],
        correction_calls=[],
        train_calls=[],
    )
    # Collaborators we don't want to actually run.
    stub._add_chat_message = lambda role, msg: stub.chat_messages.append((role, msg))
    stub.update_status = lambda msg: stub.statuses.append(msg)
    stub._run_query_correction = (
        lambda *a, **kw: stub.correction_calls.append((a, kw)))
    stub._queue_autofix_train = (
        lambda model, connection: stub.train_calls.append((model, connection)))

    for name in (
        "flag_incorrect_query", "flag_incorrect_interpretation",
        "_maybe_autocorrect_on_failure", "_current_qsc", "_train_target_model",
        "fallback_backend_value", "_autofix_train_enabled",
        "_fallback_is_local_llm", "_looks_read_only", "_chat_tab_index",
        "_on_fallback_changed",
    ):
        setattr(stub, name, types.MethodType(getattr(AIQueryUI, name), stub))
    return stub


# ── flag incorrect query ───────────────────────────────────────────────────────

def test_flag_query_local_primary_uses_fallback_correction():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback="codex"))
    stub.flag_incorrect_query()
    assert len(stub.correction_calls) == 1
    _args, kw = stub.correction_calls[0]
    assert kw["mode"] == "syntax"
    assert kw["corrector_value"] == "codex"
    assert kw["train_target"] == "primary"


def test_flag_query_local_primary_without_fallback_warns(monkeypatch):
    import common.ui.tk.ai.ai_query_ui as mod
    warned = {}
    monkeypatch.setattr(mod.messagebox, "showwarning",
                        lambda *a, **k: warned.update(title=a[0]))
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback=""))
    stub.flag_incorrect_query()
    assert not stub.correction_calls
    assert "fallback" in warned.get("title", "").lower()


def test_flag_query_other_primary_queues_fallback_llm_when_enabled():
    stub = _ui_stub(
        _FakeAgent(primary="claude", fallback="local-llm::m9"), autofix=True)
    stub.flag_incorrect_query()
    # No auto-correction for a capable primary; just a chat suggestion …
    assert not stub.correction_calls
    # … but the fallback local LLM is QUEUED (trains after a successful execute).
    assert len(stub.train_calls) == 1
    model, conn = stub.train_calls[0]
    assert model == "m9"  # resolved fallback model name
    assert conn == "mydb"


def test_pending_autofix_trains_only_after_successful_execute():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    trained = []

    class _Svc:
        def train_pairs(self, pairs, **kw):
            trained.append((pairs, kw))
            return {"ok": True, "pairs": len(pairs)}

    stub = types.SimpleNamespace(
        ai_question_text=_FakeText("list customers"),
        ai_explanation_text=_FakeText("Joins customer and orders."),
        chat_messages=[], statuses=[], _pending_autofix_train=None,
    )
    stub._add_chat_message = lambda r, m: stub.chat_messages.append((r, m))
    stub.update_status = lambda m: stub.statuses.append(m)
    stub._llm_training_service = lambda: _Svc()
    stub._run_llm_training_bg = lambda work, done: done(work(), None)
    for name in ("_queue_autofix_train", "_run_pending_autofix_train"):
        setattr(stub, name, types.MethodType(getattr(AIQueryUI, name), stub))

    stub._queue_autofix_train("m1", "mydb")
    assert stub._pending_autofix_train == {"model": "m1", "connection": "mydb"}

    # Executing on a different connection must NOT train (and keeps the queue).
    stub._run_pending_autofix_train("SELECT 1", "otherdb")
    assert not trained
    assert stub._pending_autofix_train is not None

    # Executing the corrected query on the queued connection trains once, using
    # the current question + explanation, and clears the queue.
    stub._run_pending_autofix_train("-- Corrected by fallback\nSELECT 1", "mydb")
    assert len(trained) == 1
    pairs, kw = trained[0]
    assert pairs[0]["question"] == "list customers"
    assert pairs[0]["sql"] == "SELECT 1"           # comment stripped
    assert pairs[0]["description"] == "Joins customer and orders."
    assert kw["connection"] == "mydb"
    assert stub._pending_autofix_train is None

    # A second execute does nothing (queue already consumed).
    stub._run_pending_autofix_train("SELECT 1", "mydb")
    assert len(trained) == 1


# ── flag incorrect interpretation ──────────────────────────────────────────────

def test_flag_interpretation_local_primary_uses_fallback():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback="codex"))
    stub.flag_incorrect_interpretation()
    _args, kw = stub.correction_calls[0]
    assert kw["mode"] == "interpretation"
    assert kw["corrector_value"] == "codex"
    assert kw["train_target"] == "primary"


def test_flag_interpretation_other_primary_resends_to_primary():
    stub = _ui_stub(_FakeAgent(primary="claude", primary_value="claude",
                               fallback="local-llm::m9"))
    stub.flag_incorrect_interpretation()
    _args, kw = stub.correction_calls[0]
    assert kw["mode"] == "interpretation"
    assert kw["corrector_value"] == "claude"   # re-sent to the primary
    assert kw["train_target"] == "fallback"


# ── auto-correct on execute failure ─────────────────────────────────────────────

def test_autocorrect_on_failure_local_primary():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback="codex"))
    stub._maybe_autocorrect_on_failure("syntax error near FROM")
    assert len(stub.correction_calls) == 1
    _args, kw = stub.correction_calls[0]
    assert kw["mode"] == "syntax"
    assert "syntax error" in kw["error_text"]
    # The primary's failure error is surfaced in the results note, above the
    # "generating correction…" message.
    assert "syntax error near FROM" in kw["note"]
    assert "fallback backend" in kw["note"].lower()


def test_autocorrect_skipped_for_other_primary():
    stub = _ui_stub(_FakeAgent(primary="claude", fallback="codex"))
    stub._maybe_autocorrect_on_failure("boom")
    assert not stub.correction_calls


def test_autocorrect_skipped_when_already_corrected():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback="codex"))
    stub._last_sql_corrected = True
    stub._maybe_autocorrect_on_failure("boom")
    assert not stub.correction_calls


def test_autocorrect_skipped_without_fallback():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback=""))
    stub._maybe_autocorrect_on_failure("boom")
    assert not stub.correction_calls


# ── helpers ────────────────────────────────────────────────────────────────────

def test_train_target_model_resolution():
    stub = _ui_stub(_FakeAgent(primary="local-llm", fallback="local-llm::fbm",
                               active_model="prim"))
    stub._aiqa_session_model = lambda: "prim"
    assert stub._train_target_model("primary") == "prim"
    assert stub._train_target_model("fallback") == "fbm"
    # A non-local fallback yields no training target.
    stub.ai_agent._fallback = "codex"
    assert stub._train_target_model("fallback") == ""


def test_looks_read_only():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI
    assert AIQueryUI._looks_read_only("SELECT 1") is True
    assert AIQueryUI._looks_read_only("-- comment\nWITH x AS (SELECT 1) SELECT * FROM x")
    assert AIQueryUI._looks_read_only("DELETE FROM t") is False


def test_on_fallback_changed_sets_and_persists(monkeypatch):
    import ai_query.service as svc_mod
    captured = {}
    monkeypatch.setattr(svc_mod, "_update_ai_state",
                        lambda d: captured.update(d))
    agent = _FakeAgent(primary="local-llm", fallback="")
    stub = _ui_stub(agent)
    stub.ai_fallback_var = _FakeVar("Codex")
    stub._fallback_label_to_value = {"Codex": "codex", "(none)": ""}
    stub._on_fallback_changed()
    assert agent.get_fallback_backend_value() == "codex"
    assert captured.get("fallback_backend") == "codex"


# ── source-level wiring ─────────────────────────────────────────────────────────

def test_ui_has_fallback_dropdown_and_flag_buttons():
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "ai_fallback_combo" in ui
    assert "Flag incorrect query" in ui
    assert "Flag incorrect interpretation" in ui
    assert "Auto-train on fallback-corrected queries" in ui


def test_set_question_text_replaces_nl_box():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    class _QBox(_FakeText):
        def cget(self, _opt):
            return "normal"

    stub = types.SimpleNamespace(ai_question_text=_QBox("old question"))
    AIQueryUI._set_question_text(stub, "sort by date descending")
    assert stub.ai_question_text.get() == "sort by date descending"
    # Empty/whitespace is ignored (keeps the existing question).
    AIQueryUI._set_question_text(stub, "   ")
    assert stub.ai_question_text.get() == "sort by date descending"


def test_followup_transfers_brief_to_question_box():
    """A follow-up that yields SQL mirrors the brief into the NL question box."""
    import inspect
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    src = inspect.getsource(AIQueryUI._send_followup_thread)
    assert "_set_question_text" in src
    assert "followup_message" in src


def test_followups_route_through_auto_execute_pipeline():
    """Item 2: follow-ups send the generated SQL to the editor and (when the
    auto-execute toggle is on) run it — both happen inside
    ``_start_post_ai_pipeline``, which the follow-up thread always invokes."""
    import inspect
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    src = inspect.getsource(AIQueryUI._send_followup_thread)
    assert "_start_post_ai_pipeline" in src
    pipeline = inspect.getsource(AIQueryUI._start_post_ai_pipeline)
    # SQL is rendered to the editor, then the auto pipeline may execute it.
    assert "_display_ai_response" in pipeline
    assert "_continue_pipeline_after_ai" in pipeline
