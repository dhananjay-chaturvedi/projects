"""SQL-generation cancel support + AIQueryUI.shutdown() worker cleanup."""

from __future__ import annotations

import threading
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _ui_method(name):
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    return getattr(AIQueryUI, name)


def test_stop_cancels_running_generation():
    statuses = []
    self_obj = types.SimpleNamespace(
        auto_loop_cancelled=False,
        generation_running=True,
        generation_cancelled=False,
        auto_loop_running=False,
        query_running=False,
        update_status=lambda *a, **k: statuses.append(a),
    )
    _ui_method("stop_ai_query")(self_obj)
    assert self_obj.generation_cancelled is True
    assert self_obj.auto_loop_cancelled is True


def test_finish_generation_resets_state():
    dead = types.SimpleNamespace(is_alive=lambda: False)
    self_obj = types.SimpleNamespace(
        generation_running=True,
        _generation_thread=dead,
        _worker_threads=[dead],
        _update_busy_ui=lambda: None,
    )
    # bind real _prune_worker_threads + _finish_generation
    self_obj._prune_worker_threads = types.MethodType(
        _ui_method("_prune_worker_threads"), self_obj)
    _ui_method("_finish_generation")(self_obj)
    assert self_obj.generation_running is False
    assert self_obj._generation_thread is None
    assert self_obj._worker_threads == []


def test_busy_ui_includes_generation():
    packed = {"stop": False}

    class _Btn:
        def pack(self, **k):
            packed["stop"] = True

        def pack_forget(self):
            pass

    self_obj = types.SimpleNamespace(
        query_running=False, auto_loop_running=False, generation_running=True,
        execute_query_btn=_Btn(), stop_query_btn=_Btn(), explain_query_btn=_Btn(),
    )
    _ui_method("_update_busy_ui")(self_obj)
    assert packed["stop"] is True  # Stop button shown while generating


def test_shutdown_cancels_and_joins_workers():
    joined = {"n": 0}

    class _Worker:
        def __init__(self):
            self._alive = True

        def is_alive(self):
            return self._alive

        def join(self, timeout=None):
            joined["n"] += 1
            self._alive = False

    w = _Worker()
    self_obj = types.SimpleNamespace(
        auto_loop_cancelled=False,
        cancellation_requested=False,
        generation_cancelled=False,
        current_db_manager=None,
        current_execution_thread=None,
        _generation_thread=None,
        _worker_threads=[w],
        generation_running=True,
        query_running=True,
    )
    _ui_method("shutdown")(self_obj, join_timeout=0.1)
    assert self_obj.generation_cancelled is True
    assert self_obj.cancellation_requested is True
    assert joined["n"] == 1
    assert self_obj._worker_threads == []
    assert self_obj.generation_running is False
    assert self_obj.query_running is False


def test_shutdown_skips_current_thread():
    """shutdown() must never try to join the calling thread (would deadlock)."""
    result = {}

    def run():
        self_obj = types.SimpleNamespace(
            auto_loop_cancelled=False, cancellation_requested=False,
            generation_cancelled=False, current_db_manager=None,
            current_execution_thread=threading.current_thread(),
            _generation_thread=None, _worker_threads=[],
            generation_running=False, query_running=False,
        )
        _ui_method("shutdown")(self_obj, join_timeout=0.1)
        result["ok"] = True

    t = threading.Thread(target=run)
    t.start()
    t.join(timeout=5)
    assert result.get("ok") is True


def test_shutdown_wired_into_workspace():
    ws = (ROOT / "common/ui/tk/ai/ai_query_workspace.py").read_text()
    assert ws.count("ui.shutdown()") >= 2  # close_tab + load_sessions
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "def shutdown(self" in ui
