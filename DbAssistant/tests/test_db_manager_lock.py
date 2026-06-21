"""Phase 1(a): the per-DatabaseManager lock must serialize statement execution
so a single shared manager cannot interleave cursors across threads."""

from __future__ import annotations

import threading
import time

from common.db_manager import DatabaseManager


def _make_manager():
    mgr = DatabaseManager("MariaDB")
    mgr.conn = object()  # truthy so execute_query passes the "connected" guard
    return mgr


def test_execute_query_serializes_across_threads(monkeypatch):
    mgr = _make_manager()
    state = {"active": 0, "max": 0}
    state_lock = threading.Lock()

    def fake_locked(sql, caps):
        with state_lock:
            state["active"] += 1
            state["max"] = max(state["max"], state["active"])
        time.sleep(0.02)  # hold the manager lock long enough to overlap
        with state_lock:
            state["active"] -= 1
        return {"rows": []}, None

    monkeypatch.setattr(mgr, "_execute_query_locked", fake_locked)

    threads = [threading.Thread(target=mgr.execute_query, args=("SELECT 1",))
               for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # If the lock works, only one body runs at a time.
    assert state["max"] == 1, f"observed overlap: {state['max']}"


def test_lock_is_reentrant_for_same_thread():
    mgr = _make_manager()
    # Same-thread reentry (execute_query -> reconnect -> connect) must not
    # self-deadlock; RLock allows it.
    with mgr.lock:
        with mgr.lock:
            assert True


def test_lock_property_exposes_session_lock():
    mgr = _make_manager()
    assert mgr.lock is mgr._lock
