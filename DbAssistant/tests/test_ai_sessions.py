"""Tests for multi-tab AI session manager and cross-tab orchestrator."""

import pytest

from ai_query.session_manager import (
    AISessionManager,
    SessionStatus,
    export_session_record,
    load_sessions_from_disk,
    merge_session_into_disk,
    read_sessions_file,
    save_sessions_merged,
    write_sessions_file,
)


class TestAISessionManager:
    def test_create_and_list(self):
        mgr = AISessionManager(max_sessions=5)
        s1 = mgr.create(connection_name="prod", backend="claude")
        s2 = mgr.create(connection_name="dev")
        assert s1.tab_number == 1
        assert s2.tab_number == 2
        listed = mgr.list_sessions()
        assert len(listed) == 2

    def test_resolve_tab_ref(self):
        mgr = AISessionManager()
        s = mgr.create()
        assert mgr.resolve("tab1") is s
        assert mgr.resolve("1") is s
        assert mgr.resolve(s.session_id) is s

    def test_delete_renumbers(self):
        mgr = AISessionManager()
        a = mgr.create()
        b = mgr.create()
        mgr.delete(a.session_id)
        assert mgr.get(b.session_id).tab_number == 1

    def test_max_sessions(self):
        mgr = AISessionManager(max_sessions=2)
        mgr.create()
        mgr.create()
        with pytest.raises(RuntimeError):
            mgr.create()

    def test_persistence_roundtrip(self, tmp_path):
        mgr = AISessionManager()
        s = mgr.create(connection_name="x")
        s.conversation_history.append({"role": "user", "content": "hi"})
        path = tmp_path / "sessions.json"
        save_sessions_merged(mgr, path)
        mgr2 = AISessionManager()
        load_sessions_from_disk(mgr2, path)
        restored = mgr2.resolve("tab1")
        assert restored.connection_name == "x"
        assert restored.conversation_history[0]["content"] == "hi"

    def test_slim_export_with_backend_session_id(self):
        mgr = AISessionManager()
        s = mgr.create(connection_name="prod", backend="cursor")
        s.backend_session_id = "chat_abc"
        s.conversation_history.append({"role": "user", "content": "hello"})
        rec = export_session_record(s, saved_from_close=True)
        assert rec["backend_session_id"] == "chat_abc"
        assert rec["conversation_history"] == []
        assert rec["resume_supported"] is True
        assert rec.get("fallback") is None

    def test_merge_and_load_closed_session(self, tmp_path):
        mgr = AISessionManager()
        a = mgr.create(connection_name="a")
        b = mgr.create(connection_name="b")
        a.conversation_history.append({"role": "user", "content": "q1"})
        path = tmp_path / "sessions.json"
        merge_session_into_disk(a, path, saved_from_close=True)
        mgr.delete(a.session_id)
        save_sessions_merged(mgr, path)
        stored = read_sessions_file(path)
        assert len(stored) == 2
        ids = {r["session_id"] for r in stored}
        assert a.session_id in ids
        assert b.session_id in ids

    def test_trim_stored_sessions_cap(self, tmp_path):
        path = tmp_path / "sessions.json"
        records = [
            {"session_id": f"s{i}", "tab_number": i, "saved_at": f"2026-01-0{i}T00:00:00Z"}
            for i in range(1, 6)
        ]
        write_sessions_file(records, path)
        from ai_query.session_manager import max_stored_sessions, _trim_stored_sessions

        trimmed = _trim_stored_sessions(records, 3)
        assert len(trimmed) == 3


class TestCrossTabParser:
    def test_parse_tab_references(self):
        from ai_query.cross_tab_orchestrator import CrossTabOrchestrator
        refs = CrossTabOrchestrator.parse_tab_references("use tab 2 and @tab3 please")
        assert refs == [2, 3]

    def test_parse_route_target(self):
        from ai_query.cross_tab_orchestrator import CrossTabOrchestrator
        r = CrossTabOrchestrator.parse_route_target("talk to tab 2: count orders")
        assert r == (2, "count orders")


class TestCrossTabNoDeadlock:
    """Regression: the orchestrator must not hold a session's non-reentrant
    ``_lock`` across the agent call, because the agent re-acquires that same
    lock inside ``_bind_session``. Holding it across the call deadlocks the
    worker thread, which froze "Generate SQL" for *every* backend.
    """

    def _orchestrator(self, agent):
        from ai_query.cross_tab_orchestrator import CrossTabOrchestrator

        mgr = AISessionManager()
        orch = CrossTabOrchestrator(agent, mgr, get_db_manager=lambda _name: None)
        agent.sessions = mgr
        return orch, mgr

    class _LockReentrantAgent:
        """Fake agent whose calls re-acquire ``session._lock`` like the real
        ``_bind_session`` does."""

        def __init__(self):
            self.sessions = None

        def _run(self, session_id):
            sess = self.sessions.get(session_id)
            with sess._lock:  # mirrors agent._bind_session
                return {"sql": "SELECT 1", "error": None}

        def start_new_conversation(self, text, db_manager, connection_name, *, session_id=None, peer_bundles=None):
            return self._run(session_id)

        def send_follow_up(self, text, db_manager, connection_name, *, session_id=None, peer_bundles=None):
            return self._run(session_id)

    @staticmethod
    def _run_with_watchdog(callable_, timeout=5.0):
        import threading

        box = {}

        def runner():
            box["out"] = callable_()

        t = threading.Thread(target=runner)
        t.start()
        t.join(timeout=timeout)
        assert not t.is_alive(), "orchestrator deadlocked holding session._lock across the agent call"
        return box["out"]

    def test_parse_and_execute_ask_does_not_deadlock(self):
        orch, mgr = self._orchestrator(self._LockReentrantAgent())
        sess = mgr.create(connection_name="test")
        out = self._run_with_watchdog(
            lambda: orch.parse_and_execute(sess.session_id, "list tables", None, "test", mode="ask")
        )
        assert out["result"]["sql"] == "SELECT 1"
        assert sess.status == SessionStatus.IDLE

    def test_parse_and_execute_followup_does_not_deadlock(self):
        orch, mgr = self._orchestrator(self._LockReentrantAgent())
        sess = mgr.create(connection_name="test")
        out = self._run_with_watchdog(
            lambda: orch.parse_and_execute(sess.session_id, "and the columns", None, "test", mode="followup")
        )
        assert out["result"]["sql"] == "SELECT 1"
        assert sess.status == SessionStatus.IDLE

    def test_route_to_tab_does_not_deadlock(self):
        orch, mgr = self._orchestrator(self._LockReentrantAgent())
        a = mgr.create(connection_name="a")
        b = mgr.create(connection_name="b")
        out = self._run_with_watchdog(
            lambda: orch.route_to_tab(a.session_id, b.tab_number, "count orders")
        )
        assert out["routed"] is True
        assert out["result"]["sql"] == "SELECT 1"
        assert b.status == SessionStatus.IDLE


class TestAgentSessionBind:
    def test_start_new_conversation_session_scoped(self):
        from ai_query.agent import AIQueryAgent
        agent = AIQueryAgent()
        sess = agent.sessions.create(connection_name="test")
        # Without backend, ask will fail fast — we only test history isolation
        agent.conversation_history.append({"role": "user", "content": "legacy"})
        agent.start_new_conversation.__wrapped__ if False else None
        with agent._bind_session(sess.session_id):
            agent.conversation_history.clear()
            agent.conversation_history.append({"role": "user", "content": "session"})
        assert len(agent.conversation_history) == 1
        assert agent.conversation_history[0]["content"] == "legacy"
        assert len(sess.conversation_history) == 1
        assert sess.conversation_history[0]["content"] == "session"
