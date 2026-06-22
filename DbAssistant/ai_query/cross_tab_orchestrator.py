"""
Cross-tab coordination for multi-session AI Query Assistant.

Parses tab references, pulls agent-built context bundles, routes work to the
session that owns a connection, and merges delegated execution summaries.
"""

from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Optional

from ai_query.session_manager import AISession, AISessionManager, SessionStatus

_TAB_REF_RE = re.compile(
    r"(?:@tab\s*(\d+)|\btab\s*(\d+)\b|\btalk\s+to\s+tab\s*(\d+)|"
    r"\bsend\s+to\s+tab\s*(\d+)|\buse\s+tab\s*(\d+))",
    re.IGNORECASE,
)
_ROUTE_RE = re.compile(
    r"(?:talk\s+to\s+tab\s*(\d+)\s*[:\-]\s*|send\s+to\s+tab\s*(\d+)\s*[:\-]\s*)",
    re.IGNORECASE,
)

# Bounds to keep cross-tab parsing cheap and resistant to abuse.
_MAX_TAB_NUMBER = 1000          # tab indices above this are ignored
_MAX_TAB_REFS = 50             # stop collecting after this many references
_MAX_ROUTED_MESSAGE_CHARS = 100_000  # cap a routed message before forwarding


def _parse_bounded_tab(raw: str) -> int | None:
    """Parse a tab digit-string, rejecting anything out of sane bounds."""
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    if 0 <= n <= _MAX_TAB_NUMBER:
        return n
    return None


class CrossTabOrchestrator:
    def __init__(
        self,
        agent,
        session_manager: AISessionManager,
        get_db_manager: Callable[[str], Any],
        max_workers: int | None = None,
    ):
        self.agent = agent
        self.sessions = session_manager
        self.get_db_manager = get_db_manager
        if max_workers is None:
            from ai_query import module_config as mc
            max_workers = mc.get_int("ui.ai_query", "cross_tab_max_workers", default=4)
        self.max_workers = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    @staticmethod
    def parse_tab_references(text: str) -> list[int]:
        tabs: list[int] = []
        for m in _TAB_REF_RE.finditer(text or ""):
            for g in m.groups():
                if g:
                    n = _parse_bounded_tab(g)
                    if n is not None and n not in tabs:
                        tabs.append(n)
                        if len(tabs) >= _MAX_TAB_REFS:
                            return tabs
        return tabs

    @staticmethod
    def parse_route_target(text: str) -> Optional[tuple[int, str]]:
        m = _ROUTE_RE.search(text or "")
        if not m:
            return None
        tab_n = _parse_bounded_tab(m.group(1) or m.group(2))
        if tab_n is None:
            return None
        message = text[m.end():].strip()
        if len(message) > _MAX_ROUTED_MESSAGE_CHARS:
            message = message[:_MAX_ROUTED_MESSAGE_CHARS]
        return tab_n, message

    def _session_for_tab(self, tab_number: int) -> Optional[AISession]:
        return self.sessions.get_by_tab(tab_number)

    def export_session_context(
        self,
        session_id: str,
        question_hint: str = "",
    ) -> Optional[dict[str, Any]]:
        sess = self.sessions.get(session_id)
        if not sess or sess.isolated or not sess.share_context:
            return None
        db_manager = None
        if sess.connection_name:
            try:
                db_manager = self.get_db_manager(sess.connection_name)
            except Exception:
                pass
        return self.agent.export_cross_tab_bundle(sess, db_manager, question_hint)

    def pull_peer_bundles(
        self,
        initiator: AISession,
        text: str,
    ) -> list[dict[str, Any]]:
        refs = self.parse_tab_references(text)
        bundles = []
        for tab_n in refs:
            if tab_n == initiator.tab_number:
                continue
            peer = self._session_for_tab(tab_n)
            if not peer or peer.isolated or not peer.share_context:
                continue
            if peer.status == SessionStatus.RUNNING:
                continue
            bundle = self.export_session_context(peer.session_id, text)
            if bundle:
                bundles.append(bundle)
        return bundles

    def route_to_tab(
        self,
        from_session_id: str,
        target_tab: int,
        message: str,
    ) -> dict[str, Any]:
        target = self._session_for_tab(target_tab)
        if not target:
            return {"error": f"Tab {target_tab} not found", "routed": False}
        if target.status == SessionStatus.RUNNING:
            return {
                "error": f"Tab {target_tab} is busy",
                "routed": False,
                "waiting": True,
            }
        initiator = self.sessions.get(from_session_id)
        # Flip status under the lock only; the agent re-acquires ``target._lock``
        # inside ``_bind_session`` during the call, so holding it across
        # ``start_new_conversation`` would deadlock (non-reentrant lock).
        with target._lock:
            target.status = SessionStatus.RUNNING
        try:
            db_manager = self.get_db_manager(target.connection_name)
            result = self.agent.start_new_conversation(
                message,
                db_manager,
                target.connection_name,
                session_id=target.session_id,
            )
            return {
                "routed": True,
                "target_tab": target_tab,
                "target_session_id": target.session_id,
                "result": result,
                "from_tab": initiator.tab_number if initiator else None,
            }
        except Exception as exc:
            return {"error": str(exc), "routed": False}
        finally:
            with target._lock:
                target.status = SessionStatus.IDLE

    def coordinate_team(
        self,
        initiator_session_id: str,
        instruction: str,
    ) -> dict[str, Any]:
        initiator = self.sessions.get(initiator_session_id)
        if not initiator:
            return {"error": "Initiator session not found"}
        tabs = self.parse_tab_references(instruction)
        if not tabs:
            return {"error": "No tab references found for team coordination"}
        futures = {}
        for tab_n in tabs:
            if tab_n == initiator.tab_number:
                continue
            peer = self._session_for_tab(tab_n)
            if not peer or peer.isolated:
                continue
            futures[self._pool.submit(self.route_to_tab, initiator_session_id, tab_n, instruction)] = tab_n
        outcomes = []
        for fut in as_completed(futures):
            outcomes.append({"tab": futures[fut], **fut.result()})
        return {"coordinated": True, "outcomes": outcomes}

    def parse_and_execute(
        self,
        session_id: str,
        text: str,
        db_manager,
        connection_name: str,
        *,
        mode: str = "ask",
    ) -> dict[str, Any]:
        """Handle cross-tab routing/pull before a normal ask/follow-up."""
        meta: dict[str, Any] = {
            "cross_tab_messages": [],
            "peer_bundles": [],
            "delegated": [],
        }
        route = self.parse_route_target(text)
        if route:
            tab_n, msg = route
            if msg:
                routed = self.route_to_tab(session_id, tab_n, msg)
                meta["delegated"].append(routed)
                initiator = self.sessions.get(session_id)
                if routed.get("routed") and initiator:
                    summary = self.agent.export_query_result_summary(
                        self.sessions.get_by_tab(tab_n),
                        routed.get("result") or {},
                    )
                    meta["cross_tab_messages"].append(
                        f"[Tab {tab_n} executed] {summary}"
                    )
                return {
                    "handled": True,
                    "skip_local_ai": True,
                    **meta,
                    "result": routed.get("result"),
                }

        initiator = self.sessions.get(session_id)
        if initiator:
            meta["peer_bundles"] = self.pull_peer_bundles(initiator, text)

        fn = (
            self.agent.send_follow_up
            if mode == "followup"
            else self.agent.start_new_conversation
        )
        # Only hold the per-session lock to flip status. The agent re-acquires
        # this same lock inside ``_bind_session`` during the call, and it is a
        # non-reentrant ``threading.Lock`` — holding it across ``fn`` would
        # deadlock the worker thread. Concurrency across sessions is already
        # serialized by the agent's ``_session_bind_lock``.
        if initiator:
            with initiator._lock:
                initiator.status = SessionStatus.RUNNING
        try:
            result = fn(
                text,
                db_manager,
                connection_name,
                session_id=session_id,
                peer_bundles=meta["peer_bundles"],
            )
        finally:
            if initiator:
                with initiator._lock:
                    initiator.status = SessionStatus.IDLE
        return {"handled": False, "result": result, **meta}
