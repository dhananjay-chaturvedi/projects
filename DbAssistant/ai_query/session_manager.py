"""
AI Query session registry — one session per UI tab / CLI session / API session.
"""

from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from ai_query.sql_modes import migrate_stored_sql_mode

_DEFAULT_MAX_STORED_SESSIONS = 50
_MAX_HISTORY_ON_DISK = 20


def _max_history_on_disk() -> int:
    try:
        from ai_query import module_config as mc
        return mc.get_int("ai.limits", "max_history_on_disk",
                          default=_MAX_HISTORY_ON_DISK)
    except Exception:
        return _MAX_HISTORY_ON_DISK


class SessionStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    WAITING = "waiting_on_tab"


@dataclass
class AISession:
    session_id: str
    tab_number: int
    connection_name: str = ""
    backend: str = ""
    conversation_history: list = field(default_factory=list)
    current_sql: Optional[str] = None
    current_db_type: Optional[str] = None
    status: SessionStatus = SessionStatus.IDLE
    share_context: bool = True
    isolated: bool = False
    last_result_summary: str = ""
    last_explanation_text: str = ""
    last_query_output_text: str = ""
    original_problem_statement: str = ""
    sql_mode: str = "summary"
    sql_execution_rules: str = ""
    sql_modes_v2: bool = True
    auto_loop_iteration: int = 0
    backend_session_id: Optional[str] = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "tab_number": self.tab_number,
            "connection_name": self.connection_name,
            "backend": self.backend,
            "status": self.status.value,
            "share_context": self.share_context,
            "isolated": self.isolated,
            "current_sql": self.current_sql,
            "current_db_type": self.current_db_type,
            "sql_mode": self.sql_mode,
            "sql_execution_rules": self.sql_execution_rules,
            "sql_modes_v2": self.sql_modes_v2,
            "message_count": len(self.conversation_history),
            "has_active_conversation": len(self.conversation_history) > 0,
        }

    def summary(self) -> dict[str, Any]:
        d = self.to_dict()
        d["conversation_summary"] = {
            "message_count": len(self.conversation_history),
            "has_sql": bool(self.current_sql),
        }
        return d


class AISessionManager:
    """In-memory registry of AI query sessions."""

    def __init__(self, max_sessions: int = 20):
        self._sessions: dict[str, AISession] = {}
        self._lock = threading.Lock()
        self._max_sessions = max_sessions
        self._next_tab = 1

    @property
    def default_session_id(self) -> Optional[str]:
        with self._lock:
            if not self._sessions:
                return None
            return next(iter(self._sessions.values())).session_id

    def create(
        self,
        connection_name: str = "",
        backend: str = "",
        *,
        isolated: bool = False,
        share_context: bool = True,
    ) -> AISession:
        with self._lock:
            if len(self._sessions) >= self._max_sessions:
                raise RuntimeError(f"Maximum session limit ({self._max_sessions}) reached.")
            sid = str(uuid.uuid4())
            tab = self._next_tab
            self._next_tab += 1
            sess = AISession(
                session_id=sid,
                tab_number=tab,
                connection_name=connection_name,
                backend=backend,
                isolated=isolated,
                share_context=share_context,
            )
            self._sessions[sid] = sess
            return sess

    def get(self, session_id: str) -> Optional[AISession]:
        with self._lock:
            return self._sessions.get(session_id)

    def resolve(self, ref: str) -> Optional[AISession]:
        ref = (ref or "").strip()
        if not ref:
            return None
        with self._lock:
            if ref in self._sessions:
                return self._sessions[ref]
            low = ref.lower().replace("tab", "").strip()
            if low.isdigit():
                n = int(low)
                for s in self._sessions.values():
                    if s.tab_number == n:
                        return s
            # Intentionally no prefix matching: a short/partial ``ref`` must not
            # resolve an unintended session. Only exact session id or tab number.
        return None

    def get_by_tab(self, tab_number: int) -> Optional[AISession]:
        with self._lock:
            for s in self._sessions.values():
                if s.tab_number == tab_number:
                    return s
        return None

    def list_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [s.to_dict() for s in sorted(self._sessions.values(), key=lambda x: x.tab_number)]

    def delete(self, session_id: str) -> bool:
        with self._lock:
            if session_id not in self._sessions:
                return False
            del self._sessions[session_id]
            self._renumber_tabs()
            return True

    def _renumber_tabs(self) -> None:
        for i, sess in enumerate(sorted(self._sessions.values(), key=lambda x: x.tab_number), start=1):
            sess.tab_number = i
        self._next_tab = len(self._sessions) + 1

    def export_state(self) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for s in self._sessions.values():
                out.append(export_session_record(s))
            return out

    def import_state(self, data: list[dict[str, Any]]) -> None:
        with self._lock:
            self._sessions.clear()
            for item in data:
                history = list(item.get("conversation_history") or [])
                if not history and item.get("fallback"):
                    last = (item.get("fallback") or {}).get("last_user_message")
                    if last:
                        history = [{"role": "user", "content": last}]
                sess = AISession(
                    session_id=item["session_id"],
                    tab_number=item["tab_number"],
                    connection_name=item.get("connection_name", ""),
                    backend=item.get("backend", ""),
                    conversation_history=history,
                    current_sql=item.get("current_sql"),
                    current_db_type=item.get("current_db_type"),
                    status=SessionStatus(item.get("status", "idle")),
                    share_context=bool(item.get("share_context", True)),
                    isolated=bool(item.get("isolated", False)),
                    last_result_summary=item.get("last_result_summary", ""),
                    last_explanation_text=item.get("last_explanation_text", ""),
                    last_query_output_text=item.get("last_query_output_text", ""),
                    original_problem_statement=item.get("original_problem_statement", ""),
                    sql_mode=migrate_stored_sql_mode(
                        item.get("sql_mode", "summary"),
                        sql_modes_v2=bool(item.get("sql_modes_v2", False)),
                    ),
                    sql_execution_rules=item.get("sql_execution_rules", ""),
                    sql_modes_v2=True,
                    auto_loop_iteration=int(item.get("auto_loop_iteration", 0)),
                    backend_session_id=item.get("backend_session_id") or None,
                )
                self._sessions[sess.session_id] = sess
            self._renumber_tabs()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _last_user_message(sess: AISession) -> str:
    for msg in reversed(sess.conversation_history):
        if isinstance(msg, dict) and msg.get("role") == "user":
            return str(msg.get("content") or "")[:500]
    return (sess.original_problem_statement or "")[:500]


def _build_fallback(sess: AISession) -> dict[str, Any]:
    return {
        "last_user_message": _last_user_message(sess),
        "sql_mode": sess.sql_mode,
        "message_count": len(sess.conversation_history),
    }


def export_session_record(
    sess: AISession,
    *,
    saved_from_close: bool = False,
    slim_on_backend_id: bool = True,
) -> dict[str, Any]:
    """Serialize one session for disk. Slim when a backend resume ID exists."""
    resume_supported = bool(sess.backend_session_id)
    if slim_on_backend_id and sess.backend_session_id:
        history: list = []
    else:
        history = list(sess.conversation_history)
        _hist_cap = _max_history_on_disk()
        if len(history) > _hist_cap:
            history = history[-_hist_cap:]

    record: dict[str, Any] = {
        "session_id": sess.session_id,
        "tab_number": sess.tab_number,
        "connection_name": sess.connection_name,
        "backend": sess.backend,
        "backend_session_id": sess.backend_session_id,
        "conversation_history": history,
        "current_sql": sess.current_sql,
        "current_db_type": sess.current_db_type,
        "status": SessionStatus.IDLE.value,
        "share_context": sess.share_context,
        "isolated": sess.isolated,
        "last_result_summary": sess.last_result_summary,
        "last_explanation_text": sess.last_explanation_text,
        "last_query_output_text": sess.last_query_output_text,
        "original_problem_statement": sess.original_problem_statement,
        "sql_mode": sess.sql_mode,
        "sql_execution_rules": sess.sql_execution_rules,
        "sql_modes_v2": sess.sql_modes_v2,
        "auto_loop_iteration": sess.auto_loop_iteration,
        "resume_supported": resume_supported,
    }
    if not resume_supported:
        record["fallback"] = _build_fallback(sess)
    if saved_from_close:
        record["saved_from_close"] = True
        record["saved_at"] = _utc_now_iso()
    return record


def max_stored_sessions() -> int:
    try:
        from ai_query import module_config as mc
        cap = mc.get_int("ai.limits", "max_stored_sessions", default=0)
        if cap > 0:
            return cap
        return mc.get_int("ai.limits", "default_max_stored_sessions",
                          default=_DEFAULT_MAX_STORED_SESSIONS)
    except Exception:
        return _DEFAULT_MAX_STORED_SESSIONS


def default_sessions_path() -> Path:
    try:
        from ai_query import module_config as mc
        filename = mc.get("ai.limits", "sessions_filename",
                          default="sessions.json") or "sessions.json"
    except Exception:
        filename = "sessions.json"
    # Reject any path component (``/``, ``\`` or ``..``) so the configured
    # filename can't escape the persistence directory via path traversal.
    base = os.path.basename(filename)
    if base != filename or base in ("", ".", ".."):
        filename = "sessions.json"
    return default_persistence_dir() / filename


def read_sessions_file(path: Path | None = None) -> list[dict[str, Any]]:
    path = path or default_sessions_path()
    if not path.is_file():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        sessions = data.get("sessions")
        if isinstance(sessions, list):
            return sessions
    raise ValueError(f"Invalid sessions file format: {path}")


def _trim_stored_sessions(records: list[dict[str, Any]], cap: int) -> list[dict[str, Any]]:
    if len(records) <= cap:
        return records
    records = sorted(
        records,
        key=lambda r: r.get("saved_at") or "",
        reverse=True,
    )
    return records[:cap]


def write_sessions_file(
    records: list[dict[str, Any]], path: Path | None = None
) -> Path:
    path = path or default_sessions_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    trimmed = _trim_stored_sessions(records, max_stored_sessions())
    payload = json.dumps(trimmed, indent=2)
    import uuid
    tmp = path.with_name(path.stem + "." + uuid.uuid4().hex + ".tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)  # atomic rename; no half-written file on crash
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
    return path


def merge_session_into_disk(
    sess: AISession,
    path: Path | None = None,
    *,
    saved_from_close: bool = False,
) -> Path:
    from common.concurrency import file_lock

    path = path or default_sessions_path()
    # Hold the lock across read→modify→write so concurrent savers (multiple
    # tabs / UIs / API) don't drop each other's sessions.
    with file_lock(path):
        by_id = {r["session_id"]: r for r in read_sessions_file(path)}
        by_id[sess.session_id] = export_session_record(
            sess, saved_from_close=saved_from_close
        )
        return write_sessions_file(list(by_id.values()), path)


def save_sessions_merged(manager: AISessionManager, path: Path | None = None) -> Path:
    """Save open tabs and keep closed-but-saved sessions already on disk."""
    from common.concurrency import file_lock

    path = path or default_sessions_path()
    with file_lock(path):
        on_disk = {r["session_id"]: r for r in read_sessions_file(path)}
        open_ids: set[str] = set()
        merged: list[dict[str, Any]] = []
        for meta in manager.list_sessions():
            sid = meta["session_id"]
            open_ids.add(sid)
            live = manager.get(sid)
            if live:
                merged.append(export_session_record(live, saved_from_close=False))
        for sid, rec in on_disk.items():
            if sid not in open_ids:
                merged.append(rec)
        return write_sessions_file(merged, path)


def default_persistence_dir() -> Path:
    from common import paths as _paths

    return _paths.ai_sessions_dir()


def load_sessions_from_disk(manager: AISessionManager, path: Path | None = None) -> Path:
    path = path or default_sessions_path()
    data = read_sessions_file(path)
    manager.import_state(data)
    return path
