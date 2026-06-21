"""
AI Query service layer — natural language to SQL and multi-session API.

Uses a :class:`CoreDBService` for DB connections. Shipped with ``ai_query/`` only.
"""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING, Any, Optional

from common.config_loader import console_print

if TYPE_CHECKING:
    from common.headless.db_service import CoreDBService

try:
    from ai_query.agent import AIQueryAgent
    _AI_AVAILABLE = True
except ImportError:
    _AI_AVAILABLE = False
    AIQueryAgent = None


_HOME_DIR = os.path.realpath(os.path.expanduser("~"))


def _safe_error(exc: Exception) -> str:
    """Return a caller-safe error string and log the full detail server-side.

    Raw exception text can embed absolute filesystem paths (e.g. a developer's
    home directory) which should not leak to API callers. We log everything with
    full fidelity, then redact the home-directory prefix from the returned text.
    """
    detail = str(exc)
    console_print(f"[ai.service] {type(exc).__name__}: {detail}")
    if _HOME_DIR and _HOME_DIR in detail:
        detail = detail.replace(_HOME_DIR, "~")
    return detail


def make_service(core: Optional["CoreDBService"] = None):
    """Core + AI composite for module-only CLI/API."""
    from common.headless.composite import composite_service
    from common.headless.db_service import CoreDBService

    core = core or CoreDBService()
    return composite_service(core, AIService(core))


_AI_STATE_FILENAME = "ai_state.json"


class _UnavailableFeature:
    def __init__(self, feature: str):
        self.feature = feature

    def _result(self, *_args, **_kwargs) -> dict:
        return {
            "ok": False,
            "error": f"{self.feature} is not available in this build.",
            "reason": f"{self.feature} is not shipped with the Standard edition.",
        }

    def __getattr__(self, _name: str):
        return self._result


def _ai_state_path():
    """Path to the persistent AI service state (CLI/API toggles).

    The file is human-readable JSON. We use it for settings that must survive
    a fresh CLI invocation — primarily ``mask_pii`` and ``active_backend``.
    Storage lives in :func:`common.paths.session_dir` (default
    ``<DBASSISTANT_HOME>/session/ai_state.json``).
    """
    from common import paths as _paths

    return _paths.ai_state_path()


def _read_ai_state() -> dict:
    import json
    path = _ai_state_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_ai_state(state: dict) -> None:
    from common.concurrency import atomic_write_text
    import json
    atomic_write_text(_ai_state_path(), json.dumps(state, indent=2))


def _update_ai_state(updates: dict) -> None:
    """Merge *updates* into the persisted AI state atomically under a lock.

    Concurrent setters (PII toggle, backend selection from different UIs / API)
    no longer clobber each other's keys.
    """
    from common.concurrency import update_json_locked

    def _mut(cur):
        cur = cur if isinstance(cur, dict) else {}
        cur.update(updates)
        return cur

    update_json_locked(_ai_state_path(), _mut, default={})


class AIService:
    """Headless AI query API backed by a core connection service."""

    def __init__(self, core: Any):
        self._core = core
        self._ai = AIQueryAgent() if _AI_AVAILABLE else None
        # Registry of in-flight harvest jobs -> threading.Event, so a separate
        # CLI/API/UI call can request a graceful stop of a running harvest.
        # Guarded by a lock since harvest workers register/deregister jobs
        # concurrently with stop requests from other threads.
        self._harvest_cancels: dict[str, Any] = {}
        self._harvest_cancels_lock = threading.Lock()
        # Re-apply any persisted settings (e.g. PII toggle, last backend).
        # Backend restore goes through verify=True so the underlying backend
        # object actually becomes "available" — without that, _call_ai
        # short-circuits with "No AI backend available."
        if self._ai is not None:
            persisted = _read_ai_state()
            if "mask_pii" in persisted and hasattr(self._ai, "set_mask_pii"):
                try:
                    self._ai.set_mask_pii(bool(persisted["mask_pii"]))
                except Exception:
                    pass
            backend = persisted.get("active_backend") or ""
            if backend:
                try:
                    if not self._ai.set_backend(backend, verify=True, quiet=True):
                        # Fall back to remembering the choice even when probe
                        # fails (network blip, missing CLI): the user gets a
                        # clearer error later instead of a silent default.
                        self._ai.set_backend(backend, verify=False, quiet=True)
                except Exception:
                    pass
            fallback = persisted.get("fallback_backend") or ""
            if fallback and hasattr(self._ai, "set_fallback_backend"):
                try:
                    self._ai.set_fallback_backend(fallback, verify=False)
                except Exception:
                    pass

    def _ai_or_error(self) -> tuple:
        if not _AI_AVAILABLE or self._ai is None:
            return None, {
                "error": "AI query agent not available (check ai_query/agent.py)"
            }
        return self._ai, None

    def _resolve_session_ref(self, ref: str):
        ai, err = self._ai_or_error()
        if err:
            return None, err
        sess = ai.sessions.resolve(ref)
        if not sess:
            return None, {"error": f"Session not found: {ref}"}
        return sess, None

    def _orchestrator(self):
        from ai_query.cross_tab_orchestrator import CrossTabOrchestrator

        return CrossTabOrchestrator(
            self._ai,
            self._ai.sessions,
            lambda name: self._core.get_manager(name),
        )

    def list_ai_backends(self) -> dict:
        if not _AI_AVAILABLE or self._ai is None:
            return {
                "available": False,
                "error": "AI query agent not available (check ai_query/agent.py)",
                "all": [],
                "ready": [],
                "active": "",
                "options": [],
                "active_value": "",
                "local_models": [],
                "fallback": "",
                "fallback_value": "",
            }
        try:
            return {
                "available": True,
                "error": None,
                "all": self._ai.list_all_backends(),
                "ready": self._ai.list_available_backends(),
                "active": self._ai.get_active_backend_name(),
                "options": self._ai.list_backend_options(),
                "active_value": self._ai.get_active_backend_value(),
                "local_models": self._ai.list_local_models(),
                "fallback": self._ai.get_fallback_backend_name(),
                "fallback_value": self._ai.get_fallback_backend_value(),
            }
        except Exception as exc:
            return {
                "available": False,
                "error": _safe_error(exc),
                "all": [],
                "ready": [],
                "active": "",
                "options": [],
                "active_value": "",
                "local_models": [],
            }

    def ai_query(
        self,
        name: str,
        question: str,
        backend: str | None = None,
        *,
        sql_mode: str | None = None,
        sql_execution_rules: str | None = None,
    ) -> dict:
        if not _AI_AVAILABLE or self._ai is None:
            return {
                "sql": None,
                "explanation": None,
                "error": "AI query agent not available (check ai_query/agent.py)",
            }
        if backend and not self._ai.set_backend(backend):
            return {
                "sql": None,
                "explanation": None,
                "error": f"AI backend '{backend}' is not available.",
            }
        if not backend and not self._ai.is_available():
            self._ai.auto_select_backend()
        try:
            from ai_query.sql_execution_service import default_execution_rules_from_config
            from ai_query.sql_modes import normalize_sql_mode

            mgr = self._core.get_manager(name)
            sess = self._ai.sessions.create(connection_name=name, backend=backend or "")
            sess.sql_mode = normalize_sql_mode(sql_mode or sess.sql_mode or "summary")
            sess.sql_execution_rules = (
                sql_execution_rules
                if sql_execution_rules is not None
                else default_execution_rules_from_config()
            )
            sess.sql_modes_v2 = True
            result = self._ai.start_new_conversation(
                question, mgr, name, session_id=sess.session_id
            )
            self._ai.sessions.delete(sess.session_id)
            return {
                "sql": result.get("sql") or result.get("summary_sql"),
                "summary_sql": result.get("summary_sql") or result.get("sql"),
                "explanation": result.get("explanation"),
                "error": result.get("error"),
                "sql_mode": sess.sql_mode,
                "summary_mode_blocked": result.get("summary_mode_blocked"),
                "satisfied": result.get("satisfied"),
                "prompt_tokens_est": result.get("prompt_tokens_est"),
            }
        except Exception as exc:
            return {"sql": None, "explanation": None, "error": _safe_error(exc)}

    def ai_session_create(
        self,
        connection: str = "",
        backend: str | None = None,
        *,
        isolated: bool = False,
        share_context: bool = True,
        sql_mode: str | None = None,
        sql_execution_rules: str | None = None,
    ) -> dict:
        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            from ai_query.sql_execution_service import default_execution_rules_from_config
            from ai_query.sql_modes import normalize_sql_mode

            sess = ai.sessions.create(
                connection_name=connection,
                backend=backend or "",
                isolated=isolated,
                share_context=share_context,
            )
            if sql_mode:
                sess.sql_mode = normalize_sql_mode(sql_mode)
            if sql_execution_rules is not None:
                sess.sql_execution_rules = sql_execution_rules
            elif not sess.sql_execution_rules:
                sess.sql_execution_rules = default_execution_rules_from_config()
            sess.sql_modes_v2 = True
            if backend:
                ai.set_backend(backend, verify=False)
                sess.backend = backend
            return {"session": sess.to_dict(), "error": None}
        except Exception as exc:
            return {"session": None, "error": _safe_error(exc)}

    def ai_session_list(self) -> dict:
        ai, err = self._ai_or_error()
        if err:
            return {**err, "sessions": []}
        return {"sessions": ai.sessions.list_sessions(), "error": None}

    def ai_session_get(self, ref: str) -> dict:
        sess, err = self._resolve_session_ref(ref)
        if err:
            return {**err, "session": None}
        return {"session": sess.summary(), "error": None}

    def ai_session_delete(self, ref: str) -> dict:
        ai, err = self._ai_or_error()
        if err:
            return err
        sess, err = self._resolve_session_ref(ref)
        if err:
            return err
        ok = ai.sessions.delete(sess.session_id)
        return {"deleted": ok, "error": None if ok else "Session not found"}

    def ai_session_update(self, ref: str, **fields) -> dict:
        sess, err = self._resolve_session_ref(ref)
        if err:
            return err
        from ai_query.sql_modes import normalize_sql_mode

        if "connection" in fields and fields["connection"] is not None:
            sess.connection_name = fields["connection"]
        if "backend" in fields and fields["backend"] is not None:
            sess.backend = fields["backend"]
            self._ai.set_backend(fields["backend"], verify=False)
        if "share_context" in fields and fields["share_context"] is not None:
            sess.share_context = bool(fields["share_context"])
        if "isolated" in fields and fields["isolated"] is not None:
            sess.isolated = bool(fields["isolated"])
        if "sql_mode" in fields and fields["sql_mode"] is not None:
            sess.sql_mode = normalize_sql_mode(fields["sql_mode"])
            sess.sql_modes_v2 = True
        if "sql_execution_rules" in fields and fields["sql_execution_rules"] is not None:
            sess.sql_execution_rules = fields["sql_execution_rules"]
        return {"session": sess.to_dict(), "error": None}

    def ai_session_ask(self, ref: str, question: str, *, mode: str = "ask") -> dict:
        ai, err = self._ai_or_error()
        if err:
            return {**err, "sql": None, "explanation": None}
        sess, err = self._resolve_session_ref(ref)
        if err:
            return {**err, "sql": None, "explanation": None}
        conn = sess.connection_name
        if not conn:
            return {
                "sql": None,
                "explanation": None,
                "error": "Session has no connection",
            }
        if sess.backend and not ai.is_available():
            ai.set_backend(sess.backend, verify=False)
        elif not ai.is_available():
            ai.auto_select_backend()
        try:
            mgr = self._core.get_manager(conn)
            out = self._orchestrator().parse_and_execute(
                sess.session_id,
                question,
                mgr,
                conn,
                mode="followup" if mode == "followup" else "ask",
            )
            if out.get("skip_local_ai"):
                result = out.get("result") or {}
                return {
                    "sql": result.get("sql"),
                    "explanation": result.get("explanation"),
                    "error": result.get("error"),
                    "cross_tab": out,
                }
            result = out.get("result") or {}
            return {
                "sql": result.get("sql") or result.get("summary_sql"),
                "summary_sql": result.get("summary_sql") or result.get("sql"),
                "explanation": result.get("explanation"),
                "error": result.get("error"),
                "sql_mode": sess.sql_mode,
                "summary_mode_blocked": result.get("summary_mode_blocked"),
                "satisfied": result.get("satisfied"),
                "cross_tab": {
                    "messages": out.get("cross_tab_messages", []),
                    "peer_bundles": len(out.get("peer_bundles") or []),
                },
            }
        except Exception as exc:
            return {"sql": None, "explanation": None, "error": _safe_error(exc)}

    def ai_session_execute_sql(self, ref: str, sql: str) -> dict:
        ai, err = self._ai_or_error()
        if err:
            return {**err, "result": None}
        sess, err = self._resolve_session_ref(ref)
        if err:
            return {**err, "result": None}
        conn = sess.connection_name
        if not conn:
            return {"error": "Session has no connection", "result": None}
        try:
            from ai_query.sql_execution_service import execute_sql_with_rules

            mgr = self._core.get_manager(conn)
            out = execute_sql_with_rules(
                sql,
                mgr,
                sql_mode=sess.sql_mode,
                rules_text=sess.sql_execution_rules,
                agent=ai,
                connection_name=conn,
            )
            if out.get("blocked"):
                return {
                    "error": out.get("error"),
                    "blocked": True,
                    "explain_output": out.get("explain_output"),
                    "result": None,
                }
            if out.get("error"):
                return {
                    "error": out.get("error"),
                    "explain_output": out.get("explain_output"),
                    "result": None,
                }
            return {
                "error": None,
                "explain_output": out.get("explain_output"),
                "explain_note": out.get("explain_note"),
                "result": out.get("result"),
            }
        except Exception as exc:
            return {"error": _safe_error(exc), "result": None}

    def ai_execute_sql(self, connection: str, sql: str) -> dict:
        """Execute AI-generated SQL against *connection* with a hard read-only guard.

        Unlike :meth:`ai_session_execute_sql`, no AI session is required. The
        read-only guard rejects any data/schema-mutating statement before it can
        reach the database, so AI surfaces can never run DROP/DELETE/UPDATE/etc.
        """
        if not connection:
            return {"error": "Connection required"}
        from common.sql_guard import assert_read_only

        db_type = ""
        try:
            profile = self._core.get_connection_profile(connection) or {}
            db_type = profile.get("type") or profile.get("db_type") or ""
        except Exception:
            db_type = ""
        guard_err = assert_read_only(sql, db_type=db_type)
        if guard_err:
            return {"error": guard_err, "blocked": True}
        try:
            self._core.open_connection(connection)
        except Exception:
            pass
        return self._core.execute(connection, sql)

    def ai_session_follow_up(self, ref: str, message: str) -> dict:
        return self.ai_session_ask(ref, message, mode="followup")

    def ai_session_cross_tab(self, ref: str, instruction: str) -> dict:
        ai, err = self._ai_or_error()
        if err:
            return err
        sess, err = self._resolve_session_ref(ref)
        if err:
            return err
        orch = self._orchestrator()
        route = orch.parse_route_target(instruction)
        if route:
            tab_n, msg = route
            return orch.route_to_tab(sess.session_id, tab_n, msg or instruction)
        tabs = orch.parse_tab_references(instruction)
        if len(tabs) > 1:
            return orch.coordinate_team(sess.session_id, instruction)
        conn = sess.connection_name
        if not conn:
            return {"error": "Session has no connection"}
        mgr = self._core.get_manager(conn)
        return orch.parse_and_execute(sess.session_id, instruction, mgr, conn)

    def ai_session_save(self, path: str | None = None) -> dict:
        from pathlib import Path as P

        from ai_query.session_manager import save_sessions_merged

        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            p = save_sessions_merged(ai.sessions, P(path) if path else None)
            return {"path": str(p), "error": None}
        except Exception as exc:
            return {"path": None, "error": _safe_error(exc)}

    # ------------------------------------------------------------------
    # Parity additions (Phase 6) — explain / optimize / review SQL,
    # backend configure, schema-cache info/clear/show, PII toggle.
    # ------------------------------------------------------------------

    def _resolve_db_type(self, connection: str | None) -> str:
        """Best-effort resolution of a connection's db_type. Returns ``"SQL"``
        when the connection name is empty or cannot be resolved (no DB call).
        """
        if not connection:
            return "SQL"
        try:
            profile = self._core.get_connection_profile(connection)
            if profile:
                return profile.get("db_type") or "SQL"
        except Exception:
            pass
        return "SQL"

    @staticmethod
    def _is_agent_error_string(text: object) -> bool:
        """Detect known error-marker strings returned by ``AIQueryAgent``.

        ``explain_query`` / ``suggest_optimizations`` return raw strings; the
        UI inspects them by content. We mirror that detection here so the
        service surface always uses the ``{"error": ..., "...": None}``
        convention even when the underlying method doesn't.
        """
        if not isinstance(text, str):
            return False
        lower = text.lower().strip()
        if not lower:
            return True
        markers = (
            "claude cli not available",
            "ai cli not available",
            "ai backend not available",
            "could not generate",
            "error explaining",
            "error suggesting",
            "error:",
        )
        return any(lower.startswith(m) or m in lower[:120] for m in markers)

    def explain_sql(self, sql: str, connection: str = "", db_type: str = "") -> dict:
        """Explain a SQL statement (mirrors UI's "Explain query" button)."""
        ai, err = self._ai_or_error()
        if err:
            return {**err, "explanation": None}
        if not (sql or "").strip():
            return {"error": "Empty SQL.", "explanation": None}
        engine = db_type or self._resolve_db_type(connection)
        try:
            text = ai.explain_query(sql, engine)
            if self._is_agent_error_string(text):
                return {"error": str(text).strip() or "AI returned no response.",
                        "explanation": None, "db_type": engine}
            return {"error": None, "explanation": text, "db_type": engine}
        except Exception as exc:
            return {"error": _safe_error(exc), "explanation": None, "db_type": engine}

    def optimize_sql(self, sql: str, connection: str = "", db_type: str = "") -> dict:
        """Suggest optimizations for a SQL statement (UI's "Optimize")."""
        ai, err = self._ai_or_error()
        if err:
            return {**err, "suggestions": None}
        if not (sql or "").strip():
            return {"error": "Empty SQL.", "suggestions": None}
        engine = db_type or self._resolve_db_type(connection)
        try:
            text = ai.suggest_optimizations(sql, engine)
            if self._is_agent_error_string(text):
                return {"error": str(text).strip() or "AI returned no response.",
                        "suggestions": None, "db_type": engine}
            return {"error": None, "suggestions": text, "db_type": engine}
        except Exception as exc:
            return {"error": _safe_error(exc), "suggestions": None, "db_type": engine}

    def review_sql(
        self,
        sql: str,
        rules: str = "",
        connection: str = "",
        db_type: str = "",
        *,
        timeout: int | None = None,
    ) -> dict:
        """Run the UI's "Run Review" prompt against the active AI backend.

        ``rules`` is the same free-form review criteria text the UI's "Write
        Review Rules" dialog persists. When empty, we use a sensible default.
        """
        ai, err = self._ai_or_error()
        if err:
            return {**err, "review": None}
        if not (sql or "").strip():
            return {"error": "Empty SQL.", "review": None}
        if timeout is None:
            from ai_query import module_config as mc
            timeout = mc.get_int("ui.ai_query", "sql_review_timeout", default=60)
        engine = db_type or self._resolve_db_type(connection)
        criteria = (rules or "").strip() or (
            "Use standard SQL best practices and performance optimization "
            "guidelines (indexes, WHERE clauses on UPDATE/DELETE, LIMIT on "
            "large result sets, N+1 patterns, SQL injection)."
        )
        prompt = (
            f"You are an expert SQL reviewer. Review the following {engine} "
            "SQL query based on these criteria:\n\n"
            f"REVIEW CRITERIA:\n{criteria}\n\n"
            f"SQL QUERY TO REVIEW:\n{sql}\n\n"
            "Please provide a comprehensive review with the following sections:\n"
            "1. STRENGTHS: What's done well in this query\n"
            "2. ISSUES: Problems, vulnerabilities, or bad practices found\n"
            "3. RECOMMENDATIONS: Specific improvements with examples\n"
            "4. PRIORITY: Rank issues by severity (Critical/High/Medium/Low)\n"
            "5. OPTIMIZED VERSION: Provide an improved version if significant "
            "changes are needed\n"
        )
        try:
            res = ai._call_ai(prompt, timeout=timeout)
            text = (res or {}).get("response") or ""
            if not text:
                return {
                    "error": (res or {}).get("error") or "AI returned no response.",
                    "review": None, "db_type": engine,
                }
            return {"error": None, "review": text, "db_type": engine}
        except Exception as exc:
            return {"error": _safe_error(exc), "review": None, "db_type": engine}

    def configure_ai_backend(self, name: str, verify: bool = True) -> dict:
        """Set the active AI backend (UI's "Backend" dropdown).

        Persists to ``<DBASSISTANT_HOME>/session/ai_state.json`` so the choice survives CLI
        invocations and matches the long-running API server state.
        """
        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            ok = ai.set_backend(name, verify=bool(verify))
            active = ai.get_active_backend_name() if ok else ""
            if ok:
                # Persist the encoded selection (``local-llm::<model>``) so the
                # chosen local model survives CLI / API server restarts.
                _update_ai_state({"active_backend": ai.get_active_backend_value() or name})
            return {
                "ok": bool(ok),
                "active": active,
                "active_value": ai.get_active_backend_value() if ok else "",
                "message": (
                    "Backend set."
                    if ok
                    else f"AI backend '{name}' is not available."
                ),
            }
        except Exception as exc:
            return {"ok": False, "active": "", "message": str(exc)}

    def configure_ai_fallback_backend(self, name: str, verify: bool = True) -> dict:
        """Set (or clear, when *name* is empty) the fallback AI backend.

        The fallback serves as failover when the primary is unreachable and as
        the corrector that repairs SQL the primary got wrong. Persisted to
        ``ai_state.json`` so it survives CLI/API restarts.
        """
        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            ok = ai.set_fallback_backend(name, verify=bool(verify))
            value = ai.get_fallback_backend_value()
            _update_ai_state({"fallback_backend": value})
            if not (name or "").strip():
                msg = "Fallback backend cleared."
            elif ok:
                msg = "Fallback backend set."
            else:
                msg = f"Fallback backend '{name}' set but not currently available."
            return {
                "ok": bool(ok),
                "fallback": ai.get_fallback_backend_name(),
                "fallback_value": value,
                "message": msg,
            }
        except Exception as exc:
            return {"ok": False, "fallback": "", "fallback_value": "", "message": str(exc)}

    def correct_sql(
        self,
        question: str,
        sql: str,
        *,
        connection: str = "",
        db_type: str = "",
        error_text: str = "",
        mode: str = "syntax",
        backend: str = "",
    ) -> dict:
        """Repair a wrong/failed query via the fallback (or a chosen) backend.

        ``mode='syntax'`` fixes an execution failure; ``mode='interpretation'``
        rewrites a query that runs but answers the wrong intent. Returns
        ``{"ok", "sql", "explanation", "backend_used", "error"}``.
        """
        ai, err = self._ai_or_error()
        if err:
            return {"ok": False, "sql": None, "explanation": None,
                    "backend_used": "", "error": err.get("error")}
        db_manager = None
        if connection:
            try:
                db_manager = self._core.get_manager(connection)
            except Exception:
                db_manager = None
        try:
            res = ai.correct_sql(
                question, sql,
                db_type=db_type, error_text=error_text, mode=mode,
                connection_name=connection, db_manager=db_manager,
                backend_value=backend,
            )
            res["ok"] = bool(res.get("sql"))
            return res
        except Exception as exc:
            return {"ok": False, "sql": None, "explanation": None,
                    "backend_used": "", "error": _safe_error(exc)}

    def get_ai_cache_info(self) -> dict:
        """Schema-cache statistics for every connection currently cached."""
        ai, err = self._ai_or_error()
        if err:
            return {**err, "entries": []}
        try:
            entries = ai.get_cache_info() or []
            for e in entries:
                ts = e.get("timestamp")
                if ts is not None and not isinstance(ts, str):
                    e["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")
            return {"error": None, "entries": entries, "count": len(entries)}
        except Exception as exc:
            return {"error": _safe_error(exc), "entries": [], "count": 0}

    def clear_ai_cache(self, connection: str | None = None) -> dict:
        """Clear schema cache for *connection* (or all when omitted)."""
        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            ai.invalidate_cache(connection or None)
            return {
                "ok": True,
                "message": (
                    f"Cache cleared for '{connection}'." if connection
                    else "All AI caches cleared."
                ),
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def show_ai_cache(self, connection: str = "") -> dict:
        """Return cached schema info — either for one connection or last sent."""
        ai, err = self._ai_or_error()
        if err:
            return {**err, "schema": None}
        try:
            if connection:
                schema = ai.schema_cache.get(connection)
                if schema is None:
                    return {
                        "error": f"No cached schema for '{connection}'.",
                        "schema": None,
                        "connection": connection,
                    }
                return {"error": None, "schema": schema, "connection": connection}
            text = ai.get_last_schema_sent()
            return {"error": None, "schema_last_sent": text, "connection": ""}
        except Exception as exc:
            return {"error": _safe_error(exc), "schema": None}

    def get_pii_masking(self) -> dict:
        """Current state of PII masking on the AI agent."""
        ai, err = self._ai_or_error()
        if err:
            return {**err, "enabled": False}
        try:
            return {
                "error": None,
                "enabled": bool(getattr(ai, "mask_pii_enabled", True)),
            }
        except Exception as exc:
            return {"error": _safe_error(exc), "enabled": False}

    def set_pii_masking(self, enabled: bool) -> dict:
        """Toggle PII masking on the AI agent (UI's "Mask PII data" menu).

        Persists to ``<DBASSISTANT_HOME>/session/ai_state.json`` so the toggle survives CLI
        invocations (each CLI call spawns a fresh process which would
        otherwise re-read the config default).
        """
        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            ai.set_mask_pii(bool(enabled))
            _update_ai_state({"mask_pii": bool(enabled)})
            return {
                "ok": True,
                "enabled": bool(getattr(ai, "mask_pii_enabled", enabled)),
                "message": (
                    "PII masking enabled." if enabled else "PII masking disabled."
                ),
            }
        except Exception as exc:
            return {"ok": False, "enabled": False, "message": str(exc)}

    def ai_session_load(self, path: str | None = None) -> dict:
        from pathlib import Path as P

        from ai_query.session_manager import load_sessions_from_disk

        ai, err = self._ai_or_error()
        if err:
            return err
        try:
            p = load_sessions_from_disk(ai.sessions, P(path) if path else None)
            return {
                "path": str(p),
                "sessions": ai.sessions.list_sessions(),
                "error": None,
            }
        except Exception as exc:
            return {"path": None, "sessions": [], "error": _safe_error(exc)}

    # ------------------------------------------------------------------
    # RAG — retrieval-augmented Generate SQL (delegates to RagService).
    # Shared by UI (rag_panel + Use RAG toggle), CLI (`ai rag …`) and API.
    # ------------------------------------------------------------------

    def _rag(self):
        rag = getattr(self, "_rag_service", None)
        if rag is None:
            try:
                from ai_assistant.rag.service import RagService

                rag = RagService(self._core, self._ai)
            except ImportError:
                rag = _UnavailableFeature("RAG")
            self._rag_service = rag
        return rag

    def rag_index(self, connection: str, *, rebuild: bool = False) -> dict:
        return self._rag().index(connection, rebuild=rebuild)

    def rag_status(self, connection: str = "") -> dict:
        return self._rag().status(connection)

    @staticmethod
    def _rag_top_k(k: int | None) -> int:
        if k is not None:
            return k
        from ai_query import module_config as mc
        return mc.get_int("ai.rag", "top_k", default=8)

    def rag_search(self, connection: str, query: str, k: int | None = None) -> dict:
        return self._rag().search(connection, query, k=self._rag_top_k(k))

    def rag_context(self, connection: str, query: str, k: int | None = None) -> dict:
        return self._rag().context(connection, query, k=self._rag_top_k(k))

    def rag_ask(
        self,
        connection: str,
        question: str,
        *,
        k: int | None = None,
        backend: str | None = None,
        auto_index: bool = True,
    ) -> dict:
        return self._rag().ask(
            connection, question, k=self._rag_top_k(k),
            backend=backend, auto_index=auto_index,
        )

    def rag_add_example(
        self, connection: str, question: str, sql: str, description: str = ""
    ) -> dict:
        return self._rag().add_example(connection, question, sql, description)

    def rag_add_glossary(self, connection: str, term: str, definition: str) -> dict:
        return self._rag().add_glossary(connection, term, definition)

    def rag_add_examples(
        self, connection: str, records: list[dict], *, standalone: bool = False
    ) -> dict:
        return self._rag().add_examples(connection, records, standalone=standalone)

    def rag_add_examples_from_file(
        self,
        connection: str,
        file_path: str = "",
        *,
        content: str = "",
        fmt: str = "auto",
        standalone: bool = False,
    ) -> dict:
        return self._rag().add_examples_from_file(
            connection, file_path, content=content, fmt=fmt, standalone=standalone
        )

    def rag_add_document(
        self,
        scope: str,
        *,
        text: str | None = None,
        file_path: str | None = None,
        title: str = "",
        source: str = "",
        standalone: bool = False,
    ) -> dict:
        try:
            from ai_assistant.rag.service import RagDocumentRequest
        except ImportError:
            return self._rag().add_document()

        return self._rag().add_document(
            RagDocumentRequest(
                scope=scope,
                text=text,
                file_path=file_path,
                title=title,
                source=source,
                standalone=standalone,
            )
        )

    def rag_documents(self, scope: str) -> dict:
        return self._rag().documents(scope)

    def rag_remove_document(self, scope: str, source: str) -> dict:
        return self._rag().remove_document(scope, source)

    def rag_analytics_library(self) -> dict:
        return self._rag().analytics_library()

    def rag_seed_analytics(
        self, scope: str, categories: list[str] | None = None,
        *, standalone: bool = False,
    ) -> dict:
        return self._rag().seed_analytics(scope, categories, standalone=standalone)

    def rag_breakdown(self, scope: str) -> dict:
        return self._rag().breakdown(scope)

    def rag_clear(self, connection: str) -> dict:
        return self._rag().clear(connection)

    def rag_preview(self, connection: str, query: str, k: int | None = None) -> dict:
        return self._rag().preview(connection, query, k=self._rag_top_k(k))

    def rag_scope_overview(self, scope: str) -> dict:
        return self._rag().scope_overview(scope)

    def rag_eval(
        self,
        connection: str,
        *,
        gold: list[dict] | None = None,
        k: int | None = None,
        per_case: bool = False,
    ) -> dict:
        return self._rag().evaluate(
            connection, gold=gold, k=self._rag_top_k(k), per_case=per_case
        )

    def rag_drift(self, connection: str) -> dict:
        return self._rag().drift(connection)

    def rag_reindex_stale(
        self, connections: list[str] | None = None, *, force: bool = False
    ) -> dict:
        return self._rag().reindex_stale(connections, force=force)

    def rag_reindex_schedule_status(self) -> dict:
        return self._rag().reindex_schedule_status()

    def rag_reindex_schedule_start(self) -> dict:
        return self._rag().reindex_schedule_start()

    def rag_reindex_schedule_stop(self) -> dict:
        return self._rag().reindex_schedule_stop()

    def rag_search_multi(
        self, scopes: list[str], query: str, k: int | None = None
    ) -> dict:
        return self._rag().search_multi(scopes, query, k=self._rag_top_k(k))

    def rag_preview_multi(
        self, scopes: list[str], query: str, k: int | None = None
    ) -> dict:
        return self._rag().preview_multi(scopes, query, k=self._rag_top_k(k))

    def rag_add_codebase(
        self,
        folder: str,
        scope: str,
        *,
        standalone: bool = True,
        replace: bool = True,
        max_files: int | None = None,
    ) -> dict:
        return self._rag().add_codebase(
            folder, scope, standalone=standalone, replace=replace, max_files=max_files,
        )

    # ------------------------------------------------------------------
    # LLM — local trainable NL->SQL model (delegates to LlmService).
    # Shared by UI (llm_panel / Train LLM), CLI (`ai llm …`) and API.
    # ------------------------------------------------------------------

    def _llm(self):
        llm = getattr(self, "_llm_service", None)
        if llm is None:
            try:
                from ai_assistant.llm.service import LlmService

                llm = LlmService()
            except ImportError:
                llm = _UnavailableFeature("LLM")
            self._llm_service = llm
        return llm

    def _llm_trainer(self):
        trainer = getattr(self, "_llm_training_service", None)
        if trainer is None:
            try:
                from ai_assistant.llm.training_service import LlmTrainingService

                trainer = LlmTrainingService(self._core)
            except ImportError:
                trainer = _UnavailableFeature("LLM training")
            self._llm_training_service = trainer
        return trainer

    def _llm_harvester(self, backend: str | None = None, *, gen_workers: int = 1):
        """Build an LlmHarvestService whose backend access reuses the live AI
        Query Assistant path (one-shot generation, raw text, follow-up threads).

        Backend callables are wired ONLY when a backend is actually usable
        (explicit ``backend`` or an already-available agent). When no backend is
        available, harvesting falls back to curated-corpus + capture sources so
        it never blocks on backend calls.

        When *gen_workers* > 1, each worker thread gets its own ``AIService``
        (and ``AIQueryAgent``) via a factory so parallel generation is safe.
        """
        try:
            from ai_assistant.llm.harvest_service import LlmHarvestService
        except ImportError:
            return _UnavailableFeature("LLM harvest")

        ai = self._ai
        backend_ready = bool(backend) or (ai is not None and ai.is_available())
        if backend and ai is not None and not ai.is_available():
            backend_ready = bool(ai.set_backend(backend, verify=False))
        if not backend_ready:
            return LlmHarvestService(self._core)

        workers = max(1, int(gen_workers or 1))
        core = self._core
        parent = self

        def gen_text(prompt: str) -> str:
            agent, err = parent._ai_or_error()
            if err:
                return ""
            if backend:
                agent.set_backend(backend, verify=False)
            res = agent._call_ai(prompt, timeout=getattr(parent, "default_timeout", 120))
            return (res or {}).get("response") or ""

        def run_thread(connection: str, base_q: str, followups: list) -> list:
            created = parent.ai_session_create(connection=connection, backend=backend or None)
            sess = created.get("session") or {}
            ref = sess.get("session_id") or sess.get("ref") or sess.get("id")
            if not ref:
                return []
            turns: list[dict] = []
            try:
                base = parent.ai_session_ask(ref, base_q)
                turns.append({"question": base_q, "sql": base.get("sql"),
                              "explanation": base.get("explanation")})
                for f in followups:
                    r = parent.ai_session_follow_up(ref, f)
                    turns.append({"question": f, "sql": r.get("sql"),
                                  "explanation": r.get("explanation")})
            finally:
                try:
                    parent.ai_session_delete(ref)
                except Exception:
                    pass
            return turns

        if workers > 1:
            import threading

            _local = threading.local()

            def gen_sql_factory():
                if getattr(_local, "fn", None) is None:
                    svc = AIService(core)
                    if backend and svc._ai:
                        svc._ai.set_backend(backend, verify=False)
                    elif ai is not None and ai._active_backend and svc._ai:
                        svc._ai.set_backend(ai._active_backend.name, verify=False)
                    _local.fn = lambda c, q: svc.ai_query(c, q, backend=backend)
                return _local.fn

            return LlmHarvestService(
                self._core,
                generate_sql_factory=gen_sql_factory,
                generate_text_fn=gen_text,
                run_thread_fn=run_thread,
            )

        def gen_sql(connection: str, question: str) -> dict:
            return self.ai_query(connection, question, backend=backend)

        return LlmHarvestService(
            self._core,
            generate_sql_fn=gen_sql,
            generate_text_fn=gen_text,
            run_thread_fn=run_thread,
        )

    def llm_harvest(
        self,
        body: dict,
        *,
        progress: Any = None,
        should_stop: Any = None,
    ) -> dict:
        """Auto-harvest a validated NL->SQL corpus and train local models.

        Triggered only via an explicit Train-LLM action. Backend access reuses
        the AI Query Assistant; live AI ask/follow-up behavior is unchanged.
        Missing fields fall back to the ``ai.llm.harvest`` config section.

        Graceful cancellation: pass ``should_stop`` (a ``Callable[[], bool]``)
        directly, and/or include ``harvest_id`` in *body* so another call to
        :meth:`llm_harvest_stop` can request a stop. Stops happen only at safe
        checkpoints; a model write is never interrupted.
        """
        import threading

        body = self._apply_harvest_config(dict(body or {}))
        backend = str(body.get("backend") or "").strip() or None
        gen_workers = max(1, int(body.get("gen_workers") or 1))

        harvest_id = str(body.get("harvest_id") or "").strip()
        event = None
        if harvest_id:
            event = threading.Event()
            with self._harvest_cancels_lock:
                self._harvest_cancels[harvest_id] = event

        def _should_stop() -> bool:
            if callable(should_stop) and should_stop():
                return True
            return bool(event is not None and event.is_set())

        try:
            return self._llm_harvester(backend, gen_workers=gen_workers).harvest(
                body, on_progress=progress, should_stop=_should_stop,
            )
        finally:
            if harvest_id:
                with self._harvest_cancels_lock:
                    self._harvest_cancels.pop(harvest_id, None)

    def llm_enrich_templates(
        self,
        body: dict,
        *,
        progress: Any = None,
        should_stop: Any = None,
    ) -> dict:
        """Enrich the reusable NL->SQL template library per dialect via the AI.

        Uses the selected backend to produce reusable, placeholder-parameterised
        templates for each dialect from a set of generic query intents, validates
        them, and persists them so the next train teaches the local model a single
        template it can reuse across every connection of that dialect (object
        names are filled from the connected schema at generation time).
        """
        from ai_assistant.llm.dataset import normalize_db_type

        body = dict(body or {})
        backend = str(body.get("backend") or "").strip() or None
        harvester = self._llm_harvester(backend)
        gen_text = getattr(harvester, "_generate_text_fn", None)
        if gen_text is None:
            return {"ok": False,
                    "error": "No AI backend available to enrich templates. "
                             "Select/configure a backend first."}

        conns = [str(c).strip() for c in (body.get("connections") or [])
                 if str(c).strip()]
        single = str(body.get("connection") or "").strip()
        if single and single not in conns:
            conns.insert(0, single)
        conn_by_dbtype: dict[str, str] = {}
        for c in conns:
            dt = normalize_db_type(self._resolve_db_type(c))
            if dt and dt not in conn_by_dbtype:
                conn_by_dbtype[dt] = c

        questions = body.get("questions") or None
        questions_file = str(body.get("questions_file") or "").strip()
        if questions_file:
            try:
                from ai_assistant.llm.question_import import load_questions_from_file

                loaded = load_questions_from_file(questions_file)
                questions = list(questions or []) + list(loaded)
            except Exception as exc:  # noqa: BLE001
                return {"ok": False,
                        "error": f"Failed to load questions file: {exc}"}

        from ai_assistant.llm.template_enrichment import enrich_templates

        return enrich_templates(
            generate_text_fn=gen_text,
            questions=questions,
            db_types=body.get("db_types") or None,
            conn_by_dbtype=conn_by_dbtype,
            core=self._core,
            limit_per_type=int(body.get("limit_per_type") or 0),
            persist=bool(body.get("persist", True)),
            on_progress=progress,
            should_stop=should_stop,
        )

    def llm_template_store_summary(self) -> dict:
        """Summary of the persisted AI-enriched template library."""
        from ai_assistant.llm import template_store

        return {"ok": True, **template_store.summary()}

    def llm_template_store_clear(self) -> dict:
        """Remove all AI-enriched templates from the store."""
        from ai_assistant.llm import template_store

        return template_store.clear()

    def llm_harvest_stop(self, harvest_id: str) -> dict:
        """Request a graceful stop of a running harvest by id.

        The harvest finishes any in-flight backend question, then trains on the
        pairs collected so far (the model write always completes) and returns.
        """
        hid = str(harvest_id or "").strip()
        with self._harvest_cancels_lock:
            event = self._harvest_cancels.get(hid)
        if event is None:
            return {"ok": False, "error": f"No running harvest with id '{hid}'."}
        event.set()
        return {"ok": True, "harvest_id": hid, "stopping": True}

    def llm_harvest_schedule_status(self) -> dict:
        from ai_assistant.llm.scheduler import get_harvest_scheduler

        return {"ok": True, **get_harvest_scheduler(self).status()}

    def llm_harvest_schedule_start(self) -> dict:
        from ai_assistant.llm.scheduler import get_harvest_scheduler

        sched = get_harvest_scheduler(self)
        sched.start()
        return {"ok": True, **sched.status()}

    def llm_harvest_schedule_stop(self) -> dict:
        from ai_assistant.llm.scheduler import get_harvest_scheduler

        sched = get_harvest_scheduler(self)
        sched.stop()
        return {"ok": True, **sched.status()}

    @staticmethod
    def _apply_harvest_config(body: dict) -> dict:
        """Fill harvest body defaults from the ``ai.llm.harvest`` config."""
        try:
            from ai_query import module_config as mc
        except Exception:
            return body

        def _default(key, getter, cfg_key=None):
            if key not in body or body[key] in (None, ""):
                body[key] = getter("ai.llm.harvest", cfg_key or key)

        try:
            _default("use_curated", mc.get_bool)
            _default("use_captures", mc.get_bool)
            _default("followups", mc.get_bool, "use_followups")
            _default("mine_db", mc.get_bool)
            if not body.get("train_mode"):
                body["train_mode"] = mc.get("ai.llm", "train_mode", default="full")
            if not body.get("training_depth"):
                body["training_depth"] = mc.get(
                    "ai.llm.harvest", "training_depth", default="offline")
            if not body.get("complexity"):
                raw = mc.get("ai.llm.harvest", "complexity") or ""
                body["complexity"] = [c.strip() for c in raw.split(",") if c.strip()] or None
            _default("generated_questions", mc.get_int)
            _default("max_questions", mc.get_int)
            _default("sample_limit", mc.get_int)
            _default("max_tables", mc.get_int)
            _default("gen_workers", mc.get_int)
            _default("gen_timeout", mc.get_int)
            _default("gen_retries", mc.get_int)
            _default("retry_backlog", mc.get_bool)
            _default("max_consecutive_failures", mc.get_int)
            if not body.get("template_mode"):
                body["template_mode"] = mc.get(
                    "ai.llm.harvest", "template_mode",
                    default=mc.get("ai.llm", "template_mode", default="both"),
                )
        except Exception:
            pass
        return body

    def llm_engines(self) -> dict:
        return self._llm().engines()

    def llm_train(
        self,
        *,
        name: str = "default",
        engine: str | None = None,
        include_sample: bool = True,
        dataset_path: str | None = None,
        rag_connection: str = "",
        overrides: dict | None = None,
    ) -> dict:
        return self._llm().train(
            name=name,
            engine=engine,
            include_sample=include_sample,
            dataset_path=dataset_path,
            rag_connection=rag_connection,
            overrides=overrides,
        )

    def llm_status(self, name: str = "default") -> dict:
        return self._llm().status(name)

    def llm_list(self) -> dict:
        return self._llm().list_models()

    def llm_generate(
        self,
        question: str,
        *,
        name: str = "default",
        engine: str | None = None,
        max_new: int = 0,
        temperature: float | None = None,
        connection: str = "",
        alternatives: bool = False,
    ) -> dict:
        db_type = ""
        db_manager = None
        if connection and self._core is not None:
            try:
                profile = self._core.get_connection_profile(connection) or {}
                db_type = profile.get("db_type", "") or ""
            except Exception:
                db_type = ""
            try:
                db_manager = self._core.get_manager(connection)
            except Exception:
                db_manager = None
        return self._llm().generate(
            question,
            name=name,
            engine=engine,
            max_new=max_new,
            temperature=temperature,
            connection=connection,
            db_type=db_type,
            core=self._core,
            alternatives=bool(alternatives),
            live={"db_manager": db_manager},
        )

    def llm_eval(
        self,
        *,
        name: str = "default",
        connection: str = "",
        depth: str | None = None,
        include_sample: bool = False,
        rag_connection: str = "",
        dataset_path: str | None = None,
    ) -> dict:
        db_type = ""
        if connection and self._core is not None:
            try:
                profile = self._core.get_connection_profile(connection) or {}
                db_type = profile.get("db_type", "") or ""
            except Exception:
                db_type = ""
        return self._llm().evaluate(
            name=name,
            connection=connection,
            db_type=db_type,
            core=self._core,
            depth=depth,
            include_sample=include_sample,
            rag_connection=rag_connection,
            dataset_path=dataset_path,
        )

    def llm_export(
        self, path: str, *, include_sample: bool = True, rag_connection: str = ""
    ) -> dict:
        return self._llm().export_dataset(
            path, include_sample=include_sample, rag_connection=rag_connection
        )

    def llm_model_dataset(
        self, *, name: str = "default", query: str = "", limit: int = 0
    ) -> dict:
        """Inspect the exact NL->SQL pairs a trained model was built on.

        Used to verify a specific question/SQL is "in" the model. ``query``
        filters by substring (question / SQL / description).
        """
        return self._llm().dataset(name, query=query, limit=limit)

    def llm_model_versions(self, *, name: str = "default") -> dict:
        """List saved snapshots (versions) of a trained model, newest first.

        A snapshot is taken automatically before every (re)train so a failed
        run can roll back and users can restore an earlier model on demand.
        """
        try:
            versions = self._llm().list_versions(name)
            return {"ok": True, "name": name, "versions": versions,
                    "count": len(versions)}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": _safe_error(exc), "versions": [], "count": 0}

    def llm_model_restore(self, *, name: str = "default", version: str = "") -> dict:
        """Roll a model back to a saved snapshot (current state is snapshotted first)."""
        if not str(version or "").strip():
            return {"ok": False, "error": "A version id is required."}
        try:
            return self._llm().restore_version(name, str(version).strip())
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": _safe_error(exc)}

    def llm_dataset(
        self, *, include_sample: bool = True, rag_connection: str = ""
    ) -> dict:
        """Return the NL->SQL dataset as JSONL text (no server-side write).

        Used by the Web UI export button so the file is downloaded by the
        browser instead of written to the server's filesystem.
        """
        return self._llm().export_dataset_content(
            include_sample=include_sample, rag_connection=rag_connection
        )

    def llm_train_rich(self, body: dict, *, progress: Any = None) -> dict:
        return self._llm_trainer().train_llm(body, on_progress=progress)

    def llm_train_multi(self, body: dict, *, progress: Any = None) -> dict:
        """Train one model from SEVERAL connections in parallel (shard + merge)."""
        return self._llm_trainer().train_from_connections(body, on_progress=progress)

    def llm_mine_pairs(self, body: dict) -> dict:
        return self._llm_trainer().mine_training_pairs(body)

    def llm_rag_status(self, connection: str = "") -> dict:
        return self._llm_trainer().rag_status(connection)

    def llm_index_rag(self, connection: str, *, rebuild: bool = False) -> dict:
        return self._llm_trainer().index_rag(connection, rebuild=rebuild)

    def llm_train_pairs(self, body: dict) -> dict:
        names = [str(n).strip() for n in (body.get("names") or body.get("train_llm") or [])
                 if str(n).strip()]
        new_name = str(body.get("new_name") or body.get("train_new_name") or "").strip()
        if new_name and new_name not in names:
            names.append(new_name)
        return self._llm_trainer().train_pairs(
            list(body.get("pairs") or []),
            names=names,
            engine=str(body.get("engine") or body.get("train_engine") or "").strip() or None,
            connection=str(body.get("connection") or "").strip(),
            include_sample=bool(body.get("include_sample", False)),
            use_rag=bool(body.get("use_rag", False)),
            train_mode=str(body.get("train_mode") or "full"),
        )
