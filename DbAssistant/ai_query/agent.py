# ---------------------------------------------------------------------
# description: AI manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""
AI Query Agent for Natural Language Database Queries
Supports multiple CLI-based AI backends: Claude, Cursor Agent, Codex.
Backend is auto-detected at startup; can be switched at runtime via set_backend().
"""

from contextlib import contextmanager
from datetime import datetime
import logging as _logging
import threading

from common.config_loader import console_print
from ai_query import module_config as mc
from ai_query.prompt_assembly import (
    build_schema_digest,
    cache_covers_needs,
    extract_referenced_tables,
    followup_instructions_block,
    format_table_schemas,
    looks_like_sql,
    merge_cross_tab_parts,
    needs_escalation,
    prompt_flags,
    schema_safety_rules_block,
    system_instructions_block,
)
from ai_query.sql_validation import validate_sql_against_schema
from ai_query.token_meter import record_prompt

# Max length for a database-type label embedded in an AI prompt.
_MAX_DB_TYPE_LEN = 40


def _safe_db_type(value) -> str:
    """Sanitize a DB-type label before interpolating it into an AI prompt.

    ``db_type`` comes from connection metadata, not user input, but a
    mis-configured connection or odd schema import could contain newlines or
    control characters that break the surrounding prompt structure. Collapse to
    a single short, printable token.
    """
    text = str(value or "").strip()
    text = " ".join(text.split())  # collapse all whitespace/newlines
    text = "".join(ch for ch in text if ch.isprintable())
    return text[:_MAX_DB_TYPE_LEN] or "SQL"


class AIQueryAgent:
    """AI-powered natural language to SQL converter — multi-backend."""

    def __init__(self):
        self.cli_available = False   # kept for backward-compat; True if any backend available
        self.cli_path = None         # kept for backward-compat

        self.conversation_history = []
        self.current_sql = None
        self.current_db_type = None
        self.last_context_sent = None

        # Timeouts — read from [ai.claude] (legacy key) or [ai] section
        self.default_timeout  = mc.get_int("ai.claude", "timeout",              default=120)
        self.simple_timeout   = mc.get_int("ai.claude", "simple_query_timeout", default=120)
        self.complex_timeout  = mc.get_int("ai.claude", "complex_query_timeout",default=180)
        self.followup_timeout = mc.get_int("ai.claude", "followup_timeout",     default=180)
        self.max_output_tokens= mc.get_int("ai.claude", "max_output_tokens",    default=4000)

        # Cache limits
        self.max_tables_fetch    = mc.get_int("ai.cache", "max_tables_fetch",    default=50)
        self.max_tables_detailed = mc.get_int("ai.cache", "max_tables_detailed", default=10)
        self.max_tables_display  = mc.get_int("ai.cache", "max_tables_display",  default=100)
        # Schema/context cache TTL in seconds. 0 (default) disables expiry, so a
        # cached schema lives until an explicit invalidate (e.g. reconnect) or a
        # db_type mismatch. A positive value forces a refresh once stale, so a
        # long-lived session eventually picks up DDL changes.
        self.cache_ttl_seconds   = mc.get_int("ai.cache", "ttl_seconds",         default=0)

        # Most recent ranked RAG hits (list of dicts with score/ref/kind/text),
        # populated by _augment_with_rag for the UI ranking panel.
        self.last_rag_hits: list = []
        self.last_prompt_tokens_est: int = 0
        self._refine_context_cache: dict[str, str] = {}

        # Enhanced caching
        self.schema_cache   = {}
        self.context_cache  = {}
        self.cache_metadata = {}
        self._cache_lock = threading.RLock()
        self._session_bind_lock = threading.RLock()

        # ── Backend registry (LAZY: no probing here) ───────────────────────
        # Construction is intentionally cheap — no subprocess, no network.
        # We only set the *active* backend to whatever the config asks for,
        # and we do NOT verify it.  Verification happens when the user
        # actually selects/uses it.
        from ai_query.backends import AIBackendRegistry
        self._registry = AIBackendRegistry()

        default_name = self._registry.get_default_name()      # "" if "auto"
        self._active_backend = (
            self._registry.get(default_name) if default_name else None
        )
        # cli_available stays False until a backend is verified
        self.cli_available = False

        # ── Fallback backend (optional) ─────────────────────────────────────
        # Serves two roles: (1) failover when the primary backend is
        # unavailable/unreachable, and (2) the corrector that fixes SQL the
        # primary (e.g. a local LLM) got wrong. Stored as the encoded selection
        # string (``local-llm::<model>`` is supported) and resolved lazily.
        self._fallback_value = (
            (mc.get("ai", "fallback_backend", default="") or "").strip()
        )

        from ai_query.session_manager import AISessionManager
        self.sessions = AISessionManager(
            max_sessions=mc.get_int("ai", "max_sessions", default=20)
        )

        self.mask_pii_enabled = mc.get_bool("ai", "mask_pii", default=True)
        self._last_pii_mask_findings: list[str] = []
        self.last_prompt_sent: str | None = None

    # ------------------------------------------------------------------
    # PII masking (outbound AI prompts only)
    # ------------------------------------------------------------------

    def set_mask_pii(self, enabled: bool) -> None:
        """Enable or disable PII/secret masking in prompts sent to AI backends."""
        self.mask_pii_enabled = bool(enabled)

    def mask_text_for_ai(self, text: str) -> str:
        """Mask PII/secrets in *text* when masking is enabled (prompt build only)."""
        if not self.mask_pii_enabled or not text:
            return text
        from ai_query.pii_masker import mask_pii

        result = mask_pii(text)
        if result.masked:
            self._last_pii_mask_findings = result.findings
            console_print(
                f"[PII] Masked {len(result.findings)} sensitive segment(s) before AI call"
            )
        return result.text

    def get_last_prompt_sent(self) -> str:
        """Return the full prompt last sent to the AI backend (for debugging)."""
        if not self.last_prompt_sent:
            return "No prompt has been sent to AI yet. Generate a query or send a follow-up first."
        return self.last_prompt_sent

    def _parse_and_build_result(
        self,
        response: str,
        *,
        keep_sql: str | None = None,
        context=None,
    ) -> dict:
        """Parse structured AI response and apply schema validation to summary SQL."""
        from ai_query.response_parser import parse_structured_ai_response, build_agent_result

        parsed = parse_structured_ai_response(response)
        result = build_agent_result(parsed, keep_sql=keep_sql)
        summary_sql = result.get("summary_sql")

        if summary_sql and context:
            validation_warnings = self._validate_sql_against_schema(summary_sql, context)
            if validation_warnings:
                warning_text = "\n\n⚠️ SCHEMA VALIDATION WARNINGS:\n" + "\n".join(
                    f"  • {w}" for w in validation_warnings
                )
                result["explanation"] = (result.get("explanation") or "") + warning_text

        return result

    def _sql_mode_for_session(self, session_id=None) -> str:
        from ai_query.sql_modes import normalize_sql_mode

        if session_id:
            sess = self.sessions.get(session_id)
            if sess and sess.sql_mode:
                return normalize_sql_mode(sess.sql_mode)
        return "summary"

    def _execution_rules_for_session(self, session_id=None) -> str:
        if session_id:
            sess = self.sessions.get(session_id)
            if sess and getattr(sess, "sql_execution_rules", ""):
                return sess.sql_execution_rules
        return ""

    def _user_table_names_from_context(self, context) -> list[str]:
        if not context:
            return []
        schema = context.get("schema") or {}
        names = schema.get("tables") or []
        if names:
            return list(names)
        table_schemas = schema.get("table_schemas") or {}
        return list(table_schemas.keys())

    def _apply_sql_mode_validation(self, result: dict, context, sql_mode: str) -> dict:
        from ai_query.sql_modes import is_strict_summary

        if not is_strict_summary(sql_mode):
            return result
        summary_sql = result.get("summary_sql")
        if not summary_sql or not context:
            return result
        from ai_query.summary_sql_validator import validate_summary_mode_sql

        violations = validate_summary_mode_sql(
            summary_sql,
            context.get("database_type", ""),
            self._user_table_names_from_context(context),
        )
        if violations:
            result["summary_mode_blocked"] = True
            result["explanation"] = (
                (result.get("explanation") or "")
                + "\n\n⚠️ STRICT SUMMARY MODE (SQL not auto-runnable):\n"
                + "\n".join(f"  • {v}" for v in violations)
            )
        return result

    def run_auto_refine(
        self,
        problem_statement: str,
        db_manager,
        connection_name: str,
        session_id=None,
        panel_context: dict | None = None,
    ) -> dict:
        """Evaluate progress and optionally emit the next structured response."""
        panel_context = panel_context or {}
        if not self.cli_available:
            return {"error": "AI not available", "satisfied": False}

        context = self.get_cached_comprehensive_context(
            db_manager, connection_name, problem_statement
        )
        cache_key = f"{connection_name}:{context.get('_tables_signature', '')}"
        db_context = self._refine_context_cache.get(cache_key)
        if not db_context:
            db_context = self._build_intelligent_context(context, problem_statement)
            self._refine_context_cache[cache_key] = db_context
        sql_mode = panel_context.get("sql_mode") or self._sql_mode_for_session(session_id)
        tab_number = None
        if session_id:
            sess = self.sessions.get(session_id)
            if sess:
                tab_number = sess.tab_number

        from ai_query.response_parser import response_format_instructions

        format_block = response_format_instructions(
            context.get("database_type", db_manager.db_type),
            connection_name,
            tab_number,
            sql_mode,
            auto_refine=True,
            execution_rules=self._execution_rules_for_session(session_id),
        )

        prompt = f"""You are refining an answer in an automated loop for a database assistant.

ORIGINAL PROBLEM:
{self.mask_text_for_ai(problem_statement)}

CURRENT ITERATION: {panel_context.get('iteration', 0)}

LAST SUMMARY_SQL (Generated SQL panel):
{panel_context.get('summary_sql') or 'None'}

LAST EXPLANATION:
{panel_context.get('explanation') or 'None'}

LAST QUERY OUTPUT (Query results panel):
{self.mask_text_for_ai((panel_context.get('query_output') or '')[:8000])}

{db_context}

Review the problem, explanation, and query output. If the problem is fully answered,
set SATISFIED: yes and provide a brief final EXPLANATION (SUMMARY_SQL may be NO CHANGE).

If not satisfied, produce an improved structured response per the format below.

{format_block}
"""

        ai_result = self._call_ai(
            prompt, timeout=self.followup_timeout, path="auto_refine", tier=1
        )
        if not ai_result.get("response"):
            return {
                "error": ai_result.get("error") or "Auto-refine failed",
                "satisfied": False,
                "prompt_tokens_est": ai_result.get("prompt_tokens_est"),
            }

        parsed = self._parse_and_build_result(
            ai_result["response"],
            keep_sql=self.current_sql,
            context=context,
        )
        parsed = self._apply_sql_mode_validation(parsed, context, sql_mode)
        if parsed.get("satisfied"):
            parsed["is_clarification"] = True
        parsed["prompt_tokens_est"] = ai_result.get("prompt_tokens_est")
        return parsed

    # ------------------------------------------------------------------
    # Backend management (new multi-backend API)
    # ------------------------------------------------------------------

    def list_all_backends(self) -> list[str]:
        """Return names of ALL configured backends (no probing)."""
        return self._registry.list_all_names()

    def list_available_backends(self) -> list[str]:
        """Return only backends that have been verified available."""
        return self._registry.available_names()

    def get_active_backend_name(self) -> str:
        return self._active_backend.name if self._active_backend else ""

    # ── local-llm model selection ────────────────────────────────────────────
    @staticmethod
    def _split_selection(name: str) -> tuple[str, str]:
        """Split a backend selection ``local-llm::<model>`` into (backend, model).

        Non-local selections (or bare names) return an empty model.
        """
        raw = (name or "").strip()
        if "::" in raw:
            backend, _, model = raw.partition("::")
            return backend.strip(), model.strip()
        return raw, ""

    def _apply_local_model(self, model: str) -> None:
        if not model:
            return
        try:
            from ai_query.backends.local_llm_backend import LocalLlmBackend

            LocalLlmBackend.set_active_model(model)
        except Exception:
            pass

    def list_local_models(self) -> list[dict]:
        """List locally-trained NL->SQL models as ``[{"name", "engine"}, ...]``."""
        try:
            from ai_assistant.llm.service import LlmService

            return LlmService().list_models().get("models") or []
        except Exception:
            return []

    def get_active_local_model(self) -> str:
        lb = self._registry.get("local-llm")
        try:
            return lb._model_name() if lb is not None else ""
        except Exception:
            return ""

    def list_backend_options(self) -> list[dict]:
        """Selectable backend entries, expanding local-llm into one per model.

        Each option is ``{"value", "label", "backend", "model", "ready",
        "active"}`` where ``value`` is the token to pass back when selecting
        (``local-llm::<model>`` for trained local models).
        """
        ready = set(self.list_available_backends())
        active = self.get_active_backend_name()
        active_model = self.get_active_local_model()
        options: list[dict] = []
        for name in self.list_all_backends():
            backend = self._registry.get(name)
            resume = bool(getattr(backend, "supports_resume", False))
            if name == "local-llm":
                models = self.list_local_models()
                if not models:
                    display = backend.display_name if backend else name
                    options.append({
                        "value": name, "label": f"{display} (no model trained)",
                        "backend": name, "model": "", "ready": False,
                        "active": name == active, "resume_supported": resume,
                    })
                    continue
                for m in models:
                    mname = m.get("name", "")
                    eng = m.get("engine", "")
                    label = f"{mname} (local {eng})" if eng else f"{mname} (local)"
                    options.append({
                        "value": f"{name}::{mname}", "label": label,
                        "backend": name, "model": mname, "ready": True,
                        "active": (name == active and mname == active_model),
                        "resume_supported": resume,
                    })
            else:
                display = backend.display_name if backend else name
                options.append({
                    "value": name, "label": f"{name}  ({display})",
                    "backend": name, "model": "", "ready": name in ready,
                    "active": name == active, "resume_supported": resume,
                })
        return options

    def get_active_backend_value(self) -> str:
        """Encoded active selection (``local-llm::<model>`` for the local backend)."""
        name = self.get_active_backend_name()
        if name == "local-llm":
            model = self.get_active_local_model()
            return f"{name}::{model}" if model else name
        return name

    def check_backend(self, name: str, force: bool = True) -> dict:
        """
        Probe a single backend (subprocess / network).  This is the
        explicit entry-point called by the UI when the user selects
        a backend in the dropdown.

        Returns:
            {"available": bool, "reason": str, "info": dict}
        """
        name, model = self._split_selection(name)
        self._apply_local_model(model)
        backend = self._registry.get(name)
        if not backend:
            return {"available": False, "reason": f"Unknown backend '{name}'", "info": {}}
        ok = backend.check_availability(force=force)
        return {
            "available": ok,
            "reason":    "" if ok else (backend.get_unavailable_reason() or "Unknown error"),
            "info":      backend.get_info(),
        }

    def set_backend(self, name: str, verify: bool = True, *, quiet: bool = False) -> bool:
        """
        Switch to a different AI backend.

        verify=True (default) probes the backend before switching.
        quiet=True suppresses user-facing console output (used during silent
        startup restore so we don't pollute stdout of unrelated CLI commands).
        A selection of the form ``local-llm::<model>`` also sets the active local
        model for the session before switching.
        Returns True on success, False if the backend is unavailable.
        """
        name, model = self._split_selection(name)
        self._apply_local_model(model)
        backend = self._registry.get(name)
        if not backend:
            if not quiet:
                console_print(f"AI backend '{name}' not registered.")
            return False
        if verify and not backend.check_availability(force=True):
            reason = backend.get_unavailable_reason() or "not available"
            if not quiet:
                console_print(f"AI backend '{name}' unavailable: {reason}")
            self._active_backend = backend          # remember user's choice
            self.cli_available  = False
            return False
        self._active_backend = backend
        self.cli_available  = True
        if not quiet:
            console_print(f"AI backend switched to: {backend.display_name}")
        return True

    def is_available(self):
        """True if the currently-selected backend has been verified."""
        return self.cli_available

    def auto_select_backend(self, *, quiet: bool = False) -> bool:
        """
        Pick the first available backend (config default, then in registry order).
        Used by headless / CLI / API entry points where there is no dropdown.
        Probes lazily; stops at the first that works.

        quiet=True suppresses the user-facing announcement (used by background
        service restore paths).
        """
        if self.cli_available and self._active_backend:
            return True

        ordered = []
        default_name = self._registry.get_default_name()
        if default_name:
            ordered.append(default_name)
        ordered += [n for n in self._registry.list_all_names() if n not in ordered]

        for name in ordered:
            backend = self._registry.get(name)
            if backend and backend.check_availability():
                self._active_backend = backend
                self.cli_available  = True
                if not quiet:
                    console_print(f"AI backend auto-selected: {backend.display_name}")
                return True
        return False

    def get_api_info(self):
        """Return display info for the active backend (no probing)."""
        if self._active_backend and self._active_backend.is_available():
            info = self._active_backend.get_info()
            return {
                "status":       info.get("status", "Connected"),
                "provider":     info.get("provider", self._active_backend.display_name),
                "model":        info.get("model", ""),
                "instructions": info.get("note", ""),
            }
        if self._active_backend:
            return {
                "status":       "Not Verified",
                "provider":     self._active_backend.display_name,
                "model":        "",
                "instructions": "Select this backend in the dropdown to check availability.",
            }
        return {
            "status":       "Not Selected",
            "provider":     "",
            "model":        "",
            "instructions": "Pick an AI backend from the dropdown.",
        }

    def _invoke_backend(self, backend, prompt, timeout, *, allow_resume=True):
        """Call one backend with optional session resume; return its raw result."""
        supports_resume = (
            allow_resume and bool(getattr(backend, "supports_resume", False)))
        resume_id = None
        bound_sid = getattr(self, "_bound_session_id", None)
        if supports_resume and bound_sid:
            sess = self.sessions.get(bound_sid)
            if sess and sess.backend_session_id:
                resume_id = sess.backend_session_id

        self.last_prompt_sent = prompt
        result = backend.call(prompt, timeout=timeout, resume_session_id=resume_id)
        new_sid = result.get("backend_session_id")
        if supports_resume and bound_sid and new_sid:
            sess = self.sessions.get(bound_sid)
            if sess:
                sess.backend_session_id = new_sid
        return result

    def _call_ai(self, prompt, max_tokens=None, timeout=None, *, path="unknown", tier=1):
        """
        Send *prompt* to the active AI backend, with automatic failover to the
        configured fallback backend when the primary is unavailable/unreachable
        or its call fails.

        Returns:
            dict with 'response' (str or None) and 'error' (str or None), plus
            ``used_fallback``/``backend_used`` when the fallback served the call.
        """
        if timeout is None:
            timeout = self.default_timeout

        backend_name = self.get_active_backend_name()
        rec = record_prompt(path=path, prompt=prompt, backend=backend_name, tier=tier)
        self.last_prompt_tokens_est = int(rec.get("prompt_tokens_est", 0))

        primary = self._active_backend
        fb = self._fallback_backend_obj()

        # Primary is verified-available: try it first, fail over on a call error.
        if primary is not None and primary.is_available():
            result = self._invoke_backend(primary, prompt, timeout)
            if result.get("response"):
                result["prompt_tokens_est"] = self.last_prompt_tokens_est
                return result
            if fb is not None and fb is not primary:
                try:
                    if fb.check_availability():
                        fb_res = self._invoke_backend(fb, prompt, timeout,
                                                      allow_resume=False)
                        if fb_res.get("response"):
                            fb_res["used_fallback"] = True
                            fb_res["backend_used"] = fb.name
                            fb_res["prompt_tokens_est"] = self.last_prompt_tokens_est
                            return fb_res
                except Exception as exc:
                    console_print(
                        f"[ai] fallback backend '{getattr(fb, 'name', '?')}' "
                        f"failed after primary error: {exc}"
                    )
            result["prompt_tokens_est"] = self.last_prompt_tokens_est
            return result

        # Primary unavailable/unknown/unreachable: serve from the fallback.
        if fb is not None:
            try:
                if fb.check_availability():
                    fb_res = self._invoke_backend(fb, prompt, timeout,
                                                  allow_resume=False)
                    fb_res["used_fallback"] = True
                    fb_res["backend_used"] = fb.name
                    fb_res["prompt_tokens_est"] = self.last_prompt_tokens_est
                    return fb_res
            except Exception as exc:
                console_print(
                    f"[ai] fallback backend '{getattr(fb, 'name', '?')}' "
                    f"failed while primary unavailable: {exc}"
                )
                return {"response": None,
                        "error": f"No AI backend available (fallback error: {exc}).",
                        "prompt_tokens_est": self.last_prompt_tokens_est}
        return {"response": None, "error": "No AI backend available.",
                "prompt_tokens_est": self.last_prompt_tokens_est}

    def active_backend_supports_resume(self) -> bool:
        """True when the active backend can resume a prior conversation."""
        return bool(getattr(self._active_backend, "supports_resume", False))

    def backend_supports_resume(self, name: str) -> bool:
        """True when the named backend can resume a prior conversation."""
        backend_name, _ = self._split_selection(name)
        b = self._registry.get(backend_name)
        return bool(getattr(b, "supports_resume", False)) if b else False

    # ------------------------------------------------------------------
    # Fallback backend (failover + SQL corrector)
    # ------------------------------------------------------------------

    def _fallback_backend_obj(self):
        """Resolve the configured fallback selection to a backend instance.

        The fallback may encode a local model (``local-llm::<model>``). We do
        NOT mutate the active local model here — that is applied transiently at
        call time via :meth:`call_backend` so the primary's model is preserved.
        """
        value = (getattr(self, "_fallback_value", "") or "").strip()
        if not value:
            return None
        name, _model = self._split_selection(value)
        return self._registry.get(name)

    def set_fallback_backend(self, name: str, verify: bool = True) -> bool:
        """Choose the fallback backend (encoded selection accepted).

        Pass an empty string to clear it. ``verify=True`` probes availability
        but the selection is remembered regardless so the corrector can retry
        later. Returns True when set (and available when verifying)."""
        value = (name or "").strip()
        self._fallback_value = value
        if not value:
            return True
        backend_name, _model = self._split_selection(value)
        backend = self._registry.get(backend_name)
        if not backend:
            return False
        if verify:
            return bool(backend.check_availability(force=True))
        return True

    def get_fallback_backend_value(self) -> str:
        """Encoded fallback selection (``local-llm::<model>`` or name or "")."""
        return getattr(self, "_fallback_value", "") or ""

    def get_fallback_backend_name(self) -> str:
        value = self.get_fallback_backend_value()
        if not value:
            return ""
        name, _ = self._split_selection(value)
        return name

    def has_fallback_backend(self) -> bool:
        return bool(self.get_fallback_backend_value())

    def call_backend(self, value: str, prompt: str, timeout=None) -> dict:
        """Call a specific backend by encoded selection regardless of the active
        one. Applies a local model only for the duration of this call so the
        session's active local model is not disturbed.

        Returns ``{"response", "error", "backend"}``.
        """
        if timeout is None:
            timeout = self.default_timeout
        value = (value or "").strip()
        if not value:
            return {"response": None, "error": "No backend selected.", "backend": ""}
        name, model = self._split_selection(value)
        backend = self._registry.get(name)
        if backend is None:
            return {"response": None,
                    "error": f"Unknown backend '{name}'", "backend": name}
        if not backend.check_availability(force=True):
            reason = backend.get_unavailable_reason() or "not available"
            return {"response": None,
                    "error": f"Backend '{name}' unavailable: {reason}",
                    "backend": name}
        prev_model = None
        applied_model = False
        if name == "local-llm" and model:
            try:
                from ai_query.backends.local_llm_backend import LocalLlmBackend
                prev_model = LocalLlmBackend.get_active_model_override()
                self._apply_local_model(model)
                applied_model = True
            except Exception:
                applied_model = False
        try:
            result = self._invoke_backend(backend, prompt, timeout,
                                          allow_resume=False)
        finally:
            if applied_model:
                try:
                    from ai_query.backends.local_llm_backend import LocalLlmBackend
                    LocalLlmBackend.set_active_model(prev_model or "")
                except Exception:
                    pass
        result["backend"] = name
        return result

    def correct_sql(
        self,
        question: str,
        bad_sql: str,
        *,
        db_type: str = "",
        error_text: str = "",
        mode: str = "syntax",
        connection_name: str = "",
        db_manager=None,
        backend_value: str = "",
    ) -> dict:
        """Ask the fallback (or a chosen) backend to repair ``bad_sql``.

        ``mode='syntax'`` repairs an execution failure; ``mode='interpretation'``
        rewrites a query that runs but answers the wrong intent. When a
        ``db_manager`` is supplied, schema context is included for accuracy.

        Returns ``{"sql", "explanation", "error", "backend_used"}``.
        """
        question = (question or "").strip()
        bad_sql = (bad_sql or "").strip()
        if not question:
            return {"sql": None, "explanation": None,
                    "error": "No question to correct.", "backend_used": ""}

        target = (backend_value or "").strip() or self.get_fallback_backend_value()
        if not target:
            return {"sql": None, "explanation": None,
                    "error": "No fallback backend configured.", "backend_used": ""}

        # Resolve dialect + schema context.
        dialect = (db_type or "").strip()
        db_context = ""
        if db_manager is not None:
            try:
                if not dialect:
                    dialect = getattr(db_manager, "db_type", "") or ""
                context = self.get_cached_comprehensive_context(
                    db_manager, connection_name, question)
                db_context = self._build_intelligent_context(context, question)
                if not dialect:
                    dialect = context.get("database_type", "") or ""
            except Exception:
                db_context = ""
        dialect = dialect or "sql"

        from ai_query.response_parser import response_format_instructions
        format_block = response_format_instructions(dialect, connection_name)

        q_for_ai = self.mask_text_for_ai(question)
        sql_for_ai = self.mask_text_for_ai(bad_sql)
        err_for_ai = self.mask_text_for_ai(error_text or "")

        if mode == "interpretation":
            instruction = (
                "The SQL below executes but DOES NOT correctly answer the user's "
                "request — it reflects a wrong interpretation of the intent. "
                "Re-read the question carefully and write a query that accurately "
                "answers what was actually asked (correct columns, filters, "
                "grouping, joins and aggregation)."
            )
        else:
            instruction = (
                "The SQL below FAILED to execute against the connected database. "
                "Return a corrected query that is directly executable and answers "
                "the question (fix syntax, identifiers, joins, subqueries, date "
                "handling and dialect-specific functions)."
            )

        prompt = f"""You are an expert {dialect} engineer correcting a database query.

{instruction}

QUESTION:
{q_for_ai}

CURRENT SQL:
{sql_for_ai}
"""
        if err_for_ai:
            prompt += f"\nEXECUTION ERROR:\n{err_for_ai}\n"
        if db_context:
            prompt += f"\nDATABASE CONTEXT:\n{db_context}\n"
        prompt += (
            f"\nReturn ONLY a valid, directly-executable {dialect} query and a brief "
            f"explanation of the fix.\n\n{format_block}"
        )

        res = self.call_backend(target, prompt)
        if res.get("error") and not res.get("response"):
            return {"sql": None, "explanation": None,
                    "error": res["error"], "backend_used": res.get("backend", "")}
        parsed = self._parse_and_build_result(res.get("response") or "")
        sql = parsed.get("summary_sql") or parsed.get("sql")
        return {
            "sql": sql,
            "explanation": parsed.get("explanation"),
            "error": None if sql else "Fallback backend returned no SQL.",
            "backend_used": res.get("backend", ""),
        }

    # kept as alias so any external code that calls _call_claude_cli still works
    def _call_claude_cli(self, prompt, max_tokens=None, timeout=None):
        return self._call_ai(prompt, max_tokens=max_tokens, timeout=timeout)

    def get_schema_info(
        self, db_manager, limit=None, include_schemas=True, schema_limit=None
    ):
        """
        Get comprehensive database schema information for context

        Args:
            db_manager: DatabaseManager instance
            limit: Max number of table names to retrieve (uses config default if None)
            include_schemas: Whether to fetch detailed table schemas
            schema_limit: Max number of tables to fetch detailed schemas for

        Returns:
            dict with database_type, tables, table_count, and table_schemas
        """
        # Use configured defaults if not provided
        if limit is None:
            limit = self.max_tables_fetch
        if schema_limit is None:
            schema_limit = self.max_tables_detailed

        from common.database_registry import DatabaseRegistry

        schema_info = {
            "database_type": _safe_db_type(db_manager.db_type),
            "tables": [],
            "table_count": 0,
            "table_schemas": {},  # Detailed schema for tables
        }

        try:
            # Get table names using registry
            console_print(f"  Fetching table list from {db_manager.db_type}...")
            tables = (
                DatabaseRegistry.execute_operation(
                    db_manager.db_type, "getTables", db_manager.conn
                )
                or []
            )

            schema_info["table_count"] = len(tables)
            schema_info["tables"] = tables[:limit]

            console_print(f"  Found {len(tables)} tables in database")

            # Fetch detailed schemas for the first N tables
            if (
                include_schemas
                and tables
                and DatabaseRegistry.supports_operation(
                    db_manager.db_type, "getTableSchema"
                )
            ):
                tables_to_fetch = tables[:schema_limit]
                console_print(
                    f"  Fetching detailed schema for first {len(tables_to_fetch)} tables..."
                )

                for table_name in tables_to_fetch:
                    try:
                        schema = DatabaseRegistry.execute_operation(
                            db_manager.db_type,
                            "getTableSchema",
                            db_manager.conn,
                            table_name,
                        )
                        if schema:
                            schema_info["table_schemas"][table_name] = schema
                    except Exception as e:
                        console_print(
                            f"    Warning: Could not get schema for {table_name}: {e}"
                        )
                        # Continue with other tables

                console_print(
                    f"  Successfully retrieved schema for {len(schema_info['table_schemas'])} table(s)"
                )

        except Exception as e:
            console_print(f"Error getting schema info: {e}")
            import traceback

            traceback.print_exc()
            console_print("  Will generate query with limited context")

        return schema_info

    def _analyze_question_complexity(self, question):
        """
        Analyze question to determine what context is needed

        Returns:
            dict with flags for what context to load
        """
        question_lower = question.lower()

        # Keywords that indicate need for different context types (English + Japanese)
        relationship_keywords = [
            # English
            "join",
            "relationship",
            "related",
            "foreign key",
            "reference",
            "connect",
            "link",
            "between",
            # Japanese
            "結合",
            "関連",
            "外部キー",
            "参照",
            "リレーション",
            "紐付",
            "繋",
        ]
        performance_keywords = [
            # English
            "slow",
            "performance",
            "optimize",
            "index",
            "explain",
            "bottleneck",
            "lock",
            "block",
            "session",
            "process",
            "running",
            # Japanese
            "遅い",
            "パフォーマンス",
            "最適化",
            "インデックス",
            "ボトルネック",
            "ロック",
            "セッション",
            "プロセス",
            "実行中",
            "速度",
            "高速化",
        ]
        analysis_keywords = [
            # English
            "analyze",
            "structure",
            "schema",
            "design",
            "model",
            "tablespace",
            "database",
            "report",
            "summary",
            "overview",
            # Japanese
            "分析",
            "構造",
            "スキーマ",
            "設計",
            "モデル",
            "テーブルスペース",
            "データベース",
            "レポート",
            "要約",
            "概要",
            "一覧",
        ]
        system_keywords = [
            # English
            "user",
            "role",
            "permission",
            "access",
            "grant",
            "privilege",
            # Japanese
            "ユーザー",
            "ロール",
            "権限",
            "アクセス",
            "許可",
            "特権",
        ]

        # Detect complexity
        needs_relationships = any(
            keyword in question_lower for keyword in relationship_keywords
        )
        needs_performance = any(
            keyword in question_lower for keyword in performance_keywords
        )
        needs_analysis = any(keyword in question_lower for keyword in analysis_keywords)
        needs_system = any(keyword in question_lower for keyword in system_keywords)

        # Check if question is simple (just basic SELECT/INSERT/UPDATE/DELETE)
        simple_patterns = [
            # English
            "select",
            "show",
            "get",
            "list",
            "find",
            "display",
            # Japanese
            "表示",
            "取得",
            "検索",
            "一覧",
            "見せ",
            "探",
            "リスト",
        ]
        is_simple = any(
            question_lower.startswith(pattern) for pattern in simple_patterns
        )

        # Word count heuristic - longer questions tend to be more complex
        word_count = len(question.split())
        is_complex = word_count > 10

        return {
            "needs_relationships": needs_relationships or is_complex,
            "needs_performance": needs_performance,
            "needs_analysis": needs_analysis,
            "needs_system": needs_system,
            "is_simple": is_simple
            and not (needs_relationships or needs_performance or needs_analysis),
            "complexity_score": sum(
                [needs_relationships, needs_performance, needs_analysis, needs_system]
            ),
        }

    def get_comprehensive_db_context(self, db_manager, connection_name, question=""):
        """
        Adaptively collect database context based on question complexity

        Args:
            db_manager: DatabaseManager instance
            connection_name: Connection identifier
            question: User's question to analyze for context needs

        Returns:
            dict with database context information (adaptive based on question)
        """
        from common.database_registry import DatabaseRegistry

        if db_manager is None or getattr(db_manager, "conn", None) is None:
            raise ConnectionError(
                f"Not connected to '{connection_name}'. Establish the database "
                f"connection before building query context."
            )

        # Analyze what context is needed
        analysis = self._analyze_question_complexity(question)

        console_print(
            f"[Context Analysis] Complexity: {'Simple' if analysis['is_simple'] else 'Complex'} "
            f"(score: {analysis['complexity_score']})"
        )

        context = {
            "database_type": _safe_db_type(db_manager.db_type),
            "question_complexity": analysis["complexity_score"],
            "schema": {},
            "system": {},
            "relationships": {},
            "performance": {},
            "metadata": {},
        }

        # 1. Basic Schema (ALWAYS collect - needed for all queries)
        tables = (
            DatabaseRegistry.execute_operation(
                db_manager.db_type, "getTables", db_manager.conn
            )
            or []
        )
        context["schema"]["tables"] = tables[: self.max_tables_display]
        context["schema"]["table_count"] = len(tables)

        # 2. Detailed table schemas
        # Simple queries: use max_tables_detailed
        # Complex queries: double the limit for complex queries
        schema_limit = (
            self.max_tables_detailed
            if analysis["is_simple"]
            else (self.max_tables_detailed * 2)
        )
        console_print(f"[Context] Loading detailed schemas for {schema_limit} tables")

        table_schemas = {}
        for table_name in tables[:schema_limit]:
            if DatabaseRegistry.supports_operation(
                db_manager.db_type, "getTableSchema"
            ):
                try:
                    schema = DatabaseRegistry.execute_operation(
                        db_manager.db_type,
                        "getTableSchema",
                        db_manager.conn,
                        table_name,
                    )
                    if schema:
                        table_schemas[table_name] = schema
                except Exception as e:
                    console_print(
                        f"    Warning: Could not get schema for {table_name}: {e}"
                    )

        context["schema"]["table_schemas"] = table_schemas

        # 3. Relationships & Constraints (if needed for complex queries)
        if analysis["needs_relationships"] or not analysis["is_simple"]:
            console_print("[Context] Loading relationships (constraints, indexes)")
            if DatabaseRegistry.supports_operation(
                db_manager.db_type, "getConstraints"
            ):
                try:
                    constraints = DatabaseRegistry.execute_operation(
                        db_manager.db_type, "getConstraints", db_manager.conn
                    )
                    context["relationships"]["constraints"] = (
                        constraints[:50] if constraints else []
                    )
                except Exception as e:
                    console_print(f"    Note: Could not get constraints: {e}")

            if DatabaseRegistry.supports_operation(db_manager.db_type, "getIndexes"):
                try:
                    indexes = DatabaseRegistry.execute_operation(
                        db_manager.db_type, "getIndexes", db_manager.conn
                    )
                    context["relationships"]["indexes"] = (
                        indexes[:50] if indexes else []
                    )
                except Exception as e:
                    console_print(f"    Note: Could not get indexes: {e}")

        # 4. Views, Procedures, Functions (if analysis needed)
        if analysis["needs_analysis"]:
            console_print(
                "[Context] Loading database objects (views, procedures, functions)"
            )
            for obj_type in ["getViews", "getProcedures", "getFunctions"]:
                if DatabaseRegistry.supports_operation(db_manager.db_type, obj_type):
                    try:
                        objects = DatabaseRegistry.execute_operation(
                            db_manager.db_type, obj_type, db_manager.conn
                        )
                        context["schema"][obj_type.lower()] = (
                            objects[:30] if objects else []
                        )
                    except Exception as e:
                        console_print(f"    Note: Could not get {obj_type}: {e}")

        # 5. System Information (if system query or analysis)
        if analysis["needs_system"] or analysis["needs_analysis"]:
            console_print("[Context] Loading system information (users, roles)")
            for op in ["getUsers", "getRoles"]:
                if DatabaseRegistry.supports_operation(db_manager.db_type, op):
                    try:
                        result = DatabaseRegistry.execute_operation(
                            db_manager.db_type, op, db_manager.conn
                        )
                        context["system"][op.lower()] = result[:20] if result else []
                    except Exception as e:
                        console_print(f"    Note: Could not get {op}: {e}")

        # Always get version (lightweight)
        version = DatabaseRegistry.execute_operation(
            db_manager.db_type, "getVersion", db_manager.conn
        )
        context["metadata"]["version"] = version

        # 6. Performance/Process Information (if troubleshooting query)
        if analysis["needs_performance"]:
            console_print(
                "[Context] Loading performance data (processes, sessions, activity)"
            )
            # MySQL/MariaDB: Process List
            if DatabaseRegistry.supports_operation(
                db_manager.db_type, "getProcessList"
            ):
                try:
                    processes = DatabaseRegistry.execute_operation(
                        db_manager.db_type, "getProcessList", db_manager.conn
                    )
                    context["performance"]["processes"] = (
                        processes[:20] if processes else []
                    )
                except Exception as e:
                    console_print(f"    Note: Could not get process list: {e}")

            # PostgreSQL: Activity
            if DatabaseRegistry.supports_operation(db_manager.db_type, "getActivity"):
                try:
                    activity = DatabaseRegistry.execute_operation(
                        db_manager.db_type, "getActivity", db_manager.conn
                    )
                    context["performance"]["activity"] = (
                        activity[:20] if activity else []
                    )
                except Exception as e:
                    console_print(f"    Note: Could not get activity: {e}")

            # Oracle: Sessions
            if DatabaseRegistry.supports_operation(db_manager.db_type, "getSessions"):
                try:
                    sessions = DatabaseRegistry.execute_operation(
                        db_manager.db_type, "getSessions", db_manager.conn
                    )
                    context["performance"]["sessions"] = (
                        sessions[:20] if sessions else []
                    )
                except Exception as e:
                    console_print(f"    Note: Could not get sessions: {e}")

        # 7. Tablespaces/Databases (if database-level analysis)
        if analysis["needs_analysis"]:
            console_print(
                "[Context] Loading storage metadata (tablespaces, databases, schemas)"
            )
            for op in ["getTablespaces", "getDatabases", "getSchemas"]:
                if DatabaseRegistry.supports_operation(db_manager.db_type, op):
                    try:
                        result = DatabaseRegistry.execute_operation(
                            db_manager.db_type, op, db_manager.conn
                        )
                        context["metadata"][op.lower()] = result[:30] if result else []
                    except Exception as e:
                        console_print(f"    Note: Could not get {op}: {e}")

        console_print("[Context] Context collection complete")
        context["_analysis"] = {
            k: analysis[k]
            for k in (
                "needs_relationships",
                "needs_performance",
                "needs_analysis",
                "needs_system",
                "is_simple",
                "complexity_score",
            )
        }
        context["_tables_signature"] = tuple(sorted(tables[: self.max_tables_display]))
        return context

    def _cache_is_expired(self, cache_key) -> bool:
        """True when a cached entry is older than the configured TTL.

        Caller must hold ``self._cache_lock``. Returns False when TTL is
        disabled (<= 0) or there is no timestamp recorded.
        """
        ttl = getattr(self, "cache_ttl_seconds", 0) or 0
        if ttl <= 0:
            return False
        metadata = self.cache_metadata.get(cache_key, {})
        ts = metadata.get("timestamp")
        if ts is None:
            return False
        try:
            age = (datetime.now() - ts).total_seconds()
        except Exception:
            return False
        return age > ttl

    def _schema_drift_detected(self, connection_name, db_manager, cached_data) -> bool:
        """Cheap DDL drift check: compare live table list to cached signature."""
        if not prompt_flags().get("schema_drift_check"):
            return False
        cached_sig = cached_data.get("_tables_signature")
        if not cached_sig:
            return False
        try:
            from common.database_registry import DatabaseRegistry

            tables = (
                DatabaseRegistry.execute_operation(
                    db_manager.db_type, "getTables", db_manager.conn
                )
                or []
            )
            live_sig = tuple(sorted(tables[: self.max_tables_display]))
            return live_sig != cached_sig
        except Exception as exc:
            console_print(f"[Context Cache] Drift check skipped: {exc}")
            return False

    def get_cached_schema_info(
        self,
        db_manager,
        connection_name,
        limit=None,
        schema_limit=None,
        include_schemas=True,
        force_refresh=False,
    ):
        """
        Get schema info with caching to avoid redundant database queries.

        Args:
            db_manager: Database manager instance
            connection_name: Unique connection identifier
            limit: Max number of tables to fetch (uses config default if None)
            schema_limit: Max tables to get detailed schemas for (uses config default if None)
            include_schemas: Whether to include detailed schema info
            force_refresh: If True, bypass cache and fetch fresh data

        Returns:
            Cached or fresh schema info dict
        """
        # Use configured defaults if not provided
        if limit is None:
            limit = self.max_tables_fetch
        if schema_limit is None:
            schema_limit = self.max_tables_detailed

        cache_key = connection_name

        # Check cache validity
        with self._cache_lock:
            if not force_refresh and cache_key in self.schema_cache:
                # Validate cached data matches current connection
                metadata = self.cache_metadata.get(cache_key, {})
                if metadata.get("db_type") != db_manager.db_type:
                    # DB type mismatch - invalidate cache
                    console_print(
                        f"[Schema Cache] Cache INVALID for {connection_name} (type mismatch)"
                    )
                    self.invalidate_cache(connection_name)
                elif self._cache_is_expired(cache_key):
                    console_print(
                        f"[Schema Cache] Cache EXPIRED for {connection_name} "
                        f"(TTL {self.cache_ttl_seconds}s)"
                    )
                    self.invalidate_cache(connection_name)
                else:
                    console_print(f"[Schema Cache] Cache HIT for {connection_name}")
                    return self.schema_cache[cache_key]

        # Cache miss or force refresh - fetch fresh data
        console_print(
            f"[Schema Cache] Cache MISS for {connection_name} - fetching from database"
        )
        schema_info = self.get_schema_info(
            db_manager, limit, include_schemas, schema_limit
        )

        # Store in cache
        with self._cache_lock:
            self.schema_cache[cache_key] = schema_info
            self.cache_metadata[cache_key] = {
                "db_type": db_manager.db_type,
                "timestamp": datetime.now(),
            }
        console_print(
            f"[Schema Cache] Cached schema for {connection_name} ({len(schema_info.get('tables', []))} tables)"
        )

        return schema_info

    def get_cached_comprehensive_context(
        self, db_manager, connection_name, question="", force_refresh=False
    ):
        """
        Get comprehensive context with intelligent caching

        Cache strategy:
        - Cache by complexity level (simple/complex)
        - Reuse cached context if current question needs <= cached complexity
        - Fetch fresh if current question needs > cached complexity
        """
        cache_key = connection_name

        # Analyze current question
        analysis = self._analyze_question_complexity(question)
        complexity_needed = analysis["complexity_score"]

        with self._cache_lock:
            if not force_refresh and cache_key in self.context_cache:
                cached_data = self.context_cache[cache_key]
                metadata = self.cache_metadata.get(cache_key, {})

                # Check if cached data is valid
                if metadata.get("db_type") != db_manager.db_type:
                    pass  # type mismatch falls through to a fresh fetch below
                elif self._cache_is_expired(cache_key):
                    console_print(
                        f"[Context Cache] Cache EXPIRED for {connection_name} "
                        f"(TTL {self.cache_ttl_seconds}s)"
                    )
                    self.invalidate_cache(connection_name)
                else:
                    if cache_covers_needs(cached_data, analysis):
                        if self._schema_drift_detected(
                            connection_name, db_manager, cached_data
                        ):
                            console_print(
                                f"[Context Cache] Schema drift for {connection_name} — refreshing"
                            )
                            self.invalidate_cache(connection_name)
                        else:
                            cached_analysis = cached_data.get("_analysis") or {}
                            console_print(
                                f"[Context Cache] Cache HIT for {connection_name} "
                                f"(covers needs, score "
                                f"{cached_analysis.get('complexity_score', 0)})"
                            )
                            return cached_data
                    else:
                        cached_complexity = cached_data.get("question_complexity", 0)
                        console_print(
                            f"[Context Cache] Cache insufficient for {connection_name} "
                            f"(cached: {cached_complexity}, needed: {complexity_needed}) "
                            f"— fetching more"
                        )

        console_print(
            f"[Context Cache] Cache MISS for {connection_name} - fetching from database"
        )
        context = self.get_comprehensive_db_context(
            db_manager, connection_name, question
        )

        # Store in cache
        with self._cache_lock:
            self.context_cache[cache_key] = context
            self.cache_metadata[cache_key] = {
                "db_type": db_manager.db_type,
                "timestamp": datetime.now(),
            }

        return context

    def invalidate_cache(self, connection_name=None):
        """Invalidate all caches"""
        with self._cache_lock:
            if connection_name:
                if connection_name in self.schema_cache:
                    del self.schema_cache[connection_name]
                if connection_name in self.context_cache:
                    del self.context_cache[connection_name]
                if connection_name in self.cache_metadata:
                    del self.cache_metadata[connection_name]
                console_print(f"[Cache] Invalidated all caches for {connection_name}")
            else:
                self.schema_cache.clear()
                self.context_cache.clear()
                self.cache_metadata.clear()
                console_print("[Cache] Cleared all caches")

    def get_cache_info(self):
        """Get information about cached schemas for debugging/UI display"""
        info = []
        with self._cache_lock:
            for conn_name, metadata in self.cache_metadata.items():
                schema_info = self.schema_cache.get(conn_name, {})
                info.append(
                    {
                        "connection": conn_name,
                        "db_type": metadata.get("db_type"),
                        "timestamp": metadata.get("timestamp"),
                        "table_count": len(schema_info.get("tables", [])),
                    }
                )
        return info

    def get_last_schema_sent(self):
        """Get the schema context that was last sent to AI for debugging"""
        if not self.last_context_sent:
            return "No schema has been sent to AI yet. Generate a query first."

        context = self.last_context_sent
        schema = context.get("schema", {})
        table_schemas = schema.get("table_schemas", {})

        if not table_schemas:
            return "No detailed schema information was available."

        output = "SCHEMA INFORMATION SENT TO AI\n"
        output += "=" * 80 + "\n\n"
        output += f"Database Type: {context.get('database_type', 'Unknown')}\n"
        output += f"Total Tables: {schema.get('table_count', 0)}\n"
        output += f"Detailed Schemas Loaded: {len(table_schemas)}\n\n"

        output += "AVAILABLE COLUMNS BY TABLE:\n"
        output += "-" * 80 + "\n\n"

        for table_name, columns in sorted(table_schemas.items()):
            output += f"TABLE: {table_name}\n"
            if columns:
                output += (
                    "  Columns: " + ", ".join([col["name"] for col in columns]) + "\n"
                )
            else:
                output += "  (No columns available)\n"
            output += "\n"

        output += "\n" + "=" * 80 + "\n"
        output += "Use these EXACT column names in your queries.\n"
        output += "If AI used different names, they are incorrect.\n"

        return output

    def _build_intelligent_context(
        self,
        context,
        user_question="",
        *,
        tier: int = 1,
        schema_mode: str = "full",
        referenced_tables: set[str] | None = None,
    ):
        """
        Build comprehensive, intelligent database context for AI prompt.

        schema_mode: ``full`` (default) or ``digest`` (follow-up compact view).
        tier: 1 = brief/compact, 2 = detailed (progressive escalation).
        """
        flags = prompt_flags()
        compact = flags["compact_schema"]
        if schema_mode == "digest" and flags["dedup_followup_schema"]:
            return build_schema_digest(
                context,
                referenced_tables or set(),
                compact=compact,
            )

        db_type = context["database_type"]
        complexity = context.get("question_complexity", 0)
        analysis = context.get("_analysis") or {}
        is_simple = analysis.get("is_simple", complexity == 0)
        header_sep = "\n" if compact else ("=" * 100 + "\n")

        output = header_sep
        output += "DATABASE CONTEXT\n" if compact else "COMPREHENSIVE DATABASE CONTEXT\n"
        if not compact:
            output += "=" * 100 + "\n"
        output += "\n"

        output += f"DB: {db_type}"
        if context.get("metadata", {}).get("version"):
            output += f" | Version: {context['metadata']['version']}"
        output += f" | Level: {'Basic' if complexity == 0 else 'Enhanced'}\n\n"

        schema = context.get("schema", {})
        output += f"Tables: {schema.get('table_count', 0)}"
        if schema.get("getviews"):
            output += f" | Views: {len(schema['getviews'])}"
        if schema.get("getprocedures"):
            output += f" | Procedures: {len(schema['getprocedures'])}"
        if schema.get("getfunctions"):
            output += f" | Functions: {len(schema['getfunctions'])}"
        output += "\n\n"

        table_schemas = schema.get("table_schemas", {})
        if table_schemas:
            if flags["consolidate_instructions"]:
                output += schema_safety_rules_block(db_type, tier=tier) + "\n\n"
            max_tables = 15 if tier == 1 else 20
            output += format_table_schemas(
                table_schemas,
                compact=compact,
                max_tables=max_tables,
            )

        relationships = context.get("relationships", {})
        if relationships:
            output += "RELATIONSHIPS:\n"
            constraints = relationships.get("constraints", [])
            if constraints:
                output += f"  Constraints: {', '.join(constraints[:30])}\n"
                if len(constraints) > 30:
                    output += f"  ... +{len(constraints) - 30} more\n"
            indexes = relationships.get("indexes", [])
            if indexes:
                output += f"  Indexes: {', '.join(indexes[:30])}\n"
                if len(indexes) > 30:
                    output += f"  ... +{len(indexes) - 30} more\n"
            output += "\n"

        all_tables = schema.get("tables", [])
        if all_tables:
            tables_shown = set(table_schemas.keys())
            remaining = [t for t in all_tables if t not in tables_shown]
            if remaining:
                output += f"OTHER TABLES ({len(remaining)}):\n"
                output += "  " + ", ".join(remaining[:40]) + "\n"
                if len(remaining) > 40:
                    output += f"  ... +{len(remaining) - 40} more\n"
                output += "\n"

        system = context.get("system", {})
        if system:
            output += "SYSTEM:\n"
            users = system.get("getusers", [])
            if users:
                output += f"  Users: {', '.join(users[:15])}\n"
            roles = system.get("getroles", [])
            if roles:
                output += f"  Roles: {', '.join(roles[:15])}\n"
            output += "\n"

        performance = context.get("performance", {})
        if performance:
            output += "PERFORMANCE:\n"
            if "processes" in performance:
                output += f"  Processes: {len(performance['processes'])}\n"
            if "activity" in performance:
                output += f"  Connections: {len(performance['activity'])}\n"
            if "sessions" in performance:
                output += f"  Sessions: {len(performance['sessions'])}\n"
            output += "\n"

        metadata = context.get("metadata", {})
        storage_items = [
            k
            for k in ("gettablespaces", "getdatabases", "getschemas")
            if k in metadata and metadata[k]
        ]
        if storage_items:
            output += "STORAGE:\n"
            for key in storage_items:
                items = metadata[key]
                if items:
                    label = key.replace("get", "").title()
                    output += f"  {label}: {', '.join(items[:10])}\n"
            output += "\n"

        if not compact:
            output += "=" * 100 + "\n"
        return output

    def _validate_sql_against_schema(self, sql, context):
        """Validate SQL identifiers against schema (AST-first, regex fallback)."""
        return validate_sql_against_schema(
            sql,
            context,
            db_type=context.get("database_type", "") if context else "",
        )

    # ------------------------------------------------------------------
    # Session-scoped conversation (multi-tab)
    # ------------------------------------------------------------------

    @contextmanager
    def _bind_session(self, session_id=None):
        """Bind agent conversation fields to a session for the duration of a call."""
        with self._session_bind_lock:
            prev_bound = getattr(self, "_bound_session_id", None)
            self._bound_session_id = session_id
            if not session_id:
                try:
                    yield None
                finally:
                    self._bound_session_id = prev_bound
                return
            sess = self.sessions.get(session_id)
            if not sess:
                self._bound_session_id = prev_bound
                raise ValueError(f"Unknown session: {session_id}")
            with sess._lock:
                prev_hist = self.conversation_history
                prev_sql = self.current_sql
                prev_db = self.current_db_type
                self.conversation_history = sess.conversation_history
                self.current_sql = sess.current_sql
                self.current_db_type = sess.current_db_type
                if sess.backend:
                    self.set_backend(sess.backend, verify=False)
            try:
                yield sess
            finally:
                with sess._lock:
                    sess.conversation_history = self.conversation_history
                    sess.current_sql = self.current_sql
                    sess.current_db_type = self.current_db_type
                    self.conversation_history = prev_hist
                    self.current_sql = prev_sql
                    self.current_db_type = prev_db
                self._bound_session_id = prev_bound

    def export_cross_tab_bundle(self, session, db_manager, question_hint=""):
        """Build a shareable context bundle for cross-tab prompts."""
        excerpt = session.conversation_history[-6:] if session.conversation_history else []
        schema_text = ""
        db_type = session.current_db_type
        if db_manager is not None and getattr(db_manager, "conn", None) is not None:
            ctx = self.get_cached_comprehensive_context(
                db_manager, session.connection_name, question_hint or "schema overview"
            )
            schema_text = self._build_intelligent_context(ctx, question_hint or "")
            db_type = db_type or ctx.get("database_type") or getattr(db_manager, "db_type", None)
        return {
            "tab_number": session.tab_number,
            "connection_name": session.connection_name,
            "backend": session.backend,
            "db_type": db_type,
            "current_sql": session.current_sql,
            "conversation_excerpt": excerpt,
            "schema_context": schema_text,
            "last_result_summary": session.last_result_summary or "",
            "last_explanation_text": getattr(session, "last_explanation_text", "") or "",
            "last_query_output_text": getattr(session, "last_query_output_text", "") or "",
            "sql_mode": getattr(session, "sql_mode", "summary"),
            "original_problem": getattr(session, "original_problem_statement", "") or "",
        }

    def build_cross_tab_prompt(self, local_context_text, peer_bundles, user_message):
        """Merge local DB context with peer-tab bundles for the AI prompt."""
        flags = prompt_flags()
        return merge_cross_tab_parts(
            local_context_text,
            peer_bundles or [],
            user_message,
            mask_fn=self.mask_text_for_ai,
            dedup=flags["dedup_crosstab_schema"],
        )

    def export_query_result_summary(self, session, query_result):
        """Summarize SQL generation/execution for sharing with other tabs."""
        if not session:
            return ""
        if not query_result:
            return session.last_result_summary or "No result"
        if query_result.get("error"):
            summary = f"Error: {query_result['error']}"
        else:
            sql = query_result.get("sql") or session.current_sql or ""
            expl = (query_result.get("explanation") or "")[:400]
            summary = f"SQL generated ({len(sql)} chars). {expl}"
        session.last_result_summary = summary
        return summary

    def execute_in_session(self, session_id, sql, db_manager):
        """Execute SQL on the connection owned by *session_id*."""
        sess = self.sessions.get(session_id)
        if not sess:
            raise ValueError(f"Unknown session: {session_id}")
        if db_manager is None or getattr(db_manager, "conn", None) is None:
            raise ConnectionError(f"Not connected for session tab {sess.tab_number}")
        from common.sql_guard import assert_read_only

        guard_err = assert_read_only(sql, db_type=getattr(db_manager, "db_type", "") or "")
        if guard_err:
            sess.last_result_summary = guard_err
            return {
                "result": {"error": guard_err, "blocked": True},
                "summary": guard_err,
                "blocked": True,
                "error": guard_err,
            }
        result = db_manager.execute_query(sql)
        summary = f"Executed on tab {sess.tab_number}: {result.get('rowcount', 0)} rows"
        sess.last_result_summary = summary
        return {"result": result, "summary": summary}


    def set_use_rag(self, enabled: bool) -> None:
        """Toggle retrieval-augmented grounding for the normal Generate SQL flow.

        Driven by the UI's "Use RAG" checkbox. When on (and the connection has a
        built RAG index), :meth:`_augment_with_rag` injects the most relevant
        schema objects + saved examples/glossary into the prompt.
        """
        self._use_rag = bool(enabled)

    def _rag_enabled(self) -> bool:
        if not getattr(self, "_use_rag", False):
            return False
        try:
            from ai_query import module_config as mc

            return mc.get_bool("ai.rag", "enabled", default=True)
        except Exception:
            return True

    def _augment_with_rag(
        self, db_context, question, connection_name, db_manager, analysis=None
    ):
        """Append RAG-retrieved schema context to the prompt (opt-in, best-effort).

        Uses the local vector index built for *connection_name*. Returns
        *db_context* unchanged when RAG is off, nothing is indexed, or any error
        occurs — so the normal flow is never broken by retrieval.
        """
        # Reset per-call so a panel never shows stale hits from a prior question.
        self.last_rag_hits = []
        if not self._rag_enabled() or not connection_name:
            return db_context
        try:
            from ai_query import module_config as mc
            from ai_assistant.rag.embeddings import get_embedder
            from ai_assistant.rag.retriever import RagRetriever
            from ai_assistant.rag.service import default_index_path
            from ai_assistant.rag.vector_store import SqliteVectorStore

            if mc.get_bool("ai.rag", "gate_by_complexity", default=True):
                analysis = analysis or self._analyze_question_complexity(question)
                if analysis.get("is_simple"):
                    return db_context

            store = SqliteVectorStore(default_index_path())
            if store.count(connection_name) == 0:
                return db_context
            embedder = get_embedder(
                mc.get("ai.rag", "embedding_provider", default="hash"),
                model=mc.get("ai.rag", "embedding_model", default="all-MiniLM-L6-v2"),
                dim=mc.get_int("ai.rag", "embedding_dim", default=256),
            )
            retr = RagRetriever(
                store, embedder,
                lexical_alpha=mc.get_float("ai.rag", "lexical_alpha", default=0.3),
            )
            top_k = mc.get_int("ai.rag", "top_k", default=8)
            if analysis and not analysis.get("is_simple") and analysis.get(
                "complexity_score", 0
            ) == 1:
                top_k = max(3, top_k // 2)
            hits = retr.search(
                connection_name, question,
                k=top_k,
            )
            # Record ranked hits (with scores) so the UI/CLI/API can show which
            # context was retrieved and how relevant it was.
            self.last_rag_hits = [h.to_dict() for h in hits]
            block = retr.format_context(hits)
        except Exception:
            block = ""
        if block:
            return f"{db_context}\n\n{block}\n"
        return db_context

    def _compose_ask_prompt(
        self,
        *,
        tier: int,
        db_context: str,
        context: dict,
        question_for_ai: str,
        format_block: str,
    ) -> str:
        """Assemble an ask prompt for Tier-1 (brief) or Tier-2 (detailed)."""
        flags = prompt_flags()
        db_type = context["database_type"]
        system_instructions = system_instructions_block(tier=tier)

        if flags["consolidate_instructions"]:
            rules = schema_safety_rules_block(db_type, tier=tier)
            rules_header = f"{rules}\n\n"
        elif tier >= 2:
            rules_header = f"""
═══════════════════════════════════════════════════════════════════════════════════
CRITICAL RULES - ZERO TOLERANCE FOR VIOLATIONS
═══════════════════════════════════════════════════════════════════════════════════

1. NEVER use column names that are NOT in the schema above
2. ONLY use the EXACT column names shown in the schema (case-sensitive for {db_type})
3. BEFORE writing SQL, verify EACH column exists in the schema
4. If a column is missing, state clearly — do NOT fabricate SQL
5. For {db_type}: use EXACT case for tables and columns

═══════════════════════════════════════════════════════════════════════════════════

"""
        else:
            rules_header = (
                f"Use ONLY exact schema column/table names (case-sensitive for {db_type}).\n\n"
            )

        step_block = ""
        if tier >= 2 or not flags["consolidate_instructions"]:
            step_block = """
STEP-BY-STEP PROCESS:
1. Read the question carefully
2. Identify which tables are needed
3. List EXACT column names from schema for those tables
4. Verify EVERY column you use exists in the schema above
5. Write SUMMARY_SQL using ONLY verified columns/objects
6. Provide DETAIL_SQL and INSIGHTS when helpful

"""

        remember = ""
        if tier >= 2 or not flags["consolidate_instructions"]:
            remember = (
                "\nREMEMBER: Using column names not in the schema is COMPLETELY UNACCEPTABLE.\n"
            )

        return f"""{system_instructions}

{db_context}

{rules_header}USER QUESTION: {question_for_ai}
{step_block}{format_block}{remember}"""

    def ask_question(
        self,
        question,
        db_manager,
        connection_name,
        peer_bundles=None,
        session_id=None,
    ):
        """
        Convert natural language question to SQL with adaptive intelligence

        Args:
            question: Natural language question
            db_manager: DatabaseManager instance
            connection_name: Connection identifier

        Returns:
            dict with 'sql', 'explanation', 'error'
        """
        if not self.cli_available:
            active = self.get_active_backend_name()
            if active:
                backend = self._registry.get(active)
                reason = (backend.get_unavailable_reason() if backend else "") or \
                         "Backend has not been verified — select it in the dropdown."
                err = f"AI backend '{active}' not ready: {reason}"
            else:
                err = "No AI backend selected. Pick one in the AI Query Assistant dropdown."
            return {"sql": None, "explanation": None, "error": err}

        if db_manager is None or getattr(db_manager, "conn", None) is None:
            err = (
                f"Not connected to '{connection_name}'. The database connection is "
                f"not established — connect to the database first, then retry your question."
            )
            console_print(f"[AI Agent] {err}")
            return {"sql": None, "explanation": None, "error": err}

        try:
            console_print("\n=== Intelligent Database Agent ===")
            console_print(f"Question: {question}")
            console_print(f"Database: {db_manager.db_type}")
            console_print(f"Connection: {connection_name}")

            if self.get_active_backend_name() == "local-llm":
                from ai_query.backends.local_llm_backend import LocalLlmBackend

                # Keep generation aligned with external backends: use the
                # connected DB for schema context, then execute later through
                # the shared guarded execution path instead of probing here.
                LocalLlmBackend.set_runtime(
                    connection=connection_name,
                    db_type=getattr(db_manager, "db_type", "") or "",
                    db_manager=db_manager,
                    ai_agent=self,
                )

            context = self.get_cached_comprehensive_context(
                db_manager, connection_name, question
            )
            analysis = context.get("_analysis") or self._analyze_question_complexity(question)

            # Remember scope for RAG retrieval / Local RAG backend isolation.
            self._current_connection_name = connection_name
            self._current_db_manager = db_manager

            question_for_ai = self.mask_text_for_ai(question)

            tab_number = None
            if session_id:
                sess = self.sessions.get(session_id)
                if sess:
                    tab_number = sess.tab_number

            from ai_query.response_parser import response_format_instructions

            sql_mode = self._sql_mode_for_session(session_id)
            flags = prompt_flags()
            complexity = context.get("question_complexity", 0)
            is_simple = analysis.get("is_simple", complexity == 0)

            def _prompt_for_tier(tier: int) -> str:
                db_ctx = self._build_intelligent_context(
                    context, question, tier=tier
                )
                if peer_bundles:
                    db_ctx = self.build_cross_tab_prompt(
                        db_ctx, peer_bundles, question
                    )
                db_ctx = self._augment_with_rag(
                    db_ctx,
                    question,
                    connection_name,
                    db_manager,
                    analysis,
                )
                fmt = response_format_instructions(
                    context.get("database_type", db_manager.db_type),
                    connection_name,
                    tab_number,
                    sql_mode,
                    execution_rules=self._execution_rules_for_session(session_id),
                    complexity=complexity,
                    is_simple=is_simple,
                )
                return self._compose_ask_prompt(
                    tier=tier,
                    db_context=db_ctx,
                    context=context,
                    question_for_ai=question_for_ai,
                    format_block=fmt,
                )

            timeout = self.complex_timeout if complexity >= 2 else self.simple_timeout
            console_print(
                f"Calling Intelligent Database Agent (timeout: {timeout}s, "
                f"complexity: {complexity})..."
            )

            prompt = _prompt_for_tier(1)
            result = self._call_ai(prompt, timeout=timeout, path="ask", tier=1)
            tier_used = 1

            if not result["response"]:
                error_msg = result["error"] or "Failed to get response from Claude CLI"
                return {
                    "sql": None,
                    "explanation": None,
                    "error": error_msg,
                    "prompt_tokens_est": result.get("prompt_tokens_est"),
                }

            response = result["response"]
            parsed_result = self._parse_and_build_result(response, context=context)
            parsed_result = self._apply_sql_mode_validation(
                parsed_result, context, sql_mode
            )
            sql = parsed_result.get("summary_sql")
            validation_warnings = (
                self._validate_sql_against_schema(sql, context) if sql else []
            )

            if (
                flags["progressive_escalation"]
                and needs_escalation(parsed_result, sql, validation_warnings)
            ):
                console_print("[AI Agent] Tier-1 response insufficient — escalating prompt")
                prompt = _prompt_for_tier(2)
                result = self._call_ai(prompt, timeout=timeout, path="ask", tier=2)
                tier_used = 2
                if result.get("response"):
                    response = result["response"]
                    parsed_result = self._parse_and_build_result(
                        response, context=context
                    )
                    parsed_result = self._apply_sql_mode_validation(
                        parsed_result, context, sql_mode
                    )
                    sql = parsed_result.get("summary_sql")
                    validation_warnings = (
                        self._validate_sql_against_schema(sql, context) if sql else []
                    )

            explanation = parsed_result.get("explanation")

            if not sql and not parsed_result.get("is_clarification"):
                candidate = response.strip()
                if looks_like_sql(
                    candidate, context.get("database_type", db_manager.db_type)
                ):
                    sql = candidate

            # Store in conversation
            self.current_sql = sql
            self.current_db_type = db_manager.db_type
            self.last_context_sent = context  # Store for debugging

            console_print("✓ Intelligent query generated successfully")
            console_print(f"  Summary SQL length: {len(sql) if sql else 0} characters")
            console_print(f"  Complexity: {context.get('question_complexity', 0)}")
            console_print("=" * 35 + "\n")

            # Capture the error state BEFORE clearing it — used for training signal.
            _gen_err = parsed_result.get("error")
            parsed_result["error"] = None
            if (
                not parsed_result.get("summary_sql")
                and not parsed_result.get("sql")
                and not parsed_result.get("is_clarification")
            ):
                return {
                    "sql": None,
                    "explanation": None,
                    "error": _gen_err or "AI produced no SQL for this question.",
                }
            if not parsed_result.get("summary_sql") and sql:
                parsed_result["summary_sql"] = sql
                parsed_result["sql"] = sql
            if not explanation:
                parsed_result["explanation"] = "Query generated successfully"
            # Surface the ranked RAG hits used for this answer (empty when RAG is
            # off or nothing was retrieved).
            parsed_result["rag_hits"] = list(getattr(self, "last_rag_hits", []) or [])
            parsed_result["prompt_tokens_est"] = result.get("prompt_tokens_est")
            parsed_result["prompt_tier"] = tier_used
            try:
                from ai_assistant.capture.pipeline import maybe_capture_turn
                _execution_signal = {
                    "ok": bool(sql) and not bool(_gen_err),
                    "error": _gen_err or None,
                }
                maybe_capture_turn(
                    question=question,
                    prompt=prompt,
                    raw_response=response,
                    parsed=parsed_result,
                    context=context,
                    connection_name=connection_name,
                    db_manager=db_manager,
                    backend=getattr(self._active_backend, "name", "") or "",
                    session_id=session_id,
                    is_followup=False,
                    execution=_execution_signal,
                )
            except Exception:
                _logging.getLogger(__name__).debug(
                    "Capture failed in ask_question", exc_info=True
                )
            return parsed_result

        except ConnectionError as e:
            console_print(f"[AI Agent] {e}")
            return {"sql": None, "explanation": None, "error": str(e)}
        except Exception as e:
            import traceback

            error_detail = traceback.format_exc()
            console_print(f"Error in intelligent agent: {error_detail}")
            return {"sql": None, "explanation": None, "error": f"Error: {str(e)}"}

    def explain_query(self, sql, db_type):
        """Explain what a SQL query does"""
        if not self.cli_available:
            return "Claude CLI not available"

        db_type = _safe_db_type(db_type)
        try:
            prompt = f"""Explain the following {db_type} SQL query in simple terms:

{sql}

Provide a clear, concise explanation of:
1. What data this query retrieves
2. What conditions/filters are applied
3. How the results are organized

Keep the explanation simple and easy to understand."""

            result = self._call_ai(prompt)
            if result["response"]:
                return result["response"]
            else:
                return f"Could not generate explanation: {result['error']}"

        except Exception as e:
            return f"Error explaining query: {str(e)}"

    def suggest_optimizations(self, sql, db_type):
        """Suggest optimizations for a SQL query"""
        if not self.cli_available:
            return "Claude CLI not available"

        db_type = _safe_db_type(db_type)
        try:
            prompt = f"""Analyze this {db_type} SQL query and suggest optimizations:

{sql}

Provide specific suggestions for:
1. Index usage
2. Query structure improvements
3. Performance considerations
4. {db_type}-specific optimizations

Format as a numbered list of actionable suggestions."""

            result = self._call_ai(prompt)
            if result["response"]:
                return result["response"]
            else:
                return f"Could not generate suggestions: {result['error']}"

        except Exception as e:
            return f"Error suggesting optimizations: {str(e)}"

    def start_new_conversation(
        self,
        initial_question,
        db_manager,
        connection_name,
        session_id=None,
        peer_bundles=None,
    ):
        """
        Start a new conversation with adaptive intelligence.

        When *session_id* is set, conversation state is stored on that session.
        """
        with self._bind_session(session_id) as sess:
            if sess:
                sess.connection_name = connection_name or sess.connection_name
            self.conversation_history.clear()
            self.current_sql = None
            self.current_db_type = db_manager.db_type if db_manager else None

            result = self.ask_question(
                initial_question,
                db_manager,
                connection_name,
                peer_bundles=peer_bundles,
                session_id=session_id,
            )

            if result["sql"] and not result["error"]:
                self.current_sql = result["sql"]
                self.conversation_history.append(
                    {"role": "user", "content": initial_question}
                )
                self.conversation_history.append(
                    {
                        "role": "assistant",
                        "content": f"Generated SQL:\n{result['sql']}\n\nExplanation: {result['explanation']}",
                    }
                )
            return result

    def send_follow_up(self, follow_up_message, db_manager, connection_name, session_id=None, peer_bundles=None):
        """
        Send a follow-up message with adaptive context awareness

        Args:
            follow_up_message: The follow-up question or correction request
            db_manager: DatabaseManager instance
            connection_name: Unique connection identifier for caching

        Returns:
            dict with 'sql', 'explanation', 'error', 'is_clarification'
        """
        with self._bind_session(session_id) as sess:
            if sess:
                sess.connection_name = connection_name or sess.connection_name
            return self._send_follow_up_impl(
                follow_up_message,
                db_manager,
                connection_name,
                peer_bundles=peer_bundles,
                session_id=session_id,
            )

    def _send_follow_up_impl(
        self,
        follow_up_message,
        db_manager,
        connection_name,
        peer_bundles=None,
        session_id=None,
    ):
        if not self.cli_available:
            return {
                "sql": None,
                "explanation": None,
                "error": "Claude CLI not available",
                "is_clarification": False,
            }

        if not self.conversation_history:
            return self.start_new_conversation(
                follow_up_message,
                db_manager,
                connection_name,
                session_id=session_id,
                peer_bundles=peer_bundles,
            )

        try:
            # Remember scope for RAG retrieval / Local RAG backend isolation.
            self._current_connection_name = connection_name
            self._current_db_manager = db_manager

            if self.get_active_backend_name() == "local-llm":
                from ai_query.backends.local_llm_backend import LocalLlmBackend

                # Keep follow-up generation non-blocking; connected-DB results
                # still come from the explicit guarded execution pipeline.
                LocalLlmBackend.set_runtime(
                    connection=connection_name,
                    db_type=getattr(db_manager, "db_type", "") or "",
                    db_manager=db_manager,
                    ai_agent=self,
                )

            context = self.get_cached_comprehensive_context(
                db_manager, connection_name, follow_up_message
            )
            analysis = context.get("_analysis") or self._analyze_question_complexity(
                follow_up_message
            )
            flags = prompt_flags()
            all_tables = context.get("schema", {}).get("tables", [])
            conv_blob = "\n".join(
                str(m.get("content", "")) for m in self.conversation_history
            )
            referenced = extract_referenced_tables(
                follow_up_message,
                self.current_sql or "",
                conv_blob,
                all_tables=all_tables,
            )
            schema_mode = "full"
            if flags["dedup_followup_schema"] and len(self.conversation_history) >= 2:
                schema_mode = "digest"

            db_context = self._build_intelligent_context(
                context,
                follow_up_message,
                schema_mode=schema_mode,
                referenced_tables=referenced,
            )
            if peer_bundles:
                db_context = self.build_cross_tab_prompt(
                    db_context, peer_bundles, follow_up_message
                )
            db_context = self._augment_with_rag(
                db_context,
                follow_up_message,
                connection_name,
                db_manager,
                analysis,
            )

            # Build conversation context - limit to last 10 messages to prevent context overflow
            max_history = 10
            recent_history = (
                self.conversation_history[-max_history:]
                if len(self.conversation_history) > max_history
                else self.conversation_history
            )

            conversation_text = ""
            if len(self.conversation_history) > max_history:
                conversation_text += f"[Earlier conversation omitted - showing last {max_history} messages]\n"

            for msg in recent_history:
                role = "User" if msg["role"] == "user" else "Assistant"
                content = self.mask_text_for_ai(msg["content"])
                conversation_text += f"\n{role}: {content}\n"

            follow_up_for_ai = self.mask_text_for_ai(follow_up_message)

            tab_number = None
            if session_id:
                sess = self.sessions.get(session_id)
                if sess:
                    tab_number = sess.tab_number

            from ai_query.response_parser import response_format_instructions
            sql_mode = self._sql_mode_for_session(session_id)
            complexity = context.get("question_complexity", 0)
            is_simple = analysis.get("is_simple", complexity == 0)
            format_block = response_format_instructions(
                context.get("database_type", db_manager.db_type),
                connection_name,
                tab_number,
                sql_mode,
                execution_rules=self._execution_rules_for_session(session_id),
                complexity=complexity,
                is_simple=is_simple,
            )

            instructions = followup_instructions_block(
                context["database_type"], tier=1 if flags["consolidate_instructions"] else 2
            )

            prompt = f"""You are an INTELLIGENT DATABASE AGENT helping refine queries.

LANGUAGE SUPPORT: Respond in the SAME language as the user's message.

{db_context}

Previous Conversation:
{conversation_text}

Current SQL Query:
{self.current_sql or 'None'}

User's Follow-up Message: {follow_up_for_ai}

USER QUESTION: {follow_up_for_ai}

{instructions}

{format_block}
"""

            console_print("\n=== AI Follow-up ===")
            console_print(f"Follow-up: {follow_up_message}")
            console_print(
                f"Context: {len(self.conversation_history)} previous messages (using last {len(recent_history)})"
            )
            console_print("Calling Claude CLI...")

            # Use longer timeout for follow-up messages due to conversation context
            result = self._call_ai(
                prompt, timeout=self.followup_timeout, path="follow_up", tier=1
            )

            if not result["response"]:
                error_msg = result["error"] or "Failed to get response from Claude CLI"
                return {
                    "sql": None,
                    "explanation": None,
                    "error": error_msg,
                    "is_clarification": False,
                }

            response = result["response"]

            parsed_result = self._parse_and_build_result(
                response,
                keep_sql=self.current_sql,
                context=context,
            )
            parsed_result = self._apply_sql_mode_validation(
                parsed_result, context, sql_mode
            )
            is_clarification = parsed_result.get("is_clarification", False)
            sql = parsed_result.get("summary_sql")
            explanation = parsed_result.get("explanation")

            if is_clarification:
                sql = self.current_sql

            # Update conversation history
            self.conversation_history.append(
                {"role": "user", "content": follow_up_message}
            )
            self.conversation_history.append(
                {
                    "role": "assistant",
                    "content": f"{'Clarification' if is_clarification else 'Updated SQL'}:\n{sql}\n\nExplanation: {explanation}",
                }
            )

            if not is_clarification and sql:
                previous_sql_for_capture = self.current_sql
                self.current_sql = sql
            else:
                previous_sql_for_capture = self.current_sql

            console_print(
                f"✓ Follow-up processed: {'Clarification' if is_clarification else 'SQL Updated'}"
            )
            console_print("=" * 30 + "\n")

            if not is_clarification and not sql:
                return {
                    "sql": None,
                    "explanation": None,
                    "error": parsed_result.get("error")
                    or "AI produced no SQL for this follow-up.",
                    "is_clarification": False,
                }

            parsed_result["error"] = None
            parsed_result["is_clarification"] = is_clarification
            parsed_result["sql"] = sql
            parsed_result["summary_sql"] = sql
            if not explanation:
                parsed_result["explanation"] = (
                    "Clarification provided" if is_clarification else "Query updated successfully"
                )
            parsed_result["prompt_tokens_est"] = result.get("prompt_tokens_est")
            try:
                from ai_assistant.capture.pipeline import maybe_capture_turn
                import logging as _logging

                _gen_err = parsed_result.get("error")
                _execution_signal = {
                    "ok": bool(sql) and not bool(_gen_err),
                    "error": _gen_err or None,
                }
                maybe_capture_turn(
                    question=follow_up_message,
                    prompt=prompt,
                    raw_response=response,
                    parsed=parsed_result,
                    context=context,
                    connection_name=connection_name,
                    db_manager=db_manager,
                    backend=getattr(self._active_backend, "name", "") or "",
                    session_id=session_id,
                    is_followup=True,
                    previous_sql=previous_sql_for_capture,
                    execution=_execution_signal,
                )
            except Exception:
                _logging.getLogger(__name__).debug(
                    "Capture failed in send_follow_up", exc_info=True
                )
            return parsed_result

        except Exception as e:
            import traceback

            error_detail = traceback.format_exc()
            console_print(f"Error in send_follow_up: {error_detail}")
            return {
                "sql": None,
                "explanation": None,
                "error": f"Error processing follow-up: {str(e)}",
                "is_clarification": False,
            }

    def clear_conversation(self, session_id=None):
        """Clear conversation history for legacy agent or a specific session."""
        with self._bind_session(session_id):
            self.conversation_history.clear()
            self.current_sql = None
            self.current_db_type = None

    def get_conversation_summary(self, session_id=None):
        """Get a summary of the conversation history."""
        with self._bind_session(session_id):
            return {
                "message_count": len(self.conversation_history),
                "has_active_conversation": len(self.conversation_history) > 0,
                "current_sql": self.current_sql,
            }
