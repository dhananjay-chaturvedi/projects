"""Local LLM backend — generate SQL from your own trained NL->SQL model.

This is a fully offline AI backend: instead of shelling out to an external CLI
(Claude/Cursor/Codex), it runs the local model trained via the LLM panel /
``dbtool ai llm train`` (python / numpy / pytorch / ollama engine). It needs no
network and no API key.

It becomes "available" only once a model has been trained; until then it reports
a clear reason so the user knows to train one (optionally seeded with their RAG
examples). The model name is read from ``[ai.llm] active_model`` (default
``default``).
"""

from __future__ import annotations

import re
import threading
from typing import Any, Optional

from ai_query.backends import AIBackend

_QUESTION_RE = re.compile(r"^USER QUESTION:\s*(.+?)\s*$", re.MULTILINE)

# Per-generation runtime context (live DB connection, schema core, etc.) is
# set right before ``call()`` runs *in the same worker thread*, so it is held in
# thread-local storage. This keeps concurrent tabs / server requests isolated:
# thread B's connection can never clobber thread A's mid-generation.
_tls = threading.local()


class LocalLlmBackend(AIBackend):
    name = "local-llm"
    display_name = "Local LLM (trained)"
    cli_command = ""  # no external CLI — pure in-process generation
    # The local NL->SQL model is stateless: every prompt is generated
    # independently with no server-side conversation, so there is nothing to
    # resume. Follow-ups re-send the needed context in the prompt itself.
    supports_resume = False

    # Session-scoped override for the active model (set when the user picks a
    # specific "<model> (local <engine>)" entry in the backend dropdown). When
    # empty, the model name falls back to ``[ai.llm] active_model`` config.
    # Guarded by ``_model_lock`` so reads/writes are atomic across threads.
    _active_model_override: str = ""
    _model_lock = threading.RLock()

    def __init__(self) -> None:
        super().__init__()
        self._svc = None

    @classmethod
    def set_runtime(
        cls,
        *,
        connection: str = "",
        db_type: str = "",
        core: Any = None,
        executor: Any = None,
        db_manager: Any = None,
        ai_agent: Any = None,
    ) -> None:
        _tls.runtime = {
            "connection": connection or "",
            "db_type": db_type or "",
            "core": core,
            "executor": executor,
            "db_manager": db_manager,
            "ai_agent": ai_agent,
        }

    @classmethod
    def _get_runtime(cls) -> dict[str, Any]:
        return getattr(_tls, "runtime", None) or {}

    @classmethod
    def set_active_model(cls, model: str) -> None:
        """Override the active model for this session (backend dropdown picks)."""
        with cls._model_lock:
            cls._active_model_override = (model or "").strip()

    @classmethod
    def get_active_model_override(cls) -> str:
        """Current model override (thread-safe read)."""
        with cls._model_lock:
            return cls._active_model_override

    # ── availability ─────────────────────────────────────────────────────────
    def _service(self):
        if self._svc is None:
            from ai_assistant.llm.service import LlmService

            self._svc = LlmService()
        return self._svc

    def _model_name(self) -> str:
        override = self.get_active_model_override()
        if override:
            return override
        try:
            from ai_query import module_config as mc

            return (mc.get("ai.llm", "active_model", default="default") or "default").strip()
        except Exception:
            return "default"

    def _detect(self) -> bool:
        try:
            st = self._service().status(self._model_name())
        except Exception as exc:  # noqa: BLE001
            self._unavail_reason = f"Local LLM unavailable: {exc}"
            return False
        if st.get("ok") and st.get("trained"):
            return True
        self._unavail_reason = (
            f"No trained model '{self._model_name()}' yet. Train one in the LLM "
            "panel or with `dbtool ai llm train` (optionally --rag-conn to learn "
            "from your saved examples)."
        )
        return False

    def get_info(self) -> dict:
        name = self._model_name()
        engine = ""
        try:
            st = self._service().status(name)
            engine = st.get("engine", "") if st.get("trained") else ""
        except Exception:
            pass
        return {
            "provider": self.display_name,
            "model": f"{name}" + (f" ({engine})" if engine else ""),
            "status": "Connected" if self.is_available() else "Not Available",
            "note": "Runs your locally-trained NL->SQL model — no external calls",
            "resume_supported": bool(self.supports_resume),
        }

    # ── call ───────────────────────────────────────────────────────────────--
    def call(
        self,
        prompt: str,
        timeout: int = 120,
        resume_session_id: Optional[str] = None,
    ) -> dict:
        question = self._extract_question(prompt)
        if not question:
            return {
                "response": None,
                "error": "Local LLM could not identify a question to answer.",
                "backend_session_id": None,
            }
        rt = self._get_runtime()
        try:
            out = self._service().generate(
                question,
                name=self._model_name(),
                connection=rt.get("connection", ""),
                db_type=rt.get("db_type", ""),
                core=rt.get("core"),
                live={
                    "executor": rt.get("executor"),
                    "db_manager": rt.get("db_manager"),
                    "ai_pick_fn": self._ai_pick_fn(rt.get("ai_agent")),
                },
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "response": None,
                "error": f"Local LLM generation failed: {exc}",
                "backend_session_id": None,
            }
        if not out.get("ok") or not out.get("sql"):
            return {
                "response": None,
                "error": out.get("error") or out.get("reason") or "Local LLM produced no SQL.",
                "backend_session_id": None,
            }
        sql = out["sql"]
        valid = bool(out.get("valid"))
        reason = out.get("reason") or ""
        has_live_ctx = bool(rt.get("connection") and rt.get("core"))
        if has_live_ctx and not valid:
            return {
                "response": None,
                "error": (
                    f"Local LLM produced invalid SQL"
                    + (f": {reason}" if reason else "")
                    + ". Retrain with valid SQL or pick a different backend."
                ),
                "backend_session_id": None,
            }
        badge = "valid SQL" if valid else f"invalid SQL ({reason})" if reason else "invalid SQL"
        explanation = (out.get("explanation") or "").strip()
        if not explanation:
            explanation = (
                f"Generated locally by your trained NL->SQL model. "
                f"Validation: {badge}."
            )
        if out.get("resolved") and out.get("mappings"):
            mapping_note = ", ".join(
                f"{k}→{v}" for k, v in (out.get("mappings") or {}).items())
            explanation += (
                f"\n\nPlaceholder resolution ({out.get('resolution', 'deterministic')}): "
                f"{mapping_note}"
            )
        elif out.get("ambiguous"):
            explanation += (
                "\n\nPlaceholder resolution: ambiguous — could not map schema objects "
                "confidently. Refine the question or pick a different table."
            )
        elif out.get("resolution_error"):
            explanation += f"\n\nPlaceholder resolution: {out['resolution_error']}"
        # Emit in a format both the RAG parser and the response parser accept.
        response = (
            "SQL:\n```sql\n"
            f"{sql}\n```\n"
            "SUMMARY_SQL:\n"
            f"{sql}\n\n"
            "EXPLANATION:\n"
            f"{explanation}"
        )
        return {"response": response, "error": None, "backend_session_id": None}

    # ── helpers ────────────────────────────────────────────────────────────--
    @staticmethod
    def _ai_pick_fn(agent) -> Any:
        """Build a fallback-AI disambiguation picker when configured."""
        if agent is None:
            return None
        try:
            fb = agent.get_fallback_backend_value()
        except Exception:
            fb = ""
        if not fb:
            return None

        def _pick(candidates: list, question: str, sql: str):
            import re

            lines = []
            for i, c in enumerate(candidates[:6]):
                lines.append(
                    f"{i}: table={c.get('label')} score={c.get('score', 0):.2f} "
                    f"sql={str(c.get('sql', ''))[:160]}"
                )
            prompt = (
                "Pick the best candidate index (0-based) for resolving placeholder "
                f"SQL against the live schema.\nQUESTION: {question}\n"
                f"TEMPLATE SQL: {sql}\nCANDIDATES:\n" + "\n".join(lines) +
                "\nReply with ONLY the index number."
            )
            try:
                res = agent.call_backend(fb, prompt)
            except Exception:
                return None
            text = (res or {}).get("response") or ""
            m = re.search(r"\b(\d+)\b", text)
            if not m:
                return None
            idx = int(m.group(1))
            if 0 <= idx < len(candidates):
                return candidates[idx]
            return None

        return _pick

    @staticmethod
    def _extract_question(prompt: str) -> str:
        m = _QUESTION_RE.search(prompt or "")
        if m:
            return m.group(1).strip()
        text = (prompt or "").strip()
        return text if len(text) <= 500 else ""
