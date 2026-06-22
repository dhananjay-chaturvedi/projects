"""AI bridges used by the App Builder.

There are two deliberately separate channels:

* :class:`DirectChatBridge` is used for ``from_scratch`` app generation. It
  calls the selected chat/backend agent directly and does not attach database
  scope or use AI Query Assistant conversations.
* :class:`AiQueryBridge` is used where database scope is intentional. It sets
  the active connection/db manager before calling the same backend, and pairs
  with :class:`DbUnderstandingClient` for AI Query Assistant data understanding.
"""

from __future__ import annotations

from typing import Any, Optional


from ai_assistant.app_builder.pii_util import mask_if_enabled


class DirectChatBridge:
    """Direct code-agent bridge for from-scratch app generation.

    This uses the existing selected chat/backend agent (`Cursor Agent`, Claude,
    Codex, etc.) through its raw ``_call_ai`` method. It intentionally does not
    set ``_current_connection_name``/``_current_db_manager`` and does not call
    ``start_new_conversation``; from-scratch builds must not involve the AI Query
    Assistant/database channel.
    """

    def __init__(self, agent: Any, *, timeout: int = 180, mask_pii: bool = False) -> None:
        self._agent = agent
        self._timeout = int(timeout)
        self._mask_pii = bool(mask_pii)

    def available(self) -> bool:
        """True when the selected direct backend can be called."""
        try:
            backend = getattr(self._agent, "_active_backend", None)
            if backend is not None:
                return bool(backend.is_available())
            if hasattr(self._agent, "is_available"):
                return bool(self._agent.is_available())
            return False
        except Exception:
            return False

    def generate(self, prompt: str) -> str:
        """Call the selected backend directly and return its text response."""
        prompt = mask_if_enabled(prompt, self._mask_pii)
        if hasattr(self._agent, "_call_ai"):
            result = self._agent._call_ai(prompt, timeout=self._timeout)
        else:
            backend = getattr(self._agent, "_active_backend", None)
            if backend is None:
                raise RuntimeError("No direct AI backend available.")
            result = backend.call(prompt, timeout=self._timeout)
        if isinstance(result, dict):
            if result.get("error"):
                raise RuntimeError(str(result["error"]))
            return result.get("response") or ""
        return str(result or "")


class AiQueryBridge:
    """Send framed prompts with DB scope where a database channel is intended."""

    def __init__(
        self,
        agent: Any = None,
        *,
        connection_name: str = "",
        db_manager: Any = None,
        timeout: int = 180,
        mask_pii: bool = False,
    ) -> None:
        self._agent = agent
        self._connection_name = connection_name
        self._db_manager = db_manager
        self._timeout = timeout
        self._mask_pii = bool(mask_pii)

    def _ensure_agent(self) -> Any:
        if self._agent is None:
            from ai_query.agent import AIQueryAgent

            self._agent = AIQueryAgent()
        return self._agent

    def available(self) -> bool:
        """True when the AI Query Assistant has a usable backend."""
        try:
            agent = self._ensure_agent()
            backend = getattr(agent, "_active_backend", None)
            return bool(backend and backend.is_available())
        except Exception:
            return False

    def generate(self, prompt: str) -> str:
        """Relay *prompt* through the AI Query Assistant and return the text.

        Scope (connection / db manager) is set so the local RAG backend stays
        isolated per database; it is a no-op for external CLI backends.
        """
        agent = self._ensure_agent()
        try:
            agent._current_connection_name = self._connection_name or ""
            agent._current_db_manager = self._db_manager
        except Exception:
            pass
        prompt = mask_if_enabled(prompt, self._mask_pii)
        result = agent._call_ai(prompt, timeout=self._timeout)
        if isinstance(result, dict):
            if result.get("error"):
                raise RuntimeError(str(result["error"]))
            return result.get("response") or ""
        return str(result or "")


def make_bridge(
    *,
    agent: Any = None,
    connection_name: str = "",
    db_manager: Any = None,
    timeout: int = 180,
    mask_pii: bool = False,
) -> Optional[AiQueryBridge]:
    """Construct a bridge; returns ``None`` if no agent can be created."""
    try:
        return AiQueryBridge(
            agent=agent, connection_name=connection_name,
            db_manager=db_manager, timeout=timeout, mask_pii=mask_pii,
        )
    except Exception:
        return None
