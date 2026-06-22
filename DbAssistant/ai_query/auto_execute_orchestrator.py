"""
Auto-execute pipeline: optional SQL run + AI refinement loop until satisfied or stopped.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable, Optional

from common.config_loader import console_print
from ai_query import module_config as mc


def _normalize_sql_for_hash(sql: str) -> str:
    text = (sql or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text.rstrip(";")


class AutoExecuteOrchestrator:
    """Coordinates auto SQL execution and AI refinement iterations."""

    def __init__(self, agent, max_iterations: int | None = None):
        self.agent = agent
        self.max_iterations = max_iterations or mc.get_int(
            "ui.ai_query", "auto_loop_max_iterations", default=5
        )
        self._sql_hashes: list[str] = []

    def build_panel_context(
        self,
        *,
        problem_statement: str,
        summary_sql: str,
        explanation: str,
        query_output: str,
        iteration: int,
        sql_mode: str,
    ) -> dict[str, Any]:
        return {
            "problem_statement": problem_statement,
            "summary_sql": summary_sql or "",
            "explanation": explanation or "",
            "query_output": query_output or "",
            "iteration": iteration,
            "sql_mode": sql_mode,
        }

    def run_refine_step(
        self,
        panel_context: dict[str, Any],
        db_manager,
        connection_name: str,
        session_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Ask the AI to evaluate progress and optionally produce the next SUMMARY_SQL."""
        return self.agent.run_auto_refine(
            panel_context["problem_statement"],
            db_manager,
            connection_name,
            session_id=session_id,
            panel_context=panel_context,
        )

    def record_sql(self, sql: str | None) -> bool:
        """Record normalized SQL; return True if this repeats a prior iteration (oscillation)."""
        if not sql:
            return False
        digest = hashlib.sha256(_normalize_sql_for_hash(sql).encode()).hexdigest()[:16]
        if digest in self._sql_hashes:
            return True
        self._sql_hashes.append(digest)
        return False

    def reset_sql_history(self) -> None:
        self._sql_hashes.clear()

    def should_continue(
        self,
        result: dict[str, Any],
        iteration: int,
        cancelled: Callable[[], bool],
    ) -> bool:
        if cancelled():
            return False
        if result.get("satisfied"):
            console_print("[AutoExecute] Problem marked satisfied by AI.")
            return False
        if result.get("error"):
            console_print(f"[AutoExecute] Stopping on error: {result['error']}")
            return False
        sql = result.get("summary_sql") or result.get("sql")
        if self.record_sql(sql):
            console_print("[AutoExecute] Repeated SQL detected — stopping oscillation.")
            return False
        if iteration >= self.max_iterations:
            console_print(f"[AutoExecute] Max iterations ({self.max_iterations}) reached.")
            return False
        return True
