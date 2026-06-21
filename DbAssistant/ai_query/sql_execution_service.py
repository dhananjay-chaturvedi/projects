"""
Shared SQL execution with mode-aware rules (CLI, API, UI).
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from ai_query.sql_execution_rules import (
    evaluate_execution_rules,
    format_explain_output,
)
from ai_query.sql_modes import execution_rules_apply, normalize_sql_mode


def default_execution_rules_from_config() -> str:
    default = (
        "Always use LIMIT clause for SELECT on user tables\n"
        "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
    )
    from ai_query import module_config as mc
    return mc.get("ui.ai_query", "sql_execution_rules", default="").strip() or default


def user_table_names(agent, db_manager, connection_name: str) -> list[str]:
    try:
        ctx = agent.get_cached_comprehensive_context(
            db_manager, connection_name, "schema"
        )
        return agent._user_table_names_from_context(ctx)
    except Exception:
        return []


def check_execution_allowed(
    sql: str,
    *,
    sql_mode: str,
    rules_text: str,
    db_manager,
    agent,
    connection_name: str,
) -> dict[str, Any]:
    """
    Return {allowed, blocked_reason, explain_sql, explain_note}.
    """
    if not execution_rules_apply(sql_mode):
        return {"allowed": True, "blocked_reason": "", "explain_sql": "", "explain_note": ""}

    rules = (rules_text or "").strip()
    if not rules:
        return {"allowed": True, "blocked_reason": "", "explain_sql": "", "explain_note": ""}

    check = evaluate_execution_rules(
        sql,
        rules,
        user_table_names=user_table_names(agent, db_manager, connection_name),
        db_type=getattr(db_manager, "db_type", ""),
    )
    return {
        "allowed": check.allowed,
        "blocked_reason": check.blocked_reason or "",
        "explain_sql": check.explain_sql or "",
        "explain_note": check.explain_note or "",
    }


def execute_sql_after_gate(
    sql: str,
    db_manager,
    gate: dict[str, Any],
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> dict[str, Any]:
    """
    Run EXPLAIN (when *gate* requires it) then *sql*.

    *gate* is the dict returned by :func:`check_execution_allowed`.
    """
    # Hard read-only guard: the AI Query Assistant must NEVER run mutating SQL
    # (DROP/DELETE/UPDATE/INSERT/...) against a live connection. This is enforced
    # here — the single chokepoint every AI execution path funnels through —
    # regardless of sql_mode or user-configured execution rules.
    from common.sql_guard import inspect_read_only

    ok, reason, _offending = inspect_read_only(
        sql, db_type=getattr(db_manager, "db_type", "") or ""
    )
    if not ok:
        return {
            "error": reason,
            "blocked": True,
            "allowed": False,
            "cancelled": False,
            "explain_output": "",
            "explain_note": gate.get("explain_note") or "",
            "result": None,
        }

    if cancel_check and cancel_check():
        return {
            "error": "",
            "blocked": False,
            "allowed": True,
            "cancelled": True,
            "explain_output": "",
            "explain_note": gate.get("explain_note") or "",
            "result": None,
        }

    explain_output = ""
    if gate.get("explain_sql"):
        exp, exp_err = db_manager.execute_query(gate["explain_sql"])
        explain_output = format_explain_output(exp, exp_err)
        if exp_err:
            return {
                "error": "EXPLAIN failed; SQL was not executed.\n" + explain_output,
                "blocked": True,
                "allowed": False,
                "cancelled": False,
                "explain_output": explain_output,
                "explain_note": gate.get("explain_note") or "",
                "result": None,
            }
        if cancel_check and cancel_check():
            return {
                "error": "",
                "blocked": False,
                "allowed": True,
                "cancelled": True,
                "explain_output": explain_output,
                "explain_note": gate.get("explain_note") or "",
                "result": None,
            }

    result, error = db_manager.execute_query(sql)
    if cancel_check and cancel_check():
        return {
            "error": error or "",
            "blocked": False,
            "allowed": True,
            "cancelled": True,
            "explain_output": explain_output,
            "explain_note": gate.get("explain_note") or "",
            "result": result,
        }

    return {
        "error": error,
        "blocked": False,
        "allowed": True,
        "cancelled": False,
        "explain_output": explain_output,
        "explain_note": gate.get("explain_note") or "",
        "result": result,
    }


def execute_sql_with_rules(
    sql: str,
    db_manager,
    *,
    sql_mode: str = "summary",
    rules_text: str = "",
    agent=None,
    connection_name: str = "",
    cancel_check: Optional[Callable[[], bool]] = None,
) -> dict[str, Any]:
    """
    Run pre-execution checks then optionally EXPLAIN + query.

    Returns {error, blocked, explain_output, result, allowed, cancelled}.
    """
    gate = check_execution_allowed(
        sql,
        sql_mode=normalize_sql_mode(sql_mode),
        rules_text=rules_text,
        db_manager=db_manager,
        agent=agent,
        connection_name=connection_name,
    )
    if not gate["allowed"]:
        return {
            "error": gate["blocked_reason"],
            "blocked": True,
            "allowed": False,
            "cancelled": False,
            "explain_output": "",
            "result": None,
        }

    out = execute_sql_after_gate(
        sql, db_manager, gate, cancel_check=cancel_check
    )
    if out.get("cancelled"):
        return out
    if out.get("blocked"):
        return {
            "error": out.get("error"),
            "blocked": True,
            "allowed": False,
            "cancelled": False,
            "explain_output": out.get("explain_output") or "",
            "explain_note": out.get("explain_note") or "",
            "result": None,
        }
    if out.get("error"):
        return {
            "error": out["error"],
            "blocked": False,
            "allowed": True,
            "cancelled": False,
            "explain_output": out.get("explain_output") or "",
            "explain_note": out.get("explain_note") or "",
            "result": None,
        }
    return {
        "error": None,
        "blocked": False,
        "allowed": True,
        "cancelled": False,
        "explain_output": out.get("explain_output") or "",
        "explain_note": out.get("explain_note") or "",
        "result": out.get("result"),
    }
