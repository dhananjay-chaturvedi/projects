"""Runtime SQL validation for generated NL->SQL output.

Parse via sqlglot, then optionally dry-run EXPLAIN / LIMIT 0 against a live
connection using the shared execution-rules helpers.
"""

from __future__ import annotations

import re
from typing import Any

from ai_assistant.llm.validation import normalize_sql, parse_sql
from ai_query.sql_execution_rules import build_explain_sql


def _limit_zero_sql(sql: str, db_type: str) -> str:
    s = normalize_sql(sql)
    key = (db_type or "").lower()
    if re.search(r"\b(LIMIT|TOP|FETCH FIRST|ROWNUM)\b", s, re.I):
        return s
    if "sqlserver" in key or "mssql" in key:
        return re.sub(r"^\s*SELECT\b", "SELECT TOP 0", s, count=1, flags=re.I)
    if "oracle" in key:
        return f"{s} FETCH FIRST 0 ROWS ONLY"
    return f"{s} LIMIT 0"


def _exec_ok(
    core: Any,
    connection: str,
    sql: str,
    *,
    executor: Any = None,
) -> tuple[bool, str]:
    if executor is not None:
        try:
            if callable(executor):
                res = executor(sql)
            else:
                res = executor.execute(connection, sql)
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        if isinstance(res, tuple) and len(res) == 2:
            raw, err = res
            if err:
                return False, str(err)
            return True, ""
        if isinstance(res, dict) and res.get("error"):
            return False, str(res.get("error"))
        return True, ""
    if core is None or not connection:
        return False, "No connection for execution check"
    try:
        res = core.execute(connection, sql)
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    if isinstance(res, dict) and res.get("error"):
        return False, str(res.get("error"))
    return True, ""


def check_sql(
    sql: str,
    *,
    db_type: str = "",
    core: Any = None,
    connection: str = "",
    explain: bool = True,
    limit_zero: bool = True,
    executor: Any = None,
) -> dict[str, Any]:
    """Validate generated SQL: parse + optional EXPLAIN / LIMIT 0 dry-run."""
    from ai_assistant.llm.dataset import normalize_db_type

    if normalize_db_type(db_type) in ("mongodb", "documentdb"):
        s = (sql or "").strip()
        ok = bool(s) and (s.startswith("db.") or s.startswith("{"))
        live_ok = False
        if ok and connection and (core is not None or executor is not None):
            live_ok, _err = _exec_ok(core, connection, s, executor=executor)
        return {
            "sql": s,
            "parse_ok": ok,
            "executable": live_ok if (connection and (core or executor)) else ok,
            "valid": live_ok if (connection and (core or executor)) else ok,
            "error": "" if ok else "Invalid MongoDB query",
            "normalized": s,
        }

    parse_ok, normalized, parse_reason = parse_sql(sql, db_type=db_type or None)
    out: dict[str, Any] = {
        "sql": normalized or normalize_sql(sql),
        "parse_ok": parse_ok,
        "executable": False,
        "valid": parse_ok,
        "error": parse_reason or "",
        "normalized": normalized or normalize_sql(sql),
        "checks": [],
    }
    if not parse_ok:
        out["valid"] = False
        return out

    if core is None or not connection:
        if executor is None:
            out["checks"].append("parse_only")
            return out

    if explain:
        explain_sql = build_explain_sql(normalized, db_type)
        ok, err = _exec_ok(core, connection, explain_sql, executor=executor)
        out["checks"].append("explain")
        if ok:
            out["executable"] = True
        else:
            out["error"] = err or "EXPLAIN failed"
            out["valid"] = False
            return out

    if limit_zero:
        probe = _limit_zero_sql(normalized, db_type)
        ok, err = _exec_ok(core, connection, probe, executor=executor)
        out["checks"].append("limit_zero")
        if ok:
            out["executable"] = True
            out["valid"] = True
        else:
            out["error"] = err or "LIMIT 0 probe failed"
            out["valid"] = False
    elif out.get("executable"):
        out["valid"] = True

    return out
