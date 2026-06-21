"""
Pre-execution checks driven by user-written SQL execution rules (summary/open modes).

Rules are free text (one per line). Built-in enforcement activates when a line
mentions keywords such as ``limit`` or ``explain`` + ``join``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable

from ai_query.sql_table_refs import extract_table_refs, is_system_reference


@dataclass
class ExecutionRulesResult:
    allowed: bool = True
    blocked_reason: str = ""
    warnings: list[str] = field(default_factory=list)
    run_explain_first: bool = False
    explain_sql: str = ""
    explain_note: str = ""


def _count_joins(sql: str) -> int:
    if not sql:
        return 0
    return len(re.findall(r"(?i)\bjoin\b", _scan_text(sql)))


def _is_select(sql: str) -> bool:
    return bool(re.match(r"(?i)\s*select\b", (sql or "").strip()))


def _has_limit_clause(sql: str) -> bool:
    if not sql:
        return False
    scan = _scan_text(sql)
    patterns = (
        r"(?i)\blimit\s+\d+",
        r"(?i)\bfetch\s+(first|next)\s+\d+",
        r"(?i)\btop\s+\d+",
        r"(?i)\brownum\s*<=?\s*\d+",
        r"(?i)\brownum\s*<\s*\d+",
    )
    return any(re.search(p, scan) for p in patterns)


def _scan_text(sql: str) -> str:
    """Return SQL with comments and string literal contents blanked out.

    Rule checks must not be bypassed by text like ``'limit 10'`` or
    ``-- join``. We preserve quote/comment delimiters as whitespace so token
    boundaries remain stable.
    """
    out = []
    i = 0
    in_squote = False
    in_dquote = False
    in_line_comment = False
    in_block_comment = False
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append("\n")
            else:
                out.append(" ")
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                out.extend("  ")
                i += 2
            else:
                out.append(" ")
                i += 1
            continue

        if not (in_squote or in_dquote) and ch == "-" and nxt == "-":
            in_line_comment = True
            out.extend("  ")
            i += 2
            continue
        if not (in_squote or in_dquote) and ch == "#":
            in_line_comment = True
            out.append(" ")
            i += 1
            continue
        if not (in_squote or in_dquote) and ch == "/" and nxt == "*":
            in_block_comment = True
            out.extend("  ")
            i += 2
            continue

        if ch == "'" and not in_dquote:
            if in_squote and nxt == "'":
                out.extend("  ")
                i += 2
                continue
            in_squote = not in_squote
            out.append(" ")
            i += 1
            continue
        if ch == '"' and not in_squote:
            if in_dquote and nxt == '"':
                out.extend("  ")
                i += 2
                continue
            in_dquote = not in_dquote
            out.append(" ")
            i += 1
            continue

        if in_squote or in_dquote:
            out.append(" ")
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def _user_table_refs(
    sql: str, user_table_names: Iterable[str], db_type: str
) -> set[str]:
    user_tables = {t.lower() for t in user_table_names if t}
    refs = extract_table_refs(sql)
    hits: set[str] = set()
    for ref in refs:
        base = ref.split(".")[-1].lower()
        if base in user_tables or ref.lower() in user_tables:
            hits.add(ref)
            continue
        if not is_system_reference(ref, db_type):
            # Unknown non-system ref — treat as user table for safety
            hits.add(ref)
    return hits


def _parse_rule_flags(rules_text: str) -> dict[str, bool]:
    lines = [ln.strip() for ln in (rules_text or "").splitlines() if ln.strip()]
    flags = {
        "require_limit_user_tables": False,
        "explain_before_multi_join": False,
    }
    join_threshold = 2
    for line in lines:
        low = line.lower()
        if "limit" in low and ("user" in low or "table" in low or "select" in low):
            flags["require_limit_user_tables"] = True
        if "explain" in low and "join" in low:
            flags["explain_before_multi_join"] = True
        m = re.search(r"(\d+)\s*\+?\s*join", low)
        if m:
            join_threshold = max(2, int(m.group(1)))
    flags["join_threshold"] = join_threshold
    return flags


def build_explain_sql(sql: str, db_type: str) -> str:
    """Build a dialect-specific EXPLAIN for *sql*."""
    stripped = (sql or "").strip().rstrip(";")
    key = (db_type or "").lower()
    if "postgres" in key:
        return f"EXPLAIN {stripped}"
    if "oracle" in key:
        return f"EXPLAIN PLAN FOR {stripped}"
    if "sqlserver" in key or "mssql" in key:
        return f"SET SHOWPLAN_ALL ON;\n{stripped};\nSET SHOWPLAN_ALL OFF;"
    # mysql, mariadb, sqlite, default
    return f"EXPLAIN {stripped}"


def evaluate_execution_rules(
    sql: str,
    rules_text: str,
    *,
    user_table_names: Iterable[str] | None = None,
    db_type: str = "",
) -> ExecutionRulesResult:
    """
    Evaluate *sql* against *rules_text*.

    Returns blocked result when a hard rule fails; may set ``run_explain_first``
    when explain-before-join rule applies.
    """
    result = ExecutionRulesResult()
    if not (rules_text or "").strip() or not (sql or "").strip():
        return result

    flags = _parse_rule_flags(rules_text)
    user_tables = list(user_table_names or [])
    user_refs = _user_table_refs(sql, user_tables, db_type)

    if flags["require_limit_user_tables"] and _is_select(sql) and user_refs:
        if not _has_limit_clause(sql):
            tables = ", ".join(sorted(user_refs))
            result.allowed = False
            result.blocked_reason = (
                "SQL execution rules: SELECT on user table(s) "
                f"({tables}) requires a LIMIT/TOP/FETCH/ROWNUM clause."
            )
            return result

    if flags["explain_before_multi_join"] and _is_select(sql):
        joins = _count_joins(sql)
        threshold = int(flags.get("join_threshold", 2))
        if joins >= threshold:
            result.run_explain_first = True
            result.explain_sql = build_explain_sql(sql, db_type)
            result.explain_note = (
                f"SQL execution rules: running EXPLAIN before execute "
                f"({joins} JOIN(s) detected, threshold {threshold})."
            )

    return result


_EXPLAIN_MAX_ROWS = 50


def format_explain_output(raw_result: dict | None, error: str | None = None) -> str:
    if error:
        return f"EXPLAIN failed:\n{error}\n"
    if not raw_result:
        return "EXPLAIN returned no result.\n"
    if raw_result.get("message"):
        return f"EXPLAIN:\n{raw_result['message']}\n"
    cols = raw_result.get("columns") or []
    rows = raw_result.get("rows") or []
    lines = ["EXPLAIN plan:", " | ".join(str(c) for c in cols)]
    lines.append("-" * 60)
    for row in rows[:_EXPLAIN_MAX_ROWS]:
        lines.append(" | ".join(str(c) for c in row))
    if len(rows) > _EXPLAIN_MAX_ROWS:
        lines.append(
            f"... ({len(rows) - _EXPLAIN_MAX_ROWS} more rows truncated)")
    return "\n".join(lines) + "\n"
