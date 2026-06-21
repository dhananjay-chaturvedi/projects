"""
Parse structured AI Query Assistant responses (summary SQL pipeline).

Expected model sections (in order):
  CONTEXT, SUMMARY_SQL, EXPLANATION, DETAIL_SQL, INSIGHTS

Legacy ``SQL:`` / ``EXPLANATION:`` responses are still supported.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from ai_query.sql_modes import normalize_sql_mode, performance_rules_block


_SECTION_NAMES = (
    "CONTEXT",
    "SUMMARY_SQL",
    "SQL",
    "EXPLANATION",
    "DETAIL_SQL",
    "INSIGHTS",
    "SATISFIED",
)


def _clean_sql_block(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    sql = text.strip()
    if sql.upper() == "NO CHANGE":
        return None
    sql = re.sub(r"^```sql\s*\n", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"^```\s*\n", "", sql)
    sql = re.sub(r"^~~~\s*\n", "", sql)
    sql = re.sub(r"\n```$", "", sql)
    sql = re.sub(r"\n~~~$", "", sql)
    return sql.strip() or None


def _split_sections(response: str) -> dict[str, str]:
    """Split *response* into named sections by header labels."""
    if not response:
        return {}
    pattern = re.compile(
        r"(?mi)^\s*(" + "|".join(_SECTION_NAMES) + r")\s*:\s*\n",
    )
    matches = list(pattern.finditer(response))
    if not matches:
        return {}
    out: dict[str, str] = {}
    for i, m in enumerate(matches):
        name = m.group(1).upper()
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(response)
        body = response[start:end].strip()
        if name == "SQL" and "SUMMARY_SQL" not in out:
            out["SUMMARY_SQL"] = body
        elif name not in out:
            out[name] = body
    return out


def parse_structured_ai_response(response: str) -> dict[str, Any]:
    """
    Parse an AI response into summary/detail SQL, explanation, and insights.

    Returns dict with keys: summary_sql, explanation, detail_sql, insights, context,
    is_clarification, raw_sections.
    """
    sections = _split_sections(response)
    summary_raw = sections.get("SUMMARY_SQL") or sections.get("SQL")
    explanation = sections.get("EXPLANATION")
    detail_raw = sections.get("DETAIL_SQL")
    insights = sections.get("INSIGHTS")
    context = sections.get("CONTEXT")
    satisfied_raw = sections.get("SATISFIED", "")

    # Legacy fallback when no structured headers matched
    if not sections:
        sql_match = re.search(
            r"SQL:\s*\n(.+?)(?=\n\s*EXPLANATION:|\Z)",
            response,
            re.DOTALL | re.IGNORECASE,
        )
        explanation_match = re.search(
            r"EXPLANATION:\s*\n(.+)", response, re.DOTALL | re.IGNORECASE
        )
        # Do NOT use the raw full response as SQL — that risks injecting prose
        # into the database. If no SQL header matched, leave summary_raw as None.
        summary_raw = sql_match.group(1).strip() if sql_match else None
        explanation = (
            explanation_match.group(1).strip() if explanation_match else response.strip() or None
        )

    summary_sql = _clean_sql_block(summary_raw)
    detail_sql = _clean_sql_block(detail_raw)

    combined = (summary_raw or "") + (explanation or "")
    is_clarification = "NO CHANGE" in combined.upper() and not summary_sql
    satisfied = bool(re.search(r"\byes\b", (satisfied_raw or ""), re.IGNORECASE))

    return {
        "summary_sql": summary_sql,
        "explanation": (explanation or "").strip() or None,
        "detail_sql": detail_sql,
        "insights": (insights or "").strip() or None,
        "context": (context or "").strip() or None,
        "is_clarification": is_clarification,
        "satisfied": satisfied,
        "raw_sections": sections,
    }


def build_agent_result(
    parsed: dict[str, Any],
    *,
    error: Optional[str] = None,
    keep_sql: Optional[str] = None,
) -> dict[str, Any]:
    """Normalize parser output to the agent/UI result contract."""
    is_clarification = bool(parsed.get("is_clarification"))
    summary_sql = parsed.get("summary_sql")
    if is_clarification and keep_sql:
        summary_sql = keep_sql

    explanation = parsed.get("explanation") or ""
    detail_sql = parsed.get("detail_sql")
    insights = parsed.get("insights")
    context = parsed.get("context")

    if context:
        explanation = f"{context}\n\n{explanation}".strip()

    if detail_sql:
        explanation = (
            f"{explanation}\n\n--- Detail SQL (reference only) ---\n{detail_sql}"
        ).strip()

    if insights:
        explanation = (
            f"{explanation}\n\n--- Insights & follow-up ideas ---\n{insights}"
        ).strip()

    return {
        "sql": summary_sql,
        "summary_sql": summary_sql,
        "explanation": explanation or None,
        "detail_sql": detail_sql,
        "insights": insights,
        "context": context,
        "error": error,
        "is_clarification": is_clarification,
        "satisfied": bool(parsed.get("satisfied")),
    }


def catalog_view_guidance(db_type: str) -> str:
    """Database-specific hints for SUMMARY_SQL using catalog/system views."""
    key = (db_type or "").lower()
    guides = {
        "mysql": (
            "Prefer information_schema (TABLES, COLUMNS, STATISTICS, KEY_COLUMN_USAGE) "
            "and performance_schema for counts/metadata. Use sys schema when available."
        ),
        "mariadb": (
            "Prefer information_schema and performance_schema for metadata summaries."
        ),
        "postgresql": (
            "Prefer pg_catalog (pg_tables, pg_class, pg_attribute, pg_indexes) and "
            "information_schema for object counts and structure validation."
        ),
        "oracle": (
            "Prefer ALL_/USER_/DBA_ views (ALL_TABLES, ALL_TAB_COLUMNS, ALL_CONSTRAINTS) "
            "and V$ views for session/performance context where relevant."
        ),
        "sqlserver": (
            "Prefer sys.tables, sys.columns, sys.indexes, sys.foreign_keys, and "
            "INFORMATION_SCHEMA views for metadata summaries."
        ),
        "sqlite": (
            "Prefer sqlite_master and pragma_table_info() for schema validation summaries."
        ),
    }
    for name, text in guides.items():
        if name in key:
            return text
    return (
        "Prefer catalog/system/metadata views native to this engine when summarizing "
        "schema or validating answers; otherwise use verified user tables with aggregates."
    )


def response_format_instructions(
    db_type: str,
    connection_name: str = "",
    tab_number: Optional[int] = None,
    sql_mode: str = "summary",
    *,
    auto_refine: bool = False,
    execution_rules: str = "",
    full_format: bool | None = None,
    complexity: int = 0,
    is_simple: bool = False,
) -> str:
    """Prompt block requiring structured sections for the summary SQL pipeline."""
    tab_line = f"Tab {tab_number}" if tab_number else "this session"
    catalog = catalog_view_guidance(db_type)
    mode = normalize_sql_mode(sql_mode)

    if full_format is None:
        from ai_query.prompt_assembly import should_use_full_format_block

        full_format = should_use_full_format_block(
            sql_mode=mode,
            complexity=complexity,
            is_simple=is_simple,
            auto_refine=auto_refine,
        )

    if mode == "strict_summary":
        summary_rules = f"""SUMMARY_SQL:
[Exactly ONE fast, efficient executable query for the Generated SQL panel.
 STRICT: use ONLY catalog/system/metadata views — NEVER user-schema tables.
 Always process via metadata; prefer aggregates and narrow filters on catalog views.
 {catalog}
 Do NOT put multiple statements here.]"""
    elif mode == "summary":
        summary_rules = f"""SUMMARY_SQL:
[Exactly ONE efficient executable query for the Generated SQL panel.
 For metadata/structure questions, prefer fast catalog/system views: {catalog}
 For data/business questions, use verified user-schema tables when required.
 Do NOT put multiple statements here.]"""
    else:
        summary_rules = """SUMMARY_SQL:
[Exactly ONE optimized executable query that directly answers the user's problem.
 Use any valid tables/views/columns from the schema — no artificial catalog-only limit.
 Prefer efficient patterns: selective filters, limits on large scans, indexed join keys.
 Do NOT put multiple statements here.]"""

    perf = performance_rules_block()

    execution_block = ""
    if execution_rules and mode in ("summary", "open"):
        execution_block = f"""
USER SQL EXECUTION RULES (honor when generating SUMMARY_SQL):
{execution_rules.strip()}
"""

    satisfied_block = ""
    if auto_refine:
        satisfied_block = """
SATISFIED:
[yes or no — yes only if the original problem is fully answered for this connection/schema]
"""

    detail_block = ""
    insights_block = ""
    if full_format:
        detail_block = """
DETAIL_SQL:
[Optional — additional example/alternative queries for the Explanation panel only.
 These are NOT auto-executed. Use NO CHANGE if none.]
"""
        insights_block = """
INSIGHTS:
[Bulleted follow-up questions, deeper analysis ideas, and next steps for the user.]
"""

    return f"""
RESPONSE FORMAT — use these exact section headers in order:

CONTEXT:
- Connection: {connection_name or '(selected connection)'}
- Working tab: {tab_line}
- Database type: {db_type}
- SQL mode: {mode}
- Schema objects you reference (tables/columns/views)

{summary_rules}

EXPLANATION:
[Narrative tied to this schema and connection — what the summary shows and why.]
{detail_block}{insights_block}{satisfied_block}
{perf}
{execution_block}
Rules:
- SUMMARY_SQL must use ONLY columns/tables valid for the active SQL mode ({mode}).
- Always anchor answers to the connection and schema above.
- Respond in the user's language.
"""
