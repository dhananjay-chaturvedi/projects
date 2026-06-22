"""Prompt compaction, deduplication, and escalation helpers for AI Query."""

from __future__ import annotations

import re
from typing import Any

from ai_query import module_config as mc

_IDENTIFIER_RE = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b")

_SQL_START_RE = re.compile(
    r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|EXPLAIN|SHOW|DESCRIBE|DESC|"
    r"CREATE|ALTER|DROP|TRUNCATE|MERGE|CALL|EXEC|EXECUTE|GRANT|REVOKE|"
    r"SET|USE|BEGIN|COMMIT|ROLLBACK|PRAGMA)\b",
    re.IGNORECASE,
)

_MONGO_START_RE = re.compile(
    r"^\s*(\{|\[|db\.|aggregate|find|count|distinct|insert|update|delete|"
    r"remove|mapReduce|getCollection)",
    re.IGNORECASE,
)


def prompt_flags() -> dict[str, Any]:
    """Read reversible prompt-optimization flags (defaults ON)."""
    full_fmt = (mc.get("ai.prompt", "full_format_block", default="auto") or "auto").strip().lower()
    return {
        "compact_schema": mc.get_bool("ai.prompt", "compact_schema", default=True),
        "consolidate_instructions": mc.get_bool(
            "ai.prompt", "consolidate_instructions", default=True
        ),
        "dedup_followup_schema": mc.get_bool(
            "ai.prompt", "dedup_followup_schema", default=True
        ),
        "dedup_crosstab_schema": mc.get_bool(
            "ai.prompt", "dedup_crosstab_schema", default=True
        ),
        "progressive_escalation": mc.get_bool(
            "ai.prompt", "progressive_escalation", default=True
        ),
        "full_format_block": full_fmt,
        "schema_drift_check": mc.get_bool("ai.cache", "schema_drift_check", default=False),
    }


def looks_like_sql(text: str, db_type: str = "") -> bool:
    """True when *text* plausibly starts with executable SQL or a Mongo command."""
    if not text or not text.strip():
        return False
    sample = text.strip()
    # Reject obvious prose (long sentence without SQL punctuation)
    if len(sample.split()) > 12 and not _SQL_START_RE.match(sample):
        first_line = sample.split("\n", 1)[0]
        if not _SQL_START_RE.match(first_line) and ";" not in first_line[:200]:
            return False
    db = (db_type or "").lower()
    if "mongo" in db:
        return bool(_MONGO_START_RE.match(sample))
    if _SQL_START_RE.match(sample):
        return True
    if sample.startswith("(") and "SELECT" in sample.upper():
        return True
    return False


def cache_covers_needs(cached: dict[str, Any], analysis: dict[str, Any]) -> bool:
    """True when cached context includes every section the question needs."""
    cached_analysis = cached.get("_analysis") or {}
    for key in ("needs_relationships", "needs_performance", "needs_analysis", "needs_system"):
        if analysis.get(key) and not cached_analysis.get(key):
            return False
    return True


def extract_referenced_tables(*texts: str, all_tables: list[str] | None = None) -> set[str]:
    """Heuristically find table names mentioned in SQL or conversation text."""
    refs: set[str] = set()
    tables = {t.lower(): t for t in (all_tables or [])}
    blob = "\n".join(t for t in texts if t)
    if not blob:
        return refs
    for match in _IDENTIFIER_RE.findall(blob):
        key = match.lower()
        if key in tables:
            refs.add(tables[key])
    return refs


def schema_safety_rules_block(db_type: str, *, tier: int = 1) -> str:
    """Consolidated schema-safety rules (one block instead of five repeats)."""
    if tier >= 2:
        return f"""SCHEMA RULES (mandatory):
- Use ONLY column/table names listed in the schema below — never guess or invent names.
- Copy names EXACTLY (case-sensitive for {db_type}).
- Before writing SQL, verify each table and column exists in the schema.
- If a requested column is missing, say so clearly — do not fabricate SQL.
- Use relationships (constraints/indexes) for JOINs when needed."""
    return (
        "SCHEMA RULES: Use ONLY exact table/column names from the schema below. "
        f"Case-sensitive for {db_type}. Never guess column names."
    )


def system_instructions_block(*, tier: int = 1) -> str:
    if tier >= 2:
        return """You are an INTELLIGENT DATABASE AGENT with adaptive capabilities.

LANGUAGE SUPPORT:
- Accept questions in ANY language (English, Japanese, etc.)
- Respond in the SAME language as the user's question
- Provide explanations in the user's language

You understand:
- Table schemas, columns, data types, constraints
- Relationships between tables (foreign keys, constraints, indexes)
- Database performance and optimization
- System metadata (users, roles, processes, sessions)
- Database-specific syntax and best practices

Your capabilities adapt to the question:
- Simple queries: Generate clean, accurate SQL
- Complex queries: Use relationships, optimize with indexes
- Performance questions: Analyze processes, sessions, system state
- Analysis questions: Provide insights on structure, data modeling
- Troubleshooting: Diagnose issues using performance metrics

Always:
1. Generate ACCURATE SQL using ONLY columns from the schema
2. Understand relationships and use proper JOINs
3. Consider performance and suggest optimizations
4. Provide context-aware, helpful explanations
5. Adapt your approach based on what the user is asking"""
    return (
        "You are a database SQL assistant. Respond in the user's language. "
        "Generate accurate SQL using ONLY schema objects shown below."
    )


def followup_instructions_block(db_type: str, *, tier: int = 1) -> str:
    rules = schema_safety_rules_block(db_type, tier=tier)
    if tier >= 2:
        return f"""{rules}

The user wants to refine or correct the query. Please:
1. Understand if they're pointing out an error, asking for modifications, or asking a clarification question
2. Update SUMMARY_SQL if needed, or use NO CHANGE in DETAIL_SQL when only clarifying
3. Explain what you changed and why, including performance considerations
4. Suggest follow-up ideas in INSIGHTS

Important:
- If the user mentions an error, fix it in SUMMARY_SQL
- If the user asks for changes (like "add a WHERE clause", "sort by date"), modify SUMMARY_SQL accordingly
- If the user just asks for clarification, explain but keep SUMMARY_SQL unchanged (say NO CHANGE in DETAIL_SQL)
- Use {db_type}-specific syntax and best practices"""
    return (
        f"{rules}\n"
        "Refine the query per the follow-up. Update SUMMARY_SQL when needed; "
        f"use {db_type}-specific syntax."
    )


def _format_compact_columns(columns: list[dict]) -> str:
    parts = []
    for col in columns:
        name = col.get("name", "")
        col_type = col.get("type", "")
        nullable = "" if col.get("nullable", True) else " NOT NULL"
        default = f" DEFAULT {col['default']}" if col.get("default") else ""
        parts.append(f"{name}:{col_type}{nullable}{default}")
    return ", ".join(parts)


def _format_verbose_table(table_name: str, columns: list[dict]) -> str:
    out = f"┏━━ TABLE: {table_name} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    if not columns:
        out += "┃   (No columns or access denied)\n┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        return out
    out += (
        "┃   COLUMN NAME"
        + " " * 15
        + "DATA TYPE"
        + " " * 15
        + "NULL?     DEFAULT\n"
    )
    out += "┃   " + "─" * 70 + "\n"
    max_name_len = max(len(col["name"]) for col in columns) if columns else 20
    max_type_len = max(len(col["type"]) for col in columns) if columns else 20
    for col in columns:
        name = col["name"].ljust(max_name_len)
        col_type = col["type"].ljust(max_type_len)
        nullable = "NULL    " if col["nullable"] else "NOT NULL"
        default = f" | {col['default']}" if col.get("default") else ""
        out += f"┃   {name}  {col_type}  {nullable}{default}\n"
    out += "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    return out


def _format_compact_table(table_name: str, columns: list[dict]) -> str:
    if not columns:
        return f"TABLE {table_name}: (no columns)\n"
    return f"TABLE {table_name}: {_format_compact_columns(columns)}\n"


def format_table_schemas(
    table_schemas: dict[str, list[dict]],
    *,
    compact: bool,
    max_tables: int = 15,
    only_tables: set[str] | None = None,
) -> str:
    """Format detailed table schemas in compact or verbose (legacy) style."""
    if not table_schemas:
        return ""
    lines: list[str] = []
    if compact:
        lines.append("TABLE SCHEMAS (exact column names):\n")
    else:
        lines.append("DETAILED TABLE SCHEMAS - READ CAREFULLY:\n")
        lines.append("IMPORTANT: These are the EXACT, ACTUAL column names from the database.\n")
        lines.append("You MUST use these EXACT names - NO variations, NO guessing!\n")
    shown = 0
    for table_name in sorted(table_schemas):
        if only_tables and table_name not in only_tables:
            continue
        columns = table_schemas[table_name]
        if compact:
            lines.append(_format_compact_table(table_name, columns))
        else:
            lines.append(_format_verbose_table(table_name, columns))
        shown += 1
        if shown >= max_tables:
            break
    if only_tables:
        lines.append(f"(Digest: {shown} referenced table(s) with full columns)\n")
    else:
        lines.append(f"(Showing {shown} tables with full column details)\n\n")
    return "".join(lines)


def build_schema_digest(
    context: dict[str, Any],
    referenced_tables: set[str],
    *,
    compact: bool = True,
) -> str:
    """Compact schema for follow-up turns: table list + referenced table detail."""
    schema = context.get("schema", {})
    all_tables = schema.get("tables", [])
    table_schemas = schema.get("table_schemas", {})
    db_type = context.get("database_type", "SQL")
    lines = [
        f"DB: {db_type} | Tables: {schema.get('table_count', len(all_tables))}\n",
        "SCHEMA DIGEST (full detail only for referenced tables):\n",
    ]
    if all_tables:
        preview = ", ".join(all_tables[:60])
        suffix = f" ... +{len(all_tables) - 60} more" if len(all_tables) > 60 else ""
        lines.append(f"All tables: {preview}{suffix}\n\n")
    detail_tables = referenced_tables & set(table_schemas) if referenced_tables else set()
    if not detail_tables and table_schemas:
        detail_tables = set(list(table_schemas.keys())[:3])
    lines.append(
        format_table_schemas(
            table_schemas,
            compact=compact,
            max_tables=len(detail_tables) or 3,
            only_tables=detail_tables or None,
        )
    )
    return "".join(lines)


def dedupe_peer_bundles(peer_bundles: list[dict]) -> list[dict]:
    """Merge peer bundles that share the same connection (schema sent once)."""
    if not peer_bundles:
        return []
    by_conn: dict[str, dict] = {}
    order: list[str] = []
    for bundle in peer_bundles:
        conn = (bundle.get("connection_name") or "").strip() or f"__tab_{bundle.get('tab_number')}"
        if conn not in by_conn:
            by_conn[conn] = {
                **bundle,
                "tab_numbers": [bundle.get("tab_number")],
            }
            order.append(conn)
        else:
            merged = by_conn[conn]
            merged["tab_numbers"] = list(merged.get("tab_numbers", [])) + [
                bundle.get("tab_number")
            ]
            for key in (
                "current_sql",
                "last_result_summary",
                "last_explanation_text",
                "last_query_output_text",
            ):
                if bundle.get(key) and not merged.get(key):
                    merged[key] = bundle[key]
            if bundle.get("conversation_excerpt"):
                merged.setdefault("conversation_excerpt", []).extend(
                    bundle["conversation_excerpt"]
                )
    return [by_conn[c] for c in order]


def merge_cross_tab_parts(
    local_context_text: str,
    peer_bundles: list[dict],
    user_message: str,
    *,
    mask_fn,
    dedup: bool,
) -> str:
    """Build cross-tab prompt text with optional per-connection schema dedup."""
    parts = [local_context_text]
    bundles = dedupe_peer_bundles(peer_bundles) if dedup else peer_bundles
    if bundles:
        parts.append("\n=== CROSS-TAB CONTEXT FROM OTHER SESSIONS ===\n")
        for b in bundles:
            tabs = b.get("tab_numbers") or [b.get("tab_number")]
            tab_label = ", ".join(str(t) for t in tabs if t is not None)
            parts.append(
                f"--- Tab(s) {tab_label} ({b.get('connection_name') or 'no connection'}) ---\n"
                f"DB type: {b.get('db_type') or 'unknown'}\n"
            )
            if b.get("current_sql"):
                parts.append(f"Current SQL:\n{b['current_sql']}\n")
            if b.get("last_result_summary"):
                parts.append(f"Last result summary:\n{b['last_result_summary']}\n")
            if b.get("last_explanation_text"):
                parts.append(f"Last explanation:\n{b['last_explanation_text'][:2000]}\n")
            if b.get("last_query_output_text"):
                parts.append(f"Last query output:\n{b['last_query_output_text'][:2000]}\n")
            if b.get("schema_context"):
                parts.append(f"{b['schema_context']}\n")
            excerpt = b.get("conversation_excerpt") or []
            if excerpt:
                parts.append("Recent conversation:\n")
                for msg in excerpt[-4:]:
                    role = msg.get("role", "user")
                    content = mask_fn(str(msg.get("content", "")))[:500]
                    parts.append(f"{role}: {content}\n")
    parts.append(f"\nUSER QUESTION: {mask_fn(user_message)}\n")
    return "\n".join(parts)


def should_use_full_format_block(
    *,
    sql_mode: str,
    complexity: int,
    is_simple: bool,
    auto_refine: bool = False,
) -> bool:
    """Decide whether to include DETAIL_SQL/INSIGHTS sections in the format block."""
    flags = prompt_flags()
    mode = (flags.get("full_format_block") or "auto").lower()
    if mode == "always":
        return True
    if mode == "never":
        return False
    if auto_refine:
        return True
    if complexity >= 2 or not is_simple:
        return True
    return sql_mode not in ("summary", "strict_summary")


def needs_escalation(
    parsed: dict[str, Any],
    sql: str | None,
    validation_warnings: list[str],
) -> bool:
    """True when Tier-1 response should be retried with a more detailed prompt."""
    if parsed.get("is_clarification"):
        return False
    if parsed.get("error"):
        return True
    if not sql:
        return True
    if validation_warnings:
        return True
    return False
