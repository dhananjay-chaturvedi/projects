"""
Validate SUMMARY_SQL in strict Summary mode — catalog/system views only.
"""

from __future__ import annotations

from typing import Iterable

from ai_query.sql_table_refs import extract_table_refs, is_system_reference


def validate_summary_mode_sql(
    sql: str,
    db_type: str,
    user_table_names: Iterable[str] | None = None,
) -> list[str]:
    """
    Return validation errors when *sql* is not summary/catalog oriented.
    In Summary mode user-schema tables are rejected.
    """
    if not sql or not sql.strip():
        return []
    user_tables = {t.lower() for t in (user_table_names or []) if t}
    violations: list[str] = []
    refs = extract_table_refs(sql)
    for ref in refs:
        base = ref.split(".")[-1].lower()
        if base in user_tables or ref.lower() in user_tables:
            violations.append(
                f"Summary mode: user table '{ref}' is not allowed — use catalog/system views only."
            )
            continue
        if not is_system_reference(ref, db_type):
            violations.append(
                f"Summary mode: '{ref}' does not appear to be a catalog/system view."
            )
    return violations
