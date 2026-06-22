"""
SQL mode constants and normalization for the AI Query Assistant.

Modes:
  strict_summary — metadata/catalog views only (enforced in code)
  summary        — catalog-first; user tables when needed for the answer
  open           — no scope restrictions; optimized SQL for the real problem
"""

from __future__ import annotations

VALID_SQL_MODES = frozenset({"strict_summary", "summary", "open"})

_MODE_LABELS = {
    "strict_summary": "Strict summary",
    "summary": "Summary",
    "open": "Open",
}


def normalize_sql_mode(mode: str | None, *, legacy: bool = False) -> str:
    """Return a canonical sql_mode value."""
    raw = (mode or "summary").lower().replace("-", "_").strip()
    if raw in ("strict", "strict_summary", "metadata"):
        return "strict_summary"
    if raw == "open":
        return "open"
    if raw == "summary":
        return "summary"
    return "summary"


def migrate_stored_sql_mode(stored: str | None, *, sql_modes_v2: bool = False) -> str:
    """Map persisted session sql_mode to current semantics."""
    if not stored:
        return "summary"
    raw = stored.lower().strip()
    if sql_modes_v2:
        return normalize_sql_mode(stored)
    if raw == "open":
        return "summary"
    if raw == "summary":
        return "strict_summary"
    return normalize_sql_mode(stored)


def sql_mode_label(mode: str | None) -> str:
    return _MODE_LABELS.get(normalize_sql_mode(mode), "Summary")


def is_strict_summary(mode: str | None) -> bool:
    return normalize_sql_mode(mode) == "strict_summary"


def execution_rules_apply(mode: str | None) -> bool:
    """User-defined SQL execution rules apply in summary and open modes."""
    return normalize_sql_mode(mode) in ("summary", "open")


def performance_rules_block() -> str:
    return """
PERFORMANCE RULES (all SQL modes):
- Emit exactly ONE statement in SUMMARY_SQL.
- Filter early; avoid full table scans on large user tables when avoidable.
- Use LIMIT/TOP/FETCH/ROWNUM caps for exploratory or large result sets.
- Prefer aggregates (COUNT, SUM) over returning all rows for summaries.
- Use indexed/join-key columns in WHERE and JOIN predicates when possible.
"""
