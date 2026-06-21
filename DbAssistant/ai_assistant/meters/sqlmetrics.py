"""Deterministic SQL analysis helpers used by the response-side meters.

Pure parsing/regex — no database connection and no model calls. Used to decide
whether generated SQL parses, which tables/identifiers it references, and how
well those line up with the question and the real schema.
"""

from __future__ import annotations

import re

_SQL_KEYWORDS = frozenset(
    """
    select from where group by order having limit offset join inner left right
    full outer cross on as and or not in is null like between exists union all
    insert into values update set delete create table view index drop alter add
    distinct count sum avg min max case when then else end asc desc primary key
    foreign references default with using natural fetch top rownum over partition
    """.split()
)

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_TABLE_RE = re.compile(
    rf"\b(?:from|join|into|update)\s+((?:{_IDENT}\.)?{_IDENT})", re.IGNORECASE
)
_WORD_RE = re.compile(_IDENT)


_SQL_VERBS = {
    "select", "insert", "update", "delete", "create", "drop",
    "alter", "with", "explain", "truncate", "replace", "merge",
}


def parses(sql: str) -> bool:
    """True when *sql* looks like a real SQL statement.

    ``sqlparse`` is a non-validating tokenizer (it won't reject malformed SQL),
    so the strongest deterministic signal available without executing is that
    the leading verb is a recognized SQL command. Full validity is confirmed
    separately via execution feedback.
    """
    if not sql or not sql.strip():
        return False
    try:
        import sqlparse

        for stmt in sqlparse.parse(sql):
            if not str(stmt).strip():
                continue
            stype = stmt.get_type()
            if stype and stype != "UNKNOWN":
                return True
        return False
    except Exception:
        head = sql.strip().split(None, 1)[0].lower()
        return head in _SQL_VERBS


def statement_count(sql: str) -> int:
    if not sql or not sql.strip():
        return 0
    try:
        import sqlparse

        return len([s for s in sqlparse.parse(sql) if str(s).strip()])
    except Exception:
        return len([p for p in sql.split(";") if p.strip()])


def referenced_tables(sql: str) -> set[str]:
    """Best-effort set of table names referenced (unqualified, lower-cased)."""
    out: set[str] = set()
    for m in _TABLE_RE.findall(sql or ""):
        name = m.split(".")[-1].strip().strip('`"[]')
        if name and name.lower() not in _SQL_KEYWORDS:
            out.add(name.lower())
    return out


def referenced_identifiers(sql: str) -> set[str]:
    """All identifier-like tokens in *sql* minus SQL keywords (lower-cased)."""
    words = {w.lower() for w in _WORD_RE.findall(sql or "")}
    return {w for w in words if w not in _SQL_KEYWORDS and not w.isdigit()}


def has_limit(sql: str) -> bool:
    return bool(re.search(r"\b(limit|top|fetch|rownum)\b", sql or "", re.IGNORECASE))


def schema_identifier_set(schema: dict[str, list[str]]) -> set[str]:
    """Flatten ``{table: [cols]}`` into a lower-cased identifier set."""
    out: set[str] = set()
    for table, cols in (schema or {}).items():
        out.add(str(table).split(".")[-1].lower())
        for c in cols or []:
            out.add(str(c).lower())
    return out


def unknown_tables(sql: str, schema: dict[str, list[str]]) -> set[str]:
    """Referenced tables that do not exist in *schema* (hallucinated relations)."""
    if not schema:
        return set()
    known = {str(t).split(".")[-1].lower() for t in schema}
    return referenced_tables(sql) - known
