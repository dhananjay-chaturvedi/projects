"""Read-only SQL guard for AI-driven surfaces.

The AI Query Assistant and the App Builder must NEVER execute data- or
schema-mutating statements (DROP / DELETE / UPDATE / INSERT / TRUNCATE / ALTER /
CREATE / GRANT / ...) against a user's live database connection. This module is
the single shared chokepoint that classifies a SQL string and rejects any
mutating statement before it can reach ``cursor.execute``.

This is a hard, non-configurable safety guarantee for those surfaces. The
general-purpose SQL Editor and Data Migration tools deliberately do NOT use this
guard — only the AI surfaces do.
"""

from __future__ import annotations

import re

from common.sql_splitter import split_sql_statements, strip_sql_comments

# Leading statement keywords that mutate data or schema. Kept as an explicit,
# auditable denylist. EXPLAIN is allowed (the AI execution gate runs it itself);
# SELECT / WITH / SHOW / DESCRIBE / PRAGMA / VALUES / TABLE are read-only.
AI_FORBIDDEN_KEYWORDS: frozenset[str] = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "UPSERT",
        "REPLACE",
        "TRUNCATE",
        "DROP",
        "ALTER",
        "CREATE",
        "RENAME",
        "GRANT",
        "REVOKE",
        "COMMENT",
        "CALL",
        "EXEC",
        "EXECUTE",
        "ATTACH",
        "DETACH",
        "COPY",
        "LOAD",
        "IMPORT",
        "MOVE",
        "PUT",
        "VACUUM",
        "REINDEX",
        "CLUSTER",
        "LOCK",
        "UNLOCK",
        "SET",
        "RESET",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "START",
    }
)

# The statements the user explicitly forbids the AI surfaces from ever running,
# plus their close destructive siblings. Used by paths (like additive schema
# deploy) that legitimately need CREATE but must still never destroy data.
DESTRUCTIVE_KEYWORDS: frozenset[str] = frozenset(
    {"DROP", "DELETE", "UPDATE", "TRUNCATE", "ALTER", "RENAME", "REPLACE", "MERGE"}
)

_LEADING_WORD_RE = re.compile(r"^\s*([A-Za-z_]+)")
# A CTE (WITH ...) that ultimately performs a data-modifying operation, e.g.
# ``WITH x AS (...) DELETE FROM ...`` or ``WITH x AS (...) INSERT INTO ...``.
_MODIFYING_CTE_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE
)
# SELECT statements that still mutate data/files/schema despite leading with SELECT.
_SELECT_WRITE_SIDE_EFFECTS = (
    re.compile(r"(?i)\bINTO\s+(OUTFILE|DUMPFILE)\b"),
    re.compile(r"(?i)\bSELECT\b[\s\S]+\bINTO\s+(?!OUTFILE|DUMPFILE|VARIABLE|@)[#\[A-Za-z_]"),
)


def _leading_keyword(statement: str) -> str:
    match = _LEADING_WORD_RE.match(statement or "")
    return match.group(1).upper() if match else ""


def _sqlglot_is_modifying(statement: str, db_type: str = "") -> bool:
    """Best-effort structural check using sqlglot when available.

    Returns True when the parsed statement contains any data/schema mutation
    node anywhere in its tree (catches CTE-wrapped DML and vendor quirks).
    """
    try:
        import sqlglot
        from sqlglot import expressions as exp
    except Exception:  # noqa: BLE001 - sqlglot optional
        return False

    dialect = None
    key = (db_type or "").strip().lower().replace(" ", "")
    dialect_map = {
        "mysql": "mysql",
        "mariadb": "mysql",
        "postgresql": "postgres",
        "postgres": "postgres",
        "sqlite": "sqlite",
        "oracle": "oracle",
        "sqlserver": "tsql",
        "mssql": "tsql",
    }
    dialect = dialect_map.get(key)

    try:
        parsed = (
            sqlglot.parse_one(statement, read=dialect)
            if dialect
            else sqlglot.parse_one(statement)
        )
    except Exception:  # noqa: BLE001 - fall back to keyword check
        return False
    if parsed is None:
        return False

    modifying_types = (
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Drop,
        exp.Alter,
        exp.Create,
        exp.Merge,
    )
    if isinstance(parsed, modifying_types):
        return True
    for node_type in modifying_types:
        if parsed.find(node_type) is not None:
            return True
    # exp.Command covers TRUNCATE / GRANT / REVOKE / VACUUM / etc.
    cmd = parsed.find(exp.Command)
    if cmd is not None:
        name = (getattr(cmd, "name", "") or "").upper()
        if name in AI_FORBIDDEN_KEYWORDS:
            return True
    return False


def inspect_read_only(sql: str, *, db_type: str = "") -> tuple[bool, str, list[str]]:
    """Classify *sql* and report whether it is safe (read-only) for AI surfaces.

    Returns ``(ok, reason, offending)`` where *offending* is the list of
    forbidden leading keywords found. *ok* is True only when every statement is
    read-only.
    """
    text = (sql or "").strip()
    if not text:
        return False, "Empty SQL.", []

    statements = split_sql_statements(text)
    if not statements:
        return False, "Empty SQL.", []

    offending: list[str] = []
    for stmt in statements:
        bare = strip_sql_comments(stmt).strip()
        if not bare:
            continue
        keyword = _leading_keyword(bare)
        if keyword in AI_FORBIDDEN_KEYWORDS:
            offending.append(keyword)
            continue
        # WITH-prefixed data-modifying CTEs (top-level keyword is WITH but the
        # statement ends in INSERT/UPDATE/DELETE/MERGE).
        if keyword == "WITH" and _MODIFYING_CTE_RE.search(
            strip_sql_comments(re.sub(r"'[^']*'", "", bare))
        ):
            offending.append("WITH+DML")
            continue
        scan = strip_sql_comments(re.sub(r"'[^']*'", "", bare))
        if any(p.search(scan) for p in _SELECT_WRITE_SIDE_EFFECTS):
            offending.append("SELECT_INTO")
            continue
        if _sqlglot_is_modifying(bare, db_type):
            offending.append(keyword or "UNKNOWN")

    if offending:
        uniq = sorted(set(offending))
        return (
            False,
            (
                "Blocked: the AI assistant is read-only and cannot run "
                f"data/schema-changing statements ({', '.join(uniq)}). "
                "Only SELECT-style queries are allowed here."
            ),
            offending,
        )
    return True, "", []


def assert_read_only(sql: str, *, db_type: str = "") -> str:
    """Return an error string if *sql* is not read-only, else ``""``."""
    ok, reason, _ = inspect_read_only(sql, db_type=db_type)
    return "" if ok else reason


def find_destructive(sql: str) -> list[str]:
    """Return destructive leading keywords (DROP/DELETE/UPDATE/...) in *sql*.

    Used by additive-only paths (e.g. schema deploy) that need CREATE/INSERT but
    must still never run a destructive statement.
    """
    found: list[str] = []
    for stmt in split_sql_statements((sql or "").strip()):
        bare = strip_sql_comments(stmt).strip()
        if not bare:
            continue
        keyword = _leading_keyword(bare)
        if keyword in DESTRUCTIVE_KEYWORDS:
            found.append(keyword)
    return found
