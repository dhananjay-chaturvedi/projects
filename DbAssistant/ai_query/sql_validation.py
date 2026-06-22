"""SQL identifier validation against schema (AST-first, regex fallback)."""

from __future__ import annotations

import re
from typing import Any

_IDENTIFIER_RE = re.compile(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b")

_SQL_KEYWORDS = frozenset(
    {
        "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON",
        "AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "AS",
        "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT",
        "EXCEPT", "DISTINCT", "ALL", "ASC", "DESC", "COUNT", "SUM", "AVG", "MAX",
        "MIN", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "EXTRACT", "SUBSTRING",
        "COALESCE", "NULLIF", "ABS", "CEIL", "CEILING", "CONCAT", "CURRENT_DATE",
        "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATE", "DATEDIFF", "DATE_FORMAT",
        "DATE_TRUNC", "DAY", "IF", "IFNULL", "ISNULL", "LOWER", "MONTH", "NOW",
        "ROUND", "TO_CHAR", "TO_DATE", "TRIM", "UPPER", "YEAR", "OVER", "PARTITION",
        "WINDOW", "ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT",
        "TRUE", "FALSE", "WITH", "RECURSIVE", "LATERAL", "CROSS", "FULL", "NATURAL",
        "USING", "VALUES", "INTO", "SET", "INSERT", "UPDATE", "DELETE", "TABLE",
    }
)


def _dialect_for_sqlglot(db_type: str) -> str | None:
    key = (db_type or "").lower()
    mapping = {
        "mysql": "mysql",
        "mariadb": "mysql",
        "postgresql": "postgres",
        "postgres": "postgres",
        "oracle": "oracle",
        "sqlserver": "tsql",
        "mssql": "tsql",
        "sqlite": "sqlite",
        "snowflake": "snowflake",
        "bigquery": "bigquery",
        "redshift": "redshift",
    }
    for name, dialect in mapping.items():
        if name in key:
            return dialect
    return None


def _extract_identifiers_ast(sql: str, dialect: str) -> set[str] | None:
    try:
        import sqlglot
        from sqlglot import exp
    except Exception:
        return None
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return None
    if tree is None:
        return None
    ids: set[str] = set()
    for node in tree.walk():
        if isinstance(node, exp.Column):
            col = node.name
            if col:
                ids.add(col)
        elif isinstance(node, exp.Table):
            table = node.name
            if table:
                ids.add(table)
    return ids


def _extract_identifiers_regex(sql: str) -> set[str]:
    return set(_IDENTIFIER_RE.findall(sql))


def validate_sql_against_schema(
    sql: str,
    context: dict[str, Any],
    *,
    db_type: str = "",
) -> list[str]:
    """Return warning strings for identifiers not found in the schema."""
    if not sql or not context:
        return []
    table_schemas = context.get("schema", {}).get("table_schemas", {})
    if not table_schemas:
        return []

    valid_columns: dict[str, list[str]] = {}
    table_names_lower = {t.lower(): t for t in table_schemas}
    for table_name, columns in table_schemas.items():
        for col in columns:
            key = col["name"].lower()
            valid_columns.setdefault(key, []).append(table_name)

    dialect = _dialect_for_sqlglot(db_type or context.get("database_type", ""))
    identifiers: set[str] | None = None
    if dialect:
        identifiers = _extract_identifiers_ast(sql, dialect)
    if identifiers is None:
        identifiers = _extract_identifiers_regex(sql)

    suspicious: list[str] = []
    for word in identifiers:
        word_upper = word.upper()
        word_lower = word.lower()
        if word_upper in _SQL_KEYWORDS:
            continue
        if word_lower in table_names_lower:
            continue
        if len(word) < 2:
            continue
        if word_lower in valid_columns:
            continue
        if word_upper.startswith("PG_") or word_upper.startswith("SYS"):
            continue
        if word not in suspicious:
            suspicious.append(word)

    warnings: list[str] = []
    if suspicious:
        warnings.append(
            f"The SQL uses column name(s) not found in schema: {', '.join(suspicious[:10])}"
        )
        warnings.append(
            "Please verify the query with your database. The AI may have used incorrect column names."
        )
        if len(suspicious) > 10:
            warnings.append(f"... and {len(suspicious) - 10} more suspicious identifiers")
    return warnings
