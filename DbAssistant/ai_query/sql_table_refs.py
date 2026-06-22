"""
Shared SQL table reference parsing and catalog/system detection.

Used by strict-summary validation and pre-execution rule checks.
"""

from __future__ import annotations

import re

_SYSTEM_HINTS = (
    r"information_schema",
    r"performance_schema",
    r"pg_catalog",
    r"sqlite_master",
    r"^sys\.",
    r"^SYS\.",
    r"^ALL_",
    r"^DBA_",
    r"^USER_",
    r"^V\$",
    r"^v\$",
    r"^GV\$",
    r"^DBA_",
    r"INFORMATION_SCHEMA",
    r"pragma_",
)


def extract_table_refs(sql: str) -> set[str]:
    if not sql:
        return set()
    refs: set[str] = set()
    for m in re.finditer(
        r"(?i)\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([`\"'\[\]\w$.]+(?:\.[`\"'\[\]\w$]+)*)",
        sql,
    ):
        token = m.group(1).strip("`\"[]")
        refs.add(token)
    return {r for r in refs if r and r.upper() not in ("SELECT", "DUAL")}


def is_system_reference(name: str, db_type: str) -> bool:
    low = name.lower()
    for pat in _SYSTEM_HINTS:
        if re.search(pat, name, re.IGNORECASE):
            return True
    if "mysql" in (db_type or "").lower() or "mariadb" in (db_type or "").lower():
        if low.startswith("sys.") or "information_schema" in low or "performance_schema" in low:
            return True
    if "postgres" in (db_type or "").lower():
        if low.startswith("pg_") or "pg_catalog" in low:
            return True
    if "oracle" in (db_type or "").lower():
        if low.startswith(("all_", "dba_", "user_", "v$", "gv$")):
            return True
    if "sqlserver" in (db_type or "").lower() or "mssql" in (db_type or "").lower():
        if low.startswith("sys.") or "information_schema" in low:
            return True
    if "sqlite" in (db_type or "").lower():
        if low == "sqlite_master" or low.startswith("pragma_"):
            return True
    return False
