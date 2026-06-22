"""Deploy a generated app's schema into a selected database connection.

Strictly opt-in and guarded: nothing here runs unless the caller has the user's
explicit approval (the "Deploy tables to selected connection" checkbox, which is
OFF by default, plus a confirmation in interactive mode). We only ever issue
``CREATE TABLE IF NOT EXISTS`` statements taken from the app's own
``src/db/schema.sql`` — never DROP/ALTER/INSERT — so deploying is additive and
safe to re-run.

The DB manager is duck-typed: it only needs ``execute_query(sql) -> (rows, err)``.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

# We only allow additive table creation to be deployed.
_ALLOWED = re.compile(r"^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS", re.IGNORECASE)


def extract_ddl(files: Mapping[str, str]) -> list[str]:
    """Return the CREATE TABLE statements from the generated app's schema.sql."""
    sql = ""
    for path, content in files.items():
        if path.replace("\\", "/").endswith("db/schema.sql"):
            sql = content or ""
            break
    if not sql:
        return []
    statements = [s.strip() for s in sql.split(";")]
    return [s for s in statements if _ALLOWED.match(s)]


def deploy_schema(
    db_manager: Any,
    files: Mapping[str, str],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Deploy the app's CREATE TABLE statements to *db_manager*.

    Returns a structured report; ``deployed`` is True only when at least one
    statement was actually executed without error. With ``dry_run`` we validate
    + count the statements but execute nothing.
    """
    ddl = extract_ddl(files)
    report: dict[str, Any] = {
        "deployed": False, "dry_run": bool(dry_run),
        "statements": len(ddl), "executed": 0, "errors": [],
    }
    if not ddl:
        report["errors"].append("no CREATE TABLE statements found in schema.sql")
        return report
    if db_manager is None:
        report["errors"].append("no database connection available to deploy to")
        return report
    if dry_run:
        return report

    from common.sql_guard import find_destructive

    executed = 0
    for stmt in ddl:
        # Defense in depth: extract_ddl already restricts to additive
        # CREATE TABLE IF NOT EXISTS, but never let a destructive statement run.
        destructive = find_destructive(stmt)
        if destructive:
            report["errors"].append(
                f"refused destructive statement ({', '.join(sorted(set(destructive)))})"
            )
            continue
        try:
            _rows, err = db_manager.execute_query(stmt)
        except Exception as exc:  # noqa: BLE001
            err = str(exc)
        if err:
            report["errors"].append(str(err))
        else:
            executed += 1
    report["executed"] = executed
    report["deployed"] = executed > 0
    return report
