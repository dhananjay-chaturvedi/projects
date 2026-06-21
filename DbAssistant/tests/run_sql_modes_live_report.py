#!/usr/bin/env python3
"""Manual validation report for SQL modes against local MariaDB (test@localhost:3306)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ai_query.agent import AIQueryAgent
from ai_query.sql_execution_rules import evaluate_execution_rules, format_explain_output
from ai_query.sql_modes import execution_rules_apply, sql_mode_label
from common.db_manager import DatabaseManager

MYSQL = dict(
    host="localhost",
    port=3306,
    username="dheeru",
    password="dheeru",
    database="test",
)

RULES = (
    "Always use LIMIT clause for SELECT on user tables\n"
    "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
)

QUERIES = {
    "strict_catalog": (
        "SELECT COUNT(*) AS c FROM information_schema.tables "
        "WHERE table_schema = DATABASE()"
    ),
    "user_no_limit": "SELECT * FROM EMPLOYEES",
    "user_with_limit": "SELECT EMP_ID, EMP_NAME FROM EMPLOYEES LIMIT 3",
    "multi_join": (
        "SELECT e.EMP_ID, d.DEPT_NAME, p.PRODUCT_NAME "
        "FROM EMPLOYEES e "
        "JOIN DEPARTMENTS d ON d.DEPT_ID = 1 "
        "JOIN PRODUCTS p ON p.PRODUCT_ID = 1 "
        "LIMIT 5"
    ),
}


def main() -> int:
    print("=" * 72)
    print("SQL MODES LIVE VALIDATION — local_mariadb (test@localhost:3306)")
    print("=" * 72)

    mgr = DatabaseManager("MariaDB")
    mgr.connect(**MYSQL)

    tables_r, _ = mgr.execute_query("SHOW TABLES")
    user_tables = [r[0] for r in tables_r.get("rows", [])]
    ctx = {"database_type": "MariaDB", "schema": {"tables": user_tables}}

    agent = AIQueryAgent.__new__(AIQueryAgent)
    failures = 0

    def check(name: str, ok: bool, detail: str = ""):
        nonlocal failures
        status = "PASS" if ok else "FAIL"
        if not ok:
            failures += 1
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))

    for mode in ("strict_summary", "summary", "open"):
        print(f"\n--- SQL mode: {sql_mode_label(mode)} ({mode}) ---")

        for label, sql in QUERIES.items():
            result = {"summary_sql": sql, "explanation": ""}
            validated = agent._apply_sql_mode_validation(dict(result), ctx, mode)
            blocked = bool(validated.get("summary_mode_blocked"))

            if mode == "strict_summary" and label == "user_no_limit":
                check("strict blocks user table SQL", blocked)
            elif mode == "strict_summary" and label == "strict_catalog":
                check("strict allows catalog SQL", not blocked)
                if not blocked:
                    _, err = mgr.execute_query(sql)
                    check("strict catalog executes on DB", err is None, err or "")
            elif mode in ("summary", "open") and label == "user_with_limit":
                check(f"{mode} allows user SQL", not blocked)

            if execution_rules_apply(mode) and label in (
                "user_no_limit",
                "user_with_limit",
                "multi_join",
            ):
                gate = evaluate_execution_rules(
                    sql, RULES, user_table_names=user_tables, db_type="MariaDB"
                )
                if label == "user_no_limit":
                    check(f"{mode} execution rules block no LIMIT", not gate.allowed)
                elif label == "user_with_limit":
                    check(f"{mode} execution rules allow LIMIT", gate.allowed)
                    if gate.allowed:
                        _, err = mgr.execute_query(sql)
                        check(f"{mode} user query executes", err is None, err or "")
                elif label == "multi_join":
                    check(
                        f"{mode} EXPLAIN rule triggers",
                        gate.run_explain_first,
                    )
                    if gate.run_explain_first:
                        exp, eerr = mgr.execute_query(gate.explain_sql)
                        check(
                            f"{mode} EXPLAIN runs on DB",
                            eerr is None,
                            eerr or format_explain_output(exp)[:80],
                        )
                        _, merr = mgr.execute_query(sql)
                        check(f"{mode} join query executes", merr is None, merr or "")

    mgr.disconnect()
    print("\n" + "=" * 72)
    if failures:
        print(f"RESULT: {failures} failure(s)")
        return 1
    print("RESULT: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
