#!/usr/bin/env python3
"""Standalone Cursor Agent report for SQL modes (local MariaDB)."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ai_query.agent import AIQueryAgent
from ai_query.summary_sql_validator import validate_summary_mode_sql
from common.db_manager import DatabaseManager

MYSQL = dict(
    host="localhost",
    port=3306,
    username="dheeru",
    password="dheeru",
    database="test",
)

CASES = [
    (
        "strict_summary",
        "How many tables are in the current database? Use catalog views only.",
        lambda sql, r, ctx, agent: (
            not validate_summary_mode_sql(sql, "MariaDB", agent._user_table_names_from_context(ctx))
        ),
    ),
    (
        "summary",
        "Show up to 5 employee first names from EMPLOYEES.",
        lambda sql, r, ctx, agent: "employees" in sql.lower(),
    ),
    (
        "open",
        "Count all rows in EMPLOYEES table.",
        lambda sql, r, ctx, agent: "employees" in sql.lower() and not r.get("summary_mode_blocked"),
    ),
]


def main() -> int:
    print("=" * 72)
    print("CURSOR AGENT SQL MODE AI TEST — local_mariadb")
    print("=" * 72)

    agent = AIQueryAgent()
    if not agent.set_backend("cursor", verify=True):
        print("FAIL: Cursor Agent not available")
        return 1

    mgr = DatabaseManager("MariaDB")
    mgr.connect(**MYSQL)
    failures = 0

    for mode, question, validator in CASES:
        sess = agent.sessions.create(connection_name="local_mariadb", backend="cursor")
        sess.sql_mode = mode
        sess.sql_modes_v2 = True
        print(f"\n--- {mode} ---")
        t0 = time.time()
        result = agent.start_new_conversation(
            question, mgr, "local_mariadb", session_id=sess.session_id
        )
        elapsed = time.time() - t0
        sql = result.get("summary_sql") or result.get("sql") or ""
        print(f"  time: {elapsed:.1f}s")
        print(f"  sql: {sql[:200]}")
        if result.get("error"):
            print(f"  FAIL: {result['error']}")
            failures += 1
            continue
        ctx = agent.get_cached_comprehensive_context(mgr, "local_mariadb", "schema")
        ok = validator(sql, result, ctx, agent)
        if ok and sql.strip():
            _, err = mgr.execute_query(sql)
            if err:
                print(f"  FAIL execute: {err}")
                failures += 1
            else:
                print("  PASS (AI + execute)")
        else:
            print("  FAIL validation")
            failures += 1
        agent.sessions.delete(sess.session_id)

    mgr.disconnect()
    print("\n" + "=" * 72)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
