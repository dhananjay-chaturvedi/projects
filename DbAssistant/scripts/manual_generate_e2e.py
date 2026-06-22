"""Real end-to-end Generate SQL against live MariaDB using an available backend.

Exercises the new compact-prompt + progressive-escalation ask path and the
token meter on a real backend call, then executes the generated SQL.

Run:  PYTHONPATH=. .venv/bin/python scripts/manual_generate_e2e.py
"""

from __future__ import annotations

import sys

from ai_query.service import make_service
from ai_query import token_meter


def run_sql(mgr, sql):
    out = mgr.execute_query(sql)
    if isinstance(out, tuple):
        result, error = out
    else:
        result, error = out, None
    return (result or {}), error


def main() -> int:
    svc = make_service()
    cm = svc._core._cm
    name = None
    for c in cm.get_all_connections():
        if (c.get("db_type") or "").lower() in ("mariadb", "mysql"):
            name = c.get("name")
            break
    if not name:
        print("No MariaDB connection saved.")
        return 2

    agent = svc._ai
    # Pick the first verified-available backend.
    avail = agent.list_available_backends()
    print("available backends:", avail)
    if not avail:
        # Try to auto-select (probes backends).
        agent.auto_select_backend()
        avail = agent.list_available_backends()
        print("after auto-select:", avail, "active:", agent.get_active_backend_name())
    if not agent.get_active_backend_name():
        print("No AI backend available; skipping live generation.")
        return 0

    captured = []
    token_meter.register_capture_hook(lambda r: captured.append(r))
    try:
        questions = [
            "how many tables are in this database",
            "list the first 5 products",
        ]
        for q in questions:
            print("\n" + "=" * 70)
            print("QUESTION:", q)
            print("=" * 70)
            r = svc.ai_query(name, q)
            print("backend:", agent.get_active_backend_name())
            print("prompt_tokens_est:", r.get("prompt_tokens_est"))
            print("error:", r.get("error"))
            sql = r.get("sql") or r.get("summary_sql")
            print("generated SQL:", sql)
            if sql and not r.get("error"):
                mgr = svc._core.get_manager(name)
                res, err = run_sql(mgr, sql)
                if err:
                    print("EXEC ERROR:", err)
                else:
                    print("EXEC OK rows:", res.get("rowcount"), "->", str(res.get("rows"))[:200])
    finally:
        token_meter.clear_capture_hooks()

    print("\nToken meter records captured:", len(captured))
    for rec in captured:
        print("  path=%s tier=%s tokens=%s backend=%s" % (
            rec.get("path"), rec.get("tier"), rec.get("prompt_tokens_est"), rec.get("backend")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
