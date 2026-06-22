"""Manual end-to-end test of AI Query token-efficiency paths against live MariaDB.

Connects via the app's service layer (credentials decrypted internally; never
printed), seeds a small sample dataset, and exercises:
  - compact vs verbose schema rendering (Phase 1)
  - follow-up schema digest (Phase 2)
  - cross-tab dedup (Phase 2)
  - RAG gating + conditional format block (Phase 3)
  - token meter capture (Phase 0)
  - looks_like_sql guard + AST validation (Phase 6)
  - auto-refine oscillation detection (Phase 6)

Run:  .venv/bin/python scripts/manual_token_efficiency_e2e.py
"""

from __future__ import annotations

import sys

from ai_query.service import make_service
from ai_query import prompt_assembly as pa
from ai_query import token_meter
from ai_query.auto_execute_orchestrator import AutoExecuteOrchestrator
from ai_query.response_parser import response_format_instructions, _clean_sql_block
from ai_query.sql_validation import validate_sql_against_schema


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def find_mariadb_manager(svc):
    cm = svc._core._cm
    conns = cm.get_all_connections()
    for c in conns:
        if (c.get("db_type") or "").lower() in ("mariadb", "mysql"):
            return c.get("name")
    return None


def run_sql(mgr, sql):
    """Execute SQL via db_manager, returning (result_dict, error)."""
    out = mgr.execute_query(sql)
    if isinstance(out, tuple):
        result, error = out
    else:
        result, error = out, None
    return (result or {}), error


def seed_sample_data(mgr):
    """Create a deterministic sample dataset (idempotent)."""
    stmts = [
        "DROP TABLE IF EXISTS te_orders",
        "DROP TABLE IF EXISTS te_customers",
        """CREATE TABLE te_customers (
            id INT PRIMARY KEY,
            email VARCHAR(120) NOT NULL,
            country VARCHAR(40),
            created_at DATE
        )""",
        """CREATE TABLE te_orders (
            id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20),
            CONSTRAINT fk_te_cust FOREIGN KEY (customer_id) REFERENCES te_customers(id)
        )""",
        "INSERT INTO te_customers VALUES (1,'a@x.com','US','2024-01-01')",
        "INSERT INTO te_customers VALUES (2,'b@x.com','IN','2024-02-01')",
        "INSERT INTO te_customers VALUES (3,'c@x.com','US','2024-03-01')",
        "INSERT INTO te_orders VALUES (10,1,100.50,'paid')",
        "INSERT INTO te_orders VALUES (11,1,25.00,'pending')",
        "INSERT INTO te_orders VALUES (12,2,300.00,'paid')",
    ]
    for s in stmts:
        run_sql(mgr, s)


def main() -> int:
    svc = make_service()
    name = find_mariadb_manager(svc)
    if not name:
        print("No MariaDB/MySQL connection saved; cannot run live test.")
        return 2
    print(f"Using MariaDB connection (name hidden for safety): <{name[:3]}...>")

    mgr = svc._core.get_manager(name)
    if mgr is None or getattr(mgr, "conn", None) is None:
        print("Could not establish live connection.")
        return 2

    banner("STEP 1 — Seed sample data (te_customers, te_orders)")
    try:
        seed_sample_data(mgr)
        print("Seeded 3 customers + 3 orders.")
    except Exception as exc:
        print(f"Seed warning: {exc}")

    cust, _ = run_sql(mgr, "SELECT * FROM te_customers ORDER BY id")
    print("te_customers rows:", cust.get("rowcount"), "->", cust.get("rows"))
    orders, _ = run_sql(mgr, "SELECT * FROM te_orders ORDER BY id")
    print("te_orders rows:", orders.get("rowcount"), "->", orders.get("rows"))

    agent = svc._ai

    banner("STEP 2 — Build live comprehensive context (real MariaDB schema)")
    ctx = agent.get_comprehensive_db_context(mgr, name, "show all customers")
    tnames = ctx.get("schema", {}).get("table_schemas", {})
    print("Tables with detailed schema:", len(tnames))
    print("_analysis:", ctx.get("_analysis"))
    print("tables_signature present:", bool(ctx.get("_tables_signature")))

    banner("STEP 3 — Phase 1: compact vs verbose schema rendering")
    compact = agent._build_intelligent_context(ctx, "show all customers", tier=1)
    # Force verbose by toggling the flag off via direct formatter call
    table_schemas = ctx["schema"]["table_schemas"]
    verbose = pa.format_table_schemas(table_schemas, compact=False, max_tables=20)
    compact_only = pa.format_table_schemas(table_schemas, compact=True, max_tables=20)
    print(f"compact context tokens (est): {token_meter.estimate_tokens(compact)}")
    print(f"verbose table block chars: {len(verbose)}  compact table block chars: {len(compact_only)}")
    print("box-drawing in compact?", "yes" if "\u250f" in compact else "no")
    # Verify all te_customers columns survive compaction
    for col in ("id", "email", "country", "created_at"):
        present = col in compact
        print(f"  preserves te_customers.{col}: {present}")

    banner("STEP 4 — Phase 2: follow-up schema digest (only referenced tables)")
    refs = pa.extract_referenced_tables(
        "show orders for paid customers",
        "SELECT * FROM te_orders",
        all_tables=ctx["schema"]["tables"],
    )
    print("referenced tables detected:", refs)
    digest = agent._build_intelligent_context(
        ctx, "show orders", schema_mode="digest", referenced_tables=refs
    )
    print(f"digest tokens (est): {token_meter.estimate_tokens(digest)} vs full {token_meter.estimate_tokens(compact)}")
    print("digest mentions te_orders:", "te_orders" in digest)

    banner("STEP 5 — Phase 2: cross-tab dedup by connection")
    b1 = {"tab_number": 1, "connection_name": name, "db_type": "mariadb", "schema_context": "S1"}
    b2 = {"tab_number": 2, "connection_name": name, "db_type": "mariadb", "schema_context": "S2"}
    merged = pa.dedupe_peer_bundles([b1, b2])
    print(f"2 tabs same connection -> {len(merged)} bundle, tabs={merged[0]['tab_numbers']}")

    banner("STEP 6 — Phase 3: conditional format block (simple vs complex)")
    simple_fmt = response_format_instructions("mariadb", name, sql_mode="summary", complexity=0, is_simple=True)
    complex_fmt = response_format_instructions("mariadb", name, sql_mode="open", complexity=3, is_simple=False)
    print("simple omits DETAIL_SQL:", "DETAIL_SQL" not in simple_fmt)
    print("complex includes DETAIL_SQL:", "DETAIL_SQL" in complex_fmt)

    banner("STEP 7 — Phase 6: AST validation against live schema")
    good = "SELECT id, email, country FROM te_customers WHERE country = 'US'"
    bad = "SELECT id, mystery_column FROM te_customers"
    print("good SQL warnings:", validate_sql_against_schema(good, ctx, db_type="mariadb"))
    print("bad SQL warnings:", validate_sql_against_schema(bad, ctx, db_type="mariadb"))
    # Execute the good SQL to prove it's valid against the live DB
    res, err = run_sql(mgr, good)
    print("executed good SQL ->", res.get("rowcount"), "rows:", res.get("rows"), "err:", err)

    banner("STEP 8 — Phase 6: looks_like_sql guard")
    print("SELECT ... ->", pa.looks_like_sql(good, "mariadb"))
    print("prose ->", pa.looks_like_sql(
        "I think you should look at the customers table for this information here.",
        "mariadb",
    ))

    banner("STEP 9 — Phase 6: tilde fence + oscillation detection")
    print("~~~ fence cleaned:", _clean_sql_block("~~~\nSELECT 1\n~~~"))
    orch = AutoExecuteOrchestrator(agent, max_iterations=5)
    print("first SELECT 1:", orch.record_sql("SELECT 1"))
    print("repeat SELECT 1 (oscillation):", orch.record_sql("select 1;"))

    banner("STEP 10 — Phase 0: token meter capture on a real assembly")
    captured = []
    token_meter.register_capture_hook(lambda r: captured.append(r))
    try:
        token_meter.record_prompt(path="manual_e2e", prompt=compact, backend="manual", tier=1)
    finally:
        token_meter.clear_capture_hooks()
    print("captured record:", {k: captured[0][k] for k in ("path", "prompt_tokens_est", "tier")})

    banner("CLEANUP — drop sample tables")
    run_sql(mgr, "DROP TABLE IF EXISTS te_orders")
    run_sql(mgr, "DROP TABLE IF EXISTS te_customers")
    print("dropped te_orders, te_customers")

    banner("RESULT: all live MariaDB token-efficiency checks completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
