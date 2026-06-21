"""
End-to-end self-test / demo for the RAG pipeline.

Run with::

    python -m ai_assistant.rag.selftest

It builds a sample SQLite database, indexes it, then runs several retrieval
queries and exercises the feedback loop — all without a saved connection or an
AI backend. If an AI backend is available it also demonstrates a full
RAG-augmented ``ask``.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


class _DirectCore:
    """Minimal core shim so RagService can run against an in-memory manager."""

    def __init__(self, manager, name: str, db_type: str = "SQLite"):
        self._mgr = manager
        self._name = name
        self._db_type = db_type

    def get_manager(self, name: str):
        return self._mgr

    def get_connection_profile(self, name: str):
        return {"name": self._name, "db_type": self._db_type}


def main() -> int:
    from ai_assistant.rag.sample_data import build_sample_manager
    from ai_assistant.rag.service import RagService

    tmp = Path(tempfile.mkdtemp(prefix="rag_selftest_"))
    db_path = tmp / "sample_shop.db"
    index_path = tmp / "rag_index.db"
    conn_name = "rag_sample"

    print(f"[selftest] sample DB : {db_path}")
    print(f"[selftest] index DB  : {index_path}")

    mgr = build_sample_manager(db_path)
    core = _DirectCore(mgr, conn_name)
    svc = RagService(core, agent=None, index_path=index_path)

    # 1) index
    res = svc.index(conn_name, rebuild=True)
    assert res.get("ok"), f"index failed: {res}"
    print(f"\n[1] indexed {res['indexed']} schema docs "
          f"(provider={res['provider']}, dim={res['dim']})")

    # 2) status
    st = svc.status(conn_name)
    assert st.get("ok") and st.get("indexed"), st
    print(f"[2] status: doc_count={st['doc_count']} meta={st['meta']}")

    # 3) retrieval queries
    queries = [
        "which customers spent the most money?",
        "list products that are out of stock",
        "show pending orders with their total amount",
        "what payment methods were used?",
    ]
    for q in queries:
        out = svc.search(conn_name, q, k=3)
        assert out.get("ok"), out
        top = ", ".join(f"{h['ref']}({h['score']})" for h in out["hits"])
        print(f"\n[3] Q: {q}\n    top hits: {top}")

    # sanity: a stock question should surface the products table first
    stock = svc.search(conn_name, "out of stock products", k=3)["hits"]
    assert stock and stock[0]["ref"] == "products", stock
    print("\n[3b] OK - 'products' ranked #1 for stock question")

    # 4) formatted context
    ctx = svc.context(conn_name, "total revenue per customer", k=4)
    assert ctx.get("ok") and "RETRIEVED DATABASE CONTEXT" in ctx["context"]
    print("\n[4] formatted context preview:")
    print("\n".join(ctx["context"].splitlines()[:12]))

    # 5) feedback loop: glossary + example, then confirm retrievable
    g = svc.add_glossary(conn_name, "lifetime spend",
                         "total of orders.total_amount per customer")
    assert g.get("ok"), g
    e = svc.add_example(
        conn_name,
        "top spending customers",
        "SELECT customer_id, SUM(total_amount) AS spend FROM orders "
        "GROUP BY customer_id ORDER BY spend DESC;",
        description="lifetime spend ranking",
    )
    assert e.get("ok"), e
    hits = svc.search(conn_name, "lifetime spend ranking", k=5)["hits"]
    kinds = {h["kind"] for h in hits}
    assert "example" in kinds or "glossary" in kinds, hits
    print(f"\n[5] feedback loop OK - retrieved kinds: {sorted(kinds)}")

    # 6) optional: full RAG ask if an AI backend is wired up
    try:
        from ai_query.agent import AIQueryAgent

        agent = AIQueryAgent()
        if agent.auto_select_backend(quiet=True):
            svc_ai = RagService(core, agent=agent, index_path=index_path)
            ask = svc_ai.ask(conn_name, "which customers spent the most?", k=5)
            print("\n[6] RAG ask result:")
            print("    SQL:", (ask.get("sql") or ask.get("error")))
        else:
            print("\n[6] (skipped RAG ask - no AI backend available)")
    except Exception as exc:  # noqa: BLE001
        print(f"\n[6] (skipped RAG ask - {exc})")

    print("\n[selftest] ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
