"""Tests for the offline RAG + local LLM subsystems.

Covers:
    * hashing embeddings (deterministic, normalised, semantic-ish ranking)
    * the SQLite vector store (upsert / load / count / delete / meta)
    * the RagService end-to-end against a real sample SQLite database
      (index, status, search ranking, context block, feedback loop, clear,
       per-connection isolation)
    * the LlmService (engine listing, train+generate on the python engine,
      status, model listing, dataset export, dedup, fallback)
    * registration of the offline Local LLM backend

Everything runs fully offline with the dependency-free defaults (hashing
embedder + pure-python LLM engine), so results are stable on any machine.
"""

from __future__ import annotations

import math

import pytest

from ai_assistant.rag.documents import Document
from ai_assistant.rag.embeddings import (
    HashingEmbedder,
    get_embedder,
    tokenize,
)
from ai_assistant.rag.vector_store import SqliteVectorStore


# ── test doubles ──────────────────────────────────────────────────────────────
class _DirectCore:
    """Minimal core shim so RagService runs against an in-memory manager."""

    def __init__(self, manager, name: str, db_type: str = "SQLite"):
        self._mgr = manager
        self._name = name
        self._db_type = db_type

    def get_manager(self, name: str):
        return self._mgr

    def get_connection_profile(self, name: str):
        return {"name": self._name, "db_type": self._db_type}


@pytest.fixture()
def sample_service(tmp_path):
    """A RagService wired to a freshly-built sample SQLite database."""
    from ai_assistant.rag.sample_data import build_sample_manager
    from ai_assistant.rag.service import RagService

    mgr = build_sample_manager(tmp_path / "shop.db")
    core = _DirectCore(mgr, "shop")
    svc = RagService(core, agent=None, index_path=tmp_path / "rag_index.db")
    return svc


# ── embeddings ────────────────────────────────────────────────────────────────
def _cosine(a, b):
    n = min(len(a), len(b))
    return sum(a[i] * b[i] for i in range(n))


def test_embedder_deterministic_and_normalized():
    e = HashingEmbedder(dim=256)
    v1 = e.embed_one("how many customers placed an order")
    v2 = e.embed_one("how many customers placed an order")
    assert v1 == v2
    assert len(v1) == 256
    norm = math.sqrt(sum(x * x for x in v1))
    assert norm == pytest.approx(1.0, abs=1e-6)


def test_embedder_self_similarity_is_one():
    e = HashingEmbedder()
    v = e.embed_one("list all products by category")
    assert _cosine(v, v) == pytest.approx(1.0, abs=1e-6)


def test_similar_text_scores_higher_than_unrelated():
    e = HashingEmbedder()
    base = e.embed_one("total number of customers in the database")
    similar = e.embed_one("count of customers in the database")
    unrelated = e.embed_one("drop the products table and delete everything")
    assert _cosine(base, similar) > _cosine(base, unrelated)


def test_empty_text_embeds_to_zero_vector():
    e = HashingEmbedder(dim=64)
    assert _cosine(e.embed_one(""), e.embed_one("anything")) == 0.0


def test_tokenize_splits_snake_and_camel_case():
    toks = set(tokenize("customer_id OrderDate"))
    assert {"customer", "id", "customer_id"} <= toks
    assert {"order", "date", "orderdate"} <= toks


def test_get_embedder_falls_back_to_hashing():
    # sentence-transformers is not installed in this environment.
    emb = get_embedder("sentence-transformers", dim=128)
    assert emb.name == "hash"
    assert emb.dim == 128


# ── vector store ──────────────────────────────────────────────────────────────
def _doc(doc_id, kind, ref, text, **meta):
    return Document(doc_id=doc_id, kind=kind, ref=ref, text=text, metadata=meta)


def test_vector_store_upsert_load_count(tmp_path):
    store = SqliteVectorStore(tmp_path / "v.db")
    e = HashingEmbedder(dim=64)
    docs = [
        _doc("table:customers", "table", "customers", "Table: customers"),
        _doc("table:products", "table", "products", "Table: products"),
    ]
    n = store.upsert_documents(
        "c1", docs, e.embed([d.text for d in docs]), provider="hash", dim=64
    )
    assert n == 2
    assert store.count("c1") == 2
    loaded = store.load_documents("c1")
    assert {d.ref for d in loaded} == {"customers", "products"}


def test_vector_store_upsert_is_idempotent_by_doc_id(tmp_path):
    store = SqliteVectorStore(tmp_path / "v.db")
    e = HashingEmbedder(dim=32)
    d = _doc("table:x", "table", "x", "first")
    store.upsert_documents("c1", [d], e.embed(["first"]), provider="hash", dim=32)
    d2 = _doc("table:x", "table", "x", "second")
    store.upsert_documents("c1", [d2], e.embed(["second"]), provider="hash", dim=32)
    assert store.count("c1") == 1
    assert store.load_documents("c1")[0].text == "second"


def test_vector_store_delete_connection_and_kinds(tmp_path):
    store = SqliteVectorStore(tmp_path / "v.db")
    e = HashingEmbedder(dim=32)
    docs = [
        _doc("table:a", "table", "a", "table a"),
        _doc("glossary:g", "glossary", "g", "term g"),
    ]
    store.upsert_documents("c1", docs, e.embed([d.text for d in docs]),
                           provider="hash", dim=32)
    assert store.delete_kinds("c1", ["table"]) == 1
    assert store.count("c1") == 1  # glossary survives
    assert store.delete_connection("c1") == 1
    assert store.count("c1") == 0


def test_vector_store_meta_roundtrip(tmp_path):
    store = SqliteVectorStore(tmp_path / "v.db")
    e = HashingEmbedder(dim=32)
    store.upsert_documents("c1", [_doc("t:a", "table", "a", "a")],
                           e.embed(["a"]), provider="hash", dim=32)
    store.set_meta("c1", db_type="SQLite", provider="hash", dim=32)
    meta = store.get_meta("c1")
    assert meta and meta["db_type"] == "SQLite" and meta["doc_count"] == 1
    assert any(m["connection"] == "c1" for m in store.list_meta())


# ── RagService end-to-end ───────────────────────────────────────────────────--
def test_rag_index_and_status(sample_service):
    res = sample_service.index("shop", rebuild=True)
    assert res["ok"] is True
    assert res["indexed"] >= 6  # 6 tables in the sample schema
    st = sample_service.status("shop")
    assert st["ok"] and st["indexed"]
    assert st["doc_count"] >= 6


def test_rag_search_ranks_relevant_table_first(sample_service):
    sample_service.index("shop", rebuild=True)
    hits = sample_service.search("shop", "products that are out of stock", k=3)
    assert hits["ok"]
    assert hits["hits"][0]["ref"] == "products"


def test_rag_context_block_is_prompt_ready(sample_service):
    sample_service.index("shop", rebuild=True)
    ctx = sample_service.context("shop", "total revenue per customer", k=4)
    assert ctx["ok"]
    assert "RETRIEVED DATABASE CONTEXT" in ctx["context"]


def test_rag_feedback_loop_glossary_and_example(sample_service):
    sample_service.index("shop", rebuild=True)
    g = sample_service.add_glossary(
        "shop", "lifetime spend", "total of orders.total_amount per customer"
    )
    assert g["ok"]
    e = sample_service.add_example(
        "shop",
        "top spending customers",
        "SELECT customer_id, SUM(total_amount) AS spend FROM orders "
        "GROUP BY customer_id ORDER BY spend DESC;",
        description="lifetime spend ranking",
    )
    assert e["ok"]
    hits = sample_service.search("shop", "lifetime spend ranking", k=5)["hits"]
    kinds = {h["kind"] for h in hits}
    assert "example" in kinds or "glossary" in kinds


def test_rag_reindex_preserves_user_knowledge(sample_service):
    sample_service.index("shop", rebuild=True)
    sample_service.add_glossary("shop", "vip", "a gold loyalty_tier customer")
    before = sample_service.store().count("shop")
    # A non-rebuild reindex refreshes schema docs but keeps glossary/examples.
    sample_service.index("shop", rebuild=False)
    after = sample_service.store().count("shop")
    assert after == before  # glossary doc survived the schema refresh
    hits = sample_service.search("shop", "vip customer", k=5)["hits"]
    assert any(h["kind"] == "glossary" for h in hits)


def test_rag_clear_removes_index(sample_service):
    sample_service.index("shop", rebuild=True)
    out = sample_service.clear("shop")
    assert out["ok"] and out["removed"] >= 6
    assert sample_service.status("shop")["indexed"] is False


def test_rag_isolation_between_connections(tmp_path):
    from ai_assistant.rag.sample_data import build_sample_manager
    from ai_assistant.rag.service import RagService

    index_path = tmp_path / "shared_index.db"
    mgr_a = build_sample_manager(tmp_path / "a.db")
    svc_a = RagService(_DirectCore(mgr_a, "a"), index_path=index_path)
    svc_a.index("a", rebuild=True)
    # Connection "b" was never indexed in the shared store.
    svc_b = RagService(_DirectCore(mgr_a, "b"), index_path=index_path)
    assert svc_b.status("b")["indexed"] is False
    assert svc_b.search("b", "customers", k=3)["hits"] == []


def test_rag_document_ingestion_supports_standalone_scope(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    svc = RagService(_NoCore(), index_path=tmp_path / "docs.db")
    res = svc.add_document(
        "kb",
        text="Revenue is recognized monthly.\n\nChurn means a customer cancels.",
        title="Metrics Guide",
        source="metrics.md",
        standalone=True,
    )
    assert res["ok"] and res["chunks"] == 1
    docs = svc.documents("kb")
    assert docs["documents"] == [
        {"source": "metrics.md", "title": "Metrics Guide", "chunks": 1}
    ]
    breakdown = svc.breakdown("kb")
    assert breakdown["counts"]["document"] == 1
    hits = svc.search("kb", "monthly revenue", k=3)["hits"]
    assert any(h["kind"] == "document" for h in hits)


def test_rag_analytics_seed_is_searchable_and_trainable(tmp_path, monkeypatch):
    from ai_assistant.llm.service import LlmService
    from ai_assistant.rag import service as rag_service_mod
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    index_path = tmp_path / "analytics.db"
    svc = RagService(_NoCore(), index_path=index_path)
    res = svc.seed_analytics("kb", standalone=True)
    assert res["ok"] and res["seeded"] >= 20
    assert svc.breakdown("kb")["counts"]["analytical"] == res["seeded"]
    hits = svc.search("kb", "top rows by amount", k=5)["hits"]
    assert any(h["kind"] == "analytical" for h in hits)

    monkeypatch.setattr(rag_service_mod, "default_index_path", lambda: index_path)
    pairs = LlmService._rag_examples("kb")
    assert len(pairs) == res["seeded"]
    assert {"question", "sql"} <= set(pairs[0])


def test_rag_api_routes_include_documents_and_analytics():
    from ai_query.api import build_router

    router = build_router()
    paths = {getattr(r, "path", "") for r in router.routes}
    assert "/api/ai/rag/document" in paths
    assert "/api/ai/rag/documents" in paths
    assert "/api/ai/rag/remove-document" in paths
    assert "/api/ai/rag/analytics" in paths
    assert "/api/ai/rag/seed-analytics" in paths
    assert "/api/ai/rag/breakdown" in paths


def test_rag_index_requires_live_connection(tmp_path):
    from ai_assistant.rag.service import RagService

    class _Disconnected:
        conn = None
        db_type = "SQLite"

    svc = RagService(_DirectCore(_Disconnected(), "dead"),
                     index_path=tmp_path / "i.db")
    res = svc.index("dead", rebuild=True)
    assert res["ok"] is False
    assert "connect" in (res["error"] or "").lower()


# ── LlmService ────────────────────────────────────────────────────────────────
def test_llm_engines_includes_python(tmp_path):
    from ai_assistant.llm.service import LlmService

    out = LlmService(models_dir=tmp_path).engines()
    assert out["ok"]
    names = {e["name"] for e in out["engines"]}
    assert "python" in names
    py = next(e for e in out["engines"] if e["name"] == "python")
    assert py["available"] is True


def test_llm_collect_pairs_dedups(tmp_path):
    from ai_assistant.llm.service import LlmService

    svc = LlmService(models_dir=tmp_path)
    pairs = svc.collect_pairs(include_sample=True)
    keys = {(p["question"].lower(), p["sql"].lower()) for p in pairs}
    assert len(keys) == len(pairs)  # no duplicates
    assert pairs  # sample set is non-empty


def test_llm_train_and_generate_python_engine(tmp_path):
    from ai_assistant.llm.service import LlmService

    svc = LlmService(models_dir=tmp_path)
    r = svc.train(name="t1", engine="python", include_sample=True,
                  overrides={"epochs": 40, "min_loss": 0.02})
    assert r["ok"], r.get("error")
    assert r["engine"] == "python"
    assert r["num_pairs"] > 0

    st = svc.status("t1")
    assert st["ok"] and st["trained"] and st["engine"] == "python"

    models = svc.list_models()["models"]
    assert any(m["name"] == "t1" for m in models)

    # The model should produce SQL that references the right table for a
    # question it was trained on.
    g = svc.generate("list all customers", name="t1")
    assert g["ok"], g.get("error")
    assert "customers" in (g["sql"] or "").lower()


def test_llm_generate_untrained_model_errors(tmp_path):
    from ai_assistant.llm.service import LlmService

    g = LlmService(models_dir=tmp_path).generate("anything", name="missing")
    assert g["ok"] is False
    assert "not trained" in (g["error"] or "").lower()


def test_llm_export_dataset(tmp_path):
    from ai_assistant.llm.service import LlmService

    out = tmp_path / "pairs.jsonl"
    res = LlmService(models_dir=tmp_path).export_dataset(str(out))
    assert res["ok"] and res["count"] > 0
    assert out.is_file()
    assert out.read_text(encoding="utf-8").strip()


def test_llm_resolve_engine_falls_back_to_python():
    from ai_assistant.llm.engines import resolve_engine

    eng, used, did_fb = resolve_engine("nonexistent_engine_xyz", "python")
    assert used == "python"
    assert did_fb is True


# ── Local LLM backend ─────────────────────────────────────────────────────--
def test_local_llm_backend_registered():
    from ai_query.backends import AIBackendRegistry

    reg = AIBackendRegistry()
    assert "local-llm" in reg.list_all_names()
    assert reg.get("local-llm") is not None
    # The old capture-based RAG backend is gone.
    assert "local-rag" not in reg.list_all_names()


def test_local_llm_backend_unavailable_without_model(tmp_path, monkeypatch):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    b = LocalLlmBackend()
    assert b.check_availability(force=True) is False
    assert "train" in b.get_unavailable_reason().lower()
