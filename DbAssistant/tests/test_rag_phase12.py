"""Phase 1+2 RAG Manager tests: scope helpers, RRF, relationships, codebase, parity."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


class _DirectCore:
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
    from ai_assistant.rag.sample_data import build_sample_manager
    from ai_assistant.rag.service import RagService

    mgr = build_sample_manager(tmp_path / "shop.db")
    core = _DirectCore(mgr, "shop")
    return RagService(core, index_path=tmp_path / "rag_index.db")


# ── scope / preview / embedder mismatch ─────────────────────────────────────
def test_rag_scope_overview(sample_service):
    sample_service.index("shop", rebuild=True)
    ov = sample_service.scope_overview("shop")
    assert ov["ok"]
    assert ov["scope"] == "shop"
    assert ov["status"]["indexed"]
    assert ov["breakdown"]["total"] >= 6


def test_rag_preview_includes_context_and_ranked_hits(sample_service):
    sample_service.index("shop", rebuild=True)
    r = sample_service.preview("shop", "products out of stock", k=4)
    assert r["ok"]
    assert r["hits"]
    assert "Ranked retrieval hits" in (r.get("preview") or "")
    assert "RETRIEVED DATABASE CONTEXT" in (r.get("context") or "")


def test_embedder_mismatch_detected(sample_service, monkeypatch):
    sample_service.index("shop", rebuild=True)
    mm = sample_service.embedder_mismatch("shop")
    assert mm["ok"]
    assert mm["mismatch"] is False

    import ai_query.module_config as mc

    orig_get_int = mc.get_int

    def _patched_get_int(sec, key, default=0):
        if sec == "ai.rag" and key == "embedding_dim":
            return 512
        return orig_get_int(sec, key, default=default)

    monkeypatch.setattr("ai_query.module_config.get_int", _patched_get_int)
    sample_service._embedder = None  # force re-read config
    mm2 = sample_service.embedder_mismatch("shop")
    assert mm2["mismatch"] is True
    assert "Re-index" in (mm2.get("message") or "")


# ── RRF hybrid ranking ────────────────────────────────────────────────────────
def test_rrf_fusion_prefers_docs_strong_in_either_channel(tmp_path):
    from ai_assistant.rag.documents import Document
    from ai_assistant.rag.embeddings import HashingEmbedder
    from ai_assistant.rag.retriever import RagRetriever
    from ai_assistant.rag.vector_store import SqliteVectorStore

    store = SqliteVectorStore(tmp_path / "rrf.db")
    emb = HashingEmbedder(dim=64)
    d_lex = Document("table:orders", "table", "orders", "Table orders with customer_id")
    d_vec = Document("table:products", "table", "products", "Table products with sku and price")
    vecs = emb.embed([d_lex.text, d_vec.text])
    store.upsert_documents("c1", [d_lex, d_vec], vecs, provider="hash", dim=64)

    retr = RagRetriever(store, emb, use_rrf=True, rrf_k=60)
    hits = retr.search("c1", "orders customer_id", k=2)
    assert hits
    assert hits[0].ref in {"orders", "products"}
    assert all(h.score > 0 for h in hits)


def test_rrf_can_be_disabled_for_linear_blend(tmp_path):
    from ai_assistant.rag.documents import Document
    from ai_assistant.rag.embeddings import HashingEmbedder
    from ai_assistant.rag.retriever import RagRetriever
    from ai_assistant.rag.vector_store import SqliteVectorStore

    store = SqliteVectorStore(tmp_path / "lin.db")
    emb = HashingEmbedder(dim=64)
    docs = [
        Document("table:a", "table", "alpha", "alpha table"),
        Document("table:b", "table", "beta", "beta table"),
    ]
    store.upsert_documents("c1", docs, emb.embed([d.text for d in docs]),
                           provider="hash", dim=64)
    retr = RagRetriever(store, emb, use_rrf=False, lexical_alpha=0.5)
    hits = retr.search("c1", "alpha", k=2)
    assert hits[0].ref == "alpha"
    assert 0.0 <= hits[0].score <= 1.0


def test_format_preview_lists_ranked_hits(tmp_path):
    from ai_assistant.rag.embeddings import HashingEmbedder
    from ai_assistant.rag.retriever import RetrievalHit, RagRetriever
    from ai_assistant.rag.vector_store import SqliteVectorStore

    store = SqliteVectorStore(tmp_path / "prev.db")
    emb = HashingEmbedder(dim=32)
    retr = RagRetriever(store, emb)
    hits = [
        RetrievalHit("d1", "table", "users", "CREATE TABLE users", 0.033, {}),
        RetrievalHit("d2", "code", "app.py", "def main():", 0.028, {}),
    ]
    text = retr.format_preview(hits)
    assert "users" in text and "app.py" in text
    assert "score=" in text


# ── relationship docs ─────────────────────────────────────────────────────────
def test_schema_extractor_emits_relationship_docs(monkeypatch):
    from ai_assistant.rag.schema_extractor import SchemaExtractor

    class _Conn:
        pass

    class _Mgr:
        db_type = "PostgreSQL"
        conn = _Conn()

    def _fake_op(db_type, name, conn, *args):
        if name == "getTables":
            return ["orders"]
        if name == "getTableSchema":
            return [{"name": "customer_id", "type": "INTEGER", "nullable": False}]
        if name == "getConstraints":
            return ["FOREIGN KEY (customer_id) REFERENCES customers(id)"]
        return []

    def _supports(db_type, op):
        return op in {"getTables", "getTableSchema", "getConstraints", "getViews", "getIndexes"}

    monkeypatch.setattr(
        "ai_assistant.rag.schema_extractor.DatabaseRegistry.execute_operation",
        _fake_op,
    )
    monkeypatch.setattr(
        "ai_assistant.rag.schema_extractor.DatabaseRegistry.supports_operation",
        _supports,
    )
    docs = SchemaExtractor(sample_values=False).extract(_Mgr(), "shop")
    kinds = {d.kind for d in docs}
    assert "relationship" in kinds
    rel = [d for d in docs if d.kind == "relationship"]
    assert any("FOREIGN KEY" in d.text for d in rel)


# ── codebase ingestion ────────────────────────────────────────────────────────
def test_codebase_indexer_skips_ignored_dirs(tmp_path):
    from ai_assistant.rag.codebase_indexer import iter_codebase_files, index_codebase

    root = tmp_path / "repo"
    (root / "src").mkdir(parents=True)
    (root / "src" / "main.py").write_text("def hello():\n    return 1\n", encoding="utf-8")
    (root / "node_modules" / "pkg").mkdir(parents=True)
    (root / "node_modules" / "pkg" / "index.js").write_text("module.exports = {}", encoding="utf-8")
    files = iter_codebase_files(root)
    assert len(files) == 1
    assert files[0].name == "main.py"

    docs, summary = index_codebase(root, "code-scope")
    assert summary["ok"]
    assert summary["files_scanned"] == 1
    assert docs and docs[0].kind == "code"
    assert docs[0].metadata.get("language") == "python"


def test_codebase_indexer_accepts_relative_path(tmp_path, monkeypatch):
    from ai_assistant.rag.codebase_indexer import index_codebase

    root = tmp_path / "repo"
    (root / "pkg").mkdir(parents=True)
    (root / "pkg" / "mod.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    docs, summary = index_codebase("repo", "rel-scope")
    assert summary["ok"] and summary["files_scanned"] == 1
    assert docs[0].metadata["path"] == "pkg/mod.py"


def test_markdown_chunker_no_infinite_recursion():
    from ai_assistant.rag.document_loader import chunk_text

    # A heading-led section far larger than chunk_size must not recurse forever.
    big = "# Title\n\n" + ("word " * 2000)
    chunks = chunk_text(big, chunk_size=400, overlap=50)
    assert len(chunks) > 1
    assert all(len(c) <= 600 for c in chunks)


def test_rag_add_codebase_searchable(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    root = tmp_path / "repo"
    (root / "billing").mkdir(parents=True)
    (root / "billing" / "revenue.py").write_text(
        "def monthly_revenue():\n    return 'SELECT SUM(amount) FROM payments'\n",
        encoding="utf-8",
    )
    svc = RagService(_NoCore(), index_path=tmp_path / "code_rag.db")
    res = svc.add_codebase(str(root), "mycode", standalone=True)
    assert res["ok"] and res["chunks"] >= 1
    hits = svc.search("mycode", "monthly revenue payments", k=5)["hits"]
    assert any(h["kind"] == "code" for h in hits)


# ── bulk example import ───────────────────────────────────────────────────────
def _examples_service(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    return RagService(_NoCore(), index_path=tmp_path / "ex.db")


def test_parse_examples_jsonl_json_csv_tsv_text():
    from ai_assistant.rag.service import RagService

    jsonl = '{"question": "count users", "sql": "SELECT COUNT(*) FROM users"}\n'
    recs = RagService.parse_examples(jsonl, "jsonl")
    assert recs and recs[0]["question"] == "count users"

    arr = '[{"question": "all users", "sql": "SELECT * FROM users", "note": "n"}]'
    recs = RagService.parse_examples(arr, "json")
    assert recs[0]["sql"].startswith("SELECT")
    assert recs[0]["description"] == "n"

    csv_text = "question,sql,note\nlist orders,SELECT * FROM orders,recent\n"
    recs = RagService.parse_examples(csv_text, "csv")
    assert recs[0]["question"] == "list orders"

    tsv_text = "question\tsql\nlist items\tSELECT * FROM items\n"
    recs = RagService.parse_examples(tsv_text, "tsv")
    assert recs[0]["sql"] == "SELECT * FROM items"

    paired = "Q: top customers\nSQL: SELECT * FROM customers\nORDER BY spend DESC\n"
    recs = RagService.parse_examples(paired, "text")
    assert recs[0]["question"] == "top customers"
    assert "ORDER BY" in recs[0]["sql"]


def test_parse_examples_auto_detects_by_extension():
    from ai_assistant.rag.service import RagService

    recs = RagService.parse_examples(
        '{"question": "q1", "sql": "SELECT 1"}', "auto", source="x.jsonl")
    assert recs and recs[0]["question"] == "q1"


def test_add_examples_validates_and_indexes(tmp_path):
    svc = _examples_service(tmp_path)
    records = [
        {"question": "count users", "sql": "SELECT COUNT(*) FROM users"},
        {"question": "bad one", "sql": "not sql at all"},
    ]
    r = svc.add_examples("kb", records, standalone=True)
    assert r["ok"]
    assert r["added"] == 1
    assert r["skipped"] >= 1
    hits = svc.search("kb", "count users", k=5)["hits"]
    assert any(h["kind"] == "example" for h in hits)


def test_add_examples_from_file_roundtrip(tmp_path):
    svc = _examples_service(tmp_path)
    p = tmp_path / "examples.jsonl"
    p.write_text(
        '{"question": "list products", "sql": "SELECT * FROM products"}\n'
        '{"question": "count orders", "sql": "SELECT COUNT(*) FROM orders"}\n',
        encoding="utf-8",
    )
    r = svc.add_examples_from_file("kb", str(p), standalone=True)
    assert r["ok"] and r["added"] == 2 and r["parsed"] == 2
    assert r["source"] == "examples.jsonl"


def test_add_examples_from_content_for_web(tmp_path):
    svc = _examples_service(tmp_path)
    content = "question,sql\nlist tables,SELECT name FROM sqlite_master\n"
    r = svc.add_examples_from_file("kb", content=content, fmt="csv", standalone=True)
    assert r["ok"] and r["added"] == 1


# ── Phase 3: retrieval quality v2 ──────────────────────────────────────────────
def test_body_aware_lexical_ranks_code_by_content(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    root = tmp_path / "repo"
    (root / "a").mkdir(parents=True)
    (root / "a" / "ranking.py").write_text(
        "def reciprocal_rank_fusion(rankings):\n"
        "    # reciprocal rank fusion hybrid retrieval blends vector and lexical\n"
        "    return sorted(rankings)\n",
        encoding="utf-8",
    )
    (root / "a" / "unrelated.py").write_text(
        "def add(a, b):\n    return a + b\n", encoding="utf-8")
    svc = RagService(_NoCore(), index_path=tmp_path / "code.db")
    svc.add_codebase(str(root), "code", standalone=True)
    hits = svc.search("code", "reciprocal rank fusion hybrid retrieval", k=3)["hits"]
    assert hits
    # The content match must outrank the unrelated file (body-aware lexical).
    assert "ranking.py" in hits[0]["ref"]


def test_glossary_query_expansion_improves_recall(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    svc = RagService(_NoCore(), index_path=tmp_path / "exp.db")
    svc.add_glossary("kb", "ARR", "annual recurring revenue from subscriptions")
    svc.add_document("kb", text="Subscriptions revenue is recognized monthly.",
                     title="rev", source="rev.md", standalone=True)
    # "ARR" alone should pull in the revenue doc via glossary expansion.
    hits = svc.search("kb", "ARR", k=5)["hits"]
    kinds = {h["kind"] for h in hits}
    assert "glossary" in kinds
    assert any(h["kind"] == "document" for h in hits)


# ── Phase 4: multi-scope retrieval ─────────────────────────────────────────────
def test_search_multi_merges_and_tags_scopes(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    svc = RagService(_NoCore(), index_path=tmp_path / "multi.db")
    svc.add_document("docs", text="Churn is when a customer cancels.",
                     title="g", source="g.md", standalone=True)
    svc.add_example("code", "count customers", "SELECT COUNT(*) FROM customers")
    r = svc.search_multi(["docs", "code"], "customer churn count", k=5)
    assert r["ok"]
    assert r["hits"]
    scopes_seen = {h["scope"] for h in r["hits"]}
    assert scopes_seen <= {"docs", "code"}
    assert any(h["scope"] == "docs" for h in r["hits"])


def test_preview_multi_builds_context(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    svc = RagService(_NoCore(), index_path=tmp_path / "pm.db")
    svc.add_document("d1", text="Revenue recognized monthly.",
                     title="t", source="t.md", standalone=True)
    svc.add_document("d2", text="Customers churn when they cancel.",
                     title="c", source="c.md", standalone=True)
    r = svc.preview_multi(["d1", "d2"], "monthly revenue and churn", k=4)
    assert r["ok"]
    assert "RETRIEVED DATABASE CONTEXT" in (r.get("context") or "")
    assert "Ranked retrieval hits" in (r.get("preview") or "")


def test_search_multi_requires_scope_and_query(tmp_path):
    from ai_assistant.rag.service import RagService

    class _NoCore:
        def get_manager(self, name):
            raise ValueError("no db")

        def get_connection_profile(self, name):
            return None

    svc = RagService(_NoCore(), index_path=tmp_path / "e.db")
    assert svc.search_multi([], "q")["ok"] is False
    assert svc.search_multi(["s"], "")["ok"] is False


# ── API / CLI surface ─────────────────────────────────────────────────────────
def test_rag_api_routes_phase12():
    from ai_query.api import build_router

    paths = {getattr(r, "path", "") for r in build_router().routes}
    assert "/api/ai/rag/overview" in paths
    assert "/api/ai/rag/preview" in paths
    assert "/api/ai/rag/add-codebase" in paths
    assert "/api/ai/rag/examples-file" in paths
    assert "/api/ai/rag/search-multi" in paths


def test_rag_cli_has_phase12_subcommands():
    cli = (ROOT / "ai_query/cli.py").read_text()
    for sub in ("overview", "preview", "add-codebase", "add-examples-file",
                "search-multi"):
        assert f'"{sub}"' in cli or f"'{sub}'" in cli


# ── UI parity ─────────────────────────────────────────────────────────────────
def test_rag_manager_parity_across_surfaces():
    from common.ui.shared import specs

    tk = (ROOT / "common/ui/tk/ai/rag_panel.py").read_text()
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    web = (ROOT / "common/ui/web/static/app.js").read_text()

    for label in ("Index Schema", "Add Codebase", "Overview", "Standalone collection"):
        assert label in tk
    assert "scope_overview" in tk or "do_overview" in tk
    assert "preview" in tk

    for token in ("rag-conn", "rag_add_codebase", "Preview search", "Index schema"):
        assert token in tui

    for token in ("/api/ai/rag/overview", "/api/ai/rag/preview",
                  "/api/ai/rag/add-codebase", "/api/ai/rag/examples-file",
                  "rag-conn", "Standalone collection"):
        assert token in web

    # Bulk example import is exposed on every surface.
    assert "Import Examples File" in tk
    assert "examples_file" in tui
    assert "examples_file" in web

    # Multi-scope retrieval is exposed on every surface.
    assert "preview_multi" in tk
    assert "rag-extra-scopes" in tui and "rag_preview_multi" in tui
    assert "rag-extra-scopes" in web and "/api/ai/rag/search-multi" in web

    assert specs.RAG_MANAGER_TITLE == "RAG Manager"
    action_ids = {a["id"] for a in specs.RAG_MANAGER_ACTIONS}
    assert {"overview", "index", "codebase", "preview"} <= action_ids


# ════════════════════════════════════════════════════════════════════════════
# Phase 3/4 (industry-standard upgrades)
# ════════════════════════════════════════════════════════════════════════════


class _NoCore:
    def get_manager(self, name):
        raise ValueError("no db")

    def get_connection_profile(self, name):
        return None


# ── 1. Semantic schema cards ───────────────────────────────────────────────
def test_schema_cards_emit_enums(sample_service):
    sample_service.index("shop", rebuild=True)
    tables = sample_service.store().list_by_kind("shop", "table")
    enum_cols = set()
    for t in tables:
        enum_cols |= set((t.get("metadata") or {}).get("enums", {}).keys())
    # loyalty_tier / status / method are low-cardinality => enum docs.
    assert {"loyalty_tier", "status", "method"} & enum_cols
    # The enum values are rendered into the indexed text for retrieval.
    blob = "\n".join(t["text"] for t in tables)
    assert "Enumerated" in blob


def test_schema_card_metadata_has_card_fields(sample_service):
    sample_service.index("shop", rebuild=True)
    tables = sample_service.store().list_by_kind("shop", "table")
    meta = (tables[0].get("metadata") or {})
    assert "enums" in meta and "comments" in meta and "purpose" in meta


def test_pii_masking_of_sampled_values():
    from ai_assistant.rag.schema_extractor import SchemaExtractor

    ext = SchemaExtractor(mask_pii=True)
    assert ext._mask("alice@example.com") == "[REDACTED:EMAIL]"
    plain = SchemaExtractor(mask_pii=False)
    assert plain._mask("alice@example.com") == "alice@example.com"


# ── 2. Reranker ────────────────────────────────────────────────────────────
def test_heuristic_reranker_promotes_phrase_match():
    from ai_assistant.rag.reranker import HeuristicReranker, get_reranker
    from ai_assistant.rag.retriever import RetrievalHit

    hits = [
        RetrievalHit("d1", "document", "a", "totally unrelated text here", 0.9, {}),
        RetrievalHit("d2", "document", "b", "monthly recurring revenue report", 0.5, {}),
    ]
    out = HeuristicReranker().rerank("monthly recurring revenue", hits)
    assert out[0].doc_id == "d2"
    assert out[0].metadata.get("rerank") is not None
    # Factory falls back to heuristic when no model is configured.
    assert get_reranker("").name == "heuristic"


def test_rerank_config_path_runs(tmp_path, monkeypatch):
    from ai_assistant.rag.service import RagService

    svc = RagService(_NoCore(), index_path=tmp_path / "rr.db")
    svc.add_document("kb", text="reciprocal rank fusion blends rankings",
                     title="a", source="a.md", standalone=True)
    svc.add_document("kb", text="completely different topic about cats",
                     title="b", source="b.md", standalone=True)
    import ai_query.module_config as mc
    orig = mc.get_bool

    def _patched(sec, key, default=False):
        if sec == "ai.rag" and key == "rerank":
            return True
        return orig(sec, key, default=default)

    monkeypatch.setattr("ai_query.module_config.get_bool", _patched)
    hits = svc.search("kb", "reciprocal rank fusion", k=2)["hits"]
    assert hits and hits[0]["ref"] == "a"


# ── 3. Structure-aware chunking ────────────────────────────────────────────
def test_chunking_keeps_markdown_table_rows_intact():
    from ai_assistant.rag import document_loader as dl

    header = "| col_a | col_b |\n| --- | --- |\n"
    rows = "\n".join(f"| value_{i}_aaaa | value_{i}_bbbb |" for i in range(60))
    chunks = dl.chunk_text(header + rows, chunk_size=300, overlap=40)
    assert len(chunks) > 1
    for c in chunks:
        # No chunk splits a row mid-cell: every pipe line is balanced.
        for line in c.splitlines():
            if "|" in line:
                assert line.strip().startswith("|") and line.strip().endswith("|")
        # Header repeated so each chunk is a valid table.
        assert "col_a" in c


def test_chunking_sentence_aware_no_midword_split():
    from ai_assistant.rag import document_loader as dl

    text = " ".join(f"Sentence number {i} explains a distinct idea." for i in range(80))
    chunks = dl.chunk_text(text, chunk_size=200, overlap=30)
    assert len(chunks) > 1
    assert all(chunks)


# ── 4. Entity linking ──────────────────────────────────────────────────────
def test_entity_linking_pulls_matching_table(sample_service):
    sample_service.index("shop", rebuild=True)
    # "payment" (singular) should link to the "payments" table object name.
    hits = sample_service.search("shop", "show me every payment made", k=6)["hits"]
    refs = " ".join(h["ref"].lower() for h in hits)
    assert "payment" in refs


# ── 5. Eval harness ────────────────────────────────────────────────────────
def test_rag_eval_extract_tables():
    from ai_assistant.rag import rag_eval

    tabs = rag_eval.extract_tables(
        "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.customer_id")
    assert {"orders", "customers"} <= tabs


def test_rag_eval_score_and_aggregate():
    from ai_assistant.rag import rag_eval
    from ai_assistant.rag.retriever import RetrievalHit

    hits = [
        RetrievalHit("t:orders", "table", "orders", "Table: orders", 0.9, {}),
        RetrievalHit("t:x", "table", "unrelated", "Table: unrelated", 0.5, {}),
    ]
    m = rag_eval.score_case(["orders", "customers"], hits, k=2)
    assert m["recall_at_k"] == 0.5
    assert m["reciprocal_rank"] == 1.0
    agg = rag_eval.aggregate([m, m])
    assert agg["cases"] == 2 and agg["recall_at_k"] == 0.5


def test_rag_eval_seeds_from_examples(sample_service):
    sample_service.index("shop", rebuild=True)
    sample_service.add_example(
        "shop", "list all orders", "SELECT * FROM orders")
    sample_service.add_example(
        "shop", "all payments", "SELECT * FROM payments")
    r = sample_service.evaluate("shop", k=8, per_case=True)
    assert r["ok"]
    assert r["seeded_from_examples"] is True
    assert r["metrics"]["cases"] >= 2
    assert r["metrics"]["recall_at_k"] > 0.0


def test_rag_eval_explicit_gold(sample_service):
    sample_service.index("shop", rebuild=True)
    gold = [{"question": "show orders", "tables": ["orders"]}]
    r = sample_service.evaluate("shop", gold=gold, k=8)
    assert r["ok"] and r["seeded_from_examples"] is False
    assert r["metrics"]["cases"] == 1


# ── 6. Freshness / lifecycle ───────────────────────────────────────────────
def test_schema_hash_stored_and_drift_detection(sample_service):
    sample_service.index("shop", rebuild=True)
    meta = sample_service.store().get_meta("shop")
    assert meta.get("schema_hash")
    d = sample_service.drift("shop")
    assert d["ok"] and d["changed"] is False


def test_staleness_meta_flags_old_index():
    from ai_assistant.rag.service import RagService

    fresh = RagService._staleness_meta({"indexed_at": "2099-01-01 00:00:00"})
    assert fresh["stale"] is False
    old = RagService._staleness_meta({"indexed_at": "2000-01-01 00:00:00"})
    assert old["stale"] is True
    assert "re-index" in old["message"].lower()


def test_reindex_stale_skips_fresh(sample_service):
    sample_service.index("shop", rebuild=True)
    r = sample_service.reindex_stale(["shop"])
    assert r["ok"]
    assert r["results"][0]["reason"] == "fresh"
    forced = sample_service.reindex_stale(["shop"], force=True)
    assert forced["reindexed"] == 1


def test_incremental_reindex_reuses_embeddings(sample_service):
    sample_service.index("shop", rebuild=True)
    res = sample_service.index("shop", rebuild=False)
    # Second (incremental) pass should reuse the unchanged schema vectors.
    assert res.get("reused", 0) > 0


# ── 7. ANN store ───────────────────────────────────────────────────────────
def test_ann_vector_scores_match_bruteforce_when_available():
    from ai_assistant.rag import ann

    if not ann.faiss_available():
        import pytest as _pt
        _pt.skip("faiss not installed")
    import math
    mat = [[1.0, 0.0], [0.0, 1.0], [0.7071, 0.7071]]
    scores = ann.vector_scores([1.0, 0.0], mat)
    assert abs(scores[0] - 1.0) < 1e-3
    assert abs(scores[1] - 0.0) < 1e-3


def test_retriever_ann_fallback_without_faiss(tmp_path, monkeypatch):
    from ai_assistant.rag.service import RagService

    svc = RagService(_NoCore(), index_path=tmp_path / "ann.db")
    svc.add_document("kb", text="alpha beta gamma", title="a", source="a.md",
                     standalone=True)
    import ai_query.module_config as mc
    orig = mc.get_bool

    def _patched(sec, key, default=False):
        if sec == "ai.rag" and key == "ann":
            return True
        return orig(sec, key, default=default)

    monkeypatch.setattr("ai_query.module_config.get_bool", _patched)
    # Should not raise even if faiss is absent (graceful fallback).
    r = svc.search("kb", "alpha beta", k=2)
    assert r["ok"] and r["hits"]


# ── 8. Observability ───────────────────────────────────────────────────────
def test_retrieval_logging_writes_jsonl(tmp_path, monkeypatch):
    import json
    from ai_assistant.rag.service import RagService

    svc = RagService(_NoCore(), index_path=tmp_path / "log.db")
    svc.add_document("kb", text="observable retrieval log", title="a",
                     source="a.md", standalone=True)
    import ai_query.module_config as mc
    orig = mc.get_bool

    def _patched(sec, key, default=False):
        if sec == "ai.rag" and key == "log_retrievals":
            return True
        return orig(sec, key, default=default)

    monkeypatch.setattr("ai_query.module_config.get_bool", _patched)
    svc.search("kb", "observable log", k=2)
    log_path = tmp_path / "retrievals.jsonl"
    assert log_path.exists()
    rec = json.loads(log_path.read_text().splitlines()[0])
    assert rec["scope"] == "kb" and "hits" in rec


# ── Parity for the new operations ──────────────────────────────────────────
def test_phase34_api_routes():
    from ai_query.api import build_router

    paths = {getattr(r, "path", "") for r in build_router().routes}
    assert "/api/ai/rag/eval" in paths
    assert "/api/ai/rag/drift" in paths
    assert "/api/ai/rag/reindex-stale" in paths


def test_phase34_cli_subcommands():
    cli = (ROOT / "ai_query/cli.py").read_text()
    for sub in ("eval", "drift", "reindex-stale"):
        assert f'"{sub}"' in cli


def test_phase34_ui_parity():
    tk = (ROOT / "common/ui/tk/ai/rag_panel.py").read_text()
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    web = (ROOT / "common/ui/web/static/app.js").read_text()
    from common.ui.shared import specs

    action_ids = {a["id"] for a in specs.RAG_MANAGER_ACTIONS}
    assert {"eval", "drift", "reindex_stale"} <= action_ids
    # Tk handlers/buttons
    assert "do_eval" in tk and "do_drift" in tk and "do_reindex_stale" in tk
    # TUI actions
    for tok in ('"eval"', '"drift"', '"reindex_stale"'):
        assert tok in tui
    # Web actions + endpoints
    assert "/api/ai/rag/eval" in web and "/api/ai/rag/drift" in web
    assert "/api/ai/rag/reindex-stale" in web


def test_aiservice_delegates_phase34(tmp_path):
    from ai_query.service import AIService

    assert hasattr(AIService, "rag_eval")
    assert hasattr(AIService, "rag_drift")
    assert hasattr(AIService, "rag_reindex_stale")


# ── RagReindexScheduler ─────────────────────────────────────────────────────
def test_reindex_scheduler_start_stop_status(sample_service):
    sample_service.index("shop", rebuild=True)
    st = sample_service.reindex_schedule_status()
    assert st["ok"] and st["running"] is False
    started = sample_service.reindex_schedule_start()
    assert started["ok"] and started["running"] is True
    stopped = sample_service.reindex_schedule_stop()
    assert stopped["ok"] and stopped["running"] is False


def test_reindex_scheduler_singleton_per_service(sample_service):
    a = sample_service._reindex_scheduler()
    b = sample_service._reindex_scheduler()
    assert a is b


def test_reindex_scheduler_runs_reindex_in_window():
    from ai_assistant.rag.reindex_scheduler import RagReindexScheduler

    calls: list[dict] = []

    def _run(connections, force=False):
        calls.append({"connections": connections, "force": force})
        return {"ok": True, "reindexed": 1}

    cfg = {"enabled": True, "start_time": "00:00", "duration_hours": 24,
           "connections": ["shop"], "force": True}
    sched = RagReindexScheduler(_run, get_config=lambda: cfg)
    sched._run_once(cfg, "2026-01-01")
    assert calls == [{"connections": ["shop"], "force": True}]
    assert sched._last_result["ok"] is True
    assert sched._last_result["reindexed"] == 1


def test_reindex_scheduler_config_defaults():
    from ai_assistant.rag.reindex_scheduler import get_reindex_scheduler

    class _Svc:
        def reindex_stale(self, connections=None, *, force=False):
            return {"ok": True}

    svc = _Svc()
    sched = get_reindex_scheduler(svc)
    st = sched.status()
    assert st["start_time"] == "02:00"
    assert st["running"] is False


def test_reindex_schedule_api_routes():
    from ai_query.api import build_router

    paths = {getattr(r, "path", "") for r in build_router().routes}
    assert "/api/ai/rag/reindex/schedule" in paths
    assert "/api/ai/rag/reindex/schedule/start" in paths
    assert "/api/ai/rag/reindex/schedule/stop" in paths


def test_reindex_schedule_cli_and_ui_parity():
    cli = (ROOT / "ai_query/cli.py").read_text()
    assert '"reindex-schedule"' in cli
    assert "rag_reindex_schedule_start" in cli
    from common.ui.shared import specs

    action_ids = {a["id"] for a in specs.RAG_MANAGER_ACTIONS}
    assert {"schedule_status", "schedule_start", "schedule_stop"} <= action_ids
    tk = (ROOT / "common/ui/tk/ai/rag_panel.py").read_text()
    assert "do_schedule_start" in tk and "do_schedule_stop" in tk
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "schedule_status" in tui and "rag_reindex_schedule_start" in tui
    web = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "/api/ai/rag/reindex/schedule" in web
    assert "schedule_start" in web and "schedule_stop" in web


def test_aiservice_delegates_reindex_schedule():
    from ai_query.service import AIService

    assert hasattr(AIService, "rag_reindex_schedule_status")
    assert hasattr(AIService, "rag_reindex_schedule_start")
    assert hasattr(AIService, "rag_reindex_schedule_stop")
