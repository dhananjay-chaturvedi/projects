"""RAG hits with scores are captured by the agent and rendered in the UI."""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_augment_with_rag_records_hits(monkeypatch):
    from ai_query.agent import AIQueryAgent
    from ai_assistant.rag.retriever import RetrievalHit

    self_obj = types.SimpleNamespace(last_rag_hits=["stale"])
    self_obj._rag_enabled = lambda: True

    hits = [
        RetrievalHit("d1", "table", "public.users", "CREATE TABLE users ...", 0.91, {}),
        RetrievalHit("d2", "example", "q1", "SELECT * FROM users", 0.42, {}),
    ]

    class _Store:
        def __init__(self, *a, **k):
            pass

        def count(self, conn):
            return 2

    class _Retr:
        def __init__(self, *a, **k):
            pass

        def search(self, conn, q, k=8):
            return hits

        def format_context(self, hits):
            return "CONTEXT BLOCK"

    monkeypatch.setattr("ai_assistant.rag.vector_store.SqliteVectorStore", _Store)
    monkeypatch.setattr("ai_assistant.rag.retriever.RagRetriever", _Retr)
    monkeypatch.setattr("ai_assistant.rag.service.default_index_path", lambda: ":mem:")
    monkeypatch.setattr(
        "ai_assistant.rag.embeddings.get_embedder", lambda *a, **k: object())

    out = AIQueryAgent._augment_with_rag(
        self_obj,
        "ctx",
        "list users",
        "conn",
        None,
        analysis={"is_simple": False, "complexity_score": 1},
    )
    assert "CONTEXT BLOCK" in out
    assert len(self_obj.last_rag_hits) == 2
    assert self_obj.last_rag_hits[0]["score"] == 0.91
    assert self_obj.last_rag_hits[0]["ref"] == "public.users"


def test_augment_with_rag_resets_hits_when_disabled():
    from ai_query.agent import AIQueryAgent

    self_obj = types.SimpleNamespace(last_rag_hits=["stale"])
    self_obj._rag_enabled = lambda: False
    out = AIQueryAgent._augment_with_rag(self_obj, "ctx", "q", "conn", None)
    assert out == "ctx"
    assert self_obj.last_rag_hits == []


def test_ui_renders_rag_hits():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    written = {}

    class _Txt:
        def config(self, **k):
            pass

        def delete(self, *a):
            written["cleared"] = True

        def insert(self, idx, text):
            written["text"] = text

    self_obj = types.SimpleNamespace(ai_rag_text=_Txt())
    AIQueryUI._display_rag_hits(self_obj, [
        {"score": 0.88, "kind": "table", "ref": "public.users", "text": "T"},
    ])
    assert "0.88" in written["text"]
    assert "public.users" in written["text"]

    # Empty -> friendly placeholder, not a crash.
    AIQueryUI._display_rag_hits(self_obj, [])
    assert "No RAG context" in written["text"]


def test_rag_hits_wired():
    agent = (ROOT / "ai_query/agent.py").read_text()
    assert 'parsed_result["rag_hits"]' in agent
    assert "self.last_rag_hits = [h.to_dict() for h in hits]" in agent
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "_display_rag_hits" in ui and "RAG context" in ui
