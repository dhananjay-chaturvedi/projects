"""
RAG retriever: embed the question, score documents, return the most relevant.

Scoring supports two modes (config ``ai.rag use_rrf``):

* **RRF (default)** — reciprocal rank fusion of vector cosine and lexical
  rankings (industry-standard hybrid retrieval).
* **Linear blend** — ``(1 - alpha) * cosine + alpha * lexical`` (legacy).

The lexical term rewards exact token overlap with object/column names — critical
for schema retrieval.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ai_assistant.rag.embeddings import EmbeddingProvider, tokenize
from ai_assistant.rag.vector_store import SqliteVectorStore
from ai_query import module_config as mc


@dataclass
class RetrievalHit:
    doc_id: str
    kind: str
    ref: str
    text: str
    score: float
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "kind": self.kind,
            "ref": self.ref,
            "score": round(self.score, 4),
            "text": self.text,
            "metadata": self.metadata,
        }


def _dot(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    return sum(a[i] * b[i] for i in range(n))


def _rrf(rank: int, k: int = 60) -> float:
    return 1.0 / (k + rank + 1)


class RagRetriever:
    def __init__(
        self,
        store: SqliteVectorStore,
        embedder: EmbeddingProvider,
        *,
        lexical_alpha: float = 0.3,
        use_rrf: bool | None = None,
        rrf_k: int | None = None,
    ):
        self.store = store
        self.embedder = embedder
        self.lexical_alpha = max(0.0, min(1.0, lexical_alpha))
        if use_rrf is None:
            use_rrf = mc.get_bool("ai.rag", "use_rrf", default=True)
        self.use_rrf = bool(use_rrf)
        self.rrf_k = int(rrf_k if rrf_k is not None else mc.get_int("ai.rag", "rrf_k", default=60))

    def search(
        self, connection: str, query: str, k: int = 8
    ) -> list[RetrievalHit]:
        docs = self.store.load_documents(connection)
        if not docs:
            return []
        q_vec = self.embedder.embed_one(query)
        q_tokens = set(tokenize(query))
        lex_tokens = self._expand_query(q_tokens, docs)
        lex_tokens = self._link_entities(q_tokens, lex_tokens, docs)

        cosines = self._vector_scores(q_vec, docs)
        scored: list[tuple[Any, float, float, float]] = []
        for d, cos in zip(docs, cosines):
            cos_n = (cos + 1.0) / 2.0  # map [-1,1] -> [0,1]
            lex = self._lexical(lex_tokens, d, q_tokens=q_tokens)
            scored.append((d, cos_n, lex, 0.0))

        if self.use_rrf and len(scored) > 1:
            by_cos = sorted(scored, key=lambda t: t[1], reverse=True)
            by_lex = sorted(scored, key=lambda t: t[2], reverse=True)
            cos_rank = {id(t[0]): i for i, t in enumerate(by_cos)}
            lex_rank = {id(t[0]): i for i, t in enumerate(by_lex)}
            fused: list[RetrievalHit] = []
            for d, cos_n, lex, _ in scored:
                rrf_score = _rrf(cos_rank[id(d)], self.rrf_k) + _rrf(lex_rank[id(d)], self.rrf_k)
                fused.append(
                    RetrievalHit(d.doc_id, d.kind, d.ref, d.text, rrf_score, d.metadata)
                )
            fused.sort(key=lambda h: h.score, reverse=True)
            return self._maybe_rerank(query, fused, k)

        hits: list[RetrievalHit] = []
        for d, cos_n, lex, _ in scored:
            score = (1 - self.lexical_alpha) * cos_n + self.lexical_alpha * lex
            hits.append(RetrievalHit(d.doc_id, d.kind, d.ref, d.text, score, d.metadata))
        hits.sort(key=lambda h: h.score, reverse=True)
        return self._maybe_rerank(query, hits, k)

    def _vector_scores(self, q_vec: list[float], docs) -> list[float]:
        """Cosine of the query against every doc, via FAISS when enabled."""
        if mc.get_bool("ai.rag", "ann", default=False) and len(docs) > 1:
            try:
                from ai_assistant.rag import ann

                if ann.faiss_available():
                    return ann.vector_scores(q_vec, [d.embedding for d in docs])
            except Exception:  # noqa: BLE001
                pass
        return [_dot(q_vec, d.embedding) for d in docs]

    def _maybe_rerank(
        self, query: str, hits: list[RetrievalHit], k: int
    ) -> list[RetrievalHit]:
        """Optionally rerank the top-N candidates before truncating to *k*."""
        k = max(1, k)
        if not hits or not mc.get_bool("ai.rag", "rerank", default=False):
            return hits[:k]
        top_n = max(k, mc.get_int("ai.rag", "rerank_top_n", default=20))
        head, tail = hits[:top_n], hits[top_n:]
        try:
            from ai_assistant.rag.reranker import get_reranker

            model = mc.get("ai.rag", "rerank_model", default="").strip()
            reranked = get_reranker(model).rerank(query, head)
        except Exception:  # noqa: BLE001
            reranked = head
        return (reranked + tail)[:k]

    def _link_entities(
        self, q_tokens: set[str], lex_tokens: set[str], docs
    ) -> set[str]:
        """Map question nouns to indexed table/column names (entity linking).

        For every schema object name token, if a question token is a prefix or
        near-substring of it (or vice-versa), fold the object's name tokens into
        the lexical match set. This pulls the right tables/columns into context
        even when the user's wording is close but not identical — directly
        attacking the "model referenced a column that doesn't exist" failure.
        Gated by ``ai.rag entity_linking``.
        """
        if not q_tokens or not mc.get_bool("ai.rag", "entity_linking", default=True):
            return lex_tokens
        # Collect object-name tokens (table refs + column names) once.
        object_tokens: set[str] = set()
        for d in docs:
            if d.kind not in ("table", "view", "relationship", "index"):
                continue
            object_tokens |= set(tokenize(d.ref))
            for c in (d.metadata or {}).get("columns", []) or []:
                object_tokens |= set(tokenize(str(c)))
        if not object_tokens:
            return lex_tokens
        linked = set(lex_tokens)
        for qt in q_tokens:
            if len(qt) < 4:
                continue
            for ot in object_tokens:
                if ot in q_tokens:
                    continue
                if (qt == ot or qt.startswith(ot) or ot.startswith(qt)
                        or (len(qt) >= 5 and qt in ot)
                        or (len(ot) >= 5 and ot in qt)):
                    linked.add(ot)
        return linked

    # Kinds whose body text carries the signal (no column metadata to lean on).
    _BODY_KINDS = frozenset({"code", "document", "example", "analytical", "glossary"})

    def _expand_query(self, q_tokens: set[str], docs) -> set[str]:
        """Expand query tokens with definitions of matching glossary terms.

        If the question mentions a business term that's indexed in the glossary,
        fold the term's definition tokens into the lexical match set so rows that
        speak the underlying schema language are still retrieved. Vector scoring
        keeps using the original query. Gated by ``ai.rag query_expansion``.
        """
        if not q_tokens or not mc.get_bool("ai.rag", "query_expansion", default=True):
            return q_tokens
        expanded = set(q_tokens)
        for d in docs:
            if d.kind != "glossary":
                continue
            meta = d.metadata or {}
            term_tokens = set(tokenize(str(meta.get("term", ""))))
            if term_tokens and term_tokens & q_tokens:
                expanded |= set(tokenize(str(meta.get("definition", ""))))
        return expanded

    def _lexical(self, match_tokens: set[str], doc, *,
                 q_tokens: set[str] | None = None) -> float:
        """Lexical overlap score. *match_tokens* may include glossary-expanded
        tokens; *q_tokens* (raw question tokens) drives the exact-name boost."""
        raw = q_tokens if q_tokens is not None else match_tokens
        if not match_tokens:
            return 0.0
        ref_tokens = set(tokenize(doc.ref))
        col_tokens: set[str] = set()
        for c in (doc.metadata or {}).get("columns", []) or []:
            col_tokens |= set(tokenize(str(c)))
        name_tokens = ref_tokens | col_tokens

        # Body tokens always contribute, but weighted below name matches. This is
        # essential for code/document chunks whose ref is just a filename/title.
        body_tokens = set(tokenize((doc.text or "")[:2000]))
        if not (name_tokens or body_tokens):
            return 0.0

        denom = max(1, len(match_tokens))
        name_overlap = len(match_tokens & name_tokens) / denom
        body_overlap = len(match_tokens & body_tokens) / denom
        if name_overlap == 0 and body_overlap == 0:
            return 0.0

        # Body-dominant kinds lean harder on body overlap; schema objects keep
        # name precision so exact table/column hits still win.
        if doc.kind in self._BODY_KINDS and not col_tokens:
            score = 0.45 * name_overlap + 0.55 * body_overlap
        else:
            score = 0.75 * name_overlap + 0.25 * body_overlap

        name_boost = 0.4 if (raw & ref_tokens) else 0.0
        return min(1.0, score + name_boost)

    # ------------------------------------------------------------------

    def format_context(self, hits: list[RetrievalHit]) -> str:
        """Render hits into a prompt-ready, grouped context block."""
        if not hits:
            return "(no relevant schema found in the RAG index)"

        order = {
            "table": 0, "view": 1, "relationship": 2, "index": 3,
            "glossary": 4, "example": 5, "analytical": 6, "document": 7, "code": 8,
        }
        hits = sorted(hits, key=lambda h: (order.get(h.kind, 9), -h.score))

        sections: dict[str, list[str]] = {}
        for h in hits:
            sections.setdefault(h.kind, []).append(h.text)

        titles = {
            "table": "RELEVANT TABLES",
            "view": "RELEVANT VIEWS",
            "relationship": "RELATIONSHIPS",
            "index": "INDEXES",
            "glossary": "BUSINESS GLOSSARY",
            "example": "SIMILAR EXAMPLE QUERIES",
            "analytical": "ANALYTICAL QUERY PATTERNS",
            "document": "REFERENCE DOCUMENTS",
            "code": "SOURCE CODE",
        }
        out = ["=== RETRIEVED DATABASE CONTEXT (RAG) ==="]
        for kind, blocks in sections.items():
            out.append(f"\n--- {titles.get(kind, kind.upper())} ---")
            out.extend(blocks)
        out.append("\n=== END RETRIEVED CONTEXT ===")
        return "\n".join(out)

    def format_preview(self, hits: list[RetrievalHit]) -> str:
        """Human-readable ranked hit list for the RAG Manager UI."""
        if not hits:
            return "(no matches)"
        lines = ["Ranked retrieval hits:", ""]
        for i, h in enumerate(hits, 1):
            scope = (h.metadata or {}).get("scope")
            scope_tag = f" {{{scope}}}" if scope else ""
            lines.append(f"  {i:>2}. score={h.score:.4f}  [{h.kind}]{scope_tag} {h.ref}")
            snippet = (h.text or "").replace("\n", " ")[:120]
            if snippet:
                lines.append(f"      {snippet}…")
        return "\n".join(lines)
