"""
Optional reranking stage for the RAG retriever.

After hybrid fusion (RRF of vector + lexical), an optional reranker re-orders
the top-N candidates for higher precision:

* :class:`HeuristicReranker` — zero-dependency, always available. Rewards exact
  phrase containment and full query-token coverage of a hit's body, blended with
  the candidate's existing fusion score. This is the floor that ships offline.
* :class:`CrossEncoderReranker` — uses a ``sentence-transformers`` CrossEncoder
  (e.g. ``cross-encoder/ms-marco-MiniLM-L-6-v2``) to directly score
  ``(query, passage)`` relevance. Used only when the optional package and model
  are present; otherwise we fall back to the heuristic reranker.

A reranker exposes a single method::

    rerank(query: str, hits: list) -> list   # reordered, same objects
"""

from __future__ import annotations

from typing import Any

from ai_assistant.rag.embeddings import tokenize
from common.config_loader import console_print


class Reranker:
    name = "base"

    def rerank(self, query: str, hits: list[Any]) -> list[Any]:
        raise NotImplementedError


class HeuristicReranker(Reranker):
    """Dependency-free reranker: phrase + token-coverage signals."""

    name = "heuristic"

    def rerank(self, query: str, hits: list[Any]) -> list[Any]:
        if not hits:
            return hits
        q_tokens = set(tokenize(query))
        q_lower = (query or "").strip().lower()
        if not q_tokens:
            return hits
        base = [h.score for h in hits]
        lo, hi = min(base), max(base)
        span = (hi - lo) or 1.0
        rescored: list[tuple[float, int, Any]] = []
        for idx, h in enumerate(hits):
            text = (h.text or "")
            body_tokens = set(tokenize(text[:2000]))
            coverage = len(q_tokens & body_tokens) / len(q_tokens)
            phrase = 1.0 if q_lower and q_lower in text.lower() else 0.0
            ref_tokens = set(tokenize(getattr(h, "ref", "") or ""))
            ref_cov = len(q_tokens & ref_tokens) / len(q_tokens)
            norm_fusion = (h.score - lo) / span
            rel = 0.30 * norm_fusion + 0.40 * coverage + 0.15 * ref_cov + 0.15 * phrase
            # Stash the reranker relevance for transparency in previews.
            try:
                if isinstance(h.metadata, dict):
                    h.metadata = {**h.metadata, "rerank": round(rel, 4)}
            except Exception:  # noqa: BLE001
                pass
            rescored.append((rel, idx, h))
        rescored.sort(key=lambda t: (-t[0], t[1]))
        return [h for _, _, h in rescored]


class CrossEncoderReranker(Reranker):
    """CrossEncoder reranker (optional ``sentence-transformers`` dependency)."""

    name = "cross-encoder"

    def __init__(self, model_name: str):
        from sentence_transformers import CrossEncoder  # type: ignore

        self._model = CrossEncoder(model_name)
        self.model_name = model_name

    def rerank(self, query: str, hits: list[Any]) -> list[Any]:
        if not hits:
            return hits
        pairs = [(query, (h.text or "")[:2000]) for h in hits]
        scores = self._model.predict(pairs)
        scored = list(zip(scores, range(len(hits)), hits))
        for sc, _, h in scored:
            try:
                if isinstance(h.metadata, dict):
                    h.metadata = {**h.metadata, "rerank": round(float(sc), 4)}
            except Exception:  # noqa: BLE001
                pass
        scored.sort(key=lambda t: (-float(t[0]), t[1]))
        return [h for _, _, h in scored]


def get_reranker(model: str = "") -> Reranker:
    """Return a CrossEncoder reranker when available, else the heuristic one."""
    if model:
        try:
            return CrossEncoderReranker(model)
        except Exception as exc:  # noqa: BLE001
            console_print(
                f"[RAG] cross-encoder reranker unavailable ({exc}); "
                f"using heuristic reranker."
            )
    return HeuristicReranker()
