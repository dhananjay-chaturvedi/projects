"""
Embedding providers for the RAG pipeline.

The AI Query backends (Claude / Cursor / Codex CLIs) do *not* expose an
embeddings endpoint, so the vector representation has to be produced locally.
Two providers are offered:

* ``hash``  — a zero-dependency, deterministic hashing embedder (default).
              It bag-of-hashes word tokens + bigrams into a fixed-dim vector
              with sublinear term weighting and L2 normalisation. This gives
              robust lexical/semantic-ish matching, which is exactly what
              schema retrieval needs (object names + keywords dominate), and
              it works fully offline with no model download.

* ``sentence-transformers`` — higher-quality semantic embeddings via the
              optional ``sentence-transformers`` package. Used only when it is
              installed and selected in config; otherwise we fall back to
              ``hash`` so the feature never hard-fails.

Every provider implements:
    dim                       -> int
    embed(list[str])          -> list[list[float]]   (L2-normalised)
    embed_one(str)            -> list[float]
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import List

from common.config_loader import console_print

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def tokenize(text: str) -> list[str]:
    """Lowercase word tokens plus split on snake_case / camelCase boundaries.

    ``customer_id`` -> ``customer``, ``id``, ``customer_id``
    ``OrderDate``   -> ``order``, ``date``, ``orderdate``
    """
    raw = _TOKEN_RE.findall(text or "")
    out: list[str] = []
    for tok in raw:
        low = tok.lower()
        out.append(low)
        # snake_case parts
        if "_" in low:
            out.extend(p for p in low.split("_") if p)
        # camelCase parts
        camel = re.findall(r"[A-Z]?[a-z0-9]+", tok)
        if len(camel) > 1:
            out.extend(c.lower() for c in camel)
    return out


class EmbeddingProvider:
    """Abstract embedding provider."""

    name: str = "base"
    dim: int = 256

    def embed(self, texts: List[str]) -> List[List[float]]:
        raise NotImplementedError

    def embed_one(self, text: str) -> List[float]:
        return self.embed([text])[0]


class HashingEmbedder(EmbeddingProvider):
    """Deterministic, dependency-free hashing embedder (the default)."""

    name = "hash"

    def __init__(self, dim: int = 256):
        self.dim = max(32, int(dim))

    def _hash(self, token: str) -> tuple[int, int]:
        """Return (bucket, sign) for a token using a stable digest."""
        h = hashlib.md5(token.encode("utf-8")).digest()
        bucket = int.from_bytes(h[:4], "little") % self.dim
        sign = 1 if (h[4] & 1) else -1
        return bucket, sign

    def embed(self, texts: List[str]) -> List[List[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            vec = [0.0] * self.dim
            counts: dict[str, int] = {}
            tokens = tokenize(text)
            # word bigrams capture phrases like "order date", "total amount"
            bigrams = [
                f"{tokens[i]}_{tokens[i + 1]}" for i in range(len(tokens) - 1)
            ]
            for tok in tokens + bigrams:
                counts[tok] = counts.get(tok, 0) + 1
            for tok, cnt in counts.items():
                bucket, sign = self._hash(tok)
                # sublinear term weighting damps very frequent tokens
                vec[bucket] += sign * (1.0 + math.log(cnt))
            vectors.append(_l2_normalize(vec))
        return vectors


class SentenceTransformerEmbedder(EmbeddingProvider):
    """Optional high-quality embedder backed by ``sentence-transformers``."""

    name = "sentence-transformers"

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        from sentence_transformers import SentenceTransformer  # type: ignore

        self._model = SentenceTransformer(model_name)
        self.dim = int(self._model.get_sentence_embedding_dimension())
        self.model_name = model_name

    def embed(self, texts: List[str]) -> List[List[float]]:
        raw = self._model.encode(
            list(texts), normalize_embeddings=True, convert_to_numpy=True
        )
        return [list(map(float, row)) for row in raw]


def _l2_normalize(vec: List[float]) -> List[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    if norm <= 1e-12:
        return vec
    return [v / norm for v in vec]


def get_embedder(
    provider: str = "hash",
    *,
    model: str = "all-MiniLM-L6-v2",
    dim: int = 256,
) -> EmbeddingProvider:
    """Factory: return an embedder for the configured provider.

    Falls back to :class:`HashingEmbedder` when the requested provider is
    unavailable so the RAG feature never hard-fails on a missing dependency.
    """
    prov = (provider or "hash").strip().lower()
    if prov in ("st", "sentence-transformers", "sentencetransformers"):
        try:
            return SentenceTransformerEmbedder(model_name=model)
        except Exception as exc:  # noqa: BLE001
            console_print(
                f"[RAG] sentence-transformers unavailable ({exc}); "
                f"falling back to hashing embedder."
            )
            return HashingEmbedder(dim=dim)
    return HashingEmbedder(dim=dim)
