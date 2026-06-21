"""
Optional approximate-nearest-neighbour (ANN) acceleration for vector scoring.

The default retriever scans every stored vector in pure Python — perfectly fine
for schema-scale corpora. When an index grows very large (many documents / a
huge schema), enable ``ai.rag ann`` to offload the vector similarity to FAISS
(``faiss-cpu``) when it is installed.

The vectors live in the same :class:`SqliteVectorStore`; FAISS is built on the
fly from the loaded embeddings, so nothing about the storage format changes and
the feature degrades gracefully to the Python path when FAISS is absent.

Public surface::

    faiss_available() -> bool
    vector_scores(query, matrix) -> list[float]   # cosine per row, same order
"""

from __future__ import annotations

from typing import List


def faiss_available() -> bool:
    try:
        import faiss  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


def vector_scores(query: List[float], matrix: List[List[float]]) -> List[float]:
    """Return the cosine similarity of *query* against each row of *matrix*.

    Vectors are already L2-normalised by the embedder, so inner product equals
    cosine. Uses a FAISS ``IndexFlatIP`` for the heavy lifting; raises if FAISS
    is unavailable (callers fall back to the Python path).
    """
    import faiss  # type: ignore
    import numpy as np  # type: ignore

    if not matrix:
        return []
    dim = len(matrix[0])
    mat = np.asarray(matrix, dtype="float32")
    q = np.asarray([query[:dim]], dtype="float32")
    index = faiss.IndexFlatIP(dim)
    index.add(mat)
    # Retrieve all rows so every document keeps a comparable score for fusion.
    scores, idxs = index.search(q, len(matrix))
    out = [0.0] * len(matrix)
    for score, i in zip(scores[0], idxs[0]):
        if 0 <= int(i) < len(out):
            out[int(i)] = float(score)
    return out
