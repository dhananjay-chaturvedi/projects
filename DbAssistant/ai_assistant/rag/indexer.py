"""
RAG indexer: extract schema documents, embed them, and persist to the store.

Schema documents are fully rebuilt on each run; user-authored knowledge
(glossary terms, NL->SQL examples) is preserved so the feedback loop keeps
accumulating value across reindexes.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from common.config_loader import console_print
from ai_assistant.rag.documents import SCHEMA_KINDS, Document
from ai_assistant.rag.embeddings import EmbeddingProvider
from ai_assistant.rag.schema_extractor import SchemaExtractor
from ai_assistant.rag.vector_store import SqliteVectorStore


def schema_hash(docs: list[Document]) -> str:
    """Stable digest of the schema docs, for drift / staleness detection."""
    h = hashlib.sha256()
    for d in sorted(docs, key=lambda x: x.doc_id):
        if d.kind in SCHEMA_KINDS:
            h.update(d.doc_id.encode("utf-8"))
            h.update(b"\x00")
            h.update((d.text or "").encode("utf-8"))
            h.update(b"\x01")
    return h.hexdigest()


class RagIndexer:
    def __init__(
        self,
        store: SqliteVectorStore,
        embedder: EmbeddingProvider,
        extractor: SchemaExtractor | None = None,
    ):
        self.store = store
        self.embedder = embedder
        self.extractor = extractor or SchemaExtractor()
        self._index_locks: dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

    def _index_lock(self, connection_name: str) -> threading.Lock:
        with self._locks_lock:
            if connection_name not in self._index_locks:
                self._index_locks[connection_name] = threading.Lock()
            return self._index_locks[connection_name]

    def index(
        self,
        db_manager,
        connection_name: str,
        *,
        rebuild: bool = False,
    ) -> dict[str, Any]:
        """(Re)build the schema index for *connection_name*.

        Args:
            rebuild: when True, drop ALL documents (including user knowledge)
                     for a clean slate. When False, only schema documents are
                     refreshed; glossary/examples are kept.
        """
        with self._index_lock(connection_name):
            return self._index_locked(db_manager, connection_name, rebuild=rebuild)

    def _index_locked(
        self,
        db_manager,
        connection_name: str,
        *,
        rebuild: bool = False,
    ) -> dict[str, Any]:
        started = time.time()
        # Capture existing schema embeddings *before* deletion so an incremental
        # reindex can reuse vectors for objects whose text did not change.
        reuse: dict[str, tuple[str, list[float]]] = {}
        if not rebuild:
            meta = self.store.get_meta(connection_name) or {}
            if (meta.get("provider") == self.embedder.name
                    and int(meta.get("dim") or 0) == self.embedder.dim):
                for sd in self.store.load_documents(connection_name):
                    if sd.kind in SCHEMA_KINDS:
                        reuse[sd.doc_id] = (sd.text, sd.embedding)

        if rebuild:
            self.store.delete_connection(connection_name)
        else:
            self.store.delete_kinds(connection_name, SCHEMA_KINDS)

        docs = self.extractor.extract(db_manager, connection_name)
        indexed, reused = self._embed_and_store(
            connection_name, docs, db_manager.db_type, reuse=reuse
        )

        self.store.set_meta(
            connection_name,
            db_type=db_manager.db_type,
            provider=self.embedder.name,
            dim=self.embedder.dim,
            schema_hash=schema_hash(docs),
        )
        elapsed = round(time.time() - started, 3)
        total = self.store.count(connection_name)
        console_print(
            f"[RAG] Indexed {indexed} schema docs for '{connection_name}' "
            f"({reused} reused) in {elapsed}s "
            f"(total docs incl. user knowledge: {total})."
        )
        return {
            "ok": True,
            "connection": connection_name,
            "db_type": db_manager.db_type,
            "indexed": indexed,
            "reused": reused,
            "doc_count": total,
            "provider": self.embedder.name,
            "dim": self.embedder.dim,
            "elapsed_sec": elapsed,
            "error": None,
        }

    def add_documents(self, connection_name: str, docs: list[Document]) -> int:
        """Embed and persist arbitrary documents (used for glossary/examples)."""
        indexed, _ = self._embed_and_store(connection_name, docs, db_type="")
        return indexed

    def _embed_and_store(
        self,
        connection_name: str,
        docs: list[Document],
        db_type: str,
        *,
        reuse: dict[str, tuple[str, list[float]]] | None = None,
    ) -> tuple[int, int]:
        """Embed *docs* and persist them, returning ``(stored, reused)``.

        When *reuse* maps ``doc_id -> (text, embedding)`` for already-indexed
        objects, documents whose text is unchanged skip re-embedding entirely
        (incremental reindex); only new/changed objects are embedded.
        """
        if not docs:
            return 0, 0
        reuse = reuse or {}
        embeddings: list[list[float] | None] = [None] * len(docs)
        to_embed: list[int] = []
        reused = 0
        for i, d in enumerate(docs):
            prev = reuse.get(d.doc_id)
            if prev is not None and prev[0] == d.text:
                embeddings[i] = prev[1]
                reused += 1
            else:
                to_embed.append(i)
        if to_embed:
            new_vecs = self.embedder.embed([docs[i].text for i in to_embed])
            for j, i in enumerate(to_embed):
                embeddings[i] = new_vecs[j]
        stored = self.store.upsert_documents(
            connection_name,
            docs,
            [e for e in embeddings if e is not None],
            provider=self.embedder.name,
            dim=self.embedder.dim,
        )
        return stored, reused
