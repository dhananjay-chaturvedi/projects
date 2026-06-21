"""
SQLite-backed vector store for the RAG index.

A single SQLite file holds the index for *all* connections (partitioned by the
``connection`` column). Embeddings are stored as packed float32 blobs. Cosine
similarity is computed in Python over the rows belonging to a connection —
ample for schema-scale corpora (hundreds–thousands of objects) and keeping the
tool dependency-free and fully offline.

Schema::

    rag_documents(
        id, connection, doc_id, kind, ref, text,
        embedding BLOB, dim, provider, metadata, created_at,
        UNIQUE(connection, doc_id)
    )
    rag_meta(connection PK, db_type, provider, dim, doc_count, indexed_at)
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from array import array
from pathlib import Path
from typing import Any, Iterable, Optional

from ai_assistant.rag.documents import Document

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rag_documents (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    connection  TEXT NOT NULL,
    doc_id      TEXT NOT NULL,
    kind        TEXT NOT NULL,
    ref         TEXT NOT NULL,
    text        TEXT NOT NULL,
    embedding   BLOB NOT NULL,
    dim         INTEGER NOT NULL,
    provider    TEXT NOT NULL,
    metadata    TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL,
    UNIQUE(connection, doc_id)
);
CREATE INDEX IF NOT EXISTS ix_rag_docs_conn ON rag_documents(connection);
CREATE INDEX IF NOT EXISTS ix_rag_docs_kind ON rag_documents(connection, kind);

CREATE TABLE IF NOT EXISTS rag_meta (
    connection  TEXT PRIMARY KEY,
    db_type     TEXT NOT NULL DEFAULT '',
    provider    TEXT NOT NULL DEFAULT '',
    dim         INTEGER NOT NULL DEFAULT 0,
    doc_count   INTEGER NOT NULL DEFAULT 0,
    indexed_at  TEXT NOT NULL DEFAULT '',
    schema_hash TEXT NOT NULL DEFAULT ''
);
"""


def _pack(vec: list[float]) -> bytes:
    return array("f", vec).tobytes()


def _unpack(blob: bytes) -> list[float]:
    arr = array("f")
    arr.frombytes(blob)
    return list(arr)


class StoredDoc:
    """A document loaded back from the store, with its embedding."""

    __slots__ = ("doc_id", "kind", "ref", "text", "embedding", "metadata")

    def __init__(self, doc_id, kind, ref, text, embedding, metadata):
        self.doc_id = doc_id
        self.kind = kind
        self.ref = ref
        self.text = text
        self.embedding = embedding
        self.metadata = metadata


class SqliteVectorStore:
    """Thread-safe, file-based vector store."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(_SCHEMA)
            # Best-effort migration for indexes created before schema_hash existed.
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(rag_meta)")}
            if "schema_hash" not in cols:
                try:
                    conn.execute(
                        "ALTER TABLE rag_meta ADD COLUMN schema_hash "
                        "TEXT NOT NULL DEFAULT ''"
                    )
                except sqlite3.OperationalError:
                    pass

    # ── writes ────────────────────────────────────────────────────────────

    def upsert_documents(
        self,
        connection: str,
        docs: Iterable[Document],
        embeddings: list[list[float]],
        *,
        provider: str,
        dim: int,
    ) -> int:
        docs = list(docs)
        if len(docs) != len(embeddings):
            raise ValueError("docs and embeddings length mismatch")
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for doc, emb in zip(docs, embeddings):
            rows.append(
                (
                    connection,
                    doc.doc_id,
                    doc.kind,
                    doc.ref,
                    doc.text,
                    _pack(emb),
                    dim,
                    provider,
                    json.dumps(doc.metadata or {}, default=str),
                    now,
                )
            )
        with self._lock, self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO rag_documents
                    (connection, doc_id, kind, ref, text, embedding, dim,
                     provider, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connection, doc_id) DO UPDATE SET
                    kind=excluded.kind, ref=excluded.ref, text=excluded.text,
                    embedding=excluded.embedding, dim=excluded.dim,
                    provider=excluded.provider, metadata=excluded.metadata,
                    created_at=excluded.created_at
                """,
                rows,
            )
        return len(rows)

    def delete_connection(self, connection: str) -> int:
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM rag_documents WHERE connection = ?", (connection,)
            )
            conn.execute("DELETE FROM rag_meta WHERE connection = ?", (connection,))
            return cur.rowcount

    def delete_kinds(self, connection: str, kinds: Iterable[str]) -> int:
        kinds = list(kinds)
        if not kinds:
            return 0
        placeholders = ",".join("?" for _ in kinds)
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                f"DELETE FROM rag_documents WHERE connection = ? "
                f"AND kind IN ({placeholders})",
                (connection, *kinds),
            )
            return cur.rowcount

    def delete_doc_prefix(self, connection: str, prefix: str) -> int:
        """Delete all documents whose ``doc_id`` starts with *prefix*.

        Used to remove every chunk of one uploaded document (chunks share a
        ``document:<source-slug>:`` prefix).
        """
        like = prefix.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM rag_documents WHERE connection = ? "
                "AND doc_id LIKE ? ESCAPE '\\'",
                (connection, like),
            )
            return cur.rowcount

    def set_meta(
        self,
        connection: str,
        *,
        db_type: str,
        provider: str,
        dim: int,
        schema_hash: str | None = None,
    ) -> None:
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        with self._lock, self._connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM rag_documents WHERE connection = ?",
                (connection,),
            ).fetchone()[0]
            # Preserve an existing schema_hash when the caller doesn't supply one
            # (e.g. a glossary/example write should not wipe schema-drift state).
            if schema_hash is None:
                row = conn.execute(
                    "SELECT schema_hash FROM rag_meta WHERE connection = ?",
                    (connection,),
                ).fetchone()
                schema_hash = (row["schema_hash"] if row else "") or ""
            conn.execute(
                """
                INSERT INTO rag_meta
                    (connection, db_type, provider, dim, doc_count, indexed_at,
                     schema_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connection) DO UPDATE SET
                    db_type=excluded.db_type, provider=excluded.provider,
                    dim=excluded.dim, doc_count=excluded.doc_count,
                    indexed_at=excluded.indexed_at, schema_hash=excluded.schema_hash
                """,
                (connection, db_type, provider, dim, count, now, schema_hash),
            )

    # ── reads ─────────────────────────────────────────────────────────────

    def count(self, connection: str) -> int:
        with self._lock, self._connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM rag_documents WHERE connection = ?",
                (connection,),
            ).fetchone()[0]

    def counts_by_kind(self, connection: str) -> dict[str, int]:
        """Return a ``{kind: count}`` breakdown for a connection/scope."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT kind, COUNT(*) AS cnt FROM rag_documents "
                "WHERE connection = ? GROUP BY kind",
                (connection,),
            ).fetchall()
        return {r["kind"]: r["cnt"] for r in rows}

    def list_by_kind(self, connection: str, kind: str) -> list[dict[str, Any]]:
        """Return lightweight rows (no embedding blob) for one kind."""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT doc_id, ref, text, metadata FROM rag_documents "
                "WHERE connection = ? AND kind = ? ORDER BY doc_id",
                (connection, kind),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                meta = json.loads(r["metadata"]) if r["metadata"] else {}
            except Exception:
                meta = {}
            out.append({
                "doc_id": r["doc_id"],
                "ref": r["ref"],
                "text": r["text"],
                "metadata": meta,
            })
        return out

    def load_documents(self, connection: str) -> list[StoredDoc]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT doc_id, kind, ref, text, embedding, metadata "
                "FROM rag_documents WHERE connection = ?",
                (connection,),
            ).fetchall()
        out: list[StoredDoc] = []
        for r in rows:
            try:
                meta = json.loads(r["metadata"]) if r["metadata"] else {}
            except Exception:
                meta = {}
            out.append(
                StoredDoc(
                    r["doc_id"],
                    r["kind"],
                    r["ref"],
                    r["text"],
                    _unpack(r["embedding"]),
                    meta,
                )
            )
        return out

    def get_meta(self, connection: str) -> Optional[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            r = conn.execute(
                "SELECT * FROM rag_meta WHERE connection = ?", (connection,)
            ).fetchone()
        return dict(r) if r else None

    def list_meta(self) -> list[dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM rag_meta ORDER BY connection"
            ).fetchall()
        return [dict(r) for r in rows]
