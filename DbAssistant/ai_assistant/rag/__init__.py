# ---------------------------------------------------------------------
# description: Retrieval-Augmented Generation (RAG) for connected databases
# initial version: 09-JUN-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------
"""
RAG subsystem for the AI Query Assistant.

Instead of dumping a (truncated) full schema into every prompt, this package
indexes the connected database's schema + business knowledge into a local
vector store and, at query time, retrieves only the objects relevant to the
question. The retrieved context is then injected into the AI backend prompt.

Public surface:
    Document            -- one indexable unit (table/view/glossary/example)
    get_embedder        -- factory for the configured embedding provider
    SqliteVectorStore   -- local, file-based vector index (no external service)
    SchemaExtractor     -- turns a live DB connection into Documents
    RagIndexer          -- extract -> embed -> store
    RagRetriever        -- embed query -> similarity search -> format context
    RagService          -- shared logic used by UI / CLI / API (parity)
"""

from __future__ import annotations

from ai_assistant.rag.documents import Document
from ai_assistant.rag.embeddings import get_embedder
from ai_assistant.rag.vector_store import SqliteVectorStore
from ai_assistant.rag.schema_extractor import SchemaExtractor
from ai_assistant.rag.indexer import RagIndexer
from ai_assistant.rag.retriever import RagRetriever, RetrievalHit

__all__ = [
    "Document",
    "get_embedder",
    "SqliteVectorStore",
    "SchemaExtractor",
    "RagIndexer",
    "RagRetriever",
    "RetrievalHit",
]
