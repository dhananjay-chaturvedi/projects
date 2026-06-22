"""Document model shared across the RAG pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Kinds of indexable knowledge. Schema kinds are rebuilt on every reindex;
# user kinds (glossary/example/document/analytical) are preserved across
# reindex so the feedback loop and uploaded knowledge accumulate value.
#
#   glossary    -- a business term + definition
#   example     -- a user-authored NL->SQL example (feedback loop)
#   analytical  -- a generic, schema-agnostic analytical NL->SQL pattern
#                  (seeded from the built-in library; also trains the LLM)
#   document    -- a chunk of an uploaded reference document (text/PDF/DOCX)
SCHEMA_KINDS = ("table", "view", "relationship", "index")
USER_KINDS = ("glossary", "example", "analytical", "document", "code")
ALL_KINDS = SCHEMA_KINDS + USER_KINDS

# Kinds that carry a NL->SQL pair usable for training the local LLM.
TRAINABLE_KINDS = ("example", "analytical")


@dataclass
class Document:
    """A single retrievable unit of database knowledge.

    Attributes:
        doc_id:   stable identifier, unique per connection (e.g. ``table:orders``).
        kind:     one of :data:`ALL_KINDS`.
        ref:      the object/term name (e.g. ``orders``, ``ARR``).
        text:     the embeddable + human-readable content.
        metadata: arbitrary structured extras (column list, sql, etc.).
    """

    doc_id: str
    kind: str
    ref: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "doc_id": self.doc_id,
            "kind": self.kind,
            "ref": self.ref,
            "text": self.text,
            "metadata": dict(self.metadata or {}),
        }
