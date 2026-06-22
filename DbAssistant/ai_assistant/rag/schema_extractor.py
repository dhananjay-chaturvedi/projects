"""
Turn a live database connection into RAG :class:`Document` objects.

Reuses the same shared introspection path the AI agent already relies on
(:class:`common.database_registry.DatabaseRegistry`) so RAG sees the exact same
schema the rest of the tool does, across every supported engine.

One document is produced per:
    * table   (name + full column list with types/null/default/PK)
    * view    (name)
    * index   (one rolled-up document listing index names)

Optionally, a few representative sample values per text column are appended to
table documents (helps the model map "status" -> 'shipped'/'pending', etc.).
Sampling is read-only and best-effort; failures never abort indexing.
"""

from __future__ import annotations

import re
from typing import Any

from common.config_loader import console_print
from common.database_registry import DatabaseRegistry
from ai_assistant.rag.documents import Document


class SchemaExtractor:
    def __init__(
        self,
        *,
        max_tables: int = 500,
        sample_values: bool = True,
        sample_limit: int = 5,
        sample_columns: int = 6,
        mask_pii: bool = False,
        column_comments: bool = True,
        enum_max_distinct: int = 12,
        purpose_summarizer=None,
    ):
        self.max_tables = max_tables
        self.sample_values = sample_values
        self.sample_limit = sample_limit
        self.sample_columns = sample_columns
        self.mask_pii = mask_pii
        self.column_comments = column_comments
        self.enum_max_distinct = enum_max_distinct
        # Optional callable(table, columns_text) -> str: an AI one-liner per table.
        self.purpose_summarizer = purpose_summarizer

    # ------------------------------------------------------------------

    def extract(self, db_manager, connection_name: str = "") -> list[Document]:
        if db_manager is None or getattr(db_manager, "conn", None) is None:
            raise ConnectionError(
                f"Not connected to '{connection_name}'. Establish the database "
                f"connection before building the RAG index."
            )

        db_type = db_manager.db_type
        conn = db_manager.conn
        docs: list[Document] = []

        tables = self._op(db_type, "getTables", conn) or []
        for table in tables[: self.max_tables]:
            docs.append(self._table_document(db_manager, db_type, conn, table))

        # Views
        if DatabaseRegistry.supports_operation(db_type, "getViews"):
            views = self._op(db_type, "getViews", conn) or []
            for view in views:
                docs.append(
                    Document(
                        doc_id=f"view:{view}",
                        kind="view",
                        ref=str(view),
                        text=f"View: {view}\nDatabase object type: VIEW ({db_type}).",
                        metadata={"object": "view", "name": view},
                    )
                )

        # Indexes (rolled up into a single hint document)
        if DatabaseRegistry.supports_operation(db_type, "getIndexes"):
            indexes = self._op(db_type, "getIndexes", conn) or []
            if indexes:
                names = [str(i) for i in indexes]
                docs.append(
                    Document(
                        doc_id="index:_all",
                        kind="index",
                        ref="indexes",
                        text="Indexes available in this database: "
                        + ", ".join(names[:200]),
                        metadata={"object": "index", "names": names},
                    )
                )

        # Foreign keys / relationships (from constraints catalog)
        if DatabaseRegistry.supports_operation(db_type, "getConstraints"):
            docs.extend(self._relationship_documents(db_type, conn))

        console_print(
            f"[RAG] Extracted {len(docs)} schema documents from "
            f"'{connection_name or db_type}' ({len(tables)} tables)."
        )
        return docs

    # ------------------------------------------------------------------

    def _table_document(self, db_manager, db_type, conn, table) -> Document:
        columns: list[dict[str, Any]] = []
        if DatabaseRegistry.supports_operation(db_type, "getTableSchema"):
            try:
                columns = (
                    self._op(db_type, "getTableSchema", conn, table) or []
                )
            except Exception as exc:  # noqa: BLE001
                console_print(f"[RAG] schema fetch failed for {table}: {exc}")

        lines = [f"Table: {table}", f"Database type: {db_type}"]
        col_names: list[str] = []
        pk_cols: list[str] = []
        col_comments: dict[str, str] = {}
        col_lines: list[str] = []
        for col in columns:
            name = str(col.get("name", ""))
            col_names.append(name)
            ctype = str(col.get("type", ""))
            nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
            default = col.get("default")
            extra = f" DEFAULT {default}" if default not in (None, "") else ""
            if "PRIMARY KEY" in ctype.upper():
                pk_cols.append(name)
            # Column comment/description (engines that expose it via getTableSchema).
            comment = ""
            if self.column_comments:
                comment = str(
                    col.get("comment") or col.get("description") or ""
                ).strip()
                if comment:
                    col_comments[name] = comment
            cmt = f" -- {comment}" if comment else ""
            col_lines.append(f"  - {name} ({ctype}) {nullable}{extra}{cmt}")
        lines.append(f"Columns ({len(col_names)}):")
        lines.extend(col_lines)

        if pk_cols:
            lines.append(f"Primary key: {', '.join(pk_cols)}")

        samples: dict[str, list[str]] = {}
        enums: dict[str, list[str]] = {}
        if self.sample_values and col_names:
            samples, enums = self._value_profile(db_manager, table, columns)
            if enums:
                lines.append("Enumerated / low-cardinality columns:")
                for cname, vals in enums.items():
                    lines.append(f"  - {cname} ∈ {{{', '.join(vals)}}}")
            if samples:
                lines.append("Sample values:")
                for cname, vals in samples.items():
                    lines.append(f"  - {cname}: {', '.join(vals)}")

        purpose = ""
        if self.purpose_summarizer is not None:
            try:
                purpose = (self.purpose_summarizer(str(table), "\n".join(col_lines)) or "").strip()
            except Exception:  # noqa: BLE001
                purpose = ""
            if purpose:
                lines.insert(1, f"Purpose: {purpose}")

        return Document(
            doc_id=f"table:{table}",
            kind="table",
            ref=str(table),
            text="\n".join(lines),
            metadata={
                "object": "table",
                "name": table,
                "columns": col_names,
                "primary_key": pk_cols,
                "samples": samples,
                "enums": enums,
                "comments": col_comments,
                "purpose": purpose,
            },
        )

    def _mask(self, value: str) -> str:
        """PII-mask a sampled value when masking is enabled (governance)."""
        if not self.mask_pii or not value:
            return value
        try:
            from ai_query.pii_masker import mask_pii

            return mask_pii(value).text
        except Exception:  # noqa: BLE001
            return value

    def _value_profile(
        self, db_manager, table, columns
    ) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        """Best-effort value profiling for low-cardinality text columns.

        Returns ``(samples, enums)``. *enums* holds columns whose full distinct
        set is small (<= ``enum_max_distinct``); *samples* holds a few example
        values for the rest. Values are PII-masked when masking is enabled.
        """
        samples: dict[str, list[str]] = {}
        enums: dict[str, list[str]] = {}
        conn = getattr(db_manager, "conn", None)
        if conn is None:
            return samples, enums
        # Only sample short text-ish columns; skip obvious id/blob/date columns.
        candidates = []
        for col in columns:
            name = str(col.get("name", ""))
            ctype = str(col.get("type", "")).upper()
            if not name:
                continue
            if any(b in ctype for b in ("BLOB", "BYTEA", "IMAGE")):
                continue
            if name.lower().endswith("id") or name.lower() == "id":
                continue
            if "CHAR" in ctype or "TEXT" in ctype or "ENUM" in ctype:
                candidates.append(name)
        candidates = candidates[: self.sample_columns]
        if not candidates:
            return samples, enums

        enum_cap = max(self.sample_limit, int(self.enum_max_distinct))
        for name in candidates:
            try:
                cur = conn.cursor()
                # Fetch up to enum_cap+1 distinct values to decide enum vs sample.
                cur.execute(
                    f'SELECT DISTINCT "{name}" FROM "{table}" '
                    f'WHERE "{name}" IS NOT NULL LIMIT {enum_cap + 1}'
                )
                vals = [str(r[0]) for r in cur.fetchall() if r and r[0] is not None]
                cur.close()
                vals = [self._mask(v) for v in (s.strip() for s in vals)
                        if v and len(v) <= 40]
                if not vals:
                    continue
                if len(vals) <= self.enum_max_distinct:
                    enums[name] = vals
                else:
                    samples[name] = vals[: int(self.sample_limit)]
            except Exception:
                # identifier quoting / type may not allow sampling; just skip
                continue
        return samples, enums

    def _relationship_documents(self, db_type: str, conn) -> list[Document]:
        """Build relationship docs from the engine's constraints catalog."""
        docs: list[Document] = []
        try:
            raw = self._op(db_type, "getConstraints", conn) or []
        except Exception as exc:  # noqa: BLE001
            console_print(f"[RAG] constraints fetch failed: {exc}")
            return docs
        fk_lines = [
            str(line) for line in raw
            if "FOREIGN KEY" in str(line).upper() or "REFERENCES" in str(line).upper()
        ]
        if not fk_lines:
            return docs
        # One summary doc for broad retrieval
        docs.append(
            Document(
                doc_id="relationship:_summary",
                kind="relationship",
                ref="foreign_keys",
                text="Foreign key relationships in this database:\n"
                + "\n".join(f"  - {line}" for line in fk_lines[:300]),
                metadata={"object": "relationship", "count": len(fk_lines)},
            )
        )
        # Per-constraint docs for precise join hints (cap to avoid explosion)
        for i, line in enumerate(fk_lines[:100]):
            slug = re.sub(r"\W+", "_", line.lower())[:50] or f"fk_{i}"
            docs.append(
                Document(
                    doc_id=f"relationship:{slug}",
                    kind="relationship",
                    ref=line.split("(")[0].strip() if "(" in line else line[:60],
                    text=f"Relationship: {line}\nUse this when joining related tables.",
                    metadata={"object": "relationship", "constraint": line},
                )
            )
        return docs

    @staticmethod
    def _op(db_type, name, conn, *args):
        return DatabaseRegistry.execute_operation(db_type, name, conn, *args)
