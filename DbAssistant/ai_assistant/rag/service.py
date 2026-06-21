"""
RagService — the single shared code path for all RAG operations.

UI (Tkinter), CLI (``dbtool ai rag ...``) and API (``/api/ai/rag/...``) all call
into this class so business logic lives in exactly one place (per the project's
UI/CLI/API parity rule).

Responsibilities:
    * resolve a live connection via the core DB service
    * (re)build / clear the per-connection vector index
    * retrieve relevant schema context for a question
    * generate RAG-augmented SQL via the active AI backend
    * grow the corpus through glossary terms and NL->SQL examples (feedback loop)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional

from ai_query import module_config as mc
from ai_assistant.rag.documents import Document
from ai_assistant.rag.embeddings import get_embedder
from ai_assistant.rag.indexer import RagIndexer
from ai_assistant.rag.retriever import RagRetriever
from ai_assistant.rag.schema_extractor import SchemaExtractor
from ai_assistant.rag.vector_store import SqliteVectorStore
from ai_assistant.rag import analytics_library, document_loader


def _slug(text: str, *, limit: int = 60) -> str:
    s = re.sub(r"\W+", "_", (text or "").strip().lower()).strip("_")
    return s[:limit] or "item"


def default_index_path() -> Path:
    """Location of the shared RAG SQLite index file."""
    from common import paths as _paths

    return _paths.session_dir() / "rag" / "rag_index.db"


@dataclass(frozen=True)
class RagDocumentRequest:
    """Document ingestion request for a RAG scope."""

    scope: str
    text: str | None = None
    file_path: str | None = None
    title: str = ""
    source: str = ""
    standalone: bool = False
    chunk_size: int | None = None
    overlap: int | None = None

    @classmethod
    def from_call(
        cls, request: "RagDocumentRequest | str", values: Mapping[str, Any]
    ) -> "RagDocumentRequest":
        """Coerce request-object or legacy scope+keyword calls."""
        if isinstance(request, cls):
            src = {**request.__dict__, **dict(values)}
        else:
            src = {"scope": request, **dict(values)}
        return cls(
            scope=src.get("scope", ""),
            text=src.get("text"),
            file_path=src.get("file_path"),
            title=src.get("title", "") or "",
            source=src.get("source", "") or "",
            standalone=bool(src.get("standalone", False)),
            chunk_size=src.get("chunk_size"),
            overlap=src.get("overlap"),
        )


class RagService:
    """Shared RAG logic. ``core`` resolves connections; ``agent`` generates SQL."""

    def __init__(self, core: Any, agent: Any = None, *, index_path: str | Path | None = None):
        self._core = core
        self._agent = agent
        self._index_path = Path(index_path) if index_path else default_index_path()
        self._store: Optional[SqliteVectorStore] = None
        self._embedder = None

    # ── lazy singletons ───────────────────────────────────────────────────

    def store(self) -> SqliteVectorStore:
        if self._store is None:
            self._store = SqliteVectorStore(self._index_path)
        return self._store

    def embedder(self):
        if self._embedder is None:
            self._embedder = get_embedder(
                mc.get("ai.rag", "embedding_provider", default="hash"),
                model=mc.get("ai.rag", "embedding_model", default="all-MiniLM-L6-v2"),
                dim=mc.get_int("ai.rag", "embedding_dim", default=256),
            )
        return self._embedder

    def _extractor(self) -> SchemaExtractor:
        return SchemaExtractor(
            max_tables=mc.get_int("ai.rag", "max_tables", default=500),
            sample_values=mc.get_bool("ai.rag", "sample_values", default=True),
            sample_limit=mc.get_int("ai.rag", "sample_limit", default=5),
            mask_pii=mc.get_bool("ai.rag", "mask_samples", default=False)
            or mc.get_bool("ai", "mask_pii", default=True),
            column_comments=mc.get_bool("ai.rag", "column_comments", default=True),
            enum_max_distinct=mc.get_int("ai.rag", "enum_max_distinct", default=12),
            purpose_summarizer=self._purpose_summarizer(),
        )

    def _purpose_summarizer(self):
        """Return a callable(table, columns_text) -> one-line purpose, or None.

        Gated by ``ai.rag table_purpose`` (default off, to keep indexing fully
        offline). Uses the AI agent only when present and available.
        """
        if not mc.get_bool("ai.rag", "table_purpose", default=False):
            return None
        agent = self._agent
        if agent is None or not hasattr(agent, "_call_ai"):
            return None

        def _summarize(table: str, columns_text: str) -> str:
            prompt = (
                "In one short sentence, describe the business purpose of this "
                f"database table. Reply with the sentence only.\n\nTable: {table}\n"
                f"{columns_text}"
            )
            try:
                res = agent._call_ai(prompt, path="rag.table_purpose", tier=1)
                text = (res or {}).get("response") or ""
                lines = [ln for ln in text.strip().splitlines() if ln.strip()]
                return lines[0][:200] if lines else ""
            except Exception:  # noqa: BLE001
                return ""

        return _summarize

    def _indexer(self) -> RagIndexer:
        return RagIndexer(self.store(), self.embedder(), self._extractor())

    def _retriever(self) -> RagRetriever:
        return RagRetriever(
            self.store(),
            self.embedder(),
            lexical_alpha=mc.get_float("ai.rag", "lexical_alpha", default=0.3),
        )

    def _manager(self, connection: str):
        if not connection:
            raise ValueError("A connection name is required.")
        return self._core.get_manager(connection)

    # ── operations ────────────────────────────────────────────────────────

    def index(self, connection: str, *, rebuild: bool = False) -> dict[str, Any]:
        """Build/refresh the RAG index for *connection*."""
        try:
            mgr = self._manager(connection)
            return self._indexer().index(mgr, connection, rebuild=rebuild)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "connection": connection, "error": str(exc)}

    def status(self, connection: str = "") -> dict[str, Any]:
        """Return index metadata for one connection or all indexed connections."""
        try:
            store = self.store()
            if connection:
                meta = store.get_meta(connection)
                emb = self.embedder()
                mismatch = self._embedder_mismatch_meta(meta, emb)
                return {
                    "ok": True,
                    "connection": connection,
                    "indexed": bool(meta),
                    "meta": meta,
                    "doc_count": store.count(connection),
                    "embedder_mismatch": mismatch,
                    "staleness": self._staleness_meta(meta),
                    "error": None,
                }
            conns = store.list_meta()
            for m in conns:
                m["staleness"] = self._staleness_meta(m)
            return {"ok": True, "connections": conns, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def _multi_hits(self, scopes: list[str], query: str, k: int):
        """Retrieve + globally re-rank hits across several scopes."""
        retr = self._retriever()
        hits = []
        for sc in scopes:
            for h in retr.search(sc, query, k=k):
                h.metadata = {**(h.metadata or {}), "scope": sc}
                hits.append(h)
        hits.sort(key=lambda h: h.score, reverse=True)
        top = hits[: max(1, k)]
        self._log_retrieval(",".join(scopes), query, top, path="multi")
        return top

    def search_multi(
        self, scopes: list[str], query: str, k: int = 8
    ) -> dict[str, Any]:
        """Search across multiple scopes (e.g. a DB schema + a codebase + docs)
        and return a single globally-ranked hit list, each tagged with its scope."""
        try:
            scopes = [s.strip() for s in (scopes or []) if s and s.strip()]
            if not scopes:
                return {"ok": False, "error": "At least one scope is required.", "hits": []}
            if not (query or "").strip():
                return {"ok": False, "error": "A query is required.", "hits": []}
            hits = self._multi_hits(scopes, query, k)
            out = []
            for h in hits:
                d = h.to_dict()
                d["scope"] = (h.metadata or {}).get("scope")
                out.append(d)
            return {"ok": True, "scopes": scopes, "query": query,
                    "hits": out, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "hits": [], "error": str(exc)}

    def preview_multi(
        self, scopes: list[str], query: str, k: int = 8
    ) -> dict[str, Any]:
        """Multi-scope search + ranked preview + combined context block."""
        try:
            scopes = [s.strip() for s in (scopes or []) if s and s.strip()]
            if not scopes:
                return {"ok": False, "error": "At least one scope is required.", "hits": []}
            if not (query or "").strip():
                return {"ok": False, "error": "A query is required.", "hits": []}
            retr = self._retriever()
            hits = self._multi_hits(scopes, query, k)
            return {
                "ok": True,
                "scopes": scopes,
                "query": query,
                "hits": [{**h.to_dict(), "scope": (h.metadata or {}).get("scope")}
                         for h in hits],
                "preview": retr.format_preview(hits),
                "context": retr.format_context(hits),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "hits": [], "error": str(exc)}

    def scope_overview(self, scope: str) -> dict[str, Any]:
        """Status + per-kind breakdown + embedder mismatch for one scope."""
        st = self.status(scope)
        br = self.breakdown(scope)
        return {
            "ok": st.get("ok", False) and br.get("ok", False),
            "scope": scope,
            "status": st,
            "breakdown": br,
            "error": st.get("error") or br.get("error"),
        }

    @staticmethod
    def _embedder_mismatch_meta(meta: dict | None, embedder) -> dict[str, Any]:
        if not meta:
            return {"mismatch": False, "message": ""}
        ip = (meta.get("provider") or "").strip()
        idim = int(meta.get("dim") or 0)
        cp = embedder.name
        cdim = int(embedder.dim)
        mismatch = bool(ip and (ip != cp or idim != cdim))
        msg = ""
        if mismatch:
            msg = (
                f"Index was built with provider={ip!r} dim={idim}; "
                f"current config is provider={cp!r} dim={cdim}. Re-index recommended."
            )
        return {
            "mismatch": mismatch,
            "indexed_provider": ip,
            "indexed_dim": idim,
            "current_provider": cp,
            "current_dim": cdim,
            "message": msg,
        }

    def embedder_mismatch(self, scope: str) -> dict[str, Any]:
        try:
            meta = self.store().get_meta(scope)
            return {"ok": True, **self._embedder_mismatch_meta(meta, self.embedder())}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "mismatch": False, "error": str(exc)}

    @staticmethod
    def _staleness_meta(meta: dict | None) -> dict[str, Any]:
        """Age-based staleness from ``indexed_at`` vs ``stale_after_days``."""
        if not meta or not meta.get("indexed_at"):
            return {"stale": False, "age_days": None, "message": ""}
        import time as _t
        from datetime import datetime

        threshold = mc.get_int("ai.rag", "stale_after_days", default=7)
        try:
            ts = datetime.strptime(str(meta["indexed_at"]), "%Y-%m-%d %H:%M:%S")
            age_days = max(0.0, (_t.time() - ts.timestamp()) / 86400.0)
        except Exception:  # noqa: BLE001
            return {"stale": False, "age_days": None, "message": ""}
        stale = age_days > threshold
        msg = (
            f"Index is {age_days:.1f} days old (> {threshold}d); re-index recommended."
            if stale else ""
        )
        return {"stale": stale, "age_days": round(age_days, 1),
                "threshold_days": threshold, "message": msg}

    def drift(self, connection: str) -> dict[str, Any]:
        """Detect schema drift by re-hashing the live schema vs the index.

        Connects, extracts schema docs, hashes them, and compares against the
        stored ``schema_hash``. Use this before deciding to re-index.
        """
        from ai_assistant.rag.indexer import schema_hash as _hash

        try:
            meta = self.store().get_meta(connection) or {}
            stored = str(meta.get("schema_hash") or "")
            mgr = self._manager(connection)
            docs = self._extractor().extract(mgr, connection)
            current = _hash(docs)
            changed = bool(stored) and stored != current
            return {
                "ok": True,
                "connection": connection,
                "changed": changed or not stored,
                "had_hash": bool(stored),
                "stored_hash": stored,
                "current_hash": current,
                "message": (
                    "Schema changed since last index; re-index recommended."
                    if changed else "Schema unchanged."
                ),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "connection": connection, "error": str(exc)}

    def reindex_stale(
        self, connections: list[str] | None = None, *, force: bool = False
    ) -> dict[str, Any]:
        """Incrementally re-index connections that are age-stale or schema-drifted.

        With *force*, all targeted connections are re-indexed regardless of
        staleness. Re-indexing is incremental: unchanged objects reuse their
        existing embeddings.
        """
        try:
            store = self.store()
            targets = connections or [m["connection"] for m in store.list_meta()]
            results: list[dict[str, Any]] = []
            for conn in targets:
                meta = store.get_meta(conn)
                stale = self._staleness_meta(meta).get("stale", False)
                drifted = False
                reason = "forced" if force else ""
                if not force:
                    d = self.drift(conn)
                    drifted = bool(d.get("ok") and d.get("changed"))
                    if stale:
                        reason = "stale"
                    if drifted:
                        reason = "schema_changed" if not stale else "stale+schema_changed"
                if force or stale or drifted:
                    res = self.index(conn, rebuild=False)
                    res["reason"] = reason
                    results.append(res)
                else:
                    results.append({"ok": True, "connection": conn,
                                    "skipped": True, "reason": "fresh"})
            reindexed = [r for r in results if not r.get("skipped")]
            return {"ok": True, "reindexed": len(reindexed),
                    "results": results, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── scheduled (daily) incremental re-index ─────────────────────────────
    def _reindex_scheduler(self):
        from ai_assistant.rag.reindex_scheduler import get_reindex_scheduler

        return get_reindex_scheduler(self)

    def reindex_schedule_status(self) -> dict[str, Any]:
        try:
            return {"ok": True, **self._reindex_scheduler().status()}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def reindex_schedule_start(self) -> dict[str, Any]:
        try:
            sched = self._reindex_scheduler()
            sched.start()
            return {"ok": True, **sched.status()}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def reindex_schedule_stop(self) -> dict[str, Any]:
        try:
            sched = self._reindex_scheduler()
            sched.stop()
            return {"ok": True, **sched.status()}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def evaluate(
        self,
        connection: str,
        *,
        gold: list[Mapping[str, Any]] | None = None,
        k: int = 8,
        per_case: bool = False,
    ) -> dict[str, Any]:
        """Run the retrieval eval harness for *connection*.

        When *gold* is omitted, gold cases are seeded automatically from the
        indexed NL->SQL examples (each example's SQL tables are the expected
        set). Returns aggregate recall@k / MRR / context-precision metrics.
        """
        from ai_assistant.rag import rag_eval

        try:
            dialect = self._resolve_db_type(connection) or ""
            cases = [dict(g) for g in (gold or [])]
            seeded = False
            if not cases:
                examples = self.store().list_by_kind(connection, "example")
                cases = rag_eval.gold_from_examples(examples, dialect=dialect)
                seeded = True
            if not cases:
                return {
                    "ok": False,
                    "connection": connection,
                    "error": (
                        "No gold cases. Provide cases or add NL->SQL examples "
                        "(rag add-examples) to seed them."
                    ),
                    "cases": 0,
                }
            retr = self._retriever()
            case_metrics: list[dict[str, Any]] = []
            for case in cases:
                question = (case.get("question") or case.get("q") or "").strip()
                expected = case.get("tables") or case.get("expected") or []
                if isinstance(expected, str):
                    expected = [t.strip() for t in expected.split(",") if t.strip()]
                if not question:
                    continue
                hits = retr.search(connection, question, k=k)
                m = rag_eval.score_case(expected, hits, k=k)
                m["question"] = question
                case_metrics.append(m)
            agg = rag_eval.aggregate(case_metrics)
            out = {
                "ok": True,
                "connection": connection,
                "k": k,
                "seeded_from_examples": seeded,
                "metrics": agg,
                "error": None,
            }
            if per_case:
                out["cases_detail"] = case_metrics
            return out
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "connection": connection, "error": str(exc)}

    def search(self, connection: str, query: str, k: int = 8) -> dict[str, Any]:
        """Return raw retrieval hits for a query."""
        try:
            retr = self._retriever()
            hits = retr.search(connection, query, k=k)
            self._log_retrieval(connection, query, hits, path="search")
            return {
                "ok": True,
                "connection": connection,
                "query": query,
                "hits": [h.to_dict() for h in hits],
                "count": len(hits),
                "preview": retr.format_preview(hits),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "hits": [], "error": str(exc)}

    def preview(self, connection: str, query: str, k: int = 8) -> dict[str, Any]:
        """Search + formatted context + preview text for the RAG Manager."""
        try:
            retr = self._retriever()
            hits = retr.search(connection, query, k=k)
            self._log_retrieval(connection, query, hits, path="preview")
            return {
                "ok": True,
                "connection": connection,
                "query": query,
                "hits": [h.to_dict() for h in hits],
                "context": retr.format_context(hits),
                "preview": retr.format_preview(hits),
                "embedder_mismatch": self.embedder_mismatch(connection),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "hits": [], "context": "", "preview": "", "error": str(exc)}

    def context(self, connection: str, query: str, k: int = 8) -> dict[str, Any]:
        """Return the formatted, prompt-ready context block for a query."""
        try:
            retr = self._retriever()
            hits = retr.search(connection, query, k=k)
            self._log_retrieval(connection, query, hits, path="context")
            return {
                "ok": True,
                "connection": connection,
                "query": query,
                "context": retr.format_context(hits),
                "hits": [h.to_dict() for h in hits],
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "context": "", "hits": [], "error": str(exc)}

    def _log_retrieval(self, scope, query, hits, *, path: str = "search") -> None:
        """Append retrieval hits + scores to a JSONL log (observability).

        Gated by ``ai.rag log_retrievals``. Best-effort: never raises into the
        retrieval path. Mirrors the existing token-meter style of per-ask logs.
        """
        if not mc.get_bool("ai.rag", "log_retrievals", default=False):
            return
        try:
            import json as _json
            import time as _t

            log_path = self._index_path.parent / "retrievals.jsonl"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            rec = {
                "ts": _t.strftime("%Y-%m-%d %H:%M:%S"),
                "path": path,
                "scope": scope,
                "query": (query or "")[:500],
                "hits": [
                    {
                        "doc_id": getattr(h, "doc_id", ""),
                        "kind": getattr(h, "kind", ""),
                        "ref": getattr(h, "ref", ""),
                        "score": round(float(getattr(h, "score", 0.0)), 4),
                        "rerank": (getattr(h, "metadata", {}) or {}).get("rerank"),
                    }
                    for h in (hits or [])
                ],
            }
            with open(log_path, "a", encoding="utf-8") as fh:
                fh.write(_json.dumps(rec, default=str) + "\n")
        except Exception:  # noqa: BLE001
            pass

    def ask(
        self,
        connection: str,
        question: str,
        *,
        k: int = 8,
        backend: str | None = None,
        auto_index: bool = True,
    ) -> dict[str, Any]:
        """Generate RAG-augmented SQL for *question* on *connection*.

        Retrieves the relevant schema context, injects it into a focused prompt,
        and calls the active AI backend. Auto-builds the index on first use.
        """
        if self._agent is None:
            return {
                "sql": None,
                "explanation": None,
                "context": "",
                "hits": [],
                "error": "AI agent not available for RAG generation.",
            }
        try:
            store = self.store()
            if store.count(connection) == 0 and auto_index:
                idx = self.index(connection)
                if not idx.get("ok"):
                    return {
                        "sql": None,
                        "explanation": None,
                        "context": "",
                        "hits": [],
                        "error": idx.get("error") or "Failed to build RAG index.",
                    }

            # backend selection (mirror AIService.ai_query behaviour)
            if backend and not self._agent.set_backend(backend):
                return {
                    "sql": None,
                    "explanation": None,
                    "context": "",
                    "hits": [],
                    "error": f"AI backend '{backend}' is not available.",
                }
            if not backend and not self._agent.is_available():
                self._agent.auto_select_backend()

            retr = self._retriever()
            hits = retr.search(connection, question, k=k)
            context = retr.format_context(hits)
            db_type = self._resolve_db_type(connection)
            prompt = self._build_prompt(question, context, db_type)

            res = self._agent._call_ai(prompt)
            text = (res or {}).get("response") or ""
            if not text:
                return {
                    "sql": None,
                    "explanation": None,
                    "context": context,
                    "hits": [h.to_dict() for h in hits],
                    "error": (res or {}).get("error") or "AI returned no response.",
                }
            sql, explanation = self._parse_sql(text)
            return {
                "sql": sql,
                "explanation": explanation or text,
                "context": context,
                "hits": [h.to_dict() for h in hits],
                "db_type": db_type,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "sql": None,
                "explanation": None,
                "context": "",
                "hits": [],
                "error": str(exc),
            }

    def add_example(
        self, connection: str, question: str, sql: str, description: str = ""
    ) -> dict[str, Any]:
        """Persist a NL->SQL example (feedback loop) into the index."""
        try:
            from ai_assistant.llm.validation import validate_pair

            db_type = self._resolve_db_type(connection)
            ok, cleaned, reason = validate_pair(
                {"question": question, "sql": sql, "description": description},
                db_type=db_type,
            )
            if not ok:
                return {"ok": False, "error": reason or "Invalid NL->SQL pair."}
            qn = cleaned["question"]
            sqn = cleaned["sql"]
            description = cleaned.get("description") or ""
            doc_id = "example:" + re.sub(r"\W+", "_", qn.lower())[:60]
            text = (
                f"Example question: {qn}\n"
                + (f"Notes: {description}\n" if description else "")
                + f"SQL:\n{sqn}"
            )
            doc = Document(
                doc_id=doc_id,
                kind="example",
                ref=qn[:60],
                text=text,
                metadata={"question": qn, "sql": sqn, "description": description},
            )
            self._indexer().add_documents(connection, [doc])
            self.store().set_meta(
                connection,
                db_type=self._resolve_db_type(connection),
                provider=self.embedder().name,
                dim=self.embedder().dim,
            )
            return {"ok": True, "doc_id": doc_id, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def add_examples(
        self,
        connection: str,
        records: list[Mapping[str, Any]],
        *,
        standalone: bool = False,
    ) -> dict[str, Any]:
        """Bulk-add validated NL->SQL examples (feedback loop) into *connection*.

        *records* is a list of dicts with ``question``/``sql`` (and optional
        ``description``/``note``). Each pair is validated + de-duplicated through
        the shared LLM validator, so bad rows are reported, not indexed.
        """
        try:
            from ai_assistant.llm.validation import validate_pairs

            pairs = []
            for r in records or []:
                pairs.append({
                    "question": (r.get("question") or r.get("q") or "").strip(),
                    "sql": (r.get("sql") or "").strip(),
                    "description": (
                        r.get("description") or r.get("note") or r.get("category") or ""
                    ).strip(),
                })
            if not pairs:
                return {"ok": False, "error": "No example records provided.",
                        "added": 0, "skipped": 0}

            db_type = self._resolve_db_type(connection) if not standalone else None
            kept, stats = validate_pairs(pairs, db_type=db_type)
            if not kept:
                return {
                    "ok": False,
                    "added": 0,
                    "skipped": stats.get("rejected", 0),
                    "reasons": stats.get("reasons", {}),
                    "error": "No valid examples after validation.",
                }

            docs: list[Document] = []
            doc_ids: list[str] = []
            for pair in kept:
                qn = pair["question"]
                sqn = pair["sql"]
                description = pair.get("description") or ""
                doc_id = "example:" + re.sub(r"\W+", "_", qn.lower())[:60]
                text = (
                    f"Example question: {qn}\n"
                    + (f"Notes: {description}\n" if description else "")
                    + f"SQL:\n{sqn}"
                )
                docs.append(Document(
                    doc_id=doc_id,
                    kind="example",
                    ref=qn[:60],
                    text=text,
                    metadata={"question": qn, "sql": sqn, "description": description},
                ))
                doc_ids.append(doc_id)

            self._indexer().add_documents(connection, docs)
            self._touch_meta(connection, standalone=standalone)
            return {
                "ok": True,
                "added": len(docs),
                "skipped": stats.get("rejected", 0),
                "reasons": stats.get("reasons", {}),
                "doc_ids": doc_ids,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "added": 0, "skipped": 0, "error": str(exc)}

    @staticmethod
    def parse_examples(content: str, fmt: str = "auto", *, source: str = "") -> list[dict]:
        """Parse NL->SQL example records from *content*.

        Supported *fmt*: ``jsonl``, ``json``, ``csv``, ``tsv``, ``text`` (paired
        ``Q:``/``SQL:`` blocks). ``auto`` infers from *source* extension then
        from the content shape.
        """
        import csv as _csv
        import io as _io
        import json as _json

        text = content or ""
        fmt = (fmt or "auto").lower().lstrip(".")
        if fmt == "auto":
            ext = Path(source).suffix.lower().lstrip(".") if source else ""
            if ext in ("jsonl", "json", "csv", "tsv"):
                fmt = ext
            else:
                stripped = text.lstrip()
                if stripped.startswith("[") or stripped.startswith("{"):
                    fmt = "json" if stripped.startswith("[") else "jsonl"
                elif "\t" in text.splitlines()[0] if text.splitlines() else False:
                    fmt = "tsv"
                elif "," in (text.splitlines()[0] if text.splitlines() else ""):
                    fmt = "csv"
                else:
                    fmt = "text"

        records: list[dict] = []

        def _norm(d: Mapping[str, Any]) -> dict:
            low = { (k or "").strip().lower(): v for k, v in d.items() }
            return {
                "question": str(low.get("question") or low.get("q") or "").strip(),
                "sql": str(low.get("sql") or low.get("query") or "").strip(),
                "description": str(
                    low.get("description") or low.get("note") or low.get("category") or ""
                ).strip(),
            }

        if fmt == "jsonl":
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = _json.loads(line)
                    if isinstance(obj, dict):
                        records.append(_norm(obj))
                except Exception:  # noqa: BLE001
                    continue
        elif fmt == "json":
            try:
                data = _json.loads(text)
            except Exception:  # noqa: BLE001
                data = []
            if isinstance(data, dict):
                data = data.get("examples") or data.get("pairs") or []
            for obj in data or []:
                if isinstance(obj, dict):
                    records.append(_norm(obj))
        elif fmt in ("csv", "tsv"):
            delim = "\t" if fmt == "tsv" else ","
            reader = _csv.DictReader(_io.StringIO(text), delimiter=delim)
            for row in reader:
                records.append(_norm(row))
        else:  # paired text: Q:/SQL: blocks
            q, sql_lines, note = "", [], ""
            in_sql = False
            for raw in text.splitlines():
                line = raw.rstrip("\n")
                low = line.strip().lower()
                if low.startswith(("q:", "question:")):
                    if q and sql_lines:
                        records.append({"question": q, "sql": "\n".join(sql_lines).strip(),
                                        "description": note})
                    q = line.split(":", 1)[1].strip()
                    sql_lines, note, in_sql = [], "", False
                elif low.startswith(("sql:", "a:")):
                    sql_lines = [line.split(":", 1)[1].strip()]
                    in_sql = True
                elif low.startswith(("note:", "description:")):
                    note = line.split(":", 1)[1].strip()
                    in_sql = False
                elif in_sql and line.strip():
                    sql_lines.append(line)
            if q and sql_lines:
                records.append({"question": q, "sql": "\n".join(sql_lines).strip(),
                                "description": note})

        return [r for r in records if r.get("question") or r.get("sql")]

    def add_examples_from_file(
        self,
        connection: str,
        file_path: str = "",
        *,
        content: str = "",
        fmt: str = "auto",
        standalone: bool = False,
    ) -> dict[str, Any]:
        """Import NL->SQL examples from a file path (CLI/desktop) or raw *content*
        (web upload read client-side). See :meth:`parse_examples` for formats."""
        try:
            source = file_path or ""
            body = content
            if file_path and not content:
                p = Path(file_path).expanduser()
                if not p.is_file():
                    return {"ok": False, "error": f"File not found: {p}",
                            "added": 0, "skipped": 0}
                try:
                    body = p.read_text(encoding="utf-8", errors="replace")
                except Exception as exc:  # noqa: BLE001
                    return {"ok": False, "error": f"Failed to read {p.name}: {exc}",
                            "added": 0, "skipped": 0}
            if not (body or "").strip():
                return {"ok": False, "error": "No example content to import.",
                        "added": 0, "skipped": 0}
            records = self.parse_examples(body, fmt, source=source)
            if not records:
                return {"ok": False,
                        "error": "No example records found (expected JSONL/JSON/CSV/TSV/Q:SQL: text).",
                        "added": 0, "skipped": 0}
            out = self.add_examples(connection, records, standalone=standalone)
            out["parsed"] = len(records)
            if source:
                out["source"] = Path(source).name
            return out
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "added": 0, "skipped": 0, "error": str(exc)}

    def add_glossary(
        self, connection: str, term: str, definition: str
    ) -> dict[str, Any]:
        """Persist a business-glossary term into the index."""
        try:
            t = (term or "").strip()
            d = (definition or "").strip()
            if not t or not d:
                return {"ok": False, "error": "Both term and definition are required."}
            doc_id = "glossary:" + re.sub(r"\W+", "_", t.lower())[:60]
            doc = Document(
                doc_id=doc_id,
                kind="glossary",
                ref=t,
                text=f"Business term: {t}\nDefinition: {d}",
                metadata={"term": t, "definition": d},
            )
            self._indexer().add_documents(connection, [doc])
            return {"ok": True, "doc_id": doc_id, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── documents (uploaded reference knowledge) ───────────────────────────

    def add_document(
        self,
        request: RagDocumentRequest | str,
        **legacy_fields,
    ) -> dict[str, Any]:
        """Chunk + embed a document into *scope*'s RAG index (kind=document).

        Provide either raw ``text`` (e.g. pasted, or a file read client-side in
        the web UI) or a server-side ``file_path`` (CLI / desktop). *scope* is a
        connection name or a free-form standalone collection label.
        """
        request = RagDocumentRequest.from_call(request, legacy_fields)
        scope = request.scope
        text = request.text
        file_path = request.file_path
        title = request.title
        source = request.source
        standalone = request.standalone
        chunk_size = request.chunk_size
        overlap = request.overlap
        try:
            if not (scope or "").strip():
                return {"ok": False, "error": "A scope (connection or collection name) is required."}
            body = text or ""
            if file_path:
                body, err = document_loader.load_file(file_path)
                if err:
                    return {"ok": False, "error": err}
                if not source:
                    source = Path(file_path).name
                if not title:
                    title = Path(file_path).stem
            body = (body or "").strip()
            if not body:
                return {"ok": False, "error": "No document text to index."}
            if not source:
                source = (title or "pasted-text").strip()
            if not title:
                title = source

            cs = chunk_size if chunk_size is not None else mc.get_int(
                "ai.rag", "chunk_size", default=1000)
            ov = overlap if overlap is not None else mc.get_int(
                "ai.rag", "chunk_overlap", default=150)
            chunks = document_loader.chunk_text(body, chunk_size=cs, overlap=ov)
            if not chunks:
                return {"ok": False, "error": "Document produced no chunks."}

            src_slug = _slug(source)
            # Replace any prior version of the same source before re-adding.
            self.store().delete_doc_prefix(scope, f"document:{src_slug}:")
            docs = []
            for i, chunk in enumerate(chunks):
                docs.append(Document(
                    doc_id=f"document:{src_slug}:{i}",
                    kind="document",
                    ref=title,
                    text=f"[{title}] {chunk}",
                    metadata={
                        "source": source,
                        "title": title,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                    },
                ))
            self._indexer().add_documents(scope, docs)
            self._touch_meta(scope, standalone=standalone)
            return {
                "ok": True, "scope": scope, "source": source, "title": title,
                "chunks": len(chunks), "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def documents(self, scope: str) -> dict[str, Any]:
        """List uploaded documents in *scope*, grouped by source."""
        try:
            rows = self.store().list_by_kind(scope, "document")
            grouped: dict[str, dict[str, Any]] = {}
            for r in rows:
                meta = r.get("metadata") or {}
                src = meta.get("source") or r.get("ref") or "?"
                g = grouped.setdefault(src, {
                    "source": src,
                    "title": meta.get("title") or src,
                    "chunks": 0,
                })
                g["chunks"] += 1
            return {"ok": True, "scope": scope,
                    "documents": list(grouped.values()), "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "documents": [], "error": str(exc)}

    def remove_document(self, scope: str, source: str) -> dict[str, Any]:
        """Remove every chunk of one uploaded document from *scope*."""
        try:
            removed = self.store().delete_doc_prefix(scope, f"document:{_slug(source)}:")
            return {"ok": True, "scope": scope, "source": source,
                    "removed": removed, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── analytical query library ───────────────────────────────────────────

    def analytics_library(self) -> dict[str, Any]:
        """Return the built-in, schema-agnostic analytical query patterns."""
        return {
            "ok": True,
            "categories": analytics_library.categories(),
            "queries": analytics_library.ANALYTICAL_QUERIES,
            "placeholders": analytics_library.PLACEHOLDERS,
            "error": None,
        }

    def seed_analytics(
        self, scope: str, categories: list[str] | None = None,
        *, standalone: bool = False,
    ) -> dict[str, Any]:
        """Seed generic analytical NL->SQL patterns into *scope* (kind=analytical).

        These patterns ground retrieval and also feed the local LLM trainer.
        """
        try:
            if not (scope or "").strip():
                return {"ok": False, "error": "A scope is required."}
            entries = analytics_library.queries_for(categories)
            if not entries:
                return {"ok": False, "error": "No analytical patterns matched."}
            docs = []
            for q in entries:
                docs.append(Document(
                    doc_id=f"analytical:{q['category']}:{_slug(q['question'])}",
                    kind="analytical",
                    ref=q["question"],
                    text=(f"Analytical pattern ({q['category']}): {q['question']}\n"
                          + (f"Note: {q['note']}\n" if q.get("note") else "")
                          + f"SQL:\n{q['sql']}"),
                    metadata={
                        "question": q["question"], "sql": q["sql"],
                        "category": q["category"], "note": q.get("note", ""),
                    },
                ))
            self._indexer().add_documents(scope, docs)
            self._touch_meta(scope, standalone=standalone)
            return {"ok": True, "scope": scope, "seeded": len(docs), "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── inventory ──────────────────────────────────────────────────────────

    def breakdown(self, scope: str) -> dict[str, Any]:
        """Return a per-kind document breakdown + meta for *scope* (informative UI)."""
        try:
            store = self.store()
            counts = store.counts_by_kind(scope)
            meta = store.get_meta(scope) or {}
            return {
                "ok": True,
                "scope": scope,
                "counts": counts,
                "total": sum(counts.values()),
                "meta": meta,
                "embedder_mismatch": self._embedder_mismatch_meta(meta, self.embedder()),
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "counts": {}, "total": 0, "error": str(exc)}

    def add_codebase(
        self,
        folder: str,
        scope: str,
        *,
        standalone: bool = True,
        chunk_size: int | None = None,
        overlap: int | None = None,
        max_files: int | None = None,
        replace: bool = True,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        """Index a source tree into *scope* as kind=code chunks."""
        from ai_assistant.rag.codebase_indexer import index_codebase

        scope = (scope or "").strip()
        if not scope:
            return {"ok": False, "error": "A scope name is required."}
        cs = chunk_size if chunk_size is not None else mc.get_int("ai.rag", "chunk_size", default=1000)
        ov = overlap if overlap is not None else mc.get_int("ai.rag", "chunk_overlap", default=150)
        mf = max_files if max_files is not None else mc.get_int("ai.rag", "codebase_max_files", default=500)
        mfb = mc.get_int("ai.rag", "codebase_max_file_bytes", default=512000)

        docs, summary = index_codebase(
            folder, scope,
            chunk_size=cs, overlap=ov, max_files=mf, max_file_bytes=mfb,
            on_progress=on_progress,
        )
        if not summary.get("ok") or not docs:
            return {"ok": False, **summary}

        if replace:
            self.store().delete_doc_prefix(scope, "code:")

        indexed = self._indexer().add_documents(scope, docs)
        self._touch_meta(scope, standalone=standalone)
        return {
            "ok": True,
            "scope": scope,
            "folder": summary.get("folder"),
            "files_scanned": summary.get("files_scanned", 0),
            "chunks": indexed,
            "errors": summary.get("errors") or [],
            "error": None,
        }

    def clear(self, connection: str) -> dict[str, Any]:
        """Delete the entire RAG index for *connection*."""
        try:
            removed = self.store().delete_connection(connection)
            return {"ok": True, "removed": removed, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── helpers ───────────────────────────────────────────────────────────

    def _meta_db_type(self, scope: str) -> str:
        """Resolve a db_type for meta, returning '' for standalone collections."""
        try:
            profile = self._core.get_connection_profile(scope)
            if profile and profile.get("db_type"):
                return profile["db_type"]
        except Exception:
            pass
        try:
            mgr = self._core.get_manager(scope)
            return getattr(mgr, "db_type", "") or ""
        except Exception:
            return ""

    def _touch_meta(self, scope: str, *, standalone: bool = False) -> None:
        """Refresh rag_meta for *scope* so it appears in status listings."""
        db_type = "" if standalone else self._meta_db_type(scope)
        self.store().set_meta(
            scope,
            db_type=db_type,
            provider=self.embedder().name,
            dim=self.embedder().dim,
        )

    def _resolve_db_type(self, connection: str) -> str:
        try:
            profile = self._core.get_connection_profile(connection)
            if profile:
                return profile.get("db_type") or "SQL"
        except Exception:
            pass
        # fall back to a live manager if available
        try:
            mgr = self._core.get_manager(connection)
            return getattr(mgr, "db_type", "SQL") or "SQL"
        except Exception:
            return "SQL"

    @staticmethod
    def _build_prompt(question: str, context: str, db_type: str) -> str:
        return f"""You are an expert {db_type} SQL generator using RETRIEVAL-AUGMENTED context.

{context}

RULES:
1. Use ONLY tables/columns that appear in the retrieved context above.
2. If a needed table/column is not in the context, say so instead of guessing.
3. Generate valid {db_type} SQL.

Respond in EXACTLY this format:
SQL:
```sql
<your single SQL statement>
```
EXPLANATION:
<one or two sentences explaining the query>

USER QUESTION: {question}
"""

    @staticmethod
    def _parse_sql(text: str) -> tuple[Optional[str], Optional[str]]:
        """Extract the SQL (fenced block preferred) and explanation from a reply."""
        sql = None
        m = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if m:
            sql = m.group(1).strip()
        else:
            m = re.search(r"```\s*(.*?)```", text, re.DOTALL)
            if m:
                sql = m.group(1).strip()
            else:
                m = re.search(r"\bSQL:\s*(.+?)(?:\nEXPLANATION:|\Z)", text,
                              re.DOTALL | re.IGNORECASE)
                if m:
                    sql = m.group(1).strip().strip("`")

        explanation = None
        em = re.search(r"EXPLANATION:\s*(.+)", text, re.DOTALL | re.IGNORECASE)
        if em:
            explanation = em.group(1).strip()
        return (sql or None), explanation
