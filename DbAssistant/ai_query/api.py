"""
REST API surface for the AI Query Assistant module.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field
try:
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v1 fallback
    ConfigDict = None

# Imported at module scope so postponed annotations (``from __future__ import
# annotations``) on endpoint params like ``request: Request`` resolve against
# this module's globals during OpenAPI generation. FastAPI is otherwise imported
# lazily inside build_router; this guarded import keeps api.py importable without
# fastapi installed (build_router would raise a clear error first in that case).
try:  # pragma: no cover - trivial import guard
    from fastapi import Request
except Exception:  # pragma: no cover
    Request = None  # type: ignore[assignment,misc]


class AIQueryRequest(BaseModel):
    connection: str = Field(..., examples=["my_mysql"])
    question: str = Field(..., examples=["show tables with more than 1000 rows"])
    backend: str = Field("", examples=[""], description="Optional AI backend; blank = auto")
    sql_mode: str = Field(
        "",
        description="strict_summary | summary | open (default: summary)",
    )
    sql_execution_rules: str = Field(
        "",
        description="Optional execution rules text (summary/open modes)",
    )


class SessionCreate(BaseModel):
    connection: str = ""
    backend: str = ""
    isolated: bool = False
    share_context: bool = True
    sql_mode: str = Field("", description="strict_summary | summary | open")
    sql_execution_rules: str = ""


class SessionMessage(BaseModel):
    message: str
    mode: str = Field("ask", description="ask or followup")


class SessionCrossTab(BaseModel):
    instruction: str


class SessionExecuteSQL(BaseModel):
    sql: str


class AIExecuteSQL(BaseModel):
    connection: str = Field(..., examples=["my_mysql"])
    sql: str = Field(..., examples=["SELECT * FROM users LIMIT 10"])


class SessionPatch(BaseModel):
    connection: str | None = None
    backend: str | None = None
    share_context: bool | None = None
    isolated: bool | None = None
    sql_mode: str | None = None
    sql_execution_rules: str | None = None


class SessionSaveLoad(BaseModel):
    path: str = ""


class AiConfigSet(BaseModel):
    section: str
    key: str
    value: str


class SQLAnalyseRequest(BaseModel):
    sql: str = Field(..., min_length=1, examples=["SELECT * FROM users"])
    connection: str = Field("", examples=["my_mysql"])
    db_type: str = Field("", examples=["MySQL", "PostgreSQL"])


class SQLReviewRequest(SQLAnalyseRequest):
    rules: str = Field("", description="Optional review rules text")
    timeout: Optional[int] = Field(
        None, ge=5, le=600,
        description="Review timeout (seconds); blank => [ui.ai_query] sql_review_timeout")


class BackendConfigureRequest(BaseModel):
    backend: str = Field(..., examples=["claude"])
    verify: bool = Field(True, description="Skip availability check when false")


class FallbackBackendRequest(BaseModel):
    backend: str = Field("", examples=["claude"],
                         description="Fallback backend; blank clears it")
    verify: bool = Field(True, description="Skip availability check when false")


class CorrectSqlRequest(BaseModel):
    question: str = Field(..., examples=["list all customers"])
    sql: str = Field(..., examples=["SELECT * FROM customer"])
    connection: str = Field("", description="Connection for schema/dialect context")
    db_type: str = Field("", description="Dialect override; blank => from connection")
    error_text: str = Field("", description="Execution error text (mode=syntax)")
    mode: str = Field("syntax", examples=["syntax", "interpretation"],
                      description="'syntax' fixes failures; 'interpretation' fixes intent")
    backend: str = Field("", description="Override corrector backend; blank => fallback")


class PIIToggleRequest(BaseModel):
    enabled: bool = Field(..., examples=[True])


# ── RAG (retrieval-augmented Generate SQL) ───────────────────────────────────
class RagIndexRequest(BaseModel):
    connection: str = Field(..., examples=["my_mysql"])
    rebuild: bool = Field(False, description="Drop and rebuild the index")


class RagQueryRequest(BaseModel):
    connection: str = Field(..., examples=["my_mysql"])
    query: str = Field(..., examples=["which customers spent the most?"])
    k: Optional[int] = Field(None, ge=1, le=50,
                             description="Top-K hits; blank => [ai.rag] top_k")


class RagAskRequest(BaseModel):
    connection: str
    question: str
    k: Optional[int] = Field(None, ge=1, le=50,
                             description="Top-K hits; blank => [ai.rag] top_k")
    backend: str = ""


class RagExampleRequest(BaseModel):
    connection: str
    question: str
    sql: str
    description: str = ""


class RagExamplesFileRequest(BaseModel):
    connection: str
    content: str = Field("", description="Raw file text; preferred for remote Web UI")
    file_path: str = Field("", description="Server-side file path; local/CLI use only")
    fmt: str = Field("auto", description="auto|jsonl|json|csv|tsv|text")
    standalone: bool = False


class RagGlossaryRequest(BaseModel):
    connection: str
    term: str
    definition: str


class RagDocumentRequest(BaseModel):
    scope: str = Field(..., description="Connection name or standalone collection")
    content: str = Field("", description="Raw document text; preferred for remote Web UI")
    file_path: str = Field("", description="Server-side file path; local/CLI use only")
    title: str = ""
    source: str = Field("", description="Document name or filename")
    standalone: bool = False


class RagRemoveDocumentRequest(BaseModel):
    scope: str
    source: str


class RagSeedAnalyticsRequest(BaseModel):
    scope: str
    categories: list[str] = Field(default_factory=list)
    standalone: bool = False


class RagMultiSearchRequest(BaseModel):
    scopes: list[str] = Field(..., description="Scope/collection names to search across")
    query: str
    k: int | None = Field(None, description="Top-K hits; blank => [ai.rag] top_k")


class RagEvalRequest(BaseModel):
    connection: str
    gold: list[dict] | None = Field(
        None, description="Gold cases (question + tables). Omit to seed from examples.")
    k: int | None = Field(None, description="Top-K hits; blank => [ai.rag] top_k")
    per_case: bool = Field(False, description="Include per-case metrics")


class RagReindexStaleRequest(BaseModel):
    connections: list[str] = Field(
        default_factory=list, description="Connections to check (empty => all indexed)")
    force: bool = Field(False, description="Re-index regardless of staleness")


class RagCodebaseRequest(BaseModel):
    folder: str = Field(..., description="Path to application source root")
    scope: str = Field(..., description="RAG scope / collection name")
    standalone: bool = Field(True, description="Standalone scope (not a DB connection)")
    replace: bool = Field(True, description="Replace prior code: chunks in this scope")
    max_files: int = Field(0, ge=0, description="Cap files scanned (0=config default)")


# ── Local trainable NL->SQL LLM ──────────────────────────────────────────────
class LlmTrainRequest(BaseModel):
    name: str = "default"
    engine: str = Field("", description="python|numpy|pytorch|ollama (blank=config)")
    include_sample: bool = True
    dataset_path: str = ""
    rag_connection: str = Field(
        "", description="Fold this connection's saved RAG examples into training")


class LlmRestoreRequest(BaseModel):
    name: str = "default"
    version: str = Field(..., description="Version id from /api/ai/llm/versions")


class LlmEnrichTemplatesRequest(BaseModel):
    backend: str = Field("", description="AI backend for enrichment (blank => active)")
    db_types: list[str] = Field(
        default_factory=list,
        description="Dialects to enrich; empty => all SQL dialects")
    connections: list[str] = Field(
        default_factory=list,
        description="Connections for optional live catalog validation")
    questions_file: str = Field(
        "", description="Intents file path; blank => built-in intent set")
    limit_per_type: int = Field(0, ge=0, description="Cap intents per dialect (0=all)")
    persist: bool = Field(True, description="Persist accepted templates to the store")


class LlmHarvestRequest(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "allow"
    template_mode: str = Field(
        "both",
        description="concrete | placeholder | both — how object templates are trained",
    )


class LlmGenerateRequest(BaseModel):
    question: str
    name: str = "default"
    engine: str = ""
    max_new: int = Field(0, ge=0, le=2048,
                         description="Max generated tokens; 0 => [ai.llm] max_new_tokens")
    temperature: Optional[float] = Field(
        None, ge=0.0, le=2.0,
        description="Sampling temperature; blank => [ai.llm] temperature")
    connection: str = ""
    alternatives: bool = Field(
        False, description="Also return alternative SQL syntaxes saved for the question")


class LlmEvalRequest(BaseModel):
    name: str = "default"
    connection: str = ""
    depth: str = Field("", description="lightweight|full (blank=config)")
    include_sample: bool = False
    rag_connection: str = ""
    dataset_path: str = ""


class LlmExportRequest(BaseModel):
    path: str
    include_sample: bool = True
    rag_connection: str = ""


class LlmTrainRichRequest(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "allow"


class LlmTrainMultiRequest(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "allow"


class LlmMineTrainingPairsRequest(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "allow"


class LlmDatasetRequest(BaseModel):
    include_sample: bool = True
    rag_connection: str = ""


def build_router(svc=None):
    import re as _re
    import threading as _threading
    import time as _time

    from fastapi import APIRouter, HTTPException

    from ai_query import module_config as _mc

    if svc is None:
        from ai_query.service import make_service

        svc = make_service()

    router = APIRouter(tags=["AI"])

    # Identifiers that arrive as path/body params and feed lookups; restrict to a
    # safe charset so they can't be used for traversal/enumeration tricks.
    _ID_RE = _re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")

    def _error(detail: str, status: int = 400):
        raise HTTPException(status_code=status, detail=detail)

    def _validate_id(value: str, *, what: str = "id") -> str:
        v = (value or "").strip()
        if not _ID_RE.match(v):
            _error(f"Invalid {what} format.", 400)
        return v

    class _RateLimiter:
        """Simple in-process sliding-window limiter, keyed by client identity."""

        def __init__(self) -> None:
            self._hits: dict[str, list[float]] = {}
            self._lock = _threading.Lock()

        def check(self, key: str) -> bool:
            max_calls = _mc.get_int("ai.limits", "api_rate_limit", default=30)
            window = _mc.get_int("ai.limits", "api_rate_window", default=60)
            if max_calls <= 0:
                return True  # limiter disabled
            now = _time.monotonic()
            cutoff = now - max(1, window)
            with self._lock:
                q = self._hits.setdefault(key, [])
                while q and q[0] < cutoff:
                    q.pop(0)
                if len(q) >= max_calls:
                    return False
                q.append(now)
                # Opportunistic cleanup so the dict can't grow without bound.
                if len(self._hits) > 4096:
                    for k in [k for k, v in self._hits.items() if not v]:
                        self._hits.pop(k, None)
                return True

    _ai_rate_limiter = _RateLimiter()

    def _enforce_rate(request: Request) -> None:
        client = getattr(request, "client", None)
        key = (getattr(client, "host", None) or "anon")
        if not _ai_rate_limiter.check(key):
            _error("Rate limit exceeded for AI endpoints. Try again shortly.", 429)

    def _model_dump(req) -> dict:
        if hasattr(req, "model_dump"):
            return req.model_dump()
        return req.dict()

    def _resolve_home_path(file_path: str) -> str:
        import os as _os
        from pathlib import Path as _Path

        try:
            resolved = _Path(file_path).expanduser().resolve()
        except Exception:
            _error("Invalid file_path.", 400)
        home = _Path(_os.path.expanduser("~")).resolve()
        try:
            inside = resolved.is_relative_to(home)
        except AttributeError:
            inside = home == resolved or home in resolved.parents
        if not inside:
            _error("file_path must be within the user home directory.", 400)
        return str(resolved)

    def _resolve_export_path(path: str) -> str:
        from pathlib import Path as _Path
        from common import paths as _paths

        root = (_paths.dbassistant_home() / "exports").resolve()
        raw = _Path(path).expanduser()
        candidate = raw if raw.is_absolute() else root / raw
        try:
            resolved = candidate.resolve()
        except Exception:
            _error("Invalid export path.", 400)
        try:
            inside = resolved.is_relative_to(root)
        except AttributeError:
            inside = root == resolved or root in resolved.parents
        if not inside:
            _error("Export path must stay within the DBASSISTANT_HOME exports directory.", 400)
        return str(resolved)

    @router.post("/api/ai/query")
    def ai_query(req: AIQueryRequest, request: Request):
        """Convert a natural-language question to SQL using the AI agent."""
        _enforce_rate(request)
        r = svc.ai_query(
            req.connection,
            req.question,
            backend=req.backend or None,
            sql_mode=req.sql_mode or None,
            sql_execution_rules=req.sql_execution_rules or None,
        )
        if r.get("error") and not r.get("sql"):
            _error(r["error"])
        return r

    @router.post("/api/ai/execute-sql")
    def ai_execute_sql(req: AIExecuteSQL):
        """Execute AI-generated SQL with a hard read-only guard (no session).

        Rejects any data/schema-mutating statement (DROP/DELETE/UPDATE/...).
        """
        r = svc.ai_execute_sql(req.connection, req.sql)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/ai/backends")
    def ai_backends():
        """List configured AI backends and which are verified available."""
        info = svc.list_ai_backends()
        if not info.get("available"):
            _error(info.get("error") or "AI not available.")
        return info

    @router.post("/api/ai/sessions")
    def ai_session_create(req: SessionCreate):
        r = svc.ai_session_create(
            req.connection,
            req.backend or None,
            isolated=req.isolated,
            share_context=req.share_context,
            sql_mode=req.sql_mode or None,
            sql_execution_rules=req.sql_execution_rules or None,
        )
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/ai/sessions")
    def ai_session_list():
        r = svc.ai_session_list()
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/ai/sessions/{session_id}")
    def ai_session_get(session_id: str):
        session_id = _validate_id(session_id, what="session id")
        r = svc.ai_session_get(session_id)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.patch("/api/ai/sessions/{session_id}")
    def ai_session_patch(session_id: str, req: SessionPatch):
        session_id = _validate_id(session_id, what="session id")
        r = svc.ai_session_update(session_id, **req.model_dump(exclude_unset=True))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.delete("/api/ai/sessions/{session_id}")
    def ai_session_delete(session_id: str):
        session_id = _validate_id(session_id, what="session id")
        r = svc.ai_session_delete(session_id)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/sessions/{session_id}/messages")
    def ai_session_message(session_id: str, req: SessionMessage, request: Request):
        session_id = _validate_id(session_id, what="session id")
        _enforce_rate(request)
        mode = "followup" if req.mode == "followup" else "ask"
        r = svc.ai_session_ask(session_id, req.message, mode=mode)
        if r.get("error") and not r.get("sql"):
            _error(r["error"])
        return r

    @router.post("/api/ai/sessions/{session_id}/cross-tab")
    def ai_session_cross(session_id: str, req: SessionCrossTab, request: Request):
        session_id = _validate_id(session_id, what="session id")
        _enforce_rate(request)
        r = svc.ai_session_cross_tab(session_id, req.instruction)
        # Fail closed: a downstream error must surface as a non-2xx status even
        # when the instruction was routed to another tab, so HTTP-status-based
        # clients don't silently miss the failure.
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/sessions/{session_id}/execute-sql")
    def ai_session_execute_sql(session_id: str, req: SessionExecuteSQL):
        """Execute SQL with session sql_mode and execution rules."""
        session_id = _validate_id(session_id, what="session id")
        r = svc.ai_session_execute_sql(session_id, req.sql)
        if r.get("error") and not r.get("result"):
            _error(r["error"])
        return r

    @router.post("/api/ai/sessions/save")
    def ai_sessions_save(req: SessionSaveLoad):
        r = svc.ai_session_save(req.path or None)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/sessions/load")
    def ai_sessions_load(req: SessionSaveLoad):
        r = svc.ai_session_load(req.path or None)
        if r.get("error"):
            _error(r["error"])
        return r

    # -- Phase 6 parity ----------------------------------------------------

    @router.post("/api/ai/explain")
    def ai_explain(req: SQLAnalyseRequest, request: Request):
        _enforce_rate(request)
        if not hasattr(svc, "explain_sql"):
            _error("explain_sql not supported.", 501)
        r = svc.explain_sql(req.sql, connection=req.connection, db_type=req.db_type)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/optimize")
    def ai_optimize(req: SQLAnalyseRequest, request: Request):
        _enforce_rate(request)
        if not hasattr(svc, "optimize_sql"):
            _error("optimize_sql not supported.", 501)
        r = svc.optimize_sql(req.sql, connection=req.connection, db_type=req.db_type)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/review")
    def ai_review(req: SQLReviewRequest, request: Request):
        _enforce_rate(request)
        if not hasattr(svc, "review_sql"):
            _error("review_sql not supported.", 501)
        r = svc.review_sql(
            req.sql, rules=req.rules, connection=req.connection,
            db_type=req.db_type, timeout=req.timeout,
        )
        if r.get("error"):
            _error(r["error"])
        return r

    @router.put("/api/ai/backend")
    def ai_backend_configure(req: BackendConfigureRequest):
        if not hasattr(svc, "configure_ai_backend"):
            _error("configure_ai_backend not supported.", 501)
        r = svc.configure_ai_backend(req.backend, verify=req.verify)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.put("/api/ai/fallback-backend")
    def ai_fallback_backend_configure(req: FallbackBackendRequest):
        if not hasattr(svc, "configure_ai_fallback_backend"):
            _error("configure_ai_fallback_backend not supported.", 501)
        r = svc.configure_ai_fallback_backend(req.backend, verify=req.verify)
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/ai/correct-sql")
    def ai_correct_sql(req: CorrectSqlRequest):
        if not hasattr(svc, "correct_sql"):
            _error("correct_sql not supported.", 501)
        r = svc.correct_sql(
            req.question, req.sql,
            connection=req.connection, db_type=req.db_type,
            error_text=req.error_text, mode=req.mode, backend=req.backend,
        )
        if r.get("error") and not r.get("sql"):
            _error(r["error"])
        return r

    @router.get("/api/ai/cache")
    def ai_cache_info():
        if not hasattr(svc, "get_ai_cache_info"):
            _error("get_ai_cache_info not supported.", 501)
        r = svc.get_ai_cache_info()
        if r.get("error"):
            _error(r["error"])
        return r

    @router.delete("/api/ai/cache")
    def ai_cache_clear(connection: str = ""):
        if not hasattr(svc, "clear_ai_cache"):
            _error("clear_ai_cache not supported.", 501)
        r = svc.clear_ai_cache(connection or None)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.get("/api/ai/cache/show")
    def ai_cache_show(connection: str = ""):
        if not hasattr(svc, "show_ai_cache"):
            _error("show_ai_cache not supported.", 501)
        r = svc.show_ai_cache(connection or "")
        if r.get("error"):
            _error(r["error"], 404 if "No cached" in r["error"] else 400)
        return r

    @router.get("/api/ai/pii")
    def ai_pii_status():
        if not hasattr(svc, "get_pii_masking"):
            _error("get_pii_masking not supported.", 501)
        r = svc.get_pii_masking()
        if r.get("error"):
            _error(r["error"])
        return r

    @router.put("/api/ai/pii")
    def ai_pii_set(req: PIIToggleRequest):
        if not hasattr(svc, "set_pii_masking"):
            _error("set_pii_masking not supported.", 501)
        r = svc.set_pii_masking(req.enabled)
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.get("/api/ai/config", tags=["Config"])
    def ai_config_get():
        from ai_query import module_config as mc
        return {
            "ok": True,
            "config": {s: {k: mc.get(s, k) for k in keys} for s, keys in mc.DEFAULTS.items()},
            "path": str(mc.config_path() or mc.live_path()),
        }

    @router.post("/api/ai/config", tags=["Config"])
    def ai_config_set(req: AiConfigSet):
        from ai_query import module_config as mc
        if req.section not in mc.DEFAULTS or req.key not in mc.DEFAULTS[req.section]:
            _error(f"Unknown setting {req.section}.{req.key}")
        mc.set_value(req.section, req.key, req.value)
        return {"ok": True, "message": f"{req.section}.{req.key} saved."}

    @router.post("/api/ai/config/restore", tags=["Config"])
    def ai_config_restore():
        from ai_query import module_config as mc
        mc.restore_defaults()
        return {"ok": True, "message": "config.ini restored."}

    # -- RAG (retrieval-augmented Generate SQL) ----------------------------

    @router.post("/api/ai/rag/index", tags=["RAG"])
    def rag_index(req: RagIndexRequest):
        r = svc.rag_index(req.connection, rebuild=req.rebuild)
        if not r.get("ok"):
            _error(r.get("error") or "Indexing failed.")
        return r

    @router.get("/api/ai/rag/status", tags=["RAG"])
    def rag_status(connection: str = ""):
        r = svc.rag_status(connection or "")
        if not r.get("ok"):
            _error(r.get("error") or "Status failed.")
        return r

    @router.post("/api/ai/rag/search", tags=["RAG"])
    def rag_search(req: RagQueryRequest):
        r = svc.rag_search(req.connection, req.query, k=req.k)
        if not r.get("ok"):
            _error(r.get("error") or "Search failed.")
        return r

    @router.post("/api/ai/rag/context", tags=["RAG"])
    def rag_context(req: RagQueryRequest):
        r = svc.rag_context(req.connection, req.query, k=req.k)
        if not r.get("ok"):
            _error(r.get("error") or "Context failed.")
        return r

    @router.post("/api/ai/rag/ask", tags=["RAG"])
    def rag_ask(req: RagAskRequest):
        r = svc.rag_ask(req.connection, req.question, k=req.k,
                        backend=req.backend or None)
        if r.get("error") and not r.get("sql"):
            _error(r["error"])
        return r

    @router.post("/api/ai/rag/example", tags=["RAG"])
    def rag_example(req: RagExampleRequest):
        r = svc.rag_add_example(req.connection, req.question, req.sql, req.description)
        if not r.get("ok"):
            _error(r.get("error") or "Add example failed.")
        return r

    @router.post("/api/ai/rag/examples-file", tags=["RAG"])
    def rag_examples_file(req: RagExamplesFileRequest):
        r = svc.rag_add_examples_from_file(
            req.connection, req.file_path, content=req.content,
            fmt=req.fmt, standalone=req.standalone,
        )
        if not r.get("ok"):
            _error(r.get("error") or "Example import failed.")
        return r

    @router.post("/api/ai/rag/glossary", tags=["RAG"])
    def rag_glossary(req: RagGlossaryRequest):
        r = svc.rag_add_glossary(req.connection, req.term, req.definition)
        if not r.get("ok"):
            _error(r.get("error") or "Add glossary failed.")
        return r

    @router.post("/api/ai/rag/document", tags=["RAG"])
    def rag_document(req: RagDocumentRequest):
        """Index a document into a connection or standalone RAG scope.

        The Web UI should prefer ``content`` so uploads remain remote-safe:
        text is read in the browser and the server never dereferences a user
        path. ``file_path`` is retained for local/CLI compatibility.
        """
        file_path = req.file_path or None
        if file_path:
            # Reject path traversal attempts. Resolve to an absolute path and
            # confirm it doesn't escape the user's home directory. This is a
            # defence-in-depth measure — the primary safeguard is to send
            # content (text) rather than file_path from remote callers.
            file_path = _resolve_home_path(file_path)
        r = svc.rag_add_document(
            req.scope,
            text=req.content or None,
            file_path=file_path,
            title=req.title,
            source=req.source,
            standalone=req.standalone,
        )
        if not r.get("ok"):
            _error(r.get("error") or "Add document failed.")
        return r

    @router.get("/api/ai/rag/documents", tags=["RAG"])
    def rag_documents(scope: str):
        r = svc.rag_documents(scope)
        if not r.get("ok"):
            _error(r.get("error") or "List documents failed.")
        return r

    @router.post("/api/ai/rag/remove-document", tags=["RAG"])
    def rag_remove_document(req: RagRemoveDocumentRequest):
        r = svc.rag_remove_document(req.scope, req.source)
        if not r.get("ok"):
            _error(r.get("error") or "Remove document failed.")
        return r

    @router.get("/api/ai/rag/analytics", tags=["RAG"])
    def rag_analytics():
        return svc.rag_analytics_library()

    @router.post("/api/ai/rag/seed-analytics", tags=["RAG"])
    def rag_seed_analytics(req: RagSeedAnalyticsRequest):
        r = svc.rag_seed_analytics(
            req.scope, req.categories or None, standalone=req.standalone
        )
        if not r.get("ok"):
            _error(r.get("error") or "Seed analytics failed.")
        return r

    @router.get("/api/ai/rag/breakdown", tags=["RAG"])
    def rag_breakdown(scope: str):
        r = svc.rag_breakdown(scope)
        if not r.get("ok"):
            _error(r.get("error") or "Breakdown failed.")
        return r

    @router.get("/api/ai/rag/overview", tags=["RAG"])
    def rag_overview(scope: str):
        r = svc.rag_scope_overview(scope)
        if not r.get("ok"):
            _error(r.get("error") or "Overview failed.")
        return r

    @router.post("/api/ai/rag/preview", tags=["RAG"])
    def rag_preview(req: RagQueryRequest):
        r = svc.rag_preview(req.connection, req.query, k=req.k)
        if not r.get("ok"):
            _error(r.get("error") or "Preview failed.")
        return r

    @router.post("/api/ai/rag/search-multi", tags=["RAG"])
    def rag_search_multi(req: RagMultiSearchRequest):
        r = svc.rag_preview_multi(req.scopes, req.query, k=req.k)
        if not r.get("ok"):
            _error(r.get("error") or "Multi-scope search failed.")
        return r

    @router.post("/api/ai/rag/eval", tags=["RAG"])
    def rag_eval(req: RagEvalRequest):
        r = svc.rag_eval(req.connection, gold=req.gold, k=req.k,
                         per_case=req.per_case)
        if not r.get("ok"):
            _error(r.get("error") or "Eval failed.")
        return r

    @router.get("/api/ai/rag/drift", tags=["RAG"])
    def rag_drift(connection: str):
        r = svc.rag_drift(connection)
        if not r.get("ok"):
            _error(r.get("error") or "Drift check failed.")
        return r

    @router.post("/api/ai/rag/reindex-stale", tags=["RAG"])
    def rag_reindex_stale(req: RagReindexStaleRequest):
        r = svc.rag_reindex_stale(req.connections or None, force=req.force)
        if not r.get("ok"):
            _error(r.get("error") or "Reindex failed.")
        return r

    @router.get("/api/ai/rag/reindex/schedule", tags=["RAG"])
    def rag_reindex_schedule_status():
        return svc.rag_reindex_schedule_status()

    @router.post("/api/ai/rag/reindex/schedule/start", tags=["RAG"])
    def rag_reindex_schedule_start():
        return svc.rag_reindex_schedule_start()

    @router.post("/api/ai/rag/reindex/schedule/stop", tags=["RAG"])
    def rag_reindex_schedule_stop():
        return svc.rag_reindex_schedule_stop()

    @router.post("/api/ai/rag/add-codebase", tags=["RAG"])
    def rag_add_codebase(req: RagCodebaseRequest):
        mf = req.max_files if req.max_files > 0 else None
        r = svc.rag_add_codebase(
            req.folder, req.scope,
            standalone=req.standalone, replace=req.replace, max_files=mf,
        )
        if not r.get("ok"):
            _error(r.get("error") or "Codebase indexing failed.")
        return r

    @router.delete("/api/ai/rag", tags=["RAG"])
    def rag_clear(connection: str):
        r = svc.rag_clear(connection)
        if not r.get("ok"):
            _error(r.get("error") or "Clear failed.")
        return r

    # -- LLM (local trainable NL->SQL model) -------------------------------

    @router.get("/api/ai/llm/engines", tags=["LLM"])
    def llm_engines():
        return svc.llm_engines()

    @router.post("/api/ai/llm/train", tags=["LLM"])
    def llm_train(req: LlmTrainRequest):
        r = svc.llm_train(
            name=req.name,
            engine=req.engine or None,
            include_sample=req.include_sample,
            dataset_path=req.dataset_path or None,
            rag_connection=req.rag_connection or "",
        )
        if not r.get("ok"):
            _error(r.get("error") or "Training failed.")
        return r

    @router.get("/api/ai/llm/status", tags=["LLM"])
    def llm_status(name: str = "default"):
        r = svc.llm_status(name)
        if not r.get("ok"):
            _error(r.get("error") or "Status failed.")
        return r

    @router.get("/api/ai/llm/models", tags=["LLM"])
    def llm_models():
        return svc.llm_list()

    @router.get("/api/ai/llm/model-dataset", tags=["LLM"])
    def llm_model_dataset(name: str = "default", query: str = "", limit: int = 0):
        """Inspect the exact NL->SQL pairs a trained model was built on.

        Use ``query`` to verify a specific question/SQL is "in" the model.
        """
        r = svc.llm_model_dataset(name=name, query=query or "", limit=limit or 0)
        if not r.get("ok"):
            _error(r.get("error") or "Dataset lookup failed.")
        return r

    @router.get("/api/ai/llm/versions", tags=["LLM"])
    def llm_versions(name: str = "default"):
        """List saved snapshots (versions) of a trained model, newest first."""
        r = svc.llm_model_versions(name=name)
        if not r.get("ok"):
            _error(r.get("error") or "Could not list versions.")
        return r

    @router.post("/api/ai/llm/restore", tags=["LLM"])
    def llm_restore(req: LlmRestoreRequest):
        """Roll a model back to a saved snapshot/version."""
        r = svc.llm_model_restore(name=req.name, version=req.version)
        if not r.get("ok"):
            _error(r.get("error") or "Restore failed.")
        return r

    @router.post("/api/ai/llm/generate", tags=["LLM"])
    def llm_generate(req: LlmGenerateRequest):
        r = svc.llm_generate(
            req.question, name=req.name, engine=req.engine or None,
            max_new=req.max_new, temperature=req.temperature,
            connection=req.connection or "",
            alternatives=bool(req.alternatives),
        )
        if not r.get("ok"):
            _error(r.get("error") or "Generation failed.")
        return r

    @router.post("/api/ai/llm/eval", tags=["LLM"])
    def llm_eval(req: LlmEvalRequest):
        r = svc.llm_eval(
            name=req.name,
            connection=req.connection or "",
            depth=req.depth or None,
            include_sample=req.include_sample,
            rag_connection=req.rag_connection or "",
            dataset_path=req.dataset_path or None,
        )
        if not r.get("ok"):
            _error(r.get("error") or "Evaluation failed.")
        return r

    @router.post("/api/ai/llm/export", tags=["LLM"])
    def llm_export(req: LlmExportRequest):
        r = svc.llm_export(_resolve_export_path(req.path), include_sample=req.include_sample,
                           rag_connection=req.rag_connection or "")
        if not r.get("ok"):
            _error(r.get("error") or "Export failed.")
        return r

    @router.post("/api/ai/llm/dataset", tags=["LLM"])
    def llm_dataset(req: LlmDatasetRequest):
        """Return the NL->SQL dataset as JSONL text for a browser download.

        Remote-access-safe: nothing is written to the server filesystem; the
        client saves the returned ``content`` locally.
        """
        r = svc.llm_dataset(include_sample=req.include_sample,
                            rag_connection=req.rag_connection or "")
        if not r.get("ok"):
            _error(r.get("error") or "Dataset export failed.")
        return r

    @router.post("/api/ai/llm/train-llm", tags=["LLM"])
    def llm_train_rich(body: LlmTrainRichRequest):
        r = svc.llm_train_rich(_model_dump(body))
        if not r.get("ok"):
            _error(r.get("error") or r.get("reason") or "Training failed.")
        return r

    @router.post("/api/ai/llm/train-multi", tags=["LLM"])
    def llm_train_multi(body: LlmTrainMultiRequest):
        """Train one model from several connections in parallel (shard + merge)."""
        r = svc.llm_train_multi(_model_dump(body))
        if not r.get("ok"):
            _error(r.get("error") or r.get("reason") or "Multi-connection training failed.")
        return r

    @router.post("/api/ai/llm/mine-training-pairs", tags=["LLM"])
    def llm_mine_training_pairs(body: LlmMineTrainingPairsRequest):
        r = svc.llm_mine_pairs(_model_dump(body))
        if not r.get("ok"):
            _error(r.get("error") or "Mining failed.")
        return r

    @router.get("/api/ai/llm/rag-status", tags=["LLM"])
    def llm_rag_status(connection: str = ""):
        r = svc.llm_rag_status(connection)
        if not r.get("ok", False):  # fail closed: missing "ok" => treat as error
            _error(r.get("error") or "RAG status failed.")
        return r

    @router.post("/api/ai/llm/index-rag", tags=["LLM"])
    def llm_index_rag(body: dict):
        r = svc.llm_index_rag(
            str(body.get("connection") or ""),
            rebuild=bool(body.get("rebuild", False)),
        )
        if not r.get("ok"):
            _error(r.get("error") or "RAG indexing failed.")
        return r

    @router.post("/api/ai/llm/train-pairs", tags=["LLM"])
    def llm_train_pairs(body: dict):
        r = svc.llm_train_pairs(body)
        if not r.get("ok"):
            _error(r.get("error") or r.get("reason") or "Training failed.")
        return r

    @router.post("/api/ai/llm/harvest", tags=["LLM"])
    def llm_harvest(req: LlmHarvestRequest):
        r = svc.llm_harvest(req.model_dump(exclude_unset=True))
        if not r.get("ok"):
            _error(r.get("error") or "Harvest failed.")
        return r

    @router.post("/api/ai/llm/harvest/stop", tags=["LLM"])
    def llm_harvest_stop(body: dict):
        # Graceful stop: pass the same ``harvest_id`` used to start the harvest.
        harvest_id = _validate_id(
            str((body or {}).get("harvest_id") or ""), what="harvest id")
        r = svc.llm_harvest_stop(harvest_id)
        if not r.get("ok"):
            _error(r.get("error") or "No running harvest to stop.")
        return r

    @router.post("/api/ai/llm/enrich-templates", tags=["LLM"])
    def llm_enrich_templates(req: LlmEnrichTemplatesRequest):
        r = svc.llm_enrich_templates(req.model_dump())
        if not r.get("ok"):
            _error(r.get("error") or "Template enrichment produced no templates.")
        return r

    @router.get("/api/ai/llm/templates", tags=["LLM"])
    def llm_template_store_summary():
        return svc.llm_template_store_summary()

    @router.delete("/api/ai/llm/templates", tags=["LLM"])
    def llm_template_store_clear():
        return svc.llm_template_store_clear()

    @router.get("/api/ai/llm/harvest/schedule", tags=["LLM"])
    def llm_harvest_schedule_status():
        return svc.llm_harvest_schedule_status()

    @router.post("/api/ai/llm/harvest/schedule/start", tags=["LLM"])
    def llm_harvest_schedule_start():
        return svc.llm_harvest_schedule_start()

    @router.post("/api/ai/llm/harvest/schedule/stop", tags=["LLM"])
    def llm_harvest_schedule_stop():
        return svc.llm_harvest_schedule_stop()

    # -- LLM background jobs (live SSE progress) -----------------------------
    try:
        from ai_assistant.llm.jobs import get_llm_job_manager
        from fastapi.responses import StreamingResponse
    except ImportError:
        get_llm_job_manager = None
        StreamingResponse = None

    llm_jobs = get_llm_job_manager(svc) if get_llm_job_manager else None

    @router.post("/api/ai/llm/jobs", tags=["LLM"])
    def llm_job_start(body: dict):
        """Start a background train or harvest job with SSE event stream."""
        if llm_jobs is None:
            _error("LLM jobs are not available in this build.", status=501)
        r = llm_jobs.start(body or {})
        if not r.get("ok"):
            _error(r.get("error") or "Failed to start LLM job.")
        return r

    @router.get("/api/ai/llm/jobs/{job_id}", tags=["LLM"])
    def llm_job_status(job_id: str):
        if llm_jobs is None:
            _error("LLM jobs are not available in this build.", status=501)
        r = llm_jobs.status(job_id)
        if not r.get("ok"):
            _error(r.get("error") or "Job not found.", status=404)
        return r

    @router.get("/api/ai/llm/jobs/{job_id}/events", tags=["LLM"])
    def llm_job_events(job_id: str, cursor: int = 0):
        """SSE stream of training/harvest progress events."""
        if llm_jobs is None or StreamingResponse is None:
            _error("LLM jobs are not available in this build.", status=501)

        def _gen():
            yield from llm_jobs.iter_events_sse(job_id, cursor)

        return StreamingResponse(_gen(), media_type="text/event-stream")

    @router.get("/api/ai/llm/jobs/{job_id}/events/poll", tags=["LLM"])
    def llm_job_events_poll(job_id: str, cursor: int = 0):
        """Polling fallback: return events since *cursor*."""
        if llm_jobs is None:
            _error("LLM jobs are not available in this build.", status=501)
        st = llm_jobs.status(job_id)
        if not st.get("ok"):
            _error(st.get("error") or "Job not found.", status=404)
        return {
            "events": llm_jobs.events(job_id, cursor),
            **st,
        }

    @router.post("/api/ai/llm/jobs/{job_id}/stop", tags=["LLM"])
    def llm_job_stop(job_id: str):
        if llm_jobs is None:
            _error("LLM jobs are not available in this build.", status=501)
        r = llm_jobs.stop(job_id)
        if not r.get("ok"):
            _error(r.get("error") or "Job not found.", status=404)
        return r

    return router
