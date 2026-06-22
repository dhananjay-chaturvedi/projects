"""Capture pipeline — score with meters, persist if quality gate passes."""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger(__name__)

from ai_assistant.capture.identity import capture_scope
from ai_assistant.capture.record import CaptureRecord
from ai_assistant.capture.store import IsolatedCaptureStore
from ai_assistant.meters import MeterSuite
from common import paths as app_paths


def _enabled() -> bool:
    return os.environ.get("DBTOOL_CAPTURE", "1").strip().lower() not in (
        "0", "false", "no", "off"
    )


def _schema_from_context(context: dict | None) -> dict[str, list[str]]:
    if not context:
        return {}
    schema = context.get("schema") or {}
    table_schemas = schema.get("table_schemas") or {}
    if isinstance(table_schemas, dict) and table_schemas:
        out: dict[str, list[str]] = {}
        for table, info in table_schemas.items():
            cols: list[str] = []
            if isinstance(info, dict):
                for c in info.get("columns") or info.get("fields") or []:
                    if isinstance(c, dict):
                        cols.append(str(c.get("name") or c.get("column") or ""))
                    else:
                        cols.append(str(c))
            elif isinstance(info, list):
                cols = [str(c) for c in info]
            out[str(table).split(".")[-1]] = [c for c in cols if c]
        return out
    tables = schema.get("tables") or []
    return {str(t).split(".")[-1]: [] for t in tables}


def _excerpt(text: str, limit: int = 2000) -> str:
    t = (text or "").strip()
    return t if len(t) <= limit else t[: limit - 1] + "…"


@dataclass(frozen=True)
class CaptureTurn:
    question: str
    prompt: str
    raw_response: str
    parsed: dict[str, Any]
    context: dict | None
    connection_name: str
    db_manager: Any
    backend: str = ""
    session_id: str | None = None
    is_followup: bool = False
    previous_sql: str | None = None
    project_id: str | None = None
    purpose: str = "llm_training"
    execution: dict | None = None


class CapturePipeline:
    """Record grounded turns and export training sets."""

    def __init__(self, store: IsolatedCaptureStore | None = None) -> None:
        self.store = store or IsolatedCaptureStore(app_paths.ai_capture_dir())
        self.meters = MeterSuite()

    def record_turn(self, turn: CaptureTurn) -> CaptureRecord | None:
        pid, conn_slug, db_slug = capture_scope(
            project_id=turn.project_id,
            connection_name=turn.connection_name,
            db_manager=turn.db_manager,
        )
        schema = _schema_from_context(turn.context)
        sql = turn.parsed.get("summary_sql") or turn.parsed.get("sql") or ""
        verdict = self.meters.output.evaluate(
            turn.question,
            sql or turn.raw_response,
            schema=schema or None,
            execution=turn.execution,
            previous_sql=turn.previous_sql,
            is_followup=turn.is_followup,
        )
        is_clarification = bool(turn.parsed.get("is_clarification"))
        is_refusal = not sql and not is_clarification and not turn.parsed.get("error")
        rec = CaptureRecord(
            project_id=pid,
            connection_name=conn_slug,
            database=db_slug,
            backend=turn.backend,
            session_id=turn.session_id,
            is_followup=turn.is_followup,
            question=turn.question,
            db_context_excerpt=_excerpt(turn.prompt),
            schema_snapshot=schema,
            prompt_hash=hashlib.sha256(turn.prompt.encode()).hexdigest()[:16],
            raw_response=turn.raw_response,
            sql=sql or None,
            explanation=turn.parsed.get("explanation"),
            error=turn.parsed.get("error"),
            quality_score=verdict.score,
            quality_accepted=verdict.accepted,
            quality=verdict.as_dict(),
            purpose=turn.purpose,
            is_refusal=is_refusal,
            is_clarification=is_clarification,
        )
        # Always append for audit; export filters on quality_accepted.
        self.store.append(rec)
        return rec


_pipeline: CapturePipeline | None = None


def get_pipeline() -> CapturePipeline:
    global _pipeline
    if _pipeline is None:
        _pipeline = CapturePipeline()
    return _pipeline


def maybe_capture_turn(**kwargs: Any) -> CaptureRecord | None:
    """Best-effort capture; never raises (AI Query must keep working)."""
    if not _enabled():
        return None
    try:
        return get_pipeline().record_turn(CaptureTurn(**kwargs))
    except Exception:
        _log.debug("Capture pipeline error (suppressed)", exc_info=True)
        return None
