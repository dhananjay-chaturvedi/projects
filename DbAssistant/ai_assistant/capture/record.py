"""Capture record schema — one grounded AI turn."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class CaptureRecord:
    """A single request→context→response tuple suitable for training/export."""

    record_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    captured_at: str = field(default_factory=_utc_now)
    project_id: str = ""
    connection_name: str = ""
    database: str = ""
    backend: str = ""
    session_id: str | None = None
    is_followup: bool = False
    # Inputs
    question: str = ""
    db_context_excerpt: str = ""
    schema_snapshot: dict[str, list[str]] = field(default_factory=dict)
    prompt_hash: str = ""
    # Outputs
    raw_response: str = ""
    sql: str | None = None
    explanation: str | None = None
    error: str | None = None
    # Deterministic quality (from meters)
    quality_score: float = 0.0
    quality_accepted: bool = False
    quality: dict[str, Any] = field(default_factory=dict)
    # Provenance
    source: str = "ai_query"
    purpose: str = "llm_training"  # llm_training | audit | app_builder
    # Turn classification — kept out of training unless intentional
    is_refusal: bool = False       # AI said it cannot answer (no SQL produced)
    is_clarification: bool = False  # AI asked for clarification instead of SQL
    # Meter versioning — lets stale records be re-scored after logic changes
    meter_version: str = "1"

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json_line(cls, line: str) -> CaptureRecord:
        data = json.loads(line)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
