"""Data comparison option objects for schema converter surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Event
from typing import Any, Mapping


@dataclass(frozen=True)
class DataCompareOptions:
    """Options for comparing data between a source and target table."""

    target_table: str | None = None
    mode: str = "sample"
    sample_size: int | None = None
    stop_event: Event | None = None
    batch_size: int | None = None

    @classmethod
    def from_source(
        cls, source: "DataCompareOptions | Mapping[str, Any] | None" = None
    ) -> "DataCompareOptions":
        """Coerce an existing options object, a legacy kwargs mapping, or ``None``."""
        if isinstance(source, cls):
            return source
        src: Mapping[str, Any] = source or {}
        return cls(
            target_table=src.get("target_table"),
            mode=src.get("mode") or "sample",
            sample_size=src.get("sample_size"),
            stop_event=src.get("stop_event"),
            batch_size=src.get("batch_size"),
        )
