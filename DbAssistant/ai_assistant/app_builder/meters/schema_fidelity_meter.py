"""schema_fidelity_meter — does the app reflect real tables/relations?"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, coverage


class SchemaFidelityMeter(Meter):
    name = "schema_fidelity_meter"
    default_threshold = 0.55

    def measure(
        self,
        files: Mapping[str, str],
        *,
        schema: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        tables = set((schema or {}).keys())
        if not tables:
            return Measurement(
                meter=self.name, score=1.0,
                components={"table_recall": 1.0},
                weights={"table_recall": 1.0},
                evidence={"applicable": False},
                issues=[],
                threshold=threshold if threshold is not None else self.default_threshold,
            )
        blob = "\n".join(files.values()).lower() + " " + " ".join(files).lower()
        present = {t for t in tables if t.lower() in blob}
        score = coverage(tables, present)
        missing = sorted(tables - present)
        return Measurement(
            meter=self.name,
            score=score,
            components={"table_recall": score},
            weights={"table_recall": 1.0},
            evidence={"expected_tables": sorted(tables), "present": sorted(present)},
            issues=[f"table not reflected in app: {m}" for m in missing[:8]],
            threshold=threshold if threshold is not None else self.default_threshold,
        )
