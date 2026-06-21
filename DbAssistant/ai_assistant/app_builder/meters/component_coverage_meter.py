"""component_coverage_meter — predicted app covers recovered components."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, coverage


class ComponentCoverageMeter(Meter):
    name = "component_coverage_meter"
    default_threshold = 0.45

    def measure(
        self,
        files: Mapping[str, str],
        *,
        components: list[str] | None = None,
        profile: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        expected: set[str] = set(components or [])
        prof = profile or {}
        if prof.get("entrypoints"):
            expected.add("entrypoints")
        if prof.get("routes"):
            expected.add("api_routes")
        if prof.get("services"):
            expected.add("services")
        if prof.get("db_tables"):
            expected.add("data_models")
        if not expected:
            return Measurement(
                meter=self.name, score=1.0,
                components={"coverage": 1.0},
                weights={"coverage": 1.0},
                evidence={"applicable": False},
                issues=[],
                threshold=threshold if threshold is not None else self.default_threshold,
            )
        blob = "\n".join(files.values()).lower() + " " + " ".join(files).lower()
        present = {c for c in expected if c.lower().replace("_", " ") in blob or c in blob}
        score = coverage(expected, present)
        return Measurement(
            meter=self.name,
            score=score,
            components={"coverage": score},
            weights={"coverage": 1.0},
            evidence={"expected": sorted(expected), "present": sorted(present)},
            issues=[f"component not covered: {m}" for m in sorted(expected - present)[:8]],
            threshold=threshold if threshold is not None else self.default_threshold,
        )
