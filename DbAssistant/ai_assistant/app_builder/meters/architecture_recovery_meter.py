"""architecture_recovery_meter — does the build recover codebase architecture?"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, coverage


class ArchitectureRecoveryMeter(Meter):
    name = "architecture_recovery_meter"
    default_threshold = 0.5

    def measure(
        self,
        files: Mapping[str, str],
        *,
        profile: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        prof = profile or {}
        expected: set[str] = set()
        for r in prof.get("routes") or []:
            expected.add(str(r).strip("/").split("/")[0] or r)
        for s in prof.get("services") or []:
            expected.add(str(s).split(":")[-1].lower())
        if not expected:
            return Measurement(
                meter=self.name, score=1.0,
                components={"recovery": 1.0},
                weights={"recovery": 1.0},
                evidence={"applicable": False},
                issues=[],
                threshold=threshold if threshold is not None else self.default_threshold,
            )
        blob = "\n".join(files.values()).lower() + " " + " ".join(files).lower()
        present = {e for e in expected if e.lower() in blob}
        score = coverage(expected, present)
        return Measurement(
            meter=self.name,
            score=score,
            components={"recovery": score},
            weights={"recovery": 1.0},
            evidence={"expected": sorted(expected), "present": sorted(present)},
            issues=[f"architecture element missing: {m}" for m in sorted(expected - present)[:8]],
            threshold=threshold if threshold is not None else self.default_threshold,
        )
