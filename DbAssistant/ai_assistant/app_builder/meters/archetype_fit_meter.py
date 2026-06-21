"""archetype_fit_meter — does the built app expose predicted archetype surfaces?"""

from __future__ import annotations

from collections.abc import Mapping

from ai_assistant.app_builder.archetypes import expected_surfaces
from ai_assistant.meters.base import Meter, Measurement, coverage


class ArchetypeFitMeter(Meter):
    name = "archetype_fit_meter"
    default_threshold = 0.65

    def measure(
        self,
        files: Mapping[str, str],
        *,
        archetype: str = "",
        threshold: float | None = None,
    ) -> Measurement:
        surfaces = set(expected_surfaces(archetype or "generic_crud"))
        blob = "\n".join(files.values()).lower() + " " + " ".join(files).lower()
        present = {s for s in surfaces if s.lower() in blob}
        score = coverage(surfaces, present)
        missing = sorted(surfaces - present)
        return Measurement(
            meter=self.name,
            score=score,
            components={"surface_coverage": score},
            weights={"surface_coverage": 1.0},
            evidence={
                "archetype": archetype,
                "expected": sorted(surfaces),
                "present": sorted(present),
                "missing": missing,
            },
            issues=[f"missing surface: {m}" for m in missing[:8]],
            threshold=threshold if threshold is not None else self.default_threshold,
        )
