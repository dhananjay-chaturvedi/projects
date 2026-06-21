"""metadata_completeness_meter — did we capture enough DB metadata kinds?"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, weighted_score


class MetadataCompletenessMeter(Meter):
    name = "metadata_completeness_meter"
    default_threshold = 0.6

    _KINDS = ("tables", "views", "indexes", "constraints", "triggers", "sequences")

    def measure(self, profile: Mapping[str, Any]) -> Measurement:
        kinds = dict(profile.get("metadata_kinds") or {})
        if not kinds and profile.get("tables"):
            kinds = {"tables": True}
        # Tables are mandatory. Other object classes are opportunistic: many
        # small schemas legitimately have no views/triggers/sequences, so their
        # absence is evidence but not a hard failure.
        components = {"tables": 1.0 if kinds.get("tables") else 0.0}
        weights = {"tables": 3.0}
        optional_seen = sum(1 for k in self._KINDS[1:] if kinds.get(k))
        components["optional_catalogs"] = 1.0 if optional_seen else 0.5
        weights["optional_catalogs"] = 1.0
        issues = []
        if not kinds.get("tables"):
            issues.append("missing metadata kind: tables")
        return Measurement(
            meter=self.name,
            score=weighted_score(components, weights),
            components=components,
            weights=weights,
            evidence={"kinds": kinds, "table_count": len(profile.get("tables") or [])},
            issues=issues,
            threshold=self.default_threshold,
        )
