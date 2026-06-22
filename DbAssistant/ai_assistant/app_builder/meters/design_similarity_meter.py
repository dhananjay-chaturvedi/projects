"""design_similarity_meter — do the A/B/C sessions agree on the design?

Before a single file is built, each session produces a plan (see
:mod:`ai_assistant.app_builder.meters.design_plan`). This meter measures the
*syntactic* design similarity across those plans: for each design dimension
(entities, components, features, endpoints) it computes the mean pairwise
Jaccard overlap, then weights the dimensions into one normalized score.

The App Builder Assistant uses this as the "session understanding" gate: only
when similarity is high (default ≥ 0.8) do the sessions share a common mental
model and the build is allowed to start. Otherwise the disagreement is
surfaced (which dimension diverges, and the symmetric differences) so the
sessions can reconcile.

Fully deterministic: set maths over the parsed plans, never a model call.
"""

from __future__ import annotations

from collections.abc import Sequence
from itertools import combinations

from ai_assistant.meters.base import Meter, Measurement, jaccard, weighted_score

from ai_assistant.app_builder.meters.design_plan import DesignPlan

_DIMENSIONS = ("entities", "components", "features", "endpoints")
_WEIGHTS = {
    "entities": 3.0,
    "components": 2.5,
    "features": 2.5,
    "endpoints": 1.0,
}


class DesignSimilarityMeter(Meter):
    """Score how closely a set of session design plans agree."""

    name = "design_similarity_meter"
    default_threshold = 0.8

    def measure(
        self,
        plans: Sequence[DesignPlan],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        plans = [p for p in plans if p is not None]
        thr = self.default_threshold if threshold is None else threshold

        if len(plans) < 2:
            # Cannot disagree with itself — trivially aligned but flagged.
            return Measurement(
                meter=self.name, score=1.0,
                components={d: 1.0 for d in _DIMENSIONS}, weights=dict(_WEIGHTS),
                evidence={"plans": len(plans), "note": "fewer than 2 plans"},
                issues=[], threshold=thr,
            )

        sigs = [p.signature_sets() for p in plans]
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        divergence: dict[str, list[str]] = {}
        skipped: list[str] = []
        for dim in _DIMENSIONS:
            # Only score a dimension when at least two plans actually describe
            # it — otherwise an asymmetry (e.g. only the builder lists HTTP
            # paths) would unfairly drag the agreement score down.
            contributing = sum(1 for s in sigs if s[dim])
            if contributing < 2:
                skipped.append(dim)
                continue
            pair_scores: list[float] = []
            disagreements: set[str] = set()
            for a, b in combinations(sigs, 2):
                sa, sb = a[dim], b[dim]
                pair_scores.append(jaccard(sa, sb))
                disagreements |= (sa ^ sb)
            components[dim] = (sum(pair_scores) / len(pair_scores)
                               if pair_scores else 1.0)
            weights[dim] = _WEIGHTS[dim]
            if disagreements:
                divergence[dim] = sorted(disagreements)[:12]

        if not components:
            # Nothing comparable across plans — treat as undecided/low.
            return Measurement(
                meter=self.name, score=0.0,
                components={d: 0.0 for d in _DIMENSIONS}, weights=dict(_WEIGHTS),
                evidence={"note": "no comparable design dimensions",
                          "plans": [p.as_dict() for p in plans]},
                issues=["sessions produced no comparable design detail"],
                threshold=thr,
            )

        score = weighted_score(components, weights)

        issues: list[str] = []
        if score < thr:
            worst = min(components, key=lambda d: components[d])
            issues.append(
                f"sessions disagree on the design (similarity {score:.0%} < "
                f"{thr:.0%}); weakest dimension: {worst}")
            for dim, diff in divergence.items():
                if components[dim] < thr:
                    issues.append(f"{dim} mismatch: " + ", ".join(diff))

        return Measurement(
            meter=self.name, score=score, components=components,
            weights=weights,
            evidence={
                "plans": [p.as_dict() for p in plans],
                "divergence": divergence,
                "skipped_dimensions": skipped,
                "per_dimension": {k: round(v, 4) for k, v in components.items()},
            },
            issues=issues, threshold=thr,
        )
