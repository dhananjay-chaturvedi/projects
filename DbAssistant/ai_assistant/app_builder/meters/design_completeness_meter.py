"""design_completeness_meter — how much of the agreed design is built yet?

Session B evaluates build progress by comparing the *agreed* design (the total
set of entities/components/features the sessions settled on during the
understanding phase) against what the produced files actually implement. The
result is a normalized completeness score plus the explicit list of what is
done and what is still pending — exactly the "how much design is complete and
how much is pending" signal the product spec asks B to compute.

Fully deterministic: token/set math over the produced files vs the design plan.
"""

from __future__ import annotations

from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score
from ai_assistant.meters.requirement_fidelity_meter import _variants

from ai_assistant.app_builder.meters.design_plan import DesignPlan


def _haystack(files: Mapping[str, str]) -> str:
    parts: list[str] = []
    for path, content in files.items():
        parts.append(path.lower())
        if path.lower().endswith(
                (".py", ".html", ".md", ".txt", ".sql", ".css", ".js",
                 ".yaml", ".yml", ".json")):
            parts.append((content or "").lower())
    return "\n".join(parts)


def _present(token: str, haystack: str) -> bool:
    return any(v in haystack for v in _variants(token))


class DesignCompletenessMeter(Meter):
    """Score implemented-vs-total design components from produced files."""

    name = "design_completeness_meter"
    default_threshold = 0.8

    def measure(
        self,
        design: DesignPlan,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        haystack = _haystack(files)

        dims = {
            "entities": sorted(design.entities),
            "components": sorted(design.components),
            "features": sorted(design.features),
        }
        weights = {"entities": 3.0, "components": 2.0, "features": 2.0}

        components: dict[str, float] = {}
        done: dict[str, list[str]] = {}
        pending: dict[str, list[str]] = {}
        for dim, items in dims.items():
            if not items:
                components[dim] = 1.0
                continue
            hit = [t for t in items if _present(t, haystack)]
            miss = [t for t in items if t not in hit]
            components[dim] = len(hit) / len(items)
            done[dim] = hit
            pending[dim] = miss

        score = weighted_score(components, weights)

        total_items = sum(len(v) for v in dims.values())
        done_items = sum(len(v) for v in done.values())
        issues: list[str] = []
        flat_pending = [t for v in pending.values() for t in v]
        if flat_pending:
            issues.append("design not fully implemented; pending: "
                          + ", ".join(flat_pending[:12]))

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={
                "implemented": done,
                "pending": pending,
                "implemented_count": done_items,
                "total_count": total_items,
                "percent_complete": round(
                    (done_items / total_items) if total_items else 1.0, 4),
            },
            issues=issues, threshold=thr,
        )
