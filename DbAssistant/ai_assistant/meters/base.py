"""Foundational types for the code/math-based measurement subsystem.

Meters are *deterministic*: they score AI requests, responses and generated
artifacts using parsing, set math and rule checks — never by asking a model.
Every meter returns a :class:`Measurement` with a normalized ``[0, 1]`` score,
the weighted components that produced it, machine-readable evidence, a list of
issues and a pass/fail gate. Management systems compose several meters and add
gating + history.

Design goals (per product spec): rely on logic/maths to map requests,
responses, follow-ups, accuracy, integrity, optimisation, robustness,
reliability and correctness — prompts are only ever used elsewhere to elicit
data points, not to compute these scores.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def clamp01(value: float) -> float:
    """Clamp a number to the ``[0.0, 1.0]`` range."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return 0.0
    if v != v:  # NaN
        return 0.0
    return 0.0 if v < 0.0 else 1.0 if v > 1.0 else v


def weighted_score(components: dict[str, float], weights: dict[str, float]) -> float:
    """Weighted average of component scores, ignoring zero-weight components.

    Only components that are present in *both* dicts contribute, so a meter can
    omit a component (e.g. no schema available) and have it cleanly excluded
    from the denominator instead of dragging the score to zero.
    """
    total_w = sum(max(0.0, weights.get(k, 0.0)) for k in components)
    if total_w <= 0.0:
        return 0.0
    acc = sum(clamp01(v) * max(0.0, weights.get(k, 0.0)) for k, v in components.items())
    return clamp01(acc / total_w)


def jaccard(a: set, b: set) -> float:
    """Jaccard similarity of two sets (1.0 when both empty)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def coverage(required: set, present: set) -> float:
    """Fraction of *required* items that are present (1.0 when nothing required)."""
    if not required:
        return 1.0
    return len(required & present) / len(required)


def prf(expected: set, produced: set) -> tuple[float, float, float]:
    """Return (precision, recall, f1) for a produced vs expected set."""
    if not expected and not produced:
        return 1.0, 1.0, 1.0
    tp = len(expected & produced)
    precision = tp / len(produced) if produced else 0.0
    recall = tp / len(expected) if expected else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return precision, recall, f1


def diminishing(count: int, *, half_life: float = 2.0) -> float:
    """Map a non-negative count to ``(0, 1]`` where 0 -> 1.0 and more -> 0.

    Uses ``half_life / (half_life + count)`` so the first few occurrences hurt
    the most. Handy for turning "number of errors/violations" into a score.
    """
    c = max(0, int(count))
    hl = max(1e-9, float(half_life))
    return hl / (hl + c)


@dataclass
class Measurement:
    """A single deterministic measurement."""

    meter: str
    score: float
    components: dict[str, float] = field(default_factory=dict)
    weights: dict[str, float] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)
    threshold: float = 0.7

    def __post_init__(self) -> None:
        self.score = clamp01(self.score)

    @property
    def passed(self) -> bool:
        return self.score >= self.threshold

    @property
    def grade(self) -> str:
        s = self.score
        if s >= 0.9:
            return "A"
        if s >= 0.8:
            return "B"
        if s >= 0.7:
            return "C"
        if s >= 0.5:
            return "D"
        return "F"

    def as_dict(self) -> dict[str, Any]:
        return {
            "meter": self.meter,
            "score": round(self.score, 4),
            "grade": self.grade,
            "passed": self.passed,
            "threshold": self.threshold,
            "components": {k: round(clamp01(v), 4) for k, v in self.components.items()},
            "weights": dict(self.weights),
            "issues": list(self.issues),
            "evidence": self.evidence,
        }


class Meter:
    """Base class for all meters."""

    name: str = "meter"
    default_threshold: float = 0.7

    def measure(self, *args: Any, **kwargs: Any) -> Measurement:  # pragma: no cover
        raise NotImplementedError

    def _result(
        self,
        components: dict[str, float],
        weights: dict[str, float],
        *,
        evidence: dict[str, Any] | None = None,
        issues: list[str] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        return Measurement(
            meter=self.name,
            score=weighted_score(components, weights),
            components=components,
            weights={k: weights.get(k, 0.0) for k in components},
            evidence=evidence or {},
            issues=issues or [],
            threshold=self.default_threshold if threshold is None else threshold,
        )
