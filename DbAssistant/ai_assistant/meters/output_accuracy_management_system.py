"""output_accuracy_management_system — compose response meters + gate + history.

Runs accuracy_meter, error_meter and understanding_meter over an AI response,
combines them into a single output-quality score, decides accept/reject against
configurable gates, and keeps a rolling history so callers (the LLM dataset
builder, the AI Query UI) can trend quality over time and only feed
high-quality samples into training.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ai_assistant.meters.accuracy_meter import AccuracyMeter
from ai_assistant.meters.base import Measurement, clamp01, weighted_score
from ai_assistant.meters.error_meter import ErrorMeter
from ai_assistant.meters.understanding_meter import UnderstandingMeter


@dataclass
class OutputVerdict:
    score: float
    accepted: bool
    measurements: dict[str, Measurement]
    issues: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "accepted": self.accepted,
            "issues": list(self.issues),
            "measurements": {k: m.as_dict() for k, m in self.measurements.items()},
        }


class OutputAccuracyManagementSystem:
    name = "output_accuracy_management_system"

    # Component weights for the blended output score.
    WEIGHTS = {"accuracy_meter": 3.0, "error_meter": 2.0, "understanding_meter": 2.0}
    # Acceptance gate.
    accept_threshold = 0.75

    def __init__(self) -> None:
        self._accuracy = AccuracyMeter()
        self._error = ErrorMeter()
        self._understanding = UnderstandingMeter()
        self.history: list[OutputVerdict] = []

    def evaluate(
        self,
        question: str,
        sql: str,
        *,
        schema: dict[str, list[str]] | None = None,
        execution: dict[str, Any] | None = None,
        previous_sql: str | None = None,
        is_followup: bool = False,
    ) -> OutputVerdict:
        acc = self._accuracy.measure(question, sql, schema=schema, execution=execution)
        err = self._error.measure(sql, schema=schema, execution=execution)
        und = self._understanding.measure(
            question, sql, previous_sql=previous_sql, is_followup=is_followup
        )
        measurements = {m.meter: m for m in (acc, err, und)}
        blended = weighted_score(
            {k: m.score for k, m in measurements.items()}, self.WEIGHTS
        )
        # A hard error (parse/execution) caps the output regardless of the blend.
        hard_fail = (not acc.passed and acc.components.get("sql_parses", 1.0) == 0.0) or (
            execution is not None and not (execution.get("ok") and not execution.get("error"))
        )
        score = clamp01(min(blended, 0.5) if hard_fail else blended)
        issues = [i for m in measurements.values() for i in m.issues]
        verdict = OutputVerdict(score, score >= self.accept_threshold, measurements, issues)
        self.history.append(verdict)
        return verdict

    def stats(self) -> dict[str, Any]:
        if not self.history:
            return {"count": 0, "mean_score": 0.0, "accept_rate": 0.0}
        n = len(self.history)
        return {
            "count": n,
            "mean_score": round(sum(v.score for v in self.history) / n, 4),
            "accept_rate": round(sum(1 for v in self.history if v.accepted) / n, 4),
        }
