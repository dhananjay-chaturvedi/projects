"""input_reliability_management_system — gate AI runs on input quality.

Before a request is sent to an agent (or used for training), this validates the
inputs deterministically: a reachable connection, a loaded schema, a sane
question, PII masking applied when required, and sufficient context. It returns
a reliability score and a hard ``allow`` gate so low-reliability inputs can be
blocked rather than producing junk that later poisons accuracy/training.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, clamp01


@dataclass
class InputSignal:
    connection_ok: bool = False
    schema_loaded: bool = False
    schema_table_count: int = 0
    question: str = ""
    pii_required: bool = False
    pii_masked: bool = False
    context_completeness: float = 0.0  # 0..1, fraction of expected context present
    min_question_len: int = 3
    max_question_len: int = 2000


class InputReliabilityManagementSystem(Meter):
    name = "input_reliability_management_system"
    default_threshold = 0.7

    def measure(self, signal: InputSignal | dict[str, Any]) -> Measurement:
        if isinstance(signal, dict):
            signal = InputSignal(**signal)

        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        issues: list[str] = []

        components["connection"] = 1.0 if signal.connection_ok else 0.0
        weights["connection"] = 3.0
        if not signal.connection_ok:
            issues.append("connection is not reachable")

        components["schema"] = 1.0 if signal.schema_loaded else 0.0
        weights["schema"] = 2.0
        if not signal.schema_loaded:
            issues.append("schema not loaded for grounding")

        q = (signal.question or "").strip()
        q_ok = signal.min_question_len <= len(q) <= signal.max_question_len
        components["question_validity"] = 1.0 if q_ok else 0.0
        weights["question_validity"] = 2.0
        if not q_ok:
            issues.append("question is empty, too short or too long")

        if signal.pii_required:
            components["pii_masking"] = 1.0 if signal.pii_masked else 0.0
            weights["pii_masking"] = 2.0
            if not signal.pii_masked:
                issues.append("PII masking required but not applied")

        components["context_completeness"] = clamp01(signal.context_completeness)
        weights["context_completeness"] = 1.0

        m = self._result(components, weights, issues=issues,
                         evidence={"schema_table_count": signal.schema_table_count})
        # Hard gate: never allow when the connection or a required mask is missing.
        allow = (
            signal.connection_ok
            and q_ok
            and (not signal.pii_required or signal.pii_masked)
            and m.passed
        )
        m.evidence["allow"] = allow
        return m
