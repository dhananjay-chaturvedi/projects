"""accuracy_meter — how accurately an AI SQL answer matches the request + reality.

Deterministic signals only:
* the generated SQL parses;
* every table it references exists in the real schema (no hallucinated tables);
* identifiers used exist as real columns/tables;
* the SQL actually executed (when execution feedback is supplied);
* the SQL's identifiers cover the entities implied by the question.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.meters import sqlmetrics as sm
from ai_assistant.meters.base import Meter, Measurement, jaccard

_STOP = frozenset(
    """a an the of for to in on at by with and or how many much show list get
    find all give me what which who whose are is be do does count number total
    each every from into table tables database row rows record records""".split()
)


def _question_terms(question: str) -> set[str]:
    raw = sm._WORD_RE.findall((question or "").lower())
    return {w for w in raw if w not in _STOP and len(w) > 2 and not w.isdigit()}


class AccuracyMeter(Meter):
    name = "accuracy_meter"
    default_threshold = 0.75

    def measure(
        self,
        question: str,
        sql: str,
        *,
        schema: dict[str, list[str]] | None = None,
        execution: dict[str, Any] | None = None,
    ) -> Measurement:
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        issues: list[str] = []
        evidence: dict[str, Any] = {}

        parsed = sm.parses(sql)
        components["sql_parses"] = 1.0 if parsed else 0.0
        weights["sql_parses"] = 2.0
        if not parsed:
            issues.append("generated SQL does not parse")

        tables = sm.referenced_tables(sql)
        idents = sm.referenced_identifiers(sql)
        evidence["referenced_tables"] = sorted(tables)

        if schema:
            schema_ids = sm.schema_identifier_set(schema)
            unknown_t = sm.unknown_tables(sql, schema)
            evidence["unknown_tables"] = sorted(unknown_t)
            components["table_validity"] = (
                1.0 - (len(unknown_t) / len(tables)) if tables else 1.0
            )
            weights["table_validity"] = 2.0
            if unknown_t:
                issues.append(f"references unknown table(s): {', '.join(sorted(unknown_t))}")

            unknown_ids = {i for i in idents if i not in schema_ids}
            evidence["unknown_identifiers"] = sorted(unknown_ids)
            components["identifier_validity"] = (
                1.0 - (len(unknown_ids) / len(idents)) if idents else 1.0
            )
            weights["identifier_validity"] = 1.0

        if execution is not None:
            ok = bool(execution.get("ok")) and not execution.get("error")
            components["execution_success"] = 1.0 if ok else 0.0
            weights["execution_success"] = 3.0
            if not ok:
                issues.append(f"execution failed: {execution.get('error', 'unknown error')}")
            evidence["rowcount"] = execution.get("rowcount")

        qterms = _question_terms(question)
        intent = jaccard(qterms, idents) if qterms else 1.0
        # Reward partial overlap generously: coverage of question terms by SQL.
        covered = len(qterms & idents) / len(qterms) if qterms else 1.0
        components["intent_coverage"] = max(intent, covered)
        weights["intent_coverage"] = 1.0
        evidence["question_terms"] = sorted(qterms)

        return self._result(components, weights, evidence=evidence, issues=issues)
