"""error_meter — quantifies the error load of an AI response.

Counts concrete, machine-detectable errors (syntax, schema/hallucination,
execution, structural) and maps the total to a score where 0 errors -> 1.0 and
each additional error drives the score down with diminishing returns.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.meters import sqlmetrics as sm
from ai_assistant.meters.base import Meter, Measurement, diminishing


class ErrorMeter(Meter):
    name = "error_meter"
    default_threshold = 0.8

    def measure(
        self,
        sql: str,
        *,
        schema: dict[str, list[str]] | None = None,
        execution: dict[str, Any] | None = None,
        expects_results: bool = True,
    ) -> Measurement:
        errors: list[str] = []

        if not sm.parses(sql):
            errors.append("syntax: SQL does not parse")

        if schema:
            for t in sorted(sm.unknown_tables(sql, schema)):
                errors.append(f"schema: unknown table '{t}'")
            schema_ids = sm.schema_identifier_set(schema)
            for ident in sorted(sm.referenced_identifiers(sql) - schema_ids):
                errors.append(f"hallucination: unknown identifier '{ident}'")

        if execution is not None:
            if execution.get("error"):
                errors.append(f"execution: {execution['error']}")
            elif execution.get("ok") and expects_results:
                rc = execution.get("rowcount")
                if isinstance(rc, int) and rc < 0:
                    errors.append("execution: negative rowcount reported")

        # Structural sanity: empty / multi-statement where one was expected.
        if sql and sm.statement_count(sql) > 1 and expects_results:
            errors.append("structure: multiple statements for a single answer")

        score = diminishing(len(errors), half_life=1.5)
        return self._result(
            {"error_free": score},
            {"error_free": 1.0},
            evidence={"error_count": len(errors), "errors": errors},
            issues=errors,
        )
