"""data_understanding_meter — did we truly understand the DB before building?

For ``from_database`` builds the product requirement is explicit: the App Builder
must use the AI Query Assistant to understand the data first — the meaning of the
tables, the nature/kind of the data, real sample rows, and the data flow / what
application the schema supports — *before* generating an app. This meter scores
the consolidated :class:`DataInsight` so the build cannot proceed as "complete"
on a shallow, schema-only guess.

Deterministic: it inspects the gathered insight structure (counts, sample rows,
per-table notes, an overall app summary and a data-flow description). It does not
call a model — the model work happened upstream in the AI Query Assistant; this
meter audits that the work was actually done and is rich enough.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, weighted_score


class DataUnderstandingMeter(Meter):
    """Audit the completeness/richness of a gathered DataInsight."""

    name = "data_understanding_meter"
    default_threshold = 0.7

    def measure(self, insight: Mapping[str, Any]) -> Measurement:
        tables = list(insight.get("tables") or [])
        n = len(tables)

        if n == 0:
            return Measurement(
                meter=self.name, score=0.0,
                components={"metadata": 0.0, "sample_data": 0.0,
                            "nature": 0.0, "app_summary": 0.0, "data_flow": 0.0},
                weights={"metadata": 2.0, "sample_data": 3.0, "nature": 2.0,
                         "app_summary": 1.0, "data_flow": 1.0},
                evidence={"tables": 0},
                issues=["no tables were analyzed — DB understanding is empty"],
                threshold=self.default_threshold,
            )

        with_cols = sum(1 for t in tables if t.get("columns"))
        with_sample = sum(1 for t in tables if t.get("sample_rows"))
        with_note = sum(1 for t in tables if (t.get("note") or "").strip())

        metadata = with_cols / n
        sample_data = with_sample / n
        nature = with_note / n
        app_summary = 1.0 if (insight.get("app_summary") or "").strip() else 0.0
        data_flow = 1.0 if (insight.get("data_flow") or "").strip() else 0.0

        components = {
            "metadata": metadata, "sample_data": sample_data, "nature": nature,
            "app_summary": app_summary, "data_flow": data_flow,
        }
        weights = {"metadata": 2.0, "sample_data": 3.0, "nature": 2.0,
                   "app_summary": 1.0, "data_flow": 1.0}

        issues: list[str] = []
        no_cols = [t.get("name", "?") for t in tables if not t.get("columns")]
        no_sample = [t.get("name", "?") for t in tables if not t.get("sample_rows")]
        no_note = [t.get("name", "?") for t in tables
                   if not (t.get("note") or "").strip()]
        if no_cols:
            issues.append("no column metadata for: " + ", ".join(no_cols[:8]))
        if no_sample:
            issues.append("no sample data read for: " + ", ".join(no_sample[:8]))
        if no_note:
            issues.append("no nature-of-data note for: " + ", ".join(no_note[:8]))
        if not app_summary:
            issues.append("missing overall app summary (what app this data supports)")
        if not data_flow:
            issues.append("missing data-flow description")

        return Measurement(
            meter=self.name,
            score=weighted_score(components, weights),
            components=components,
            weights=weights,
            evidence={
                "tables": n, "with_columns": with_cols,
                "with_sample_rows": with_sample, "with_notes": with_note,
                "has_app_summary": bool(app_summary),
                "has_data_flow": bool(data_flow),
            },
            issues=issues,
            threshold=self.default_threshold,
        )
