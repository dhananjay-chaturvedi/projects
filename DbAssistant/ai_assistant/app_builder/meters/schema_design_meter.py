"""schema_design_meter — SDS (Schema Design Score).

Modeled on the Schema Design metric from application-level build benchmarks
(SWE-WebDevBench G2), which calls poor schema design "the single most expensive
technical debt category in web applications." It checks the data layer for:

* a data model at all (ORM models, CREATE TABLE, or a typed schema),
* primary keys,
* referential integrity (foreign keys / relationships) when there are ≥2 tables,
* indexing on lookup/foreign-key columns,
* parameterized access (no string-formatted SQL),
* timestamps / soft-delete hygiene (soft).

Deterministic: regex/structural analysis over the produced files.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

_TABLE_RE = re.compile(r"create\s+table\s+(?:if\s+not\s+exists\s+)?[\"`']?(\w+)",
                       re.IGNORECASE)
_MODEL_RE = re.compile(
    r"class\s+\w+\s*\((?:[^)]*\b(?:Base|Model|SQLModel|db\.Model)\b[^)]*)\)")
_PK_RE = re.compile(r"primary\s+key|primary_key\s*=\s*True|\bid\s+integer",
                    re.IGNORECASE)
_FK_RE = re.compile(r"foreign\s+key|ForeignKey|references\s+\w+|relationship\(",
                    re.IGNORECASE)
_INDEX_RE = re.compile(r"create\s+index|index\s*=\s*True|\bUniqueConstraint",
                       re.IGNORECASE)
_BAD_SQL_RE = re.compile(
    r'(?:execute|executemany)\s*\(\s*f["\']|(?:execute|executemany)\s*\([^)]*%\s',
    re.IGNORECASE)
_TS_RE = re.compile(r"created_at|updated_at|timestamp|datetime", re.IGNORECASE)


class SchemaDesignMeter(Meter):
    """Score the quality of the generated data model / schema."""

    name = "schema_design_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        blob = "\n".join(
            c for p, c in files.items()
            if p.endswith((".py", ".sql")) and c)

        tables = set(_TABLE_RE.findall(blob))
        n_models = len(_MODEL_RE.findall(blob))
        table_count = max(len(tables), n_models)

        has_model = table_count > 0
        has_pk = bool(_PK_RE.search(blob))
        has_fk = bool(_FK_RE.search(blob))
        has_index = bool(_INDEX_RE.search(blob))
        bad_sql = bool(_BAD_SQL_RE.search(blob))
        has_ts = bool(_TS_RE.search(blob))

        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        issues: list[str] = []

        components["has_model"] = 1.0 if has_model else 0.0
        weights["has_model"] = 3.0
        if not has_model:
            issues.append("no data model / schema found")

        components["primary_keys"] = 1.0 if has_pk else 0.0
        weights["primary_keys"] = 2.0
        if has_model and not has_pk:
            issues.append("no primary keys declared")

        # Referential integrity only matters with multiple tables.
        if table_count >= 2:
            components["referential_integrity"] = 1.0 if has_fk else 0.0
            weights["referential_integrity"] = 2.0
            if not has_fk:
                issues.append("multiple tables but no foreign keys / relationships")

        components["indexing"] = 1.0 if has_index else 0.0
        weights["indexing"] = 1.0
        if has_model and not has_index:
            issues.append("no indexes / unique constraints (soft)")

        components["parameterized_access"] = 0.0 if bad_sql else 1.0
        weights["parameterized_access"] = 2.0
        if bad_sql:
            issues.append("string-formatted SQL detected — use parameters")

        components["timestamps"] = 1.0 if has_ts else 0.0
        weights["timestamps"] = 0.5

        # If there is genuinely no data layer, don't punish a stateless app to 0;
        # report a neutral-low score and flag it.
        if not has_model:
            score = 0.3 if blob else 0.0
        else:
            score = weighted_score(components, weights)

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"tables": sorted(tables), "table_count": table_count,
                      "has_pk": has_pk, "has_fk": has_fk, "has_index": has_index,
                      "string_sql": bad_sql, "timestamps": has_ts},
            issues=issues, threshold=thr,
        )
