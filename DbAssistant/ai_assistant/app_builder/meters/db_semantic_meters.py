"""DB semantic meters for predicted apps built from database profiles."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Measurement, Meter, coverage, weighted_score


def _blob(files: Mapping[str, str]) -> str:
    return ("\n".join(files.values()) + "\n" + "\n".join(files)).lower()


def _tokens(text: str) -> set[str]:
    return {t for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", text.lower())}


def _profile_tables(profile: Mapping[str, Any] | None) -> list[dict]:
    return list((profile or {}).get("tables") or [])


class RelationshipFidelityMeter(Meter):
    name = "relationship_fidelity_meter"
    default_threshold = 0.65

    def measure(
        self, files: Mapping[str, str], *, profile: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        rels = list((profile or {}).get("relationships") or [])
        if not rels:
            return Measurement(
                self.name, 1.0, {"edge_recall": 1.0}, {"edge_recall": 1.0},
                {"applicable": False}, [], threshold or self.default_threshold)
        text = _blob(files)
        expected = {
            f"{r.get('from_table')}.{r.get('from_column')}->{r.get('to_table')}.{r.get('to_column')}"
            for r in rels
        }
        present = set()
        for r in rels:
            ft = str(r.get("from_table") or "").lower()
            tt = str(r.get("to_table") or "").lower()
            fc = str(r.get("from_column") or "").lower()
            if ft in text and tt in text and (
                fc in text or "join" in text or "relationship" in text
                or f"{tt}/" in text or f"{ft}/" in text
            ):
                present.add(f"{r.get('from_table')}.{r.get('from_column')}->{r.get('to_table')}.{r.get('to_column')}")
        score = coverage(expected, present)
        missing = sorted(expected - present)
        return Measurement(
            self.name, score, {"edge_recall": score}, {"edge_recall": 1.0},
            {"expected": sorted(expected), "present": sorted(present)},
            [f"relationship not reflected in app: {m}" for m in missing[:8]],
            threshold or self.default_threshold,
        )


class EntityRoleFitMeter(Meter):
    name = "entity_role_fit_meter"
    default_threshold = 0.65

    def measure(
        self, files: Mapping[str, str], *, profile: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        tables = [t for t in _profile_tables(profile) if t.get("role")]
        if not tables:
            return Measurement(
                self.name, 1.0, {"role_fit": 1.0}, {"role_fit": 1.0},
                {"applicable": False}, [], threshold or self.default_threshold)
        text = _blob(files)
        scores: list[float] = []
        issues: list[str] = []
        for table in tables:
            name = str(table.get("name") or "").lower()
            role = str(table.get("role") or "")
            table_present = 1.0 if name in text else 0.0
            role_score = table_present
            if role == "junction":
                role_score = 1.0 if name in text and any(k in text for k in ("associate", "assign", "link", "attach", "detach")) else 0.45 * table_present
            elif role == "lookup":
                role_score = 1.0 if name in text and any(k in text for k in ("select", "option", "filter", "choice")) else 0.55 * table_present
            elif role == "audit":
                role_score = 1.0 if name in text and any(k in text for k in ("timeline", "read-only", "readonly", "history", "event")) else 0.55 * table_present
            elif role == "transaction":
                role_score = 1.0 if name in text and any(k in text for k in ("detail", "filter", "timeline", "status", "dashboard")) else 0.7 * table_present
            scores.append(role_score)
            if role_score < 0.65:
                issues.append(f"{table.get('name')} role '{role}' is not clearly reflected")
        score = sum(scores) / len(scores)
        return Measurement(
            self.name, score, {"role_fit": score}, {"role_fit": 1.0},
            {"roles": {t.get("name"): t.get("role") for t in tables}},
            issues[:8], threshold or self.default_threshold,
        )


class DataSemanticsMeter(Meter):
    name = "data_semantics_meter"
    default_threshold = 0.6

    _EXPECT = {
        "money": ("currency", "$", "amount", "price", "total"),
        "temporal": ("date", "time", "calendar", "datetime"),
        "enum": ("select", "option", "choice", "filter"),
        "pii": ("mask", "privacy", "email", "phone", "redact"),
        "status": ("status", "state", "filter"),
    }

    def measure(
        self, files: Mapping[str, str], *, profile: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        tagged = []
        for table in _profile_tables(profile):
            for col in table.get("columns") or []:
                for tag in col.get("semantic_tags") or []:
                    if tag in self._EXPECT:
                        tagged.append((table.get("name"), col.get("name"), tag))
        if not tagged:
            return Measurement(
                self.name, 1.0, {"semantic_widget_fit": 1.0},
                {"semantic_widget_fit": 1.0}, {"applicable": False}, [],
                threshold or self.default_threshold)
        text = _blob(files)
        hits = []
        for table, col, tag in tagged:
            if str(col or "").lower() in text and any(k in text for k in self._EXPECT[tag]):
                hits.append((table, col, tag))
        score = len(hits) / len(tagged)
        missing = [f"{t}.{c} ({tag})" for t, c, tag in tagged if (t, c, tag) not in hits]
        return Measurement(
            self.name, score, {"semantic_widget_fit": score},
            {"semantic_widget_fit": 1.0},
            {"tagged": tagged, "present": hits},
            [f"semantic treatment missing for {m}" for m in missing[:8]],
            threshold or self.default_threshold,
        )


class WorkflowCoverageMeter(Meter):
    name = "workflow_coverage_meter"
    default_threshold = 0.65

    def measure(
        self, files: Mapping[str, str], *, insight: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        features = [str(f).strip() for f in (insight or {}).get("app_features") or [] if str(f).strip()]
        if not features:
            return Measurement(
                self.name, 1.0, {"feature_surface_recall": 1.0},
                {"feature_surface_recall": 1.0}, {"applicable": False}, [],
                threshold or self.default_threshold)
        text = _blob(files)
        present = []
        for feature in features:
            toks = _tokens(feature)
            if toks and sum(1 for t in toks if t in text) / len(toks) >= 0.4:
                present.append(feature)
        score = len(present) / len(features)
        missing = [f for f in features if f not in present]
        return Measurement(
            self.name, score, {"feature_surface_recall": score},
            {"feature_surface_recall": 1.0},
            {"expected_features": features, "present": present},
            [f"predicted workflow not surfaced: {m}" for m in missing[:8]],
            threshold or self.default_threshold,
        )


class PredictionGroundingMeter(Meter):
    name = "prediction_grounding_meter"
    default_threshold = 0.65

    def measure(
        self, *, profile: Mapping[str, Any] | None = None,
        insight: Mapping[str, Any] | None = None,
        threshold: float | None = None,
    ) -> Measurement:
        if not insight:
            return Measurement(
                self.name, 1.0, {"grounding": 1.0}, {"grounding": 1.0},
                {"applicable": False}, [], threshold or self.default_threshold)
        prediction = " ".join([
            str(insight.get("app_name") or ""),
            str(insight.get("app_summary") or ""),
            str(insight.get("data_flow") or ""),
            " ".join(str(f) for f in insight.get("app_features") or []),
        ])
        pred_tokens = _tokens(prediction)
        if not pred_tokens:
            return Measurement(
                self.name, 1.0, {"grounding": 1.0}, {"grounding": 1.0},
                {"applicable": False}, [], threshold or self.default_threshold)
        db_text = []
        for table in _profile_tables(profile):
            db_text.append(str(table.get("name") or ""))
            db_text += [str(c.get("name") or "") for c in table.get("columns") or []]
            db_text.append(str(table.get("role") or ""))
        for rel in (profile or {}).get("relationships") or []:
            db_text += [str(rel.get("from_table") or ""), str(rel.get("to_table") or "")]
        grounded_tokens = _tokens(" ".join(db_text))
        matched = {t for t in pred_tokens if t in grounded_tokens}
        # Product words such as "app", "manage", and "dashboard" are expected
        # not to be table names, so cap the penalty from generic vocabulary.
        score = min(1.0, (len(matched) / max(1, len(pred_tokens))) + 0.45)
        return Measurement(
            self.name, score, {"grounding": score}, {"grounding": 1.0},
            {"matched_tokens": sorted(matched), "db_tokens": sorted(grounded_tokens)[:100]},
            [] if score >= (threshold or self.default_threshold)
            else ["predicted app/workflows are weakly grounded in profiled tables and columns"],
            threshold or self.default_threshold,
        )
