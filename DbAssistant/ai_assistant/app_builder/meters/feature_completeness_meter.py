"""feature_completeness_meter — FCS (Feature Completeness Score).

Modeled on the Feature Completeness metric from application-level build
benchmarks (e.g. SWE-WebDevBench G1): of the features the user/requirement
asked for, how many are actually wired into the produced app — not merely
named, but reachable (a route/handler/template/test references them).

Deterministic: token/set math over produced files, never a model call.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from ai_assistant.meters.base import Meter, Measurement, clamp01
from ai_assistant.meters.requirement_fidelity_meter import _tokens, _variants


def _haystack(files: Mapping[str, str]) -> str:
    parts: list[str] = []
    for path, content in files.items():
        parts.append(path.lower())
        if path.lower().endswith(
                (".py", ".html", ".md", ".txt", ".sql", ".css", ".js",
                 ".yaml", ".yml", ".json")):
            parts.append((content or "").lower())
    return "\n".join(parts)


class FeatureCompletenessMeter(Meter):
    """Fraction of requested features that are actually implemented."""

    name = "feature_completeness_meter"
    default_threshold = 0.85

    def measure(
        self,
        *,
        features: Iterable[str],
        files: Mapping[str, str],
        description: str = "",
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        haystack = _haystack(files)

        wanted: list[str] = []
        seen: set[str] = set()
        for f in features:
            for t in _tokens(str(f)):
                if t not in seen:
                    seen.add(t)
                    wanted.append(t)
        # Description verbs/nouns enrich the feature set when none were given.
        if not wanted and description:
            wanted = _tokens(description)

        delivered: list[str] = []
        missing: list[str] = []
        for feat in wanted:
            if any(v in haystack for v in _variants(feat)):
                delivered.append(feat)
            else:
                missing.append(feat)

        fcs = 1.0 if not wanted else len(delivered) / len(wanted)
        # Reachability bonus: are there routes/handlers at all?
        reachable = bool(re.search(
            r"@router\.(get|post|put|delete|patch)|@app\.(get|post|put|delete|patch)"
            r"|def\s+\w+\s*\(", haystack))

        components = {
            "feature_recall": fcs,
            "reachable": 1.0 if reachable else 0.0,
        }
        weights = {"feature_recall": 4.0, "reachable": 1.0}
        score = clamp01(
            (components["feature_recall"] * 4.0 + components["reachable"]) / 5.0)

        issues: list[str] = []
        if missing:
            issues.append("features not yet implemented: " + ", ".join(missing[:12]))
        if not reachable:
            issues.append("no routes/handlers found — features may be unreachable")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"requested": wanted, "delivered": delivered,
                      "missing": missing, "fcs": round(fcs, 4)},
            issues=issues, threshold=thr,
        )
