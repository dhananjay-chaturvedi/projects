"""business_intent_meter — BIF (Business Intent Fidelity).

Modeled on the Business Intent Fidelity metric from application-level build
benchmarks (SWE-WebDevBench G1): does the produced app stay true to the
*domain and intent* the user described, rather than drifting into a generic
scaffold? A grocery store must stay groceries, not become a generic shop.

This reuses the deterministic intent-token extraction so the score reflects how
strongly the app's own vocabulary (routes, templates, identifiers, page text)
echoes the user's domain words, with extra credit when the domain noun appears
in user-facing surfaces (templates/docs), which is where intent is visible.

Deterministic: token/set math over produced files, never a model call.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score
from ai_assistant.meters.requirement_fidelity_meter import _tokens, _variants


class BusinessIntentMeter(Meter):
    """Score how faithfully the app reflects the user's business domain."""

    name = "business_intent_meter"
    default_threshold = 0.7

    def measure(
        self,
        *,
        description: str,
        files: Mapping[str, str],
        entities: Iterable[str] = (),
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold

        tokens = _tokens(description)
        for e in entities:
            for t in _tokens(str(e)):
                if t not in tokens:
                    tokens.append(t)

        code_blob, surface_blob = self._split(files)

        matched_code: list[str] = []
        matched_surface: list[str] = []
        missing: list[str] = []
        for t in tokens:
            vs = _variants(t)
            in_code = any(v in code_blob for v in vs)
            in_surface = any(v in surface_blob for v in vs)
            if in_code:
                matched_code.append(t)
            if in_surface:
                matched_surface.append(t)
            if not (in_code or in_surface):
                missing.append(t)

        code_recall = 1.0 if not tokens else len(matched_code) / len(tokens)
        surface_recall = 1.0 if not tokens else len(matched_surface) / len(tokens)

        components = {
            "domain_in_code": code_recall,
            "domain_in_surface": surface_recall,
        }
        weights = {"domain_in_code": 2.0, "domain_in_surface": 3.0}
        score = weighted_score(components, weights)

        issues: list[str] = []
        if missing:
            issues.append("business domain not reflected: " + ", ".join(missing[:12]))
        if surface_recall < code_recall - 0.3:
            issues.append("domain present in code but not user-facing surfaces")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"domain_tokens": tokens, "in_code": matched_code,
                      "in_surface": matched_surface, "missing": missing},
            issues=issues, threshold=thr,
        )

    @staticmethod
    def _split(files: Mapping[str, str]) -> tuple[str, str]:
        code: list[str] = []
        surface: list[str] = []
        for path, content in files.items():
            low = path.lower()
            body = (content or "").lower()
            if low.endswith((".html", ".md", ".rst", ".txt")) or low.startswith(
                    "templates/") or low.startswith("docs/"):
                surface.append(low)
                surface.append(body)
            elif low.endswith((".py", ".js", ".sql", ".yaml", ".yml", ".json")):
                code.append(low)
                code.append(body)
        return "\n".join(code), "\n".join(surface)
