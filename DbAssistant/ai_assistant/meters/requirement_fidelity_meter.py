"""requirement_fidelity_meter — how close is the built app to the user's prompt?

``requirement_coverage_meter`` answers CRUD/storefront recall (does each entity
have API + UI + tests). This meter is the *archetype-agnostic* fidelity signal
for arbitrary requests ("a staff shift scheduling app", "a blog with comments",
"a ticket booking app"): it extracts the salient intent tokens from the user's
description and measures how strongly the produced application reflects them —
across route paths, template names, identifiers, page text and docs — plus
whether the app actually ships a UI, an API and tests.

This is what catches "the user asked for X but we built generic CRUD": if the
build does not echo the request's own vocabulary/intent, fidelity is low and the
auto-build loop is told exactly which parts of the request are missing.

Fully deterministic: token/set math over the produced files, never a model call.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

# Generic words that carry no app-specific intent.
_STOP = {
    "app", "application", "apps", "system", "tool", "website", "site", "web",
    "api", "apis", "data", "database", "databases", "db", "user", "users",
    "the", "a", "an", "and", "or", "with", "for", "that", "this", "build",
    "building", "create", "creating", "make", "making", "manage", "manages",
    "managing", "management", "track", "tracks", "tracking", "store", "stores",
    "storing", "list", "lists", "listing", "of", "to", "all", "their", "your",
    "my", "our", "simple", "small", "based", "using", "use", "able", "ability",
    "add", "edit", "delete", "update", "view", "show", "new", "records",
    "record", "from", "scratch", "into", "able", "where", "which", "page",
    "pages", "screen", "screens", "able",
}


def _tokens(text: str) -> list[str]:
    """Salient lowercase intent tokens from a free-text description."""
    words = re.findall(r"[A-Za-z][A-Za-z_]+", (text or "").lower())
    out: list[str] = []
    seen: set[str] = set()
    for w in words:
        if len(w) < 3 or w in _STOP or w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out


def _variants(token: str) -> set[str]:
    """Cheap morphological variants so 'scheduling'≈'schedule', 'shift'≈'shifts'."""
    v = {token}
    if token.endswith("ies") and len(token) > 4:
        v.add(token[:-3] + "y")
    if token.endswith("es") and len(token) > 3:
        v.add(token[:-2])
    if token.endswith("s") and len(token) > 3:
        v.add(token[:-1])
    if token.endswith("ing") and len(token) > 5:
        stem = token[:-3]
        v.add(stem)
        v.add(stem + "e")  # scheduling -> schedul(e)
    if token.endswith("ed") and len(token) > 4:
        stem = token[:-2]
        v.add(stem)
        v.add(stem + "e")
    v.add(token + "s")
    return v


class RequirementFidelityMeter(Meter):
    """Score how faithfully the produced app reflects the requested app."""

    name = "requirement_fidelity_meter"
    default_threshold = 0.7

    def measure(
        self,
        *,
        description: str,
        files: Mapping[str, str],
        entities: Iterable[str] = (),
    ) -> Measurement:
        tokens = _tokens(description)
        # Entities the user explicitly named also count as intent tokens.
        for e in entities:
            for t in _tokens(str(e)):
                if t not in tokens:
                    tokens.append(t)

        haystack = self._haystack(files)
        matched: list[str] = []
        missing: list[str] = []
        for t in tokens:
            if any(v in haystack for v in _variants(t)):
                matched.append(t)
            else:
                missing.append(t)
        intent_recall = 1.0 if not tokens else len(matched) / len(tokens)

        has_api = bool(re.search(r"APIRouter|@router\.(get|post|put|delete|patch)",
                                 haystack))
        has_ui = ("htmlresponse" in haystack or "jinja2templates" in haystack
                  or any(p.startswith("templates/") for p in files))
        has_tests = any(
            p.startswith("tests/") and p.endswith(".py")
            and ("def test_" in (files.get(p) or ""))
            for p in files
        )

        components = {
            "intent_recall": intent_recall,
            "has_api": 1.0 if has_api else 0.0,
            "has_ui": 1.0 if has_ui else 0.0,
            "has_tests": 1.0 if has_tests else 0.0,
        }
        weights = {"intent_recall": 3.0, "has_api": 1.0,
                   "has_ui": 1.0, "has_tests": 1.0}

        issues: list[str] = []
        if missing:
            issues.append("app does not reflect requested intent: "
                          + ", ".join(missing[:12]))
        if not has_api:
            issues.append("no HTTP API found")
        if not has_ui:
            issues.append("no user interface (templates/HTML) found")
        if not has_tests:
            issues.append("no tests found")

        return Measurement(
            meter=self.name,
            score=weighted_score(components, weights),
            components=components,
            weights=weights,
            evidence={
                "intent_tokens": tokens,
                "matched": matched,
                "missing": missing,
                "has_api": has_api, "has_ui": has_ui, "has_tests": has_tests,
            },
            issues=issues,
            threshold=self.default_threshold,
        )

    @staticmethod
    def _haystack(files: Mapping[str, str]) -> str:
        parts: list[str] = []
        for path, content in files.items():
            parts.append(path.lower())
            low = path.lower()
            if low.endswith((".py", ".html", ".md", ".txt", ".sql", ".css",
                             ".js", ".yaml", ".yml")):
                parts.append((content or "").lower())
        return "\n".join(parts)
