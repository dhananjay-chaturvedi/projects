"""backend_logic_meter — BLS (Backend Logic Score).

Modeled on the Backend Logic metric from application-level build benchmarks
(SWE-WebDevBench G2): API design, route structure, and business-logic
correctness. Deterministic checks for:

* HTTP routes / handlers exist,
* a spread of verbs (not just GET) for state-changing apps,
* error handling on the request path (raised HTTP errors / try-except),
* input validation (schemas / Pydantic / explicit checks),
* status codes / responses,
* a separation between routing and logic (services/models referenced).

Deterministic: regex/structural analysis over produced Python files.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

_ROUTE_RE = re.compile(
    r"@(?:app|router)\.(get|post|put|delete|patch)\b", re.IGNORECASE)
_FLASK_ROUTE_RE = re.compile(r"@\w+\.route\(", re.IGNORECASE)
_RAISE_HTTP_RE = re.compile(r"HTTPException|abort\(|raise\s+\w*Error", re.IGNORECASE)
_TRYEXC_RE = re.compile(r"\btry\s*:", re.IGNORECASE)
_VALIDATION_RE = re.compile(
    r"BaseModel|pydantic|Field\(|validator|\.is_valid\(|schema|Schema",
    re.IGNORECASE)
_STATUS_RE = re.compile(r"status_code|status\.HTTP|return\s+jsonify|JSONResponse",
                        re.IGNORECASE)
_SERVICE_RE = re.compile(r"service|repository|crud|models?\.", re.IGNORECASE)
# CRUD operation detection: HTTP verbs map directly, plus function-name verbs so
# CLI/service-layer CRUD also counts.
_CRUD_NAME_RE = {
    "create": re.compile(r"\b(create|add|insert|new|register|post)_?\w*\(",
                         re.IGNORECASE),
    "read": re.compile(r"\b(get|list|read|fetch|find|show|view|all)_?\w*\(",
                       re.IGNORECASE),
    "update": re.compile(r"\b(update|edit|modify|patch|put|set)_?\w*\(",
                         re.IGNORECASE),
    "delete": re.compile(r"\b(delete|remove|destroy|drop|del)_?\w*\(",
                         re.IGNORECASE),
}
_VERB_TO_CRUD = {"post": "create", "get": "read",
                 "put": "update", "patch": "update", "delete": "delete"}


class BackendLogicMeter(Meter):
    """Score API design and business-logic correctness of the backend."""

    name = "backend_logic_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        blob = "\n".join(c for p, c in files.items() if p.endswith(".py") and c)

        verbs = {v.lower() for v in _ROUTE_RE.findall(blob)}
        n_routes = len(_ROUTE_RE.findall(blob)) + len(_FLASK_ROUTE_RE.findall(blob))

        has_routes = n_routes > 0
        verb_spread = len(verbs) >= 2 or bool(_FLASK_ROUTE_RE.search(blob))
        has_errors = bool(_RAISE_HTTP_RE.search(blob) or _TRYEXC_RE.search(blob))
        has_validation = bool(_VALIDATION_RE.search(blob))
        has_status = bool(_STATUS_RE.search(blob))
        has_separation = bool(_SERVICE_RE.search(blob))

        # CRUD coverage: which of create/read/update/delete are present, via
        # HTTP verbs OR function-name verbs (so CLI/service CRUD also counts).
        crud_ops = {_VERB_TO_CRUD[v] for v in verbs if v in _VERB_TO_CRUD}
        for op, rx in _CRUD_NAME_RE.items():
            if rx.search(blob):
                crud_ops.add(op)
        crud_coverage = len(crud_ops) / 4.0
        # Only judge CRUD completeness for apps that actually mutate state
        # (a read-only/report app legitimately lacks update/delete).
        is_crud_app = bool(crud_ops & {"create", "update", "delete"})

        components = {
            "has_routes": 1.0 if has_routes else 0.0,
            "verb_spread": 1.0 if verb_spread else 0.0,
            "error_handling": 1.0 if has_errors else 0.0,
            "input_validation": 1.0 if has_validation else 0.0,
            "status_codes": 1.0 if has_status else 0.0,
            "layer_separation": 1.0 if has_separation else 0.0,
        }
        weights = {
            "has_routes": 3.0, "verb_spread": 1.5, "error_handling": 2.0,
            "input_validation": 2.0, "status_codes": 1.0, "layer_separation": 1.0,
        }
        if is_crud_app:
            components["crud_coverage"] = crud_coverage
            weights["crud_coverage"] = 2.0
        score = weighted_score(components, weights)

        issues: list[str] = []
        if not has_routes:
            issues.append("no HTTP routes/handlers found")
        if has_routes and not verb_spread:
            issues.append("only one HTTP verb — state changes may be missing")
        if not has_errors:
            issues.append("no error handling on the request path")
        if not has_validation:
            issues.append("no input validation / request schemas")
        if is_crud_app and crud_coverage < 1.0:
            missing_crud = sorted({"create", "read", "update", "delete"}
                                  - crud_ops)
            issues.append("incomplete CRUD — missing: " + ", ".join(missing_crud))

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"routes": n_routes, "verbs": sorted(verbs),
                      "error_handling": has_errors, "validation": has_validation,
                      "crud_ops": sorted(crud_ops),
                      "crud_coverage": round(crud_coverage, 4)},
            issues=issues, threshold=thr,
        )
