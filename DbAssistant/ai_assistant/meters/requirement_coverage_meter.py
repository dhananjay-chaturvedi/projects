"""requirement_coverage_meter — is every requested requirement actually built?

Structural meters answer "is the code good?". This meter answers the *other*
half: "did we build what was asked?". For each requested **entity** and each
requested **feature** (list / create / edit / delete) it checks three surfaces:

* **API**   — a matching JSON route exists (GET/POST/PUT/DELETE) for the entity,
* **UI**    — a server-rendered page/route exists for the entity + feature, and
* **tests** — the entity is exercised by the generated test suite.

It is fully deterministic: routes are extracted from the produced source with
the :mod:`ast` module (decorator inspection), never by asking a model. The
output drives the auto-build loop's "done" condition — the loop keeps asking the
AI to fill gaps until each requirement has API + UI + tests.

Design note — why a *completeness* meter alongside quality meters:
modern eval stacks deliberately separate "faithfulness/quality of what was
produced" from "did it cover the request". RAG evaluation frameworks (e.g.
RAGAS) split *faithfulness* (grounding) from *answer relevance / context
recall* (completeness w.r.t. the request); code-generation benchmarks
(HumanEval-style *functional correctness* / pass@k) score whether the produced
code satisfies the asked-for behavior, not just whether it parses. This meter is
the app-builder analogue of those *recall/coverage* signals: precision-style
quality stays in the build/code meters, requirement recall lives here.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable, Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

# HTTP verbs we treat as route decorators.
_HTTP_METHODS = {"get", "post", "put", "delete", "patch"}

# Features the builder can expose and the API/UI shape each one needs.
_KNOWN_FEATURES = ("list", "create", "edit", "delete")

_REGISTRY_RE = re.compile(r'["\']table["\']\s*:\s*["\']([A-Za-z0-9_]+)["\']')
_REGISTRY_NAME_RE = re.compile(r"(entity|entities|table|tables|registry)", re.I)
_GENERIC_LIST_RE = re.compile(r"^/\{[^/}]+\}$")
_GENERIC_NEW_RE = re.compile(r"^/\{[^/}]+\}/new$")
_GENERIC_EDIT_RE = re.compile(r"^/\{[^/}]+\}/\{[^/}]+\}/edit$")
_GENERIC_DELETE_RE = re.compile(r"^/\{[^/}]+\}/\{[^/}]+\}/delete$")
_GENERIC_API_COLLECTION_RE = re.compile(r"^/api/\{[^/}]+\}$")
_GENERIC_API_ITEM_RE = re.compile(r"^/api/\{[^/}]+\}/\{[^/}]+\}$")
_TEST_ENTITY_LOOP_RE = re.compile(
    r"for\s+\w+\s+in\s+(?:\w+\.)?"
    r"(?:ENTITIES|ENTITY_REGISTRY|TABLE_REGISTRY|REGISTRY|table_names\(\)|get_entities\(\))",
    re.I,
)


def _call_name(func: ast.AST) -> str:
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _join(prefix: str, path: str) -> str:
    p = (prefix or "").rstrip("/")
    a = path if path.startswith("/") else "/" + path
    full = (p + a) or "/"
    if len(full) > 1:
        full = "/" + full.strip("/")
    return full


def _extract_routes(code: str) -> list[tuple[str, str]]:
    """Return ``(method, full_path)`` for every route decorator in *code*.

    Router/app ``prefix=`` is honored so ``APIRouter(prefix="/api")`` +
    ``@router.get("/customers")`` resolves to ``GET /api/customers``.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []
    prefixes: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            if _call_name(node.value.func) in ("APIRouter", "FastAPI"):
                prefix = ""
                for kw in node.value.keywords:
                    if kw.arg == "prefix" and isinstance(kw.value, ast.Constant):
                        prefix = str(kw.value.value)
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        prefixes[tgt.id] = prefix
    routes: list[tuple[str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if not (isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute)):
                continue
            method = dec.func.attr.lower()
            if method not in _HTTP_METHODS or not dec.args:
                continue
            arg0 = dec.args[0]
            if not (isinstance(arg0, ast.Constant) and isinstance(arg0.value, str)):
                continue
            owner = dec.func.value
            owner_name = owner.id if isinstance(owner, ast.Name) else ""
            routes.append((method, _join(prefixes.get(owner_name, ""), arg0.value)))
    return routes


def _is_web_file(code: str) -> bool:
    return any(m in code for m in ("HTMLResponse", "Jinja2Templates", "RedirectResponse"))


def _literal_segments(path: str) -> list[str]:
    return [s for s in path.strip("/").split("/") if s and not s.startswith("{")]


def _has_param(path: str) -> bool:
    return "{" in path


def _norm_name(value: object) -> str:
    return str(value or "").strip().lower()


def _extract_registry_tables(code: str) -> set[str]:
    """Find entity/table registry values used by metadata-driven CRUD apps."""
    tables = {m.group(1).lower() for m in _REGISTRY_RE.finditer(code)}
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return tables

    def _string_values(node: ast.AST) -> set[str]:
        found: set[str] = set()
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.add(_norm_name(node.value))
        elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            for elt in node.elts:
                found |= _string_values(elt)
        elif isinstance(node, ast.Dict):
            for key, val in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    key_name = _norm_name(key.value)
                    if key_name == "table":
                        found |= _string_values(val)
                    elif _REGISTRY_NAME_RE.search(key_name):
                        found.add(key_name)
                found |= _string_values(val)
        return {v for v in found if re.match(r"^[a-z][a-z0-9_]*$", v)}

    for node in ast.walk(tree):
        targets: list[ast.AST] = []
        value: ast.AST | None = None
        if isinstance(node, ast.Assign):
            targets, value = list(node.targets), node.value
        elif isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        if value is None:
            continue
        for target in targets:
            name = target.id if isinstance(target, ast.Name) else ""
            if name and _REGISTRY_NAME_RE.search(name):
                if isinstance(value, ast.Dict):
                    for key in value.keys:
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            key_name = _norm_name(key.value)
                            if re.match(r"^[a-z][a-z0-9_]*$", key_name):
                                tables.add(key_name)
                tables |= _string_values(value)
    return tables


class RequirementCoverageMeter(Meter):
    """Score how completely the build covers the requested entities/features."""

    name = "requirement_coverage_meter"
    default_threshold = 0.9

    def measure(
        self,
        *,
        entities: Iterable[str],
        features: Iterable[str],
        files: Mapping[str, str],
        services: Iterable[str] = (),
        kind: str = "crud",
    ) -> Measurement:
        if kind == "storefront":
            return self._measure_storefront(files, services)
        tables = [str(e).strip().lower() for e in entities if str(e).strip()]
        feats = [f for f in _KNOWN_FEATURES if f in set(features or ())] or list(
            _KNOWN_FEATURES
        )
        files = dict(files or {})

        api_routes, web_routes, registry = self._scan(files)
        test_text = self._test_text(files)
        generic = self._generic_web_flags(web_routes)
        generic_api = self._generic_api_flags(api_routes)

        per_entity: dict[str, dict] = {}
        gaps: list[str] = []
        api_scores: list[float] = []
        ui_scores: list[float] = []
        test_scores: list[float] = []

        for table in tables:
            api_ok = {
                f: self._api_covered(table, f, api_routes, generic_api, registry)
                for f in feats
            }
            ui_ok = {f: self._ui_covered(table, f, web_routes, generic, registry)
                     for f in feats}
            tested = self._tests_covered(table, test_text, registry)

            api_frac = sum(api_ok.values()) / len(feats)
            ui_frac = sum(ui_ok.values()) / len(feats)
            api_scores.append(api_frac)
            ui_scores.append(ui_frac)
            test_scores.append(1.0 if tested else 0.0)

            missing_api = [f for f in feats if not api_ok[f]]
            missing_ui = [f for f in feats if not ui_ok[f]]
            per_entity[table] = {
                "api": {f: api_ok[f] for f in feats},
                "ui": {f: ui_ok[f] for f in feats},
                "tests": tested,
                "missing_api": missing_api,
                "missing_ui": missing_ui,
            }
            if missing_api:
                gaps.append(f"{table}: missing API for {', '.join(missing_api)}")
            if missing_ui:
                gaps.append(f"{table}: missing UI for {', '.join(missing_ui)}")
            if not tested:
                gaps.append(f"{table}: no test exercises this entity")

        api_cov = sum(api_scores) / len(api_scores) if api_scores else 1.0
        ui_cov = sum(ui_scores) / len(ui_scores) if ui_scores else 1.0
        test_cov = sum(test_scores) / len(test_scores) if test_scores else 1.0

        # Requested infra services that left a footprint (recall on services).
        svc = [str(s).strip().lower() for s in services if str(s).strip()]
        svc_cov = self._service_coverage(svc, files)
        if svc:
            missing_svc = [s for s in svc if not self._service_present(s, files)]
            for s in missing_svc:
                gaps.append(f"service '{s}' is requested but not wired in")

        components = {
            "api_coverage": api_cov,
            "ui_coverage": ui_cov,
            "test_coverage": test_cov,
            "service_coverage": svc_cov,
        }
        weights = {
            "api_coverage": 3.0,
            "ui_coverage": 2.0,
            "test_coverage": 3.0,
            "service_coverage": 1.0,
        }
        score = weighted_score(components, weights)
        return Measurement(
            meter=self.name,
            score=score,
            components=components,
            weights={k: weights[k] for k in components},
            evidence={
                "entities": tables,
                "features": feats,
                "per_entity": per_entity,
                "gaps": gaps,
                "fully_covered": not gaps,
            },
            issues=gaps,
            threshold=self.default_threshold,
        )

    # ── storefront (ecommerce) coverage ──────────────────────────────────────
    def _measure_storefront(
        self, files: Mapping[str, str], services: Iterable[str]
    ) -> Measurement:
        """A storefront's requirements are its shopping surfaces, not table CRUD.

        Checks the catalog API (list/detail), an order/checkout API, the
        customer-facing pages (home, catalog, product, cart, checkout) and that
        the tests exercise the catalog + the cart/checkout flow.
        """
        api_routes, web_routes, _ = self._scan(files)
        test_text = self._test_text(files).lower()

        def _api(table: str, method: str, collection: bool) -> bool:
            for m, path in api_routes:
                segs = _literal_segments(path)
                if not (segs and segs[-1] == table and m == method):
                    continue
                if collection and not _has_param(path):
                    return True
                if not collection and _has_param(path):
                    return True
            return False

        web_paths = {p.rstrip("/") or "/" for _, p in web_routes}

        def _web(pred) -> bool:
            return any(pred(p) for p in web_paths)

        api_checks = {
            "products list API": _api("products", "get", True),
            "product detail API": _api("products", "get", False),
            "checkout/orders API": _api("orders", "post", True),
        }
        ui_checks = {
            "home page": "/" in web_paths,
            "catalog page": _web(lambda p: p == "/products"),
            "product detail page": _web(lambda p: p.startswith("/products/")),
            "cart page": _web(lambda p: p == "/cart"),
            "checkout page": _web(lambda p: p == "/checkout"),
        }
        tests_ok = "products" in test_text and (
            "cart" in test_text or "checkout" in test_text
        )

        api_cov = sum(api_checks.values()) / len(api_checks)
        ui_cov = sum(ui_checks.values()) / len(ui_checks)
        test_cov = 1.0 if tests_ok else 0.0

        svc = [str(s).strip().lower() for s in services if str(s).strip()]
        svc_cov = self._service_coverage(svc, files)

        gaps: list[str] = []
        gaps += [f"missing {name}" for name, ok in api_checks.items() if not ok]
        gaps += [f"missing {name}" for name, ok in ui_checks.items() if not ok]
        if not tests_ok:
            gaps.append("tests do not exercise the catalog + cart/checkout flow")
        for s in svc:
            if not self._service_present(s, files):
                gaps.append(f"service '{s}' is requested but not wired in")

        components = {
            "api_coverage": api_cov, "ui_coverage": ui_cov,
            "test_coverage": test_cov, "service_coverage": svc_cov,
        }
        weights = {"api_coverage": 3.0, "ui_coverage": 3.0,
                   "test_coverage": 2.0, "service_coverage": 1.0}
        return Measurement(
            meter=self.name, score=weighted_score(components, weights),
            components=components, weights={k: weights[k] for k in components},
            evidence={
                "kind": "storefront",
                "api": api_checks, "ui": ui_checks, "tests": tests_ok,
                "gaps": gaps, "fully_covered": not gaps,
            },
            issues=gaps, threshold=self.default_threshold,
        )

    # ── scanning helpers ─────────────────────────────────────────────────────
    @staticmethod
    def _test_text(files: Mapping[str, str]) -> str:
        """Concatenate real test modules' source.

        Excludes ``tests/test_sample_data/`` — those are data fixtures that list
        every entity for setup and would otherwise falsely satisfy the
        per-entity "tested" signal.
        """
        return "\n".join(
            c for p, c in files.items()
            if p.startswith("tests/") and p.endswith(".py")
            and not p.startswith("tests/test_sample_data/")
        )

    def _scan(self, files: Mapping[str, str]):
        api_routes: list[tuple[str, str]] = []
        web_routes: list[tuple[str, str]] = []
        registry: set[str] = set()
        for path, code in files.items():
            if not path.endswith(".py") or path.startswith("tests/"):
                continue
            registry |= _extract_registry_tables(code)
            routes = _extract_routes(code)
            if not routes:
                continue
            if _is_web_file(code):
                web_routes += routes
            else:
                api_routes += routes
        return api_routes, web_routes, registry

    @staticmethod
    def _generic_web_flags(web_routes: list[tuple[str, str]]) -> dict[str, bool]:
        paths = [p for _, p in web_routes]
        return {
            "list": any(_GENERIC_LIST_RE.match(p) for p in paths),
            "create": any(_GENERIC_NEW_RE.match(p) for p in paths),
            "edit": any(_GENERIC_EDIT_RE.match(p) for p in paths),
            "delete": any(_GENERIC_DELETE_RE.match(p) for p in paths),
        }

    @staticmethod
    def _generic_api_flags(api_routes: list[tuple[str, str]]) -> dict[str, bool]:
        return {
            "list": any(m == "get" and _GENERIC_API_COLLECTION_RE.match(p)
                        for m, p in api_routes),
            "create": any(m == "post" and _GENERIC_API_COLLECTION_RE.match(p)
                          for m, p in api_routes),
            "edit": any(m in ("put", "patch") and _GENERIC_API_ITEM_RE.match(p)
                        for m, p in api_routes),
            "delete": any(m == "delete" and _GENERIC_API_ITEM_RE.match(p)
                          for m, p in api_routes),
        }

    @staticmethod
    def _api_covered(
        table: str,
        feature: str,
        routes: list[tuple[str, str]],
        generic: dict[str, bool],
        registry: set[str],
    ) -> bool:
        if generic.get(feature) and table in registry:
            return True
        for method, path in routes:
            segs = _literal_segments(path)
            # The route targets this entity when its last literal segment is the
            # table (collection: /api/customers; item: /api/customers/{id}).
            on_resource = bool(segs) and segs[-1] == table
            if not on_resource:
                continue
            collection = not _has_param(path)
            item = _has_param(path)
            if feature == "list" and method == "get" and collection:
                return True
            if feature == "create" and method == "post" and collection:
                return True
            if feature == "edit" and method in ("put", "patch") and item:
                return True
            if feature == "delete" and method == "delete" and item:
                return True
        return False

    @staticmethod
    def _tests_covered(table: str, test_text: str, registry: set[str]) -> bool:
        if re.search(r"\b" + re.escape(table) + r"\b", test_text):
            return True
        if table not in registry:
            return False
        if not _TEST_ENTITY_LOOP_RE.search(test_text):
            return False
        low = test_text.lower()
        return (
            "/api/{entity}" in low
            or "f\"/api/{entity}" in low
            or "f'/api/{entity}" in low
            or "client.get" in low
            or "client.post" in low
        )

    @staticmethod
    def _ui_covered(
        table: str,
        feature: str,
        web_routes: list[tuple[str, str]],
        generic: dict[str, bool],
        registry: set[str],
    ) -> bool:
        registered = (table in registry) if registry else True
        if generic.get(feature) and registered:
            return True
        # Literal per-entity web routes (AI may rewrite the generic ones).
        for _, path in web_routes:
            segs = _literal_segments(path)
            if table not in segs:
                continue
            if feature == "list" and path.rstrip("/") == "/" + table:
                return True
            if feature == "create" and path.endswith("/new") and segs[0] == table:
                return True
            if feature == "edit" and path.endswith("/edit") and segs[0] == table:
                return True
            if feature == "delete" and path.endswith("/delete") and segs[0] == table:
                return True
        return False

    # ── infra service recall ─────────────────────────────────────────────────
    _SERVICE_MARKERS: dict[str, tuple[str, ...]] = {
        "notification": ("notification",),
        "document": ("docs/", "document"),
        "hosting": ("dockerfile", "deploy/", "hosting"),
        "ci_cd": (".github/workflows", "ci.yml"),
        "database": ("schema.sql", "src/db/"),
        "monitoring": ("monitoring", "health"),
        "ai_builder": ("builder_hooks", "src/ai/"),
    }

    def _service_present(self, service: str, files: Mapping[str, str]) -> bool:
        markers = self._SERVICE_MARKERS.get(service, (service,))
        low = [p.lower() for p in files]
        if any(any(m in p for m in markers) for p in low):
            return True
        blob = "\n".join(files.values()).lower()
        return any(m in blob for m in markers)

    def _service_coverage(self, services: list[str], files: Mapping[str, str]) -> float:
        if not services:
            return 1.0
        present = sum(1 for s in services if self._service_present(s, files))
        return present / len(services)
