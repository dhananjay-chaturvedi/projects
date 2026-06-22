"""architecture_meter — ARC (Architecture Score).

Modeled on the Architecture metric from application-level build benchmarks
(SWE-WebDevBench G2): overall system design — separation of layers, dependency
management, and pattern consistency. Deterministic checks for:

* a recognizable layout (a source package, not everything in one file),
* layer separation (models / routes-or-api / services / templates-or-ui),
* a dependency manifest (requirements.txt / pyproject / package.json),
* an app entry point / runnable contract,
* tests living in their own location,
* config/settings separated from logic.

Deterministic: path/structure analysis over the produced file set.
"""

from __future__ import annotations

from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score


class ArchitectureMeter(Meter):
    """Score the structural/architectural quality of the produced app."""

    name = "architecture_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        paths = [p.lower() for p in files]

        def any_path(*frags: str) -> bool:
            return any(any(f in p for f in frags) for p in paths)

        n_py = sum(1 for p in paths if p.endswith(".py"))

        has_package = any("/" in p for p in paths) and n_py >= 2
        has_models = any_path("model", "schema", "entit")
        has_routes = any_path("route", "/api", "view", "controller", "handler",
                              "endpoint")
        has_services = any_path("service", "repository", "crud", "logic")
        has_ui = any_path("template", ".html", "static/", "frontend")
        has_manifest = any(
            p.endswith(("requirements.txt", "pyproject.toml", "package.json",
                        "pipfile", "setup.py")) for p in paths)
        has_entry = any_path("app.py", "main.py", "__init__.py", "manage.py",
                             "wsgi.py", "asgi.py")
        has_tests = any("test" in p and p.endswith(".py") for p in paths)
        has_config = any_path("config", "settings", ".env", "conftest")

        # Layer separation: how many distinct layers are present.
        layers = sum([has_models, has_routes or has_ui, has_services])

        components = {
            "package_layout": 1.0 if has_package else 0.0,
            "layer_separation": min(1.0, layers / 2.0),
            "dependency_manifest": 1.0 if has_manifest else 0.0,
            "entry_point": 1.0 if has_entry else 0.0,
            "tests_located": 1.0 if has_tests else 0.0,
            "config_separated": 1.0 if has_config else 0.0,
        }
        weights = {
            "package_layout": 2.0, "layer_separation": 3.0,
            "dependency_manifest": 1.5, "entry_point": 2.0,
            "tests_located": 1.5, "config_separated": 1.0,
        }
        score = weighted_score(components, weights)

        issues: list[str] = []
        if not has_package:
            issues.append("flat layout — no source package structure")
        if layers < 2:
            issues.append("weak layer separation (models/routes/services)")
        if not has_manifest:
            issues.append("no dependency manifest")
        if not has_entry:
            issues.append("no clear app entry point")
        if not has_tests:
            issues.append("no tests located in the project")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"has_models": has_models, "has_routes": has_routes,
                      "has_services": has_services, "has_ui": has_ui,
                      "layers": layers, "py_files": n_py,
                      "has_manifest": has_manifest, "has_tests": has_tests},
            issues=issues, threshold=thr,
        )
