"""build_design_accuracy_meter — does the build follow the engine's blueprint?

Independent of which exact files exist, this checks structural/design rules the
AiAppEngine mandates for every app: a tests directory, documentation, a CI/CD
pipeline config, a hosting/deploy descriptor, separation of source from tests,
and a dependency manifest. Each satisfied rule contributes to the score.
"""

from __future__ import annotations

from collections.abc import Iterable

from ai_assistant.meters.base import Meter, Measurement


def _norm(paths: Iterable[str]) -> list[str]:
    return [str(p).strip().replace("\\", "/").lstrip("./") for p in (paths or []) if str(p).strip()]


# rule_id -> (description, predicate over the produced path list)
def _has_dir(files: list[str], name: str) -> bool:
    return any(f == name or f.startswith(name + "/") or ("/" + name + "/") in f for f in files)


def _has_any(files: list[str], names: tuple[str, ...]) -> bool:
    low = [f.lower() for f in files]
    return any(any(n in f for n in names) for f in low)


_RULES: dict[str, tuple[str, object]] = {
    "tests_dir": ("a tests/ directory exists", lambda f: _has_dir(f, "tests") or _has_dir(f, "test")),
    "docs": ("documentation exists", lambda f: _has_any(f, ("readme", "docs/", ".md"))),
    "ci_cd": ("CI/CD pipeline config exists",
              lambda f: _has_any(f, (".github/workflows", ".gitlab-ci", "jenkinsfile",
                                     "ci.yml", "ci.yaml", "cd.yml"))),
    "hosting": ("hosting/deploy descriptor exists",
                lambda f: _has_any(f, ("dockerfile", "docker-compose", "procfile",
                                       "vercel.json", "fly.toml", "k8s", "deploy"))),
    "dependency_manifest": ("dependency manifest exists",
                            lambda f: _has_any(f, ("requirements.txt", "pyproject.toml",
                                                   "package.json", "go.mod", "pom.xml",
                                                   "cargo.toml"))),
    "src_separation": ("source separated from tests",
                       lambda f: _has_any(f, ("src/", "app/")) or
                       any("/" in x and not x.lower().startswith(("test", "tests")) for x in f)),
}


class BuildDesignAccuracyMeter(Meter):
    name = "build_design_accuracy_meter"
    default_threshold = 0.75

    def measure(
        self,
        *,
        produced_files: Iterable[str],
        required_rules: Iterable[str] | None = None,
    ) -> Measurement:
        files = _norm(produced_files)
        rule_ids = list(required_rules) if required_rules else list(_RULES)
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        issues: list[str] = []
        satisfied: list[str] = []
        for rid in rule_ids:
            rule = _RULES.get(rid)
            if rule is None:
                continue
            desc, pred = rule
            ok = bool(pred(files))
            components[rid] = 1.0 if ok else 0.0
            weights[rid] = 1.0
            if ok:
                satisfied.append(rid)
            else:
                issues.append(f"design rule unmet: {desc}")
        return self._result(
            components, weights, issues=issues,
            evidence={"satisfied": satisfied, "checked": rule_ids},
        )
