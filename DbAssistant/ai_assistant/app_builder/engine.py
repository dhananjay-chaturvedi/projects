"""AiAppEngine — code-enforced app-building rules (not prompt-only).

Every generated artifact passes through this engine. Rules are evaluated with
deterministic logic; prompts elsewhere only supply instructions — the engine
decides accept/reject using the meters subsystem.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ai_assistant.meters import MeterSuite


class BuildMode(str, Enum):
    FROM_SCRATCH = "from_scratch"
    FROM_CODEBASE = "from_codebase"
    FROM_DATABASE = "from_database"


# Built-in service templates the user configures (not code they write).
SERVICE_TEMPLATES = (
    "notification",
    "document",
    "hosting",
    "ci_cd",
    "database",
    "monitoring",
    "ai_builder",
)


# Standardized TEST TAXONOMY — every app ships these test folders so the
# builder (Session A) lays down a clean skeleton first and the validator
# (Session C) always has a known home for each kind of check. Order matters
# only for readability.
TEST_TAXONOMY = (
    "unit_test",        # fast, isolated unit tests
    "full_test",        # end-to-end / full-suite tests
    "write_test_cases",  # scratch space for newly written / ad-hoc cases
    "connectivity",     # app boots, routes reachable, infra wired
    "db",               # schema, queries, read-only DB probes
    "api",              # per-endpoint API contract tests
    "functionality",    # feature/behavior tests mapped to the requirement
    "test_sample_data",  # sample data fixtures used by the tests above
)

# Skeleton files derived from the taxonomy. Each test folder is a real package
# (``__init__.py``) and ``test_sample_data`` also ships a sample-data module.
SKELETON_TEST_FILES = [f"tests/{d}/__init__.py" for d in TEST_TAXONOMY] + [
    "tests/test_sample_data/sample_data.py",
]

# Documentation skeleton — Session B (advisor) and Session C (validator) read
# these to frame accurate, requirement-grounded answers and tests.
SKELETON_DOC_FILES = ["docs/README.md", "docs/requirement.txt"]

# Minimal runnable surface for from_scratch — the only enforced contract so
# platform hosting / Start app / monitoring keep working. The agent owns all
# other folders and files.
SCRATCH_CONTRACT = ["src/app.py", "requirements.txt"]

# Folder OWNED by the validator (Session C). Session C writes its own,
# independently-authored test files here (and ONLY here); Session A must never
# create, edit, or depend on it. The orchestrator hard-enforces this boundary by
# reverting any out-of-folder change C attempts, and the folder is excluded from
# A's coverage/manifest contract so it can never block or alter the build.
VALIDATOR_TEST_DIR = "validator_generated_tests"


@dataclass
class AppBlueprint:
    """User configuration — mostly toggles, not hand-written code."""

    name: str
    mode: BuildMode = BuildMode.FROM_SCRATCH
    services: list[str] = field(default_factory=list)
    connections: list[str] = field(default_factory=list)
    codebase_path: str = ""
    language: str = "python"
    description: str = ""
    entities: list[str] = field(default_factory=list)
    features: list[str] = field(default_factory=list)
    kind: str = ""  # app archetype (crud | storefront); "" = auto-detect
    build_profile: str = "prototype"  # prototype | full
    variant: str = "application"  # application | explorer
    db_app_variant: str = "application"  # application | insights_admin
    codebase_variant: str = "predicted_app"  # predicted_app | structure_metadata


@dataclass
class EngineVerdict:
    accepted: bool
    score: float
    issues: list[str] = field(default_factory=list)
    measurements: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "score": round(self.score, 4),
            "issues": list(self.issues),
            "measurements": self.measurements,
        }


class AiAppEngine:
    """Central controller for safe, reliable AI-driven app building."""

    # Structural skeleton every generated full-stack app produces, regardless of
    # archetype (API + server-rendered UI + data layer + infra config). Kept in
    # sync with the generators so build precision is fair. Archetype-specific
    # templates are added per-kind in ``expected_manifest``.
    CORE_FILES = [
        "README.md",
        "requirements.txt",
        "Dockerfile",
        ".github/workflows/ci.yml",
        "config/infra.yaml",
        "src/__init__.py",
        "src/settings.py",
        "src/app.py",
        "src/api.py",
        "src/web.py",
        "src/models.py",
        "src/repository.py",
        "src/db/__init__.py",
        "src/db/schema.py",
        "src/db/schema.sql",
        "src/db/connection.py",
        "templates/base.html",
        "templates/index.html",
        "static/style.css",
        "tests/__init__.py",
        "tests/conftest.py",
        "tests/test_app.py",
        "docs/ARCHITECTURE.md",
        "deploy/hosting.yaml",
    ] + SKELETON_TEST_FILES + SKELETON_DOC_FILES

    # Archetype-specific templates (added on top of CORE_FILES).
    _TEMPLATES_BY_KIND: dict[str, list[str]] = {
        "crud": ["templates/list.html", "templates/form.html"],
        "storefront": [
            "templates/catalog.html", "templates/product.html",
            "templates/cart.html", "templates/checkout.html",
            "templates/confirmation.html",
        ],
        "insights": [
            "templates/table_detail.html", "templates/relationships.html",
            "templates/sample.html", "src/introspect.py",
        ],
    }

    # Minimal artifacts for an analysis-only (existing) codebase build.
    CODEBASE_FILES = [
        "README.md",
        "docs/ANALYSIS.md",
        "tests/test_smoke.py",
        ".github/workflows/ci.yml",
    ]

    # Infrastructure / operational add-ons. These make an app production-ready
    # but are NOT part of the core runnable application, so the per-commit gate
    # treats them as optional suggestions and only enforces them at the start
    # and end of a build (see ``evaluate_build(include_infra=...)``).
    INFRA_SERVICES = frozenset(
        {"document", "hosting", "monitoring", "notification", "ci_cd"})
    _INFRA_PREFIXES = ("docs/", "deploy/", ".github/", "src/infra/")
    _INFRA_FILES = frozenset({"Dockerfile", "config/infra.yaml"})

    @classmethod
    def _is_infra_file(cls, path: str) -> bool:
        return (path in cls._INFRA_FILES
                or any(path.startswith(p) for p in cls._INFRA_PREFIXES))

    def __init__(self) -> None:
        self.meters = MeterSuite()

    def validate_blueprint(self, blueprint: AppBlueprint) -> EngineVerdict:
        issues: list[str] = []
        if not (blueprint.name or "").strip():
            issues.append("app name is required")
        else:
            try:
                from common.security.paths import assert_safe_name

                assert_safe_name(blueprint.name, label="app name")
            except Exception as exc:  # noqa: BLE001
                issues.append(str(exc))
        unknown = [s for s in blueprint.services if s not in SERVICE_TEMPLATES]
        if unknown:
            issues.append(f"unknown service(s): {', '.join(unknown)}")
        if blueprint.mode == BuildMode.FROM_CODEBASE and not blueprint.codebase_path:
            issues.append("codebase_path required for from_codebase mode")
        if blueprint.mode == BuildMode.FROM_DATABASE and not blueprint.connections:
            issues.append("at least one database connection required for from_database mode")
        score = 1.0 if not issues else max(0.0, 1.0 - 0.25 * len(issues))
        return EngineVerdict(accepted=not issues, score=score, issues=issues)

    _SERVICE_MARKERS: dict[str, tuple[str, ...]] = {
        "notification": ("notification",),
        "document": ("docs/", "readme"),
        "hosting": ("dockerfile", "deploy/", "hosting"),
        "ci_cd": (".github/workflows", "ci.yml"),
        "database": ("schema.sql", "src/db/"),
        "monitoring": ("monitoring", "health"),
        "ai_builder": ("builder_hooks",),
    }

    def _present_services(self, services: list[str], files: list[str]) -> list[str]:
        low = [f.lower() for f in files]
        out: list[str] = []
        for svc in services:
            markers = self._SERVICE_MARKERS.get(svc, (svc,))
            if any(any(m in f for m in markers) for f in low):
                out.append(svc)
        return out

    def resolve_kind(self, blueprint: AppBlueprint) -> str:
        """Resolve the app archetype for *blueprint* (explicit or detected)."""
        if blueprint.kind in self._TEMPLATES_BY_KIND:
            return blueprint.kind
        from ai_assistant.app_builder.requirements import detect_archetype

        return detect_archetype(blueprint.description)

    def expected_manifest(
        self, blueprint: AppBlueprint, *, include_infra: bool = True
    ) -> list[str]:
        """Files the build MUST produce for this blueprint.

        When *include_infra* is False the infrastructure add-ons (docs, hosting,
        CI, monitoring/notification modules) are excluded, leaving only the core
        runnable application surface the per-commit gate enforces every round.
        """
        if blueprint.mode == BuildMode.FROM_CODEBASE:
            if getattr(blueprint, "variant", "application") == "application":
                return list(SCRATCH_CONTRACT)
            files = sorted(set(self.CODEBASE_FILES))
            return files if include_infra else [
                f for f in files if not self._is_infra_file(f)]
        if blueprint.mode == BuildMode.FROM_SCRATCH:
            return list(SCRATCH_CONTRACT)
        files = list(self.CORE_FILES)
        files += self._TEMPLATES_BY_KIND.get(self.resolve_kind(blueprint), [])
        svc = blueprint.services
        if any(s in svc for s in ("monitoring", "notification", "document")):
            files.append("src/infra/__init__.py")
        if "monitoring" in svc:
            files.append("src/infra/monitoring.py")
        if "notification" in svc:
            files.append("src/infra/notification.py")
        if "document" in svc:
            files.append("src/infra/document.py")
        if "ai_builder" in svc:
            files += ["src/ai/__init__.py", "src/ai/builder_hooks.py"]
        files = sorted(set(files))
        if include_infra:
            return files
        return [f for f in files if not self._is_infra_file(f)]

    def advisory_layout(self, blueprint: AppBlueprint) -> dict[str, Any]:
        """Suggested folders/files for our metadata lens — never enforced."""
        test_folders = [f"tests/{d}/" for d in TEST_TAXONOMY]
        return {
            "suggested_test_folders": test_folders,
            "suggested_doc_files": list(SKELETON_DOC_FILES),
            "suggested_skeleton_files": list(SKELETON_TEST_FILES),
            "note": (
                "These are optional suggestions to help the App Builder "
                "understand and test the app — not required structure."
            ),
        }

    def infra_suggestions(
        self, blueprint: AppBlueprint, produced_files: list[str]
    ) -> list[str]:
        """Optional infra add-ons not yet present (reported, never blocking)."""
        full = set(self.expected_manifest(blueprint, include_infra=True))
        core = set(self.expected_manifest(blueprint, include_infra=False))
        produced = set(produced_files)
        out = [f"add-on infra file not yet present: {f}"
               for f in sorted(full - core) if f not in produced]
        present = set(self._present_services(blueprint.services, produced_files))
        for svc in blueprint.services:
            if svc in self.INFRA_SERVICES and svc not in present:
                out.append(f"add-on infra service not wired: {svc}")
        return out

    def evaluate_build(
        self,
        blueprint: AppBlueprint,
        produced_files: list[str],
        *,
        sample_code: str = "",
        include_infra: bool = True,
    ) -> EngineVerdict:
        bp = self.validate_blueprint(blueprint)
        if not bp.accepted:
            return bp
        if blueprint.mode == BuildMode.FROM_SCRATCH:
            expected = list(SCRATCH_CONTRACT)
            missing = [f for f in expected if f not in produced_files]
            issues = list(bp.issues)
            score = 1.0 if not missing else max(
                0.0, 1.0 - 0.25 * len(missing))
            if sample_code:
                code_m = self.meters.evaluate_code_artifact(sample_code)
                if not code_m["accepted"]:
                    issues.append("sample code failed code quality gate")
                score = min(score, code_m.get("score", score))
            return EngineVerdict(
                accepted=not missing and not issues,
                score=score,
                issues=issues,
                measurements={"contract": {"missing": missing}},
            )
        expected = self.expected_manifest(blueprint, include_infra=include_infra)
        services = list(blueprint.services)
        if blueprint.mode == BuildMode.FROM_SCRATCH:
            # Free-form builds: only the runnable contract is scored; services
            # are optional platform add-ons suggested later, never blocking.
            services = []
        elif not include_infra:
            services = [s for s in services if s not in self.INFRA_SERVICES]
        build_m = self.meters.evaluate_build(
            expected_files=expected,
            produced_files=produced_files,
            required_services=services,
            present_services=self._present_services(services, produced_files),
        )
        issues = list(bp.issues)
        if sample_code:
            code_m = self.meters.evaluate_code_artifact(sample_code)
            if not code_m["accepted"]:
                issues.append("sample code failed code quality gate")
            build_m["code"] = code_m
            if blueprint.mode == BuildMode.FROM_DATABASE and not _is_fastapi_asgi(sample_code):
                issues.append(
                    "src.app:app must be a FastAPI ASGI app; Flask/WSGI apps "
                    "cannot be launched by the platform"
                )
        accepted = build_m["accepted"] and not issues
        return EngineVerdict(
            accepted=accepted,
            score=build_m["score"],
            issues=issues,
            measurements=build_m,
        )

    def agent_metadata_packet(self, blueprint: AppBlueprint) -> dict[str, Any]:
        """Structured metadata passed to AI agents (framework always included)."""
        if blueprint.mode == BuildMode.FROM_SCRATCH:
            return {
                "engine": "AiAppEngine",
                "mode": blueprint.mode.value,
                "app_name": blueprint.name,
                "services": blueprint.services,
                "required_files": list(SCRATCH_CONTRACT),
                "advisory_layout": self.advisory_layout(blueprint),
                "rules": [
                    "keep an importable ASGI app at src.app:app",
                    "expose GET /health (liveness + database readiness)",
                    "no bare except clauses",
                    "database access must use parameterized queries",
                ],
                "service_templates": list(SERVICE_TEMPLATES),
            }
        return {
            "engine": "AiAppEngine",
            "mode": blueprint.mode.value,
            "app_name": blueprint.name,
            "services": blueprint.services,
            "required_files": self.expected_manifest(blueprint),
            "rules": [
                "use FastAPI + Jinja2 only; do not use Flask/Django/WSGI",
                "keep an importable ASGI app at src.app:app",
                "expose GET /health (liveness + database readiness)",
                "tests/ directory is mandatory",
                "no bare except clauses",
                "database access must use parameterized queries",
                "COUNT(*) helpers must use row[0] on sqlite3.Row (from "
                "src.db.connection.scalar_count); never call .values() on Row",
                "CI/CD workflow must run tests on every push",
                "document every service in docs/ARCHITECTURE.md",
            ],
            "service_templates": list(SERVICE_TEMPLATES),
        }


def _is_fastapi_asgi(code: str) -> bool:
    low = (code or "").lower()
    if "from flask" in low or "import flask" in low or "flask(" in low:
        return False
    return "fastapi" in low and ("app = fastapi(" in low or "app=fastapi(" in low)
