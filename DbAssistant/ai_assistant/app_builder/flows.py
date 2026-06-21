"""Build flows: scratch / database / codebase.

Each flow: frame a request → ask an agent (deterministic or AI) → guarantee the
engine-required scaffolding exists → write the workspace → let the AiAppEngine
validate the build with the meters subsystem. Isolation is per-app (each build
gets its own workspace directory) and per-connection for database builds.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from ai_assistant.app_builder.agent import (
    AgentRequest,
    BuildAgent,
    DeterministicAgent,
)
from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode
from ai_assistant.app_builder.codebase_profile import CodebaseProfiler
from ai_assistant.app_builder.requirements import detect_archetype
from ai_assistant.meters.codemetrics import analyze_python


class BuildFlows:
    def __init__(self, engine: Optional[AiAppEngine] = None,
                 agent: Optional[BuildAgent] = None) -> None:
        self.engine = engine or AiAppEngine()
        self.agent = agent or DeterministicAgent()

    # ── public flows ─────────────────────────────────────────────────────────
    def build_from_scratch(
        self, blueprint: AppBlueprint, workspace: Path, *, agent: Optional[BuildAgent] = None
    ) -> dict[str, Any]:
        if not blueprint.kind:
            blueprint.kind = detect_archetype(blueprint.description)
        req = self._request(blueprint)
        return self._run(blueprint, req, workspace, agent)

    def build_from_database(
        self,
        blueprint: AppBlueprint,
        workspace: Path,
        schema: dict[str, list[str]],
        *,
        agent: Optional[BuildAgent] = None,
    ) -> dict[str, Any]:
        blueprint.mode = BuildMode.FROM_DATABASE
        if "database" not in blueprint.services:
            blueprint.services = list(blueprint.services) + ["database"]
        # A directly-provided schema is a valid schema source even without a
        # live connection (e.g. pre-introspected or offline builds).
        if schema and not blueprint.connections:
            blueprint.connections = ["(provided-schema)"]
        # The insights/admin variant always builds the DB-insights dashboard,
        # not a CRUD mirror or detected archetype.
        if getattr(blueprint, "db_app_variant", "") == "insights_admin":
            blueprint.kind = "insights"
        elif not blueprint.kind:
            blueprint.kind = detect_archetype(blueprint.description, schema)
        req = self._request(blueprint)
        req.schema = schema or {}
        return self._run(blueprint, req, workspace, agent)

    def build_from_codebase(
        self,
        blueprint: AppBlueprint,
        workspace: Path,
        *,
        agent: Optional[BuildAgent] = None,
    ) -> dict[str, Any]:
        blueprint.mode = BuildMode.FROM_CODEBASE
        # Recover the architecture and turn it into a reconstruction brief so the
        # build aims at the *predicted real application* this codebase implements
        # (pages/flows/APIs/data layer) — not a generic scaffold. The brief is
        # folded into the blueprint description so both the deterministic and AI
        # agents see it.
        from ai_assistant.app_builder.codebase_app_assistant import (
            CodebaseAppBuilderAssistant,
        )

        variant = (
            "structure_metadata"
            if getattr(blueprint, "variant", "") == "explorer"
            else "predicted_app"
        )
        assistant = CodebaseAppBuilderAssistant(
            codebase_path=blueprint.codebase_path,
            user_description=blueprint.description or "",
            variant=variant,
        )
        insight = assistant.understand()
        assistant.prepare_blueprint(blueprint, insight)
        profile = insight.profile
        facts = profile.as_dict()
        req = self._request(blueprint)
        req.codebase_facts = facts
        out = self._run(blueprint, req, workspace, agent, analysis=facts)
        # Run the codebase-specific meter battery (architecture recovery,
        # component coverage, hygiene) so the deterministic surface reports the
        # same reconstruction-fidelity signal as the orchestrator/auto-build.
        try:
            from ai_assistant.app_builder.meters.registry import AppMeterRegistry

            files_map = self._read_workspace_files(workspace, out.get("files") or [])
            meters = AppMeterRegistry().evaluate_codebase_build(
                files_map, profile=facts, components=insight.components)
            out["meters"] = meters
            out["components"] = list(insight.components)
            out["insight"] = insight.as_dict()
        except Exception:
            pass
        return out

    @staticmethod
    def _read_workspace_files(workspace: Path, rel_paths: list[str]) -> dict[str, str]:
        """Read produced files back for meter evaluation (best-effort)."""
        files_map: dict[str, str] = {}
        ws = Path(workspace)
        for rel in rel_paths:
            try:
                files_map[rel] = (ws / rel).read_text(encoding="utf-8", errors="replace")
            except Exception:
                files_map[rel] = ""
        return files_map

    # ── internals ──────────────────────────────────────────────────────────--
    def _request(self, blueprint: AppBlueprint) -> AgentRequest:
        packet = self.engine.agent_metadata_packet(blueprint)
        return AgentRequest(
            mode=blueprint.mode,
            app_name=blueprint.name,
            description=blueprint.description,
            language=blueprint.language,
            services=list(blueprint.services),
            required_files=packet["required_files"],
            rules=packet["rules"],
            entities=list(blueprint.entities),
            features=list(blueprint.features),
            kind=blueprint.kind,
        )

    def _run(
        self,
        blueprint: AppBlueprint,
        req: AgentRequest,
        workspace: Path,
        agent: Optional[BuildAgent],
        analysis: Optional[dict] = None,
    ) -> dict[str, Any]:
        bp_verdict = self.engine.validate_blueprint(blueprint)
        if not bp_verdict.accepted:
            return {"ok": False, "workspace": str(workspace),
                    "files": [], "verdict": bp_verdict.as_dict()}

        use_agent = agent or self.agent
        resp = use_agent.generate(req)
        files = {f.path: f.content for f in resp.files}

        # Guarantee engine-required files exist (fill gaps deterministically).
        files = self._ensure_required(blueprint, req, files)

        workspace = Path(workspace)
        for rel, content in files.items():
            path = workspace / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

        produced = sorted(files.keys())
        sample = files.get("src/app.py", "") or next(
            (c for p, c in files.items() if p.endswith(".py")), ""
        )
        verdict = self.engine.evaluate_build(blueprint, produced, sample_code=sample)
        out: dict[str, Any] = {
            "ok": verdict.accepted,
            "workspace": str(workspace),
            "mode": blueprint.mode.value,
            "agent": resp.backend,
            "notes": resp.notes,
            "files": produced,
            "verdict": verdict.as_dict(),
        }
        if analysis is not None:
            out["analysis"] = analysis
        return out

    def _ensure_required(
        self, blueprint: AppBlueprint, req: AgentRequest, files: dict[str, str]
    ) -> dict[str, str]:
        missing = [f for f in self.engine.expected_manifest(blueprint) if f not in files]
        if not missing:
            return files
        # Deterministic agent supplies any gaps the (AI) agent left out.
        baseline = DeterministicAgent().generate(req)
        baseline_map = {f.path: f.content for f in baseline.files}
        for rel in missing:
            if rel in baseline_map:
                files[rel] = baseline_map[rel]
            else:
                files[rel] = _placeholder_for(rel, blueprint.name)
        return files


def _placeholder_for(rel: str, app_name: str) -> str:
    if rel.endswith(".py"):
        return f'"""{rel} for {app_name} (placeholder)."""\n'
    if rel.endswith(".md"):
        return f"# {app_name} — {rel}\n"
    if rel.endswith(".sql"):
        return "-- placeholder\n"
    return f"# {rel}\n"


# ── codebase analysis (real static analysis, no execution) ───────────────────-
def analyze_codebase(path: str, *, max_files: int = 400) -> dict[str, Any]:
    """Walk a Python codebase and aggregate deterministic quality facts."""
    root = Path(path) if path else None
    facts: dict[str, Any] = {
        "path": str(path or ""),
        "files": 0, "loc": 0,
        "avg_complexity": 0.0, "max_complexity": 0,
        "docstring_coverage": 0.0,
        "issues": [], "recommendations": [],
    }
    if not root or not root.exists():
        facts["issues"].append("codebase path does not exist")
        return facts

    ignored_dirs = {".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages"}
    py_files = [
        p for p in sorted(root.rglob("*.py"))
        if not any(part in ignored_dirs for part in p.relative_to(root).parts)
    ][:max_files]
    if not py_files:
        facts["issues"].append("no Python files found")
        facts["recommendations"].append("Add source files before building.")
        return facts

    total_loc = 0
    complexities: list[float] = []
    max_complexity = 0
    doc_cov: list[float] = []
    issue_set: set[str] = set()
    has_tests = False
    has_ci = (root / ".github" / "workflows").is_dir()

    for fp in py_files:
        if "test" in fp.name.lower() or "tests" in fp.parts:
            has_tests = True
        try:
            src = fp.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        cf = analyze_python(src)
        total_loc += cf.loc
        if cf.avg_complexity:
            complexities.append(cf.avg_complexity)
        max_complexity = max(max_complexity, cf.max_complexity)
        doc_cov.append(cf.docstring_coverage)
        for ap in cf.antipatterns:
            issue_set.add(f"{fp.name}: {ap}")
        if not cf.parse_ok and cf.syntax_error:
            issue_set.add(f"{fp.name}: syntax error — {cf.syntax_error}")

    facts["files"] = len(py_files)
    facts["loc"] = total_loc
    facts["avg_complexity"] = round(sum(complexities) / len(complexities), 2) if complexities else 0.0
    facts["max_complexity"] = max_complexity
    facts["docstring_coverage"] = round(sum(doc_cov) / len(doc_cov), 2) if doc_cov else 0.0
    facts["issues"] = sorted(issue_set)[:50]

    recs: list[str] = []
    if not has_tests:
        recs.append("No tests detected — add a tests/ suite.")
    if not has_ci:
        recs.append("No CI workflow — add .github/workflows/ci.yml.")
    if facts["max_complexity"] >= 15:
        recs.append("Refactor high-complexity functions (max complexity ≥ 15).")
    if facts["docstring_coverage"] < 0.5:
        recs.append("Improve docstring coverage (currently < 50%).")
    facts["recommendations"] = recs or ["Codebase looks healthy; add features safely."]
    return facts
