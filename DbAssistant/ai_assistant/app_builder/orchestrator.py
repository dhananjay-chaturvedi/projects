"""AppBuildOrchestrator — the autonomous, meter-driven build agent.

This is the "smart agent" that owns the build: it understands the requirements,
prepares a deterministic, always-runnable baseline, then (in auto mode) keeps
talking to the AI Query Assistant — round after round — asking it to enrich the
app while the AiAppEngine + meters/managers validate every change. Anything the
AI returns that fails the quality gate is rejected; the build never regresses
below the safe baseline.

Guarantees enforced here (not by prompts):

* every accepted artifact passes the code meters (managers),
* every round is scored by the engine (build accuracy + design),
* the workspace written out is always the best valid build seen, and
* the app stays lightweight + safe (parameterized SQL, no bare except, tests).
"""

from __future__ import annotations

import hashlib
import os
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.app_builder.agent import (
    AgentRequest,
    DeterministicAgent,
    parse_files,
    spec_for,
)
from ai_assistant.app_builder.engine import (
    TEST_TAXONOMY,
    AiAppEngine,
    AppBlueprint,
    BuildMode,
)
from ai_assistant.app_builder.flows import _placeholder_for
from ai_assistant.app_builder.interaction import BuildDecider, BuildDecision
from ai_assistant.app_builder.schema_deploy import deploy_schema as _deploy_schema

ProgressFn = Callable[[dict], None]


def _merge_quality(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    """Merge mode-specific meter readout into the main quality dict."""
    if not extra:
        return base
    out = dict(base)
    out["mode_specific"] = extra
    if extra.get("overall") is not None:
        out["mode_overall"] = extra.get("overall")
    return out


@dataclass
class BuildRound:
    index: int
    phase: str
    score: float
    accepted: bool
    note: str
    issues: list[str] = field(default_factory=list)
    accepted_files: int = 0
    rejected_files: int = 0
    coverage: float = 1.0
    coverage_gaps: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index, "phase": self.phase, "score": round(self.score, 4),
            "accepted": self.accepted, "note": self.note, "issues": list(self.issues),
            "accepted_files": self.accepted_files, "rejected_files": self.rejected_files,
            "coverage": round(self.coverage, 4), "coverage_gaps": list(self.coverage_gaps),
        }


@dataclass
class OrchestrationResult:
    ok: bool
    workspace: str
    final_score: float
    files: list[str]
    rounds: list[BuildRound]
    mode: str
    used_ai: bool
    requirement_coverage: float = 1.0
    coverage_ok: bool = True
    gaps: list[str] = field(default_factory=list)
    fidelity: float = 1.0
    fidelity_gaps: list[str] = field(default_factory=list)
    data_understanding: float = 1.0
    process_adherence: float = 1.0
    journal: dict[str, Any] = field(default_factory=dict)
    insight: dict[str, Any] = field(default_factory=dict)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    schema_deploy: dict[str, Any] = field(default_factory=dict)
    commits: list[dict[str, Any]] = field(default_factory=list)
    transcript: list[dict[str, Any]] = field(default_factory=list)
    agentic: bool = False
    aborted: bool = False
    stop_reason: str = ""
    agreement: dict[str, Any] = field(default_factory=dict)
    understanding: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    build_path: dict[str, Any] = field(default_factory=dict)
    preflight: dict[str, Any] = field(default_factory=dict)
    boot_check: dict[str, Any] = field(default_factory=dict)
    http_smoke: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "workspace": self.workspace,
            "verdict": {"score": round(self.final_score, 4), "accepted": self.ok},
            "score": round(self.final_score, 4),
            "files": list(self.files),
            "rounds": [r.as_dict() for r in self.rounds],
            "mode": self.mode,
            "used_ai": self.used_ai,
            "requirement_coverage": round(self.requirement_coverage, 4),
            "coverage_ok": self.coverage_ok,
            "gaps": list(self.gaps),
            "fidelity": round(self.fidelity, 4),
            "fidelity_gaps": list(self.fidelity_gaps),
            "data_understanding": round(self.data_understanding, 4),
            "process_adherence": round(self.process_adherence, 4),
            "journal": dict(self.journal),
            "insight": dict(self.insight),
            "decisions": list(self.decisions),
            "schema_deploy": dict(self.schema_deploy),
            "commits": list(self.commits),
            "transcript": list(self.transcript),
            "agentic": self.agentic,
            "aborted": self.aborted,
            "stop_reason": self.stop_reason,
            "agreement": dict(self.agreement),
            "understanding": dict(self.understanding),
            "quality": dict(self.quality),
            "build_path": dict(self.build_path),
            "preflight": dict(self.preflight),
            "boot_check": dict(self.boot_check),
            "http_smoke": dict(self.http_smoke),
            "agent": "orchestrator",
        }


@dataclass(frozen=True)
class OrchestratorConfig:
    max_rounds: int = 4
    target_score: float = 0.9
    target_coverage: float = 0.9
    timeout: int = 180
    max_wall_clock_seconds: float = 1800.0
    max_no_progress_rounds: int = 3
    repeat_output_limit: int = 2
    max_validations: int = 12
    max_finalize_repairs: int = 0
    validation_mode: str = "low_token"
    collaboration: bool = False


@dataclass(frozen=True)
class RunContext:
    schema: Optional[dict[str, list[str]]] = None
    bridge: Any = None
    backend: Any = None
    db_understanding: Any = None
    run_tests: bool = False
    decider: Optional[BuildDecider] = None
    deploy_schema: bool = False
    db_manager: Any = None
    on_progress: Optional[ProgressFn] = None
    force_agentic: bool = False
    cancel_event: Any = None
    mask_pii: bool = False


@dataclass(frozen=True)
class FinalizeContext:
    workspace: Path
    blueprint: AppBlueprint
    req: AgentRequest
    gate: Any
    coord: Any
    last_suggestions: list[str]
    on_progress: Any


@dataclass(frozen=True)
class FinalBuildState:
    current: dict[str, str]
    best: Any
    best_cov: dict[str, Any]


@dataclass(frozen=True)
class AutoLoopContext:
    blueprint: AppBlueprint
    req: AgentRequest
    best_files: dict[str, str]
    best: Any
    best_cov: dict[str, Any]
    rounds: list[BuildRound]
    bridge: Any
    on_progress: Optional[ProgressFn]


@dataclass(frozen=True)
class AgenticLoopContext:
    blueprint: AppBlueprint
    req: AgentRequest
    workspace: Path
    best_files: dict[str, str]
    best: Any
    best_cov: dict[str, Any]
    rounds: list[BuildRound]
    backend: Any
    schema: Optional[dict]
    on_progress: Optional[ProgressFn]


class AppBuildOrchestrator:
    """Drive a build to completion under engine + manager governance."""

    # Refinement focus rotated across AI rounds.
    PHASES = ("development", "design", "testing")

    def __init__(
        self,
        engine: Optional[AiAppEngine] = None,
        config: OrchestratorConfig | None = None,
        **overrides,
    ) -> None:
        config = config or OrchestratorConfig()
        if overrides:
            config = replace(config, **overrides)
        self.engine = engine or AiAppEngine()
        self.meters = self.engine.meters
        # When enabled, the agentic build uses the App Builder Assistant
        # collaboration pipeline: a parallel "understanding" kickoff gated on
        # design similarity, plus continuous meter-governed remediation routed
        # through Session B. Off by default so the legacy agentic flow (and its
        # tests) are unchanged; the service layer turns it on for real builds.
        self.collaboration = bool(config.collaboration)
        self._app_meters = None
        self._meter_managers = None
        self._assistant = None
        self._understanding = None
        self.max_rounds = max(0, int(config.max_rounds))
        self.target_score = float(config.target_score)
        self.target_coverage = float(config.target_coverage)
        self.timeout = int(config.timeout)
        # Hard safety stops so a build can never run forever, independent of the
        # convergence heuristics. All tunable; defaults are generous.
        self.max_wall_clock_seconds = float(config.max_wall_clock_seconds)
        self.max_no_progress_rounds = max(1, int(config.max_no_progress_rounds))
        self.repeat_output_limit = max(1, int(config.repeat_output_limit))
        # Upper bound on token-costing Session C (validator) LLM consults per
        # build. Validation is otherwise continuous (the free deterministic test
        # run happens every changed round); C is only consulted when the evidence
        # digest changes, and this cap is the final safety stop.
        self.max_validations = max(0, int(config.max_validations))
        # Post-build repair attempts when finalize_agreement says not complete.
        # Only used in uninterrupted/auto-build mode; 0 disables (tests default).
        self.max_finalize_repairs = max(0, int(config.max_finalize_repairs))
        vm = (config.validation_mode or "low_token").strip().lower()
        self.validation_mode = vm if vm in ("low_token", "thorough") else "low_token"
        #: Phases walked in thorough validation mode (one per agentic round).
        self.THOROUGH_PHASES = ("api", "db", "web", "tests")
        self._baseline = DeterministicAgent()

    # ── public ─────────────────────────────────────────────────────────────--
    def run(
        self,
        blueprint: AppBlueprint,
        workspace: str | Path,
        *,
        context: RunContext | None = None,
        **overrides,
    ) -> OrchestrationResult:
        context = context or RunContext()
        if overrides:
            context = replace(context, **overrides)
        schema = context.schema
        bridge = context.bridge
        backend = context.backend
        db_understanding = context.db_understanding
        run_tests = context.run_tests
        decider = context.decider
        deploy_schema = context.deploy_schema
        db_manager = context.db_manager
        on_progress = context.on_progress
        force_agentic = context.force_agentic
        cancel_event = context.cancel_event
        mask_pii = context.mask_pii
        workspace = Path(workspace)
        req = self._request(blueprint, schema)
        self._channels: set[str] = set()
        self._insight = None
        self._schema_deployed = False
        self._cancel_event = cancel_event
        self._mask_pii = mask_pii
        self._db_manager = db_manager
        self._coord = None  # live A/B/C coordinator, set on agentic builds
        self._aborted = False
        self._stop_reason = ""
        self._agreement = None
        self._final_quality = None
        self._mode_quality = None
        self._preflight = None  # final deterministic compile+import dry-run result
        self._boot_check = None  # final TestClient lifespan boot result
        self._http_smoke = None  # final HTTP launch smoke (uvicorn + GET /health, /)
        self._round_preflight = None  # latest per-round compile+import dry-run
        self._validator_test_authors = 0  # how many times C authored its tests
        self._validator_test_outcome = None  # latest run of C's own test folder
        self._structure_published = False  # A's scaffold shared with B/C yet?
        # Agentic builds ship NO pre-built page: Start app is enabled only once
        # Session A writes a runnable src/app.py (requirement-specific landing).
        self._baseline_ready_emitted = False
        self._build_path = {
            "mode": blueprint.mode.value,
            "path": "standard",
            "enforced_by": "structural_coverage",
            "message": "standard app-builder contract",
        }
        from ai_assistant.app_builder.builder_types import policy_for
        self._builder_policy = policy_for(blueprint.mode)
        self._build_profile = getattr(blueprint, "build_profile", "prototype")
        # Interaction control: silent by default (uninterrupted) so headless and
        # legacy callers never block; UI/interactive callers inject a decider.
        self._decider = decider or BuildDecider(uninterrupted=True)
        # Requirement targets (entities/features) come straight from the spec the
        # generator would build, so coverage is measured against the real request.
        # From-codebase explorer is an *analysis* artifact; application variant
        # is a real app reconstruction and follows the same app-fidelity path as
        # scratch/database builds.
        self._description = blueprint.description or ""
        self._fidelity_driven = False
        self._skip_launch_gates = False
        if (
            blueprint.mode == BuildMode.FROM_CODEBASE
            and (
                getattr(blueprint, "variant", "application") == "explorer"
                or not getattr(self, "_codebase_profile", None)
            )
        ):
            self._skip_launch_gates = True
            self._spec = None
            self._req_entities = []
            self._req_features = []
            self._req_services = []
            self._kind = "crud"
        else:
            self._spec = spec_for(req)
            self._req_entities = [e.table for e in self._spec.entities]
            self._req_features = list(self._spec.features)
            self._req_services = list(blueprint.services)
            self._kind = self._spec.kind
            # Keep the engine's expected manifest aligned with what the
            # generator actually emits for this archetype.
            blueprint.kind = self._spec.kind
            # A non-storefront app is judged by how well it reflects the INTENT
            # (the user's requirement and/or what the data implies), NOT by
            # CRUD-over-tables. Fold fidelity into the completion signal so
            # auto-build builds the real app — never a table-per-noun (scratch)
            # or schema-mirror (database) scaffold. Storefront keeps its own
            # archetype coverage.
            self._fidelity_driven = (
                self._kind == "crud"
                and blueprint.mode in (BuildMode.FROM_SCRATCH,
                                       BuildMode.FROM_DATABASE,
                                       BuildMode.FROM_CODEBASE)
            )
            if (self._fidelity_driven
                    and blueprint.mode in (BuildMode.FROM_SCRATCH,
                                           BuildMode.FROM_CODEBASE)):
                # In from_scratch, entities mined by Analyze are planning hints,
                # not a contract to create CRUD tables. The user's prompt is the
                # contract, and requirement fidelity drives completion.
                self._req_entities = []
                self._req_features = []
                # Infra services are optional add-ons — never block completion.
                self._req_services = []

        bp_verdict = self.engine.validate_blueprint(blueprint)
        if not bp_verdict.accepted:
            rnd = BuildRound(0, "blueprint", bp_verdict.score, False,
                             "blueprint rejected", issues=bp_verdict.issues)
            self._emit(on_progress, rnd)
            return OrchestrationResult(
                ok=False, workspace=str(workspace), final_score=bp_verdict.score,
                files=[], rounds=[rnd], mode=blueprint.mode.value, used_ai=False,
                requirement_coverage=0.0, coverage_ok=False,
            )

        # Decision: confirm the plan before building. In uninterrupted/auto this
        # is auto-approved; interactive surfaces it so the user can approve the
        # archetype + entities + features the agent intends to build.
        if self._spec is not None:
            plan = BuildDecision(
                id="confirm_plan",
                question=(
                    f"Build a {self._kind} app '{blueprint.name}' "
                    f"with entities [{', '.join(self._req_entities) or 'none'}] "
                    f"and features [{', '.join(self._req_features) or 'none'}]?"
                ),
                detail=blueprint.description,
                default=True,
            )
            if not self._decider.approved(plan):
                rnd = BuildRound(0, "plan", 0.0, False, "user cancelled the plan")
                self._emit(on_progress, rnd)
                return OrchestrationResult(
                    ok=False, workspace=str(workspace), final_score=0.0,
                    files=[], rounds=[rnd], mode=blueprint.mode.value,
                    used_ai=False, requirement_coverage=0.0, coverage_ok=False,
                    decisions=list(self._decider.log),
                )

        # DB understanding (from_database only): consult the AI Query Assistant
        # to learn the meaning/nature of the data + read real sample rows BEFORE
        # building, so the app is grounded in the actual data — not a guess.
        if (blueprint.mode == BuildMode.FROM_DATABASE
                and db_understanding is not None):
            try:
                if db_understanding.available():
                    self._emit_session_status(
                        on_progress, "answerer",
                        "understanding the database (metadata, sampling, "
                        "predicting the app)…")
                    self._insight = db_understanding.understand(schema or {})
                    self._channels.add("query_assistant")
                    if self._insight is not None:
                        app_label = (
                            getattr(self._insight, "app_name", "") or "").strip()
                        if not app_label:
                            app_label = (
                                (getattr(self._insight, "app_summary", "") or "")
                                .split(".")[0][:80])
                        self._emit_session_status(
                            on_progress, "answerer",
                            "DB understanding complete"
                            + (f" — predicted {app_label}" if app_label else "")
                            + "; preparing the build brief for Sessions A and C.")
            except Exception:  # noqa: BLE001
                self._insight = None
            # Decision: let the user approve the data understanding before it is
            # used to ground the build (interactive only; auto-approved otherwise).
            if self._insight is not None:
                ok = self._decider.approved(BuildDecision(
                    id="confirm_understanding",
                    question="Use this understanding of the database to build the app?",
                    detail=self._insight.app_summary or "",
                    default=True,
                ))
                if not ok:
                    self._insight = None  # build without grounding if rejected
            # The intent of a from_database app comes from the DATA (what kind of
            # app would use it), with the user's description as an optional hint.
            # Fold the understood app summary/flow into the fidelity target so the
            # build is judged against the implied application, not the schema.
            if self._insight is not None:
                user_description = self._description.strip()
                generation_description = "\n\n".join(s for s in (
                    user_description,
                    getattr(self._insight, "design_brief", "") or "",
                    self._insight.app_summary,
                    self._insight.data_flow,
                ) if s).strip()
                # For from_database, fidelity must be judged against the clean
                # data-inferred app intent. The raw description is still useful
                # as a generation hint, but it can contain AI Query Assistant
                # session notes (connection/schema/sql-mode text) that would
                # become phantom "requirements" in the deterministic meter.
                feature_lines = ""
                if getattr(self._insight, "app_features", None):
                    feature_lines = "User-facing features: " + "; ".join(
                        self._insight.app_features)
                fidelity_description = "\n\n".join(s for s in (
                    self._insight.app_summary,
                    feature_lines,
                    self._insight.data_flow,
                ) if s).strip()
                self._description = fidelity_description or user_description
                if getattr(self._insight, "variant", "") == "insights_admin":
                    # Insights/admin always builds the DB-insights dashboard
                    # deterministically; do not override with a detected archetype.
                    blueprint.kind = "insights"
                    req.kind = "insights"
                elif getattr(self._insight, "archetype", ""):
                    blueprint.kind = self._insight.archetype
                    req.kind = self._insight.archetype
                req.description = generation_description or self._description
                # When we CONFIDENTLY predicted the real application this data
                # supports (and the user did not ask for the insights/admin
                # variant), the raw tables are the DATA LAYER — not a checklist
                # of user-facing CRUD screens. Drop the per-table CRUD coverage
                # contract so the gate judges the build by how well it reflects
                # the predicted app (requirement fidelity), exactly as a
                # from_scratch build is judged. Without this, the gate forces a
                # 35-table CRUD mirror even though the brief says "build the real
                # app, not a schema browser". The deterministic baseline (built
                # from self._spec) stays runnable; only the *gate contract*
                # changes. The fallback/insights_admin path keeps table coverage
                # so a schema/admin reflection is still fully checked.
                build_real_app = bool(
                    getattr(self._insight, "confident", False)
                    and getattr(self._insight, "variant", "") != "insights_admin")
                if build_real_app:
                    self._req_entities = []
                    self._req_features = []
                    self._build_path = {
                        "mode": blueprint.mode.value,
                        "path": "real_app",
                        "app_name": getattr(self._insight, "app_name", "") or "",
                        "archetype": getattr(self._insight, "archetype", "") or "",
                        "confidence": "high",
                        "enforced_by": "requirement_fidelity",
                        "raw_tables_are": "data_layer",
                        "crud_contract": "disabled_for_raw_tables",
                        "message": (
                            "building REAL app workflows from the DB prediction; "
                            "raw tables are data-layer backing, not per-table CRUD screens"
                        ),
                    }
                else:
                    reason = "insights_admin variant"
                    if getattr(self._insight, "variant", "") != "insights_admin":
                        reason = "low prediction confidence"
                    self._build_path = {
                        "mode": blueprint.mode.value,
                        "path": "schema_admin",
                        "app_name": getattr(self._insight, "app_name", "") or "",
                        "archetype": getattr(self._insight, "archetype", "") or "",
                        "confidence": (
                            "high" if getattr(self._insight, "confident", False)
                            else "low"
                        ),
                        "reason": reason,
                        "enforced_by": "per_table_schema_coverage",
                        "raw_tables_are": "user_visible_admin_resources",
                        "crud_contract": "enabled_for_raw_tables",
                        "message": (
                            "building schema/admin reflection; raw-table CRUD "
                            "coverage remains enforced"
                        ),
                    }
                self._emit_agent(on_progress, {
                    "session": "system",
                    "event": {
                        "type": "build_path",
                        "text": self._build_path["message"],
                        "detail": dict(self._build_path),
                    },
                })

        used_ai = bool(bridge is not None and bridge.available())
        # Fidelity-driven coverage is useful only when there is an AI/code-agent
        # loop that can act on fidelity gaps. Deterministic-only callers should
        # keep seeing structural coverage for the runnable baseline (legacy API
        # behavior and the baseline coverage contract).
        self._coverage_fidelity_active = bool(
            self._fidelity_driven and used_ai)

        # Decide up front whether a writable code-agent (Session A) will run.
        # When it will, we do NOT pre-build a generic page: Session A authors the
        # skeleton AND a requirement-specific launchable page itself.
        from ai_assistant.app_builder.agent_runner import supports_agentic_write
        will_use_agentic = bool(used_ai and (
            force_agentic
            or (backend is not None and supports_agentic_write(backend))))

        # Round 0 — baseline. The deterministic/auto path lays down a fully
        # runnable app; the agentic path seeds ONLY a skeleton (no pre-built
        # page), so the launch page Session A creates is purely requirement-driven.
        if will_use_agentic:
            best_files = self._agentic_baseline(req)
            # Score against the required manifest (in-memory only) so coverage is
            # measured, but never write a generic page to disk.
            eval_files = self._ensure_required(blueprint, req, dict(best_files))
            baseline_label = "skeleton seeded — Session A will author the app"
        else:
            best_files = {f.path: f.content
                          for f in self._baseline.generate(req).files}
            best_files = self._ensure_required(blueprint, req, best_files)
            eval_files = best_files
            baseline_label = "deterministic baseline (runnable app)"
        best = self._evaluate(blueprint, eval_files)
        best_cov = self._coverage(eval_files)
        rounds = [BuildRound(
            0, "baseline", best.score, best.accepted,
            baseline_label,
            issues=best.issues, accepted_files=len(best_files),
            coverage=best_cov["score"], coverage_gaps=best_cov["gaps"])]
        self._emit(on_progress, rounds[-1])
        # Write the seed early so Open folder works. For the auto path this is a
        # runnable stub → signal baseline_ready now. For the agentic path there is
        # no page yet → baseline_ready is emitted once Session A writes src/app.py.
        self._write(workspace, best_files, overwrite=False)
        if not will_use_agentic:
            self._emit_baseline_ready(on_progress, workspace)

        agentic = False
        commits: list[dict[str, Any]] = []
        transcript: list[dict[str, Any]] = []
        if used_ai:
            self._channels.add("code_agent")
            if will_use_agentic:
                agentic = True
                self._channels.add("agentic_runner")
                best_files, best, best_cov, commits, transcript = (
                    self._agentic_loop(
                        AgenticLoopContext(
                            blueprint=blueprint,
                            req=req,
                            workspace=workspace,
                            best_files=best_files,
                            best=best,
                            best_cov=best_cov,
                            rounds=rounds,
                            backend=backend,
                            schema=schema,
                            on_progress=on_progress,
                        ),
                    ))
            else:
                self._auto_loop(
                    AutoLoopContext(
                        blueprint=blueprint,
                        req=req,
                        best_files=best_files,
                        best=best,
                        best_cov=best_cov,
                        rounds=rounds,
                        bridge=bridge,
                        on_progress=on_progress,
                    )
                )
                best_files, best, best_cov = self._best_so_far

        # Persist guarantee: adopt whatever Session A actually left on disk so no
        # completed work is dropped from the shipped/served app. The per-round
        # gate only tracks files it saw change; this reconciles the final set with
        # the live workspace (the agent may have written outside a gate diff, e.g.
        # on the prime turn). Re-score from the reconciled set so the verdict and
        # served app reflect what is truly on disk.
        if agentic:
            reconciled = self._reconcile_with_workspace(workspace, best_files)
            if reconciled != best_files:
                best_files = reconciled
                eval_files = self._ensure_required(blueprint, req, dict(best_files))
                best = self._evaluate(blueprint, eval_files)
                best_cov = self._coverage(eval_files)

        self._write(workspace, best_files)
        self._reconcile_data_layer_workspace(workspace, blueprint, req, on_progress)
        best_files = self._reconcile_with_workspace(workspace, best_files)

        # Authoritative compile + import dry-run (and launch smoke) on EXACTLY the
        # files that will be launched. Guarantees the shipped app boots (Start
        # app) instead of crashing; also covers the deterministic/auto path which
        # has no Session C.
        self._run_launch_gates(workspace, on_progress, emit_pass=False)
        self._maybe_stub_launch_fallback(workspace, blueprint, on_progress)

        # Schema deployment (from_scratch + connection only): strictly opt-in and
        # OFF by default. Even when requested, interactive mode confirms it first.
        deploy_report = self._maybe_deploy_schema(
            blueprint, best_files, deploy_schema, db_manager)

        # Sample data + tests: prove each functionality works in parallel.
        test_outcome = self._run_tests(workspace) if run_tests else None

        fidelity = self.meters.evaluate_fidelity(
            description=blueprint.description, files=best_files,
            entities=self._req_entities,
        )
        du = self._data_understanding_verdict()
        mode_quality = self._mode_specific_quality(blueprint, best_files, schema)
        self._mode_quality = mode_quality
        journal = self._journal(blueprint, best_files, test_outcome)
        process = self.meters.evaluate_process(journal)

        aborted = bool(getattr(self, "_aborted", False))
        agreement = dict(getattr(self, "_agreement", None) or {})
        meters_ok = self._done(best, best_cov)
        ok_build = agreement.get("complete", meters_ok) if agreement else meters_ok
        # A build that does not compile/import or does not serve HTTP can never be
        # "ok": it would crash on launch. The deterministic gates are the final
        # say on runnability.
        preflight_ok, smoke_ok = self._runnable_ok()
        if not (preflight_ok and smoke_ok):
            ok_build = False
        return OrchestrationResult(
            ok=(ok_build and not aborted),
            workspace=str(workspace),
            final_score=best.score, files=sorted(best_files), rounds=rounds,
            mode=blueprint.mode.value, used_ai=used_ai,
            requirement_coverage=best_cov["score"],
            coverage_ok=best_cov["score"] >= self.target_coverage,
            gaps=best_cov["gaps"],
            fidelity=fidelity["score"], fidelity_gaps=fidelity["missing"],
            data_understanding=du["score"],
            process_adherence=process["score"],
            journal=journal,
            insight=self._insight.as_dict() if self._insight is not None else {},
            decisions=list(self._decider.log),
            schema_deploy=deploy_report,
            commits=commits,
            transcript=transcript,
            agentic=agentic,
            aborted=aborted,
            stop_reason=getattr(self, "_stop_reason", ""),
            agreement=agreement,
            understanding=(self._understanding.as_dict()
                           if getattr(self, "_understanding", None) else {}),
            quality=_merge_quality(
                dict(getattr(self, "_final_quality", None) or {})
                if getattr(self, "_final_quality", None)
                else (dict(self._assistant.last_quality)
                      if getattr(self, "_assistant", None) is not None
                      and self._assistant.last_quality else {}),
                dict(getattr(self, "_mode_quality", None) or {}),
            ),
            build_path=dict(getattr(self, "_build_path", {}) or {}),
            preflight=(self._preflight.as_dict()
                       if getattr(self, "_preflight", None) is not None else {}),
            boot_check=(self._boot_check.as_dict()
                        if getattr(self, "_boot_check", None) is not None else {}),
            http_smoke=(self._http_smoke.as_dict()
                        if getattr(self, "_http_smoke", None) is not None else {}),
        )

    # ── deterministic runnability gates (shared by run() and agentic-final) ────
    def _run_launch_gates(self, workspace, on_progress, *,
                          emit_pass: bool) -> None:
        """Run the deterministic runnability gates on the shipped app.

        Compiles + import-dry-runs ``src.app:app`` (and each src module) and,
        when that passes, HTTP-smoke-tests the launch. Results are recorded on
        ``self._preflight`` / ``self._http_smoke``. Only runnable apps have a
        launchable ASGI entrypoint — a from-codebase analysis artifact has no
        ``src/app.py``, so the gate does not apply. With ``emit_pass`` the
        passing results are also surfaced as validator events (agentic-final);
        otherwise only a failing code gate is surfaced (the auto/deterministic
        path).
        """
        from ai_assistant.app_builder import preflight as _preflight_mod

        if getattr(self, "_skip_launch_gates", False):
            return
        if not (workspace / "src" / "app.py").exists():
            return
        self._preflight = _preflight_mod.dry_run(workspace, timeout=self.timeout)
        if emit_pass or not self._preflight.ok:
            self._emit_agent(on_progress, {
                "session": "validator",
                "event": {
                    "type": "validation",
                    "text": ("compile + import dry-run "
                             + ("PASSED" if self._preflight.ok
                                else "FAILED:\n" + self._preflight.digest())),
                    "detail": {"clean": self._preflight.ok,
                               **self._preflight.as_dict()},
                },
            })
        self._boot_check = _preflight_mod.boot_check(
            workspace, timeout=min(self.timeout, 60))
        if emit_pass or not self._boot_check.ok:
            self._emit_agent(on_progress, {
                "session": "validator",
                "event": {
                    "type": "validation",
                    "text": self._boot_check.digest(),
                    "detail": {"clean": self._boot_check.ok,
                               **self._boot_check.as_dict()},
                },
            })
        if self._preflight.ok and getattr(self, "_http_smoke", None) is None:
            self._http_smoke = _preflight_mod.http_smoke(
                workspace, timeout=min(self.timeout, 45))
            if emit_pass:
                smoke_ok = self._http_smoke.skipped or self._http_smoke.ok
                self._emit_agent(on_progress, {
                    "session": "validator",
                    "event": {
                        "type": "validation",
                        "text": self._http_smoke.digest(),
                        "detail": {"clean": smoke_ok,
                                   **self._http_smoke.as_dict()},
                    },
                })

    def _runnable_ok(self) -> tuple[bool, bool]:
        """Return ``(preflight_ok, smoke_ok)`` from the recorded gates.

        A gate that never ran or was skipped is treated as ok (not a failure).
        Boot failure makes the final verdict incomplete, but never disables UI
        launch/open controls.
        """
        pf = getattr(self, "_preflight", None)
        boot = getattr(self, "_boot_check", None)
        sm = getattr(self, "_http_smoke", None)
        preflight_ok = (pf is None or pf.ok) and (boot is None or boot.ok)
        smoke_ok = sm is None or sm.skipped or sm.ok
        return preflight_ok, smoke_ok

    # ── auto-mode loop ───────────────────────────────────────────────────────
    def _auto_loop(self, context: AutoLoopContext):
        blueprint = context.blueprint
        req = context.req
        best_files = context.best_files
        best = context.best
        best_cov = context.best_cov
        rounds = context.rounds
        bridge = context.bridge
        on_progress = context.on_progress
        current = dict(best_files)
        self._best_so_far = (dict(best_files), best, best_cov)
        stale = 0
        for i in range(1, self.max_rounds + 1):
            # Focus on whatever the requirements still lack; if coverage is
            # complete, rotate the structural phases instead.
            phase = "coverage" if best_cov["gaps"] else self.PHASES[
                (i - 1) % len(self.PHASES)]
            prompt = self._frame_iteration(blueprint, req, current, best,
                                           best_cov, phase)
            self._emit(on_progress, BuildRound(
                i, phase, best.score, best.accepted,
                "contacting AI backend to close requirement gaps...",
                coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
            try:
                text = bridge.generate(prompt)
            except Exception as exc:  # noqa: BLE001
                rounds.append(BuildRound(i, phase, best.score, best.accepted,
                                         f"AI bridge error: {exc}",
                                         coverage=best_cov["score"],
                                         coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break

            produced = parse_files(text)
            accepted_files, rejected = self._gate_files(produced)
            if not accepted_files:
                rounds.append(BuildRound(
                    i, phase, best.score, best.accepted,
                    "converged — no new safe files from AI",
                    rejected_files=len(rejected), coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break

            candidate = dict(current)
            candidate.update(accepted_files)
            candidate = self._ensure_required(blueprint, req, candidate)
            verdict = self._evaluate(blueprint, candidate)
            cov = self._coverage(candidate)

            improved = self._better(verdict, cov, best, best_cov, len(candidate),
                                    len(best_files))

            # Decision (interactive only): let the user approve/skip/stop this
            # round's AI suggestions. Auto/uninterrupted always applies.
            action = "apply"
            if self._decider.interactive:
                action = self._decider.decide(BuildDecision(
                    id=f"apply_round_{i}",
                    question=(
                        f"Round {i} ({phase}) produced "
                        f"{len(accepted_files)} file(s) — "
                        f"score {round(verdict.score, 3)}, "
                        f"coverage {round(cov['score'], 3)}. Apply these changes?"
                    ),
                    kind="choice", options=["apply", "skip", "stop"],
                    default="apply",
                ))
            if action == "stop":
                rounds.append(BuildRound(
                    i, phase, best.score, best.accepted,
                    "stopped by user", accepted_files=0,
                    rejected_files=len(rejected), coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            if action == "skip":
                rounds.append(BuildRound(
                    i, phase, verdict.score, verdict.accepted,
                    f"AI {phase}: {len(accepted_files)} file(s) skipped by user",
                    accepted_files=0, rejected_files=len(rejected),
                    coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                continue

            rounds.append(BuildRound(
                i, phase, verdict.score, verdict.accepted,
                f"AI {phase}: kept {len(accepted_files)} file(s)"
                + (", improved" if improved else ", no gain"),
                issues=verdict.issues,
                accepted_files=len(accepted_files), rejected_files=len(rejected),
                coverage=cov["score"], coverage_gaps=cov["gaps"]))
            self._emit(on_progress, rounds[-1])

            if improved:
                best, best_files, best_cov = verdict, dict(candidate), cov
                current = dict(candidate)
                self._best_so_far = (dict(best_files), best, best_cov)
                stale = 0
            else:
                stale += 1
                # Don't give up while requirements remain unmet — keep asking the
                # AI to close the gaps (bounded by max_rounds). Only allow the
                # convergence stop once the app is complete.
                if stale >= 2 and not best_cov["gaps"]:
                    break

    def _agentic_loop(
        self,
        context: AgenticLoopContext,
    ) -> tuple[dict[str, str], Any, dict[str, Any],
               list[dict[str, Any]], list[dict[str, Any]]]:
        """Agentic path: dual sessions, direct writes, per-commit gating."""
        blueprint = context.blueprint
        req = context.req
        workspace = context.workspace
        best_files = context.best_files
        best = context.best
        best_cov = context.best_cov
        rounds = context.rounds
        backend = context.backend
        schema = context.schema
        on_progress = context.on_progress
        from ai_assistant.app_builder.build_session import (
            AnswerSession,
            BuilderSession,
            DualSessionCoordinator,
            ValidatorSession,
        )
        from ai_assistant.app_builder.commit_gate import CommitGate, snapshot_workspace
        from ai_assistant.app_builder.governance import make_brief

        workspace = Path(workspace)
        self._write(workspace, best_files, overwrite=False)
        insight_text = ""
        if getattr(self, "_insight", None) is not None:
            insight_text = self._insight.app_summary or ""

        brief = make_brief(
            blueprint, self.engine,
            connection_name=(blueprint.connections or [""])[0],
            schema=schema,
            data_insight=insight_text,
            target_score=self.target_score,
            target_coverage=self.target_coverage,
        )
        transcript: list[dict[str, Any]] = []
        commits: list[dict[str, Any]] = []
        cancel_event = getattr(self, "_cancel_event", None)

        def on_event(payload: dict[str, Any]) -> None:
            transcript.append(payload)
            self._emit_agent(on_progress, payload)

        def _cancelled() -> bool:
            return bool(cancel_event is not None and cancel_event.is_set())

        builder = BuilderSession(backend, workspace, timeout=self.timeout,
                                 on_event=on_event, cancel_event=cancel_event,
                                 mask_pii=getattr(self, "_mask_pii", False))
        answerer = AnswerSession(backend, workspace, timeout=self.timeout,
                                 on_event=on_event, cancel_event=cancel_event,
                                 mask_pii=getattr(self, "_mask_pii", False))
        # Session C — validates the build. Same backend; the App Builder Assistant
        # invokes it sparingly with a compact evidence digest to keep tokens low.
        validator = ValidatorSession(backend, workspace, timeout=self.timeout,
                                     on_event=on_event, cancel_event=cancel_event,
                                     mask_pii=getattr(self, "_mask_pii", False))

        # Make the requirement machine-understandable so the App Builder
        # Assistant can answer the agent with balanced, math-based decisions.
        from ai_assistant.app_builder.decision import (
            DecisionEngine,
            build_requirement_model,
        )
        from ai_assistant.app_builder.mediation import (
            BuildProgress,
            ContextMediator,
        )

        req_model = build_requirement_model(
            self._description,
            entities=list(getattr(self, "_req_entities", []) or []),
            features=list(getattr(self, "_req_features", []) or []),
            kind=getattr(self, "_kind", "crud"),
        )
        decision_engine = DecisionEngine(req_model)
        # The App Builder Assistant mediates context/progress between sessions.
        mediator = ContextMediator(
            requirement_model=req_model, brief=brief,
            structure_enforced=blueprint.mode != BuildMode.FROM_SCRATCH,
        )
        progress = BuildProgress(
            phase="baseline", coverage=best_cov["score"], score=best.score,
            accepted=best.accepted, files_built=len(best_files),
            gaps=list(best_cov["gaps"]))

        def on_decision(record: dict[str, Any]) -> None:
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "decision",
                          "text": record.get("answer", ""),
                          "detail": record},
            })

        def on_review(record: dict[str, Any]) -> None:
            rules = record.get("injected_rules") or []
            if record.get("aligned") and not rules:
                return  # nothing noteworthy to surface
            head = ("advisor reply aligned" if record.get("aligned")
                    else "advisor reply re-aligned")
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "review",
                          "text": f"{head}: injected {len(rules)} rule(s)",
                          "detail": record},
            })

        def on_validation(record: dict[str, Any]) -> None:
            clean = record.get("clean")
            head = ("validation: complete — no issues" if clean
                    else "validation: issues found"
                    + (" (queued for builder)" if record.get("queued")
                       else (" (relayed to builder)" if record.get("relayed")
                             else "")))
            self._emit_agent(on_progress, {
                "session": "validator",
                "event": {"type": "validation",
                          "text": f"{head}\n{record.get('findings', '')}",
                          "detail": record},
            })

        def on_relay(rec: dict[str, Any]) -> None:
            # Session B's box shows only traffic bound for A (what B shares with A).
            # C-internal findings stay in C's box via on_validation.
            direction = rec.get("direction", "")
            if direction in ("c_to_b", "b_to_c"):
                return
            label = {
                "to_b": "App Builder → B",
                "user_to_b": "User → B",
                "a_to_b": "A → B (question)",
                "b_to_a": "B → A",
            }.get(direction, direction)
            self._emit_agent(on_progress, {
                "session": "answerer",
                "event": {"type": "relay",
                          "text": f"{label}: {rec.get('text', '')}",
                          "detail": rec},
            })

        coord = DualSessionCoordinator(
            builder, answerer, brief, self._decider,
            decision_engine=decision_engine, on_decision=on_decision,
            on_review=on_review, mediator=mediator, progress=progress,
            validator=validator, on_validation=on_validation,
            on_relay=on_relay)
        if self._insight is not None:
            coord.design_brief = (
                getattr(self._insight, "design_brief", "") or "").strip()
            coord.db_context = (
                getattr(self._insight, "prompt_block", lambda: "")() or "").strip()
        # Keep the live A/B/C sessions reachable after the build so the user can
        # continue chatting with them in interactive mode (post-build takeover).
        self._coord = coord
        svc_ref = getattr(self, "_service_ref", None)
        if svc_ref is not None:
            svc_ref.last_coordinator = coord
            job_id = getattr(svc_ref, "_active_job_id", None)
            if job_id:
                svc_ref._job_coordinators[job_id] = coord
        # Surface the derived priorities/targets up front so the build log shows
        # what the assistant is optimizing for.
        self._emit_agent(on_progress, {
            "session": "system",
            "event": {"type": "requirement_model",
                      "text": ("optimizing for: "
                               + ", ".join(req_model.top_dimensions(3))),
                      "detail": req_model.as_dict()},
        })
        # Agentic builds ship NO pre-built page: Session A authors the skeleton
        # AND a requirement-specific launchable page. Enable Start/Open only once
        # a runnable src/app.py exists (emitted from the build loop). If one
        # already exists (e.g. resuming a build), signal it now.
        if (workspace / "src" / "app.py").exists():
            self._emit_baseline_ready(on_progress, workspace)
        else:
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "status",
                          "text": "workspace ready — Session A will prepare the "
                                  "skeleton and a requirement-specific launch page, "
                                  "then Start app becomes available."},
            })
        from_db = blueprint.mode == BuildMode.FROM_DATABASE
        prime_msg = (
            "priming Session B with the full governance brief; Sessions A and C "
            "with minimal role-only primers…")
        self._emit_agent(on_progress, {
            "session": "system",
            "event": {"type": "status", "text": prime_msg},
        })
        coord.start()
        self._emit_session_status(
            on_progress, "answerer",
            "primed with the full governance brief — about to frame the first "
            "instruction for Sessions A and C.")
        self._emit_session_status(
            on_progress, "validator",
            "minimal role primer — waiting for Session B's first instruction.")
        # ── B frames first instruction before understanding (all modes) ───────
        # Session A never receives a read-only plan/outline turn. B authors the
        # authoritative instruction first, C receives it read-only for validation
        # alignment, and A gets it only on its first write-capable build turn.
        if not _cancelled():
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "status",
                          "text": "Session B framing the first build instruction "
                                  "for Sessions A and C…"},
            })
            if from_db:
                self._emit_session_status(
                    on_progress, "answerer",
                    "framing the ordered build instruction (intent, design brief, "
                    "schema/sample data, actions) for Sessions A and C…")
                frame = coord.frame_first_instruction(self._description)
            else:
                self._emit_session_status(
                    on_progress, "answerer",
                    "framing the build brief from your request for Sessions A "
                    "and C…")
                frame = coord.kickoff(self._description)
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "kickoff",
                          "text": "first instruction framed by Session B and "
                                  "delivered to Session C; queued for Session A's "
                                  "first write turn",
                          "detail": frame},
            })

        # ── Collaboration pipeline: parallel understanding + similarity gate ───
        if self.collaboration and not _cancelled():
            self._assistant = self._make_assistant(
                builder, answerer, validator, brief, on_relay, on_progress)
            self._run_understanding_phase(on_progress, _cancelled, from_db=from_db)

        gate = CommitGate(
            self.engine, blueprint,
            req_entities=self._req_entities,
            req_features=self._req_features,
            req_services=self._req_services,
            description=self._description,
            kind=getattr(self, "_kind", "crud"),
            target_coverage=self.target_coverage,
            fidelity_driven=getattr(self, "_fidelity_driven", False),
            structure_enforced=blueprint.mode != BuildMode.FROM_SCRATCH,
        )

        # ── Plan phase: prepare an auto-approved plan BEFORE building ──────────
        # The App Builder Assistant governs the plan: the builder drafts it, B
        # answers any plan questions, and the plan is auto-approved (or surfaced
        # in interactive mode) before a single file is written for real.
        if not _cancelled():
            self._plan_phase(blueprint, req, builder, coord, rounds, on_progress)

        # Session A publishes its scaffold (folders/files it laid down) so B and
        # C work from the same real structure. C is told this is its cue to start
        # authoring tests; B uses it as the map for monitoring/review. The
        # validator folder is reserved here so A/B never touch it.
        if not _cancelled():
            self._publish_structure(workspace, on_progress)

        from ai_assistant.app_builder.agent_runner import (
            agent_is_idle,
            agent_signaled_done,
            detect_phase_done,
        )

        current = dict(best_files)
        stale = 0
        no_progress = 0
        repeat = 0
        last_fp: Optional[str] = None
        last_suggestions: list[str] = []
        # Session C cadence: validate CONTINUOUSLY — every round A produces
        # changes — but only call C's LLM when there is NEW evidence (the digest
        # changed). The deterministic test run is free and happens every changed
        # round; ``max_validations`` caps the token-costing C consults.
        validations = 0
        last_val_digest: Optional[str] = None
        build_phase: Optional[str] = None
        thorough_idx = 0
        start_time = time.monotonic()
        for i in range(1, self.max_rounds + 1):
            if _cancelled():
                self._aborted = True
                self._stop_reason = "aborted by user"
                rounds.append(BuildRound(
                    i, "aborted", best.score, best.accepted,
                    "build aborted by user", coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            if (time.monotonic() - start_time) > self.max_wall_clock_seconds:
                self._stop_reason = "time budget exhausted"
                rounds.append(BuildRound(
                    i, "stopped", best.score, best.accepted,
                    "stopped: time budget exhausted", coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            phase = build_phase or (
                "coverage" if best_cov["gaps"] else self.PHASES[
                    (i - 1) % len(self.PHASES)])
            # Refresh the mediator's progress snapshot so any question raised in
            # this turn is forwarded to the advisor with up-to-date context.
            coord.progress = BuildProgress(
                phase=phase, round=i, coverage=best_cov["score"],
                score=best.score, accepted=best.accepted,
                files_built=len(current), gaps=list(best_cov["gaps"]),
                suggestions=list(last_suggestions))
            prompt = self._frame_iteration(blueprint, req, current, best,
                                           best_cov, phase)
            build_prefix = (
                "Build/improve the application directly in this workspace. "
                "Write files and run tests. Do not just describe changes. "
                "Keep making progress toward a complete, runnable app.\n\n")
            patch_guard = (
                "PATCH the existing app in place. Do NOT recreate the project, "
                "re-scaffold folders, or rewrite working files. Touch only what "
                "is needed for the issue/gap below; preserve all passing "
                "behavior. Fix the root cause.\n\n")
            # Fold the plan-approval hand-off into the first build prompt so it
            # does not cost a separate agent round-trip (see _plan_phase).
            proceed_note = getattr(self, "_plan_proceed_note", "")
            if proceed_note:
                prompt = proceed_note + "\n\n" + build_prefix + prompt
                self._plan_proceed_note = ""
            else:
                prompt = patch_guard + build_prefix + prompt
            if self.validation_mode == "thorough":
                phases = self.THOROUGH_PHASES
                target = phases[min(thorough_idx, len(phases) - 1)]
                prompt += (
                    f"\n\nTHOROUGH BUILD — complete ONLY the '{target}' phase "
                    f"in this turn. When done emit exactly: PHASE-DONE: {target}. "
                    "Do not start other phases yet."
                )
            self._emit(on_progress, BuildRound(
                i, phase, best.score, best.accepted,
                "agentic builder session — contacting AI backend…",
                coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))

            before = snapshot_workspace(workspace)
            events = builder.send(prompt)
            if _cancelled():
                self._aborted = True
                rounds.append(BuildRound(
                    i, phase, best.score, best.accepted,
                    "build aborted by user", coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            if self._agent_events_have_error(events):
                rounds.append(BuildRound(
                    i, phase, best.score, best.accepted,
                    "agentic builder backend error; stopped",
                    coverage=best_cov["score"],
                    coverage_gaps=best_cov["gaps"],
                ))
                self._emit(on_progress, rounds[-1])
                break
            # Route only this turn's NEW genuine questions / permission requests
            # (deduped + capped inside the coordinator) so the builder is answered
            # when it truly asks — not re-answered for narration every round.
            routed = coord.route_questions(events, context=builder.transcript[-2000:])
            answered_this_round = bool(routed)
            last_text = self._events_text(events)
            phase_done = detect_phase_done(last_text)
            if phase_done:
                phase = build_phase = phase_done[-1]
                coord.progress = BuildProgress(
                    phase=phase, round=i, coverage=best_cov["score"],
                    score=best.score, accepted=best.accepted,
                    files_built=len(current), gaps=list(best_cov["gaps"]),
                    suggestions=list(last_suggestions))

            # Per-round gate is intentionally lenient: meters/managers (code
            # quality + core structure) still block bad commits, but optional
            # infra add-ons and not-yet-complete coverage do NOT trigger a revert
            # — they are surfaced as suggestions/gaps and drive the next nudge.
            verdict = gate.gate(workspace, before, label=f"round_{i}",
                                infra_blocking=False, enforce_coverage=False,
                                revert=False)
            last_suggestions = list(verdict.suggestions)
            commits.append({**verdict.as_dict(), "round": i, "phase": phase})
            self._emit_agent(on_progress, {
                "session": "gate",
                "event": {"type": "commit_verdict",
                          "text": verdict.feedback_text(),
                          "detail": verdict.as_dict()},
            })
            self._reconcile_data_layer_workspace(
                workspace, blueprint, req, on_progress)

            files = self._read_workspace(workspace)
            # The moment Session A produces a runnable app, enable Start app — the
            # launch page is whatever A wrote for THIS app's requirement.
            if not self._baseline_ready_emitted and \
                    (workspace / "src" / "app.py").exists():
                self._emit_baseline_ready(on_progress, workspace)
            files = self._ensure_required(blueprint, req, files)
            eval_verdict = self._evaluate(blueprint, files)
            cov = self._coverage(files)
            improved = self._better(eval_verdict, cov, best, best_cov,
                                    len(files), len(current))

            rounds.append(BuildRound(
                i, phase, eval_verdict.score, eval_verdict.accepted,
                f"agentic {phase}: commit "
                f"{'accepted' if verdict.accepted else 'kept (gate flagged issues)'}"
                + (", improved" if improved else ""),
                issues=eval_verdict.issues,
                accepted_files=len(verdict.files_changed),
                coverage=cov["score"], coverage_gaps=cov["gaps"]))
            self._emit(on_progress, rounds[-1])

            changed = bool(verdict.files_changed)
            if improved:
                best, best_cov = eval_verdict, cov
                stale = 0
            else:
                stale += 1
            # Keep the returned/written file set in lockstep with the workspace.
            # The per-round gate no longer reverts (revert=False), so whatever
            # the agent produced stays on disk — track it here so the final
            # write-back never deletes the agent's work.
            if changed:
                current = dict(files)

            # Progress / stuck tracking for the hard guards.
            no_progress = 0 if (improved or changed) else no_progress + 1
            fp = self._turn_fingerprint(last_text, verdict.files_changed)
            repeat = repeat + 1 if (fp and fp == last_fp) else 0
            last_fp = fp

            # Gate failed this commit. We do NOT delete anything (revert=False).
            # In uninterrupted mode the build keeps iterating on its own; in any
            # other mode we pause here and hand control to the user so they can
            # review the result and continue the conversation with the live
            # sessions instead of silently wiping progress.
            if not verdict.accepted and not self._decider.uninterrupted:
                self._stop_reason = "gate flagged issues — paused for your input"
                rounds.append(BuildRound(
                    i, "paused", eval_verdict.score, eval_verdict.accepted,
                    "paused: this commit did not pass the gate. Nothing was "
                    "deleted — review the result, then continue via "
                    "interactive / take control.",
                    issues=eval_verdict.issues,
                    coverage=cov["score"], coverage_gaps=cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break

            # ── Session C: continuous dry/unit validation during the build ─────
            # Whenever A produced changes (it "claims" an implementation), the
            # App Builder Assistant runs the test suite (deterministic, no tokens)
            # and — when the evidence is NEW — polls Session C for a verdict. C's
            # findings are framed by B and QUEUED here; they are delivered to A
            # later (deliver_feedback), only when A is free, so an unsolicited
            # note never collides with an answer A is waiting for.
            # Clear last round's code-gate result so a stale failure never
            # re-nudges A about something already fixed; it is re-set below only
            # when this round actually runs the continuous check.
            self._round_preflight = None
            if (changed or phase_done) and not _cancelled():
                components = self._components_touched(
                    list(verdict.files_changed), phase_done)
                primary = components[0]
                test_paths = self._test_paths_for_components(
                    workspace, components)
                # Continuous code gate (runs every changed round, in step with the
                # build): compile + import dry-run BEFORE the unit tests, so a
                # non-compiling / non-importing app is caught immediately and the
                # concrete error is conveyed to Session A this same round.
                self._emit_session_status(
                    on_progress, "validator",
                    f"running compile + import dry-run and unit tests for "
                    f"'{primary}'…")
                self._round_preflight = self._round_preflight_check(workspace)
                pf = self._round_preflight
                if pf is not None:
                    self._emit_agent(on_progress, {
                        "session": "validator",
                        "event": {
                            "type": "validation",
                            "text": ("compile + import dry-run "
                                     + ("PASSED" if pf.ok
                                        else "FAILED:\n" + pf.digest())),
                            "detail": {"clean": pf.ok, **pf.as_dict()},
                        },
                    })
                test_outcome = self._run_tests(workspace, paths=test_paths)
                coord.progress = BuildProgress(
                    phase=phase, round=i, coverage=best_cov["score"],
                    score=best.score, accepted=best.accepted,
                    files_built=len(current), gaps=list(best_cov["gaps"]),
                    suggestions=list(last_suggestions))
                # Collaboration mode: run the standard quality battery, surface
                # the readout, and fold the top meter findings into the evidence
                # digest so Session C/B reason against industry-standard signals.
                meter_signals = self._meter_governance(
                    current, test_outcome, component=primary)
                digest = self._validation_digest(
                    current, best, best_cov, test_outcome,
                    component=primary, test_paths=test_paths)
                digest = self._augment_digest_with_meters(digest, meter_signals)
                if pf is not None and not pf.ok:
                    # Lead the evidence with the runnability failure so C/B
                    # prioritize it — it would crash the app on launch.
                    digest = ("CODE GATE (deterministic — compile + import "
                              "dry-run) FAILED:\n" + pf.digest()
                              + "\n\n" + digest)
                pf_failed = pf is not None and not pf.ok
                # Consult C when the evidence is new (token-bounded), OR — even
                # past the cap — when the deterministic code gate failed, since a
                # broken-to-launch app must be raised to A every time it recurs.
                if (digest != last_val_digest
                        and (validations < self.max_validations or pf_failed)):
                    coord.relay_validation(
                        digest,
                        test_scope=self._test_scope_for_component(primary),
                        how_to_test=self._how_to_test_for_component(primary),
                        relay=True,
                        green_relay=bool(phase_done) and not pf_failed,
                        component=primary)
                    if validations < self.max_validations:
                        validations += 1
                    last_val_digest = digest
                if (self.validation_mode == "thorough" and phase_done
                        and thorough_idx < len(self.THOROUGH_PHASES) - 1):
                    done_set = {c.lower() for c in phase_done}
                    if self.THOROUGH_PHASES[thorough_idx] in done_set:
                        thorough_idx += 1
                # Session C authors its OWN tests in validator_generated_tests/.
                # First pass once A has real code; refresh when a component is
                # PHASE-DONE so new surfaces get covered. Sandbox-enforced and
                # non-blocking — never reverts or gates Session A's work.
                if (changed and self._structure_published
                        and self._validator_test_authors < 3
                        and (self._validator_test_authors == 0 or phase_done)):
                    self._validator_author_tests(
                        workspace, coord, brief, on_progress)

            if self._done(best, best_cov):
                self._stop_reason = "requirements met"
                break
            if _cancelled():
                self._aborted = True
                self._stop_reason = "aborted by user"
                break

            done_signal = agent_signaled_done(last_text)
            meters_ok = best.accepted and not best_cov["gaps"]
            if done_signal and meters_ok:
                self._stop_reason = "agent reported completion"
                rounds.append(BuildRound(
                    i, "stopped", best.score, best.accepted,
                    "stopped: agent reported completion and meters are satisfied",
                    coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            if no_progress >= self.max_no_progress_rounds:
                self._stop_reason = "no progress"
                rounds.append(BuildRound(
                    i, "stopped", best.score, best.accepted,
                    f"stopped: no progress for {no_progress} rounds",
                    coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break
            if repeat >= self.repeat_output_limit:
                self._stop_reason = "agent repeating output"
                rounds.append(BuildRound(
                    i, "stopped", best.score, best.accepted,
                    "stopped: agent is repeating itself / stuck",
                    coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break

            # B watches A's progress and may queue a proactive nudge when stuck.
            coord.b_progress_check(
                builder.last_events,
                no_progress=not (improved or changed),
                phase_done=bool(phase_done),
                no_progress_streak=no_progress,
            )
            # User messages (queued during auto/agent builds) take priority, then
            # ONE queued C→B validation note — only when A is free.
            user_del = coord.deliver_user_messages(builder.last_events)
            if user_del is not None:
                answered_this_round = True
                self._emit_agent(on_progress, {
                    "session": "answerer",
                    "event": {"type": "user_message_delivered",
                              "text": "queued user message delivered to builder",
                              "detail": user_del},
                })
            delivered = coord.deliver_feedback(builder.last_events)
            if delivered is not None and delivered.get("findings") is not None:
                answered_this_round = True
                self._emit_agent(on_progress, {
                    "session": "validator",
                    "event": {"type": "validation_delivered",
                              "text": "queued validation feedback delivered to "
                                      "the builder",
                              "detail": {"advice": delivered.get("advice", "")}},
                })
            elif delivered is not None:
                answered_this_round = True

            # Only push forward when it is warranted: gaps remain, the agent made
            # changes, it is idle/waiting, or it claimed done but meters disagree.
            # Otherwise there is nothing to ask for — finish gracefully.
            warranted = (
                bool(best_cov["gaps"]) or changed or answered_this_round
                or agent_is_idle(last_text) or (done_signal and not meters_ok)
            )
            if not warranted:
                self._stop_reason = "nothing left to build"
                rounds.append(BuildRound(
                    i, "stopped", best.score, best.accepted,
                    "stopped: no remaining requirements and no new changes",
                    coverage=best_cov["score"], coverage_gaps=best_cov["gaps"]))
                self._emit(on_progress, rounds[-1])
                break

            if delivered is None:
                nudge = self._forward_nudge(
                    verdict, best_cov,
                    premature_done=done_signal and not meters_ok)
                feedback_events = builder.send(nudge)
                if self._agent_events_have_error(feedback_events):
                    self._stop_reason = "backend error"
                    rounds.append(BuildRound(
                        i, phase, best.score, best.accepted,
                        "agentic builder backend error after gate feedback; "
                        "stopped",
                        coverage=best_cov["score"],
                        coverage_gaps=best_cov["gaps"],
                    ))
                    self._emit(on_progress, rounds[-1])
                    break
            if stale >= 3 and not best_cov["gaps"]:
                self._stop_reason = "stalled"
                break
        else:
            if not self._stop_reason:
                self._stop_reason = "round limit reached"

        # Final, full gate (infra enforced): a closing production-readiness
        # report over the best build with infra add-ons backfilled. This is the
        # "end of build" infra check — informational, never reverts.
        final_files = self._ensure_required(blueprint, req, dict(current))
        final = gate.evaluate_files(
            final_files, infra_blocking=True, enforce_coverage=True)
        commits.append({**final.as_dict(), "round": "final", "phase": "final"})
        self._emit_agent(on_progress, {
            "session": "gate",
            "event": {"type": "commit_verdict",
                      "text": "FINAL GATE (production-readiness)\n"
                              + final.feedback_text(),
                      "detail": final.as_dict()},
        })

        # ── Session C: full validation + A/B/C agreement after the build ───────
        paused_for_user = self._stop_reason.startswith("gate flagged issues")
        agreement: dict[str, Any] = {}
        if not _cancelled() and not paused_for_user:
            final_context = FinalizeContext(
                workspace=workspace,
                blueprint=blueprint,
                req=req,
                gate=gate,
                coord=coord,
                last_suggestions=last_suggestions,
                on_progress=on_progress,
            )
            final_state = FinalBuildState(current, best, best_cov)
            agreement, current, best, best_cov = self._finalize_with_repairs(
                final_context, final_state, _cancelled)
        self._agreement = agreement
        # Final quality readout (collaboration mode) with a fresh test run so
        # functional_correctness reflects the shipped app, not an early round.
        if self.collaboration and self._assistant is not None and self._app_meters:
            try:
                final_tests = self._run_tests(workspace)
                from ai_assistant.app_builder.meters.registry import QualityInput

                eq = self._app_meters.evaluate_quality(
                    QualityInput(
                        files=current,
                        description=self._description,
                        features=list(getattr(self, "_req_features", []) or []),
                        entities=list(getattr(self, "_req_entities", []) or []),
                        test_outcome=final_tests,
                    ))
                self._final_quality = eq
                if self._assistant is not None:
                    self._assistant.last_quality = eq.get("meters", {})
            except Exception:  # noqa: BLE001
                self._final_quality = {}

        return current, best, best_cov, commits, transcript

    # ── collaboration pipeline (App Builder Assistant) ────────────────────────
    def _make_assistant(self, builder, answerer, validator, brief,
                        on_relay, on_progress):
        """Construct the App Builder Assistant over the live A/B/C sessions."""
        from ai_assistant.app_builder.assistant import AppBuilderAssistant
        from ai_assistant.app_builder.meters.registry import AppMeterRegistry
        from ai_assistant.app_builder.meter_managers.registry import (
            MeterManagerRegistry,
        )

        if self._app_meters is None:
            self._app_meters = AppMeterRegistry()
        if self._meter_managers is None:
            self._meter_managers = MeterManagerRegistry()

        def _on_event(payload: dict[str, Any]) -> None:
            self._emit_agent(on_progress, {
                "session": "assistant",
                "event": {"type": f"assistant_{payload.get('type', 'event')}",
                          "text": "App Builder Assistant measurement",
                          "detail": payload.get("detail")},
            })

        assistant = AppBuilderAssistant(
            builder=builder, advisor=answerer, validator=validator, brief=brief,
            meters=self._app_meters, managers=self._meter_managers,
            on_relay=on_relay, on_event=_on_event)
        return assistant

    def _run_understanding_phase(self, on_progress, cancelled, *,
                                 from_db: bool = False) -> None:
        """Run the parallel kickoff + design-similarity gate before building."""
        from ai_assistant.app_builder.understanding import run_understanding_phase

        self._emit_agent(on_progress, {
            "session": "system",
            "event": {"type": "status",
                      "text": "initialization: A/B/C sharing their understanding "
                              "in parallel (understanding phase)…"},
        })
        coord = getattr(self, "_coord", None)
        if from_db and coord is not None:
            self._emit_session_status(
                on_progress, "answerer",
                "understanding phase: comparing B's framed design against "
                "Sessions A and C's understanding of the instruction…")
        else:
            self._emit_session_status(
                on_progress, "answerer",
                "understanding phase: drafting the business design — entities, "
                "components and the core features Session A must deliver…")
        self._emit_session_status(
            on_progress, "validator",
            "understanding phase: drafting the validation outline / test plan "
            "(health, core flows, edge cases, sample data)…")
        kw: dict = {}
        if coord is not None:
            kw = {
                "builder_instruction": getattr(coord, "builder_instruction", ""),
                "validator_instruction": getattr(
                    coord, "validator_instruction", ""),
                "advisor_design": getattr(coord, "_framed_brief", ""),
            }
        try:
            result = run_understanding_phase(
                self._assistant, self._description,
                entities=list(getattr(self, "_req_entities", []) or []),
                features=list(getattr(self, "_req_features", []) or []),
                threshold=0.75, max_reconcile=0,
                cancelled=cancelled, **kw)
        except Exception as exc:  # noqa: BLE001
            self._emit_agent(on_progress, {
                "session": "system",
                "event": {"type": "understanding_error", "text": str(exc)},
            })
            return
        self._understanding = result
        # Reuse the work done in this phase so it is NOT redone later:
        #  - Session C already drafted a test plan → hand it to the coordinator
        #    so the kickoff does not ask C to draft another one.
        #  - Session A already produced a build plan → the plan phase will skip
        #    re-asking A to plan and go straight to approval.
        c_plan = (result.plan_texts or {}).get("C", "")
        if c_plan and getattr(self, "_coord", None) is not None \
                and not getattr(self._coord, "_test_plan", ""):
            self._coord._test_plan = c_plan
            self._emit_agent(on_progress, {
                "session": "validator",
                "event": {"type": "test_plan",
                          "text": f"Test plan drafted (understanding phase):\n"
                                  f"{c_plan}",
                          "detail": {"plan": c_plan}},
            })
        sim = result.similarity or {}
        verdict = ("agreed" if result.ready
                   else "proceeding with best-available shared design")
        self._emit_agent(on_progress, {
            "session": "assistant",
            "event": {"type": "session_understanding",
                      "text": (f"design similarity {sim.get('score', 0):.0%} "
                               f"(need 75%) over {result.rounds} round(s) — "
                               f"{verdict}"),
                      "detail": result.as_dict()},
        })

    def _meter_governance(self, files, test_outcome, *, component="") -> None:
        """Evaluate the quality battery and route remediation via B (collab mode).

        Runs the App Builder Assistant's standard meters and turns any failing
        meter into a remediation signal through Session B. Emits the quality
        readout for the UI/C. No-op outside collaboration mode.
        """
        if not (self.collaboration and self._assistant is not None):
            return
        try:
            from ai_assistant.app_builder.meters.registry import QualityInput

            measurements = self._app_meters.quality_measurements(
                QualityInput(
                    files=files,
                    description=self._description,
                    features=list(getattr(self, "_req_features", []) or []),
                    entities=list(getattr(self, "_req_entities", []) or []),
                    test_outcome=test_outcome or {},
                ))
            policy = getattr(self, "_builder_policy", None)
            if policy is not None:
                from ai_assistant.app_builder.builder_types import meter_subset
                measurements = meter_subset(policy.meter_names(), measurements)
            report = {k: m.as_dict() for k, m in measurements.items()}
            self._assistant.last_quality = report
            self._emit_agent(None, {
                "session": "assistant",
                "event": {"type": "quality", "detail": report},
            })
            signals = self._meter_managers.manage_all(
                measurements.values(), component=component)
            # B's "how much of the agreed design is built vs pending" signal,
            # available once the understanding phase fixed the agreed design.
            self._assistant.design_completeness(files)
            return signals
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _augment_digest_with_meters(digest: str, signals) -> str:
        """Fold the top meter-manager findings into the C/B evidence digest."""
        if not signals:
            return digest
        lines = ["", "STANDARD QUALITY METERS (factual — verify before acting):"]
        for s in signals[:5]:
            miss = (" | missing: " + ", ".join(s.missing[:5])) if s.missing else ""
            lines.append(f"- {s.meter} [{s.severity.value}] "
                         f"{s.score:.0%}/{s.threshold:.0%}: {s.reason}{miss}")
        return digest + "\n" + "\n".join(lines)

    @staticmethod
    def _events_text(events: list[Any]) -> str:
        """Concatenate assistant text from a turn's events (robust to stubs)."""
        out: list[str] = []
        for ev in events or []:
            etype = getattr(getattr(ev, "type", None), "value", None) \
                or getattr(ev, "type", None)
            if etype == "assistant_text":
                txt = getattr(ev, "text", "")
                if txt:
                    out.append(txt)
        return "\n".join(out)

    @staticmethod
    def _turn_fingerprint(text: str, files_changed: list[str]) -> str:
        """Stable hash of a turn (text + changed files) for stuck detection."""
        payload = (text or "") + "|" + ",".join(sorted(files_changed or []))
        return hashlib.sha1(payload.encode("utf-8", "replace")).hexdigest()

    def _plan_phase(self, blueprint, req, builder, coord, rounds,
                    on_progress) -> None:
        """Ask the builder for an auto-approved plan, then approve and proceed."""
        plan_round = BuildRound(
            0, "plan", 1.0, True,
            "preparing an auto-approved plan (App Builder Assistant governed)…")
        rounds.append(plan_round)
        self._emit(on_progress, plan_round)
        # In collaboration mode Session A already produced a build plan during
        # the parallel understanding phase, so skip re-asking it to plan (a
        # whole extra A turn) and go straight to approval + "proceed to build".
        understanding = getattr(self, "_understanding", None)
        already_planned = bool(
            understanding is not None
            and (understanding.plan_texts or {}).get("A"))
        if not already_planned:
            plan_prompt = self._frame_plan(blueprint, req)
            try:
                builder.plan(plan_prompt)
            except Exception:  # noqa: BLE001
                return
            if getattr(builder, "cancelled", False):
                return
            # Answer any questions the plan raised (auto via Session B, or user
            # in interactive) so the plan is resolved before building.
            coord.route_questions(context=builder.transcript[-2000:])
        # App Builder Assistant approves the plan (auto-approved unless the user
        # is in interactive mode, where it is surfaced for confirmation).
        approved = self._decider.approved(BuildDecision(
            id="confirm_agent_plan",
            question="Approve this build plan and start building?",
            detail="The App Builder Assistant validated the plan against the "
                   "governance brief, meters and required infrastructure.",
            default=True,
        ))
        verdict = "approved" if approved else "revised"
        # Defer the "proceed to build" hand-off instead of spending a whole
        # builder turn on it: stash it so the FIRST build round prepends it to
        # its prompt. This removes one full agent round-trip (minutes of
        # wall-clock) from every build without changing the protocol.
        proceed = (
            f"PLAN {verdict.upper()} by the App Builder Assistant. Proceed to "
            "BUILD now. There is NO pre-built app — you create everything. "
            "FIRST lay down the folder/file skeleton from your plan, and in the "
            "SAME step write a minimal but RUNNABLE src/app.py (importable "
            "src.app:app) that exposes GET /health AND an index landing page "
            "built for THIS app's purpose and requirement — a real first page "
            "for this specific app, NOT a generic 'build in progress' "
            "placeholder. Keep it launchable from that point on. THEN implement "
            "the features, growing the page/flows as you go. Session C authors "
            "its own tests in validator_generated_tests/ — you must NOT write "
            "there. Run your compilations and tests as you build; do not wait "
            "for further confirmation.")
        coord = getattr(self, "_coord", None)
        if coord is not None:
            # Session A never received a read-only instruction turn, so deliver
            # B's FULL build instruction as part of A's first WRITE turn.
            instruction = getattr(coord, "builder_instruction", "") \
                or getattr(coord, "_builder_action", "")
            if instruction:
                proceed = f"{instruction}\n\n{proceed}"
        self._plan_proceed_note = proceed
        self._emit_agent(on_progress, {
            "session": "system",
            "event": {"type": "plan_approved", "text": verdict,
                      "detail": {"approved": approved}},
        })

    def _frame_plan(self, blueprint, req) -> str:
        """Prompt that asks the builder to produce an auto-approved plan."""
        lines = [
            "BEFORE writing any code, prepare an AUTO-APPROVED implementation "
            "PLAN for this application. The App Builder Assistant will validate "
            "it against the governance brief, the meter rubric and the required "
            "infrastructure, then approve it so you can proceed automatically.",
            "Base the plan on the BUILD BRIEF already handed to you by the App "
            "Builder Assistant. Do not ask for, quote, or reinterpret the raw "
            "user description.",
            "Do NOT wait for the user to approve unless explicitly asked.",
            "",
            f"APP: {blueprint.name}",
        ]
        lines += [
            "",
            "The plan MUST cover:",
            "  - app purpose and the main user flows",
            "  - the PROJECT SKELETON to create FIRST (docs/, src/, templates/, "
            "static/, and your tests/ layout) before any feature code — Sessions "
            "B and C will receive this structure; do NOT use validator_generated_tests/",
            "  - the data model / entities the app actually needs",
            "  - the pages/UI and the API routes",
            "  - tests placed in their tests/ folder (unit_test, full_test, api, "
            "db, connectivity, functionality, write_test_cases) plus the sample "
            "data in tests/test_sample_data used to prove each feature",
            "  - how the centralized infra services are wired in",
            "  - how the build will satisfy each meter in the rubric",
            "Keep the plan concise and actionable; then await the App Builder "
            "Assistant's approval message before building.",
        ]
        return "\n".join(lines)

    def _finalize_with_repairs(
        self,
        context: FinalizeContext,
        state: FinalBuildState,
        cancelled: Callable[[], bool],
    ) -> tuple[dict[str, Any], dict[str, str], Any, dict[str, Any]]:
        """Run final evaluation and bounded repair rounds until A/B/C agree."""
        eval_res = self._final_evaluation_pass(context, state)
        agreement = eval_res["agreement"]
        state = FinalBuildState(
            eval_res.get("current", state.current),
            eval_res.get("best", state.best),
            eval_res.get("best_cov", state.best_cov),
        )
        repair_attempts = 0
        _builder = getattr(context.coord, "builder", None)
        while (not agreement.get("complete")
               and repair_attempts < self.max_finalize_repairs
               and self._decider.uninterrupted
               and _builder is not None
               and not cancelled()):
            repair_attempts += 1
            advice = (agreement.get("statements") or {}).get("advice", "")
            if not advice:
                issues = agreement.get("issues") or []
                advice = "\n".join(f"- {i}" for i in issues)
            self._emit_agent(context.on_progress, {
                "session": "system",
                "event": {
                    "type": "status",
                    "text": (
                        f"Final repair round {repair_attempts}/"
                        f"{self.max_finalize_repairs} — Session A patching "
                        "root-cause fixes…"),
                },
            })
            _builder.send(self._final_repair_instruction(
                advice, attempt=repair_attempts))
            if cancelled():
                break
            current = dict(self._read_workspace(context.workspace))
            files = self._ensure_required(
                context.blueprint, context.req, current)
            eval_verdict = self._evaluate(context.blueprint, files)
            cov = self._coverage(files)
            if self._better(eval_verdict, cov, state.best, state.best_cov,
                            len(files), len(current)):
                state = FinalBuildState(files, eval_verdict, cov)
            else:
                state = FinalBuildState(files, state.best, state.best_cov)
            eval_res = self._final_evaluation_pass(
                context, state, repair_round=repair_attempts)
            agreement = eval_res["agreement"]
            state = FinalBuildState(
                eval_res.get("current", state.current),
                eval_res.get("best", state.best),
                eval_res.get("best_cov", state.best_cov),
            )
        if agreement.get("complete"):
            if repair_attempts:
                self._stop_reason = (
                    f"all sessions agree — build complete "
                    f"(after {repair_attempts} repair round"
                    f"{'s' if repair_attempts != 1 else ''})")
            elif (not getattr(self, "_stop_reason", "")
                  or "sessions disagree" in getattr(self, "_stop_reason", "")):
                self._stop_reason = "all sessions agree — build complete"
        elif (repair_attempts >= self.max_finalize_repairs
              and self.max_finalize_repairs > 0
              and self._decider.uninterrupted):
            self._stop_reason = "repair budget exhausted — issues remain"

        # Deterministic runnability repairs run even in interactive mode so a
        # compile/import/boot/smoke failure is patched before handoff.
        det_budget = max(self.max_finalize_repairs, 1)
        det_attempts = 0
        while (not eval_res.get("runnable_ok", True)
               and det_attempts < det_budget
               and _builder is not None
               and not cancelled()):
            det_attempts += 1
            gate_advice = self._runnable_gate_advice()
            self._emit_agent(context.on_progress, {
                "session": "system",
                "event": {
                    "type": "status",
                    "text": (
                        f"Runnability repair {det_attempts}/{det_budget} — "
                        "Session A fixing compile/import/boot/smoke failures…"),
                },
            })
            _builder.send(self._final_repair_instruction(
                gate_advice, attempt=det_attempts))
            if cancelled():
                break
            current = dict(self._read_workspace(context.workspace))
            files = self._ensure_required(
                context.blueprint, context.req, current)
            eval_verdict = self._evaluate(context.blueprint, files)
            cov = self._coverage(files)
            if self._better(eval_verdict, cov, state.best, state.best_cov,
                            len(files), len(current)):
                state = FinalBuildState(files, eval_verdict, cov)
            else:
                state = FinalBuildState(files, state.best, state.best_cov)
            eval_res = self._final_evaluation_pass(
                context, state, repair_round=det_attempts)
            agreement = eval_res["agreement"]
            state = FinalBuildState(
                eval_res.get("current", state.current),
                eval_res.get("best", state.best),
                eval_res.get("best_cov", state.best_cov),
            )

        if not eval_res.get("runnable_ok", True):
            pf, sm = self._runnable_ok()
            bits = []
            if not pf:
                bits.append("code/boot gate")
            if not sm:
                bits.append("launch smoke")
            detail = " + ".join(bits) or "runnability gate"
            self._stop_reason = (
                f"build INCOMPLETE — runnability gate failing ({detail})")

        return agreement, state.current, state.best, state.best_cov

    def _runnable_gate_advice(self) -> str:
        """Build repair advice from the last deterministic launch gates."""
        parts: list[str] = []
        pf = getattr(self, "_preflight", None)
        if pf is not None and not pf.ok:
            parts.append(
                "CODE GATE (deterministic — compile + import dry-run):\n"
                + pf.digest())
        boot = getattr(self, "_boot_check", None)
        if boot is not None and not boot.ok:
            parts.append(
                "BOOT CHECK (deterministic — TestClient lifespan):\n"
                + boot.digest())
        sm = getattr(self, "_http_smoke", None)
        if sm is not None and not sm.skipped and not sm.ok:
            parts.append(
                "LAUNCH SMOKE (deterministic — uvicorn + HTTP GET):\n"
                + sm.digest())
        if not parts:
            return (
                "The app failed deterministic runnability checks. Fix import-time "
                "crashes, boot/lifespan errors, and broken routes so "
                "src.app:app imports and serves /health and /.")
        return "\n\n".join(parts)

    def _final_repair_instruction(self, advice: str, *, attempt: int) -> str:
        """Patch-only repair prompt after finalize_agreement says not complete."""
        budget = self.max_finalize_repairs
        return "\n".join(s for s in (
            f"FINAL REVIEW — REPAIR ROUND {attempt}/{budget}.",
            "Fix the ROOT CAUSE of each item below — no bypasses, stubs, or "
            "workarounds. PATCH the existing app in place; do NOT recreate the "
            "project, re-scaffold folders, or rewrite working files. Keep the "
            "code optimized and clean. Re-run your own compile/tests when done.",
            "",
            advice.strip() or "(see issues above)",
        ) if s)

    def _final_evaluation_pass(
        self,
        context: FinalizeContext,
        state: FinalBuildState,
        repair_round: int = 0,
    ) -> dict[str, Any]:
        """Run launch gates, full tests, and A/B/C finalize_agreement once."""
        from ai_assistant.app_builder.agent_runner import agent_signaled_done
        from ai_assistant.app_builder.mediation import BuildProgress

        self._run_launch_gates(context.workspace, context.on_progress, emit_pass=True)
        final_test = self._run_tests(context.workspace)
        context.coord.progress = BuildProgress(
            phase="final", round=self.max_rounds, coverage=state.best_cov["score"],
            score=state.best.score, accepted=state.best.accepted,
            files_built=len(state.current), gaps=list(state.best_cov["gaps"]),
            suggestions=list(context.last_suggestions))
        final_files = self._ensure_required(
            context.blueprint, context.req, dict(state.current))
        final = context.gate.evaluate_files(
            final_files, infra_blocking=True, enforce_coverage=True)
        code_gate = ""
        if self._preflight is not None:
            code_gate = (
                "CODE GATE (deterministic — compile + import dry-run):\n"
                + self._preflight.digest() + "\n\n")
        boot_gate = ""
        if self._boot_check is not None:
            boot_gate = (
                "BOOT CHECK (deterministic — TestClient lifespan):\n"
                + self._boot_check.digest() + "\n\n")
        smoke_gate = ""
        if self._http_smoke is not None:
            smoke_gate = (
                "LAUNCH SMOKE (deterministic — uvicorn + HTTP GET):\n"
                + self._http_smoke.digest() + "\n\n")
        validator_gate = ""
        if self._validator_test_outcome is not None:
            vto = self._validator_test_outcome
            validator_gate = (
                "VALIDATOR TESTS (Session C's own suite — last run):\n"
                + (vto.get("summary") or
                   ("passed" if vto.get("passed") else "failed"))
                + "\n\n")
        final_digest = (
            boot_gate + smoke_gate + code_gate + validator_gate
            + self._validation_digest(
                final_files, final, state.best_cov, final_test, component="final"))
        preflight_ok, smoke_ok = self._runnable_ok()
        runnable_ok = preflight_ok and smoke_ok
        meters_ok = self._done(state.best, state.best_cov) and runnable_ok
        if not preflight_ok:
            self._stop_reason = "code/boot gate failed — app is not healthy yet"
        elif not smoke_ok and not self._stop_reason:
            self._stop_reason = (
                "launch smoke failed — app did not serve /health")
        _builder = getattr(context.coord, "builder", None)
        agent_done = agent_signaled_done(
            (getattr(_builder, "last_text", "") or "") if _builder else "")
        review_structure = self._workspace_structure(context.workspace)
        review_symbols = self._public_symbols(context.workspace)
        try:
            from ai_assistant.app_builder.build_session import FinalAgreementContext

            agreement = context.coord.finalize_agreement(
                FinalAgreementContext(
                    digest=final_digest,
                    meters_ok=meters_ok,
                    agent_done=agent_done,
                    test_scope=self._test_scope(),
                    how_to_test=self._how_to_test(),
                    code_review_evidence=final_digest,
                    structure=review_structure,
                    symbols=review_symbols,
                ))
        except TypeError:
            agreement = context.coord.finalize_agreement(
                final_digest, meters_ok=meters_ok, agent_done=agent_done,
                test_scope=self._test_scope(),
                how_to_test=self._how_to_test())
        b_review = (agreement.get("statements") or {}).get("b", "")
        if b_review and b_review.strip() not in ("", "(no confirmation)"):
            label = (
                f"Session B post-build code review"
                f"{f' (repair {repair_round})' if repair_round else ''}:")
            self._emit_agent(context.on_progress, {
                "session": "answerer",
                "event": {
                    "type": "code_review",
                    "text": f"{label}\n" + b_review.strip(),
                    "detail": {"review": b_review.strip(),
                               "repair_round": repair_round},
                },
            })
        if agreement.get("complete"):
            if not repair_round:
                self._emit_agent(context.on_progress, {
                    "session": "system",
                    "event": {
                        "type": "build_agreement",
                        "text": (
                            "Build complete — Sessions A, B and C agree. "
                            "You can Start the app, test and verify it now."
                        ),
                        "detail": agreement,
                    },
                })
        else:
            issues = agreement.get("issues") or []
            self._emit_agent(context.on_progress, {
                "session": "answerer",
                "event": {
                    "type": "build_agreement",
                    "text": (
                        "Build NOT yet agreed complete"
                        + (f" (repair {repair_round})" if repair_round else "")
                        + ". Issues to address:\n"
                        + "\n".join(f"  - {i}" for i in issues)
                    ),
                    "detail": agreement,
                },
            })
            if issues and not self._stop_reason:
                self._stop_reason = "sessions disagree — issues remain"
        return {
            "agreement": agreement,
            "current": final_files,
            "best": state.best,
            "best_cov": state.best_cov,
            "meters_ok": meters_ok,
            "runnable_ok": runnable_ok,
        }

    def _forward_nudge(self, verdict, best_cov: dict[str, Any], *,
                       premature_done: bool = False) -> str:
        """Build the forward instruction sent after a round (only when warranted)."""
        text = [
            "PATCH the existing app in place — do NOT recreate the project, "
            "re-scaffold folders, or rewrite working files. Fix the ROOT CAUSE "
            "of each issue; no bypasses or workarounds.",
            "",
            verdict.feedback_text(),
            "",
        ]
        # Runnability first: if the continuous code gate failed this round, lead
        # with the exact compile/import errors so Session A fixes them NOW — this
        # is the timely, token-free channel that always reaches A, independent of
        # the (token-bounded) Session C consults.
        pf = getattr(self, "_round_preflight", None)
        if pf is not None and not pf.ok:
            text.append(
                "CODE GATE FAILED — the app does not compile/import and would "
                "crash on launch. Fix these FIRST (then continue):")
            text += [f"  - {issue}" for issue in pf.issues()[:8]]
            text.append("")
        if pf is not None and pf.boot and not pf.boot.get("ok", True):
            text.append(
                "BOOT CHECK FAILED — TestClient ran startup/lifespan and the "
                "app was not healthy. Keep the app launchable and fix this:")
            if pf.boot.get("error"):
                text.append(f"  - {pf.boot['error']}")
            elif pf.boot.get("health_status"):
                text.append(
                    f"  - GET /health returned HTTP {pf.boot['health_status']}")
            text.append("")
        if premature_done:
            text.append("You reported completion, but the requirements below are "
                        "NOT yet satisfied. Address them, then reply DONE.")
        else:
            text.append("CONTINUE BUILDING toward a complete, runnable app.")
        gaps = list(best_cov.get("gaps", []))
        if gaps:
            text.append("Close these remaining requirements next (add the API "
                        "route, the UI page and a test for each):")
            text += [f"  - {g}" for g in gaps[:10]]
        else:
            text.append("Harden the app: improve tests, error handling and the "
                        "user-facing pages.")
        text.append("Make the changes directly in the workspace now; do not "
                    "stop to ask unless absolutely required.")
        text.append("When the app is complete and every requirement is met, "
                    "reply with a single line: DONE.")
        return "\n".join(text)

    @staticmethod
    def _read_workspace(workspace: Path) -> dict[str, str]:
        from ai_assistant.app_builder.commit_gate import read_workspace_files
        return read_workspace_files(workspace)

    def _agentic_baseline(self, req) -> dict[str, str]:
        """Skeleton-only seed for agentic builds — NO pre-built page.

        Ships just enough for the platform to install/open the workspace
        (``requirements.txt`` + the package marker). Session A authors the real
        skeleton and a requirement-specific launchable ``src/app.py`` itself.
        """
        from ai_assistant.app_builder.agent import spec_for
        from ai_assistant.app_builder.webapp import minimal_scratch_stub

        seed = dict(minimal_scratch_stub(spec_for(req)))
        # Drop the generic landing page — Session A builds a requirement-specific
        # one as the first thing it does.
        seed.pop("src/app.py", None)
        return seed

    def _reconcile_with_workspace(
        self, workspace: Path, best_files: dict[str, str],
    ) -> dict[str, str]:
        """Merge the live workspace into the tracked set so nothing is dropped.

        The per-round gate only adopts files it saw change in a turn diff; files
        Session A wrote outside that window (e.g. on the prime turn) live on disk
        but are missing from ``best_files``. Reading the workspace back guarantees
        the shipped/served app is exactly what is on disk.
        """
        disk = self._read_workspace(workspace)
        if not disk:
            return best_files
        merged = dict(best_files)
        merged.update(disk)
        return merged

    def _emit_baseline_ready(
        self, on_progress: Optional[ProgressFn], workspace: Path,
    ) -> None:
        """Signal Start app / Open folder once — idempotent across the build."""
        if getattr(self, "_baseline_ready_emitted", False):
            return
        self._baseline_ready_emitted = True
        self._emit_agent(on_progress, {
            "session": "system",
            "event": {"type": "baseline_ready",
                      "text": str(workspace),
                      "detail": {"workspace": str(workspace)}},
        })

    @staticmethod
    def _agent_events_have_error(events: list[Any]) -> bool:
        return any(getattr(getattr(ev, "type", None), "value", "") == "error"
                   or getattr(ev, "type", None) == "error" for ev in events)

    @staticmethod
    def _emit_agent(on_progress: Optional[ProgressFn], payload: dict) -> None:
        if on_progress is not None:
            try:
                on_progress({"agent_event": payload})
            except Exception:
                pass

    def _emit_session_status(
        self, on_progress: Optional[ProgressFn], session: str, text: str
    ) -> None:
        """Surface what a specific session (B/C) is doing — or that it waits.

        Routed to that session's own panel so Session B and Session C are never
        blank: they show their current activity (framing/planning/validating) or
        an explicit waiting state instead of an empty box.
        """
        self._emit_agent(on_progress, {
            "session": session,
            "event": {"type": "session_status", "text": text},
        })

    # ── internals ──────────────────────────────────────────────────────────--
    def _request(self, blueprint: AppBlueprint, schema) -> AgentRequest:
        packet = self.engine.agent_metadata_packet(blueprint)
        req = AgentRequest(
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
        if blueprint.mode == BuildMode.FROM_DATABASE:
            req.schema = schema or {}
        return req

    def _evaluate(self, blueprint, files: dict[str, str]):
        sample = files.get("src/app.py", "") or next(
            (c for p, c in files.items() if p.endswith(".py")), ""
        )
        verdict = self.engine.evaluate_build(
            blueprint, sorted(files.keys()), sample_code=sample
        )
        # Deterministic compile gate (every round): a build that does not even
        # compile can never be accepted — surface the syntax errors so the
        # builder fixes them next, exactly as Session C would demand.
        from ai_assistant.app_builder import preflight

        syntax_errors = preflight.compile_check(files=files)
        if syntax_errors:
            verdict.issues = list(verdict.issues) + [
                f"syntax error: {e}" for e in syntax_errors[:8]
            ]
            verdict.accepted = False
            verdict.measurements = {
                **dict(verdict.measurements or {}),
                "syntax_errors": list(syntax_errors),
            }
        return verdict

    def _coverage(self, files: dict[str, str]) -> dict[str, Any]:
        """Completion signal driving the loop: did we build what was requested?

        For storefront/from_database this is archetype/entity recall (the right
        signal there). For a generic from-scratch app it is *blended* with
        requirement fidelity, so the loop optimizes building the real app the
        user described — not a CRUD scaffold over guessed tables.
        """
        base = self.meters.evaluate_requirements(
            entities=self._req_entities,
            features=self._req_features,
            files=files,
            services=self._req_services,
            kind=getattr(self, "_kind", "crud"),
            threshold=self.target_coverage,
        )
        if not getattr(self, "_coverage_fidelity_active", False):
            return base
        if not self._description.strip():
            # Runnable stub only — no user intent to score fidelity against yet.
            return base
        fid = self._fidelity(files)
        score = min(base.get("score", 1.0), fid.get("score", 1.0))
        gaps = list(base.get("gaps", []))
        gaps += [f"requirement not yet reflected: {tok}"
                 for tok in fid.get("missing", [])]
        return {**base, "score": score, "gaps": gaps,
                "fully_covered": score >= self.target_coverage}

    def _fidelity(self, files: dict[str, str]) -> dict[str, Any]:
        """Fidelity of the *application* to the requirement (excludes docs/README,
        so echoing the description in a README cannot fake a faithful build)."""
        app_files = {p: c for p, c in files.items()
                     if not (p.lower().endswith(".md") or p.startswith("docs/"))}
        return self.meters.evaluate_fidelity(
            description=self._description, files=app_files,
            entities=self._req_entities,
        )

    def _done(self, verdict, cov: dict[str, Any]) -> bool:
        """The build is finished when it is structurally sound AND complete."""
        return bool(verdict.accepted and cov["score"] >= self.target_coverage)

    @staticmethod
    def _better(verdict, cov, best, best_cov, n_cand: int, n_best: int) -> bool:
        """Prefer more requirement coverage, then acceptance, size, then quality.

        Coverage (did we build what was asked?) dominates so the loop keeps
        closing requirement gaps before chasing marginal structural gains. Among
        equally-covered, equally-accepted builds we keep additional safe files
        (so useful AI contributions are retained even when the structural score
        is flat), with build quality as the final tie-breaker.
        """
        cand_key = (round(cov["score"], 6), 1 if verdict.accepted else 0,
                    n_cand, round(verdict.score, 6))
        best_key = (round(best_cov["score"], 6), 1 if best.accepted else 0,
                    n_best, round(best.score, 6))
        return cand_key > best_key

    def _gate_files(self, produced) -> tuple[dict[str, str], list[str]]:
        """Managers gate every AI file: only safe, high-quality files survive."""
        accepted: dict[str, str] = {}
        rejected: list[str] = []
        for gf in produced:
            path = gf.path
            if path.endswith(".py"):
                low = (gf.content or "").lower()
                if "from flask" in low or "import flask" in low or "flask(" in low:
                    rejected.append(path)
                    continue
                verdict = self.meters.evaluate_code_artifact(gf.content)
                if not verdict["accepted"]:
                    rejected.append(path)
                    continue
            accepted[path] = gf.content
        return accepted, rejected

    def _ensure_required(self, blueprint, req, files: dict[str, str]) -> dict[str, str]:
        missing = [f for f in self.engine.expected_manifest(blueprint) if f not in files]
        baseline = {f.path: f.content for f in self._baseline.generate(req).files}
        for rel in missing:
            files[rel] = baseline.get(rel) or _placeholder_for(rel, blueprint.name)
        if blueprint.mode == BuildMode.FROM_DATABASE:
            from ai_assistant.app_builder.workspace_contract import reconcile_file_map

            files = reconcile_file_map(files, baseline)
        return files

    def _reconcile_data_layer_workspace(
        self, workspace: Path, blueprint, req, on_progress=None,
    ) -> None:
        """Repair generated DB-runtime drift after direct agent writes."""
        if blueprint.mode != BuildMode.FROM_DATABASE:
            return
        from ai_assistant.app_builder.workspace_contract import reconcile_data_layer

        baseline = {f.path: f.content for f in self._baseline.generate(req).files}
        report = reconcile_data_layer(workspace, baseline)
        if report.changed:
            self._emit_agent(on_progress, {
                "session": "gate",
                "event": {
                    "type": "validation",
                    "text": "data-layer contract repaired — protected SQLite runtime restored",
                    "detail": {"clean": True, **report.as_dict()},
                },
            })

    def _maybe_stub_launch_fallback(
        self, workspace: Path, blueprint, on_progress=None,
    ) -> None:
        """Last resort: minimal launch page when all repairs still cannot boot."""
        if not (workspace / "src" / "app.py").exists():
            return
        from ai_assistant.app_builder import preflight

        boot = preflight.boot_check(workspace, timeout=min(self.timeout, 60))
        if boot.ok:
            return
        app_path = workspace / "src" / "app.py"
        app_path.parent.mkdir(parents=True, exist_ok=True)
        app_path.write_text(
            self._minimal_launch_app(blueprint.name, boot.error),
            encoding="utf-8",
        )
        self._emit_agent(on_progress, {
            "session": "gate",
            "event": {
                "type": "validation",
                "text": (
                    "stubbed launch page — app not functional; repairs exhausted. "
                    "Start app opens a degraded placeholder only."),
                "detail": {"clean": False, "stubbed": True, **boot.as_dict()},
            },
        })

    @staticmethod
    def _minimal_launch_app(app_name: str, reason: str = "") -> str:
        return (
            '"""Minimal launch fallback kept by App Builder."""\n'
            "from __future__ import annotations\n\n"
            "from fastapi import FastAPI\n"
            "from fastapi.responses import HTMLResponse\n\n\n"
            "app = FastAPI(title=" + repr(app_name) + ")\n\n\n"
            '@app.get("/health")\n'
            "def health() -> dict:\n"
            '    """Return liveness while the generated app is being repaired."""\n'
            "    return {\"status\": \"degraded\", \"service\": "
            + repr(app_name)
            + ", \"reason\": "
            + repr((reason or "app is being repaired")[:200])
            + "}\n\n\n"
            '@app.get("/", response_class=HTMLResponse)\n'
            "def index() -> HTMLResponse:\n"
            '    """Show a minimal page instead of leaving the app unopenable."""\n'
            "    return HTMLResponse(\n"
            "        '<!doctype html><html><head><title>"
            + app_name.replace("'", "")
            + "</title></head><body>'\n"
            "        '<main style=\"font-family:Arial,sans-serif;max-width:760px;margin:48px auto\">'\n"
            "        '<h1>App is available</h1>'\n"
            "        '<p>The generated app entrypoint is being repaired. '\n"
            "        'This fallback keeps the webpage openable while the build continues.</p>'\n"
            "        '</main></body></html>'\n"
            "    )\n"
        )

    def _frame_iteration(self, blueprint, req, files, verdict, cov, phase: str) -> str:
        lines = [
            "You are the AI co-builder for an existing FastAPI + Jinja2 web app,",
            "operating UNDER AiAppEngine governance. Improve the app for this phase.",
            f"PHASE: {phase}",
            f"APP: {blueprint.name}",
            f"CURRENT BUILD SCORE: {round(verdict.score, 4)} "
            f"(accepted={verdict.accepted})",
            f"REQUIREMENT COVERAGE: {round(cov['score'], 4)} "
            f"(target {self.target_coverage})",
        ]
        if getattr(self, "_kind", "crud") == "storefront":
            lines.append(
                "APP PURPOSE: a real customer-facing ECOMMERCE STOREFRONT — "
                "home, searchable product catalog, product detail, shopping "
                "cart, and checkout that places orders. Do NOT turn this into a "
                "CRUD admin that mirrors database tables. Use the DB schema to "
                "inform the catalog and seed realistic sample products.")
        elif blueprint.mode == BuildMode.FROM_SCRATCH and not req.schema:
            lines.append(
                "APP PURPOSE: build the COMPLETE application described in the "
                "REQUIREMENTS below — design the actual pages, flows and business "
                "logic the user needs (not a generic admin). Model ONLY the data "
                "the app requires; do NOT just expose one CRUD table per noun. "
                "Seed realistic sample data so every feature is demonstrable. "
                "YOU choose the folder structure — only keep src.app:app runnable "
                "with GET /health and an openable webpage that reflects progress.")
        elif blueprint.mode == BuildMode.FROM_DATABASE:
            lines.append(
                "APP PURPOSE: infer from the DATABASE UNDERSTANDING + schema what "
                "KIND of real application this data serves (its users, pages, "
                "flows and logic), and build THAT application. Do NOT just expose "
                "CRUD over the tables / mirror the schema. Use the real tables as "
                "the app's data layer; the description (if any) is only a hint to "
                "confirm the app type.")
        if blueprint.description:
            lines.append(f"REQUIREMENTS: {blueprint.description}")
        if self._req_entities:
            lines.append(
                "REQUESTED ENTITIES: " + ", ".join(self._req_entities))
        if self._req_features:
            lines.append(
                "REQUESTED FEATURES (each entity needs all): "
                + ", ".join(self._req_features))
        if req.schema:
            lines.append("DATABASE SCHEMA (use ONLY these tables/columns):")
            for table, cols in req.schema.items():
                lines.append(f"  - {table}({', '.join(cols)})")
        # Ground the build in the AI Query Assistant's understanding of the data.
        if getattr(self, "_insight", None) is not None:
            lines.append(self._insight.prompt_block())
        if cov["gaps"]:
            lines.append(
                "REQUIREMENT GAPS TO CLOSE (each needs an API route, a UI page, "
                "AND a test):")
            lines += [f"  - {g}" for g in cov["gaps"]]
        if verdict.issues:
            lines.append("OPEN ISSUES TO FIX:")
            lines += [f"  - {i}" for i in verdict.issues]
        if blueprint.mode == BuildMode.FROM_SCRATCH:
            lines += [
                "BUILD PROTOCOL (phase-wise, communicate with the App Builder):",
                "  - Design YOUR folder structure for this app — no fixed layout.",
                "  - Build an openable webpage early; keep src.app:app runnable.",
                "  - Build phase-wise: structure → models/db → api → web/ui → tests.",
                "  - When a component is finished emit exactly: PHASE-DONE: <component>",
                "    (e.g. api, db, web, models, tests).",
                "  - When you need user input emit: ASK: / CONFIRM: / APPROVE: "
                "<your question> — only for genuine decisions.",
                "",
                "EXISTING FILES:",
                "  " + ", ".join(sorted(files)[:60]),
                "HARD CONSTRAINTS (safety only — not layout):",
                "  - use FastAPI + Jinja2 only; do NOT use Flask/Django/WSGI",
                "  - keep src.app:app importable; expose GET /health",
                "  - keep it lightweight and dependency-light",
                "  - parameterized SQL only; no bare except; docstrings on public defs",
                "  - the app must keep starting as you add features",
                "",
                "Return ONLY changed or new files in this exact contract:",
                "=== FILE: relative/path ===",
                "<full file content>",
                "=== END FILE ===",
            ]
        else:
            lines += [
                "BUILD PROTOCOL (phase-wise, communicate with the App Builder):",
                "  - FIRST write/refresh docs/requirement.txt from the BUILD BRIEF as "
                "your recursive acceptance reference.",
                "  - Build phase-wise: skeleton → models/db → api → web/ui → tests.",
                "  - When a component is finished emit exactly: PHASE-DONE: <component>",
                "    (e.g. api, db, web, models, tests, skeleton).",
                "  - When you need user input emit: ASK: / CONFIRM: / APPROVE: "
                "<your question> — only for genuine decisions.",
                "",
                "EXISTING FILES:",
                "  " + ", ".join(sorted(files)[:60]),
                "HARD CONSTRAINTS (violations are rejected by code):",
                "  - use FastAPI + Jinja2 only; do NOT use Flask/Django/WSGI",
                "  - src/app.py must expose an ASGI FastAPI object named app",
                "  - do NOT create src/db.py; the data layer is the existing "
                "src/db/ package",
                "  - use src.db.connection.get_connection() and APP_DB_PATH only; "
                "do NOT introduce DBASSIST_DB_PATH or db.init_db()",
                "  - keep it lightweight and dependency-light",
                "  - parameterized SQL only; no bare except; docstrings on public defs",
                "  - the app must keep starting and its tests must keep passing",
                "  - reuse the existing structure (src/app.py, src/api.py, src/web.py, "
                "templates/, src/repository.py)",
                "",
                "Return ONLY changed or new files in this exact contract:",
                "=== FILE: relative/path ===",
                "<full file content>",
                "=== END FILE ===",
            ]
        return "\n".join(lines)

    def _data_understanding_verdict(self) -> dict[str, Any]:
        """Score how well the DB was understood (1.0 / N/A when not applicable)."""
        if self._insight is None:
            return {"score": 1.0, "issues": []}
        return self.meters.evaluate_data_understanding(self._insight.as_dict())

    def _mode_specific_quality(
        self,
        blueprint: Any,
        files: dict[str, str],
        schema: dict | None,
    ) -> dict[str, Any]:
        """Run DB- or codebase-specific meter batteries."""
        from ai_assistant.app_builder.engine import BuildMode
        from ai_assistant.app_builder.meters.registry import AppMeterRegistry

        reg = AppMeterRegistry()
        if blueprint.mode == BuildMode.FROM_DATABASE:
            profile = None
            archetype = ""
            insight = None
            if self._insight is not None:
                prof = getattr(self._insight, "profile", None)
                profile = prof.as_dict() if prof is not None else None
                archetype = getattr(self._insight, "archetype", "") or ""
                insight = self._insight.as_dict()
            return reg.evaluate_db_build(
                files, profile=profile, schema=schema or {},
                archetype=archetype, insight=insight,
            )
        if blueprint.mode == BuildMode.FROM_CODEBASE:
            facts = {}
            if getattr(self, "_codebase_profile", None):
                facts = self._codebase_profile
            return reg.evaluate_codebase_build(
                files, profile=facts,
                components=list(getattr(self, "_codebase_components", []) or []))
        return {"overall": 1.0, "meters": {}, "failing": [], "passed": True}

    @staticmethod
    def _has_sample_data(files: dict[str, str]) -> bool:
        """True when the build seeds sample data or its tests create/use data."""
        for path, content in files.items():
            low = (content or "")
            if path.startswith("tests/") and ("client.post" in low
                                              or "INSERT INTO" in low.upper()):
                return True
            if path.endswith(".py") and ("def seed(" in low or "SAMPLE_" in low
                                         or "executemany(" in low):
                return True
        return False

    def _journal(self, blueprint, files: dict[str, str],
                 test_outcome: Optional[dict]) -> dict[str, Any]:
        """Record the build process so process_adherence_meter can score it."""
        journal: dict[str, Any] = {
            "mode": blueprint.mode.value,
            "channels": sorted(self._channels),
            "connection": bool(blueprint.connections),
            "sample_data_created": self._has_sample_data(files),
            "tests_run": bool(test_outcome is not None),
            "tests_passed": bool(test_outcome and test_outcome.get("passed")),
            "verified_with_data": (
                self._insight is not None and bool(self._insight.tables)
            ),
            "schema_deployed": bool(self._schema_deployed),
        }
        if test_outcome is not None:
            journal["test_summary"] = test_outcome.get("summary", "")
        return journal

    def _maybe_deploy_schema(self, blueprint, files: dict[str, str],
                             deploy_schema: bool, db_manager: Any) -> dict[str, Any]:
        """Deploy the app's tables to the connection — only when truly requested.

        Guards (all must hold): build is FROM_SCRATCH, a connection/db_manager is
        present, the user ticked deploy, AND (in interactive mode) confirmed it.
        Deployment is additive (CREATE TABLE IF NOT EXISTS) and never destructive.
        """
        if not deploy_schema or blueprint.mode != BuildMode.FROM_SCRATCH:
            return {"deployed": False, "errors": [], "statements": 0, "executed": 0}
        if db_manager is None:
            return {"deployed": False, "errors": ["no connection selected"],
                    "statements": 0, "executed": 0}
        approved = self._decider.approved(BuildDecision(
            id="confirm_deploy_schema",
            question=(
                f"Deploy this app's tables to connection "
                f"'{(blueprint.connections or ['?'])[0]}'? "
                "(additive CREATE TABLE IF NOT EXISTS only)"
            ),
            critical=True,
            default=True,
        ))
        if not approved:
            return {"deployed": False, "errors": ["deployment declined"],
                    "statements": 0, "executed": 0}
        report = _deploy_schema(db_manager, files)
        self._schema_deployed = bool(report.get("deployed"))
        if self._schema_deployed:
            self._channels.add("schema_deploy")
        return report

    def _run_tests(
        self,
        workspace: Path,
        *,
        paths: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Run pytest — whole suite or targeted folders for a component.

        This is how the App Builder "tests the data": the generated suite exercises
        every functionality against seeded/sample data. Failures are surfaced (and
        feed process_adherence) rather than silently ignored.
        """
        import subprocess
        import sys

        # Tests may live anywhere the builder/validator chose to put them — we do
        # NOT require the pre-decided tests/ layout. pytest discovers every
        # test_*.py from the workspace root, so self-generated test files (in any
        # folder) are picked up automatically.
        def _has_tests() -> bool:
            for p in workspace.rglob("test_*.py"):
                if not ({".venv", "venv", "__pycache__", "node_modules"}
                        & set(p.parts)):
                    return True
            return (workspace / "tests").is_dir()

        if not _has_tests():
            return {"passed": False, "summary": "no test files found"}
        from ai_assistant.app_builder import preflight

        env = preflight.test_db_env(workspace)
        args = [sys.executable, "-m", "pytest", "-q"]
        if paths:
            existing = [p for p in paths if (workspace / p).exists()]
            if existing:
                args.extend(existing)
        try:
            proc = subprocess.run(
                args,
                cwd=str(workspace), capture_output=True, text=True,
                timeout=self.timeout, env=env,
            )
        except Exception as exc:  # noqa: BLE001
            return {"passed": False, "summary": f"test run error: {exc}"}
        tail = (proc.stdout or "").strip().splitlines()
        summary = tail[-1] if tail else ""
        return {
            "passed": proc.returncode == 0, "summary": summary,
            "returncode": proc.returncode,
            "paths": list(paths or []),
        }

    def _publish_structure(self, workspace: Path, on_progress) -> None:
        """Emit Session A's scaffold so B and C share the same real structure."""
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        structure = self._workspace_structure(workspace)
        if not structure:
            return
        self._structure_published = True
        self._emit_agent(on_progress, {
            "session": "system",
            "event": {
                "type": "structure_published",
                "text": ("Session A scaffold published to B and C. Validator "
                         f"owns {VALIDATOR_TEST_DIR}/ (A/B must not use it)."),
                "detail": {"structure": structure,
                           "validator_dir": VALIDATOR_TEST_DIR},
            },
        })

    # ── Session C: validator-owned, sandboxed test authoring ─────────────────
    @staticmethod
    def _workspace_structure(workspace: Path, *, max_entries: int = 120) -> str:
        """A compact tree of the builder's real folders/files (for B and C)."""
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        skip = {".venv", "venv", "__pycache__", "node_modules", ".git",
                ".pytest_cache", VALIDATOR_TEST_DIR}
        rels: list[str] = []
        if workspace.is_dir():
            for p in sorted(workspace.rglob("*")):
                if skip & set(p.parts):
                    continue
                if p.is_file():
                    rels.append(str(p.relative_to(workspace)))
                if len(rels) >= max_entries:
                    break
        return "\n".join(f"  {r}" for r in rels)

    @staticmethod
    def _public_symbols(workspace: Path, *, max_files: int = 40) -> str:
        """Class/function names per src module (so C's tests use real names)."""
        import ast

        src = workspace / "src"
        if not src.is_dir():
            return ""
        out: list[str] = []
        for path in sorted(src.rglob("*.py"))[:max_files]:
            if "__pycache__" in path.parts or path.name == "__init__.py":
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (OSError, SyntaxError):
                continue
            names: list[str] = []
            for node in tree.body:
                if isinstance(node, ast.ClassDef):
                    methods = [n.name for n in node.body
                               if isinstance(n, (ast.FunctionDef,
                                                 ast.AsyncFunctionDef))][:8]
                    names.append(f"class {node.name}({', '.join(methods)})"
                                 if methods else f"class {node.name}")
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    names.append(f"def {node.name}()")
            if names:
                mod = ".".join(path.relative_to(workspace).with_suffix("").parts)
                out.append(f"{mod}: " + "; ".join(names[:12]))
        return "\n".join(out)

    def _enforce_validator_sandbox(
        self, workspace: Path, before: dict[str, str],
    ) -> list[str]:
        """Revert any change the validator made OUTSIDE its own folder.

        Session C is only allowed to write under ``VALIDATOR_TEST_DIR``. This is
        the deterministic guarantee behind that promise: we diff the workspace
        against the pre-authoring snapshot and restore (or delete) every changed
        path that is not inside the validator folder, so C can never touch
        Session A's code even if the model strays. Returns the reverted paths.
        """
        from ai_assistant.app_builder.commit_gate import snapshot_workspace
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        after = snapshot_workspace(workspace)
        reverted: list[str] = []
        for rel in set(after) | set(before):
            top = rel.replace("\\", "/").split("/", 1)[0]
            if top == VALIDATOR_TEST_DIR:
                continue
            if before.get(rel) != after.get(rel):
                path = workspace / rel
                if rel in before:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(before[rel], encoding="utf-8")
                else:
                    path.unlink(missing_ok=True)
                reverted.append(rel)
        return reverted

    def _validator_author_tests(
        self, workspace: Path, coord, brief, on_progress,
    ) -> None:
        """Session C authors its own tests (READ-ONLY); the orchestrator writes.

        C never gets a write-capable session, so it can never touch Session A's
        code and its planning/similarity turns stay read-only. C emits the test
        files as text; this method writes them — and ONLY inside the validator
        folder. Non-blocking to A: runs after A's turn, never reverts A's work.
        The written tests are then picked up by the deterministic test runner.
        """
        from ai_assistant.app_builder.build_session import (
            parse_validator_test_files)
        from ai_assistant.app_builder.commit_gate import snapshot_workspace
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        validator = getattr(coord, "validator", None)
        if validator is None or not hasattr(validator, "author_tests"):
            return
        if not (workspace / "src" / "app.py").exists():
            return  # nothing to test against yet
        self._emit_session_status(
            on_progress, "validator",
            f"authoring its own test cases for {VALIDATOR_TEST_DIR}/ "
            "(read-only) from Session A's real structure and symbols…")
        before = snapshot_workspace(workspace)
        structure = self._workspace_structure(workspace)
        symbols = self._public_symbols(workspace)
        try:
            authored_text = validator.author_tests(
                brief=brief, structure=structure, symbols=symbols)
        except Exception as exc:  # noqa: BLE001
            self._emit_agent(on_progress, {
                "session": "validator",
                "event": {"type": "validation",
                          "text": f"could not author tests: {exc}"},
            })
            return
        files = parse_validator_test_files(
            authored_text or "", folder=VALIDATOR_TEST_DIR)
        written = self._write_validator_tests(workspace, files)
        # Ordered gate 1 — SYNTAX: byte-compile each freshly written test file and
        # drop any that does not parse, so a malformed file can never break pytest
        # collection for the whole folder (keeps the run clean in any state).
        broken = self._syntax_check_files(workspace, written)
        written = [w for w in written if w not in broken]
        # Backstop only — C is read-only and never wrote anything itself, so this
        # should always be empty; it guards against any unexpected disk change.
        reverted = self._enforce_validator_sandbox(workspace, before)
        if written:
            self._validator_test_authors += 1
        # Ordered gate 2 — RUN: execute ONLY the validator folder, in isolation,
        # against whatever is implemented right now. Non-blocking to A (this never
        # gates the build); the result feeds the evidence digest so C/B can reason
        # about implemented-vs-pending. Skipped tests (not-yet-implemented) keep
        # this green and auto-activate on later refreshes.
        ran = None
        if (workspace / VALIDATOR_TEST_DIR).is_dir() and any(
                (workspace / VALIDATOR_TEST_DIR).rglob("test_*.py")):
            ran = self._run_tests(workspace, paths=[VALIDATOR_TEST_DIR])
            self._validator_test_outcome = ran
        # ONE consolidated status (low noise): wrote / dropped / ran summary.
        if written:
            msg = f"wrote {len(written)} validator test file(s) to {VALIDATOR_TEST_DIR}/"
        else:
            msg = "no validator test files produced this round"
        if broken:
            msg += f"; dropped {len(broken)} with syntax errors"
        if ran is not None:
            msg += (f"; pytest {VALIDATOR_TEST_DIR}/: "
                    + (ran.get("summary") or
                       ("passed" if ran.get("passed") else "failed")))
        self._emit_agent(on_progress, {
            "session": "validator",
            "event": {"type": "validation", "text": msg,
                      "detail": {"authored": written, "dropped": broken,
                                 "reverted": reverted,
                                 "validator_tests": ran}},
        })

    def _syntax_check_files(
        self, workspace: Path, rels: list[str]
    ) -> list[str]:
        """Byte-compile *rels*; delete and return any file that fails to parse.

        Runs BEFORE the validator suite executes so a single malformed test file
        can never abort collection for the entire folder. Returns the relative
        paths that were dropped (so they are not counted as authored).
        """
        import py_compile

        broken: list[str] = []
        for rel in rels:
            path = workspace / rel
            try:
                py_compile.compile(str(path), doraise=True)
            except (py_compile.PyCompileError, SyntaxError, OSError):
                broken.append(rel)
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
        return broken

    def _write_validator_tests(
        self, workspace: Path, files: list[tuple[str, str]],
    ) -> list[str]:
        """Write C's authored test files — strictly inside the validator folder.

        The orchestrator is the ONLY writer for Session C. Each target path is
        re-validated against the resolved validator folder so a crafted path can
        never escape the sandbox (defence in depth over the parser's checks).
        """
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        folder_root = (workspace / VALIDATOR_TEST_DIR).resolve()
        written: list[str] = []
        for rel, content in files:
            path = workspace / rel
            try:
                resolved = path.resolve()
                resolved.relative_to(folder_root)
            except (ValueError, OSError):
                continue  # escapes the sandbox — refuse
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            written.append(rel.replace("\\", "/"))
        return written

    def _round_preflight_check(self, workspace: Path):
        """Quick compile + import dry-run for the continuous (per-round) gate.

        Returns a ``PreflightResult`` (or ``None`` when there is no launchable
        ``src/app.py`` yet — e.g. early rounds or analysis artifacts). Cheap by
        design (compile + one import subprocess) so it can run every round.
        """
        from ai_assistant.app_builder import preflight

        if not (workspace / "src" / "app.py").exists():
            return None
        try:
            return preflight.dry_run(workspace, timeout=self.timeout, quick=True)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _components_touched(
        files_changed: list[str],
        phase_done: Optional[list[str]] = None,
    ) -> list[str]:
        """Map changed files / PHASE-DONE markers to logical components."""
        components: list[str] = []
        seen: set[str] = set()

        def _add(comp: str) -> None:
            c = comp.strip().lower().replace("/", "_")
            if c and c not in seen:
                seen.add(c)
                components.append(c)

        for comp in phase_done or []:
            _add(comp)
        for path in files_changed:
            p = path.replace("\\", "/").lower()
            name = p.rsplit("/", 1)[-1]
            if (p.startswith("src/api") or "/api/" in p
                    or "route" in name or "endpoint" in name):
                _add("api")
            elif (any(x in p for x in (
                    "src/db", "src/models", "src/repository", "/db/", "/models/",
                    "schema", "migration"))
                    or p.startswith("models/") or "model" in name):
                _add("db")
            elif any(x in p for x in (
                    "templates/", "static/", "src/web", ".html", ".css", ".js")):
                _add("web")
            elif p.startswith("tests/") or name.startswith("test_"):
                parts = p.split("/")
                if len(parts) >= 2 and parts[1] in TEST_TAXONOMY:
                    _add(parts[1])
                else:
                    _add("tests")
            elif p.startswith("docs/"):
                _add("skeleton")
            elif p.startswith("src/"):
                _add("unit_test")
        if not components:
            _add("unit_test")
        return components

    @staticmethod
    def _test_paths_for_components(
        workspace: Path,
        components: list[str],
    ) -> list[str]:
        """Resolve component names to existing test folders under tests/."""
        mapping = {
            "api": ["tests/api"],
            "db": ["tests/db"],
            "models": ["tests/db", "tests/unit_test"],
            "repository": ["tests/db"],
            "web": ["tests/functionality", "tests/connectivity"],
            "ui": ["tests/functionality"],
            "skeleton": ["tests/connectivity", "tests/unit_test"],
            "tests": ["tests/unit_test"],
            "unit_test": ["tests/unit_test"],
            "functionality": ["tests/functionality"],
            "connectivity": ["tests/connectivity"],
        }
        paths: list[str] = []
        seen: set[str] = set()
        for comp in components:
            for rel in mapping.get(comp, [f"tests/{comp}"]):
                if rel in seen:
                    continue
                if (workspace / rel).exists():
                    seen.add(rel)
                    paths.append(rel)
        return paths or ["tests/unit_test"]

    def _test_scope_for_component(self, component: str) -> str:
        base = self._test_scope()
        plan = getattr(getattr(self, "_coord", None), "_test_plan", "") or ""
        if plan:
            return f"component: {component}; {base}; test plan: {plan[:400]}"
        return f"component: {component}; {base}"

    @staticmethod
    def _how_to_test_for_component(component: str) -> str:
        hints = {
            "api": "run tests/api against sample data; verify each endpoint contract.",
            "db": "run tests/db; verify schema, queries and read-only probes.",
            "web": "run tests/functionality + tests/connectivity; verify pages boot.",
            "skeleton": "run tests/connectivity; verify app boots and docs exist.",
        }
        specific = hints.get(
            component,
            f"run tests/{component} or tests/unit_test for this component.",
        )
        return specific + " Use tests/test_sample_data/sample_data.py for fixtures."

    def _validation_digest(
        self,
        files: dict[str, str],
        verdict,
        cov: dict,
        test_outcome: Optional[dict],
        *,
        component: str = "",
        test_paths: Optional[list[str]] = None,
    ) -> str:
        """Compact, code-computed evidence for Session C (no tokens spent here)."""
        lines = [
            f"component: {component or 'general'}",
            f"build score: {verdict.score:.2f} (accepted={verdict.accepted})",
            f"requirement coverage: {cov['score']:.2f}",
            f"files: {len(files)}",
            f"sample data present: {self._has_sample_data(files)}",
        ]
        if test_paths:
            lines.append("targeted tests: " + ", ".join(test_paths))
        if cov.get("gaps"):
            lines.append("missing requirements: " + "; ".join(cov["gaps"][:8]))
        if verdict.issues:
            lines.append("meter issues: " + "; ".join(list(verdict.issues)[:6]))
        if test_outcome is not None:
            status = "PASSED" if test_outcome.get("passed") else "FAILED"
            lines.append(f"tests: {status} — {test_outcome.get('summary', '')}")
        try:
            aq = self.meters.evaluate_app_quality(
                dict(files), description=self._description)
            checks = aq.get("checks") or {}
            passed = sum(1 for v in checks.values() if v)
            total = len(checks) or 1
            lines.append(
                f"app quality: {aq.get('score', 0):.2f} "
                f"({passed}/{total} standard checks passed)")
            for issue in (aq.get("issues") or [])[:4]:
                lines.append(f"  quality note: {issue}")
        except Exception:  # noqa: BLE001
            pass
        probe = self._db_probe()
        if probe:
            lines.append(probe)
        return "\n".join(lines)

    def _test_scope(self) -> str:
        ents = ", ".join(self._req_entities[:8]) or "n/a"
        feats = ", ".join(self._req_features[:6]) or "list/create/edit/delete"
        return f"entities: {ents}; features: {feats}"

    @staticmethod
    def _how_to_test() -> str:
        return ("run the app's pytest suite against seeded sample data; exercise "
                "each entity's CRUD and the key flows; confirm API + UI respond.")

    def _db_probe(self) -> str:
        """Read-only evidence from a live connection (reuses safe understanding).

        Never mutates user data — it only reports what the read-only DB
        understanding already gathered, so Session C can reason about real data
        without us issuing any writes.
        """
        insight = getattr(self, "_insight", None)
        if insight is None or not getattr(insight, "tables", None):
            return ""
        tables = list(insight.tables)[:8]
        return ("live DB (read-only): connected; sample tables observed — "
                + ", ".join(getattr(t, "name", str(t)) for t in tables))

    def _write(
        self, workspace: Path, files: dict[str, str], *, overwrite: bool = True,
    ) -> None:
        """Write files atomically per entry.

        Each file is staged to a temporary sibling and then ``os.replace``d
        into place so a disk/permission/encoding error mid-write can never
        leave a half-written destination file.
        """
        import os
        import tempfile

        for rel, content in files.items():
            path = workspace / rel
            if not overwrite and path.exists():
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(content)
                os.replace(tmp, path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise

    @staticmethod
    def _emit(on_progress: Optional[ProgressFn], rnd: BuildRound) -> None:
        if on_progress is not None:
            try:
                on_progress(rnd.as_dict())
            except Exception:
                pass
