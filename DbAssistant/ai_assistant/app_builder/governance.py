"""Governance brief pushed to App Builder AI sessions at start."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from ai_assistant.app_builder.engine import (
    TEST_TAXONOMY,
    VALIDATOR_TEST_DIR,
    AiAppEngine,
    AppBlueprint,
    BuildMode,
)


@dataclass
class GovernanceBrief:
    """Centralized infra, validation rules, and meter rubric for AI agents."""

    blueprint: AppBlueprint
    engine: AiAppEngine
    connection_name: str = ""
    schema_summary: str = ""
    data_insight: str = ""
    target_score: float = 0.9
    target_coverage: float = 0.9
    extra: dict[str, Any] = field(default_factory=dict)

    _TEST_PURPOSE = {
        "unit_test": "isolated unit tests",
        "full_test": "end-to-end / full-suite tests",
        "write_test_cases": "scratch space for new/ad-hoc cases",
        "connectivity": "app boots + routes/infra reachable",
        "db": "schema, queries, read-only DB probes",
        "api": "per-endpoint API contract tests",
        "functionality": "feature/behavior tests per requirement",
        "test_sample_data": "sample data fixtures for the tests above",
    }

    #: The functional bar every generated app must clear — a usable product, not
    #: a mockup. Injected into the builder's brief and checked by B (reviewer) and
    #: C (validator). Kept here as the single source so all three roles agree.
    _FUNCTIONAL_CONTRACT = (
        "PRODUCTION-FUNCTIONAL CONTRACT (this app must be genuinely usable in a "
        "production system — NOT a mockup):",
        "  - Every feature/flow MUST work END TO END: forms submit and persist, "
        "lists reflect saved data, edit/delete take effect, navigation links "
        "resolve. No dead buttons, placeholder/'coming soon' pages, stub 'TODO' "
        "handlers, or hard-coded fake output.",
        "  - Wire every component through real code paths "
        "(UI -> API -> service -> repository -> DB). A screen that merely LOOKS "
        "right but does nothing is a FAILURE.",
        "  - Deliver RICH UX: clear navigation across all pages, empty / loading / "
        "error states, input validation with user-visible feedback, and a "
        "consistent, polished layout.",
        "  - SAMPLE DATA: if no database is connected, or the connected DB does "
        "not cover every entity/feature, SEED deterministic sample data on "
        "startup (whenever a table is empty) and SHOW it in the UI so every flow "
        "is demonstrable on first launch.",
        "  - The app must start clean and every primary GET route must respond "
        "without a server (5xx) error: the deterministic launch smoke crawls your "
        "routes and any 5xx FAILS the build.",
    )

    @classmethod
    def _test_purpose(cls, d: str) -> str:
        return cls._TEST_PURPOSE.get(d, "tests")

    def render(self, *, role: str = "builder") -> str:
        """Format the governance primer for Session A or B."""
        packet = self.engine.agent_metadata_packet(self.blueprint)
        lines = [
            "=== APP BUILDER GOVERNANCE BRIEF ===",
            f"ROLE: {role}",
            f"ENGINE: {packet['engine']}",
            f"MODE: {packet['mode']}",
            f"APP: {packet['app_name']}",
            f"BUILD PROFILE: {self.blueprint.build_profile}",
            f"VARIANT: {self.blueprint.variant}",
            f"TARGET SCORE: {self.target_score}",
            f"TARGET COVERAGE: {self.target_coverage}",
            "",
            "CENTRALIZED INFRA — managed services this platform OFFERS the app",
            "(an add-on; the app consumes them, it does not reimplement them):",
            f"  services: {', '.join(packet['services']) or 'none'}",
            "  - monitoring: monitoring-as-a-service. The platform polls the "
            "app's health API. The app MUST expose GET /health reporting app "
            "liveness AND database readiness (status: healthy + database: "
            "{status}). Do NOT build a monitoring stack — just expose health.",
            "  - document: the central document store/service for the app's docs.",
            "  - hosting: web hosting is handled by the platform via "
            "deploy/hosting.yaml (health_check: /health). Do not hand-roll deploy.",
        ]
        if self.connection_name:
            lines.append(f"  database connection: {self.connection_name}")
        if self.schema_summary:
            lines.append(f"  schema: {self.schema_summary}")
        if self.data_insight:
            lines.append(f"  data understanding: {self.data_insight}")
        if self.blueprint.mode == BuildMode.FROM_SCRATCH:
            lines += [
                "",
                "RUNNABLE CONTRACT (the only required structure):",
            ]
            for f in packet["required_files"]:
                lines.append(f"  - {f}")
            lines += [
                "",
                "VALIDATION RULES (safety — not layout enforcement):",
            ]
            for rule in packet["rules"]:
                lines.append(f"  - {rule}")
            advisory = packet.get("advisory_layout") or {}
            lines += [
                "",
                "STRUCTURE FREEDOM — you design whatever folders/files THIS app",
                "needs. The taxonomy below is an OPTIONAL metadata lens for the",
                "App Builder (testing/understanding), NOT a rule:",
            ]
            for folder in advisory.get("suggested_test_folders", [])[:12]:
                lines.append(f"  - {folder} (optional)")
            for doc in advisory.get("suggested_doc_files", []):
                lines.append(f"  - {doc} (optional)")
            lines += [
                "",
                "METER RUBRIC (feedback, not layout enforcement):",
                "  - code_accuracy: syntax, style, no bare except",
                "  - requirement_fidelity: app reflects the user's description",
                "  - requirement_coverage: features demonstrable in code/UI/tests",
                "  - process_adherence: sample data, tests run when applicable",
            ]
        else:
            lines += [
                "",
                "REQUIRED FILES (manifest):",
            ]
            for f in packet["required_files"][:40]:
                lines.append(f"  - {f}")
            if len(packet["required_files"]) > 40:
                lines.append(
                    f"  ... and {len(packet['required_files']) - 40} more")
            lines += [
                "",
                "VALIDATION RULES (violations are rejected by meters/managers):",
            ]
            for rule in packet["rules"]:
                lines.append(f"  - {rule}")
            lines += [
                "",
                "PROJECT SKELETON (create this folder structure FIRST, then write",
                "feature code onto it — a clean skeleton keeps the build testable):",
                "  docs/            — README.md, requirement.txt, ARCHITECTURE.md",
                "  src/             — application code (api, web, models, repository, db)",
                "  templates/ static/ — server-rendered UI",
                "  tests/           — standardized test taxonomy:",
            ]
            lines += [
                f"    tests/{d}/ — {self._test_purpose(d)}" for d in TEST_TAXONOMY]
            lines += [
                "    tests/test_app.py — the runnable end-to-end suite",
                "",
                "METER RUBRIC (every commit is scored):",
                "  - build_accuracy: manifest completeness + service wiring",
                "  - code_accuracy: syntax, style, no bare except",
                "  - requirement_coverage: each entity/feature has API + UI + tests",
                "  - requirement_fidelity: app reflects the user's description",
                "  - process_adherence: correct channels, sample data, tests run",
            ]
        # The user's description is the app's PURPOSE. It must reach EVERY role —
        # especially the builder (Session A) — at prime time, so A never starts
        # blind and never has to ask "what is myapp?". (Previously this was only
        # given to the answerer, which is why A saw just the app name.)
        if self.blueprint.description:
            lines += [
                "",
                f"USER REQUIREMENT (what to build): {self.blueprint.description}",
            ]
        elif role == "builder":
            lines += [
                "",
                "USER REQUIREMENT: not provided yet — a BUILD BRIEF from the App "
                "Builder Assistant will follow with the app's purpose; wait for "
                "it. Only if it is still undefined, emit ASK: <one question>.",
            ]
        if role == "builder":
            lines += [
                "ENGINEERING PRINCIPLES: Fix the ROOT CAUSE of any failure — never "
                "a bypass, stub, mock-around, or try/except that hides the error. "
                "Keep the code optimized and clean (no dead code, no duplication). "
                "Prefer the smallest correct change.",
                "A BUILD BRIEF framed by the App Builder Assistant follows with "
                "the finalized requirement — build from it. If a USER REQUIREMENT "
                "is given above, the app's domain is ALREADY defined: do NOT ask "
                "the user to pick/define a domain, just build that app.",
            ]
        if self.blueprint.build_profile == "prototype":
            lines += [
                "",
                "PROTOTYPE PROFILE: prioritize speed and demonstrability. Every "
                "main workflow named in the plan must have a working happy path "
                "and visible sample/real data, but exhaustive edge cases and full "
                "production hardening can be lighter than a full build.",
            ]
        else:
            lines += [
                "",
                "FULL PROFILE: deliver the complete production-functional app — "
                "all primary workflows end-to-end, robust validation/error states, "
                "rich UX, and stronger test coverage.",
            ]
        if role == "answerer":
            lines += [
                "",
                "YOUR ROLE (Session B — ADVISOR / ANSWERER / REVIEWER):",
                "  1. ANSWERER: stand in for the user — answer Session A's questions "
                "promptly so A is never stuck waiting. Monitor A's progress; if A "
                "appears idle or blocked, proactively suggest the next concrete step.",
                "  2. REVIEWER: after A declares the build complete, review the full "
                "codebase against the USER REQUIREMENT — confirm the app is designed "
                "as expected, components are wired, and it is ready to launch for the "
                "user to start and verify.",
                "     Hold A to the PRODUCTION-FUNCTIONAL CONTRACT: every flow works "
                "end-to-end (not a good-looking but dead UI), the UX is rich, and "
                "sample data is seeded/shown when the database does not cover the "
                "whole app. Name any non-functional flow as a real blocker.",
                "You receive Session A's published folder/file structure so you know "
                "what exists. You do NOT write or edit app code. Stay faithful to the "
                "exact requirement (domain, audience, features the user named).",
            ]
        elif role == "validator":
            lines += [
                "",
                "YOUR ROLE (Session C — VALIDATOR / TEST HELPER):",
                "  1. TEST AUTHOR: write your OWN independent pytest files ONLY inside "
                f"'{VALIDATOR_TEST_DIR}/'. Session A and B must NEVER create or edit "
                "anything in that folder — it is yours alone. Derive tests from the "
                "USER REQUIREMENT plus Session A's real structure and public "
                "class/function/method names (read A's code; do not invent names).",
                "  2. VALIDATOR (read-only turns): judge compile/import dry-runs, "
                "test outcomes, and requirement coverage from the evidence the App "
                "Builder hands you. Report VERDICT: complete/incomplete with short "
                "actionable issues — Session A makes all app-code fixes.",
                "  3. FUNCTIONAL JUDGE: verify REAL functionality, not just that "
                "files exist — exercise the main routes/flows, confirm data "
                "persists and renders, the UX is usable, and sample data is "
                "seeded/shown when the DB does not cover the app. A flow that "
                "errors (5xx) or a screen that does nothing is 'incomplete'.",
                "Your tests are independent acceptance checks; they do NOT block "
                "Session A's build gate.",
            ]
        elif self.blueprint.mode == BuildMode.FROM_SCRATCH:
            lines += [
                "",
                "YOUR ROLE (Session A — BUILDER): you OWN the complete codebase.",
                "Understand the USER REQUIREMENT, prepare a plan, then create the",
                "fundamentally correct empty folder/file scaffold FIRST. Sessions B",
                "and C receive your published structure so they can advise and test.",
                f"NEVER write inside '{VALIDATOR_TEST_DIR}/' — that folder belongs to",
                "Session C only.",
                "",
                "YOUR TASK: build the complete working application in this",
                "workspace phase-wise. YOU choose the folder structure — design",
                "whatever layout THIS app needs. The only contract: keep an importable",
                "ASGI app at src.app:app with GET /health and an openable webpage.",
                "Build the openable page early, keep it runnable, and let it reflect",
                "what you have built so far as you iterate. You own compilations,",
                "unit tests, and full testing of your code.",
                "",
                "COMMUNICATION PROTOCOL (for the App Builder Assistant):",
                "  - Build in whatever phases make sense for THIS app — the order",
                "    below is only a suggestion, not a required structure.",
                "  - When a component is finished: PHASE-DONE: <component>",
                "    (api, db, web, models, tests, …). Session C validates each.",
                "  - When you need a decision: ASK: / CONFIRM: / APPROVE: <question>",
                "    — only for genuine user-directed decisions.",
                "  - The build is complete ONLY when Session C validates AND",
                "    Session B agrees — not when you alone think it is done.",
                "",
                *self._FUNCTIONAL_CONTRACT,
                "",
                "Meters give feedback on requirement fidelity and code quality —",
                "they do NOT enforce a fixed folder layout.",
            ]
        else:
            lines += [
                "",
                "YOUR ROLE (Session A — BUILDER): you OWN the complete codebase.",
                "Understand the requirement, plan, then lay down the PROJECT SKELETON",
                "FIRST so Sessions B and C can see your structure. You own all app",
                "code changes, compilations, unit tests, and full testing.",
                f"NEVER write inside '{VALIDATOR_TEST_DIR}/' — Session C's tests live",
                "there independently.",
                "",
                "YOUR TASK: build the complete working application in this",
                "workspace phase-wise. FIRST lay down the PROJECT SKELETON above",
                "(all folders, including docs/ and the tests/ taxonomy), THEN write",
                "docs/requirement.txt from the BUILD BRIEF as your recursive",
                "acceptance reference, THEN write feature code and place each test",
                "in its matching tests/ folder.",
                "",
                "COMMUNICATION PROTOCOL (for the App Builder Assistant):",
                "  - Build phase-wise: skeleton → api → db → web → tests.",
                "  - When a component is finished: PHASE-DONE: <component>",
                "    (api, db, web, models, skeleton, tests, …). Session C validates.",
                "  - When you need a decision: ASK: / CONFIRM: / APPROVE: <question>",
                "    — only for genuine user-directed decisions.",
                "  - The build is complete ONLY when Session C validates AND",
                "    Session B agrees — not when you alone think it is done.",
                "",
                *self._FUNCTIONAL_CONTRACT,
                "",
                "Every change is validated by the engine and meters. Fix any reported",
                "gaps before finishing.",
            ]
        for k, v in self.extra.items():
            lines.append(f"{k}: {v}")
        lines.append("=== END GOVERNANCE BRIEF ===")
        return "\n".join(lines)

    def render_minimal(self, *, role: str = "builder") -> str:
        """Role-only primer for FROM_DATABASE before Session B instructs A/C.

        Omits schema, design brief, user requirement, meter rubric, and infra
        blocks — Session B delivers the substantive instruction next.
        """
        packet = self.engine.agent_metadata_packet(self.blueprint)
        lines = [
            "=== APP BUILDER GOVERNANCE BRIEF (MINIMAL) ===",
            f"ROLE: {role}",
            f"APP: {packet['app_name']}",
            f"MODE: {packet['mode']}",
            f"BUILD PROFILE: {self.blueprint.build_profile}",
            f"VARIANT: {self.blueprint.variant}",
            "",
        ]
        if role == "builder":
            lines += [
                "YOUR ROLE (Session A — BUILDER): you OWN the complete codebase.",
                "ENGINEERING PRINCIPLES: Fix the ROOT CAUSE of any failure — never "
                "a bypass, stub, mock-around, or try/except that hides the error. "
                "Keep the code optimized and clean (no dead code, no duplication). "
                "Prefer the smallest correct change.",
                "Session B (the advisor) will deliver your FULL build instruction: "
                "what to build, the design brief, schema, sample data, and action "
                "steps. Do NOT start building until you receive that instruction "
                "from Session B.",
                "For ANY question emit ASK: <question> — Session B answers on "
                "behalf of the user. Never ask the user directly.",
                f"NEVER write inside '{VALIDATOR_TEST_DIR}/' — Session C owns "
                "that folder.",
                "RUNNABILITY CONTRACT: keep an importable ASGI app at src.app:app "
                "with GET /health.",
            ]
        elif role == "validator":
            lines += [
                "YOUR ROLE (Session C — VALIDATOR / TEST HELPER):",
                "Session B will deliver your FULL validation instruction: the "
                "build brief, schema, sample data, and what to validate.",
                "Do NOT author tests until you receive B's instruction.",
                f"Write your OWN independent pytest files ONLY inside "
                f"'{VALIDATOR_TEST_DIR}/'.",
                "Coordinate questions through Session B.",
            ]
        else:
            lines += [
                "YOUR ROLE (Session B — ADVISOR): you receive the full governance "
                "brief and DB understanding. Frame clear instructions for "
                "Sessions A and C.",
            ]
        lines.append("=== END GOVERNANCE BRIEF ===")
        return "\n".join(lines)


def make_brief(
    blueprint: AppBlueprint,
    engine: Optional[AiAppEngine] = None,
    *,
    connection_name: str = "",
    schema: Optional[dict] = None,
    data_insight: str = "",
    target_score: float = 0.9,
    target_coverage: float = 0.9,
) -> GovernanceBrief:
    eng = engine or AiAppEngine()
    schema_summary = ""
    if schema:
        schema_summary = ", ".join(
            f"{t}({len(cols)} cols)" for t, cols in list(schema.items())[:12])
    return GovernanceBrief(
        blueprint=blueprint,
        engine=eng,
        connection_name=connection_name,
        schema_summary=schema_summary,
        data_insight=data_insight,
        target_score=target_score,
        target_coverage=target_coverage,
    )
