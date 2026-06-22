"""Concrete meter-managers — one per build-quality meter.

Each manager inherits the shared gating maths from :class:`MeterManager` and
only customizes the *subject* and the *suggestion* so the advisor receives
domain-appropriate, factual guidance about what is missing and why. None of
this instructs Session A directly — the App Builder Assistant routes the signal
to Session B, which decides whether action is genuinely required.
"""

from __future__ import annotations

from ai_assistant.meters.base import Measurement

from ai_assistant.app_builder.meter_managers.base import MeterManager


class FunctionalCorrectnessManager(MeterManager):
    meter_name = "functional_correctness_meter"
    # Failing tests are never "just a warning" — broaden the fail band.
    warning_band = 0.05

    def _subject(self) -> str:
        return "functional correctness (test pass rate)"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        ev = m.evidence or {}
        if ev.get("executed", 0) == 0:
            return ("No tests executed — add and run tests that exercise the "
                    "core flows so correctness can be proven.")
        failed = ev.get("failed", 0)
        errored = ev.get("errors", 0)
        return (f"Fix the {failed} failing and {errored} erroring test(s); the "
                "app must pass its own tests before it is considered working.")


class FeatureCompletenessManager(MeterManager):
    meter_name = "feature_completeness_meter"

    def _subject(self) -> str:
        return "feature completeness"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        feats = (m.evidence or {}).get("missing", [])
        if feats:
            return ("Implement the missing features end-to-end (route + logic + "
                    "UI/test): " + ", ".join(feats[:8]) + ".")
        return "Wire the remaining requested features through to reachable routes."


class BusinessIntentManager(MeterManager):
    meter_name = "business_intent_meter"

    def _subject(self) -> str:
        return "business-intent fidelity"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        toks = (m.evidence or {}).get("missing", [])
        if toks:
            return ("Reflect the user's actual domain in code and user-facing "
                    "surfaces (not a generic scaffold): " + ", ".join(toks[:8])
                    + ".")
        return "Make the app's vocabulary match the user's described domain."


class SchemaDesignManager(MeterManager):
    meter_name = "schema_design_meter"

    def _subject(self) -> str:
        return "schema/data-model design"


class BackendLogicManager(MeterManager):
    meter_name = "backend_logic_meter"

    def _subject(self) -> str:
        return "backend logic / API design"


class CodeHygieneManager(MeterManager):
    meter_name = "code_hygiene_meter"
    # Hygiene rarely blocks a build outright; treat shortfalls as warnings.
    warning_band = 0.25

    def _subject(self) -> str:
        return "code hygiene / maintainability"


class ArchitectureManager(MeterManager):
    meter_name = "architecture_meter"

    def _subject(self) -> str:
        return "architecture / structure"


class CliInterfaceManager(MeterManager):
    meter_name = "cli_interface_meter"

    def _subject(self) -> str:
        return "command-line interface"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        ev = m.evidence or {}
        if ev.get("requested") and not ev.get("present"):
            return ("Add a real CLI (argparse/click/typer) with subcommands for "
                    "the core operations and a runnable entry point.")
        gaps = []
        if not ev.get("framework"):
            gaps.append("use argparse/click/typer")
        if not ev.get("entry_point"):
            gaps.append("add a __main__/console_scripts entry point")
        if not ev.get("subcommands"):
            gaps.append("expose subcommands for each operation")
        if gaps:
            return "Strengthen the CLI: " + "; ".join(gaps) + "."
        return "Round out the CLI so each core operation is runnable."


class SolidPrinciplesManager(MeterManager):
    meter_name = "solid_principles_meter"
    # Design quality rarely blocks a build outright; treat shortfalls softly.
    warning_band = 0.2

    def _subject(self) -> str:
        return "SOLID / OO design"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        ev = m.evidence or {}
        gaps = []
        if ev.get("oversized"):
            gaps.append("split god-objects into focused classes (SRP)")
        if not ev.get("abstractions"):
            gaps.append("introduce abstractions (ABC/Protocol) for key seams "
                        "(DIP/OCP)")
        if not ev.get("injected_classes"):
            gaps.append("inject collaborators via constructors (DIP)")
        if int(ev.get("isinstance_dispatch", 0)) > 4:
            gaps.append("replace type dispatch with polymorphism (OCP)")
        if gaps:
            return "Improve OO design: " + "; ".join(gaps) + "."
        return "Tighten the SOLID design of the core classes."


class MetadataCompletenessManager(MeterManager):
    meter_name = "metadata_completeness_meter"

    def _subject(self) -> str:
        return "database metadata completeness"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        kinds = (m.evidence or {}).get("kinds") or {}
        absent = [k for k, v in kinds.items() if not v]
        if absent:
            return ("Gather missing metadata via catalog ops before building: "
                    + ", ".join(absent[:8]) + ".")
        return "Complete metadata extraction (tables, views, indexes, constraints)."


class ArchetypeFitManager(MeterManager):
    meter_name = "archetype_fit_meter"

    def _subject(self) -> str:
        return "archetype surface fidelity"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        miss = (m.evidence or {}).get("missing") or missing
        if miss:
            return ("Expose predicted app surfaces in routes/UI: "
                    + ", ".join(miss[:8]) + ".")
        return "Align pages and routes with the predicted application archetype."


class SchemaFidelityManager(MeterManager):
    meter_name = "schema_fidelity_meter"

    def _subject(self) -> str:
        return "schema/data-model fidelity"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        miss = (m.evidence or {}).get("expected_tables") or []
        present = set((m.evidence or {}).get("present") or [])
        absent = [t for t in miss if t not in present]
        if absent:
            return ("Wire the app's data layer to real tables: "
                    + ", ".join(absent[:8]) + ".")
        return "Reflect the connected database tables in models and queries."


class ArchitectureRecoveryManager(MeterManager):
    meter_name = "architecture_recovery_meter"

    def _subject(self) -> str:
        return "codebase architecture recovery"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        return "Recover routes, services and entrypoints from the source codebase."


class ComponentCoverageManager(MeterManager):
    meter_name = "component_coverage_meter"

    def _subject(self) -> str:
        return "component coverage"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        return "Cover recovered components (APIs, services, models) in the built app."


class DesignCompletenessManager(MeterManager):
    meter_name = "design_completeness_meter"

    def _subject(self) -> str:
        return "design completeness"

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        pending = []
        for items in (m.evidence or {}).get("pending", {}).values():
            if isinstance(items, list):
                pending.extend(items)
        if pending:
            return ("Build the remaining agreed design pieces: "
                    + ", ".join(pending[:10]) + ".")
        return "Complete the remaining agreed design components."
