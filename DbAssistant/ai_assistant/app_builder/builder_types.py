"""Mode-specific builder policy for App Builder.

The A/B/C session protocol is shared by every app-builder mode; this module
keeps the mode-specific choices (variant mapping, profile wording, meters) in
small builder-policy classes so scratch/database/codebase can evolve
independently without duplicating orchestration logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode


APPLICATION = "application"
EXPLORER = "explorer"
FULL = "full"
PROTOTYPE = "prototype"


@dataclass(frozen=True)
class BuilderPolicy:
    """Mode-specific policy layered on top of the shared A/B/C flow."""

    mode: BuildMode
    name: str
    common_meters: tuple[str, ...]
    mode_meters: tuple[str, ...]

    def normalize_variant(self, value: str) -> str:
        value = (value or "").strip().lower()
        if value in (EXPLORER, "insights_admin", "structure_metadata", "admin"):
            return EXPLORER
        return APPLICATION

    def normalize_profile(self, value: str) -> str:
        value = (value or "").strip().lower()
        return FULL if value == FULL else PROTOTYPE

    def action_text(self, blueprint: AppBlueprint) -> str:
        profile = self.normalize_profile(blueprint.build_profile)
        variant = self.normalize_variant(blueprint.variant)
        speed = (
            "FULL BUILD PROFILE: build a production-functional app. Every main "
            "workflow must be end-to-end, polished, tested, and launchable."
            if profile == FULL else
            "PROTOTYPE BUILD PROFILE: build a fast demonstrative prototype. "
            "Every named workflow must have a working happy path and show real "
            "data requirements with seeded/sample data, but exhaustive edge "
            "cases and full test taxonomy can be lighter."
        )
        kind = (
            "VARIANT: application — build the real user-facing app."
            if variant == APPLICATION else
            "VARIANT: explorer — build a metadata/insights/admin explorer for "
            "the source material instead of reconstructing the end-user app."
        )
        return f"{speed}\n{kind}"

    def meter_names(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys((*self.common_meters, *self.mode_meters)))


COMMON_METERS = (
    "business_intent",
    "feature_completeness",
    "backend_logic",
    "code_hygiene",
    "architecture",
    "functional_correctness",
)


class ScratchBuilderPolicy(BuilderPolicy):
    def __init__(self) -> None:
        super().__init__(
            BuildMode.FROM_SCRATCH, "scratch",
            COMMON_METERS,
            ("schema_design", "solid_principles", "cli_interface"),
        )


class DatabaseBuilderPolicy(BuilderPolicy):
    def __init__(self) -> None:
        super().__init__(
            BuildMode.FROM_DATABASE, "database",
            COMMON_METERS,
            (
                "metadata_completeness", "archetype_fit", "schema_fidelity",
                "relationship_fidelity", "entity_role_fit", "data_semantics",
                "workflow_coverage", "prediction_grounding",
            ),
        )


class CodebaseBuilderPolicy(BuilderPolicy):
    def __init__(self) -> None:
        super().__init__(
            BuildMode.FROM_CODEBASE, "codebase",
            COMMON_METERS,
            ("architecture_recovery", "component_coverage"),
        )

    def normalize_variant(self, value: str) -> str:
        value = (value or "").strip().lower()
        if value in (EXPLORER, "structure_metadata", "metadata", "analysis"):
            return EXPLORER
        return APPLICATION


def policy_for(mode: BuildMode | str) -> BuilderPolicy:
    mode = BuildMode(mode)
    if mode == BuildMode.FROM_DATABASE:
        return DatabaseBuilderPolicy()
    if mode == BuildMode.FROM_CODEBASE:
        return CodebaseBuilderPolicy()
    return ScratchBuilderPolicy()


def meter_subset(names: Iterable[str], all_measurements: dict) -> dict:
    """Return a stable subset of measurements by requested meter name."""
    wanted = set(names)
    return {k: v for k, v in all_measurements.items() if k in wanted}
