"""codebase_app_builder_assistant — orchestrates from_codebase builds."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ai_assistant.app_builder.codebase_profile import CodebaseProfile, CodebaseProfiler
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode


@dataclass
class CodebaseInsight:
    profile: CodebaseProfile
    variant: str = "predicted_app"
    user_description: str = ""
    design_brief: str = ""
    components: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile.as_dict(),
            "variant": self.variant,
            "user_description": self.user_description,
            "design_brief": self.design_brief,
            "components": list(self.components),
        }

    def prompt_block(self) -> str:
        lines = ["CODEBASE UNDERSTANDING:", self.design_brief]
        if self.components:
            lines += ["", "COMPONENTS:"] + [f"  - {c}" for c in self.components[:30]]
        return "\n".join(lines)


def build_codebase_brief(insight: CodebaseInsight) -> str:
    p = insight.profile
    lines = [
        "PREDICTED APP DESIGN BRIEF (from_codebase)",
        f"Variant: {insight.variant}",
        f"Source: {p.path}",
        f"Files: {p.files}  LOC: {p.loc}",
    ]
    if insight.user_description:
        lines += [
            "",
            "USER DESCRIPTION (must be reflected precisely):",
            insight.user_description,
        ]
    if insight.variant == "structure_metadata":
        lines += [
            "",
            "BUILD GOAL: Structure / metadata explorer — show folder tree, "
            "components, APIs, dependencies, and sample I/O fields. "
            "Focus on architecture visibility, not a full rewrite.",
        ]
    else:
        lines += [
            "",
            "BUILD GOAL: Build the predicted real application this codebase "
            "implements — pages, flows, APIs, and data layer aligned with the "
            "recovered architecture.",
        ]
    if p.entrypoints:
        lines += ["", "ENTRYPOINTS:", *[f"  - {e}" for e in p.entrypoints[:10]]]
    if p.routes:
        lines += ["", "ROUTES / APIs:", *[f"  - {r}" for r in p.routes[:20]]]
    if p.services:
        lines += ["", "SERVICES:", *[f"  - {s}" for s in p.services[:15]]]
    if p.third_party_deps:
        lines += ["", "DEPENDENCIES:", ", ".join(p.third_party_deps[:20])]
    if p.db_tables:
        lines += ["", "DATA MODELS:", ", ".join(p.db_tables[:15])]
    return "\n".join(lines)


class CodebaseAppBuilderAssistant:
    """Orchestrate codebase profiling and prepare build context."""

    def __init__(
        self,
        *,
        codebase_path: str = "",
        user_description: str = "",
        variant: str = "predicted_app",
    ) -> None:
        self._path = codebase_path
        self._user_description = (user_description or "").strip()
        raw_variant = (variant or "predicted_app").strip().lower()
        if raw_variant in ("explorer", "structure_metadata", "metadata"):
            self._variant = "structure_metadata"
        else:
            self._variant = "predicted_app"
        self._profiler = CodebaseProfiler(codebase_path)

    def understand(self) -> CodebaseInsight:
        profile = self._profiler.profile()
        components = _inventory_components(profile)
        insight = CodebaseInsight(
            profile=profile,
            variant=self._variant,
            user_description=self._user_description,
            components=components,
        )
        insight.design_brief = build_codebase_brief(insight)
        return insight

    def prepare_blueprint(
        self, blueprint: AppBlueprint, insight: CodebaseInsight,
    ) -> AppBlueprint:
        blueprint.mode = BuildMode.FROM_CODEBASE
        blueprint.codebase_path = insight.profile.path
        desc_parts = [p for p in (
            insight.user_description, insight.design_brief,
        ) if p]
        if desc_parts:
            blueprint.description = "\n\n".join(desc_parts)
        return blueprint


def _inventory_components(profile: CodebaseProfile) -> list[str]:
    out: list[str] = []
    if profile.entrypoints:
        out.append(f"entrypoints ({len(profile.entrypoints)})")
    if profile.routes:
        out.append(f"api_routes ({len(profile.routes)})")
    if profile.services:
        out.append(f"services ({len(profile.services)})")
    if profile.third_party_deps:
        out.append(f"dependencies ({len(profile.third_party_deps)})")
    if profile.db_tables:
        out.append(f"data_models ({len(profile.db_tables)})")
    if profile.docs:
        out.append(f"documentation ({len(profile.docs)})")
    return out
