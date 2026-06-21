"""db_app_builder_assistant — orchestrates from_database builds.

Runs phased deterministic profiling, archetype classification, and builds a
predicted-app design brief for Session B → A/C. Reuses the existing orchestrator
for the actual build loop.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.app_builder.archetypes import expected_surfaces, get_archetype
from ai_assistant.app_builder.db_understanding import (
    DataInsight,
    DbUnderstandingClient,
)
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode


def build_design_brief(insight: DataInsight) -> str:
    """Build the predicted-app design brief from a completed DataInsight.

    The brief leads with the predicted real-world app (name, persona,
    user-facing features) and chooses the build goal by *confidence*: a
    confident prediction targets the real application; a low-confidence one (or
    the explicit insights/admin variant) falls back to the schema/admin
    reflection. ``insights_admin`` is always treated as the reflection variant.
    """
    arch = get_archetype(insight.archetype)
    arch_label = arch.label if arch else insight.archetype or "application"
    surfaces = expected_surfaces(insight.archetype)
    # Build the real app when we are confident AND not explicitly in the
    # insights/admin variant; otherwise fall back to the schema reflection.
    build_real_app = bool(
        insight.confident and insight.variant != "insights_admin")
    lines = [
        "PREDICTED APP DESIGN BRIEF (from_database)",
        f"Variant: {insight.variant}",
        f"Archetype: {arch_label} (confidence {insight.archetype_confidence:.0%})",
        f"Prediction confidence: {'high' if insight.confident else 'low'}",
    ]
    if build_real_app and insight.app_name:
        lines += ["", f"PREDICTED APP: {insight.app_name}"]
    if build_real_app and insight.persona:
        lines.append(f"PRIMARY USER: {insight.persona}")
    if insight.user_description:
        lines += [
            "",
            "USER DESCRIPTION (must be reflected precisely):",
            insight.user_description,
        ]
    if insight.app_summary:
        lines += ["", "DATA-INFERRED APPLICATION:", insight.app_summary]
    if insight.data_flow:
        lines += ["", "DATA FLOW:", insight.data_flow]
    if insight.profile is not None:
        from ai_assistant.app_builder.db_semantics import (
            relationship_summary,
            semantic_column_summary,
            table_role_summary,
        )

        rels = relationship_summary(insight.profile)
        roles = table_role_summary(insight.profile)
        tags = semantic_column_summary(insight.profile)
        if rels:
            lines += [
                "",
                "RELATIONSHIP GRAPH (declared catalog edges are authoritative; "
                "inferred edges are labeled with confidence):",
            ]
            lines += [f"  - {r}" for r in rels]
        if roles:
            lines += ["", "TABLE ROLES (drive navigation and UI treatment):"]
            lines += [f"  - {r}" for r in roles]
        if tags:
            lines += ["", "SEMANTIC COLUMN HINTS (drive widgets, formatting, and privacy):"]
            lines += [f"  - {t}" for t in tags]
        if insight.advisory_notes:
            lines += ["", "SOURCE DB ADVISORIES:"]
            lines += [f"  - {n}" for n in insight.advisory_notes]
    if build_real_app and insight.app_features:
        lines += ["", "USER-FACING FEATURES (build these workflows):"]
        lines += [f"  - {f}" for f in insight.app_features[:6]]
    if insight.variant == "insights_admin" or not build_real_app:
        if insight.variant == "insights_admin":
            lines += [
                "",
                "BUILD GOAL: DB insights / admin dashboard for DBAs — expose schema "
                "metadata, column profiling stats, relationship graph, semantic "
                "column tags, table roles, and data-quality signals. Do NOT build "
                "a generic CRUD mirror or business application.",
                "",
                "REQUIRED PAGES (must all be functional):",
                "  1. Overview dashboard — table count, total rows, FK count, per-table "
                "cards with role badges, data-quality advisories",
                "  2. Table profile — column types, PK/FK/index flags, null %, "
                "distinct counts, semantic tags, sample values",
                "  3. Relationship graph — all declared FK edges with from/to tables",
                "  4. Sample data viewer — live rows per table (read-only)",
                "",
                "DATA SOURCE: introspect the LIVE SQLite database at runtime via "
                "PRAGMA (table_info, foreign_key_list, index_list) — do NOT hardcode "
                "a static schema. The app must reflect whatever tables exist in "
                "APP_DB_PATH.",
                "",
                "DBA VALUE: help a DBA quickly understand schema structure, spot "
                "missing referential integrity, high-null columns, inferred FK "
                "candidates, and table roles (master/transaction/junction/lookup/audit).",
            ]
        else:
            lines += [
                "",
                "BUILD GOAL (fallback — app type unclear): the real-world "
                "application could not be confidently predicted from the data, "
                "so build a clear schema/data-admin reflection: table explorer, "
                "record list/detail, and sample-data viewer grounded in the "
                "real tables.",
                f"Expected surfaces: {', '.join(surfaces)}",
            ]
    else:
        lines += [
            "",
            "BUILD GOAL: Build the real user-facing application this data "
            "supports — NOT a schema browser or raw CRUD over tables. The "
            "database may back only ONE service of a larger app; still build "
            "realistic end-user workflows for the features above.",
            f"Expected surfaces: {', '.join(surfaces)}",
            "",
            "WHAT TO BUILD (and what NOT to build):",
            "  - The pages/screens are the USER-FACING FEATURES above — one "
            "purposeful workflow per feature (dashboards, streams, queues, "
            "timelines, detail views), named and organised for the PRIMARY "
            "USER, using domain language (not table names).",
            "  - Do NOT generate a generic list/create/edit/delete screen for "
            "every table. The raw tables are the DATA LAYER behind these "
            "workflows, not the navigation. Only expose create/edit forms for "
            "the few records a real user of THIS app would actually manage.",
            "  - Decide the information architecture from the FEATURES, then map "
            "each feature to the table(s) it reads/writes (see KEY TABLES).",
            "",
            "DATA REQUIREMENTS:",
            "  - Seed realistic SAMPLE DATA derived from the real sampled rows "
            "below so every screen is populated on first run.",
            "  - Services that touch the database must read/write the REAL "
            "tables and surface real event data and metrics (e.g. actual "
            "delivery/event records and their counts), not just table row "
            "totals or empty placeholders.",
        ]
    if insight.tables:
        if build_real_app:
            lines += [
                "",
                "KEY TABLES (DATA LAYER — back the features above; not a screen "
                "list):",
            ]
        else:
            lines += ["", "KEY TABLES:"]
        for t in insight.tables[:15]:
            cols = ", ".join(t.columns[:8]) if t.columns else "?"
            line = f"  - {t.name}({cols})"
            if t.note:
                line += f" — {t.note}"
            lines.append(line)
    return "\n".join(lines)


class DbAppBuilderAssistant:
    """Orchestrate phased DB understanding and prepare build context."""

    def __init__(
        self,
        *,
        query_assistant: Any = None,
        db_manager: Any = None,
        core: Any = None,
        connection_name: str = "",
        user_description: str = "",
        variant: str = "application",
        mask_pii: bool = False,
    ) -> None:
        self._client = DbUnderstandingClient(
            query_assistant=query_assistant,
            db_manager=db_manager,
            core=core,
            connection_name=connection_name,
            user_description=user_description,
            variant=variant,
            mask_pii=mask_pii,
        )

    def available(self) -> bool:
        return self._client.available()

    def understand(self, schema: dict[str, list[str]]) -> DataInsight:
        return self._client.understand(schema)

    def prepare_blueprint(
        self, blueprint: AppBlueprint, insight: DataInsight,
    ) -> AppBlueprint:
        """Enrich blueprint with archetype and description from insight."""
        blueprint.mode = BuildMode.FROM_DATABASE
        # The insights/admin variant always builds the DB-insights dashboard.
        if (getattr(blueprint, "db_app_variant", "") == "insights_admin"
                or getattr(insight, "variant", "") == "insights_admin"):
            blueprint.kind = "insights"
        elif insight.archetype:
            blueprint.kind = insight.archetype
        desc_parts = [p for p in (
            insight.user_description,
            insight.app_summary,
            insight.design_brief,
        ) if p]
        if desc_parts:
            blueprint.description = "\n\n".join(desc_parts)
        return blueprint

    def make_understanding_client(self) -> DbUnderstandingClient:
        return self._client
