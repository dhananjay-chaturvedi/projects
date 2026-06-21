"""DB-understanding channel — phased facts + AI interpretation.

Phase 1–3 are deterministic (metadata, profiling, sampling) via
:class:`~ai_assistant.app_builder.db_profile.DbProfiler`. Phase 4 uses the
normal backend chat model to interpret those bounded, schema-aware facts. The
AI Query Assistant NL→SQL path is reserved for explicit analytical lookups, not
for every interpretation turn.

All database reads for a build go through ONE persistent connection: the client
resolves a single cached DB manager up front (see ``_ensure_session``) and binds
it to the profiler, so every catalog/profiling/sampling query reuses the same
live session for the whole build instead of reconnecting per query.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from ai_query import module_config as mc

from ai_assistant.app_builder.db_profile import DbProfile, DbProfiler, DbProfilerConfig


@dataclass
class TableInsight:
    """What we learned about a single table."""

    name: str
    columns: list[str] = field(default_factory=list)
    row_count: Optional[int] = None
    sample_rows: list[dict] = field(default_factory=list)
    note: str = ""
    role: str = ""
    role_confidence: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name, "columns": list(self.columns),
            "row_count": self.row_count, "sample_rows": list(self.sample_rows),
            "note": self.note, "role": self.role,
            "role_confidence": self.role_confidence,
        }


@dataclass
class DataInsight:
    """Consolidated understanding of a database, ready to inform a build."""

    connection: str = ""
    tables: list[TableInsight] = field(default_factory=list)
    app_summary: str = ""
    data_flow: str = ""
    profile: Optional[DbProfile] = None
    archetype: str = ""
    archetype_confidence: float = 0.0
    user_description: str = ""
    variant: str = "application"
    phases_completed: list[str] = field(default_factory=list)
    design_brief: str = ""
    # Real-world app prediction. ``confident`` gates whether the build targets
    # the predicted real app or falls back to the schema/admin reflection.
    app_name: str = ""
    persona: str = ""
    app_features: list[str] = field(default_factory=list)
    confident: bool = False
    relationships: list[dict[str, Any]] = field(default_factory=list)
    advisory_notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "connection": self.connection,
            "tables": [t.as_dict() for t in self.tables],
            "app_summary": self.app_summary,
            "data_flow": self.data_flow,
            "profile": self.profile.as_dict() if self.profile else None,
            "archetype": self.archetype,
            "archetype_confidence": self.archetype_confidence,
            "user_description": self.user_description,
            "variant": self.variant,
            "phases_completed": list(self.phases_completed),
            "design_brief": self.design_brief,
            "app_name": self.app_name,
            "persona": self.persona,
            "app_features": list(self.app_features),
            "confident": self.confident,
            "relationships": [dict(r) for r in self.relationships],
            "advisory_notes": list(self.advisory_notes),
        }

    def prompt_block(self, *, max_tables: int = 25, max_rows: int = 3) -> str:
        """Render the insight as a compact block for the code-agent prompt."""
        lines = ["DATABASE UNDERSTANDING (phased profile + interpretation):"]
        if self.user_description:
            lines.append(f"  user description: {self.user_description}")
        if self.archetype:
            lines.append(
                f"  predicted archetype: {self.archetype} "
                f"(confidence {self.archetype_confidence:.0%})")
        if self.app_name:
            lines.append(f"  predicted app: {self.app_name}")
        if self.persona:
            lines.append(f"  primary user: {self.persona}")
        if self.app_features:
            lines.append(
                "  user-facing features: " + ", ".join(self.app_features[:8]))
        lines.append(
            f"  prediction confidence: {'high' if self.confident else 'low'}")
        if self.app_summary:
            lines.append(f"  app this data supports: {self.app_summary}")
        if self.data_flow:
            lines.append(f"  data flow: {self.data_flow}")
        if self.variant == "insights_admin":
            lines.append("  build variant: DB insights / admin dashboard")
        if self.profile:
            from ai_assistant.app_builder.db_semantics import (
                relationship_summary,
                semantic_column_summary,
                table_role_summary,
            )

            rels = relationship_summary(self.profile)
            roles = table_role_summary(self.profile)
            tags = semantic_column_summary(self.profile)
            if rels:
                lines.append("  relationships (declared first; inferred labeled):")
                lines += [f"    - {r}" for r in rels]
            if roles:
                lines.append("  table roles:")
                lines += [f"    - {r}" for r in roles]
            if tags:
                lines.append("  semantic columns:")
                lines += [f"    - {t}" for t in tags]
            for note in self.advisory_notes:
                lines.append(f"  advisory: {note}")
        for t in self.tables[:max_tables]:
            cols = ", ".join(t.columns) if t.columns else "?"
            head = f"  - {t.name}({cols})"
            if t.row_count is not None:
                head += f"  ~{t.row_count} rows"
            if t.role:
                head += f" role={t.role}"
            lines.append(head)
            if t.note:
                lines.append(f"      meaning: {t.note}")
            for row in t.sample_rows[:max_rows]:
                lines.append(f"      sample: {row}")
        if self.design_brief:
            lines += ["", "DESIGN BRIEF:", self.design_brief]
        return "\n".join(lines)


@dataclass(frozen=True)
class DbUnderstandingConfig:
    """Runtime/config inputs for DB understanding."""

    query_assistant: Any = None
    db_manager: Any = None
    core: Any = None
    connection_name: str = ""
    sample_rows: int = 3
    user_description: str = ""
    variant: str = "application"
    tables_per_query: int | None = None
    mask_pii: bool = False


class DbUnderstandingClient:
    """Phased DB understanding: deterministic facts + AI interpretation."""

    def __init__(
        self,
        config: DbUnderstandingConfig | None = None,
        **legacy,
    ) -> None:
        config = config or DbUnderstandingConfig(**legacy)
        self._qa = config.query_assistant
        self._db = config.db_manager
        self._core = config.core
        self._connection = config.connection_name
        self._sample_rows = max(1, int(config.sample_rows))
        self._user_description = (config.user_description or "").strip()
        self._variant = config.variant if config.variant in ("application", "insights_admin") else "application"
        tables_per_query = config.tables_per_query
        if tables_per_query is None:
            tables_per_query = mc.get_int(
                "ai.app_builder", "interpret_tables_per_query", default=10)
        # <0 is treated as 0 (all tables in a single query).
        self._tables_per_query = max(0, int(tables_per_query))
        self._mask_pii = bool(config.mask_pii)
        self._profiler = DbProfiler(
            DbProfilerConfig(
                core=config.core,
                db_manager=config.db_manager,
                connection_name=config.connection_name,
                sample_rows=config.sample_rows,
            )
        )

    def available(self) -> bool:
        return (
            self._qa is not None or self._db is not None or self._core is not None
        )

    def _ensure_session(self) -> None:
        """Open ONE persistent DB connection reused for every build query.

        All deterministic DB reads for this build (catalog metadata, bounded
        profiling, and limited sampling) go through a single cached manager.
        When only a ``core`` service is supplied, resolve its cached connection
        once (``_get_or_connect`` keeps it warm) and bind it to the profiler so
        the catalog path and the bounded-query path share the exact same live
        connection. The connection is never disconnected mid-build — the core
        keeps it cached for the whole build, so later read-only DB probes reuse
        it instead of opening a new session.
        """
        if self._db is None and self._core is not None and self._connection:
            try:
                if hasattr(self._core, "open_connection"):
                    self._core.open_connection(self._connection)
                self._db = self._core.get_manager(self._connection)
            except Exception:  # noqa: BLE001
                self._db = None
        # Keep the profiler bound to the same single connection object.
        if getattr(self._profiler, "_db", None) is not self._db:
            self._profiler._db = self._db

    def understand(self, schema: dict[str, list[str]]) -> DataInsight:
        """Run phased understanding and return a :class:`DataInsight`."""
        self._ensure_session()
        profile = self._profiler.profile(schema)
        insight = DataInsight(
            connection=self._connection,
            profile=profile,
            user_description=self._user_description,
            variant=self._variant,
            phases_completed=list(profile.phases_completed),
        )
        for tp in profile.tables:
            insight.tables.append(TableInsight(
                name=tp.name,
                columns=[c.name for c in tp.columns],
                row_count=tp.row_count_estimate,
                sample_rows=list(tp.sample_rows),
                role=getattr(tp, "role", ""),
                role_confidence=getattr(tp, "role_confidence", 0.0) or 0.0,
            ))
        insight.relationships = [dict(r) for r in getattr(profile, "relationships", [])]
        insight.advisory_notes = list(getattr(profile, "advisory_notes", []) or [])
        if self._qa is not None and insight.tables:
            self._interpret(insight, profile)
        from ai_assistant.app_builder.archetypes import classify_archetype, get_archetype

        arch = classify_archetype(profile, user_description=self._user_description)
        insight.archetype = arch.id
        insight.archetype_confidence = arch.confidence
        arch_label = (get_archetype(arch.id).label if get_archetype(arch.id)
                      else arch.id.replace("_", " "))
        # Confidence is the OR of two independent signals: the model's own
        # confidence (set in _interpret) and a strong deterministic archetype
        # match. We fall back to the schema/admin reflection ONLY when BOTH are
        # low — i.e. the archetype is the generic fallback or weakly matched.
        archetype_confident = (
            arch.id != "generic_crud" and arch.confidence >= 0.5)
        insight.confident = bool(insight.confident or archetype_confident)
        if not insight.app_summary:
            insight.app_summary = (
                f"This database most likely supports a {arch_label} workflow, "
                "inferred from table and column naming plus sampled records."
            )
        if not insight.data_flow and insight.tables:
            roots = ", ".join(t.name for t in insight.tables[:3])
            insight.data_flow = (
                f"Core records appear to originate in {roots}, then flow through "
                "related operational tables for tracking, reporting, and updates."
            )
        filled_note = False
        for ti in insight.tables:
            if not ti.note:
                cols = ", ".join(ti.columns[:5]) or "available columns"
                ti.note = f"Stores {ti.name} records with fields such as {cols}."
                filled_note = True
        if filled_note and "deterministic_interpretation" not in insight.phases_completed:
            insight.phases_completed.append("deterministic_interpretation")
        from ai_assistant.app_builder.db_app_assistant import build_design_brief

        insight.design_brief = build_design_brief(insight)
        return insight

    # ── backend interpretation over deterministic DB facts ───────────────────
    def _interpret(self, insight: DataInsight, profile: DbProfile) -> None:
        """Interpret the deterministic profile via batched backend-chat calls.

        Tables are interpreted ``interpret_tables_per_query`` at a time (config),
        each batch a single backend call that returns ALL the per-table meanings
        at once. The source of truth is the schema-aware, bounded ``DbProfile``;
        interpretation never needs to generate/execute NL→SQL for each turn.
        ``per == 1`` → one table per query (still one session); ``per == 0`` →
        every table in a single query.
        """
        insight.phases_completed.append("interpretation")
        pairs = list(zip(insight.tables, profile.tables))
        per = self._tables_per_query
        if per <= 0:
            chunks = [pairs]
        else:
            chunks = [pairs[i:i + per] for i in range(0, len(pairs), per)]

        table_list = ", ".join(t.name for t in insight.tables)
        first = True
        for chunk in chunks:
            notes = self._ask_table_notes(chunk, table_list, None, first)
            first = False
            for ti, _tp in chunk:
                note = notes.get(ti.name) or notes.get(ti.name.lower())
                if note:
                    ti.note = note[:400]
        overview = self._ask_overview(insight, profile, first)
        if overview.get("app_summary"):
            insight.app_summary = overview["app_summary"][:600]
        if overview.get("data_flow"):
            insight.data_flow = overview["data_flow"][:400]
        if overview.get("app_name"):
            insight.app_name = overview["app_name"][:120]
        if overview.get("persona"):
            insight.persona = overview["persona"][:120]
        if overview.get("app_features"):
            insight.app_features = [f[:120] for f in overview["app_features"]]
        # Confident only when the model said so AND it actually named the app.
        insight.confident = bool(
            overview.get("confident") and insight.app_summary)

    def _ask_table_notes(
        self, chunk: list[tuple[TableInsight, Any]], table_list: str,
        session_id: Optional[str], first: bool,
    ) -> dict[str, str]:
        """One call → meanings for every table in *chunk* (multi-info query)."""
        lines = [
            "You are profiling a database to understand its data.",
            f"All tables in this database: {table_list}.",
            "",
            "For EACH table below, give ONE short sentence describing what "
            "real-world data it holds and what it is used for. Do NOT describe "
            "generic CRUD. Reply with ONLY a compact JSON object mapping each "
            'table name to its sentence, e.g. {"orders": "...", "items": "..."}.',
            "",
            "Tables:",
        ]
        for ti, tp in chunk:
            cols = ", ".join(
                f"{c.name}({c.data_type})" if getattr(c, "data_type", "") else c.name
                for c in getattr(tp, "columns", [])[:12]
            ) or ", ".join(ti.columns[:12]) or "unknown"
            entry = f"- {ti.name}({cols})"
            if ti.sample_rows:
                entry += f" sample: {ti.sample_rows[0]}"
            lines.append(entry)
        text = self._ask_in_session("\n".join(lines), session_id, first)
        parsed = _extract_json(text)
        out: dict[str, str] = {}
        if isinstance(parsed, dict):
            for k, v in parsed.items():
                if isinstance(v, str) and v.strip():
                    out[str(k)] = " ".join(v.split())
                elif isinstance(v, dict):
                    desc = v.get("description") or v.get("meaning") or ""
                    if isinstance(desc, str) and desc.strip():
                        out[str(k)] = " ".join(desc.split())
        return out

    def _ask_overview(
        self, insight: DataInsight, profile: DbProfile, first: bool,
    ) -> dict[str, Any]:
        """One call → the full real-world app prediction (multi-info query).

        Returns a dict with ``app_summary``, ``data_flow``, ``app_name``,
        ``persona``, ``app_features`` and ``confident``. The model is told the
        DB may back only ONE service of a larger real-world app, and to mark
        ``confident=false`` when the application is genuinely unclear (so the
        build can fall back to the schema/admin reflection).
        """
        hint = f" User hint: {self._user_description}." if self._user_description else ""
        facts = _profile_fact_block(insight, profile)
        question = (
            f"Given these schema-aware, bounded database facts.{hint}\n\n"
            f"{facts}\n\n"
            "This database may back only ONE service of a larger real-world "
            "application — still predict the real user-facing app it serves, "
            "NOT a CRUD/schema browser over tables.\n"
            "Reply with ONLY a compact JSON object with these keys:\n"
            '"app_name": a short product-style name for the real app; '
            '"persona": the primary human user of that app; '
            '"features": an array of 3-6 concrete user-facing features '
            "(verbs/workflows, not table names); "
            '"app_summary": one or two sentences naming the real-world '
            "application this data supports; "
            '"data_flow": one sentence on the typical data flow (what is '
            "created first, what references what, the main output); "
            '"confident": true only if you can confidently name the real app, '
            "false if the application type is genuinely unclear from the data."
        )
        text = self._ask_in_session(question, None, first)
        parsed = _extract_json(text)
        out: dict[str, Any] = {
            "app_summary": "", "data_flow": "", "app_name": "",
            "persona": "", "app_features": [], "confident": False,
        }
        if isinstance(parsed, dict):
            out["app_summary"] = " ".join(
                str(parsed.get("app_summary") or "").split())
            out["data_flow"] = " ".join(
                str(parsed.get("data_flow") or "").split())
            out["app_name"] = " ".join(str(parsed.get("app_name") or "").split())
            out["persona"] = " ".join(str(parsed.get("persona") or "").split())
            feats = parsed.get("features") or parsed.get("app_features") or []
            if isinstance(feats, list):
                out["app_features"] = [
                    " ".join(str(f).split()) for f in feats if str(f).strip()
                ][:6]
            elif isinstance(feats, str) and feats.strip():
                out["app_features"] = [
                    " ".join(p.split()) for p in feats.split(",") if p.strip()
                ][:6]
            out["confident"] = bool(parsed.get("confident", False))
            return out
        # Non-JSON prose: use it as the summary; treat as low-confidence and let
        # the deterministic fallback fill the rest.
        out["app_summary"] = " ".join(text.split())
        return out

    def _ask_in_session(
        self, question: str, session_id: Optional[str], first: bool,
    ) -> str:
        """Interpret deterministic DB facts without per-turn SQL execution."""
        if self._qa is None:
            return ""
        strengthened = self._strengthen_with_backend(question, question)
        if strengthened:
            return strengthened
        # Compatibility fallback for tests/older integrations that only expose
        # the AI Query Assistant generation API. This does NOT execute generated
        # SQL; it consumes only the cleaned explanation and lets deterministic
        # fallback logic fill anything missing.
        try:
            if (not first) and session_id and hasattr(self._qa, "send_follow_up"):
                result = self._qa.send_follow_up(
                    question, self._db, self._connection, session_id=session_id)
            else:
                result = self._qa.start_new_conversation(
                    question, self._db, self._connection, session_id=session_id)
        except Exception:  # noqa: BLE001
            return ""
        if not isinstance(result, dict):
            return ""
        if result.get("error") and not result.get("explanation"):
            return ""
        return _clean_aiq_explanation(result.get("explanation") or "")

    def _strengthen_with_backend(self, question: str, factual: str) -> str:
        """Use the normal backend chat path over deterministic DB facts."""
        call_ai = getattr(self._qa, "_call_ai", None)
        if not callable(call_ai) or not factual.strip():
            return ""
        prompt = (
            "Use ONLY the schema-aware, bounded database facts below to answer "
            "the original database-understanding request. Do not add "
            "schema-validation warnings, SQL-mode notes, or generic CRUD "
            "language. Preserve the requested output shape exactly (JSON if "
            "requested).\n\n"
            f"ORIGINAL REQUEST:\n{question}\n\n"
            f"DETERMINISTIC DATABASE FACTS:\n{factual}\n"
        )
        from ai_assistant.app_builder.pii_util import mask_if_enabled

        prompt = mask_if_enabled(prompt, self._mask_pii)
        try:
            reply = call_ai(prompt, timeout=90)
        except Exception:  # noqa: BLE001
            return ""
        if isinstance(reply, dict):
            if reply.get("error"):
                return ""
            text = reply.get("response") or reply.get("text") or ""
        else:
            text = str(reply or "")
        return _clean_aiq_explanation(text).strip()


def _extract_json(text: str) -> Any:
    """Best-effort: pull the first JSON object out of a model reply."""
    if not text:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        pass
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except (ValueError, TypeError):
        return None


def _profile_fact_block(insight: DataInsight, profile: DbProfile) -> str:
    """Compact facts for backend interpretation; no free-form DB guessing."""
    lines = [
        f"connection: {insight.connection or profile.connection or 'unknown'}",
        f"db_type: {profile.db_type or 'unknown'}",
        "tables:",
    ]
    by_name = {t.name: t for t in profile.tables}
    for ti in insight.tables[:25]:
        tp = by_name.get(ti.name)
        cols = []
        for col in (getattr(tp, "columns", []) if tp else [])[:12]:
            dtype = getattr(col, "data_type", "") or ""
            marker = " pk" if getattr(col, "is_pk", False) else ""
            cols.append(f"{col.name}({dtype}{marker})" if dtype or marker else col.name)
        if not cols:
            cols = list(ti.columns[:12])
        line = f"- {ti.name}: columns={cols}"
        if ti.row_count is not None:
            line += f"; row_count_estimate={ti.row_count}"
        if ti.sample_rows:
            line += f"; sample={ti.sample_rows[:2]}"
        if getattr(tp, "role", ""):
            line += f"; role={tp.role}"
        lines.append(line)
    rels = getattr(profile, "relationships", []) or []
    if rels:
        lines.append("relationships:")
        for rel in rels[:20]:
            lines.append(
                f"- {rel.get('from_table')}.{rel.get('from_column')} -> "
                f"{rel.get('to_table')}.{rel.get('to_column')} "
                f"({rel.get('kind', 'N:1')}, {rel.get('source', 'inferred')}, "
                f"confidence={rel.get('confidence', 0)})"
            )
    extras = []
    if profile.views:
        extras.append("views=" + ", ".join(profile.views[:10]))
    if profile.indexes:
        extras.append("indexes=" + "; ".join(profile.indexes[:10]))
    if profile.constraints:
        extras.append("constraints=" + "; ".join(profile.constraints[:10]))
    if extras:
        lines.append("metadata: " + " | ".join(extras))
    return "\n".join(lines)


_WARNING_RE = re.compile(
    r"(?:\n|\r|^)\s*(?:⚠️\s*)?SCHEMA VALIDATION WARNINGS:.*$",
    re.IGNORECASE | re.DOTALL,
)


def _clean_aiq_explanation(text: str) -> str:
    """Remove AI Query Assistant schema-warning boilerplate from prose."""
    if not text:
        return ""
    text = _WARNING_RE.sub("", str(text))
    return " ".join(text.split())
