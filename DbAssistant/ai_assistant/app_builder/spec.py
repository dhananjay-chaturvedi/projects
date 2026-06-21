"""Shared app-spec model used by the requirements analyzer and the generators.

An :class:`AppSpec` is the normalized, validated description of the application
to generate: its entities (data models), the features to expose for each entity
(list / create / edit / delete), and which centralized-infra services to wire
in. It is deliberately small and dependency-free so both the deterministic
requirements analyzer and the code generator can share it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# Features the generator knows how to emit for every entity.
KNOWN_FEATURES = ("list", "create", "edit", "delete")

# App archetypes the generator can build. ``crud`` is the admin/management
# fallback; richer archetypes (``storefront`` …) build a real, purpose-specific
# application driven by the user's requirement rather than mirroring DB tables.
# ``insights`` builds a read-only DBA insights/admin dashboard that introspects
# the live database (catalog + stats) rather than CRUD-ing rows.
KNOWN_KINDS = ("crud", "storefront", "insights")


def slug(text: str) -> str:
    """Return a safe snake_case identifier (never empty)."""
    s = re.sub(r"[^A-Za-z0-9_]+", "_", (text or "").strip()).strip("_").lower()
    return s or "item"


def _singular(name: str) -> str:
    base = name.split(".")[-1]
    if base.endswith("ies") and len(base) > 3:
        return base[:-3] + "y"
    if base.endswith("ses") and len(base) > 3:
        return base[:-2]
    if base.endswith("s") and not base.endswith("ss"):
        return base[:-1]
    return base


@dataclass
class Entity:
    """A single data model / table the app manages."""

    name: str
    fields: list[str] = field(default_factory=lambda: ["id"])

    @property
    def table(self) -> str:
        """Storage table name (snake_case, plural-ish as provided)."""
        return slug(self.name)

    @property
    def singular(self) -> str:
        """Snake_case singular form, used for routes and variables."""
        return slug(_singular(self.name))

    @property
    def class_name(self) -> str:
        """PascalCase class name for the generated dataclass."""
        parts = slug(_singular(self.name)).split("_")
        return "".join(p.capitalize() for p in parts if p) or "Row"

    @property
    def label(self) -> str:
        """Human-friendly title for UI headings."""
        return self.name.replace("_", " ").strip().title()

    def safe_fields(self) -> list[str]:
        """Sanitized, de-duplicated field identifiers (always includes id)."""
        out: list[str] = []
        seen: set[str] = set()
        for f in self.fields or []:
            ident = slug(f)
            if ident and ident not in seen:
                seen.add(ident)
                out.append(ident)
        if "id" not in seen:
            out.insert(0, "id")
        return out

    def editable_fields(self) -> list[str]:
        """Fields the user edits (everything except the primary key)."""
        return [f for f in self.safe_fields() if f != "id"]


@dataclass
class AppSpec:
    """Normalized application description handed to the generator."""

    app_name: str = "myapp"
    description: str = ""
    entities: list[Entity] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    features: list[str] = field(default_factory=lambda: list(KNOWN_FEATURES))
    language: str = "python"
    kind: str = "crud"

    def normalized(self) -> "AppSpec":
        """Return a copy with safe defaults (at least one entity + features)."""
        entities = self.entities or [Entity("items", ["id", "name", "description"])]
        feats = [f for f in self.features if f in KNOWN_FEATURES] or list(KNOWN_FEATURES)
        kind = self.kind if self.kind in KNOWN_KINDS else "crud"
        return AppSpec(
            app_name=slug(self.app_name),
            description=self.description,
            entities=entities,
            services=list(self.services),
            features=feats,
            language=self.language or "python",
            kind=kind,
        )

