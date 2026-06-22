"""Curated app archetypes for from_database builds.

Deterministic classifier maps DB profile signals to the nearest archetype.
Archetypes also supply expected surfaces for fidelity meters and a no-AI
fallback target for the deterministic agent.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ai_assistant.app_builder.db_profile import DbProfile


@dataclass(frozen=True)
class ArchetypeMatch:
    id: str
    label: str
    confidence: float
    signals: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Archetype:
    id: str
    label: str
    keywords: tuple[str, ...]
    table_patterns: tuple[str, ...]
  # Surfaces the built app should expose (routes, pages, features).
    expected_surfaces: tuple[str, ...]
    description: str = ""

    def score(self, text: str, table_names: set[str]) -> tuple[float, list[str]]:
        low = text.lower()
        hits: list[str] = []
        score = 0.0
        for kw in self.keywords:
            if kw in low:
                score += 1.5
                hits.append(f"keyword:{kw}")
        for pat in self.table_patterns:
            if any(pat in t for t in table_names):
                score += 2.0
                hits.append(f"table:{pat}")
        return score, hits


ARCHETYPES: tuple[Archetype, ...] = (
    Archetype(
        "credit_card_mgmt", "Credit card / account management",
        ("card", "credit", "account", "balance", "transaction", "payment", "billing"),
        ("card", "account", "transaction", "payment", "statement"),
        ("dashboard", "accounts", "cards", "transactions", "payments", "statements"),
        "User-facing card and account management app.",
    ),
    Archetype(
        "inventory", "Inventory / warehouse",
        ("inventory", "stock", "warehouse", "sku", "product", "supplier", "shipment"),
        ("product", "inventory", "stock", "warehouse", "supplier", "order_item"),
        ("catalog", "stock_levels", "reorder", "suppliers", "shipments"),
        "Inventory tracking and replenishment app.",
    ),
    Archetype(
        "crm", "CRM / customer relationship",
        ("customer", "lead", "contact", "deal", "pipeline", "crm", "account"),
        ("customer", "contact", "lead", "opportunity", "deal", "account"),
        ("contacts", "leads", "pipeline", "deals", "activities"),
        "Sales and customer relationship app.",
    ),
    Archetype(
        "booking", "Booking / reservations",
        ("booking", "reservation", "appointment", "schedule", "room", "ticket"),
        ("booking", "reservation", "appointment", "schedule", "availability"),
        ("calendar", "bookings", "availability", "confirmations"),
        "Scheduling and reservation app.",
    ),
    Archetype(
        "ledger", "Ledger / accounting",
        ("ledger", "journal", "accounting", "debit", "credit", "gl", "invoice"),
        ("ledger", "journal", "invoice", "payment", "account", "gl_entry"),
        ("accounts", "journal", "invoices", "reports", "reconciliation"),
        "Accounting and ledger app.",
    ),
    Archetype(
        "ecommerce", "E-commerce / storefront",
        ("cart", "checkout", "order", "product", "catalog", "store", "sku"),
        ("product", "order", "cart", "customer", "order_item", "category"),
        ("catalog", "cart", "checkout", "orders", "products"),
        "Online storefront app.",
    ),
    Archetype(
        "ticketing", "Support / ticketing",
        ("ticket", "support", "issue", "case", "helpdesk", "sla"),
        ("ticket", "issue", "case", "comment", "assignee"),
        ("tickets", "queue", "assign", "resolve", "sla"),
        "Helpdesk and ticketing app.",
    ),
    Archetype(
        "messaging_notifications", "Messaging / notifications",
        ("notification", "sms", "email", "push", "message", "template",
         "delivery", "recipient", "campaign", "subscriber", "channel"),
        ("notification", "message", "template", "delivery", "recipient",
         "campaign", "subscriber"),
        ("compose", "templates", "delivery_log", "recipients", "analytics"),
        "Notification/messaging service console (e.g. SMS/email delivery).",
    ),
    Archetype(
        "audit_event_log", "Audit / event log",
        ("event", "log", "audit", "activity", "history", "tracking",
         "timeline"),
        ("event", "log", "audit", "activity", "history"),
        ("event_stream", "filters", "entity_timeline", "analytics"),
        "Activity/audit event explorer with timelines and analytics.",
    ),
    Archetype(
        "workflow_queue", "Workflow / job queue",
        ("job", "queue", "task", "worker", "dispatch", "status", "retry",
         "schedule", "pipeline"),
        ("job", "queue", "task", "worker", "dispatch", "attempt"),
        ("queue", "job_detail", "retries", "throughput"),
        "Background job/workflow processing console.",
    ),
    Archetype(
        "generic_crud", "Schema / data admin (fallback)",
        ("admin", "manage", "record", "entity"),
        (),
        ("table_explorer", "record_list", "record_detail", "data_admin"),
        "Explicit fallback: a schema/data-admin reflection used only when the "
        "real-world application cannot be confidently predicted from the data.",
    ),
)


def classify_archetype(
    profile: DbProfile,
    *,
    user_description: str = "",
) -> ArchetypeMatch:
    """Deterministically classify the nearest archetype from profile signals."""
    table_names = {t.name.lower() for t in profile.tables}
    col_text = " ".join(
        c.name for t in profile.tables for c in t.columns
    ).lower()
    blob = " ".join([
        user_description,
        " ".join(table_names),
        col_text,
        " ".join(profile.views).lower(),
    ])
    best_id = "generic_crud"
    best_label = "Generic data admin"
    best_score = 0.0
    best_hits: list[str] = []
    for arch in ARCHETYPES:
        s, hits = arch.score(blob, table_names)
        if s > best_score:
            best_score = s
            best_id = arch.id
            best_label = arch.label
            best_hits = hits
    if best_id == "inventory" and not any(
        token in blob for token in ("inventory", "stock", "warehouse", "sku", "supplier", "shipment")
    ):
        return ArchetypeMatch(
            id="generic_crud", label="Generic data admin",
            confidence=0.35, signals=best_hits,
        )
    if best_score < 2.5:
        return ArchetypeMatch(
            id="generic_crud", label="Generic data admin",
            confidence=0.35, signals=best_hits,
        )
    if best_score <= 0 and user_description.strip():
        best_hits = ["user_description"]
        best_score = 1.0
    structural_bonus = _structural_confidence(profile)
    confidence = min(1.0, (best_score / 6.0) + structural_bonus) if best_score else 0.35
    return ArchetypeMatch(
        id=best_id, label=best_label, confidence=confidence, signals=best_hits,
    )


def _structural_confidence(profile: DbProfile) -> float:
    """Small confidence lift when the schema has a clear app-like graph."""
    relationships = list(getattr(profile, "relationships", []) or [])
    if not relationships:
        return 0.0
    tables = list(getattr(profile, "tables", []) or [])
    incoming: dict[str, int] = {}
    for rel in relationships:
        target = str(rel.get("to_table") or "").lower()
        if target:
            incoming[target] = incoming.get(target, 0) + 1
    if not incoming:
        return 0.0
    role_bonus = 0.0
    roles = {getattr(t, "role", "") for t in tables}
    if "master" in roles and ("transaction" in roles or "junction" in roles):
        role_bonus = 0.08
    hub_bonus = 0.07 if max(incoming.values()) >= 2 else 0.03
    declared_bonus = 0.04 if any(r.get("source") == "declared" for r in relationships) else 0.0
    return min(0.18, role_bonus + hub_bonus + declared_bonus)


def get_archetype(arch_id: str) -> Archetype | None:
    for a in ARCHETYPES:
        if a.id == arch_id:
            return a
    return None


def expected_surfaces(arch_id: str) -> tuple[str, ...]:
    arch = get_archetype(arch_id)
    return arch.expected_surfaces if arch else ("list", "detail")
