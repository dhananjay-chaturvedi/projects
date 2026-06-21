"""Deterministic requirements analysis for the App Builder.

Turns a free-text app description (and/or a live DB schema) into a structured
:class:`AppSpec`, plus a list of clarifying questions the UI can ask the user
"time to time" before building. The logic is intentionally rule-based (no model
call) so it is fast, testable, and reproducible; an AI backend can refine the
draft spec later, but the deterministic result is always a valid baseline.
"""

from __future__ import annotations

import re

from ai_assistant.app_builder.spec import (
    KNOWN_FEATURES,
    AppSpec,
    Entity,
    slug,
)

# Words in a prompt that signal a customer-facing online store rather than an
# internal admin/management tool. Kept specific (sell/shop/cart/checkout/…) so a
# request to merely "manage products" is NOT mistaken for a storefront.
_STOREFRONT_WORDS = (
    "ecommerce", "e-commerce", "e commerce", "storefront", "online store",
    "online shop", "web store", "webshop", "shop", "sell", "selling", "buy",
    "purchase", "cart", "checkout", "marketplace", "catalog", "catalogue",
    "retail", "merch",
)
_ORDER_TABLE_HINTS = ("orders", "order", "sales", "sale", "purchases",
                      "purchase", "carts", "cart", "transactions")
_PRODUCT_TABLE_HINTS = ("products", "product", "items", "item", "catalog",
                        "inventory", "goods", "merch", "sku")
_VAGUE_SCHEMA_PROMPTS = (
    "", "build app", "build an app", "create app", "create an app",
    "build from database", "create from database", "app from database",
    "database app", "use database", "use db", "based on database",
)

_STOP = {
    "app", "application", "system", "tool", "website", "site", "web", "api",
    "data", "database", "user", "users", "the", "a", "an", "and", "or", "with",
    "for", "that", "this", "manage", "manages", "managing", "track", "tracks",
    "tracking", "store", "stores", "list", "lists", "build", "create", "creating",
    "of", "to", "all", "their", "your", "my", "our", "simple", "small",
}

# Verbs/adjectives/fillers that are NOT entities — kept out so the generator does
# not turn every word of the prompt into a database table.
_NON_ENTITY = {
    "add", "added", "adding", "edit", "editing", "update", "updates", "updating",
    "delete", "deleting", "remove", "removing", "view", "viewing", "browse",
    "browsing", "search", "searching", "show", "display", "see", "register",
    "registering", "submit", "insert", "modify", "modifying", "change",
    "changing", "archive", "cancel", "able", "ability", "records", "record",
    "online", "internal", "external", "new", "existing", "full", "complete",
    "various", "different", "multiple", "main", "also", "etc", "using", "based",
    "via", "into", "where", "when", "which", "who", "whom", "allow", "allows",
    "allowing", "let", "lets", "want", "need", "needs", "should", "would",
    "like", "able", "real", "proper", "actual", "good", "nice", "great",
    "page", "pages", "screen", "screens", "button", "buttons", "feature",
    "features", "interface", "interfaces", "able",
}


def _singular(word: str) -> str:
    """Crude singular form used only as a dedupe key (customers ≈ customer)."""
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith(("ses", "xes", "zes", "ches", "shes")) and len(word) > 4:
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 3:
        return word[:-1]
    return word

_FEATURE_HINTS = {
    "create": ("add", "create", "new", "register", "submit", "insert"),
    "edit": ("edit", "update", "modify", "change"),
    "delete": ("delete", "remove", "archive", "cancel"),
    "list": ("list", "view", "browse", "search", "show", "display", "see"),
}


def _candidate_entities(prompt: str) -> list[str]:
    """Pull likely entity nouns from the prompt (conservative heuristic).

    This is only the deterministic *fallback* data model — it intentionally does
    NOT turn every word into a table. It drops verbs/adjectives/UI words, dedupes
    singular/plural forms, and caps the count so an offline scaffold stays small.
    A real, requirement-driven app comes from the AI agent path.
    """
    words = re.findall(r"[A-Za-z][A-Za-z_]+", (prompt or "").lower())
    out: list[str] = []
    seen: set[str] = set()
    for w in words:
        if len(w) < 3 or w in _STOP or w in _NON_ENTITY:
            continue
        key = _singular(w)
        if key in seen:
            continue
        seen.add(key)
        out.append(w)
    return out[:3]


def detect_features(prompt: str) -> list[str]:
    """Detect requested CRUD features from verbs in the prompt.

    When the prompt names no explicit action verbs, default to full CRUD so the
    generated app is genuinely usable; when it does, honor the request but always
    keep listing available.
    """
    low = (prompt or "").lower()
    explicit = [f for f, hints in _FEATURE_HINTS.items() if any(h in low for h in hints)]
    if not explicit:
        return list(KNOWN_FEATURES)
    if "list" not in explicit:
        explicit.append("list")
    return [f for f in KNOWN_FEATURES if f in explicit]


def detect_archetype(description: str = "", schema: dict | None = None) -> str:
    """Infer the *kind* of app to build from the user's request and/or schema.

    The user's stated requirement wins: an "ecommerce app to sell …" builds a
    real storefront, not a CRUD mirror of the tables. When there is no stated
    intent we infer it from the schema shape (a product-like table priced for
    sale alongside an orders/cart table implies a storefront). Everything else
    defaults to a CRUD/management app.
    """
    clean = " ".join((description or "").lower().split())
    low = " " + clean + " "
    if any(w in low for w in _STOREFRONT_WORDS):
        return "storefront"
    # If the user typed a real prompt and it does not ask for a store, honor
    # that prompt. The schema should only infer a storefront when the prompt is
    # blank/vague ("build from database"), otherwise it looks like the UI is
    # ignoring "Describe the app".
    if clean and clean not in _VAGUE_SCHEMA_PROMPTS:
        return "crud"
    tables = {str(t).split(".")[-1].lower() for t in (schema or {})}
    if tables:
        has_price = any(
            any(h in str(c).lower() for h in ("price", "cost", "amount", "mrp"))
            for cols in (schema or {}).values() for c in (cols or [])
        )
        product_like = has_price or any(t in tables for t in _PRODUCT_TABLE_HINTS)
        order_like = any(t in tables for t in _ORDER_TABLE_HINTS)
        if product_like and order_like:
            return "storefront"
    return "crud"


def schema_to_entities(schema: dict[str, list[str]]) -> list[Entity]:
    """Convert an introspected ``{table: [columns]}`` schema to entities."""
    entities: list[Entity] = []
    for table, cols in (schema or {}).items():
        entities.append(Entity(name=str(table), fields=[str(c) for c in (cols or ["id"])]))
    return entities


def derive_spec(
    *,
    app_name: str = "myapp",
    description: str = "",
    schema: dict[str, list[str]] | None = None,
    services: list[str] | None = None,
    features: list[str] | None = None,
    entities: list[str] | None = None,
    kind: str | None = None,
) -> AppSpec:
    """Build a normalized :class:`AppSpec` from the available signals.

    Priority for entities: explicit ``entities`` > introspected ``schema`` >
    nouns mined from the ``description``. The app *kind* (archetype) is inferred
    from the requirement/schema unless supplied explicitly.
    """
    services = list(services or [])
    if schema:
        ents = schema_to_entities(schema)
    elif entities:
        ents = [Entity(name=slug(e), fields=["id", "name", "description"]) for e in entities if e.strip()]
    else:
        mined = _candidate_entities(description)
        ents = [Entity(name=e, fields=["id", "name", "description"]) for e in mined]
    feats = features or detect_features(description)
    spec = AppSpec(
        app_name=app_name or "myapp",
        description=description,
        entities=ents,
        services=services,
        features=feats,
        kind=kind or detect_archetype(description, schema),
    )
    return spec.normalized()


