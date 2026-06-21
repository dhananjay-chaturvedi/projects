"""design_plan — a normalized, comparable view of a session's understanding.

During the initialization (understanding) phase each session produces a
free-text plan from its own role:

* Session A (builder)   → the files/folders and components it will create.
* Session B (advisor)   → the business design: entities, features, components.
* Session C (validator) → the validation surface: components and test cases.

To check whether the three sessions *agree* before building, we need to compare
these plans syntactically. :func:`extract_plan` parses free text into a
:class:`DesignPlan` of normalized token sets (entities, components, features,
endpoints, files, data fields). The :class:`DesignSimilarityMeter` then scores
the overlap between any set of plans.

Everything here is deterministic string/set processing — no model calls.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from ai_assistant.meters.requirement_fidelity_meter import _STOP as _STOP_LOCAL

# Component synonyms → a canonical component name. Using a fixed map (instead
# of morphological stemming) means "routes", "router", "route", "endpoint" all
# collapse to the same canonical layer, so aligned plans that use different
# wording for the same module still match.
_COMPONENT_CANON = {
    "model": "model", "models": "model", "schema": "model", "schemas": "model",
    "entity": "model", "entities": "model", "table": "model", "tables": "model",
    "router": "route", "route": "route", "routes": "route",
    "endpoint": "route", "endpoints": "route", "api": "route",
    "controller": "route", "controllers": "route",
    "service": "service", "services": "service", "repository": "service",
    "repositories": "service", "crud": "service", "logic": "service",
    "view": "view", "views": "view", "template": "view", "templates": "view",
    "page": "view", "pages": "view", "ui": "view", "frontend": "view",
    "form": "view", "forms": "view",
    "auth": "auth", "authentication": "auth", "authorization": "auth",
    "login": "auth",
    "database": "database", "db": "database",
    "migration": "migration", "migrations": "migration",
    "test": "test", "tests": "test", "testing": "test",
    "config": "config", "settings": "config", "configuration": "config",
    "seed": "seed", "seeds": "seed", "fixtures": "seed", "fixture": "seed",
    "sample": "seed",
    "middleware": "middleware", "handler": "route", "handlers": "route",
    "backend": "backend",
}
_COMPONENT_HINTS = tuple(_COMPONENT_CANON)

_FILE_RE = re.compile(r"[\w./-]+\.[A-Za-z]{1,5}\b")
_ENDPOINT_RE = re.compile(
    r"(?:GET|POST|PUT|PATCH|DELETE)\s+(/[\w/{}.:-]*)", re.IGNORECASE)
_PATH_RE = re.compile(r"(?<![\w])(/[a-zA-Z][\w/{}-]*)")
# Data nouns are recognized when tied to data keywords, e.g. "orders table",
# "customer model", "entities: customer, order", "models for customers".
_ENTITY_KEYWORDS = (
    "model", "models", "entity", "entities", "table", "tables", "resource",
    "resources", "object", "objects", "record", "records",
)
# Structural / prose words that are never domain entities.
_ENTITY_NOISE = {
    "src", "lib", "pkg", "dir", "folder", "file", "files", "code", "core",
    "design", "feature", "features", "component", "components", "validation",
    "validate", "main", "app", "apps", "project", "module", "modules", "flow",
    "flows", "logic", "layer", "layers", "structure", "part", "parts", "item",
    "items", "thing", "things", "etc", "various", "several", "multiple",
}
# CRUD / flow feature verbs the plans tend to share.
_FEATURE_HINTS = (
    "create", "read", "update", "delete", "list", "search", "filter", "login",
    "logout", "register", "signup", "checkout", "cart", "payment", "export",
    "import", "upload", "download", "report", "dashboard", "notify", "assign",
    "approve", "schedule", "book", "cancel", "refund", "review", "comment",
    "rate", "favorite", "share", "validate", "authenticate", "authorize",
)


def _normalize(token: str) -> str:
    """Collapse a noun to a canonical singular stem so 'orders'≈'order'."""
    t = token.lower().strip()
    if t.endswith("ies") and len(t) > 4:
        return t[:-3] + "y"
    if t.endswith("ses") and len(t) > 4:
        return t[:-2]
    if t.endswith("s") and not t.endswith("ss") and len(t) > 3:
        return t[:-1]
    return t


def _canon_feature(token: str) -> str:
    """Canonical feature verb (singular, present)."""
    return _normalize(token)


@dataclass
class DesignPlan:
    """A normalized, set-based view of one session's design understanding."""

    role: str = ""
    entities: set[str] = field(default_factory=set)
    components: set[str] = field(default_factory=set)
    features: set[str] = field(default_factory=set)
    endpoints: set[str] = field(default_factory=set)
    files: set[str] = field(default_factory=set)
    raw: str = ""

    def as_dict(self) -> dict:
        return {
            "role": self.role,
            "entities": sorted(self.entities),
            "components": sorted(self.components),
            "features": sorted(self.features),
            "endpoints": sorted(self.endpoints),
            "files": sorted(self.files),
        }

    def signature_sets(self) -> dict[str, set[str]]:
        """The dimensions compared by the similarity meter."""
        return {
            "entities": set(self.entities),
            "components": set(self.components),
            "features": set(self.features),
            "endpoints": set(self.endpoints),
        }


def extract_plan(text: str, *, role: str = "",
                 entities: list[str] | None = None,
                 features: list[str] | None = None) -> DesignPlan:
    """Parse a free-text plan into a normalized :class:`DesignPlan`.

    Known entities/features (from the requirement model) are seeded so a plan
    that simply restates the requirement still registers them.
    """
    text = text or ""
    low = text.lower()

    ents: set[str] = {_normalize(e) for e in (entities or []) if e}
    feats: set[str] = {_canon_feature(f) for f in (features or []) if f}
    comps: set[str] = set()
    endpoints: set[str] = set()
    files: set[str] = set()

    for m in _FILE_RE.findall(text):
        # Keep only plausible source files, not version numbers like 3.12.
        base = m.split("/")[-1]
        if "." in base and not base[0].isdigit():
            files.add(base.lower())

    for ep in _ENDPOINT_RE.findall(text):
        endpoints.add(ep.lower().rstrip("/") or "/")
    for p in _PATH_RE.findall(text):
        # Treat top-level resource paths (/orders, /cart) as endpoints.
        seg = p.lower().rstrip("/")
        if 1 < len(seg) <= 40 and "/" not in seg[1:]:
            endpoints.add(seg)

    raw_words = re.findall(r"[A-Za-z][A-Za-z_]+", text)
    lowered = [w.lower() for w in raw_words]

    for w in lowered:
        canon = _COMPONENT_CANON.get(w)
        if canon:
            comps.add(canon)
    for hint in _FEATURE_HINTS:
        if re.search(r"\b" + re.escape(hint) + r"\b", low):
            feats.add(_canon_feature(hint))

    # Entities are detected only when a noun sits directly next to a data
    # keyword (e.g. "orders table", "customer model", "entities: customer,
    # order"). A tight ±2 window keeps the entity set high-signal instead of
    # grabbing every prose noun, so genuinely-aligned plans don't diverge on
    # incidental wording.
    for i, w in enumerate(lowered):
        if w not in _ENTITY_KEYWORDS:
            continue
        window = lowered[max(0, i - 2):i] + lowered[i + 1:i + 3]
        for cand in window:
            if (len(cand) >= 3 and cand not in _STOP_LOCAL
                    and cand not in _COMPONENT_CANON
                    and cand not in _FEATURE_HINTS
                    and cand not in _ENTITY_KEYWORDS
                    and cand not in _ENTITY_NOISE):
                ents.add(_normalize(cand))

    return DesignPlan(
        role=role, entities=ents, components=comps, features=feats,
        endpoints=endpoints, files=files, raw=text[:4000],
    )
