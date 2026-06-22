"""Balanced, math-driven decision making for the App Builder Assistant.

When the backend AI agent asks a question or recommends a design/architecture
choice, the App Builder Assistant should not blindly relay an answer. Instead it
makes a *balanced* decision that weighs the business intent of the requirement
against design/architecture trade-offs (performance, cost, resource use,
scalability, reliability, simplicity).

To do that deterministically, this module:

1. Makes the requirement **machine-understandable** — :class:`RequirementModel`
   tokenizes the requirement, assigns meaning to terms via a signal lexicon, and
   derives, *on the go*, the dimension **priorities** and non-functional
   **targets** (rules/parameters) for that specific requirement.
2. Scores any candidate option against the requirement with a logical +
   mathematical **fitness meter suite** (:class:`FitnessMeterSuite`), returning a
   :class:`FitnessVerdict` with a per-dimension breakdown, hard-rule violations
   and an aggregate score.
3. Picks the best-fitting option and explains why (:class:`DecisionEngine`).

The whole thing is deterministic and unit-testable — no LLM call is required to
reach or justify a decision.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# The dimensions every requirement and every option is scored on.
DIMENSIONS = (
    "business_fit",   # production/business-grade suitability for the domain
    "performance",    # latency / throughput
    "cost",           # cheapness (1.0 == very cheap to run)
    "resource",       # frugality (1.0 == very light on memory/CPU)
    "scalability",    # ability to grow with load/data
    "reliability",    # availability / correctness / safety
    "simplicity",     # how simple it is to build and operate
)

# ── meaning-based signal lexicon: keyword -> (dimension it raises, strength) ──
# This is how a free-text requirement becomes machine-understandable: each known
# phrase contributes weight to a dimension, so different requirements activate
# different priorities and rules automatically.
_SIGNALS: dict[str, tuple[str, float]] = {
    # performance
    "real-time": ("performance", 1.0), "realtime": ("performance", 1.0),
    "real time": ("performance", 1.0), "low latency": ("performance", 1.0),
    "low-latency": ("performance", 1.0), "fast": ("performance", 0.6),
    "instant": ("performance", 0.8), "responsive": ("performance", 0.6),
    "high throughput": ("performance", 0.9), "throughput": ("performance", 0.7),
    "streaming": ("performance", 0.7), "concurrent": ("performance", 0.6),
    "high performance": ("performance", 1.0),
    # scalability
    "scalable": ("scalability", 1.0), "scale": ("scalability", 0.6),
    "millions": ("scalability", 1.0), "high traffic": ("scalability", 1.0),
    "enterprise": ("scalability", 0.8), "large scale": ("scalability", 1.0),
    "large-scale": ("scalability", 1.0), "distributed": ("scalability", 0.8),
    "growth": ("scalability", 0.5), "high volume": ("scalability", 0.9),
    # cost
    "cheap": ("cost", 1.0), "low cost": ("cost", 1.0), "low-cost": ("cost", 1.0),
    "budget": ("cost", 0.9), "affordable": ("cost", 0.8),
    "cost-effective": ("cost", 0.9), "cost effective": ("cost", 0.9),
    "inexpensive": ("cost", 0.9), "minimal cost": ("cost", 1.0),
    "free": ("cost", 0.6),
    # resource
    "lightweight": ("resource", 1.0), "light weight": ("resource", 1.0),
    "low memory": ("resource", 1.0), "embedded": ("resource", 0.9),
    "efficient": ("resource", 0.6), "minimal resources": ("resource", 1.0),
    "small footprint": ("resource", 0.9), "edge": ("resource", 0.7),
    "resource constrained": ("resource", 1.0),
    # reliability / criticality
    "secure": ("reliability", 0.8), "security": ("reliability", 0.8),
    "compliance": ("reliability", 0.9), "payment": ("reliability", 1.0),
    "payments": ("reliability", 1.0), "financial": ("reliability", 1.0),
    "banking": ("reliability", 1.0), "healthcare": ("reliability", 1.0),
    "mission critical": ("reliability", 1.0), "mission-critical": ("reliability", 1.0),
    "reliable": ("reliability", 0.9), "high availability": ("reliability", 1.0),
    "transactional": ("reliability", 0.8), "audit": ("reliability", 0.7),
    "consistency": ("reliability", 0.7),
    # business_fit (domain importance / production seriousness)
    "revenue": ("business_fit", 0.8), "sales": ("business_fit", 0.6),
    "conversion": ("business_fit", 0.7), "sla": ("business_fit", 0.8),
    "production": ("business_fit", 0.7), "customer-facing": ("business_fit", 0.7),
    # simplicity
    "simple": ("simplicity", 1.0), "mvp": ("simplicity", 1.0),
    "prototype": ("simplicity", 0.9), "quick": ("simplicity", 0.7),
    "minimal": ("simplicity", 0.6), "basic": ("simplicity", 0.6),
    "poc": ("simplicity", 0.9), "proof of concept": ("simplicity", 0.9),
    "demo": ("simplicity", 0.7),
}

# Words that carry no decision signal (dropped from token meaning).
_STOPWORDS = frozenset("""
a an the and or of to for with in on at by from is are be this that it as into
app application build create make use using user users data system tool need want
should would will can please app's our your their them they we i you
""".split())


@dataclass(frozen=True)
class Token:
    """A meaningful requirement term with its assigned meaning + weight."""

    term: str
    category: str   # a DIMENSION name, or "domain" when no signal matched
    weight: float

    def as_dict(self) -> dict[str, object]:
        return {"term": self.term, "category": self.category,
                "weight": round(self.weight, 4)}


@dataclass
class RequirementModel:
    """Machine-understandable view of a requirement.

    ``priorities`` (normalized, sum == 1) and ``targets`` are *derived on the
    go* from the requirement text, so the active rules/parameters differ per
    requirement instead of being hard-coded.
    """

    raw: str
    tokens: list[Token] = field(default_factory=list)
    priorities: dict[str, float] = field(default_factory=dict)
    targets: dict[str, str] = field(default_factory=dict)
    entities: list[str] = field(default_factory=list)
    features: list[str] = field(default_factory=list)
    kind: str = "crud"

    def top_dimensions(self, n: int = 3) -> list[str]:
        return [d for d, _ in sorted(
            self.priorities.items(), key=lambda kv: kv[1], reverse=True)[:n]]

    def as_dict(self) -> dict[str, object]:
        return {
            "priorities": {d: round(v, 4) for d, v in self.priorities.items()},
            "targets": dict(self.targets),
            "tokens": [t.as_dict() for t in self.tokens],
            "top_dimensions": self.top_dimensions(),
            "kind": self.kind,
        }


def _match_signals(text: str) -> list[tuple[str, str, float]]:
    """Return (phrase, dimension, weight) for every signal phrase present."""
    found: list[tuple[str, str, float]] = []
    for phrase, (dim, weight) in _SIGNALS.items():
        # Word-ish boundary match so "scale" doesn't fire inside "scalextric".
        if re.search(r"(?<![a-z])" + re.escape(phrase) + r"(?![a-z])", text):
            found.append((phrase, dim, weight))
    return found


def build_requirement_model(
    description: str,
    *,
    entities: Optional[list[str]] = None,
    features: Optional[list[str]] = None,
    kind: str = "crud",
) -> RequirementModel:
    """Parse a requirement into a :class:`RequirementModel` (tokens + rules)."""
    text = (description or "").lower()
    entities = list(entities or [])
    features = list(features or [])

    # 1) Base priority is uniform; signals add weight to their dimension.
    priorities = {d: 1.0 for d in DIMENSIONS}
    matched = _match_signals(text)
    for _phrase, dim, weight in matched:
        priorities[dim] += weight
    # A requirement that names real entities/features is a real business app, so
    # nudge business_fit up a little (kept modest so it never dominates intent).
    priorities["business_fit"] += 0.25 * min(len(entities) + len(features), 4)
    # Storefront/transactional archetypes lean reliable + business-grade.
    if kind == "storefront":
        priorities["business_fit"] += 0.5
        priorities["reliability"] += 0.3

    total = sum(priorities.values()) or 1.0
    priorities = {d: v / total for d, v in priorities.items()}

    # 2) Tokens with assigned meaning (signal dimension, else "domain").
    sig_terms = {p: (d, w) for p, d, w in matched}
    tokens: list[Token] = []
    seen: set[str] = set()
    for phrase, (dim, weight) in sig_terms.items():
        tokens.append(Token(phrase, dim, weight))
        seen.add(phrase)
    for word in re.findall(r"[a-z][a-z0-9\-]{2,}", text):
        if word in _STOPWORDS or word in seen:
            continue
        seen.add(word)
        tokens.append(Token(word, "domain", 0.3))

    # 3) Derived non-functional targets (the "rules" for this requirement).
    targets = _derive_targets(priorities)

    return RequirementModel(
        raw=description or "", tokens=tokens, priorities=priorities,
        targets=targets, entities=entities, features=features, kind=kind)


def _derive_targets(priorities: dict[str, float]) -> dict[str, str]:
    """Turn priorities into discrete NFR targets used by the hard rules."""
    share = 1.0 / len(DIMENSIONS)
    hi = share * 1.3  # noticeably above an even split

    def cls(dim: str, high: str, std: str) -> str:
        return high if priorities.get(dim, 0.0) > hi else std

    return {
        "scale_class": cls("scalability", "high", "standard"),
        "latency_class": cls("performance", "low", "standard"),
        "budget_class": cls("cost", "tight", "standard"),
        "resource_class": cls("resource", "constrained", "standard"),
        "criticality": cls("reliability", "high", "standard"),
        "complexity_budget": cls("simplicity", "low", "standard"),
    }


# ── option knowledge base: profile each choice across the dimensions ─────────-
@dataclass(frozen=True)
class OptionProfile:
    """A design/architecture option scored on each DIMENSION in [0, 1]."""

    name: str
    category: str
    scores: dict[str, float]
    aliases: tuple[str, ...] = ()


def _p(name, category, aliases=(), **scores):
    return OptionProfile(
        name=name, category=category, aliases=tuple(aliases),
        scores={
            "business_fit": scores["business"],
            "performance": scores["performance"],
            "cost": scores["cost"],
            "resource": scores["resource"],
            "scalability": scores["scalability"],
            "reliability": scores["reliability"],
            "simplicity": scores["simplicity"],
        })


# Compact, defensible profiles for the choices the agent most often raises.
_OPTIONS: tuple[OptionProfile, ...] = (
    # databases / storage
    _p("SQLite", "database", business=0.5, performance=0.55, cost=1.0,
       resource=0.95, scalability=0.25, reliability=0.6, simplicity=0.95,
       aliases=["sqlite", "sqlite3"]),
    _p("PostgreSQL", "database", business=0.9, performance=0.8, cost=0.7,
       resource=0.55, scalability=0.85, reliability=0.95, simplicity=0.6,
       aliases=["postgres", "postgresql", "pg"]),
    _p("MySQL", "database", business=0.85, performance=0.78, cost=0.75,
       resource=0.6, scalability=0.8, reliability=0.85, simplicity=0.65,
       aliases=["mysql"]),
    _p("MariaDB", "database", business=0.85, performance=0.78, cost=0.8,
       resource=0.6, scalability=0.8, reliability=0.85, simplicity=0.65,
       aliases=["mariadb"]),
    _p("MongoDB", "database", business=0.75, performance=0.75, cost=0.6,
       resource=0.5, scalability=0.85, reliability=0.7, simplicity=0.6,
       aliases=["mongodb", "mongo"]),
    _p("Redis", "cache", business=0.7, performance=0.98, cost=0.6,
       resource=0.6, scalability=0.85, reliability=0.7, simplicity=0.7,
       aliases=["redis"]),
    _p("in-memory cache", "cache", business=0.5, performance=0.95, cost=1.0,
       resource=0.7, scalability=0.3, reliability=0.5, simplicity=0.9,
       aliases=["in-memory", "in memory", "memory cache", "local cache"]),
    # app architecture
    _p("synchronous handlers", "concurrency", business=0.6, performance=0.55,
       cost=0.9, resource=0.8, scalability=0.45, reliability=0.8,
       simplicity=0.95, aliases=["synchronous", "sync", "blocking"]),
    _p("async I/O", "concurrency", business=0.7, performance=0.9, cost=0.8,
       resource=0.7, scalability=0.85, reliability=0.75, simplicity=0.55,
       aliases=["async", "asynchronous", "asyncio", "non-blocking"]),
    _p("background worker queue", "concurrency", business=0.75, performance=0.8,
       cost=0.6, resource=0.55, scalability=0.9, reliability=0.8,
       simplicity=0.45, aliases=["celery", "task queue", "worker queue",
                                  "background jobs", "message queue", "rabbitmq",
                                  "kafka"]),
    _p("monolith", "topology", business=0.7, performance=0.7, cost=0.85,
       resource=0.7, scalability=0.5, reliability=0.75, simplicity=0.9,
       aliases=["monolith", "monolithic", "single service"]),
    _p("microservices", "topology", business=0.7, performance=0.7, cost=0.45,
       resource=0.4, scalability=0.95, reliability=0.7, simplicity=0.3,
       aliases=["microservices", "microservice"]),
    _p("server-rendered HTML", "frontend", business=0.7, performance=0.8,
       cost=0.95, resource=0.85, scalability=0.7, reliability=0.85,
       simplicity=0.9, aliases=["server-rendered", "server side rendering",
                                "ssr", "jinja", "templates"]),
    _p("single-page app", "frontend", business=0.75, performance=0.7, cost=0.7,
       resource=0.6, scalability=0.75, reliability=0.7, simplicity=0.5,
       aliases=["spa", "single page", "react", "vue", "angular"]),
)

_OPTION_INDEX: dict[str, OptionProfile] = {}
for _opt in _OPTIONS:
    _OPTION_INDEX[_opt.name.lower()] = _opt
    for _alias in _opt.aliases:
        _OPTION_INDEX[_alias.lower()] = _opt


@dataclass
class FitnessVerdict:
    """Result of scoring one option against a requirement."""

    option: str
    score: float
    breakdown: dict[str, float]   # priority-weighted contribution per dimension
    passed: bool
    violations: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "option": self.option, "score": round(self.score, 4),
            "passed": self.passed,
            "breakdown": {d: round(v, 4) for d, v in self.breakdown.items()},
            "violations": list(self.violations), "notes": list(self.notes),
        }


class FitnessMeterSuite:
    """Logical + mathematical fitness checks for an option vs a requirement."""

    def __init__(self, model: RequirementModel) -> None:
        self.model = model

    def score_profile(self, profile: OptionProfile) -> FitnessVerdict:
        pri = self.model.priorities
        breakdown = {d: pri.get(d, 0.0) * profile.scores.get(d, 0.0)
                     for d in DIMENSIONS}
        score = sum(breakdown.values())  # in [0, 1] since sum(pri) == 1
        violations, notes = self._hard_rules(profile)
        # Each hard-rule violation multiplicatively discounts the score so a
        # mathematically "okay" option that breaks a requirement rule loses.
        for _ in violations:
            score *= 0.5
        return FitnessVerdict(
            option=profile.name, score=round(score, 4), breakdown=breakdown,
            passed=not violations, violations=violations, notes=notes)

    def _hard_rules(self, profile: OptionProfile) -> tuple[list[str], list[str]]:
        t = self.model.targets
        s = profile.scores
        violations: list[str] = []
        notes: list[str] = []
        if t.get("scale_class") == "high" and s["scalability"] < 0.5:
            violations.append(
                f"{profile.name} does not scale enough for a high-scale "
                f"requirement (scalability {s['scalability']:.2f})")
        if t.get("criticality") == "high" and s["reliability"] < 0.6:
            violations.append(
                f"{profile.name} is not reliable enough for a "
                f"mission-critical requirement (reliability {s['reliability']:.2f})")
        if t.get("latency_class") == "low" and s["performance"] < 0.5:
            violations.append(
                f"{profile.name} is too slow for a low-latency requirement "
                f"(performance {s['performance']:.2f})")
        if t.get("budget_class") == "tight" and s["cost"] < 0.4:
            notes.append(
                f"{profile.name} is relatively expensive for a tight budget "
                f"(cost-fit {s['cost']:.2f})")
        if t.get("resource_class") == "constrained" and s["resource"] < 0.5:
            notes.append(
                f"{profile.name} is resource-heavy for a constrained target "
                f"(resource-fit {s['resource']:.2f})")
        if t.get("complexity_budget") == "low" and s["simplicity"] < 0.5:
            notes.append(
                f"{profile.name} adds complexity beyond a simple/MVP scope")
        return violations, notes


@dataclass
class Decision:
    """The App Builder Assistant's balanced answer to an agent question."""

    question: str
    answer: str
    chosen: Optional[str]
    candidates: list[FitnessVerdict] = field(default_factory=list)
    rationale: str = ""
    overrode_proposal: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "question": self.question, "answer": self.answer,
            "chosen": self.chosen, "rationale": self.rationale,
            "overrode_proposal": self.overrode_proposal,
            "candidates": [c.as_dict() for c in self.candidates],
        }


class DecisionEngine:
    """Make a balanced, justified decision for an agent question/recommendation."""

    def __init__(self, model: RequirementModel) -> None:
        self.model = model
        self.suite = FitnessMeterSuite(model)

    def extract_options(self, text: str) -> list[OptionProfile]:
        low = (text or "").lower()
        out: list[OptionProfile] = []
        seen: set[str] = set()
        # Longest aliases first so "in-memory cache" wins over "cache".
        for alias in sorted(_OPTION_INDEX, key=len, reverse=True):
            if re.search(r"(?<![a-z])" + re.escape(alias) + r"(?![a-z])", low):
                opt = _OPTION_INDEX[alias]
                if opt.name not in seen:
                    seen.add(opt.name)
                    out.append(opt)
        return out

    # ── the decision ─────────────────────────────────────────────────────────
    def decide(self, question: str, proposed: str = "") -> Decision:
        options = self.extract_options(f"{question}\n{proposed}")
        if not options:
            return self._generic_decision(question, proposed)

        verdicts = [self.suite.score_profile(o) for o in options]
        # Prefer options that pass the hard rules, then by score.
        best = max(verdicts, key=lambda v: (v.passed, v.score))
        proposed_opts = {o.name for o in self.extract_options(proposed)}
        overrode = bool(proposed_opts) and best.option not in proposed_opts

        rationale = self._rationale(best, verdicts, overrode, proposed_opts)
        answer = f"Use {best.option}. {rationale}"
        return Decision(
            question=question, answer=answer, chosen=best.option,
            candidates=verdicts, rationale=rationale, overrode_proposal=overrode)

    def _rationale(self, best, verdicts, overrode, proposed_opts) -> str:
        tops = ", ".join(self.model.top_dimensions(3))
        parts = [
            f"It best fits this requirement (priorities: {tops}) with fitness "
            f"{best.score:.2f}."
        ]
        if best.violations:
            parts.append("Note: " + "; ".join(best.violations) + ".")
        if best.notes:
            parts.append("Trade-offs: " + "; ".join(best.notes) + ".")
        runners = [v for v in sorted(verdicts, key=lambda v: v.score, reverse=True)
                   if v.option != best.option][:2]
        if runners:
            parts.append("Considered: " + ", ".join(
                f"{v.option} ({v.score:.2f})" for v in runners) + ".")
        if overrode and proposed_opts:
            parts.append(
                f"This overrides the suggested {', '.join(sorted(proposed_opts))} "
                "because the fitness check favors the choice above.")
        return " ".join(parts)

    def _generic_decision(self, question: str, proposed: str) -> Decision:
        """No known option in the question: endorse the proposal with guidance."""
        tops = self.model.top_dimensions(3)
        guidance = (
            f"Optimize for this requirement's priorities ({', '.join(tops)})"
            + (f" and respect the targets {self.model.targets}." if self.model.targets
               else ".")
        )
        base = (proposed or "").strip()
        if base:
            answer = f"{base}\n\nApp Builder Assistant: {guidance}"
        else:
            answer = (
                "Proceed with the simplest option that satisfies the "
                f"requirement. {guidance}")
        return Decision(
            question=question, answer=answer, chosen=None, candidates=[],
            rationale=guidance, overrode_proposal=False)
