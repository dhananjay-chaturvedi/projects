"""understanding — the parallel initialization phase ("session understanding").

When a build starts, the user's "describe the app" prompt is handed to all three
sessions **in parallel**, and each prepares its role-specific groundwork before a
single file is written:

* Session A (builder)   → the files/folders/components it will create.
* Session B (advisor)   → the business design: entities, components, features.
* Session C (validator) → the validation surface: components + test cases.

The three plans are parsed into :class:`DesignPlan` objects and compared with the
:class:`DesignSimilarityMeter`. Similarity is a **confidence signal** — not a
reason to spend another LLM round re-asking all three sessions. The agreed design
is built deterministically as the best-of-all-three merge (role-aware union).

The phase runs the three preparation calls concurrently (threads), because each
session is an independent backend session; a failure or missing method for any
role degrades gracefully to whatever plans are available.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from ai_assistant.app_builder.assistant import AppBuilderAssistant
from ai_assistant.app_builder.meters.design_plan import DesignPlan, extract_plan

# Shared constraints for all understanding-prep turns: ask-mode outlines only.
_OUTLINE_RULES = (
    "OUTLINE ONLY — do NOT create files, write code, or run tests. "
    "Return at most 15 concise bullet lines. Structured list only."
)


def _builder_prompt(description: str) -> str:
    return (
        f"{_OUTLINE_RULES}\n"
        "You are the BUILDER. List the implementation outline you will build: "
        "folders/files, data models, routes/endpoints, and core features.\n"
        f"APP: {description}")


def _advisor_prompt(description: str) -> str:
    return (
        f"{_OUTLINE_RULES}\n"
        "You are the ADVISOR standing in for the user. Enumerate the BUSINESS "
        "DESIGN: entities/data models, components/modules, and core features/flows "
        "the builder must deliver.\n"
        f"APP: {description}")


def _validator_prompt(description: str) -> str:
    return (
        f"{_OUTLINE_RULES}\n"
        "You are the VALIDATOR (read-only). Draft a concise test/validation "
        "outline: health/boot, core flows, edge cases, and sample-data coverage. "
        "At most 12 bullet points.\n"
        f"APP: {description}")


def _validator_prompt_from_instruction(instruction: str) -> str:
    return (
        f"{_OUTLINE_RULES}\n"
        "You are the VALIDATOR (read-only). Session B gave you the instruction "
        "below. Restate your understanding: what you will validate and test "
        "(health/boot, core flows, edge cases, sample-data coverage). "
        "At most 12 bullet points.\n"
        f"SESSION B INSTRUCTION:\n{instruction}")


@dataclass
class UnderstandingResult:
    """Outcome of the understanding phase."""

    ready: bool = False
    similarity: dict[str, Any] = field(default_factory=dict)
    plans: dict[str, DesignPlan] = field(default_factory=dict)
    agreed_design: Optional[DesignPlan] = None
    rounds: int = 0
    plan_texts: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "similarity": self.similarity,
            "rounds": self.rounds,
            "plans": {k: v.as_dict() for k, v in self.plans.items()},
            "agreed_design": (self.agreed_design.as_dict()
                              if self.agreed_design else None),
        }


def _best_of_plans(plans: dict[str, DesignPlan]) -> DesignPlan:
    """Deterministic best-of-all-three merge (no extra LLM reconcile round).

    Each role contributes its natural strengths:
    * A (builder)   → files, endpoints, implementation components
    * B (advisor)   → entities, features, business components
    * C (validator) → validation components and test-relevant features
    """
    merged = DesignPlan(role="agreed")
    a = plans.get("A")
    b = plans.get("B")
    c = plans.get("C")

    if a:
        merged.files |= a.files
        merged.endpoints |= a.endpoints
        merged.components |= a.components
        merged.entities |= a.entities
        merged.features |= a.features
    if b:
        merged.entities |= b.entities
        merged.features |= b.features
        merged.components |= b.components
    if c:
        merged.components |= c.components
        merged.features |= c.features

    # Safety net: union anything still missing from sparse role outputs.
    for p in plans.values():
        merged.entities |= p.entities
        merged.components |= p.components
        merged.features |= p.features
        merged.endpoints |= p.endpoints
        merged.files |= p.files
    return merged


def _run_parallel(tasks: dict[str, Callable[[], str]]) -> dict[str, str]:
    """Run role preparation callables concurrently; degrade gracefully."""
    results: dict[str, str] = {}
    if not tasks:
        return results
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {pool.submit(fn): role for role, fn in tasks.items()}
        for fut in list(futures):
            role = futures[fut]
            try:
                results[role] = fut.result() or ""
            except Exception as exc:  # noqa: BLE001
                results[role] = ""
                results[f"_{role}_error"] = str(exc)
    return results


@dataclass(frozen=True)
class UnderstandingPhaseRequest:
    """Inputs for the app-builder understanding phase."""

    assistant: AppBuilderAssistant
    description: str = ""
    entities: Optional[list[str]] = None,
    features: Optional[list[str]] = None,
    threshold: float = 0.75
    max_reconcile: int = 0
    cancelled: Optional[Callable[[], bool]] = None
    builder_instruction: str = ""
    validator_instruction: str = ""
    advisor_design: str = ""


def run_understanding_phase(
    request: UnderstandingPhaseRequest | AppBuilderAssistant,
    description: str = "",
    **legacy,
) -> UnderstandingResult:
    """Run the parallel initialization + design-similarity gate.

    Returns an :class:`UnderstandingResult`. ``ready`` is True when similarity
    meets *threshold*; otherwise the build still proceeds with the deterministic
    best-of-three ``agreed_design`` and the low similarity is recorded for
    visibility. No LLM reconcile rounds run by default (``max_reconcile=0``).
    """
    if not isinstance(request, UnderstandingPhaseRequest):
        request = UnderstandingPhaseRequest(
            assistant=request,
            description=description,
            **legacy,
        )
    assistant = request.assistant
    description = request.description
    entities = list(request.entities or [])
    features = list(request.features or [])
    threshold = request.threshold
    max_reconcile = request.max_reconcile
    cancelled = request.cancelled
    builder_instruction = request.builder_instruction
    validator_instruction = request.validator_instruction
    advisor_design = request.advisor_design
    if not entities and description:
        from ai_assistant.meters.requirement_fidelity_meter import _tokens
        entities = _tokens(description)[:12]
    if not features and description:
        from ai_assistant.app_builder.meters.design_plan import _FEATURE_HINTS
        low = description.lower()
        features = [h for h in _FEATURE_HINTS if h in low]
    builder = assistant.builder
    advisor = assistant.advisor
    validator = assistant.validator
    brief = assistant.brief

    def _cancel() -> bool:
        return bool(cancelled and cancelled())

    b_instr = (builder_instruction or "").strip()
    v_instr = (validator_instruction or "").strip()
    b_design = (advisor_design or "").strip()

    def prep_builder() -> str:
        # FROM_DATABASE: Session B already authored the plan/instruction. Session
        # A follows it verbatim and must NOT take a read-only outline turn (a
        # read-only ask/plan turn can disable writes on its persistent session).
        # A's "understanding" IS B's instruction; the real build runs later in
        # write mode. This also avoids spending an extra A turn.
        if b_instr:
            return b_instr
        # FROM_SCRATCH: no pre-authored instruction — A drafts a quick outline.
        prompt = _builder_prompt(description)
        fn = getattr(builder, "prepare_outline", None)
        if callable(fn):
            return fn(prompt, brief=brief) or ""
        fn = getattr(builder, "plan", None)
        if callable(fn):
            return fn(_builder_prompt(description)) or ""
        send = getattr(builder, "send", None)
        if callable(send):
            send(_builder_prompt(description))
            return getattr(builder, "last_text", "") or ""
        return ""

    def prep_advisor() -> str:
        if b_design:
            return b_design
        fn = getattr(advisor, "frame_answer", None)
        if callable(fn):
            return fn(_advisor_prompt(description), brief=brief, context="") or ""
        return ""

    def prep_validator() -> str:
        if validator is None:
            return ""
        v_prompt = (_validator_prompt_from_instruction(v_instr) if v_instr
                    else _validator_prompt(description))
        outline_fn = getattr(validator, "prepare_outline", None)
        if callable(outline_fn):
            return outline_fn(
                description, brief=brief, context=v_prompt) or ""
        fn = getattr(validator, "prepare_test_plan", None)
        if callable(fn):
            return fn(description, brief=brief,
                      context=_validator_prompt(description)) or ""
        return ""

    tasks: dict[str, Callable[[], str]] = {
        "A": prep_builder, "B": prep_advisor,
    }
    if validator is not None:
        tasks["C"] = prep_validator

    result = UnderstandingResult()
    if _cancel():
        return result

    texts = _run_parallel(tasks)
    result.plan_texts = {k: v for k, v in texts.items() if not k.startswith("_")}

    def _plans_from(texts: dict[str, str]) -> dict[str, DesignPlan]:
        out: dict[str, DesignPlan] = {}
        role_map = {"A": "builder", "B": "advisor", "C": "validator"}
        for role, label in role_map.items():
            if role in texts:
                out[role] = extract_plan(
                    texts[role], role=label, entities=entities,
                    features=features)
        return out

    plans = _plans_from(result.plan_texts)
    result.plans = plans

    sim = assistant.check_design_similarity(list(plans.values()),
                                            threshold=threshold)
    result.similarity = sim
    result.rounds = 1

    # Optional legacy reconcile (disabled by default). Kept for tests/back-compat.
    if max_reconcile > 0 and not sim.get("passed") and not _cancel():
        from ai_assistant.app_builder.assistant import Session

        def _share_for_reconcile() -> None:
            summary_lines = [
                "Other sessions' design plans (reconcile toward agreement):"]
            for role, plan in plans.items():
                d = plan.as_dict()
                summary_lines.append(
                    f"[{role}] entities={d['entities']} "
                    f"components={d['components']} features={d['features']}")
            try:
                assistant.assistant_note("\n".join(summary_lines), to=Session.A)
            except Exception:  # noqa: BLE001
                pass

        prev_score = float(sim.get("score", 0.0))
        reconcile_rounds = 0
        while (not sim.get("passed") and reconcile_rounds < max_reconcile
               and not _cancel()):
            _share_for_reconcile()
            texts = _run_parallel(tasks)
            result.plan_texts = {
                k: v for k, v in texts.items() if not k.startswith("_")}
            plans = _plans_from(result.plan_texts)
            result.plans = plans
            sim = assistant.check_design_similarity(list(plans.values()),
                                                    threshold=threshold)
            result.similarity = sim
            result.rounds += 1
            reconcile_rounds += 1
            new_score = float(sim.get("score", 0.0))
            if new_score <= prev_score + 1e-6:
                break
            prev_score = new_score

    agreed = _best_of_plans(plans)
    assistant.design = agreed
    result.agreed_design = agreed
    result.ready = bool(sim.get("passed"))
    return result
