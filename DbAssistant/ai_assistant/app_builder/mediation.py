"""The App Builder Assistant as a *mediator* between Session 1 and Session 2.

Session 1 (builder) does the work and occasionally asks a question. Session 2
(advisor) answers — but a bare question with a raw transcript tail is poor
context. The App Builder Assistant sits in the middle and hands the advisor a
focused brief: *why* the builder asked (the surrounding context), *where* the
build is right now (live progress against the requirement), and *what* is
expected back (a concise, balanced recommendation — no code).

This keeps both sessions aligned on context and progress without changing the
existing AI Query Assistant or chat-button logic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

from ai_assistant.app_builder.agent_runner import agent_signaled_done

# Heuristic: a reply that contains code/file content instead of advice.
_CODE_HINT_RE = re.compile(
    r"```|^\s*(?:def |class |import |from \w[\w.]* import |<[a-z!/])",
    re.MULTILINE)

# Gaps are produced by a deterministic keyword/token scan. They are a useful
# hint but can be stale or wrong (e.g. a feature is implemented under a synonym
# the scan missed). Always present them as candidates the advisor must verify
# against the builder's actual work — never as a hard "requirement not met".
_GAP_DISCLAIMER = (
    "POSSIBLE REMAINING REQUIREMENTS (auto-detected by a keyword scan — these "
    "may be stale or inaccurate; CHECK each one against what the builder has "
    "actually done before raising it, and do NOT re-request anything already "
    "implemented):")


@dataclass
class BuildProgress:
    """A live snapshot of the build the mediator shares with the advisor."""

    phase: str = "build"
    round: int = 0
    coverage: float = 0.0
    score: float = 0.0
    accepted: bool = False
    files_built: int = 0
    gaps: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)

    def summary(self) -> str:
        state = "accepted" if self.accepted else "in progress"
        head = (f"round {self.round} [{self.phase}] — {state}; "
                f"coverage {self.coverage:.2f}, build score {self.score:.2f}, "
                f"{self.files_built} files")
        return head


@dataclass
class ReplyReview:
    """The App Builder Assistant's validation of Session B's reply to A.

    ``injected_rules`` are alignment directives appended to the answer forwarded
    to Session A so both sessions stay pointed at the requirement, completion and
    testing — even when the advisor drifts.
    """

    aligned: bool = True
    issues: list[str] = field(default_factory=list)
    injected_rules: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "aligned": self.aligned,
            "issues": list(self.issues),
            "injected_rules": list(self.injected_rules),
        }


@dataclass
class ContextMediator:
    """Mediates between Session 1 and Session 2, framing context + expectations.

    ``requirement_model`` is a :class:`~ai_assistant.app_builder.decision.
    RequirementModel` (optional) so the advisor knows what the app is optimizing
    for; ``brief`` is the :class:`GovernanceBrief` (optional) for app metadata.
    """

    requirement_model: Any = None
    brief: Any = None
    #: When False (from_scratch), B/C must not enforce fixed structure/framework.
    structure_enforced: bool = True
    #: how many transcript lines of surrounding context to give the advisor
    window: int = 6

    # ── context around Session 1's question ──────────────────────────────────
    def question_context(self, question: str, builder_text: str) -> str:
        """Lines around where the builder raised *question* (why it asked)."""
        text = (builder_text or "").strip()
        if not text:
            return ""
        lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
        if not lines:
            return ""
        q_norm = " ".join((question or "").lower().split())
        idx = -1
        for i, ln in enumerate(lines):
            if q_norm and q_norm[:60] in " ".join(ln.lower().split()):
                idx = i
                break
        if idx == -1:
            # Question not found verbatim (e.g. permission) → use the tail.
            picked = lines[-self.window:]
        else:
            start = max(0, idx - self.window)
            picked = lines[start:idx + 1]
        return "\n".join(f"  {ln}" for ln in picked)

    # ── what is expected from Session 2 ──────────────────────────────────────
    def expectation(self, kind: str = "question") -> str:
        if kind == "permission":
            return ("Decide whether to APPROVE the builder's requested change, "
                    "weighing safety and scope. One short sentence.")
        dims = "business value, performance, cost, resource use, scalability, reliability"
        return (
            "Give ONE balanced recommendation the builder can act on immediately. "
            f"Weigh {dims}. Keep it to <=4 sentences, no code, no file contents — "
            "advise, do not implement.")

    # ── monitor Session B's reply + inject alignment rules ───────────────────
    def review_reply(
        self,
        question: str,
        reply: str,
        *,
        progress: Optional[BuildProgress] = None,
    ) -> ReplyReview:
        """Validate the advisor's reply and derive alignment directives.

        The App Builder Assistant keeps Session B on task: its reply must be
        direction (not code), must not declare completion while requirements
        remain, and must keep the build pointed at the requirement, testing and
        the open gaps. Anything off becomes an injected rule appended to the
        answer sent to Session A.
        """
        text = reply or ""
        low = text.lower()
        gaps = list(progress.gaps) if (progress and progress.gaps) else []
        accepted = bool(progress.accepted) if progress else False

        issues: list[str] = []
        rules: list[str] = []

        if _CODE_HINT_RE.search(text):
            issues.append("advisor reply contained code instead of direction")
            rules.append("Advise with direction only — do NOT write code or file "
                         "contents; the builder (Session A) writes all code.")

        if gaps and agent_signaled_done(text):
            issues.append("reply implies completion while requirements remain")
            if self.structure_enforced:
                rules.append("The app is NOT complete — required features are still "
                             "missing; keep building, do not stop.")
            else:
                rules.append("Consider: the user requirement may not be fully "
                             "reflected yet — keep building toward it.")

        if self.structure_enforced:
            mentions_test = any(w in low for w in
                                ("test", "pytest", "validate", "verify", "assert"))
            if (gaps or not accepted) and not mentions_test:
                issues.append(
                    "reply omits testing/validation while build is incomplete")
                rules.append("Every change must add or update tests and pass the "
                             "meters/managers before moving on.")

        foci = self._foci()
        if foci and not any(f.split("_")[0] in low for f in foci):
            rules.append("Keep the recommendation aligned to the requirement "
                         "priorities: " + ", ".join(foci) + ".")

        if gaps:
            joined = "; ".join(gaps[:5])
            if self.structure_enforced:
                rules.append(
                    "These requirements were auto-detected as possibly missing — "
                    "verify each against what the builder has already done and "
                    "direct the builder to close ONLY the ones still genuinely "
                    f"missing (skip any already implemented): {joined}.")
            else:
                rules.append(
                    "Auto-detected possible gaps (verify before raising; skip any "
                    f"already implemented): {joined}.")

        # User-perspective quality check — is B answering as the user would?
        req = self._requirement_text()
        if req:
            try:
                from ai_assistant.meters.quality_manager import QualityManager
                qm = QualityManager()
                qr = qm.review_advisor_reply(req, text)
                if not qr.aligned and qr.nudge:
                    rules.append(qr.nudge)
            except Exception:  # noqa: BLE001
                pass

        return ReplyReview(aligned=not issues, issues=issues, injected_rules=rules)

    def directives_text(self, rules: list[str]) -> str:
        """Render injected alignment rules as a directive block for Session A."""
        if not rules:
            return ""
        body = "\n".join(f"- {r}" for r in rules)
        if self.structure_enforced:
            return ("\n\nApp Builder Assistant directives (stay aligned to the "
                    "requirement, testing and completion):\n" + body)
        return ("\n\nApp Builder Assistant suggestions (requirement focus — "
                  "not structural enforcement):\n" + body)

    # ── the full mediation brief handed to the advisor ───────────────────────
    def advisor_context(
        self,
        question: str,
        *,
        builder_text: str = "",
        history: str = "",
        progress: Optional[BuildProgress] = None,
        kind: str = "question",
    ) -> str:
        """Intent-first brief: real question → requirement → progress → placement."""
        lines = ["=== App Builder Assistant — mediation brief for the ADVISOR ==="]

        app_name = self._app_name()
        if app_name:
            lines.append(f"APP: {app_name}")

        # (1) The real ask — what A wants confirmed/approved/decided.
        lines += [
            "ASK INTENT:",
            f"  {kind} — answer the builder's real question first.",
            "THE BUILDER'S QUESTION:",
            f"  {question.strip()}",
        ]
        ctx = self.question_context(question, builder_text)
        if ctx:
            lines += ["WHERE IN THE BUILD (current context):", ctx]

        # (2) App requirement.
        req = self._requirement_text()
        if req:
            lines.append(f"USER REQUIREMENT: {req}")
        focus = self._focus_line()
        if focus:
            lines.append(focus)
        targets = self._targets_line()
        if targets:
            lines.append(targets)

        # (3) Build progress.
        if progress is not None:
            lines.append(f"BUILD PROGRESS: {progress.summary()}")
            if progress.gaps:
                lines.append(_GAP_DISCLAIMER)
                lines += [f"  - {g}" for g in progress.gaps[:8]]
            if progress.suggestions:
                lines.append("OPTIONAL ADD-ONS (non-blocking suggestions):")
                lines += [f"  - {s}" for s in progress.suggestions[:5]]

        # (4) History for placement in the overall build.
        hist = _clip(history, 900)
        if hist:
            lines += ["WHAT THE BUILDER HAS DONE SO FAR (history):", f"  {hist}"]

        lines += [
            "WHAT WE NEED FROM YOU (the advisor):",
            f"  {self.expectation(kind)}",
            "=== END MEDIATION BRIEF ===",
        ]
        return "\n".join(lines)

    def user_request_context(
        self,
        request: str,
        *,
        history: str = "",
        progress: Optional[BuildProgress] = None,
    ) -> str:
        """Brief for Session B when the user sends a request (user → B → A)."""
        lines = [
            "=== App Builder Assistant — user request for the ADVISOR ===",
            "The USER sent a request. Frame it into ONE clear instruction or "
            "answer the builder (Session A) can act on immediately.",
            f"USER REQUEST: {request.strip()}",
        ]
        req = self._requirement_text()
        if req:
            lines.append(f"USER REQUIREMENT: {req}")
        if progress is not None:
            lines.append(f"BUILD PROGRESS: {progress.summary()}")
            if progress.gaps:
                lines.append(_GAP_DISCLAIMER)
                lines += [f"  - {g}" for g in progress.gaps[:8]]
        hist = _clip(history, 900)
        if hist:
            lines += ["WHAT THE BUILDER HAS DONE SO FAR:", f"  {hist}"]
        lines += [
            "WHAT WE NEED FROM YOU:",
            "  First reconcile what is already built against the request, then "
            "translate the user request into a concise instruction for the "
            "builder — no code, no file contents. Do NOT re-request work that is "
            "already done.",
            "=== END USER REQUEST BRIEF ===",
        ]
        return "\n".join(lines)

    # ── context for the validator (Session C) ────────────────────────────────
    def validator_context(
        self,
        *,
        progress: Optional[BuildProgress] = None,
        history: str = "",
        test_scope: str = "",
        how_to_test: str = "",
        framed_brief: str = "",
        component: str = "",
    ) -> str:
        """Brief for Session C: the requirement, A's progress, what + how to test.

        Session C shares Session A's context (requirement, focus, progress,
        history) so its judgement is grounded, but it only receives a compact
        brief — never the full transcript — to keep token use minimal.
        """
        lines = ["=== App Builder Assistant — validation brief for the VALIDATOR ==="]
        app_name = self._app_name()
        if app_name:
            lines.append(f"APP: {app_name}")
        brief = _clip(framed_brief, 600)
        if brief:
            lines.append(f"BUILD BRIEF (from advisor): {brief}")
        req = self._requirement_text()
        if req:
            lines.append(f"USER REQUIREMENT: {req}")
        focus = self._focus_line()
        if focus:
            lines.append(focus)
        if component:
            lines.append(f"COMPONENT UNDER TEST: {component}")

        if progress is not None:
            lines.append(f"BUILD PROGRESS: {progress.summary()}")
            if progress.gaps:
                lines.append(_GAP_DISCLAIMER)
                lines += [f"  - {g}" for g in progress.gaps[:8]]

        hist = _clip(history, 600)
        if hist:
            lines += ["WHAT THE BUILDER HAS DONE SO FAR:", f"  {hist}"]
        if test_scope:
            lines += ["TESTING SCOPE:", f"  {_clip(test_scope, 400)}"]
        if how_to_test:
            lines += ["HOW TO TEST:", f"  {_clip(how_to_test, 400)}"]

        if not self.structure_enforced:
            lines += [
                "STRUCTURE FREEDOM: Session A chooses its own folders/files. Do NOT "
                "flag missing pre-decided frameworks, standard test folders, or "
                "manifest files as issues — only judge whether the app fulfils the "
                "USER REQUIREMENT and general good practices (health, flows, tests).",
            ]
        lines += [
            "WHAT WE NEED FROM YOU (the validator):",
            "  Judge completeness vs the requirement from the evidence; list only "
            "real, actionable issues/recommendations — no code.",
            "=== END VALIDATION BRIEF ===",
        ]
        return "\n".join(lines)

    def validation_to_advice(
        self,
        findings: str,
        *,
        progress: Optional[BuildProgress] = None,
    ) -> str:
        """Frame Session C's findings as a request for the advisor (Session B).

        B turns this into ONE concrete next instruction for the builder, so the
        validator's feedback reaches A as actionable direction (A never sees C).
        """
        gaps = "; ".join(progress.gaps[:5]) if (progress and progress.gaps) else ""
        parts = [
            "The VALIDATOR (Session C) reviewed the build and reported the "
            "following. Translate it into ONE concrete next instruction for the "
            "builder to act on — fix the most important issue first.",
            f"VALIDATOR FINDINGS:\n{_clip(findings, 800)}",
        ]
        if gaps:
            parts.append(f"OPEN REQUIREMENTS: {gaps}")
        return "\n".join(parts)

    def validation_green_note(
        self,
        component: str,
        *,
        progress: Optional[BuildProgress] = None,
    ) -> str:
        """Frame a short green signal for the advisor to relay to the builder."""
        phase = progress.phase if progress is not None else "build"
        return (
            f"The VALIDATOR verified component '{component}' at phase '{phase}' "
            "with no issues. Tell the builder in ONE short sentence that this "
            "component is verified and it should proceed to the next phase."
        )

    # ── helpers ──────────────────────────────────────────────────────────────
    def _app_name(self) -> str:
        bp = getattr(self.brief, "blueprint", None)
        return getattr(bp, "name", "") or ""

    def _requirement_text(self) -> str:
        raw = getattr(self.requirement_model, "raw", "") or ""
        if raw:
            return _clip(raw, 400)
        bp = getattr(self.brief, "blueprint", None)
        return _clip(getattr(bp, "description", "") or "", 400)

    def _foci(self) -> list[str]:
        model = self.requirement_model
        if model is None or not getattr(model, "priorities", None):
            return []
        try:
            return list(model.top_dimensions(3))
        except Exception:  # noqa: BLE001
            return []

    def _focus_line(self) -> str:
        tops = self._foci()
        if not tops:
            return ""
        return "REQUIREMENT FOCUS (optimize for): " + ", ".join(tops)

    def _targets_line(self) -> str:
        targets = getattr(self.requirement_model, "targets", None)
        if not targets:
            return ""
        notable = {k: v for k, v in targets.items() if v not in ("standard",)}
        if not notable:
            return ""
        return "ARCHITECTURE TARGETS: " + ", ".join(
            f"{k}={v}" for k, v in notable.items())


_WS_RE = re.compile(r"\s+")


def _clip(text: str, limit: int) -> str:
    text = _WS_RE.sub(" ", (text or "").strip())
    return text if len(text) <= limit else text[:limit].rsplit(" ", 1)[0] + " …"
