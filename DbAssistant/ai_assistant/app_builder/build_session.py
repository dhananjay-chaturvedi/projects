"""Two-session model for App Builder agentic builds.

Session A (BuilderSession) — agent mode, writes and runs in the workspace.
Session B (AnswerSession) — ask mode, frames answers to A's questions.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from hashlib import sha1
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.app_builder.agent_runner import (
    AgentEvent,
    AgentEventType,
    AgentMode,
    AgentRunner,
    classify_ask_intent,
    detect_questions,
    extract_marked_asks,
    is_genuine_question,
)
from ai_assistant.app_builder.governance import GovernanceBrief
from ai_assistant.app_builder import session_protocol
from ai_query.backends import AIBackend

EventCallback = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class FinalAgreementContext:
    """Inputs for the final A/B/C completion agreement."""

    digest: str
    meters_ok: bool = False
    agent_done: bool = False
    test_scope: str = ""
    how_to_test: str = ""
    code_review_evidence: str = ""
    structure: str = ""
    symbols: str = ""


# A path label (optionally in backticks) followed by a fenced code block — the
# format Session C uses to hand back its authored test files as TEXT. The
# orchestrator (never C) writes these, scoped to the validator folder.
_VALIDATOR_FILE_RE = re.compile(
    r"`?(?P<path>[\w./-]+\.py)`?\s*:?\s*\n+```[\w+\-]*\n(?P<body>.*?)```",
    re.DOTALL,
)


def parse_validator_test_files(
    text: str, *, folder: str
) -> list[tuple[str, str]]:
    """Extract ``(relative_path, content)`` test files C authored as text.

    Only files that resolve INSIDE *folder* are returned; a bare filename is
    placed in *folder*, and any path pointing elsewhere (or using ``..``) is
    dropped — so the orchestrator can never be steered into writing outside the
    validator's sandbox. Later definitions of the same file win (a refresh).
    """
    out: list[tuple[str, str]] = []
    if not text:
        return out
    folder = folder.strip("/")
    for m in _VALIDATOR_FILE_RE.finditer(text):
        raw = m.group("path").strip().strip("`").replace("\\", "/")
        if raw.startswith("./"):
            raw = raw[2:]
        body = m.group("body")
        if ".." in raw.split("/"):
            continue  # path traversal — drop it
        if raw.startswith(folder + "/"):
            rel = raw
        elif "/" not in raw:
            rel = f"{folder}/{raw}"
        else:
            continue  # points outside the sandbox folder — drop it
        if not rel.endswith(".py"):
            continue
        if not body.strip():
            continue
        out = [(p, c) for (p, c) in out if p != rel]  # refresh wins
        out.append((rel, body if body.endswith("\n") else body + "\n"))
    return out


class _BaseSession:
    def __init__(
        self,
        backend: AIBackend,
        workspace: Path,
        *,
        mode: AgentMode,
        timeout: int = 300,
        on_event: Optional[EventCallback] = None,
        cancel_event: Optional[threading.Event] = None,
        mask_pii: bool = False,
    ) -> None:
        self._runner = AgentRunner(
            backend, workspace, mode=mode, timeout=timeout,
            on_event=self._wrap_event(on_event),
            cancel_event=cancel_event,
            mask_pii=mask_pii,
        )
        self._primed = False
        self.last_events: list[AgentEvent] = []

    @property
    def last_text(self) -> str:
        """Assistant text produced in the most recent turn only."""
        return "\n".join(e.text for e in self.last_events
                         if e.type == AgentEventType.ASSISTANT_TEXT and e.text)

    def recent_history(self, *, max_chars: int = 900) -> str:
        """Assistant text from earlier turns (excludes the most recent turn).

        Lets the mediator share *what the builder has done so far* with the
        advisor, not just the current line.
        """
        full = self.transcript
        last = self.last_text
        if last and full.endswith(last):
            full = full[: -len(last)]
        full = full.strip()
        return full[-max_chars:] if full else ""

    @property
    def cancelled(self) -> bool:
        return self._runner.cancelled

    def cancel(self) -> None:
        self._runner.cancel()

    @property
    def session_id(self) -> Optional[str]:
        return self._runner.session_id

    @property
    def transcript(self) -> str:
        return self._runner.transcript

    def _wrap_event(self, on_event: Optional[EventCallback]) -> Callable:
        def _inner(ev: AgentEvent) -> None:
            if not on_event:
                return
            # SESSION_ID is internal plumbing: the runner already captures it to
            # resume the persistent session. It carries no user-facing meaning
            # and is re-emitted every turn, so never surface it to the UI.
            if (ev.type == AgentEventType.SESSION_ID
                    or getattr(ev.type, "value", ev.type) == "session_id"):
                return
            on_event({
                "session": self.role,
                "event": ev.as_dict(),
            })
        return _inner

    @property
    def role(self) -> str:
        raise NotImplementedError

    def prime(self, brief: GovernanceBrief, *, minimal: bool = False) -> list[AgentEvent]:
        """Push governance brief at session start."""
        text = (brief.render_minimal(role=self.role) if minimal
                else brief.render(role=self.role))
        events = self._runner.run(text)
        self.last_events = list(events)
        self._primed = True
        return events

    def send(self, message: str, *, mode: Optional[AgentMode] = None) -> list[AgentEvent]:
        events = self._runner.run(message, mode=mode)
        self.last_events = list(events)
        return events

    def _ask_text(
        self,
        prompt: str,
        *,
        brief: GovernanceBrief,
        default: str = "",
        mode: Optional[AgentMode] = None,
    ) -> str:
        """Prime (if needed), send *prompt*, and return the concise reply.

        Collapses the prime -> send -> collect ASSISTANT_TEXT -> extract_payload
        -> concise_answer pipeline shared by the ask-mode framing helpers.
        """
        if not self._primed:
            self.prime(brief)
        events = self.send(prompt, mode=mode) if mode is not None else self.send(prompt)
        parts = [e.text for e in events if e.type == AgentEventType.ASSISTANT_TEXT]
        return concise_answer(extract_payload("\n".join(parts))) or default


class BuilderSession(_BaseSession):
    """Session A — builds the app in agent mode."""

    role = "builder"

    def __init__(self, backend: AIBackend, workspace: Path, **kwargs) -> None:
        super().__init__(backend, workspace, mode=AgentMode.BUILD, **kwargs)

    def plan(self, prompt: str) -> str:
        """Run the planning turn and return the produced plan text."""
        events = self.send(prompt)
        parts = [e.text for e in events if e.type == AgentEventType.ASSISTANT_TEXT]
        return "\n".join(parts).strip()

    def prepare_outline(self, prompt: str, *, brief: GovernanceBrief) -> str:
        """Ask-mode lightweight outline for the understanding phase only.

        Runs on Session A's OWN persistent runner (so the build never opens a
        4th agent session) but forces this single turn into ASK mode, so the
        builder cannot write files or run tests while drafting the outline. The
        prompt itself is also outline-only as a second guard. The resulting
        session is the same one later resumed for the actual build.
        """
        events = self.send(prompt, mode=AgentMode.ASK)
        parts = [e.text for e in events if e.type == AgentEventType.ASSISTANT_TEXT]
        return concise_answer("\n".join(parts)) or ""


#: Cap on the advisory answer Session B sends back to the builder. Answers are
#: decisions/recommendations, never code — so this stays short on purpose.
_ANSWER_MAX_CHARS = 800
_ANSWER_MAX_WORDS = 120

_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_INDENTED_CODE_RE = re.compile(r"(?m)^(?: {4,}|\t).*$")


def concise_answer(text: str) -> str:
    """Reduce an agent reply to a short advisory answer (no code, no dumps).

    Session B is an *advisor*: it should return decisions, suggestions and
    recommendations — not source code or large repeated blocks. We strip code
    fences/indented code, collapse blank runs, drop duplicate lines and truncate
    to a sentence-friendly cap so the builder gets guidance, not a re-paste of
    the codebase.
    """
    text = (text or "").strip()
    if not text:
        return ""
    text = _CODE_FENCE_RE.sub(" [code omitted — advise, don't paste code] ", text)
    text = _INDENTED_CODE_RE.sub("", text)
    out_lines: list[str] = []
    seen: set[str] = set()
    for raw in text.splitlines():
        line = raw.rstrip()
        key = line.strip().lower()
        if not key:
            if out_lines and out_lines[-1] == "":
                continue
            out_lines.append("")
            continue
        if key in seen:  # drop repeated lines
            continue
        seen.add(key)
        out_lines.append(line)
    cleaned = "\n".join(out_lines).strip()
    words = cleaned.split()
    if len(words) > _ANSWER_MAX_WORDS:
        cleaned = " ".join(words[:_ANSWER_MAX_WORDS]) + " …"
    if len(cleaned) > _ANSWER_MAX_CHARS:
        cleaned = cleaned[:_ANSWER_MAX_CHARS].rsplit(" ", 1)[0] + " …"
    return cleaned


#: Handoff markers (single source of truth in ``session_protocol``). Session B
#: and Session C wrap the message they intend for another session between these,
#: so the App Builder Assistant can pick out the real payload from any
#: surrounding chatter before forwarding it.
_MARK_START = session_protocol.MARK_START
_MARK_DONE = session_protocol.MARK_DONE
_MARK_RULE = session_protocol.MARK_RULE


def extract_payload(text: str) -> str:
    """Return the content the session marked for handoff (between the markers).

    Falls back to the whole (stripped) text when the markers are absent, so a
    model that forgets to wrap its reply still gets forwarded.
    """
    t = text or ""
    i = t.find(_MARK_START)
    if i == -1:
        return t.strip()
    seg = t[i + len(_MARK_START):]
    j = seg.find(_MARK_DONE)
    if j != -1:
        seg = seg[:j]
    return seg.strip()


class AnswerSession(_BaseSession):
    """Session B — an *advisor* that frames concise answers in ask mode."""

    role = "answerer"

    def __init__(self, backend: AIBackend, workspace: Path, **kwargs) -> None:
        super().__init__(backend, workspace, mode=AgentMode.ASK, **kwargs)

    def frame_answer(
        self,
        question: str,
        *,
        brief: GovernanceBrief,
        context: str = "",
    ) -> str:
        """Draft a short advisory answer to *question* for Session A.

        The builder (Session A) is the one that writes code; Session B only
        advises. The prompt and post-processing both enforce that the reply is a
        concise recommendation rather than code or a large pasted block.
        """
        prompt = "\n".join(s for s in (
            "You are the build ADVISOR, not the coder. The builder agent writes "
            "all code; you only give a decision/recommendation.",
            f"BUILDER QUESTION: {question}",
            f"BUILD CONTEXT (for reference only): {context}" if context else "",
            "Answer in this order: (1) answer the REAL question directly, "
            "(2) weigh it against the app requirement, (3) consider current "
            "build progress, (4) say whether to decide now or defer to a later "
            "phase/component. At most 4 short sentences. State the recommended "
            "choice and a one-line reason. Do NOT write code, file contents, "
            "commands, or long lists — advise only.",
            "Evaluate the build state from the context: acknowledge what is "
            "already done and point only at what is genuinely still missing. Any "
            "listed gaps are auto-detected guesses — do NOT repeat a requirement "
            "as unmet if the evidence shows it is already implemented.",
            _MARK_RULE,
        ) if s)
        return self._ask_text(prompt, brief=brief, default="(no answer generated)")

    def frame_user_request(
        self,
        request: str,
        *,
        brief: GovernanceBrief,
        context: str = "",
    ) -> str:
        """Frame a USER message (direction, bug report, change) for Session A.

        Unlike :meth:`frame_answer` (which answers A's own question), this relays
        what the USER said. B must engage with the user's actual content — the
        concrete problem/intent — and give A a clear, contextual next step. It
        must NOT fall back to a generic "requirements not fulfilled" reply.
        """
        prompt = "\n".join(s for s in (
            "You are the build ADVISOR. The USER sent the message below to steer "
            "the build (it may be a bug report, a change request, or direction).",
            f"USER MESSAGE: {request}",
            f"BUILD CONTEXT (for reference only): {context}" if context else "",
            "Engage with the USER'S ACTUAL CONTENT. If it reports a problem, "
            "restate the specific problem and give the builder a concrete next "
            "step to investigate/fix it (where to look, what to check). If it is "
            "direction, translate it into a clear instruction. Do NOT reply with "
            "a generic 'requirements are not fulfilled' — be specific and "
            "contextual. At most 4 short sentences, no code or file dumps.",
            _MARK_RULE,
        ) if s)
        return self._ask_text(prompt, brief=brief, default=request)

    def frame_confirm_completion(
        self,
        *,
        brief: GovernanceBrief,
        context: str = "",
        validator_findings: str = "",
        evidence: str = "",
        structure: str = "",
        symbols: str = "",
    ) -> str:
        """Session B performs the final post-build CODE REVIEW.

        B reviews the FULL build against deterministic evidence (compile/import
        dry-run, launch smoke, coverage, meters, tests) plus the validator's
        findings and the real code structure/symbols, then returns a structured
        review with an explicit, machine-parseable verdict line.
        """
        prompt = "\n".join(s for s in (
            "You are the build ADVISOR acting as the FINAL POST-BUILD CODE "
            "REVIEWER. Session A built the app; you now review it on behalf of "
            "the user before it ships. You do NOT write code — you review.",
            "",
            "VALIDATOR (Session C) FINDINGS:",
            validator_findings or "(no validator report)",
            "",
            "DETERMINISTIC EVIDENCE (compile/import dry-run, launch smoke, "
            "coverage, meters, tests, sample data — facts, already computed):"
            if evidence else "",
            evidence,
            "",
            "PROJECT STRUCTURE (files Session A produced):" if structure else "",
            structure,
            "",
            "PUBLIC SYMBOLS (classes/functions in the code):" if symbols else "",
            symbols,
            "",
            context,
            "",
            "REVIEW the build across EACH of these dimensions and judge whether "
            "the app matches the USER REQUIREMENT and predicted design (real "
            "user-facing workflows, not a schema/CRUD mirror):",
            "  1. Compilation/import — does the code gate show clean compile + "
            "import of src.app:app and every module?",
            "  2. Launch — does the launch smoke boot the app and serve routes "
            "without 5xx?",
            "  3. Requirement gaps — any required entity/feature/flow still "
            "missing? (treat auto-detected gaps as candidates — verify against "
            "the structure/symbols before raising.)",
            "  4. Missing components — expected pages/APIs/services/models that "
            "are absent or stubbed.",
            "  5. Code quality — hygiene/structure issues the meters flagged.",
            "  6. Code completeness — placeholders, TODOs, dead handlers, "
            "unwired modules.",
            "  7. Functionality completeness — do the main flows actually work "
            "end-to-end with seeded/sample data?",
            "",
            "OUTPUT FORMAT (strict):",
            "  - FIRST line EXACTLY: 'REVIEW VERDICT: ready' (ship it) or "
            "'REVIEW VERDICT: not_ready' (real blocker remains).",
            "  - Then at most 7 short bullets, one per dimension above, each "
            "'<dimension>: ok' or '<dimension>: <the specific issue>'.",
            "  - No code, no file contents.",
            _MARK_RULE,
        ) if s)
        return self._ask_text(prompt, brief=brief, default="(no confirmation)")

    def frame_kickoff(self, description: str, *, brief: GovernanceBrief) -> str:
        """Reframe the user's "describe the app" prompt into a build brief for A.

        The very first user prompt goes here (to Session B) before any building:
        B turns the free-text description into a clear, actionable brief the
        builder can start from. No code — just purpose, flows, entities and what
        "done" means.
        """
        prompt = "\n".join((
            "You are the build ADVISOR — you STAND IN FOR THE USER to the builder "
            "agent. The user described the app they want. Expand it into a CLEAR, "
            "actionable build brief: state the app purpose, the main user flows, "
            "the key entities/data, and what 'done' looks like. Do NOT write code.",
            "CRITICAL FIDELITY RULE: preserve EVERY concrete detail the user gave "
            "— the domain, the product category, the audience. NEVER generalize "
            "or drop words. e.g. 'grocery ecommerce app' must stay a GROCERY "
            "store (food/household items), it must NOT become a generic or "
            "electronics ecommerce app. If the user named a niche, keep it.",
            f"USER DESCRIPTION (verbatim): {description or '(none given)'}",
            _MARK_RULE,
        ))
        # B is the PLANNER — frame the build brief in plan mode (read-only).
        framed = self._ask_text(
            prompt, brief=brief,
            default=(description or "Build the requested app."),
            mode=AgentMode.PLAN)
        # Safety net: if B's framing somehow dropped the user's words, prepend the
        # verbatim description so the domain is never lost on the way to A.
        if description and description.strip().lower() not in framed.lower():
            framed = f"{description.strip()} — {framed}"
        return framed

    def frame_db_intent(
        self,
        design_brief: str,
        *,
        brief: GovernanceBrief,
        context: str = "",
    ) -> str:
        """Return ONE concise plain-English phrase for what kind of app to build.

        Used for ``from_database`` builds: Session B reads the deterministic
        design brief from DB understanding and tells A/C *what to build* in
        plain language. The full ``design_brief`` is forwarded verbatim separately.
        """
        prompt = "\n".join(s for s in (
            "You are the build ADVISOR. A database was profiled and a design "
            "brief was produced (below). Reply with EXACTLY ONE concise "
            "plain-English sentence starting with "
            "'Build exactly this kind of app:' — name the real user-facing "
            "application (domain workflows, audience, purpose). No code, no "
            "bullet lists, no schema/table names unless essential to the app "
            "type.",
            f"DESIGN BRIEF:\n{design_brief.strip()}",
            f"EXTRA CONTEXT: {context}" if context else "",
            _MARK_RULE,
        ) if s)
        # B is the PLANNER here — run this turn in plan mode (read-only).
        phrase = self._ask_text(prompt, brief=brief, default="",
                                mode=AgentMode.PLAN)
        if phrase and not phrase.lower().startswith("build exactly"):
            phrase = f"Build exactly this kind of app: {phrase}"
        return phrase or "Build exactly this kind of app: as described in the design brief."


class ValidatorSession(_BaseSession):
    """Session C — validates the build in ask mode (judges completeness).

    The validator never writes code. The App Builder Assistant does the heavy,
    deterministic work (runs tests, computes meters/coverage, probes the DB
    read-only) and hands C a compact *evidence digest*; C only judges whether
    the app is complete for the requirement and lists short, actionable issues —
    keeping token use low.
    """

    role = "validator"

    def __init__(self, backend: AIBackend, workspace: Path, **kwargs) -> None:
        super().__init__(backend, workspace, mode=AgentMode.ASK, **kwargs)

    def prepare_outline(
        self,
        requirement: str,
        *,
        brief: GovernanceBrief,
        context: str = "",
    ) -> str:
        """Draft a concise validation outline (understanding phase, ask-only)."""
        prompt = context or (
            "OUTLINE ONLY — do NOT create files or run tests. "
            "Return at most 12 bullet points.\n"
            f"USER REQUIREMENT: {requirement or '(see build brief)'}")
        return self._ask_text(prompt, brief=brief, default="(test plan pending)")

    def prepare_test_plan(
        self,
        requirement: str,
        *,
        brief: GovernanceBrief,
        context: str = "",
    ) -> str:
        """Draft a concise test plan from the requirement (one kickoff call)."""
        from ai_assistant.app_builder.engine import BuildMode

        free_form = brief.blueprint.mode == BuildMode.FROM_SCRATCH
        freedom = ""
        if free_form:
            freedom = (
                "STRUCTURE FREEDOM: Session A chooses folders/files. Plan tests for "
                "the USER REQUIREMENT and standard practices only — do NOT require "
                "specific pre-decided frameworks or test-folder layouts."
            )
        prompt = "\n".join(s for s in (
            "You are the build VALIDATOR/TESTER (read-only). Draft a CONCISE test "
            "plan for this app from the requirement and common testing practices.",
            freedom,
            f"USER REQUIREMENT: {requirement or '(see build brief)'}",
            context,
            "The build is gated by a DETERMINISTIC code check first: every module "
            "must COMPILE and the app (src.app:app) plus each src module must "
            "IMPORT cleanly (a dry run) before anything else — make that step 1. "
            "Then UNIT-TEST each code block/module with concrete sample input and "
            "expected output, generating fresh test files as needed (you are NOT "
            "limited to pre-decided test folders/files).",
            "Include: compile + import dry-run, health/boot, core user flows, "
            "per-module unit tests with sample I/O, error/edge cases, and "
            "sample-data coverage. At most 12 bullet points. No code.",
            _MARK_RULE,
        ) if s)
        return self._ask_text(prompt, brief=brief, default="(test plan pending)")

    def validate(self, digest: str, *, brief: GovernanceBrief,
                 context: str = "") -> str:
        from ai_assistant.app_builder.engine import BuildMode

        free_form = brief.blueprint.mode == BuildMode.FROM_SCRATCH
        freedom = ""
        if free_form:
            freedom = (
                "STRUCTURE FREEDOM: do NOT flag missing pre-decided frameworks, "
                "standard folders, or manifest files. Judge only whether the app "
                "fulfils the USER REQUIREMENT and general good practices."
            )
        prompt = "\n".join(s for s in (
            "You are the build VALIDATOR/TESTER (read-only). You ONLY assess the "
            "app from the evidence below — unit testing during the build and full "
            "testing after it. You do NOT write or edit files and you do NOT "
            "apply changes.",
            freedom,
            "Do NOT ask to switch to agent mode and do NOT offer to make the "
            "change yourself — the builder (Session A) makes all changes. If "
            "something is wrong, just name the issue in your verdict.",
            "The evidence includes a DETERMINISTIC code gate (compile + import "
            "dry-run of src.app:app and each src module) AND a LAUNCH SMOKE that "
            "boots the app and crawls its GET routes. If either did NOT pass, the "
            "app would crash or serve broken pages on launch — the verdict MUST be "
            "'incomplete' and the first issue MUST be that failure. Otherwise judge "
            "REAL functionality, not just file presence: each module unit-tested "
            "with sample I/O, the app's main flows work end-to-end (routes return "
            "without 5xx, data persists and renders), the UX is usable, and sample "
            "data is seeded/shown when the database does not cover the whole app.",
            context,
            f"VALIDATION EVIDENCE:\n{digest}",
            "Reply EXACTLY as: first line 'VERDICT: complete' or "
            "'VERDICT: incomplete', then at most 4 short bullet issues "
            "(most important first). No code, no file contents.",
            _MARK_RULE,
        ) if s)
        return self._ask_text(prompt, brief=brief, default="(no validation)")

    def author_tests(
        self,
        *,
        brief: GovernanceBrief,
        structure: str = "",
        symbols: str = "",
        context: str = "",
    ) -> str:
        """Author the validator's OWN pytest files — READ-ONLY, as text.

        Session C never writes to disk and never gets a write-capable session
        (so it can never touch Session A's code, and the similarity/plan turns
        stay on the same read-only session). Instead C *emits* its independent
        pytest files as labelled fenced code blocks; the orchestrator writes
        them — and ONLY inside ``VALIDATOR_TEST_DIR`` — via
        :func:`parse_validator_test_files`. Tests are derived from the app
        requirement plus the builder's real structure and public symbols so they
        exercise the actual app instead of mirroring A's own tests.
        """
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        if not self._primed:
            self.prime(brief)
        prompt = "\n".join(s for s in (
            "You are the build VALIDATOR/TESTER (READ-ONLY). You do NOT write, "
            "edit, or delete any file. Author your OWN independent pytest files "
            f"that BELONG in the '{VALIDATOR_TEST_DIR}/' folder and OUTPUT their "
            "full contents as text — the orchestrator writes them for you, only "
            "inside that folder. Never reference or modify any path outside it "
            "(the builder, Session A, owns all app code).",
            "Derive the test cases from the app's USER REQUIREMENT and the "
            "builder's REAL structure and symbols below — match the actual "
            "imports, class names, function names and method signatures (do NOT "
            "invent names).",
            "TEST WHAT EXISTS, NOT WHAT IS PLANNED. The build is in progress, so "
            "only write ACTIVE tests for modules, classes, functions and routes "
            "that ALREADY appear in the structure/symbols below. For a "
            "requirement feature that is NOT yet implemented, still write the "
            "test but mark it pending with "
            "`@pytest.mark.skip(reason=\"not yet implemented: <feature>\")` so it "
            "stays GREEN now and AUTO-ACTIVATES once Session A builds it on a "
            "later refresh. Never assert against a symbol/route that is absent.",
            "Keep imports at module load safe: import the app lazily inside "
            "fixtures/tests (not at top level) so a still-incomplete module can "
            "never break test COLLECTION for the whole folder.",
            "Cover (active only when present): app boots (import src.app:app, "
            "GET /health), each implemented user-facing feature/flow end-to-end "
            "(use fastapi TestClient where useful), and key modules on sample "
            "data. Tests must run with a plain `pytest` from the workspace root.",
            f"PROJECT STRUCTURE (from Session A):\n{structure}" if structure else "",
            f"PUBLIC SYMBOLS (from Session A's code):\n{symbols}" if symbols else "",
            context,
            "OUTPUT FORMAT (strict): for EACH test file, emit its path on its "
            f"own line as `{VALIDATOR_TEST_DIR}/<name>.py` immediately followed "
            "by a fenced ```python code block with the FULL file contents. "
            "Emit nothing else between files. Do not summarise.",
        ) if s)
        events = self.send(prompt, mode=AgentMode.ASK)
        parts = [e.text for e in events if e.type == AgentEventType.ASSISTANT_TEXT]
        return "\n".join(parts).strip()


_ISSUE_BULLET_RE = re.compile(r"(?m)^\s*[-*\d.)]+\s+\S")


def validation_is_clean(findings: str) -> bool:
    """True when the validator says complete with no outstanding issue bullets.

    Used by the coordinator to skip the advisor/builder relay (and its tokens)
    when there is genuinely nothing to fix.
    """
    low = (findings or "").lower()
    if not low or "incomplete" in low:
        return False
    if "verdict: complete" not in low and "verdict:complete" not in low:
        return False
    return not _ISSUE_BULLET_RE.search(findings or "")


def _norm_question(text: str) -> str:
    """Fingerprint a question so we never answer the same one twice."""
    return " ".join((text or "").lower().split())


@dataclass
class AtomicHandoff:
    """One bound A→B→A request/response exchange."""

    id: str
    kind: str
    question: str
    status: str = "received_from_a"
    answer: str = ""
    asked_user: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "question": self.question,
            "status": self.status,
            "answer": self.answer,
            "asked_user": self.asked_user,
        }


@dataclass
class DualSessionCoordinator:
    """Coordinate Session A + B with interaction-level routing.

    Only *new, genuine* questions (and explicit permission requests) from the
    builder's most recent turn are answered, deduped against everything already
    answered and capped per turn — so the build progresses instead of endlessly
    re-answering narration.
    """

    builder: BuilderSession
    answerer: AnswerSession
    brief: GovernanceBrief
    decider: Any  # BuildDecider
    answered: set[str] = field(default_factory=set)
    max_answers_per_turn: int = 3
    decision_engine: Any = None  # DecisionEngine — balanced, math-based choices
    on_decision: Optional[Callable[[dict[str, Any]], None]] = None
    mediator: Any = None  # ContextMediator — frames context/progress for Session B
    progress: Any = None  # BuildProgress — live snapshot shared with the advisor
    on_review: Optional[Callable[[dict[str, Any]], None]] = None  # B-reply review
    validator: Optional["ValidatorSession"] = None  # Session C — validates the build
    on_validation: Optional[Callable[[dict[str, Any]], None]] = None
    # Surfaces the cross-session traffic the App Builder Assistant mediates so the
    # UI can show it in Session B's box: what was sent TO B (kickoff/question) and
    # what B framed and sent back to A (answers, brief) or relayed from C.
    on_relay: Optional[Callable[[dict[str, Any]], None]] = None
    # Verbatim design brief from DB understanding (from_database only).
    design_brief: str = ""
    # Schema + sample rows from DB understanding (from_database only).
    db_context: str = ""
    # B's composed first instruction for A/C (from_database only).
    builder_instruction: str = ""
    validator_instruction: str = ""
    _builder_action: str = ""
    _validator_action: str = ""
    # Unsolicited (C→B) feedback waiting to be handed to A. It is delivered only
    # when A is available (not waiting on an answer to its own question), so a
    # question and its answer always stay bound and in order.
    _feedback_queue: list[dict[str, Any]] = field(default_factory=list)
    _user_queue: list[dict[str, Any]] = field(default_factory=list)
    _framed_brief: str = ""
    _test_plan: str = ""
    _green_relays: set[str] = field(default_factory=set)
    _handoffs: dict[str, AtomicHandoff] = field(default_factory=dict)
    _progress_checks: int = 0
    max_green_relays: int = 6
    max_progress_checks: int = 8

    def _relay(self, direction: str, text: str, **extra: Any) -> None:
        """Surface one mediated cross-session message in Session B's status box.

        ``direction`` is one of: ``to_b`` (App Builder → B), ``a_to_b``
        (A's question → B), ``b_to_a`` (B's framed answer/brief → A),
        ``b_to_c`` (brief → C), ``c_to_b`` (C's findings → B).
        """
        if self.on_relay is None:
            return
        rec = {"direction": direction, "text": (text or "").strip(), **extra}
        try:
            self.on_relay(rec)
        except Exception:  # noqa: BLE001
            pass

    @staticmethod
    def _handoff_id(kind: str, question: str, detail: Optional[dict] = None) -> str:
        detail = detail or {}
        raw_id = (
            detail.get("toolCallId")
            or detail.get("id")
            or ((detail.get("query") or {}).get("id")
                if isinstance(detail.get("query"), dict) else "")
        )
        basis = f"{kind}:{raw_id}:{_norm_question(question)}"
        return "rq_" + sha1(basis.encode("utf-8")).hexdigest()[:12]

    def _record_handoff(
        self, handoff_id: str, kind: str, question: str, status: str,
        *, answer: str = "", asked_user: bool = False
    ) -> AtomicHandoff:
        rec = self._handoffs.get(handoff_id)
        if rec is None:
            rec = AtomicHandoff(handoff_id, kind, question)
            self._handoffs[handoff_id] = rec
        rec.status = status
        if answer:
            rec.answer = answer
        rec.asked_user = bool(asked_user)
        return rec

    def start(self) -> None:
        self.builder.prime(self.brief, minimal=True)
        self.answerer.prime(self.brief)
        if self.validator is not None:
            self.validator.prime(self.brief, minimal=True)

    @staticmethod
    def _role_actions() -> tuple[str, str]:
        """Return (builder_action, validator_action) for from_database builds."""
        from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR

        builder_action = (
            "ACTION: write or refresh docs/requirement.txt from this brief as "
            "your acceptance reference, then build phase-wise (api → db → "
            "web → tests) emitting PHASE-DONE: <component> when each part is "
            "complete. Use FastAPI + Jinja2 only; do NOT use Flask, Django, "
            "or any WSGI-only app. Keep src.app:app as an importable ASGI "
            "FastAPI object with GET /health so the platform launches the "
            "final agent-built app. Build the predicted REAL user-facing app "
            "from the brief (not a schema/CRUD mirror): the navigation and "
            "pages are the brief's USER-FACING FEATURES (one purposeful "
            "workflow each, named in domain language) — do NOT add a generic "
            "list/create/edit/delete screen for every table. Treat the raw "
            "tables as the DATA LAYER behind those workflows; only add "
            "create/edit forms for records a real user of THIS app would "
            "manage. Seed realistic sample data so screens are populated on "
            "first run, wire every DB-backed service to the REAL tables, and "
            "surface real event data and metrics (actual records and their "
            "counts) rather than raw table row totals or empty placeholders. "
            "The build is only finished when Session C validates and Session "
            "B agrees.")
        builder_action += (
            f" RESERVED FOLDER: Session C owns '{VALIDATOR_TEST_DIR}/' for its "
            "independent tests — do NOT create or edit anything there. "
            "RUNNABILITY (hard gate): the app must COMPILE and IMPORT cleanly — "
            "`src.app:app` and every src module must import with NO error on a "
            "local SQLite DB (no external services required to boot). Write YOUR "
            "unit tests for each code block/module with concrete sample input and "
            "expected output in your own test folders. A build that does not "
            "compile/import is rejected because it would crash on launch.")
        validator_action = (
            f"ACTION: author your OWN independent pytest files ONLY inside "
            f"'{VALIDATOR_TEST_DIR}/'. Derive tests from the build brief above: "
            "compile/import dry-run, health/boot, core user flows, edge cases, "
            "and sample-data coverage. Verify REAL functionality — flows that "
            "error (5xx) or screens that do nothing are 'incomplete'. "
            "Report VERDICT: complete/incomplete with short actionable issues.")
        return builder_action, validator_action

    def frame_first_instruction(self, description: str) -> dict[str, Any]:
        """FROM_DATABASE: B frames the ordered first instruction for A and C.

        Order: (1) plain-English intent, (2) design brief + schema + samples,
        (3) role-specific action instructions.

        Session A (the builder) is NEVER given a read-only turn here: a read-only
        (ask/plan) turn can leave A's persistent session unable to write on
        resume. A's full instruction is stashed and prepended to its FIRST build
        (write) turn instead (see the orchestrator's plan phase). Only the
        read-only validator (C) receives the instruction now, to ground its
        independent test authoring and validation.
        """
        from ai_assistant.app_builder.engine import BuildMode

        issues: list[str] = []
        if self.brief.blueprint.mode != BuildMode.FROM_DATABASE:
            return {"framed": "", "issues": ["not a from_database build"]}

        db_brief = (self.design_brief or "").strip()
        db_ctx = (self.db_context or "").strip()
        if not db_brief and not db_ctx:
            return {"framed": "", "issues": ["no DB design brief available"]}

        self._relay(
            "to_b",
            "DB design brief + schema/sample data — frame the first instruction "
            "for Sessions A and C.",
            kind="kickoff",
            design_brief=db_brief[:500] + ("…" if len(db_brief) > 500 else ""))

        # Section 1 — plain-English intent (B reads full DB context).
        framing_input = db_brief
        if db_ctx:
            framing_input = f"{db_brief}\n\n{db_ctx}" if db_brief else db_ctx
        db_framer = getattr(self.answerer, "frame_db_intent", None)
        intent = (
            db_framer(framing_input, brief=self.brief, context=description or "")
            if callable(db_framer) else "")
        if not intent:
            intent = "Build exactly this kind of app: as described in the design brief."

        # Section 2 — design brief + schema + sample data. The DB context already
        # embeds the design brief at its tail (see DbUnderstanding.render); strip
        # that copy so the brief is shown/sent exactly once.
        section2_parts: list[str] = []
        if db_brief:
            section2_parts.append(f"DESIGN BRIEF:\n{db_brief}")
        if db_ctx:
            ctx_clean = db_ctx
            marker = "\nDESIGN BRIEF:"
            if db_brief and marker in ctx_clean:
                idx = ctx_clean.rfind(marker)
                tail = ctx_clean[idx + len(marker):].strip()
                if tail and tail[:160] == db_brief.strip()[:160]:
                    ctx_clean = ctx_clean[:idx].rstrip()
            section2_parts.append(
                f"DATABASE CONTEXT (schema and sample data):\n{ctx_clean}")
        section2 = "\n\n".join(section2_parts)

        framed_shared = f"1. WHAT TO BUILD:\n{intent}\n\n2. DESIGN BRIEF AND DATA:\n{section2}"
        if self.mediator is not None and hasattr(self.mediator, "review_reply"):
            review = self.mediator.review_reply(
                description, framed_shared, progress=self.progress)
            issues = list(review.issues)
            directives = self.mediator.directives_text(review.injected_rules)
            if directives:
                framed_shared = framed_shared + directives

        self._framed_brief = framed_shared
        builder_action, validator_action = self._role_actions()
        self._builder_action = builder_action
        self._validator_action = validator_action

        self.builder_instruction = (
            f"SESSION B — BUILD INSTRUCTION:\n\n{framed_shared}\n\n"
            f"3. ACTION:\n{builder_action}")
        self.validator_instruction = (
            f"SESSION B — VALIDATION INSTRUCTION:\n\n{framed_shared}\n\n"
            f"3. ACTION:\n{validator_action}")

        self._relay("b_to_a", self.builder_instruction, kind="instruction")
        self._relay("b_to_c", self.validator_instruction, kind="instruction")

        # Session A is NOT sent the instruction here — it must never take a
        # read-only turn (that can disable writes on resume). The full builder
        # instruction is prepended to A's first build turn (see _plan_phase).
        # Only the read-only validator receives its instruction now.
        if self.validator is not None:
            self.validator.send(self.validator_instruction, mode=AgentMode.ASK)

        return {"framed": framed_shared, "issues": issues}

    def kickoff(self, description: str) -> dict[str, Any]:
        """Route the first user prompt through B and stash A/C instructions.

        The user's "describe the app" prompt goes to Session B, which frames it
        into an actionable plan/instruction. Session A is not sent this turn:
        its first substantive message must be a write-capable build turn. The
        full instruction is stashed and prepended by the orchestrator.
        """
        from ai_assistant.app_builder.builder_types import policy_for
        from ai_assistant.app_builder.engine import BuildMode, VALIDATOR_TEST_DIR

        issues: list[str] = []
        if self.brief.blueprint.mode == BuildMode.FROM_DATABASE:
            # FROM_DATABASE uses frame_first_instruction + understanding gate;
            # kickoff is not called for that mode.
            return self.frame_first_instruction(description)

        framer = getattr(self.answerer, "frame_kickoff", None)
        if not callable(framer):
            return {"framed": "", "issues": ["advisor cannot frame kickoff"]}
        self._relay(
            "to_b", description or "(no description given — frame a sensible "
            "default brief)", kind="kickoff")
        framed = framer(description, brief=self.brief)
        if self.mediator is not None and hasattr(self.mediator, "review_reply"):
            review = self.mediator.review_reply(
                description, framed, progress=self.progress)
            issues = list(review.issues)
            directives = self.mediator.directives_text(review.injected_rules)
            if directives:
                framed = framed + directives
        self._framed_brief = framed

        policy = policy_for(self.brief.blueprint.mode)
        profile_action = policy.action_text(self.brief.blueprint)
        if self.brief.blueprint.mode == BuildMode.FROM_SCRATCH:
            action = (
                f"{profile_action}\n"
                "ACTION: you have FULL FREEDOM over files, folders and the "
                "website/UI layout — create whatever THIS app needs and do NOT "
                "reuse any fixed template or pre-set folders. The ONLY contract "
                "(so the platform can run it) is an importable ASGI app at "
                "src.app:app exposing GET /health. Build an openable webpage "
                "early, keep it runnable, then iterate in whatever phases make "
                "sense, emitting PHASE-DONE: <component> as each part is "
                "complete. The build is only finished when Session C validates "
                "and Session B agrees.")
        elif self.brief.blueprint.mode == BuildMode.FROM_CODEBASE:
            action = (
                f"{profile_action}\n"
                "ACTION: build from the recovered codebase understanding. If the "
                "variant is application, reconstruct the real working app this "
                "codebase represents: pages, flows, APIs, data layer, and sample "
                "data aligned with recovered routes/services/models. If the "
                "variant is explorer, build a working architecture/metadata "
                "explorer that shows folder tree, components, APIs, dependencies, "
                "and sample I/O fields. Keep src.app:app importable with "
                "GET /health and a requirement-specific landing page. The build "
                "is only finished when Session C validates and Session B agrees.")
        else:
            action, _ = self._role_actions()
        validator_action = (
            f"{profile_action}\n"
            f"ACTION: independently validate the app and author tests only inside "
            f"'{VALIDATOR_TEST_DIR}/'. Judge happy-path workflow completeness for "
            "prototype builds and production-functional end-to-end completeness "
            "for full builds. Route questions/feedback through Session B.")
        self._relay("b_to_a", framed, kind="brief")
        if description and description.strip():
            user_line = (
                "USER REQUEST (verbatim — build EXACTLY this; do not generalize "
                f"or change the domain): {description.strip()}\n")
        else:
            user_line = ""
        self.builder_instruction = (
            f"{user_line}"
            "SESSION B — BUILD INSTRUCTION:\n\n"
            "BUILD BRIEF (the advisor's elaboration of the request above — it "
            "adds detail, it does NOT replace or broaden the request): "
            f"{framed}\n{action}")
        self.validator_instruction = (
            "SESSION B — VALIDATION INSTRUCTION:\n\n"
            "BUILD BRIEF (from the advisor — use this as the acceptance "
            f"criteria when validating): {framed}\n{validator_action}")
        # Share B's framed brief with C so validation is grounded in the same
        # requirement the builder is working from.
        if self.validator is not None:
            try:
                self.validator.send(self.validator_instruction, mode=AgentMode.ASK)
            except TypeError:
                self.validator.send(self.validator_instruction)
        return {"framed": framed, "issues": issues}

    def _builder_history(self) -> str:
        """Earlier builder output (excludes the current turn) for A→B sharing."""
        fn = getattr(self.builder, "recent_history", None)
        if callable(fn):
            try:
                return fn()
            except Exception:  # noqa: BLE001
                return ""
        return ""

    def status_preface(self) -> str:
        """Compact snapshot of Session A's progress for B/C context."""
        lines: list[str] = []
        if self.progress is not None:
            lines.append(f"BUILD STATUS: {self.progress.summary()}")
            gaps = list(getattr(self.progress, "gaps", None) or [])
            if gaps:
                lines.append(
                    "POSSIBLE GAPS (auto-detected by a keyword scan — they may be "
                    "stale or wrong; VERIFY against what the builder has actually "
                    "done below before raising them, and do NOT re-request anything "
                    "already implemented): " + "; ".join(gaps[:6]))
        hist = self._builder_history()
        if hist:
            clipped = hist if len(hist) <= 400 else ("…" + hist[-400:])
            lines.append(f"BUILDER RECENT:\n{clipped}")
        return "\n".join(lines)

    def route_user_request(
        self,
        text: str,
        *,
        interactive: bool = False,
    ) -> str:
        """Route a user request through B, then hand the framed instruction to A."""
        from ai_assistant.app_builder.interaction import BuildDecision

        request = (text or "").strip()
        if not request:
            return ""
        self._relay("user_to_b", request, kind="user_request")
        ctx = self.status_preface()
        if self.mediator is not None and hasattr(
                self.mediator, "user_request_context"):
            ctx = self.mediator.user_request_context(
                request, history=self._builder_history(),
                progress=self.progress)
        # B engages with the user's actual content (bug/direction), not a
        # generic "requirements not fulfilled". The decision engine and gap
        # directives are deliberately NOT applied here: those are for A's own
        # option-decisions, and would otherwise overwrite the user's real intent.
        framer = getattr(self.answerer, "frame_user_request", None)
        if callable(framer):
            framed = framer(request, brief=self.brief, context=ctx)
        else:
            framed = self.answerer.frame_answer(
                f"USER REQUEST: {request}", brief=self.brief, context=ctx)
        if self.on_review is not None and self.mediator is not None and hasattr(
                self.mediator, "review_reply"):
            # Surface monitoring telemetry only — do not mutate B's message.
            try:
                review = self.mediator.review_reply(
                    request, framed, progress=self.progress)
                self.on_review(review.as_dict())
            except Exception:  # noqa: BLE001
                pass
        if interactive and self.decider.interactive:
            answer = self.decider.decide(BuildDecision(
                id="user_request",
                question=f"Send this instruction to the builder?\n{request}",
                kind="choice",
                options=["send_proposed", "edit", "skip"],
                default="send_proposed",
                detail=framed,
            ))
            if answer == "skip":
                return ""
        self._relay("b_to_a", framed, kind="user_instruction", request=request)
        preface = self.status_preface()
        # Always hand A the USER'S VERBATIM message plus B's contextual guidance,
        # so A never loses the real problem behind a reframed summary.
        msg = (
            f"USER MESSAGE (relayed via advisor): {request}\n\n"
            f"ADVISOR GUIDANCE: {framed}"
        )
        if preface:
            msg = preface + "\n\n" + msg
        self.builder.send(msg)
        return framed

    def queue_user_message(self, text: str) -> dict[str, Any]:
        """Queue a user note for Session B to frame and deliver to A when free."""
        request = (text or "").strip()
        if not request:
            return {}
        self._relay("user_to_b", request, kind="user_message_queued")
        ctx = self.status_preface()
        framer = getattr(self.answerer, "frame_user_request", None)
        if callable(framer):
            framed = framer(request, brief=self.brief, context=ctx)
        else:
            framed = self.answerer.frame_answer(
                f"USER MESSAGE: {request}", brief=self.brief, context=ctx)
        self._relay("b_to_a", framed, kind="user_message_queued")
        item = {"request": request, "framed": framed, "advice": framed}
        self._user_queue.append(item)
        return item

    def b_progress_check(
        self,
        events: Optional[list[AgentEvent]] = None,
        *,
        no_progress: bool = False,
        phase_done: bool = False,
        no_progress_streak: int = 0,
    ) -> Optional[str]:
        """Proactive nudge from B when A appears stuck (gated, capped)."""
        if self._progress_checks >= self.max_progress_checks:
            return None
        warranted = phase_done or (no_progress and no_progress_streak >= 2)
        if not warranted:
            return None
        if self._builder_has_pending_question(events):
            return None
        ctx = self.status_preface()
        prompt = (
            "PROGRESS WATCH: review the builder's status below. If the builder "
            "appears stuck or idle, give ONE short proactive next step; otherwise "
            "reply exactly 'no action needed'."
        )
        nudge = self.answerer.frame_answer(
            prompt, brief=self.brief, context=ctx)
        if not nudge or "no action needed" in nudge.lower():
            return None
        self._progress_checks += 1
        self._relay("b_to_a", nudge, kind="progress_nudge")
        self._user_queue.append({
            "request": "[progress watch]",
            "framed": nudge,
            "advice": nudge,
        })
        return nudge

    @staticmethod
    def _parse_review_verdict(reply: str) -> bool:
        """Interpret B's post-build code review into a ready/not-ready boolean.

        Prefers the explicit 'REVIEW VERDICT: ready|not_ready' line so the
        structured review (which legitimately contains words like 'missing'
        or 'incomplete' inside per-dimension bullets) is parsed reliably.
        Falls back to a keyword heuristic for plain-text replies (e.g. stubs).
        """
        low = (reply or "").lower()
        for line in low.splitlines():
            line = line.strip().lstrip("-*# ").strip()
            if line.startswith("review verdict"):
                _, _, val = line.partition(":")
                val = val.strip()
                if val.startswith("not") or "not_ready" in val \
                        or "not ready" in val:
                    return False
                if val.startswith("ready") or "ready" in val:
                    return True
        # Fallback heuristic (no explicit verdict line was emitted).
        confirms = any(w in low for w in (
            "ready", "complete", "done", "satisfied", "good to go",
            "start", "verify", "yes"))
        if any(w in low for w in (
                "not ready", "incomplete", "blocker", "missing")):
            confirms = False
        return confirms

    def finalize_agreement(
        self,
        context: FinalAgreementContext | str = "",
        **legacy,
    ) -> dict[str, Any]:
        """A, B and C must agree before the build is declared complete."""
        if not isinstance(context, FinalAgreementContext):
            if not context and "digest" in legacy:
                context = legacy.pop("digest")
            context = FinalAgreementContext(digest=context, **legacy)
        digest = context.digest
        meters_ok = context.meters_ok
        agent_done = context.agent_done
        test_scope = context.test_scope
        how_to_test = context.how_to_test
        code_review_evidence = context.code_review_evidence
        structure = context.structure
        symbols = context.symbols
        issues: list[str] = []
        statements: dict[str, str] = {"a": "meters/coverage satisfied" if meters_ok
                                      else "meters/coverage not satisfied"}
        if agent_done:
            statements["a"] += "; agent signaled DONE"

        c_record = self.relay_validation(
            digest, test_scope=test_scope, how_to_test=how_to_test,
            relay=False, component="final")
        c_clean = bool(c_record and c_record.get("clean"))
        statements["c"] = (c_record or {}).get("findings", "")
        if not c_clean:
            issues.append("validator: app incomplete or issues remain")

        vctx = ""
        if self.mediator is not None and hasattr(self.mediator, "validator_context"):
            vctx = self.mediator.validator_context(
                progress=self.progress, history=self._builder_history(),
                test_scope=test_scope, how_to_test=how_to_test,
                framed_brief=self._framed_brief, component="final")
        b_reply = self.answerer.frame_confirm_completion(
            brief=self.brief, context=vctx,
            validator_findings=statements["c"],
            evidence=code_review_evidence or digest,
            structure=structure, symbols=symbols)
        statements["b"] = b_reply
        b_confirms = self._parse_review_verdict(b_reply)
        if not b_confirms:
            issues.append("advisor: code review not passed")

        if not meters_ok:
            issues.append("meters/coverage targets not met")

        complete = meters_ok and c_clean and b_confirms
        if not complete and issues:
            advice_ctx = vctx or self.status_preface()
            b_question = (
                "The build is NOT yet agreed complete. Issues:\n"
                + "\n".join(f"- {i}" for i in issues)
                + "\nFrame ONE concrete next step for the builder."
            )
            advice = self.answerer.frame_answer(
                b_question, brief=self.brief, context=advice_ctx)
            if self.mediator is not None and hasattr(
                    self.mediator, "review_reply"):
                review = self.mediator.review_reply(
                    b_question, advice, progress=self.progress)
                directives = self.mediator.directives_text(
                    review.injected_rules)
                if directives:
                    advice = advice + directives
            self._relay("b_to_a", advice, kind="completion_issues")
            statements["advice"] = advice

        return {
            "complete": complete,
            "issues": issues,
            "statements": statements,
            "c_clean": c_clean,
            "b_confirms": b_confirms,
            "meters_ok": meters_ok,
        }

    def _askable_items(
        self, events: Optional[list[AgentEvent]]
    ) -> list[tuple[str, str, dict[str, Any]]]:
        """(kind, question, detail) from the latest turn."""
        events = events if events is not None else self.builder.last_events
        items: list[tuple[str, str, dict[str, Any]]] = []
        seen: set[str] = set()

        def _strip_marker(q: str) -> str:
            return re.sub(
                r"(?i)^(?:ask|confirm|approve)\s*:\s*", "", (q or "").strip())

        def _add(kind: str, q: str, detail: Optional[dict] = None) -> None:
            body = _strip_marker(q)
            fp = _norm_question(body)
            if not fp or fp in seen:
                return
            seen.add(fp)
            items.append((kind, body, dict(detail or {})))

        # Structured ASK:/CONFIRM:/APPROVE: markers take priority — they carry
        # an explicit intent the advisor can answer against requirement/progress.
        for intent, q in extract_marked_asks(self.builder.last_text):
            _add(intent, q, {"source": "marker"})
        for ev in events or []:
            if ev.type == AgentEventType.QUESTION and ev.detail.get("permission"):
                _add("permission",
                     "The agent requested permission to make a change. Approve?",
                     ev.detail)
            elif (ev.type == AgentEventType.QUESTION
                  and ev.detail.get("interaction_question")):
                _add(classify_ask_intent(ev.detail.get("prompt") or ev.text),
                     ev.text, ev.detail)
            elif ev.type == AgentEventType.QUESTION and is_genuine_question(ev.text):
                _add(classify_ask_intent(ev.text), ev.text, ev.detail)
        # Heuristic fallback for genuine questions without explicit markers.
        for q in detect_questions(self.builder.last_text):
            _add(classify_ask_intent(q), q, {"source": "heuristic"})
        return items

    def route_questions(
        self,
        events: Optional[list[AgentEvent]] = None,
        *,
        context: str = "",
        on_question: Optional[Callable[[str, str], None]] = None,
    ) -> list[dict[str, Any]]:
        """Answer the builder's NEW genuine questions (deduped, capped)."""
        from ai_assistant.app_builder.interaction import BuildDecision

        routed: list[dict[str, Any]] = []
        for kind, question, detail in self._askable_items(events):
            if len(routed) >= self.max_answers_per_turn:
                break
            fp = _norm_question(question)
            if fp in self.answered:
                continue

            handoff_id = self._handoff_id(kind, question, detail)
            h = self._record_handoff(
                handoff_id, kind, question, "received_from_a")
            self._relay(
                "a_to_b", question, kind=kind, request_id=handoff_id,
                status=h.status)
            self._record_handoff(
                handoff_id, kind, question, "sent_to_b")
            if kind == "permission":
                proposed = "Approved — proceed with the change."
            else:
                # The App Builder Assistant mediates: instead of handing Session B
                # a raw transcript tail, give it a focused brief — what A has done
                # so far (history), what it is working on now, live build progress,
                # and what is expected back.
                advisor_ctx = context
                if self.mediator is not None:
                    advisor_ctx = self.mediator.advisor_context(
                        question,
                        builder_text=(self.builder.last_text or context),
                        history=self._builder_history(),
                        progress=self.progress,
                        kind=kind,
                    )
                raw = self.answerer.frame_answer(
                    question, brief=self.brief, context=advisor_ctx)
                proposed = raw
                # The App Builder Assistant makes a balanced, math-based decision
                # over the agent's question + the AI's proposal, weighing business
                # intent vs performance/cost/resource/scalability/reliability.
                if self.decision_engine is not None:
                    decision = self.decision_engine.decide(question, raw)
                    proposed = decision.answer
                    if self.on_decision is not None:
                        try:
                            self.on_decision(decision.as_dict())
                        except Exception:  # noqa: BLE001
                            pass
                # Monitor Session B's reply: validate alignment and inject rules
                # (toward requirements, testing, completion) when it drifts.
                if self.mediator is not None and hasattr(
                        self.mediator, "review_reply"):
                    review = self.mediator.review_reply(
                        question, raw, progress=self.progress)
                    directives = self.mediator.directives_text(
                        review.injected_rules)
                    if directives:
                        proposed = proposed + directives
                    if self.on_review is not None:
                        try:
                            self.on_review(review.as_dict())
                        except Exception:  # noqa: BLE001
                            pass

            answer = proposed
            asked = False
            if self.decider.interactive:
                if detail.get("interaction_question"):
                    # Render the agent's native options; user answers A directly.
                    # Session B's framed answer is an optional recommendation.
                    agent_opts = list(detail.get("option_items") or [])
                    if not agent_opts:
                        for line in detail.get("options") or []:
                            if ": " in str(line):
                                oid, label = str(line).split(": ", 1)
                                agent_opts.append({"id": oid, "label": label})
                            else:
                                agent_opts.append(
                                    {"id": str(line), "label": str(line)})
                    answer = self.decider.decide(BuildDecision(
                        id="agent_question",
                        question=question,
                        kind="interaction",
                        agent_options=agent_opts,
                        allow_multiple=bool(detail.get("allow_multiple")),
                        recommendation=proposed,
                        default=proposed,
                        detail=proposed,
                    ))
                else:
                    answer = self.decider.decide(BuildDecision(
                        id="agent_question",
                        question=f"Builder asked: {question}",
                        kind="choice",
                        options=["send_proposed", "edit", "skip"],
                        default="send_proposed",
                        detail=proposed,
                    ))
                asked = True
                self.answered.add(fp)
                if answer == "skip":
                    h = self._record_handoff(
                        handoff_id, kind, question, "skipped_by_user",
                        asked_user=True)
                    self._relay(
                        "b_to_a", "Skipped by user; no answer delivered.",
                        kind="answer_skipped", request_id=handoff_id,
                        status=h.status, question=question)
                    continue
                if not detail.get("interaction_question"):
                    if answer == "edit" and on_question:
                        answer = on_question(question, proposed) or proposed
                    elif answer == "send_proposed":
                        answer = proposed
            else:
                # auto / uninterrupted: send the framed (or approval) answer.
                self.answered.add(fp)

            h = self._record_handoff(
                handoff_id, kind, question, "answered_by_b",
                answer=answer, asked_user=asked)
            self._relay(
                "b_to_a", answer, kind="answer", question=question,
                request_id=handoff_id, status=h.status)
            self.builder.send(
                f"ANSWER [{handoff_id}] for the bound builder question above: "
                f"{answer}")
            h = self._record_handoff(
                handoff_id, kind, question, "delivered_to_a",
                answer=answer, asked_user=asked)
            self._relay(
                "b_to_a", f"delivered answer for {handoff_id}",
                kind="answer_delivered", question=question,
                request_id=handoff_id, status=h.status)
            routed.append({
                "question": question, "answer": answer, "asked": asked,
                "kind": kind, "request_id": handoff_id,
                "status": h.status,
            })
        return routed

    def relay_validation(
        self,
        digest: str,
        *,
        test_scope: str = "",
        how_to_test: str = "",
        relay: bool = True,
        green_relay: bool = False,
        component: str = "",
    ) -> Optional[dict[str, Any]]:
        """Ask Session C to validate, then (C→B→A) push its findings to A.

        ``digest`` is the code-computed evidence (test result + meter/coverage
        scores + gaps). When C reports the app is complete with no issues we skip
        the advisor + builder turns entirely to save tokens. When ``relay`` is
        False (final, post-build validation) we only record C's verdict without
        nudging the builder, since there is nothing left to build.
        """
        if self.validator is None:
            return None

        vctx = ""
        if self.mediator is not None and hasattr(self.mediator, "validator_context"):
            vctx = self.mediator.validator_context(
                progress=self.progress, history=self._builder_history(),
                test_scope=test_scope, how_to_test=how_to_test,
                framed_brief=self._framed_brief, component=component)
        findings = self.validator.validate(
            digest, brief=self.brief, context=vctx)
        record: dict[str, Any] = {
            "findings": findings, "clean": validation_is_clean(findings),
            "relayed": False,
        }
        if self.on_validation is not None:
            try:
                self.on_validation(record)
            except Exception:  # noqa: BLE001
                pass

        if not relay:
            return record

        # Green verdict on a phase-done component: queue a short proceed note.
        if record["clean"]:
            if not green_relay or not component:
                return record
            if (component in self._green_relays
                    or len(self._green_relays) >= self.max_green_relays):
                return record
            green_note = ""
            if self.mediator is not None and hasattr(
                    self.mediator, "validation_green_note"):
                green_note = self.mediator.validation_green_note(
                    component, progress=self.progress)
            else:
                green_note = (
                    f"Component '{component}' verified — proceed to the next "
                    "phase.")
            advice = self.answerer.frame_answer(
                green_note, brief=self.brief, context=vctx)
            if self.mediator is not None and hasattr(
                    self.mediator, "review_reply"):
                review = self.mediator.review_reply(
                    green_note, advice, progress=self.progress)
                directives = self.mediator.directives_text(
                    review.injected_rules)
                if directives:
                    advice = advice + directives
            self._relay("b_to_a", advice, kind="advice_queued",
                        component=component)
            self._feedback_queue.append({
                "advice": advice, "findings": findings, "green": True,
                "component": component,
            })
            self._green_relays.add(component)
            record["queued"] = True
            record["advice"] = advice
            record["green"] = True
            return record

        # C → B: the advisor translates the validator's findings into ONE concrete
        # instruction the builder can act on; B's reply is reviewed/aligned exactly
        # like a normal answer, then handed to A. A never sees C directly.
        b_question = findings
        if self.mediator is not None and hasattr(
                self.mediator, "validation_to_advice"):
            b_question = self.mediator.validation_to_advice(
                findings, progress=self.progress)
        advice = self.answerer.frame_answer(
            b_question, brief=self.brief, context=vctx)
        if self.mediator is not None and hasattr(self.mediator, "review_reply"):
            review = self.mediator.review_reply(
                b_question, advice, progress=self.progress)
            directives = self.mediator.directives_text(review.injected_rules)
            if directives:
                advice = advice + directives
        self._relay("b_to_a", advice, kind="advice_queued",
                    component=component)
        # Queue B's framed advice rather than sending it now — the App Builder
        # Assistant delivers it to A only when A is free (deliver_feedback), so
        # an unsolicited note never collides with an answer A is waiting for.
        self._feedback_queue.append({"advice": advice, "findings": findings})
        record["queued"] = True
        record["advice"] = advice
        return record

    def _builder_has_pending_question(
        self, events: Optional[list[AgentEvent]] = None
    ) -> bool:
        """True when A still has an unanswered genuine question/permission ask."""
        return bool(self._askable_items(events))

    def deliver_user_messages(
        self, events: Optional[list[AgentEvent]] = None
    ) -> Optional[dict[str, Any]]:
        """Deliver ONE queued user message to A (priority over C feedback)."""
        if not self._user_queue:
            return None
        if self._builder_has_pending_question(events):
            return None
        item = self._user_queue.pop(0)
        if item.get("request") == "[progress watch]":
            self.builder.send(f"ADVISOR NUDGE (progress watch): {item['framed']}")
        else:
            self.builder.send(
                f"USER MESSAGE (via advisor): {item['request']}\n\n"
                f"ADVISOR GUIDANCE: {item['framed']}")
        item["delivered"] = True
        return item

    def deliver_feedback(
        self, events: Optional[list[AgentEvent]] = None
    ) -> Optional[dict[str, Any]]:
        """Hand ONE queued (C→B) feedback item to A — only when A is available.

        Call :meth:`deliver_user_messages` first in the orchestrator loop so
        user notes take priority over C feedback. A is *available* when it is not
        waiting for an answer to its own question. Returns the delivered item
        (with ``delivered=True``), or ``None`` when nothing was delivered.
        """
        if not self._feedback_queue:
            return None
        if self._builder_has_pending_question(events):
            return None
        item = self._feedback_queue.pop(0)
        self.builder.send(f"VALIDATION FEEDBACK: {item['advice']}")
        item["delivered"] = True
        return item
