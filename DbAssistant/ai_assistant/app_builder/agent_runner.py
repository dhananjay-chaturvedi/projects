"""Backend-agnostic agentic runner for the App Builder.

Launches AI CLIs in agentic (write+run) or ask mode with streaming output.
Does NOT use :meth:`AIBackend.call` — this is a separate subprocess path
reserved for App Builder autonomous builds.
"""

from __future__ import annotations

import json
import re
import subprocess
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from ai_assistant.app_builder import session_protocol
from ai_assistant.app_builder.pii_util import mask_if_enabled
from ai_query import module_config as mc
from ai_query.backends import AIBackend


class AgentMode(str, Enum):
    BUILD = "build"   # Session A — writes and runs in workspace (no --mode flag)
    ASK = "ask"       # Session B/C — Q&A / framing, read-only (no writes)
    PLAN = "plan"     # Session B — read-only planning turn that produces the plan


#: Read-only execution modes — the CLI makes no file edits in either. ``plan``
#: is planning-oriented, ``ask`` is Q&A; neither can write. Only the absence of
#: a ``--mode`` flag (``BUILD``) is write-capable.
_READONLY_MODES = frozenset({AgentMode.ASK, AgentMode.PLAN})


class AgentEventType(str, Enum):
    ASSISTANT_TEXT = "assistant_text"
    TOOL_CALL = "tool_call"
    FILE_WRITE = "file_write"
    SHELL_RUN = "shell_run"
    QUESTION = "question"
    SESSION_ID = "session_id"
    DONE = "done"
    ERROR = "error"
    NOTICE = "notice"  # non-fatal status (e.g. a turn produced no output)


# Event types that represent meaningful, user-visible activity within a turn.
# A turn that ends without ANY of these produced nothing the user can see, so
# the runner emits an explicit NOTICE rather than leaving the session silent.
_MEANINGFUL_EVENTS = frozenset({
    AgentEventType.ASSISTANT_TEXT,
    AgentEventType.TOOL_CALL,
    AgentEventType.FILE_WRITE,
    AgentEventType.SHELL_RUN,
    AgentEventType.QUESTION,
    AgentEventType.ERROR,
})


@dataclass
class AgentEvent:
    type: AgentEventType
    text: str = ""
    detail: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"type": self.type.value, "text": self.text, "detail": dict(self.detail)}


@dataclass(frozen=True)
class BackendCapabilities:
    """What each backend supports for App Builder agentic builds."""

    name: str
    agentic_write: bool
    streaming: bool
    resume: bool
    ask_mode: bool


_CAPABILITIES: dict[str, BackendCapabilities] = {
    "cursor": BackendCapabilities("cursor", True, True, True, True),
    "claude": BackendCapabilities("claude", True, True, True, True),
    "codex": BackendCapabilities("codex", True, True, False, True),
    # Offline trained NL->SQL backend: usable for Generate SQL, but never for
    # agentic file-writing app builds.
    "local-llm": BackendCapabilities("local-llm", False, False, False, False),
}


def capabilities(backend: Any) -> BackendCapabilities:
    """Return capability flags for *backend* (name or AIBackend instance)."""
    name = backend if isinstance(backend, str) else getattr(backend, "name", "")
    return _CAPABILITIES.get(name, BackendCapabilities(str(name), False, False, False, False))


def supports_agentic_write(backend: Any) -> bool:
    return capabilities(backend).agentic_write


def _cursor_model(mode: AgentMode) -> str:
    """Pick the Cursor model for *mode* (read-only turns may use a faster model)."""
    if mode in _READONLY_MODES:
        ask_model = (mc.get("ai.cursor", "ask_model", default="") or "").strip()
        if ask_model:
            return ask_model
    return mc.get("ai.cursor", "model", default="auto") or "auto"


def build_command(
    backend: AIBackend,
    *,
    prompt: str,
    workspace: Path,
    mode: AgentMode = AgentMode.BUILD,
    resume_session_id: Optional[str] = None,
) -> list[str]:
    """Construct the CLI argv for an agentic/ask invocation."""
    name = backend.name
    if name == "cursor":
        model = _cursor_model(mode)
        cmd = [
            backend._resolve_executable(), "agent",
            "--print", "--force",
            "--output-format", "stream-json",
            "--model", model,
        ]
        # Current Cursor CLI accepts only `plan` and `ask` for --mode, and BOTH
        # are read-only (no edits). Omitting --mode is the only write-capable
        # path. `ask` is Q&A (Session B answers); `plan` is the planning turn
        # (Session B produces the build plan) — neither can touch files.
        if mode == AgentMode.ASK:
            cmd.extend(["--mode", "ask"])
        elif mode == AgentMode.PLAN:
            cmd.extend(["--mode", "plan"])
        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])
        cmd.append(prompt)
        return cmd
    if name == "claude":
        # Read-only turns (ask/plan) use Claude's `plan` permission mode so the
        # advisor/validator cannot edit files; build turns accept edits.
        permission = "plan" if mode in _READONLY_MODES else "acceptEdits"
        cmd = [
            backend._resolve_executable(),
            "-p", "--output-format", "stream-json", "--verbose",
            "--permission-mode", permission,
        ]
        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])
        return cmd
    if name == "codex":
        cmd = [
            backend._resolve_executable(), "exec",
            "--json", "--dangerously-bypass-approvals-and-sandbox",
        ]
        model = mc.get("ai.codex", "model", default="")
        if model:
            cmd.extend(["--model", model])
        if mode in _READONLY_MODES:
            prompt = (
                "IMPORTANT: Reply ONLY with plain text. Do NOT edit files or "
                "run shell commands.\n" + prompt
            )
        cmd.append(prompt)
        return cmd
    raise ValueError(f"Backend {name!r} does not support agentic invocation")


def _stdin_for(backend: AIBackend, prompt: str) -> Optional[str]:
    """Backends that take the prompt on stdin instead of argv."""
    if backend.name == "claude":
        return prompt
    return None


# Stream-json event types that carry protocol metadata only — never dump the
# raw JSON line into the UI when we cannot extract human text from them.
_PROTOCOL_EV_TYPES = frozenset({
    "assistant", "user", "system", "result", "tool_use", "tool_result",
    "message", "init", "session", "ping", "stream_event",
    "thinking", "reasoning",
})


def parse_stream_line(line: str) -> list[AgentEvent]:
    """Normalize one line of stream-json (or plain text) into events."""
    line = (line or "").strip()
    if not line:
        return []
    events: list[AgentEvent] = []
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return [AgentEvent(AgentEventType.ASSISTANT_TEXT, text=line)]

    if not isinstance(payload, dict):
        return [AgentEvent(AgentEventType.ASSISTANT_TEXT, text=str(payload))]

    ev_type = str(payload.get("type") or payload.get("event") or "").lower()
    # Raw chain-of-thought / reasoning frames are internal noise: they stream as
    # space-split tokens ("I 'm not icing a conflict …") and protocol markers
    # ({"type":"thinking","subtype":"completed", …}). Never surface them.
    subtype = str(payload.get("subtype") or "").lower()
    if ("thinking" in ev_type or "reasoning" in ev_type
            or "thinking" in subtype or "reasoning" in subtype):
        return []
    sid = payload.get("session_id") or payload.get("chat_id") or payload.get("id")
    if sid and ev_type in ("", "system", "init", "session"):
        events.append(AgentEvent(
            AgentEventType.SESSION_ID, text=str(sid), detail={"session_id": str(sid)}))

    interaction = _interaction_query_event(payload, ev_type)
    if interaction is not None:
        return [interaction]

    # Claude/Cursor stream-json nests tool calls and narration under message.content.
    nested = _events_from_message_content(payload)
    if nested:
        events.extend(nested)
    else:
        text = _payload_text(payload)
        if isinstance(text, dict):
            text = text.get("text") or text.get("content") or str(text)
        if isinstance(text, str) and text.strip():
            events.append(AgentEvent(AgentEventType.ASSISTANT_TEXT, text=text.strip()))

        tool = _tool_label(payload)
        is_tool_event = bool(tool) or ev_type in (
            "tool_call", "tool_use", "tool", "file_write")
        is_file_write = (
            ev_type == "file_write"
            or bool(payload.get("path") or payload.get("file"))
            or "write" in tool.lower()
        )
        is_shell = (
            bool(payload.get("command"))
            or "shell" in tool.lower()
            or "bash" in tool.lower()
        )
        if is_tool_event:
            if is_file_write:
                path = payload.get("path") or payload.get("file") or tool
                events.append(AgentEvent(
                    AgentEventType.FILE_WRITE, text=str(path), detail=dict(payload)))
            elif is_shell:
                events.append(AgentEvent(
                    AgentEventType.SHELL_RUN,
                    text=str(payload.get("command") or tool),
                    detail=dict(payload),
                ))
            else:
                events.append(AgentEvent(
                    AgentEventType.TOOL_CALL,
                    text=tool,
                    detail=dict(payload),
                ))

    # Permission / approval requests surface as their own events so the loop can
    # auto-approve them (the builder must never sit waiting for a yes/no).
    if _is_permission_request(payload, ev_type):
        events.append(AgentEvent(
            AgentEventType.QUESTION,
            text="permission_request",
            detail={"permission": True, **payload},
        ))

    if ev_type in ("done", "complete", "result"):
        events.append(AgentEvent(AgentEventType.DONE, detail=dict(payload)))

    # Never surface raw protocol JSON (thinking blocks, tool_result echoes, etc.).
    if (not events and line
            and ev_type not in ("tool_call", "tool_use", "tool")
            and ev_type not in _PROTOCOL_EV_TYPES):
        events.append(AgentEvent(AgentEventType.ASSISTANT_TEXT, text=line))
    return events


def _events_from_message_content(payload: dict[str, Any]) -> list[AgentEvent]:
    """Extract user-visible events from nested Claude/Cursor message.content."""
    message = payload.get("message")
    if not isinstance(message, dict):
        return []
    parts = message.get("content")
    if not isinstance(parts, list):
        return []
    events: list[AgentEvent] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        kind = str(part.get("type") or "").lower()
        if kind == "text":
            text = str(part.get("text") or "").strip()
            if text:
                events.append(AgentEvent(AgentEventType.ASSISTANT_TEXT, text=text))
        elif kind in ("thinking", "redacted_thinking"):
            continue
        elif kind == "tool_use":
            name = str(part.get("name") or "tool").strip()
            tool_input = part.get("input") if isinstance(part.get("input"), dict) else {}
            path = tool_input.get("file_path") or tool_input.get("path")
            command = tool_input.get("command")
            if name.lower() in ("write", "edit", "multiedit") and path:
                events.append(AgentEvent(
                    AgentEventType.FILE_WRITE, text=str(path), detail=dict(part)))
            elif name.lower() in ("bash", "shell") and command:
                events.append(AgentEvent(
                    AgentEventType.SHELL_RUN, text=str(command), detail=dict(part)))
            else:
                label = name
                if path:
                    label = f"{name} {path}"
                elif command:
                    label = f"{name} {command}"
                events.append(AgentEvent(
                    AgentEventType.TOOL_CALL, text=label, detail=dict(part)))
        elif kind == "tool_result":
            # Tool results are echoed back to the model; suppress from the UI.
            continue
    return events


def _interaction_query_event(
    payload: dict[str, Any], ev_type: str
) -> Optional[AgentEvent]:
    """Normalize Cursor native interaction queries into App Builder events.

    Cursor in ``--print`` mode cannot wait for a UI answer, so it may stream a
    request followed by an internal "Questions skipped by the user" response.
    We still capture the request as a builder QUESTION so Session B can frame an
    answer on the next turn, and suppress the raw skipped response from the UI.
    """
    if ev_type != "interaction_query":
        return None
    subtype = str(payload.get("subtype") or "").lower()
    query_type = str(payload.get("query_type") or "")
    if subtype == "response":
        response = payload.get("response") or {}
        rejected = ""
        if isinstance(response, dict):
            nested = response.get("askQuestionInteractionResponse") or {}
            result = nested.get("result") if isinstance(nested, dict) else {}
            if isinstance(result, dict):
                rej = result.get("rejected") or {}
                if isinstance(rej, dict):
                    rejected = str(rej.get("reason") or "")
        if "questions skipped by the user" in rejected.lower():
            return AgentEvent(
                AgentEventType.NOTICE,
                text="",
                detail={
                    "interaction_response": True,
                    "suppressed_skip": True,
                    **payload,
                },
            )
        return AgentEvent(
            AgentEventType.NOTICE,
            text=rejected or "backend interaction response received",
            detail={"interaction_response": True, **payload},
        )
    if subtype != "request" or query_type != "askQuestionInteractionQuery":
        return None

    query = payload.get("query") or {}
    if not isinstance(query, dict):
        return None
    ask = query.get("askQuestionInteractionQuery") or {}
    if not isinstance(ask, dict):
        return None
    args = ask.get("args") or {}
    if not isinstance(args, dict):
        return None
    title = str(args.get("title") or "").strip()
    questions = args.get("questions") or []
    if not isinstance(questions, list) or not questions:
        return None

    structured_questions: list[dict[str, Any]] = []
    for q in questions:
        if not isinstance(q, dict):
            continue
        qp = str(q.get("prompt") or "").strip()
        allow_mult = bool(q.get("allowMultiple"))
        raw_opts = q.get("options") or []
        option_items: list[dict[str, str]] = []
        if isinstance(raw_opts, list):
            for opt in raw_opts:
                if not isinstance(opt, dict):
                    continue
                oid = str(opt.get("id") or "").strip()
                label = str(opt.get("label") or "").strip()
                if oid or label:
                    option_items.append({"id": oid, "label": label})
        structured_questions.append({
            "prompt": qp,
            "option_items": option_items,
            "allow_multiple": allow_mult,
        })

    first = structured_questions[0] if structured_questions else {}
    prompt = str(first.get("prompt") or title or "Builder asked a question").strip()
    option_items = list(first.get("option_items") or [])
    option_lines = [
        f"{item['id']}: {item['label']}".strip(": ")
        for item in option_items if item.get("id") or item.get("label")
    ]
    allow_multiple = bool(first.get("allow_multiple"))
    full = prompt
    if title and title not in prompt:
        full = f"{title}: {prompt}"
    if option_lines:
        full += "\nOptions:\n" + "\n".join(f"- {line}" for line in option_lines)
    return AgentEvent(
        AgentEventType.QUESTION,
        text=full,
        detail={
            "interaction_question": True,
            "title": title,
            "prompt": prompt,
            "options": option_lines,
            "option_items": option_items,
            "allow_multiple": allow_multiple,
            "questions": structured_questions,
            **payload,
        },
    )


def _tool_label(payload: dict[str, Any]) -> str:
    """Best human-readable label for a tool event, or empty if it is generic.

    Some streaming backends emit many low-level events shaped only like
    ``{"type": "tool_call"}``. Showing each as ``tool_call: tool_call`` creates
    noisy, useless transcript spam, so we only surface events that contain a
    meaningful tool/function/path/command label.
    """
    for key in ("tool", "tool_name", "name"):
        value = payload.get(key)
        if value:
            label = str(value).strip()
            if label and label not in ("tool_call", "tool_use", "tool"):
                return label
    function = payload.get("function")
    if isinstance(function, dict):
        name = str(function.get("name") or "").strip()
        if name:
            return name
    return ""


def _is_permission_request(payload: dict[str, Any], ev_type: str) -> bool:
    """Detect a backend permission/approval request in a stream event."""
    if ev_type in ("permission_request", "approval_request", "ask_permission",
                   "tool_permission"):
        return True
    sub = str(payload.get("subtype") or payload.get("status") or "").lower()
    if "permission" in sub or "approval" in sub:
        return True
    return bool(payload.get("permission") or payload.get("requires_approval"))


def _payload_text(payload: dict[str, Any]) -> Any:
    """Extract human text from common stream-json shapes."""
    text = (
        payload.get("text") or payload.get("content") or payload.get("message")
        or payload.get("result") or payload.get("response") or ""
    )
    if isinstance(text, dict):
        parts = text.get("content")
        if isinstance(parts, list):
            collected = []
            for part in parts:
                if isinstance(part, dict) and part.get("type") == "text":
                    collected.append(str(part.get("text") or ""))
            if collected:
                return "\n".join(collected)
        return text.get("text") or text.get("content") or ""
    if isinstance(text, list):
        collected = []
        for part in text:
            if isinstance(part, dict) and part.get("type") == "text":
                collected.append(str(part.get("text") or ""))
        if collected:
            return "\n".join(collected)
    return text


# Tokens that mark a "?"-terminated sentence as a real ask to the user/decision,
# rather than rhetorical narration. A genuine question must end with "?" AND
# contain one of these (word-ish match), so the builder is answered only when it
# truly asks something.
_DECISION_TOKENS = (
    "you", "your", "should", "shall", "which", "what", "where", "when",
    "how", "why", "do ", "does", "would", "could", "can ", "may i",
    "prefer", "approve", "confirm", "choose", "option", "want", "need",
    "use", "proceed", "ready", "ok to", " or ",
)

# Phrases that mean the agent finished — used to honor completion and stop.
_DONE_MARKERS = (
    "build complete", "build is complete", "implementation complete",
    "the app is complete", "the app is ready", "app is now complete",
    "all tests pass", "all tests passing", "all tests are passing",
    "nothing left", "no further changes", "no more changes",
    "everything is implemented", "fully implemented", "finished building",
    "i am done", "i'm done", "task complete", "task is complete",
)

# Phrases that mean the agent is idle/waiting (not a question, but a reason to
# nudge it forward once rather than answer it).
_IDLE_MARKERS = (
    "awaiting", "waiting for", "let me know", "standing by",
    "ready when you are", "please advise", "your guidance", "your input",
)


def is_genuine_question(text: str) -> bool:
    """True only for a real, user-directed/decision question (ends with '?')."""
    t = (text or "").strip().lower()
    if not t.endswith("?"):
        return False
    if len(t) < 6 or len(t) > 400:
        return False
    if len(t.split()) < 2:
        return False
    return any(tok in t for tok in _DECISION_TOKENS)


def detect_questions(text: str) -> list[str]:
    """Extract genuine, user-directed questions from assistant text.

    Only sentences that end with '?' and read as a real ask are returned;
    narration and idle/"awaiting" phrasing are intentionally ignored so the
    builder is not answered for things it never actually asked.
    """
    if not text:
        return []
    found: list[str] = []
    for seg in re.split(r"(?<=[?.!])\s+|\n+", text):
        s = seg.strip().lstrip("-*•# ").strip()
        if is_genuine_question(s) and s not in found:
            found.append(s)
    return found


# Structured ask markers Session A should emit when it needs user input.
_ASK_MARKER_RE = re.compile(
    r"(?im)^\s*(?:ASK|CONFIRM|APPROVE)\s*:\s*(.+)$",
)
_PHASE_DONE_RE = session_protocol.PHASE_DONE_RE

_CONFIRM_TOKENS = (
    "confirm", "correct", "right", "okay", "ok to", "is this", "does this",
    "should i", "shall i", "proceed with", "go ahead",
)
_APPROVE_TOKENS = (
    "approve", "permission", "allow", "may i", "can i", "should i make",
    "should i add", "should i delete", "should i change", "apply this",
)


def classify_ask_intent(question: str) -> str:
    """Token-based intent for a builder ask (confirm|approve|decide|open)."""
    t = (question or "").strip().lower()
    if not t:
        return "open"
    if any(tok in t for tok in _APPROVE_TOKENS):
        return "approve"
    if any(tok in t for tok in _CONFIRM_TOKENS):
        return "confirm"
    if t.endswith("?"):
        return "decide"
    return "open"


def extract_marked_asks(text: str) -> list[tuple[str, str]]:
    """Parse structured ASK:/CONFIRM:/APPROVE: lines from Session A output.

    Returns ``(intent, question)`` pairs. ``intent`` is one of
    ``confirm|approve|decide|open``. Marked asks are preferred over heuristic
    detection because they carry an explicit intent the advisor can answer
    against requirement and build progress.
    """
    if not text:
        return []
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for m in _ASK_MARKER_RE.finditer(text):
        body = m.group(1).strip().lstrip("-*•# ").strip()
        if not body or len(body) < 3:
            continue
        prefix = m.group(0).split(":", 1)[0].strip().upper()
        if prefix == "CONFIRM":
            intent = "confirm"
        elif prefix == "APPROVE":
            intent = "approve"
        else:
            intent = classify_ask_intent(body)
        key = " ".join(body.lower().split())
        if key in seen:
            continue
        seen.add(key)
        found.append((intent, body))
    return found


def detect_phase_done(text: str) -> list[str]:
    """Components Session A marked complete via ``PHASE-DONE: <component>``."""
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for m in _PHASE_DONE_RE.finditer(text):
        comp = m.group(1).strip().lower().replace("/", "_")
        if comp and comp not in seen:
            seen.add(comp)
            found.append(comp)
    return found


def agent_signaled_done(text: str) -> bool:
    """True when the agent reports the build is finished/complete."""
    if not text:
        return False
    low = text.lower()
    if any(m in low for m in _DONE_MARKERS):
        return True
    for line in text.splitlines():
        s = line.strip().strip(".!*_#> ").lower()
        if s in ("done", "build done", "completed", "complete"):
            return True
    return False


def agent_is_idle(text: str) -> bool:
    """True when the agent is waiting/idle rather than making progress."""
    if not text:
        return False
    low = text.lower()
    return any(m in low for m in _IDLE_MARKERS)


EventCallback = Callable[[AgentEvent], None]


class AgentRunner:
    """Run one persistent backend session with streaming events."""

    def __init__(
        self,
        backend: AIBackend,
        workspace: Path,
        *,
        mode: AgentMode = AgentMode.BUILD,
        timeout: int = 300,
        on_event: Optional[EventCallback] = None,
        cancel_event: Optional[threading.Event] = None,
        mask_pii: bool = False,
    ) -> None:
        self._backend = backend
        self._workspace = Path(workspace)
        self._mode = mode
        self._timeout = int(timeout)
        self._on_event = on_event
        self._cancel = cancel_event
        self._mask_pii = bool(mask_pii)
        self._proc: Optional[subprocess.Popen] = None
        self._session_id: Optional[str] = None
        self._transcript: list[str] = []
        self._lock = threading.Lock()

    @property
    def cancelled(self) -> bool:
        return bool(self._cancel is not None and self._cancel.is_set())

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    @property
    def transcript(self) -> str:
        return "\n".join(self._transcript)

    def run(self, prompt: str, *, mode: Optional[AgentMode] = None) -> list[AgentEvent]:
        """Send *prompt* (new or resumed) and collect all events.

        *mode* optionally overrides this runner's mode for a single turn while
        keeping the SAME persistent session (resumes ``session_id``). Used so an
        ASK-only turn (e.g. an outline) can run on the build session without
        spinning up a separate agent session.
        """
        events: list[AgentEvent] = []
        for ev in self.iter_events(prompt, mode=mode):
            events.append(ev)
        return events

    def cancel(self) -> None:
        """Kill the running subprocess (if any) — used to abort a build."""
        proc = self._proc
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass

    def iter_events(
        self, prompt: str, *, mode: Optional[AgentMode] = None
    ) -> Iterator[AgentEvent]:
        """Stream events from one agent invocation."""
        prompt = mask_if_enabled(prompt, self._mask_pii)
        if self.cancelled:
            ev = AgentEvent(AgentEventType.ERROR, text="cancelled")
            self._handle_event(ev)
            yield ev
            return
        cmd = build_command(
            self._backend,
            prompt=prompt,
            workspace=self._workspace,
            mode=mode or self._mode,
            resume_session_id=self._session_id,
        )
        stdin_text = _stdin_for(self._backend, prompt)
        # Always give the child a definite stdin: a pipe we feed (and close) when
        # there is prompt text, otherwise DEVNULL so it gets an immediate EOF and
        # can never block waiting for input.
        proc = subprocess.Popen(
            cmd,
            cwd=str(self._workspace),
            stdin=subprocess.PIPE if stdin_text else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._proc = proc
        if stdin_text and proc.stdin:
            proc.stdin.write(stdin_text)
            proc.stdin.close()

        # Watcher: if a cancel is requested while we're blocked reading stdout,
        # kill the process so the stream ends promptly.
        stop_watch = threading.Event()
        watcher: Optional[threading.Thread] = None
        if self._cancel is not None:
            def _watch() -> None:
                while not stop_watch.is_set():
                    if self._cancel.wait(timeout=0.25):
                        try:
                            proc.kill()
                        except Exception:
                            pass
                        return
                    if stop_watch.is_set():
                        return
            watcher = threading.Thread(target=_watch, daemon=True)
            watcher.start()

        assert proc.stdout is not None
        cancelled = False
        produced = False
        try:
            for line in proc.stdout:
                if self.cancelled:
                    cancelled = True
                    break
                for ev in parse_stream_line(line):
                    if ev.type in _MEANINGFUL_EVENTS:
                        produced = True
                    self._handle_event(ev)
                    yield ev
        finally:
            stop_watch.set()
            timed_out = False
            try:
                proc.wait(timeout=self._timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                timed_out = True
                err = AgentEvent(AgentEventType.ERROR,
                                 text=f"timed out after {self._timeout}s")
                self._handle_event(err)
                yield err
            err_text = ""
            if proc.stderr:
                try:
                    err_text = proc.stderr.read().strip()
                except Exception:  # noqa: BLE001
                    err_text = ""
            if cancelled or self.cancelled:
                err = AgentEvent(AgentEventType.ERROR, text="cancelled")
                self._handle_event(err)
                yield err
            elif proc.returncode and proc.returncode != 0:
                if err_text:
                    err = AgentEvent(AgentEventType.ERROR, text=err_text)
                    self._handle_event(err)
                    yield err
                else:
                    notice = AgentEvent(
                        AgentEventType.NOTICE,
                        text=(f"backend exited with code {proc.returncode} "
                              "and produced no output"),
                        detail={"returncode": proc.returncode})
                    self._handle_event(notice)
                    yield notice
            elif not produced and not timed_out:
                # The turn finished cleanly but emitted nothing the user can
                # see. Surface an explicit notice (with any stderr) so the
                # session never appears frozen/blank.
                msg = "backend produced no output this turn"
                if err_text:
                    msg += f" — stderr: {err_text[:500]}"
                notice = AgentEvent(
                    AgentEventType.NOTICE, text=msg,
                    detail={"returncode": proc.returncode,
                            "stderr": err_text[:2000]})
                self._handle_event(notice)
                yield notice
            done = AgentEvent(AgentEventType.DONE,
                              detail={"returncode": proc.returncode})
            self._handle_event(done)
            yield done

    def _handle_event(self, ev: AgentEvent) -> None:
        with self._lock:
            if ev.type == AgentEventType.SESSION_ID:
                sid = ev.detail.get("session_id") or ev.text
                if sid:
                    self._session_id = str(sid)
                return
            if ev.type == AgentEventType.ASSISTANT_TEXT and ev.text:
                self._transcript.append(ev.text)
            if self._on_event:
                try:
                    self._on_event(ev)
                except Exception:
                    pass
