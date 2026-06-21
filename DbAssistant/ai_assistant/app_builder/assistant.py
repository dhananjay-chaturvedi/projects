"""AppBuilderAssistant — the central, isolated message router for A/B/C.

The App Builder Assistant is the *only* thing that moves messages between the
three sessions, and it does so under a fixed set of rules:

* Session A (builder) may send a request/answer/progress note **directly** to
  any other session (A→B, A→C).
* Session C (validator) must always reach Session A **through** Session B
  (C→B→A) — it never speaks to A directly.
* When the assistant itself needs to tell a session something, it always routes
  **via Session B** (assistant→B→target).
* The assistant transfers content **verbatim** — it never injects, rewrites or
  appends to the body of a session's request or response. (Session B may author
  its own reply/instruction; that is B's content, not assistant injection.)

On top of routing, the assistant is the single owner of measurement: it runs the
:class:`~ai_assistant.app_builder.meters.registry.AppMeterRegistry` and the
:class:`~ai_assistant.app_builder.meter_managers.registry.MeterManagerRegistry`,
tracks build progress / correctness / design similarity, and turns failing
meters into remediation signals that it routes to Session B for a human-like
judgement before they reach Session A.

This module is deliberately decoupled from the concrete session classes: it
talks to them through a tiny duck-typed protocol (``builder.send`` /
``advisor.frame_answer`` / ``validator.validate``), so it is fully unit-testable
with light fakes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable, Mapping, Optional

from ai_assistant.app_builder.meters.design_plan import DesignPlan
from ai_assistant.app_builder.meters.registry import AppMeterRegistry, QualityInput
from ai_assistant.app_builder.meter_managers.registry import MeterManagerRegistry


class Session(str, Enum):
    """The three build sessions plus the assistant itself."""

    A = "A"  # builder
    B = "B"  # advisor / answerer
    C = "C"  # validator
    ASSISTANT = "assistant"


@dataclass
class RoutedMessage:
    """One verbatim hop recorded by the assistant for transparency/UI."""

    sender: str
    recipient: str
    text: str
    intent: str = ""
    via: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "sender": self.sender, "recipient": self.recipient,
            "intent": self.intent, "via": self.via,
            "text": (self.text or "")[:2000],
        }


class RoutingError(RuntimeError):
    """Raised when a route violates the communication rules."""


@dataclass
class AppBuilderAssistant:
    """Central router + measurement owner for the triple-session build."""

    builder: Any            # Session A: .send(text)->events, .last_text, .transcript
    advisor: Any            # Session B: .frame_answer(q, *, brief, context="")->str
    validator: Any = None   # Session C: .validate(d, *, brief, context="")->str
    brief: Any = None
    meters: AppMeterRegistry = field(default_factory=AppMeterRegistry)
    managers: MeterManagerRegistry = field(default_factory=MeterManagerRegistry)
    on_relay: Optional[Callable[[dict[str, Any]], None]] = None
    on_event: Optional[Callable[[dict[str, Any]], None]] = None

    # ── tracked state ─────────────────────────────────────────────────────────
    log: list[RoutedMessage] = field(default_factory=list)
    design: Optional[DesignPlan] = None
    last_quality: Optional[dict[str, Any]] = None

    # ── low-level verbatim routing ─────────────────────────────────────────────
    def _record(self, sender: str, recipient: str, text: str, *,
                intent: str = "", via: str = "") -> None:
        msg = RoutedMessage(sender=sender, recipient=recipient, text=text or "",
                            intent=intent, via=via)
        self.log.append(msg)
        if self.on_relay is not None:
            try:
                self.on_relay(msg.as_dict())
            except Exception:  # noqa: BLE001
                pass

    def _deliver(self, recipient: Session, text: str, *, context: str = "") -> str:
        """Hand *text* to the recipient session verbatim and return any reply."""
        if recipient is Session.A:
            self.builder.send(text)
            return ""
        if recipient is Session.B:
            framer = getattr(self.advisor, "frame_answer", None)
            if callable(framer):
                return framer(text, brief=self.brief, context=context) or ""
            return ""
        if recipient is Session.C:
            if self.validator is None:
                return ""
            sender = getattr(self.validator, "send", None)
            if callable(sender):
                sender(text)
            return ""
        return ""

    def route(self, sender: Session, recipient: Session, text: str, *,
              intent: str = "", context: str = "") -> str:
        """Route *text* from *sender* to *recipient*, enforcing the rules.

        Content is transferred verbatim. Returns the recipient's reply text when
        the recipient is Session B (the only session that synchronously replies
        through ``frame_answer``); otherwise an empty string.
        """
        sender = Session(sender)
        recipient = Session(recipient)
        if sender is recipient:
            raise RoutingError("a session cannot route to itself")

        # Rule: C must reach A via B.
        if sender is Session.C and recipient is Session.A:
            raise RoutingError("validator (C) must reach the builder (A) via B")
        # Rule: the assistant addresses sessions only via B.
        if sender is Session.ASSISTANT and recipient is not Session.B:
            raise RoutingError("assistant must route to other sessions via B")

        self._record(sender.value, recipient.value, text, intent=intent)
        return self._deliver(recipient, text, context=context)

    # ── assistant → B → target ─────────────────────────────────────────────────
    def assistant_note(self, text: str, *, to: Session = Session.A,
                       context: str = "") -> str:
        """Assistant tells a session something — always via B.

        The note goes to B verbatim; B decides what (if anything) to forward to
        the target session and authors that message itself.
        """
        to = Session(to)
        self._record(Session.ASSISTANT.value, Session.B.value, text,
                     intent="assistant_note", via=to.value)
        framer = getattr(self.advisor, "frame_answer", None)
        forwarded = ""
        if callable(framer):
            prompt = (f"The App Builder Assistant has a note intended for Session "
                      f"{to.value}. Decide what to relay and author the message.\n"
                      f"NOTE:\n{text}")
            forwarded = framer(prompt, brief=self.brief, context=context) or ""
        if forwarded and to is not Session.B:
            # B is the author; assistant just delivers B's message to the target.
            recipient = to
            if recipient is Session.C and self.validator is None:
                return forwarded
            self.route(Session.B, recipient, forwarded, intent="relayed_note")
        return forwarded

    # ── measurement ownership ───────────────────────────────────────────────────
    def check_design_similarity(
        self, plans: Iterable[DesignPlan], *, threshold: float = 0.8,
    ) -> dict[str, Any]:
        """Score A/B/C plan agreement and record it (the understanding gate)."""
        result = self.meters.evaluate_design_similarity(
            list(plans), threshold=threshold)
        self._emit("design_similarity", result)
        return result

    def evaluate_quality(
        self,
        quality: QualityInput | Mapping[str, str],
        **legacy_fields,
    ) -> dict[str, Any]:
        """Run the quality battery once, record it, and emit the readout."""
        measurements = self.meters.quality_measurements(
            QualityInput.from_source(quality, **legacy_fields))
        report = self.meters.report_from_measurements(measurements)
        self.last_quality = report
        self._last_measurements = list(measurements.values())
        self._emit("quality", report)
        return report

    def design_completeness(
        self, files: Mapping[str, str], *, threshold: float = 0.8,
    ) -> Optional[dict[str, Any]]:
        """How much of the agreed design is built (B's progress signal)."""
        if self.design is None:
            return None
        result = self.meters.evaluate_design_completeness(
            self.design, files, threshold=threshold)
        self._emit("design_completeness", result)
        return result

    # ── helpers ─────────────────────────────────────────────────────────────────
    def _emit(self, kind: str, detail: Any) -> None:
        if self.on_event is None:
            return
        try:
            self.on_event({"type": kind, "detail": detail})
        except Exception:  # noqa: BLE001
            pass
