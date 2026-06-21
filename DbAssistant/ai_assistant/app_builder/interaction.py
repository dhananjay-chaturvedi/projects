"""Interaction control for the App Builder — how much it asks the user.

The build can run at three interaction levels, plus an *uninterrupted* override:

* **uninterrupted** — the agent asks NOTHING. It understands, follows up and
  iterates entirely on its own, taking the default for every decision.
* **auto** — autonomous, but it still surfaces the few *critical* decisions
  (e.g. "deploy tables to your live connection?"). Everything else is auto-taken.
* **interactive** — the agent asks the user to approve/choose *most* decisions
  (the plan, the data understanding, whether to apply each AI round, …).

This module is UI-agnostic: it defines the decision objects and a single
:class:`BuildDecider` that resolves a decision either silently (default) or by
delegating to an injected ``ask`` callback (the Tk/Web layer supplies one that
pops a dialog). It records every decision so the result is auditable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

# Interaction levels (most → least autonomous).
UNINTERRUPTED = "uninterrupted"
AUTO = "auto"
INTERACTIVE = "interactive"
LEVELS = (UNINTERRUPTED, AUTO, INTERACTIVE)

AskFn = Callable[["BuildDecision"], Any]


@dataclass
class BuildDecision:
    """A single decision the builder may surface to the user."""

    id: str
    question: str
    kind: str = "approve"  # approve | choice | interaction (agent-native UI)
    options: list[str] = field(default_factory=list)
    default: Any = True
    critical: bool = False
    detail: str = ""
    # Agent askQuestionInteractionQuery — rendered natively in interactive mode.
    agent_options: list[dict[str, str]] = field(default_factory=list)
    allow_multiple: bool = False
    recommendation: str = ""  # Session B's proposed answer (optional for user)

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id, "question": self.question, "kind": self.kind,
            "options": list(self.options), "default": self.default,
            "critical": self.critical, "detail": self.detail,
            "agent_options": list(self.agent_options),
            "allow_multiple": self.allow_multiple,
            "recommendation": self.recommendation,
        }


class BuildDecider:
    """Resolve build decisions per interaction level; logs every answer.

    *ask* is an optional callback (supplied by the UI) that returns the user's
    answer for a :class:`BuildDecision`. When *ask* is ``None`` the decider is
    fully silent regardless of level (used for headless/auto builds).
    """

    def __init__(
        self,
        *,
        level: str = AUTO,
        uninterrupted: bool = False,
        ask: Optional[AskFn] = None,
    ) -> None:
        self.level = level if level in LEVELS else AUTO
        # An explicit uninterrupted toggle (or the uninterrupted level) wins.
        self.uninterrupted = bool(uninterrupted) or self.level == UNINTERRUPTED
        self._ask = ask
        self.log: list[dict[str, Any]] = []

    @property
    def interactive(self) -> bool:
        return (not self.uninterrupted) and self.level == INTERACTIVE \
            and self._ask is not None

    def set_level(self, level: str, *, ask: Optional[AskFn] = None) -> None:
        """Change the interaction level at runtime (e.g. user takes control).

        Attribute writes are atomic in CPython, so the build worker thread picks
        up the new level on its next decision. If *ask* is given it replaces/
        installs the callback so a switch into interactive has a working prompt.
        """
        if level in LEVELS:
            self.level = level
            self.uninterrupted = (level == UNINTERRUPTED)
        if ask is not None:
            self._ask = ask

    def take_control(self, *, ask: Optional[AskFn] = None) -> None:
        """Shortcut: switch to fully interactive so the user approves decisions."""
        self.set_level(INTERACTIVE, ask=ask)

    def _should_ask(self, decision: BuildDecision) -> bool:
        if self.uninterrupted or self._ask is None:
            return False
        if self.level == INTERACTIVE:
            return True
        if self.level == AUTO:
            return decision.critical  # auto asks only critical decisions
        return False

    def decide(self, decision: BuildDecision) -> Any:
        """Return the answer for *decision* (asking the user only when warranted)."""
        if self._should_ask(decision):
            try:
                answer = self._ask(decision)
            except Exception:  # noqa: BLE001 — never let UI errors break a build
                answer = decision.default
            asked = True
        else:
            # For critical decisions in AUTO mode with no interactive callback,
            # default to False (safe: don't auto-approve DDL deploys, etc.).
            if decision.critical and self.level == AUTO and self._ask is None and not self.uninterrupted:
                answer = False
            else:
                answer = decision.default
            asked = False
        if answer is None:
            answer = decision.default
        self.log.append({
            "id": decision.id, "question": decision.question,
            "asked": asked, "answer": answer, "critical": decision.critical,
        })
        return answer

    def approved(self, decision: BuildDecision) -> bool:
        """Convenience for yes/no decisions → bool."""
        return bool(self.decide(decision))


def decider_from_options(
    *,
    interaction: str = AUTO,
    uninterrupted: bool = False,
    ask: Optional[AskFn] = None,
) -> BuildDecider:
    """Build a :class:`BuildDecider` from plain options (e.g. request body)."""
    return BuildDecider(level=interaction, uninterrupted=uninterrupted, ask=ask)
