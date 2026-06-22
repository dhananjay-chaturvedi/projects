"""intent_classifier — deterministic question vs recommendation detection.

The App Builder Assistant routes messages between sessions and needs to know,
without calling a model, what *kind* of message a session emitted: is Session A
asking a QUESTION (it needs an answer before it can proceed), making a
RECOMMENDATION / decision, reporting PROGRESS, or signalling it is DONE?

This is the "factual domain" classifier the product spec calls for: match the
interrogatives (why / what / how / which / should I …?) for questions, and the
evaluative phrasings (we can / it is better / I recommend / a good option …)
for recommendations — purely lexical, fully deterministic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class MessageIntent(str, Enum):
    """The kind of message a session produced."""

    QUESTION = "question"
    RECOMMENDATION = "recommendation"
    PROGRESS = "progress"
    DONE = "done"
    STATEMENT = "statement"


# Interrogatives / request-for-decision markers → QUESTION.
_QUESTION_LEAD = (
    "why", "what", "how", "which", "who", "where", "when", "whose", "whom",
)
_QUESTION_PHRASES = (
    "should i", "should we", "do i", "do we", "can i", "can we", "could you",
    "would you", "is it", "are there", "shall i", "shall we", "may i",
    "do you want", "which one", "what should", "how should", "ask:", "confirm:",
    "approve:", "please confirm", "please advise", "need your input",
    "let me know", "your call", "?",
)

# Evaluative / advisory markers → RECOMMENDATION.
_RECOMMENDATION_PHRASES = (
    "i recommend", "i suggest", "we should", "we can", "you should",
    "it is better", "it's better", "a better option", "a good option",
    "the best option", "i would go with", "i'd go with", "my recommendation",
    "prefer", "advisable", "ideally", "best practice", "i propose",
    "let's use", "lets use", "go with", "opt for",
)

# Progress / status markers → PROGRESS.
_PROGRESS_PHRASES = (
    "i implemented", "i added", "i created", "i built", "i wrote", "i've added",
    "i have added", "completed", "finished", "implemented", "added the",
    "created the", "now working on", "next i will", "progress:", "status:",
    "phase-done", "phase done", "working on",
)

# Completion markers → DONE.
_DONE_PHRASES = (
    "build complete", "build is complete", "app is complete", "all done",
    "everything is done", "fully implemented", "ready for review",
    "ready for the user", "done building", "i am done", "i'm done",
    "the application is complete",
)

_WORD_RE = re.compile(r"[a-z']+")


@dataclass
class _Scan:
    has_question_mark: bool
    leads_with_interrogative: bool
    question_hits: int
    recommendation_hits: int
    progress_hits: int
    done_hits: int


def _scan(text: str) -> _Scan:
    low = (text or "").lower().strip()
    first_word = ""
    m = _WORD_RE.search(low)
    if m:
        first_word = m.group(0)
    return _Scan(
        has_question_mark="?" in low,
        leads_with_interrogative=first_word in _QUESTION_LEAD,
        question_hits=sum(1 for p in _QUESTION_PHRASES if p in low),
        recommendation_hits=sum(1 for p in _RECOMMENDATION_PHRASES if p in low),
        progress_hits=sum(1 for p in _PROGRESS_PHRASES if p in low),
        done_hits=sum(1 for p in _DONE_PHRASES if p in low),
    )


def classify_intent(text: str) -> MessageIntent:
    """Classify *text* into a :class:`MessageIntent` deterministically.

    Precedence is chosen for routing safety: an explicit DONE claim wins, then a
    QUESTION (A is blocked and needs an answer), then RECOMMENDATION, then
    PROGRESS, else a plain STATEMENT.
    """
    s = _scan(text)
    if s.done_hits and not s.has_question_mark:
        return MessageIntent.DONE
    if s.has_question_mark or s.leads_with_interrogative or s.question_hits:
        # A question mark or interrogative phrasing dominates: A wants an answer.
        return MessageIntent.QUESTION
    if s.recommendation_hits > s.progress_hits:
        return MessageIntent.RECOMMENDATION
    if s.progress_hits:
        return MessageIntent.PROGRESS
    if s.recommendation_hits:
        return MessageIntent.RECOMMENDATION
    return MessageIntent.STATEMENT


class IntentClassifier:
    """Reusable wrapper around :func:`classify_intent` with confidence scoring."""

    name = "intent_classifier"

    def detail(self, text: str) -> dict:
        """Return the intent plus the lexical evidence behind it."""
        s = _scan(text)
        intent = classify_intent(text)
        signals = {
            "question": s.question_hits + int(s.has_question_mark)
            + int(s.leads_with_interrogative),
            "recommendation": s.recommendation_hits,
            "progress": s.progress_hits,
            "done": s.done_hits,
        }
        total = sum(signals.values()) or 1
        confidence = signals.get(intent.value, 0) / total if intent.value in signals else 0.0
        return {
            "intent": intent.value,
            "confidence": round(confidence, 4),
            "signals": signals,
        }
