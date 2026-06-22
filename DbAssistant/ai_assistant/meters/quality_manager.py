"""QualityManager — user-perspective alignment check for Session B replies.

Scores how well the advisor's framed answer matches what the user actually
asked for, using deterministic token overlap (no model calls).
"""

from __future__ import annotations

from dataclasses import dataclass

from ai_assistant.meters.requirement_fidelity_meter import _tokens, _variants


@dataclass
class QualityReview:
    """Result of checking B's reply against the user requirement."""

    aligned: bool = True
    score: float = 1.0
    nudge: str = ""

    def as_dict(self) -> dict:
        return {
            "aligned": self.aligned,
            "score": round(self.score, 4),
            "nudge": self.nudge,
        }


class QualityManager:
    """Lightweight user-perspective quality gate for Session B."""

    def __init__(self, *, threshold: float = 0.35) -> None:
        self._threshold = threshold

    def review_advisor_reply(
        self,
        requirement: str,
        reply: str,
    ) -> QualityReview:
        """True when B's reply reflects the user's requirement domain."""
        if not (requirement or "").strip() or not (reply or "").strip():
            return QualityReview()

        req_tokens = _tokens(requirement)
        if not req_tokens:
            return QualityReview()

        reply_low = (reply or "").lower()
        hits = 0
        missing: list[str] = []
        for tok in req_tokens:
            if any(v in reply_low for v in _variants(tok)):
                hits += 1
            else:
                missing.append(tok)

        score = hits / len(req_tokens)
        aligned = score >= self._threshold
        nudge = ""
        if not aligned:
            hint = ", ".join(missing[:4]) if missing else requirement[:80]
            nudge = (
                "Stay faithful to the USER requirement — the advisor should "
                f"steer the builder toward: {hint}."
            )
        return QualityReview(aligned=aligned, score=score, nudge=nudge)
