"""Base types for meter-managers.

A :class:`MeterManager` consumes a :class:`~ai_assistant.meters.base.Measurement`
and decides whether the build is meeting that meter's acceptance bar. When it is
not, the manager emits a :class:`RemediationSignal`: a factual, structured
description of how far behind the app is (the *deficit*), *why* (the failing
components and the meter's own issues), and a neutral suggestion the advisor can
act on. Managers are deterministic and never call a model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ai_assistant.meters.base import Measurement


class Severity(str, Enum):
    """How urgently a meter result needs attention."""

    OK = "ok"
    WARNING = "warning"
    FAIL = "fail"


@dataclass
class RemediationSignal:
    """A structured "what is missing and why" note for the advisor (Session B)."""

    meter: str
    severity: Severity
    score: float
    threshold: float
    deficit: float
    reason: str
    missing: list[str] = field(default_factory=list)
    suggestion: str = ""
    component: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "meter": self.meter,
            "severity": self.severity.value,
            "score": round(self.score, 4),
            "threshold": round(self.threshold, 4),
            "deficit": round(self.deficit, 4),
            "reason": self.reason,
            "missing": list(self.missing),
            "suggestion": self.suggestion,
            "component": self.component,
        }


class MeterManager:
    """Default policy: gate a meter and explain any shortfall.

    Subclasses override :meth:`_subject` (what the meter is about) and
    :meth:`_suggest` (a neutral, factual remediation hint) to tailor the
    language; the gating maths is shared.
    """

    #: meter name this manager governs (must match the registry key).
    meter_name: str = "meter"
    #: results within ``warning_band`` *below* threshold are WARNING not FAIL.
    warning_band: float = 0.1

    def severity(self, m: Measurement) -> Severity:
        if m.score >= m.threshold:
            return Severity.OK
        if m.score >= m.threshold - self.warning_band:
            return Severity.WARNING
        return Severity.FAIL

    def manage(
        self, measurement: Measurement, *, component: str = "",
    ) -> RemediationSignal | None:
        """Return a remediation signal, or ``None`` when the meter is satisfied."""
        sev = self.severity(measurement)
        if sev is Severity.OK:
            return None
        deficit = max(0.0, measurement.threshold - measurement.score)
        missing = self._missing(measurement)
        reason = self._reason(measurement, sev, missing)
        return RemediationSignal(
            meter=measurement.meter,
            severity=sev,
            score=measurement.score,
            threshold=measurement.threshold,
            deficit=deficit,
            reason=reason,
            missing=missing,
            suggestion=self._suggest(measurement, missing),
            component=component,
        )

    # ── hooks subclasses may override ─────────────────────────────────────────
    def _subject(self) -> str:
        return self.meter_name.replace("_", " ")

    def _missing(self, m: Measurement) -> list[str]:
        """Failing components + any explicit missing-token evidence."""
        failing = [k for k, v in m.components.items() if v < 1.0
                   and m.weights.get(k, 0.0) > 0.0]
        ev = m.evidence or {}
        for key in ("missing", "pending"):
            val = ev.get(key)
            if isinstance(val, list):
                failing.extend(str(x) for x in val[:8])
            elif isinstance(val, dict):
                for items in val.values():
                    if isinstance(items, list):
                        failing.extend(str(x) for x in items[:8])
        # De-dupe, preserve order.
        seen: set[str] = set()
        out: list[str] = []
        for x in failing:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out[:12]

    def _reason(self, m: Measurement, sev: Severity, missing: list[str]) -> str:
        bits: list[str] = []
        if m.issues:
            bits.append("; ".join(m.issues[:3]))
        elif missing:
            bits.append("weak areas: " + ", ".join(missing[:6]))
        detail = bits[0] if bits else "below acceptance threshold"
        return (f"{self._subject()} is at {m.score:.0%} (needs "
                f"{m.threshold:.0%}). {detail}.")

    def _suggest(self, m: Measurement, missing: list[str]) -> str:
        if missing:
            return (f"Consider addressing {self._subject()}: "
                    + ", ".join(missing[:6]) + ".")
        return f"Consider improving {self._subject()}."
