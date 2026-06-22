"""functional_correctness_meter — the primary build-quality signal.

Research on application-level code generation is unanimous that *functional
correctness* — does the app actually work, as proven by executable tests — is
the dominant bottleneck and the metric that matters most. This meter turns a
pytest run outcome into a normalized score = system-test pass rate, mirroring
the "system test pass rate" used by RAL-Bench / Vibe Code Bench.

It parses the pytest summary line (``"5 passed, 1 failed in 0.3s"``) into
passed / failed / error counts and scores ``passed / total``. Errors are
treated as failures. A run with no tests scores low (you cannot prove the app
works) and is flagged.

Deterministic: arithmetic over the test outcome dict, never a model call.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from ai_assistant.meters.base import Meter, Measurement

_COUNT_RE = {
    "passed": re.compile(r"(\d+)\s+passed"),
    "failed": re.compile(r"(\d+)\s+failed"),
    "error": re.compile(r"(\d+)\s+errors?"),
    "skipped": re.compile(r"(\d+)\s+skipped"),
}


def _count(summary: str, key: str) -> int:
    m = _COUNT_RE[key].search(summary or "")
    return int(m.group(1)) if m else 0


class FunctionalCorrectnessMeter(Meter):
    """Score = system-test pass rate from a pytest outcome."""

    name = "functional_correctness_meter"
    default_threshold = 0.9

    def measure(
        self,
        test_outcome: Mapping[str, Any] | None,
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        outcome = dict(test_outcome or {})
        summary = str(outcome.get("summary", ""))

        passed = _count(summary, "passed")
        failed = _count(summary, "failed")
        errored = _count(summary, "error")
        skipped = _count(summary, "skipped")
        executed = passed + failed + errored

        issues: list[str] = []
        if executed == 0:
            # No parseable counts — fall back to pytest's boolean when present
            # (some pytest versions/plugins omit the summary line).
            if outcome.get("passed") is True:
                score = pass_rate = 1.0
                evidence_note = summary or "pytest succeeded (no summary line)"
            else:
                no_dir = "no tests" in summary.lower() or not summary
                score = pass_rate = 0.0
                issues.append("no tests executed — functional correctness unproven")
                evidence_note = "no_tests_dir" if no_dir else summary
        else:
            pass_rate = passed / executed
            score = pass_rate
            evidence_note = summary
            if failed:
                issues.append(f"{failed} failing test(s)")
            if errored:
                issues.append(f"{errored} erroring test(s)")

        return Measurement(
            meter=self.name, score=score,
            components={"pass_rate": pass_rate},
            weights={"pass_rate": 1.0},
            evidence={"passed": passed, "failed": failed, "errors": errored,
                      "skipped": skipped, "executed": executed,
                      "summary": evidence_note,
                      "returncode": outcome.get("returncode")},
            issues=issues, threshold=thr,
        )
