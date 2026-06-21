"""code_accuracy_meter — is the generated code correct?

Deterministic signals: it parses (``ast``), it compiles, optional lint issue
count, and an optional test pass ratio from a real test run. This measures the
correctness of what our code generators (app builder / AI builder) produce.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.meters import codemetrics as cm
from ai_assistant.meters.base import Meter, Measurement, clamp01, diminishing


class CodeAccuracyMeter(Meter):
    name = "code_accuracy_meter"
    default_threshold = 0.8

    def measure(
        self,
        code: str,
        *,
        language: str = "python",
        lint_issues: int | None = None,
        tests: dict[str, Any] | None = None,
    ) -> Measurement:
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        issues: list[str] = []
        evidence: dict[str, Any] = {"language": language}

        if language == "python":
            facts = cm.analyze_python(code)
            components["parses"] = 1.0 if facts.parse_ok else 0.0
            weights["parses"] = 3.0
            components["compiles"] = 1.0 if facts.compile_ok else 0.0
            weights["compiles"] = 2.0
            if not facts.parse_ok:
                issues.append(f"does not parse: {facts.syntax_error}")
            elif not facts.compile_ok:
                issues.append(f"does not compile: {facts.syntax_error}")
            evidence["loc"] = facts.loc
        else:
            # Non-Python: we cannot statically verify; rely on lint/tests only.
            evidence["note"] = "non-python: parse/compile not checked"

        if lint_issues is not None:
            components["lint_clean"] = diminishing(lint_issues, half_life=3.0)
            weights["lint_clean"] = 1.0
            evidence["lint_issues"] = lint_issues
            if lint_issues:
                issues.append(f"{lint_issues} lint issue(s)")

        if tests is not None:
            total = int(tests.get("total", 0) or 0)
            passed = int(tests.get("passed", 0) or 0)
            ratio = clamp01(passed / total) if total else 0.0
            components["tests_pass"] = ratio
            weights["tests_pass"] = 3.0
            evidence["tests"] = {"passed": passed, "total": total}
            if total and passed < total:
                issues.append(f"{total - passed}/{total} tests failing")

        return self._result(components, weights, evidence=evidence, issues=issues)
