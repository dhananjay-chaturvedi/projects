"""code_adaptability_meter — how maintainable/adaptable is the generated code?

Lower cyclomatic complexity, smaller functions, low duplication, reasonable
coupling (imports) and good docstring coverage all make code easier to change
later. Each signal is mapped to ``[0, 1]`` and blended.
"""

from __future__ import annotations

from ai_assistant.meters import codemetrics as cm
from ai_assistant.meters.base import Meter, Measurement, clamp01


def _inverse_threshold(value: float, good: float, bad: float) -> float:
    """1.0 at/below *good*, 0.0 at/above *bad*, linear in between."""
    if value <= good:
        return 1.0
    if value >= bad:
        return 0.0
    return clamp01(1.0 - (value - good) / (bad - good))


class CodeAdaptabilityMeter(Meter):
    name = "code_adaptability_meter"
    default_threshold = 0.65

    def measure(self, code: str, *, language: str = "python") -> Measurement:
        if language != "python":
            return self._result(
                {"adaptability": 0.5}, {"adaptability": 1.0},
                evidence={"note": "non-python: heuristic neutral score"},
            )
        facts = cm.analyze_python(code)
        if not facts.parse_ok:
            return self._result(
                {"adaptability": 0.0}, {"adaptability": 1.0},
                issues=["unparseable code cannot be assessed for adaptability"],
            )

        components = {
            "complexity": _inverse_threshold(facts.avg_complexity, good=4.0, bad=15.0),
            "function_size": _inverse_threshold(facts.avg_func_len, good=20.0, bad=80.0),
            "duplication": _inverse_threshold(facts.duplication, good=0.05, bad=0.4),
            "coupling": _inverse_threshold(
                facts.n_imports / max(1, facts.n_functions + facts.n_classes),
                good=1.5, bad=6.0,
            ),
            "docstrings": clamp01(facts.docstring_coverage),
        }
        weights = {
            "complexity": 2.0, "function_size": 1.5, "duplication": 1.5,
            "coupling": 1.0, "docstrings": 1.0,
        }
        issues: list[str] = []
        if facts.max_complexity > 15:
            issues.append(f"a function has high cyclomatic complexity ({facts.max_complexity})")
        if facts.max_func_len > 80:
            issues.append(f"a function is very long ({facts.max_func_len} lines)")
        if facts.duplication > 0.2:
            issues.append(f"high duplication ({facts.duplication:.0%} of lines)")

        return self._result(
            components, weights, issues=issues,
            evidence={
                "avg_complexity": round(facts.avg_complexity, 2),
                "max_complexity": facts.max_complexity,
                "avg_func_len": round(facts.avg_func_len, 2),
                "duplication": round(facts.duplication, 3),
                "docstring_coverage": round(facts.docstring_coverage, 3),
            },
        )
