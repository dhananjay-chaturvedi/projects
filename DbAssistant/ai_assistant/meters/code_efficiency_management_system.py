"""code_efficiency_management_system — efficiency of generated code.

Detects efficiency anti-patterns statically (deeply nested loops, DB/IO calls
inside loops → N+1, repeated work) and optionally folds in real runtime/memory
stats from a sandboxed execution. Produces an efficiency score plus concrete
recommendations the app builder can act on.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.meters import codemetrics as cm
from ai_assistant.meters.base import Meter, Measurement, clamp01, diminishing


class CodeEfficiencyManagementSystem(Meter):
    name = "code_efficiency_management_system"
    default_threshold = 0.7

    def measure(
        self,
        code: str,
        *,
        language: str = "python",
        runtime: dict[str, Any] | None = None,
    ) -> Measurement:
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        recommendations: list[str] = []
        evidence: dict[str, Any] = {}

        if language == "python":
            facts = cm.analyze_python(code)
            if not facts.parse_ok:
                return self._result(
                    {"efficiency": 0.0}, {"efficiency": 1.0},
                    issues=["unparseable code cannot be assessed"],
                )
            # Nested loops: depth 0/1 fine, 2 mild, 3+ poor.
            depth_penalty = max(0, facts.nested_loop_depth - 1)
            components["loop_nesting"] = diminishing(depth_penalty, half_life=1.5)
            weights["loop_nesting"] = 2.0
            if facts.nested_loop_depth >= 3:
                recommendations.append(
                    f"reduce loop nesting (depth {facts.nested_loop_depth}); "
                    "consider sets/dicts or vectorized queries"
                )

            io_anti = [a for a in facts.antipatterns if "N+1" in a or "loop" in a]
            components["io_patterns"] = diminishing(len(io_anti), half_life=1.0)
            weights["io_patterns"] = 2.0
            recommendations.extend(io_anti)

            evidence.update({
                "nested_loop_depth": facts.nested_loop_depth,
                "antipatterns": facts.antipatterns,
            })

        if runtime is not None:
            # Normalize against soft budgets if provided.
            ms = float(runtime.get("ms", 0.0) or 0.0)
            budget_ms = float(runtime.get("budget_ms", 0.0) or 0.0)
            if budget_ms > 0:
                components["runtime"] = clamp01(budget_ms / ms) if ms > 0 else 1.0
                weights["runtime"] = 2.0
                evidence["runtime_ms"] = ms
                if ms > budget_ms:
                    recommendations.append(f"runtime {ms:.0f}ms exceeds budget {budget_ms:.0f}ms")

        if not components:
            components["efficiency"] = 0.5
            weights["efficiency"] = 1.0
            evidence["note"] = "no static or runtime signals available"

        evidence["recommendations"] = recommendations
        return self._result(components, weights, evidence=evidence, issues=recommendations)
