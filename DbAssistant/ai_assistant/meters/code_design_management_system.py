"""code_design_management_system — design governance for generated code.

Enforces the design rules the AiAppEngine must always honor: sane naming, no
bare excepts, bounded class responsibilities, controlled coupling, plus the
adaptability blend. Returns a design score and an explicit list of violations
so the engine can require fixes before code is accepted.
"""

from __future__ import annotations


from ai_assistant.meters import codemetrics as cm
from ai_assistant.meters.base import Meter, Measurement, diminishing
from ai_assistant.meters.code_adaptability_meter import CodeAdaptabilityMeter


class CodeDesignManagementSystem(Meter):
    name = "code_design_management_system"
    default_threshold = 0.7

    def __init__(self) -> None:
        self._adaptability = CodeAdaptabilityMeter()

    def measure(
        self,
        code: str,
        *,
        language: str = "python",
        max_class_methods: int = 15,
    ) -> Measurement:
        if language != "python":
            return self._result(
                {"design": 0.5}, {"design": 1.0},
                evidence={"note": "non-python: heuristic neutral score"},
            )
        facts = cm.analyze_python(code)
        if not facts.parse_ok:
            return self._result(
                {"design": 0.0}, {"design": 1.0},
                issues=[f"unparseable: {facts.syntax_error}"],
            )

        violations: list[str] = []
        violations.extend(facts.bad_names)
        for a in facts.antipatterns:
            if "bare except" in a or "None with ==" in a:
                violations.append(a)

        # SRP proxy: classes with too many methods.
        import ast

        tree = ast.parse(code)
        big_classes = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                methods = [n for n in node.body
                           if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
                if len(methods) > max_class_methods:
                    big_classes.append(node.name)
                    violations.append(
                        f"class '{node.name}' has {len(methods)} methods (>{max_class_methods}); "
                        "consider splitting responsibilities"
                    )

        components = {
            "naming": diminishing(len(facts.bad_names), half_life=2.0),
            "rule_compliance": diminishing(
                len([v for v in violations if v not in facts.bad_names]), half_life=2.0
            ),
            "responsibility": diminishing(len(big_classes), half_life=1.0),
            "adaptability": self._adaptability.measure(code).score,
        }
        weights = {"naming": 1.0, "rule_compliance": 2.0,
                   "responsibility": 1.5, "adaptability": 1.5}
        return self._result(
            components, weights, issues=violations,
            evidence={"violations": violations, "big_classes": big_classes},
        )
