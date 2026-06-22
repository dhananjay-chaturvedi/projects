"""solid_principles_meter — SOLID (Object-Oriented Design Score).

Heuristic, deterministic checks for the SOLID principles over the produced
Python classes:

* **SRP** (Single Responsibility) — classes stay focused: not too many methods,
  not oversized (no god-objects).
* **OCP** (Open/Closed) — behavior is extended via inheritance/abstractions
  rather than sprawling ``isinstance`` / ``type(x) ==`` dispatch chains.
* **DIP** (Dependency Inversion) — abstractions exist (``ABC`` / ``Protocol`` /
  ``@abstractmethod``) and/or collaborators are injected via the constructor
  rather than hard-constructed inside methods.
* **ISP** (Interface Segregation) — abstract base classes / protocols are small
  and focused (few abstract methods), not fat interfaces.

Applicable only when the app is object-oriented enough to judge (``>= 2``
classes). Otherwise it reports ``applicable=False`` with a neutral score so
script-style apps are not penalized. Never calls a model.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, diminishing, weighted_score

_MIN_CLASSES = 2
_MAX_METHODS = 12
_MAX_CLASS_LINES = 200


def _is_primitive_default(node: ast.arg, default: ast.expr | None) -> bool:
    return default is not None and isinstance(
        default, (ast.Constant,))


class SolidPrinciplesMeter(Meter):
    """Score adherence to SOLID OO-design principles (heuristic, AST-based)."""

    name = "solid_principles_meter"
    default_threshold = 0.6

    def measure(
        self,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        py = {p: c for p, c in files.items() if p.endswith(".py") and c}

        classes: list[ast.ClassDef] = []
        abstractions = 0
        abstract_method_counts: list[int] = []
        injected_classes = 0
        isinstance_dispatch = 0

        for content in py.values():
            try:
                tree = ast.parse(content)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    classes.append(node)
                    bases = {
                        (b.id if isinstance(b, ast.Name) else
                         b.attr if isinstance(b, ast.Attribute) else "")
                        for b in node.bases}
                    is_abstract = bool(bases & {"ABC", "ABCMeta", "Protocol"})
                    abstract_methods = 0
                    methods = [n for n in node.body
                               if isinstance(n, (ast.FunctionDef,
                                                 ast.AsyncFunctionDef))]
                    for m in methods:
                        deco = {
                            (d.id if isinstance(d, ast.Name) else
                             d.attr if isinstance(d, ast.Attribute) else "")
                            for d in m.decorator_list}
                        if "abstractmethod" in deco:
                            abstract_methods += 1
                        if m.name == "__init__":
                            # Dependency injection: collaborators passed in
                            # (non-self params that are not primitive defaults).
                            args = m.args.args[1:]  # skip self
                            defaults = ([None] * (len(args) - len(m.args.defaults))
                                        + list(m.args.defaults))
                            injected = any(
                                not _is_primitive_default(a, d)
                                for a, d in zip(args, defaults))
                            if args and injected:
                                injected_classes += 1
                    if is_abstract or abstract_methods:
                        abstractions += 1
                        abstract_method_counts.append(
                            abstract_methods or len(methods))
                if isinstance(node, ast.Call):
                    fn = node.func
                    if isinstance(fn, ast.Name) and fn.id in ("isinstance", "type"):
                        isinstance_dispatch += 1

        n_classes = len(classes)
        if n_classes < _MIN_CLASSES:
            return Measurement(
                meter=self.name, score=1.0, components={}, weights={},
                evidence={"applicable": False, "classes": n_classes},
                issues=[], threshold=thr)

        # SRP: fraction of classes that are focused (size + method count).
        focused = 0
        oversized: list[str] = []
        for cls in classes:
            methods = sum(1 for n in cls.body
                          if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
            try:
                length = (cls.end_lineno or cls.lineno) - cls.lineno
            except AttributeError:
                length = 0
            if methods <= _MAX_METHODS and length <= _MAX_CLASS_LINES:
                focused += 1
            else:
                oversized.append(cls.name)
        srp = focused / n_classes if n_classes else 1.0

        has_abstractions = 1.0 if abstractions else 0.0
        dip_injection = 1.0 if injected_classes else 0.0
        # OCP: penalize heavy type-based dispatch (a few is fine).
        ocp = diminishing(max(0, isinstance_dispatch - 2), half_life=3.0)
        # ISP: abstract interfaces should be small (<= 6 abstract methods).
        if abstract_method_counts:
            small = sum(1 for c in abstract_method_counts if c <= 6)
            isp = small / len(abstract_method_counts)
        else:
            isp = 1.0  # no interfaces to over-fatten

        components = {
            "srp_focused_classes": srp,
            "dip_abstractions": has_abstractions,
            "dip_injection": dip_injection,
            "ocp_extensible": ocp,
            "isp_small_interfaces": isp,
        }
        weights = {
            "srp_focused_classes": 3.0, "dip_abstractions": 2.0,
            "dip_injection": 2.0, "ocp_extensible": 1.5,
            "isp_small_interfaces": 1.5,
        }
        score = weighted_score(components, weights)

        issues: list[str] = []
        if oversized:
            issues.append("god-object risk (SRP): " + ", ".join(oversized[:4]))
        if not abstractions:
            issues.append("no abstractions (ABC/Protocol/@abstractmethod) — "
                          "DIP/OCP weak")
        if not injected_classes:
            issues.append("no constructor dependency injection (DIP)")
        if isinstance_dispatch > 4:
            issues.append(f"{isinstance_dispatch} isinstance/type checks — "
                          "type dispatch hurts OCP")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"applicable": True, "classes": n_classes,
                      "abstractions": abstractions,
                      "injected_classes": injected_classes,
                      "isinstance_dispatch": isinstance_dispatch,
                      "oversized": oversized},
            issues=issues, threshold=thr)
