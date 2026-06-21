"""code_hygiene_meter — CHS (Code Hygiene Score).

Modeled on the Code Hygiene metric from application-level build benchmarks
(SWE-WebDevBench G2): maintainability factors — naming, dead code, duplication,
separation of concerns — plus the robustness checks that map to ISO/IEC 25010
maintainability. Deterministic checks for:

* parseable Python (AST compiles),
* no bare ``except:`` (robustness),
* docstrings / comments present (readability),
* no debug ``print``/leftover ``TODO``/``FIXME`` swamping the code,
* limited duplication (no large repeated blocks),
* reasonable function size (no giant functions).

Deterministic: AST + regex over produced Python files, never a model call.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Mapping

from ai_assistant.meters.base import Meter, Measurement, diminishing, weighted_score

_BARE_EXCEPT_RE = re.compile(r"except\s*:")
_TODO_RE = re.compile(r"\b(TODO|FIXME|XXX|HACK)\b")
_PRINT_RE = re.compile(r"^\s*print\(", re.MULTILINE)


class CodeHygieneMeter(Meter):
    """Score maintainability/readability hygiene of the generated code."""

    name = "code_hygiene_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        py = {p: c for p, c in files.items() if p.endswith(".py") and c}
        blob = "\n".join(py.values())

        issues: list[str] = []

        # Parseability — broken files crater hygiene.
        parse_fail = 0
        funcs: list[ast.FunctionDef] = []
        documented = 0
        for path, content in py.items():
            try:
                tree = ast.parse(content)
            except SyntaxError:
                parse_fail += 1
                issues.append(f"syntax error in {path}")
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    funcs.append(node)
                    if ast.get_docstring(node):
                        documented += 1
        parseable = 1.0 if not py else 1.0 - (parse_fail / len(py))

        bare_excepts = len(_BARE_EXCEPT_RE.findall(blob))
        todos = len(_TODO_RE.findall(blob))
        prints = len(_PRINT_RE.findall(blob))

        # Function size: penalize very long functions (>60 logical lines).
        big_funcs = 0
        for fn in funcs:
            try:
                length = (fn.end_lineno or fn.lineno) - fn.lineno
            except AttributeError:
                length = 0
            if length > 60:
                big_funcs += 1

        # Duplication: fraction of duplicate non-trivial lines.
        lines = [ln.strip() for ln in blob.splitlines()
                 if len(ln.strip()) > 20 and not ln.strip().startswith("#")]
        dup_ratio = 0.0
        if lines:
            uniq = len(set(lines))
            dup_ratio = 1.0 - (uniq / len(lines))

        doc_ratio = (documented / len(funcs)) if funcs else 1.0

        components = {
            "parseable": parseable,
            "no_bare_except": diminishing(bare_excepts, half_life=1.0),
            "documentation": doc_ratio,
            "low_debug_noise": diminishing(prints + todos, half_life=4.0),
            "function_size": diminishing(big_funcs, half_life=2.0),
            "low_duplication": 1.0 - min(1.0, dup_ratio * 2.0),
        }
        weights = {
            "parseable": 4.0, "no_bare_except": 2.0, "documentation": 1.5,
            "low_debug_noise": 1.0, "function_size": 1.0, "low_duplication": 1.5,
        }
        score = weighted_score(components, weights)

        if bare_excepts:
            issues.append(f"{bare_excepts} bare except clause(s)")
        if big_funcs:
            issues.append(f"{big_funcs} oversized function(s) (>60 lines)")
        if dup_ratio > 0.3:
            issues.append("high code duplication")
        if doc_ratio < 0.3 and funcs:
            issues.append("few functions are documented")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"parse_failures": parse_fail, "bare_excepts": bare_excepts,
                      "todos": todos, "prints": prints, "big_funcs": big_funcs,
                      "dup_ratio": round(dup_ratio, 4),
                      "doc_ratio": round(doc_ratio, 4)},
            issues=issues, threshold=thr,
        )
