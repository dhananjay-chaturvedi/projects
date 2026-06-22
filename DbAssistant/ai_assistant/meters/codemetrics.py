"""Deterministic Python source analysis used by the code-side meters.

Pure ``ast``/``compile`` based static analysis — no execution of the analysed
code and no model calls. Provides complexity, structure, naming, duplication
and anti-pattern signals that the code meters turn into normalized scores.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field


@dataclass
class CodeFacts:
    parse_ok: bool = False
    compile_ok: bool = False
    syntax_error: str = ""
    n_functions: int = 0
    n_classes: int = 0
    n_imports: int = 0
    loc: int = 0
    avg_complexity: float = 0.0
    max_complexity: int = 0
    avg_func_len: float = 0.0
    max_func_len: int = 0
    docstring_coverage: float = 1.0
    duplication: float = 0.0
    nested_loop_depth: int = 0
    bad_names: list[str] = field(default_factory=list)
    antipatterns: list[str] = field(default_factory=list)


_PASCAL = re.compile(r"^_?[A-Z][A-Za-z0-9]*$")
_SNAKE = re.compile(r"^_{0,2}[a-z][a-z0-9_]*$|^__[a-z][a-z0-9_]*__$")


def _complexity(node: ast.AST) -> int:
    """McCabe-style cyclomatic complexity for a function node."""
    score = 1
    for child in ast.walk(node):
        if isinstance(child, (ast.If, ast.For, ast.While, ast.With, ast.Try,
                              ast.ExceptHandler, ast.BoolOp, ast.IfExp,
                              ast.comprehension, ast.Assert)):
            score += 1
        elif isinstance(child, ast.BoolOp):
            score += len(child.values) - 1
    return score


def _loop_depth(node: ast.AST, current: int = 0) -> int:
    best = current
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.For, ast.While, ast.AsyncFor)):
            best = max(best, _loop_depth(child, current + 1))
        else:
            best = max(best, _loop_depth(child, current))
    return best


def _detect_antipatterns(tree: ast.AST) -> list[str]:
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.For, ast.While)):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    fn = inner.func
                    attr = getattr(fn, "attr", "") or getattr(fn, "id", "")
                    if attr in {"execute", "executemany", "query", "fetchone",
                                "fetchall", "get", "post"}:
                        found.append(f"db/io call '{attr}' inside a loop (possible N+1)")
                        break
        if isinstance(node, ast.ExceptHandler) and node.type is None:
            found.append("bare except (swallows all errors)")
        if isinstance(node, ast.Compare):
            for c in node.comparators:
                if isinstance(c, ast.Constant) and c.value is None:
                    if any(isinstance(o, (ast.Eq, ast.NotEq)) for o in node.ops):
                        found.append("comparison to None with ==/!= (use is/is not)")
    # de-dup preserve order
    seen: set[str] = set()
    out: list[str] = []
    for f in found:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def _duplication(source: str) -> float:
    """Fraction of non-trivial lines that are exact duplicates."""
    lines = [ln.strip() for ln in source.splitlines()
             if ln.strip() and not ln.strip().startswith("#")]
    if len(lines) < 2:
        return 0.0
    counts: dict[str, int] = {}
    for ln in lines:
        if len(ln) >= 12:  # ignore trivial lines like "return" / ")"
            counts[ln] = counts.get(ln, 0) + 1
    dup = sum(c - 1 for c in counts.values() if c > 1)
    return dup / len(lines)


def analyze_python(source: str) -> CodeFacts:
    facts = CodeFacts()
    facts.loc = len([ln for ln in (source or "").splitlines() if ln.strip()])
    if not source or not source.strip():
        return facts
    try:
        tree = ast.parse(source)
        facts.parse_ok = True
    except SyntaxError as exc:
        facts.syntax_error = f"{exc.msg} (line {exc.lineno})"
        return facts
    try:
        compile(tree, "<analyzed>", "exec")
        facts.compile_ok = True
    except Exception as exc:  # noqa: BLE001
        facts.syntax_error = facts.syntax_error or str(exc)

    funcs = [n for n in ast.walk(tree)
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    classes = [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    imports = [n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))]
    facts.n_functions = len(funcs)
    facts.n_classes = len(classes)
    facts.n_imports = len(imports)
    facts.nested_loop_depth = _loop_depth(tree)

    complexities = [_complexity(f) for f in funcs]
    if complexities:
        facts.avg_complexity = sum(complexities) / len(complexities)
        facts.max_complexity = max(complexities)

    lengths = []
    for f in funcs:
        if f.body:
            start = f.lineno
            end = max(getattr(n, "lineno", start) for n in ast.walk(f))
            lengths.append(max(1, end - start + 1))
    if lengths:
        facts.avg_func_len = sum(lengths) / len(lengths)
        facts.max_func_len = max(lengths)

    documentable = funcs + classes
    if documentable:
        documented = sum(1 for n in documentable if ast.get_docstring(n))
        facts.docstring_coverage = documented / len(documentable)

    for f in funcs:
        if not _SNAKE.match(f.name) and not f.name.startswith("__"):
            facts.bad_names.append(f"function '{f.name}' is not snake_case")
    for c in classes:
        if not _PASCAL.match(c.name):
            facts.bad_names.append(f"class '{c.name}' is not PascalCase")

    facts.duplication = _duplication(source)
    facts.antipatterns = _detect_antipatterns(tree)
    return facts
