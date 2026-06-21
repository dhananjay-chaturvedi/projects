"""Guardrail: source functions may not exceed the input hard limit."""

from __future__ import annotations

import ast
from pathlib import Path


MAX_PARAMS = 10
SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "built_apps",
    "node_modules",
}


def _param_count(node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    args = node.args
    positional = [a.arg for a in args.posonlyargs] + [a.arg for a in args.args]
    if positional and positional[0] in {"self", "cls"}:
        positional = positional[1:]
    return (
        len(positional)
        + len(args.kwonlyargs)
        + (1 if args.vararg else 0)
        + (1 if args.kwarg else 0)
    )


def test_no_source_function_exceeds_param_hard_limit():
    root = Path(__file__).resolve().parents[1]
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            count = _param_count(node)
            if count > MAX_PARAMS:
                rel = path.relative_to(root)
                offenders.append(f"{rel}:{node.lineno} {node.name} ({count})")
    assert offenders == []
