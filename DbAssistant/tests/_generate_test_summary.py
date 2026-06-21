"""
Generate tests/TEST_SUMMARY.md with concrete inputs/expected per test.

For each test function we statically extract:
  * Inputs:
      - parametrize cases  (concrete value tuples / ids)
      - simple assignments  (e.g.  `x = 5`, `payload = {"k": 1}`)
      - mock return values / side effects
      - monkeypatch.setattr / setenv calls
      - patch.object / patch decorators with `return_value=`
  * Expected:
      - every `assert ...` line, trimmed to source text
  * Actual outcome from JUnit XML:
      - PASS  -> "Matches expected"
      - FAIL  -> failure message from XML
      - SKIP  -> skip reason from XML
      - ERROR -> error message from XML
"""

from __future__ import annotations

import ast
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
TESTS_DIR = ROOT / "tests"
JUNIT_XML = TESTS_DIR / "_junit_results.xml"
SUMMARY_MD = TESTS_DIR / "TEST_SUMMARY.md"


# ── data ────────────────────────────────────────────────────────────────

@dataclass
class TestInfo:
    file: str
    classname: Optional[str]
    name: str
    docstring: str = ""
    fixtures: List[str] = field(default_factory=list)
    markers: List[str] = field(default_factory=list)
    parametrize_cases: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)
    expected: List[str] = field(default_factory=list)
    outcome: str = "unknown"
    duration_s: float = 0.0
    skip_reason: str = ""
    failure_message: str = ""


# ── helpers ─────────────────────────────────────────────────────────────

def _src(node: ast.AST, source: str) -> str:
    """Return the original source slice for a node (single-line trimmed)."""
    try:
        text = ast.get_source_segment(source, node) or ast.unparse(node)
    except Exception:
        text = ast.unparse(node)
    return " ".join(text.split())


def _short(s: str, limit: int = 110) -> str:
    s = " ".join((s or "").split())
    if len(s) > limit:
        s = s[: limit - 1] + "…"
    return s


def _scenario_from_name(name: str) -> str:
    s = name[len("test_") :] if name.startswith("test_") else name
    return s.replace("_", " ").strip().capitalize()


# ── AST extraction ──────────────────────────────────────────────────────

def _extract_decorator_info(dec: ast.expr, source: str) -> Tuple[Optional[str], List[str]]:
    """Return (marker_name, list-of-parametrize-cases)."""
    if isinstance(dec, ast.Attribute) and isinstance(dec.value, ast.Attribute):
        if (
            isinstance(dec.value.value, ast.Name)
            and dec.value.value.id == "pytest"
            and dec.value.attr == "mark"
        ):
            return f"@{dec.attr}", []
    if isinstance(dec, ast.Call):
        func = dec.func
        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Attribute)
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "pytest"
            and func.value.attr == "mark"
        ):
            if func.attr == "parametrize" and len(dec.args) >= 2:
                # Build readable parametrize case strings.
                cases: List[str] = []
                arg_names = dec.args[0]
                names_text = (
                    arg_names.value
                    if isinstance(arg_names, ast.Constant)
                    else "params"
                )
                values_node = dec.args[1]
                if isinstance(values_node, (ast.List, ast.Tuple)):
                    for elt in values_node.elts:
                        cases.append(f"{names_text} = {_src(elt, source)}")
                return None, cases
            return f"@{func.attr}", []
    return None, []


def _extract_inputs_and_expected(
    func: ast.FunctionDef, source: str
) -> Tuple[List[str], List[str]]:
    """Walk a test body and capture concrete inputs + assertion expressions."""
    inputs: List[str] = []
    expected: List[str] = []

    def _is_mock_returnval_call(node: ast.Call) -> bool:
        # MagicMock(return_value=...) / Mock(return_value=...)
        if isinstance(node.func, ast.Name) and node.func.id in {
            "MagicMock",
            "Mock",
            "AsyncMock",
        }:
            return any(
                isinstance(kw, ast.keyword)
                and kw.arg in {"return_value", "side_effect"}
                for kw in node.keywords
            )
        return False

    for stmt in ast.walk(func):
        # Assignments: x = literal / call / dict / list / mock-builder
        if isinstance(stmt, ast.Assign):
            for tgt in stmt.targets:
                if isinstance(tgt, ast.Name):
                    rhs = _src(stmt.value, source)
                    inputs.append(f"{tgt.id} = {_short(rhs, 80)}")
                elif isinstance(tgt, ast.Attribute):
                    # mock.foo = ... e.g. db.execute_query = MagicMock(return_value=…)
                    rhs = _src(stmt.value, source)
                    inputs.append(f"{_src(tgt, source)} = {_short(rhs, 90)}")

        # monkeypatch.setattr(...), monkeypatch.setenv(...), patch.object(...).
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            call = stmt.value
            if isinstance(call.func, ast.Attribute):
                attr_chain = []
                node = call.func
                while isinstance(node, ast.Attribute):
                    attr_chain.append(node.attr)
                    node = node.value
                if isinstance(node, ast.Name):
                    attr_chain.append(node.id)
                attr_chain.reverse()
                dotted = ".".join(attr_chain)
                if dotted.startswith("monkeypatch.") or dotted.startswith(
                    "patch."
                ):
                    inputs.append(f"{_short(_src(call, source), 100)}")

        # Assertions are the expected outputs.
        if isinstance(stmt, ast.Assert):
            expected.append(_short(_src(stmt, source), 130))

    # De-duplicate while preserving order; keep small.
    seen = set()
    inputs_dedup: List[str] = []
    for entry in inputs:
        if entry in seen:
            continue
        seen.add(entry)
        inputs_dedup.append(entry)
    seen.clear()
    expected_dedup: List[str] = []
    for entry in expected:
        if entry in seen:
            continue
        seen.add(entry)
        expected_dedup.append(entry)
    return inputs_dedup, expected_dedup


def _ast_extract(file_path: Path) -> Dict[Tuple[Optional[str], str], TestInfo]:
    result: Dict[Tuple[Optional[str], str], TestInfo] = {}
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (SyntaxError, OSError):
        return result

    def _handle_func(node: ast.FunctionDef, classname: Optional[str]) -> None:
        if not node.name.startswith("test_"):
            return
        markers: List[str] = []
        parametrize_cases: List[str] = []
        for dec in node.decorator_list:
            m, p = _extract_decorator_info(dec, source)
            if m:
                markers.append(m)
            parametrize_cases.extend(p)

        fixtures = [
            arg.arg for arg in node.args.args if arg.arg not in ("self", "cls")
        ]

        inputs, expected = _extract_inputs_and_expected(node, source)

        info = TestInfo(
            file=str(file_path.relative_to(ROOT)),
            classname=classname,
            name=node.name,
            docstring=_short(ast.get_docstring(node) or "", 160),
            fixtures=fixtures,
            markers=markers,
            parametrize_cases=parametrize_cases,
            inputs=inputs,
            expected=expected,
        )
        result[(classname, node.name)] = info

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, ast.FunctionDef):
                    _handle_func(child, node.name)
        elif isinstance(node, ast.FunctionDef):
            _handle_func(node, None)
    return result


# ── pytest runner + XML parser ──────────────────────────────────────────

def _run_pytest() -> int:
    JUNIT_XML.unlink(missing_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "tests/",
        "--maxfail=0",
        "-p",
        "no:cacheprovider",
        f"--junitxml={JUNIT_XML}",
        "-q",
        "--tb=no",
    ]
    return subprocess.call(cmd, cwd=ROOT)


def _parse_junit() -> List[TestInfo]:
    if not JUNIT_XML.is_file():
        raise FileNotFoundError(JUNIT_XML)

    ast_cache: Dict[str, Dict[Tuple[Optional[str], str], TestInfo]] = {}

    def _get_ast(rel_file: str) -> Dict[Tuple[Optional[str], str], TestInfo]:
        if rel_file not in ast_cache:
            p = ROOT / rel_file
            ast_cache[rel_file] = _ast_extract(p) if p.is_file() else {}
        return ast_cache[rel_file]

    root = ET.parse(JUNIT_XML).getroot()
    cases: List[TestInfo] = []
    for tc in root.iter("testcase"):
        classname = tc.get("classname", "")
        name = tc.get("name", "")
        time_s = float(tc.get("time", "0") or 0.0)

        module_skip = False
        if classname == "" and "." in name:
            parts = name.split(".")
            file_parts = parts[:2]
            cls = None
            module_skip = True
        else:
            parts = classname.split(".")
            file_parts = parts[:2]
            cls = parts[2] if len(parts) > 2 else None

        rel_file = "/".join(file_parts) + ".py"
        if module_skip:
            name = "<module-skip>"

        outcome = "passed"
        skip_reason = ""
        failure_message = ""
        for child in tc:
            if child.tag == "failure":
                outcome = "failed"
                failure_message = _short(child.get("message", ""), 200)
            elif child.tag == "error":
                outcome = "error"
                failure_message = _short(child.get("message", ""), 200)
            elif child.tag == "skipped":
                outcome = "skipped"
                skip_reason = _short(child.get("message", ""), 160)

        # Strip parametrize suffix to match AST entry.
        base_name = name
        param_id = None
        if "[" in name and name.endswith("]"):
            base_name = name.split("[", 1)[0]
            param_id = name[name.index("[") + 1 : -1]

        ast_info = _get_ast(rel_file).get((cls, base_name)) or _get_ast(
            rel_file
        ).get((None, base_name))

        info = TestInfo(
            file=rel_file,
            classname=cls,
            name=name,
            docstring=ast_info.docstring if ast_info else "",
            fixtures=list(ast_info.fixtures) if ast_info else [],
            markers=list(ast_info.markers) if ast_info else [],
            parametrize_cases=list(ast_info.parametrize_cases)
            if ast_info
            else [],
            inputs=list(ast_info.inputs) if ast_info else [],
            expected=list(ast_info.expected) if ast_info else [],
            outcome=outcome,
            duration_s=time_s,
            skip_reason=skip_reason,
            failure_message=failure_message,
        )
        # Attach the specific parametrize id (if any) at the top of inputs.
        if param_id:
            info.parametrize_cases = [
                c
                for c in info.parametrize_cases
                if param_id in c or c.endswith(f"= {param_id}")
            ] or [f"case: {param_id}"]
        cases.append(info)
    return cases


# ── markdown rendering ──────────────────────────────────────────────────

def _scenario_text(info: TestInfo) -> str:
    if info.name == "<module-skip>":
        return f"Module-level collection skip in `{info.file}`"
    if info.docstring:
        return info.docstring
    return _scenario_from_name(info.name)


def _inputs_cell(info: TestInfo) -> str:
    parts: List[str] = []
    if info.parametrize_cases:
        parts.extend(f"`{c}`" for c in info.parametrize_cases[:3])
    if info.fixtures:
        parts.append("fixtures: " + ", ".join(f"`{f}`" for f in info.fixtures))
    for inp in info.inputs[:6]:
        parts.append(f"`{inp}`")
    if len(info.inputs) > 6:
        parts.append(f"…(+{len(info.inputs) - 6} more)")
    return "<br>".join(parts) if parts else "—"


def _expected_cell(info: TestInfo) -> str:
    if not info.expected:
        return "—"
    items = [f"`{e}`" for e in info.expected[:6]]
    if len(info.expected) > 6:
        items.append(f"…(+{len(info.expected) - 6} more)")
    return "<br>".join(items)


def _actual_cell(info: TestInfo) -> str:
    if info.outcome == "passed":
        return "Matches expected"
    if info.outcome == "skipped":
        return f"_skip_ — {info.skip_reason or 'environment unavailable'}"
    if info.outcome in ("failed", "error"):
        return f"_{info.outcome}_ — {info.failure_message[:180]}"
    return info.outcome


def _outcome_badge(outcome: str) -> str:
    return {
        "passed": "PASS",
        "failed": "FAIL",
        "error": "ERROR",
        "skipped": "SKIP",
    }.get(outcome, outcome.upper())


def _markers_text(info: TestInfo) -> str:
    return ", ".join(info.markers)


def _write_md(cases: List[TestInfo]) -> None:
    counts = {"passed": 0, "failed": 0, "skipped": 0, "error": 0}
    total_time = 0.0
    for c in cases:
        counts[c.outcome] = counts.get(c.outcome, 0) + 1
        total_time += c.duration_s

    lines: List[str] = []
    lines.append("# Test Suite Summary — DbManagementTool")
    lines.append("")
    lines.append(
        f"**Generated:** `python tests/_generate_test_summary.py`  "
        f"**Total runtime:** {total_time:.2f}s  **Cases:** {len(cases)}"
    )
    lines.append("")
    lines.append("Each row shows the concrete **Inputs** the test sets up "
                 "(variable assignments, mock return values, parametrize "
                 "cases, fixtures), the **Expected** outcome (every `assert` "
                 "statement, verbatim), and the **Actual** result from the "
                 "live pytest run.")
    lines.append("")

    lines.append("## Totals")
    lines.append("")
    lines.append("| Status | Count |")
    lines.append("|--------|------:|")
    for k in ("passed", "failed", "error", "skipped"):
        lines.append(f"| {k.title()} | {counts.get(k, 0)} |")
    lines.append(f"| **Total** | **{len(cases)}** |")
    lines.append("")

    lines.append("## All tests")
    lines.append("")
    lines.append(
        "| # | Test ID | Scenario | Inputs (setup / fixtures / mocks) | "
        "Expected (assertions) | Actual | Markers | Time (s) |"
    )
    lines.append("|---:|---|---|---|---|---|---|---:|")
    for idx, c in enumerate(cases, 1):
        test_id = (
            f"{c.file}::"
            f"{c.classname + '::' if c.classname else ''}{c.name}"
        )
        lines.append(
            "| {n} | `{tid}` | {scenario} | {inputs} | {expected} | "
            "{actual} | {markers} | {time:.3f} |".format(
                n=idx,
                tid=test_id,
                scenario=_scenario_text(c),
                inputs=_inputs_cell(c),
                expected=_expected_cell(c),
                actual=_actual_cell(c),
                markers=_markers_text(c),
                time=c.duration_s,
            )
        )
    lines.append("")

    lines.append("## Per-module breakdown")
    lines.append("")
    by_file: Dict[str, List[TestInfo]] = {}
    for c in cases:
        by_file.setdefault(c.file, []).append(c)
    for fname in sorted(by_file):
        sub = by_file[fname]
        sub_counts = {"passed": 0, "failed": 0, "skipped": 0, "error": 0}
        for c in sub:
            sub_counts[c.outcome] = sub_counts.get(c.outcome, 0) + 1
        lines.append(
            f"### `{fname}` — P:{sub_counts.get('passed', 0)} "
            f"F:{sub_counts.get('failed', 0)} "
            f"E:{sub_counts.get('error', 0)} "
            f"S:{sub_counts.get('skipped', 0)}"
        )
        lines.append("")
        lines.append("| Test | Scenario | Inputs | Expected | Actual |")
        lines.append("|---|---|---|---|---|")
        for c in sub:
            full = (c.classname + "::" if c.classname else "") + c.name
            lines.append(
                "| `{tid}` | {scenario} | {inputs} | {expected} | {actual} |".format(
                    tid=full,
                    scenario=_scenario_text(c),
                    inputs=_inputs_cell(c),
                    expected=_expected_cell(c),
                    actual=_actual_cell(c),
                )
            )
        lines.append("")

    SUMMARY_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    rc = _run_pytest()
    cases = _parse_junit()
    _write_md(cases)
    print(f"Wrote {SUMMARY_MD.relative_to(ROOT)} with {len(cases)} cases")
    return rc


if __name__ == "__main__":
    sys.exit(main())
