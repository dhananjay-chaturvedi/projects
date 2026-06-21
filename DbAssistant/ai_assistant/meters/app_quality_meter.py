"""app_quality_meter — standard deterministic quality checks for built apps.

Runs a fixed battery of checks across produced files: runnable contract,
health endpoint, tests, sample data, code safety patterns, and soft docs.
Fully deterministic — no model calls.
"""

from __future__ import annotations

import re
from typing import Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

_BARE_EXCEPT_RE = re.compile(r"except\s*:")
_STRING_FMT_SQL_RE = re.compile(
    r'(?:execute|executemany)\s*\(\s*f["\']|\.format\s*\([^)]*\)\s*\)',
    re.IGNORECASE,
)


def _all_py(files: Mapping[str, str]) -> str:
    return "\n".join(
        c for p, c in files.items() if p.endswith(".py") and c
    )


class AppQualityMeter(Meter):
    """Standard quality battery for App Builder produced workspaces."""

    name = "app_quality_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        description: str = "",
    ) -> Measurement:
        checks: dict[str, bool] = {}
        issues: list[str] = []

        paths = set(files)
        app_py = files.get("src/app.py", "") or files.get("src/app/__init__.py", "")
        checks["runnable_contract"] = bool(app_py.strip())
        if not checks["runnable_contract"]:
            issues.append("missing importable app at src/app.py")

        checks["health_endpoint"] = "/health" in app_py or any(
            "/health" in c for p, c in files.items()
            if p.endswith(".py") and "health" in c.lower()
        )
        if not checks["runnable_contract"]:
            checks["health_endpoint"] = False
        elif not checks["health_endpoint"]:
            issues.append("GET /health not found in app code")

        test_files = [p for p in paths if "test" in p.lower() and p.endswith(".py")]
        checks["tests_present"] = bool(test_files)
        if not checks["tests_present"]:
            issues.append("no test files found")

        checks["sample_data"] = any(
            "sample" in p.lower() or "fixture" in p.lower()
            for p in paths
        )
        if not checks["sample_data"]:
            issues.append("no sample data / fixtures detected (soft)")

        py_blob = _all_py(files)
        checks["no_bare_except"] = not bool(_BARE_EXCEPT_RE.search(py_blob))
        if not checks["no_bare_except"]:
            issues.append("bare except clause detected")

        checks["parameterized_db"] = (
            "?" in py_blob or "%s" in py_blob or ":name" in py_blob
            or "execute(" not in py_blob.lower()
            or not _STRING_FMT_SQL_RE.search(py_blob)
        )
        if not checks["parameterized_db"]:
            issues.append("possible string-formatted SQL (use parameters)")

        checks["input_validation"] = any(
            w in py_blob.lower()
            for w in ("httpexception", "validationerror", "field(", "validator")
        ) or "fastapi" not in py_blob.lower()
        if not checks["input_validation"]:
            issues.append("limited input validation patterns (soft)")

        checks["readme_or_docs"] = any(
            p.lower().endswith((".md", ".rst")) or p.startswith("docs/")
            for p in paths
        )

        weights = {
            "runnable_contract": 3.0,
            "health_endpoint": 2.0,
            "tests_present": 2.0,
            "no_bare_except": 2.0,
            "parameterized_db": 1.5,
            "sample_data": 0.5,
            "input_validation": 0.5,
            "readme_or_docs": 0.3,
        }
        components = {k: 1.0 if v else 0.0 for k, v in checks.items()}
        score = weighted_score(components, weights)

        return Measurement(
            meter=self.name,
            score=score,
            components=components,
            weights=weights,
            evidence={"checks": checks, "description": (description or "")[:200]},
            issues=issues,
            threshold=self.default_threshold,
        )
