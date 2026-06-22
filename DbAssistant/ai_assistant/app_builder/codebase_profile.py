"""Deterministic codebase profiling for from_codebase app builds."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ai_query import module_config as mc


@dataclass
class CodebaseProfile:
    path: str = ""
    files: int = 0
    loc: int = 0
    folder_tree: list[str] = field(default_factory=list)
    entrypoints: list[str] = field(default_factory=list)
    routes: list[str] = field(default_factory=list)
    apis: list[str] = field(default_factory=list)
    services: list[str] = field(default_factory=list)
    third_party_deps: list[str] = field(default_factory=list)
    db_tables: list[str] = field(default_factory=list)
    docs: list[str] = field(default_factory=list)
    hld_signals: list[str] = field(default_factory=list)
    complexity: dict[str, Any] = field(default_factory=dict)
    issues: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "files": self.files,
            "loc": self.loc,
            "folder_tree": list(self.folder_tree),
            "entrypoints": list(self.entrypoints),
            "routes": list(self.routes),
            "apis": list(self.apis),
            "services": list(self.services),
            "third_party_deps": list(self.third_party_deps),
            "db_tables": list(self.db_tables),
            "docs": list(self.docs),
            "hld_signals": list(self.hld_signals),
            "complexity": dict(self.complexity),
            "issues": list(self.issues),
            "recommendations": list(self.recommendations),
        }


_ROUTE_RE = re.compile(
    r"@(?:app|router|api)\.(?:get|post|put|delete|patch)\s*\(\s*[\"']([^\"']+)",
    re.IGNORECASE,
)
_IMPORT_RE = re.compile(r"^(?:from|import)\s+(\S+)", re.MULTILINE)
_MODEL_RE = re.compile(r"class\s+(\w+)\s*\([^)]*Model[^)]*\)")


class CodebaseProfiler:
    """Walk a Python codebase and extract deterministic architecture facts."""

    def __init__(self, path: str = "", *, max_files: int | None = None) -> None:
        self._path = path
        self._max_files = max_files or mc.get_int(
            "ai.app_builder", "codebase_max_files", default=400)

    def profile(self) -> CodebaseProfile:
        root = Path(self._path) if self._path else None
        from ai_assistant.app_builder.flows import analyze_codebase

        base_facts = analyze_codebase(self._path, max_files=self._max_files)
        profile = CodebaseProfile(
            path=str(self._path or ""),
            files=base_facts.get("files", 0),
            loc=base_facts.get("loc", 0),
            issues=list(base_facts.get("issues") or []),
            recommendations=list(base_facts.get("recommendations") or []),
            complexity={
                "avg": base_facts.get("avg_complexity", 0),
                "max": base_facts.get("max_complexity", 0),
                "docstring_coverage": base_facts.get("docstring_coverage", 0),
            },
        )
        if not root or not root.exists():
            return profile

        ignored_dirs = {".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages"}
        py_files = [
            p for p in sorted(root.rglob("*.py"))
            if not any(part in ignored_dirs for part in p.relative_to(root).parts)
        ][: self._max_files]
        profile.folder_tree = _folder_tree(root, max_depth=4)
        profile.docs = [
            str(p.relative_to(root))
            for p in root.rglob("*")
            if p.is_file() and p.suffix.lower() in (".md", ".rst", ".txt")
            and ("readme" in p.name.lower() or p.name.upper() in (
                "ARCHITECTURE.MD", "DESIGN.MD", "LLD.MD", "HLD.MD"))
        ][:30]
        profile.hld_signals = [
            d for d in profile.docs
            if any(k in d.lower() for k in ("arch", "design", "hld", "lld"))
        ]

        for fp in py_files:
            rel = str(fp.relative_to(root))
            try:
                src = fp.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            if "__main__" in src or rel.endswith(("main.py", "app.py", "__main__.py")):
                profile.entrypoints.append(rel)
            for m in _ROUTE_RE.finditer(src):
                profile.routes.append(m.group(1))
            for m in _MODEL_RE.finditer(src):
                profile.db_tables.append(m.group(1))
            try:
                tree = ast.parse(src)
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef) and node.name.endswith("Service"):
                        profile.services.append(f"{rel}:{node.name}")
            except SyntaxError:
                pass
            for imp in _IMPORT_RE.findall(src):
                pkg = imp.split(".")[0]
                if pkg and not pkg.startswith(".") and pkg not in (
                    "os", "sys", "re", "json", "typing", "pathlib", "dataclasses",
                    "collections", "functools", "itertools", "datetime", "logging",
                ):
                    profile.third_party_deps.append(pkg)

        req = root / "requirements.txt"
        if req.is_file():
            for line in req.read_text(encoding="utf-8", errors="replace").splitlines():
                pkg = line.strip().split("==")[0].split(">=")[0].strip()
                if pkg and not pkg.startswith("#"):
                    profile.third_party_deps.append(pkg.split("[")[0])

        profile.routes = sorted(set(profile.routes))[:80]
        profile.apis = list(profile.routes)
        profile.services = sorted(set(profile.services))[:50]
        profile.third_party_deps = sorted(set(profile.third_party_deps))[:60]
        profile.db_tables = sorted(set(profile.db_tables))[:40]
        profile.entrypoints = sorted(set(profile.entrypoints))[:20]
        return profile


def _folder_tree(root: Path, *, max_depth: int = 4) -> list[str]:
    lines: list[str] = []
    for p in sorted(root.rglob("*")):
        if p.is_dir() and any(
            part in {".git", "__pycache__", ".venv", "node_modules"}
            for part in p.parts
        ):
            continue
        rel = p.relative_to(root)
        if len(rel.parts) > max_depth:
            continue
        prefix = "  " * (len(rel.parts) - 1)
        lines.append(f"{prefix}{rel.name}/" if p.is_dir() else f"{prefix}{rel.name}")
        if len(lines) >= 120:
            break
    return lines
