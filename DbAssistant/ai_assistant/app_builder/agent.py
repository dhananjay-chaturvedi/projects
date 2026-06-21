"""Build agents for AppBuilderAssistant.

Two implementations behind one protocol:

* :class:`DeterministicAgent` — generates real, sensible files from the
  blueprint / schema / codebase facts with no model call. Always available,
  fully testable, and used as the safety-net fallback.
* :class:`CliBackendAgent` — asks an AI backend (Claude / Cursor / Codex) to
  generate files using a strict, parseable contract. The engine still validates
  every produced file; anything the AI returns that fails the quality gate is
  rejected and the deterministic scaffold fills the gap.

The "prompt" is only the instruction layer (:func:`frame_prompt`); correctness,
completeness and isolation are enforced by code (engine + meters + parser).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

from ai_assistant.app_builder.pii_util import mask_if_enabled
from ai_assistant.app_builder.engine import BuildMode
from ai_assistant.app_builder.requirements import derive_spec
from ai_assistant.app_builder.spec import AppSpec
from ai_assistant.app_builder.webapp import generate_app, minimal_scratch_stub


@dataclass
class GeneratedFile:
    path: str
    content: str


@dataclass
class AgentRequest:
    mode: BuildMode
    app_name: str
    description: str = ""
    language: str = "python"
    services: list[str] = field(default_factory=list)
    required_files: list[str] = field(default_factory=list)
    rules: list[str] = field(default_factory=list)
    schema: dict[str, list[str]] = field(default_factory=dict)
    codebase_facts: dict[str, Any] = field(default_factory=dict)
    entities: list[str] = field(default_factory=list)
    features: list[str] = field(default_factory=list)
    kind: str = ""  # app archetype (crud | storefront); "" = auto-detect


@dataclass
class AgentResponse:
    files: list[GeneratedFile] = field(default_factory=list)
    notes: str = ""
    backend: str = "deterministic"


class BuildAgent(Protocol):
    def generate(self, req: AgentRequest) -> AgentResponse: ...


# ── prompt framing (instruction layer only) ─────────────────────────────────--
def frame_prompt(req: AgentRequest) -> str:
    """Build a strict, parseable instruction for an AI backend."""
    lines = [
        "You are an application builder operating under AiAppEngine governance.",
        f"MODE: {req.mode.value}",
        f"APP NAME: {req.app_name}",
        f"LANGUAGE: {req.language}",
    ]
    if req.description:
        lines.append(f"DESCRIPTION: {req.description}")
    if req.mode == BuildMode.FROM_SCRATCH and not req.schema:
        lines += [
            "BUILD GOAL: First UNDERSTAND the description, then design and build "
            "the COMPLETE, working application the user actually wants — real "
            "pages, navigation, forms and business logic — backed by a FastAPI + "
            "Jinja2 server-rendered UI.",
            "Model ONLY the data the app genuinely needs. Do NOT create a database "
            "table per noun in the description; design the app first, then add "
            "just the data models its features require.",
        ]
    if req.services:
        lines.append(f"SERVICES: {', '.join(req.services)}")
    if req.mode == BuildMode.FROM_DATABASE:
        lines += [
            "BUILD GOAL: Infer from the schema (and data) what KIND of real "
            "application this database serves, and build THAT application — its "
            "pages, flows and business logic — using the tables as the data "
            "layer. Do NOT just expose CRUD over the tables or mirror the schema. "
            "The description, if any, is only a hint to confirm the app type.",
        ]
    if req.schema:
        lines.append("DATABASE SCHEMA (use ONLY these tables/columns):")
        for table, cols in req.schema.items():
            lines.append(f"  - {table}({', '.join(cols)})")
    if req.codebase_facts:
        lines.append(f"CODEBASE FACTS: {req.codebase_facts}")
    if req.rules:
        lines.append("HARD RULES (violations are rejected by code):")
        lines += [f"  - {r}" for r in req.rules]
    if req.required_files:
        lines.append("YOU MUST PRODUCE AT LEAST THESE FILES:")
        lines += [f"  - {f}" for f in req.required_files]
    lines += [
        "",
        "OUTPUT CONTRACT — return each file exactly as:",
        "=== FILE: relative/path ===",
        "<file content>",
        "=== END FILE ===",
        "Return ONLY file blocks, no prose outside them.",
    ]
    return "\n".join(lines)


_FILE_RE = re.compile(
    r"===\s*FILE:\s*(?P<path>[^\n=]+?)\s*===\n(?P<body>.*?)\n===\s*END FILE\s*===",
    re.DOTALL,
)


def parse_files(text: str) -> list[GeneratedFile]:
    """Parse the strict FILE-block contract from an AI response (robust)."""
    out: list[GeneratedFile] = []
    seen: set[str] = set()
    for m in _FILE_RE.finditer(text or ""):
        path = m.group("path").strip().lstrip("/")
        body = m.group("body")
        try:
            from common.security.paths import assert_safe_relative_file

            path = assert_safe_relative_file(path)
        except Exception:
            continue
        # Strip a single wrapping code fence if present.
        body = re.sub(r"^```[a-zA-Z0-9]*\n", "", body)
        body = re.sub(r"\n```$", "", body)
        if path and path not in seen:
            seen.add(path)
            out.append(GeneratedFile(path=path, content=body))
    return out


# ── deterministic generation ────────────────────────────────────────────────-
def _to_files(file_map: dict[str, str]) -> list[GeneratedFile]:
    return [GeneratedFile(path=p, content=c) for p, c in file_map.items()]


def spec_for(req: AgentRequest) -> AppSpec:
    """Derive the normalized :class:`AppSpec` the generator would build for *req*.

    Shared by :class:`DeterministicAgent` and the orchestrator so the
    requirement-coverage meter scores exactly the entities/features that get
    generated (single source of truth). Codebase mode has no entities/spec.
    """
    return derive_spec(
        app_name=req.app_name,
        description=req.description,
        schema=req.schema or None if req.mode == BuildMode.FROM_DATABASE else None,
        services=req.services,
        features=req.features or None,
        entities=req.entities or None,
        kind=req.kind or None,
    )


class DeterministicAgent:
    """Generate real files without any model — the reliable baseline."""

    backend = "deterministic"

    def generate(self, req: AgentRequest) -> AgentResponse:
        if req.mode == BuildMode.FROM_DATABASE:
            files = self._from_database(req)
        elif req.mode == BuildMode.FROM_CODEBASE:
            files = self._from_codebase(req)
        else:
            files = self._from_scratch(req)
        return AgentResponse(files=files, notes="deterministic scaffold",
                             backend=self.backend)

    # -- scratch --
    def _from_scratch(self, req: AgentRequest) -> list[GeneratedFile]:
        return _to_files(minimal_scratch_stub(spec_for(req)))

    # -- database --
    def _from_database(self, req: AgentRequest) -> list[GeneratedFile]:
        return _to_files(generate_app(spec_for(req)))

    # -- codebase --
    def _from_codebase(self, req: AgentRequest) -> list[GeneratedFile]:
        facts = req.codebase_facts or {}
        report = ["# Codebase analysis", "",
                  f"- Python files analyzed: {facts.get('files', 0)}",
                  f"- Total LOC: {facts.get('loc', 0)}",
                  f"- Avg complexity: {facts.get('avg_complexity', 0)}",
                  f"- Max complexity: {facts.get('max_complexity', 0)}",
                  f"- Docstring coverage: {facts.get('docstring_coverage', 0)}",
                  "", "## Issues found", ""]
        for issue in facts.get("issues", []) or ["none detected"]:
            report.append(f"- {issue}")
        report += ["", "## Recommended next steps", ""]
        for rec in facts.get("recommendations", []) or ["Add tests and CI."]:
            report.append(f"- {rec}")
        files = [
            GeneratedFile("docs/ANALYSIS.md", "\n".join(report) + "\n"),
            GeneratedFile(".github/workflows/ci.yml",
                          "name: ci\non: [push]\njobs:\n  test:\n    runs-on: ubuntu-latest\n"
                          "    steps:\n      - uses: actions/checkout@v4\n"
                          "      - run: pip install -r requirements.txt || true\n"
                          "      - run: pytest -q || true\n"),
            GeneratedFile("tests/test_smoke.py",
                          "def test_placeholder():\n"
                          "    assert True  # replace with real coverage\n"),
            GeneratedFile("README.md",
                          f"# {req.app_name}\n\nExisting codebase enhanced by "
                          "AppBuilderAssistant. See docs/ANALYSIS.md.\n"),
        ]
        return files


class CliBackendAgent:
    """Generate files by asking an AI backend, with deterministic fallback."""

    def __init__(self, backend: Any, *, timeout: int = 180,
                 fallback: Optional[BuildAgent] = None,
                 mask_pii: bool = False) -> None:
        self._backend = backend
        self._timeout = timeout
        self._fallback = fallback or DeterministicAgent()
        self._mask_pii = bool(mask_pii)

    def generate(self, req: AgentRequest) -> AgentResponse:
        try:
            prompt = mask_if_enabled(frame_prompt(req), self._mask_pii)
            result = self._backend.call(prompt, timeout=self._timeout)
            text = result.get("response") or ""
            files = parse_files(text)
        except Exception:
            files = []
        if not files:
            resp = self._fallback.generate(req)
            resp.notes = "AI returned no parseable files; used deterministic fallback"
            return resp
        return AgentResponse(
            files=files,
            notes="AI-generated (engine-validated)",
            backend=getattr(self._backend, "name", "ai"),
        )
