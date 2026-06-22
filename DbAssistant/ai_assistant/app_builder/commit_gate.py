"""Per-commit governance gate for agentic App Builder writes."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint

# Directories never snapshotted or deleted on restore (runtime/build cruft).
_IGNORE_DIRS = frozenset({
    "__pycache__", ".venv", "venv", "node_modules", ".git",
    ".pytest_cache", ".mypy_cache", ".ruff_cache", "build", "dist", "var",
})

# File suffixes treated as binary/volatile — never snapshotted or deleted on restore.
_IGNORE_SUFFIXES = (
    ".pyc", ".pyo", ".so",
    ".db", ".sqlite", ".sqlite3",
    ".db-journal", ".db-wal", ".db-shm",
    ".png", ".jpg", ".jpeg", ".gif", ".ico",
    ".pdf", ".zip", ".gz",
)


@dataclass
class CommitVerdict:
    """Result of gating one agent 'commit' (a batch of workspace changes)."""

    accepted: bool
    score: float
    coverage: float
    issues: list[str] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    files_changed: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    reverted: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "score": round(self.score, 4),
            "coverage": round(self.coverage, 4),
            "issues": list(self.issues),
            "gaps": list(self.gaps),
            "files_changed": list(self.files_changed),
            "suggestions": list(self.suggestions),
            "reverted": self.reverted,
        }

    def feedback_text(self) -> str:
        """Text fed back to the builder session after gating."""
        status = "ACCEPTED" if self.accepted else "REJECTED"
        lines = [
            f"COMMIT GATE: {status}  score={self.score:.3f}  "
            f"coverage={self.coverage:.3f}",
        ]
        if self.files_changed:
            lines.append("Files changed: " + ", ".join(self.files_changed[:20]))
        for issue in self.issues[:10]:
            lines.append(f"  issue: {issue}")
        for gap in self.gaps[:10]:
            lines.append(f"  gap: {gap}")
        for sug in self.suggestions[:6]:
            lines.append(f"  suggestion (optional add-on): {sug}")
        if not self.accepted:
            if self.reverted:
                lines.append(
                    "The above changes were reverted. Please fix the issues "
                    "and retry.")
            else:
                lines.append(
                    "The above changes were KEPT (nothing deleted). Please "
                    "address the issues above.")
        return "\n".join(lines)


@dataclass(frozen=True)
class CommitGateConfig:
    """Policy inputs for commit-gate evaluation."""

    req_entities: Optional[list[str]] = None
    req_features: Optional[list[str]] = None
    req_services: Optional[list[str]] = None
    description: str = ""
    kind: str = "crud"
    target_coverage: float = 0.9
    fidelity_driven: bool = False
    structure_enforced: bool = True


def _snapshot_path_ignored(rel_path: Path) -> bool:
    """Return True when *rel_path* must not be snapshotted or deleted on restore."""
    if _IGNORE_DIRS & set(rel_path.parts):
        return True
    name = rel_path.name.lower()
    if any(name.endswith(suffix) for suffix in _IGNORE_SUFFIXES):
        return True
    return _hidden_path_ignored(rel_path)


def snapshot_workspace(workspace: Path) -> dict[str, str]:
    """Capture UTF-8 text files under *workspace* for revert.

    Binary and volatile artifacts (``.db``, ``__pycache__``, ``var/``, etc.) are
    skipped so a failed commit revert cannot corrupt runtime databases or bytecode.
    """
    snap: dict[str, str] = {}
    if not workspace.is_dir():
        return snap
    for path in workspace.rglob("*"):
        if not path.is_file():
            continue
        rel_path = path.relative_to(workspace)
        rel = str(rel_path)
        if _snapshot_path_ignored(rel_path):
            continue
        try:
            snap[rel] = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        except OSError:
            pass
    return snap


def _hidden_path_ignored(rel_path: Path) -> bool:
    """Ignore hidden editor/cache files but keep required app dot-artifacts."""
    if not any(part.startswith(".") for part in rel_path.parts):
        return False
    allowed_prefixes = (".github",)
    if rel_path.parts and rel_path.parts[0] in allowed_prefixes:
        return False
    return True


def restore_snapshot(workspace: Path, snapshot: dict[str, str]) -> None:
    """Restore *workspace* to *snapshot* (revert failed commit)."""
    current = {
        str(p.relative_to(workspace))
        for p in workspace.rglob("*")
        if p.is_file() and not _snapshot_path_ignored(p.relative_to(workspace))
    }
    for rel in current - set(snapshot):
        try:
            (workspace / rel).unlink(missing_ok=True)
        except OSError:
            pass
    for rel, content in snapshot.items():
        path = workspace / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def read_workspace_files(workspace: Path) -> dict[str, str]:
    """Read relative-path -> content for evaluation."""
    return snapshot_workspace(workspace)


class CommitGate:
    """Validate agent workspace changes through engine + meters."""

    def __init__(
        self,
        engine: AiAppEngine,
        blueprint: AppBlueprint,
        config: CommitGateConfig | None = None,
        **legacy,
    ) -> None:
        config = config or CommitGateConfig(**legacy)
        self.engine = engine
        self.blueprint = blueprint
        self.meters = engine.meters
        self._req_entities = list(config.req_entities or [])
        self._req_features = list(config.req_features or [])
        self._req_services = list(config.req_services or [])
        self._description = config.description
        self._kind = config.kind
        self._target_coverage = config.target_coverage
        self._fidelity_driven = config.fidelity_driven
        self._structure_enforced = config.structure_enforced

    def evaluate_files(
        self,
        files: dict[str, str],
        *,
        infra_blocking: bool = True,
        enforce_coverage: bool = True,
    ) -> CommitVerdict:
        """Score the workspace.

        ``infra_blocking`` — when False, infrastructure add-ons (docs, hosting,
        CI, monitoring) are NOT required to accept the commit; they are reported
        as optional ``suggestions`` instead. ``enforce_coverage`` — when False,
        an incomplete-but-valid commit is still accepted (coverage gaps are fed
        back as a nudge rather than triggering a revert). Both default to True so
        the start/end gates remain strict; the per-round loop relaxes them.
        """
        sample = files.get("src/app.py", "") or next(
            (c for p, c in files.items() if p.endswith(".py")), "")
        verdict = self.engine.evaluate_build(
            self.blueprint, sorted(files.keys()), sample_code=sample,
            include_infra=infra_blocking)
        cov = self._coverage(files)
        issues = list(verdict.issues)
        cov_ok = cov["score"] >= self._target_coverage
        suggestions: list[str] = []
        if not self._structure_enforced:
            # from_scratch: structure is advisory — never block on manifest or
            # coverage gaps; surface them as feedback only.
            if not verdict.accepted:
                suggestions.extend(
                    f"advisory: {i}" for i in verdict.issues[:10])
            suggestions.extend(
                f"advisory gap: {g}" for g in cov.get("gaps", [])[:10])
            if not infra_blocking:
                suggestions.extend(self.engine.infra_suggestions(
                    self.blueprint, sorted(files.keys())))
            accepted = True
            issues = []
        else:
            accepted = bool(verdict.accepted and (cov_ok or not enforce_coverage))
            if not infra_blocking:
                suggestions = self.engine.infra_suggestions(
                    self.blueprint, sorted(files.keys()))
        return CommitVerdict(
            accepted=accepted,
            score=verdict.score,
            coverage=cov["score"],
            issues=issues,
            gaps=list(cov.get("gaps", [])),
            suggestions=suggestions,
        )

    def gate(
        self,
        workspace: Path,
        before: dict[str, str],
        *,
        label: str = "commit",
        infra_blocking: bool = True,
        enforce_coverage: bool = True,
        revert: bool = True,
    ) -> CommitVerdict:
        """Evaluate workspace vs *before* snapshot; optionally revert bad commits.

        With the per-round defaults (``infra_blocking=False``,
        ``enforce_coverage=False``) a commit fails the gate only when it is
        genuinely bad — broken/low-quality code or a missing core surface — not
        for being incomplete or lacking optional infra.

        ``revert`` — when False the agent's work is NEVER deleted on a failed
        gate; the files stay on disk and the orchestrator decides what to do
        (keep iterating in uninterrupted mode, or pause for the user). The
        per-round loop passes ``revert=False`` so partial progress is preserved;
        callers that want a hard rollback (e.g. unit tests) keep the default.
        """
        after = read_workspace_files(workspace)
        changed = sorted(
            (set(after) - set(before))
            | {k for k in before if before.get(k) != after.get(k)}
        )
        verdict = self.evaluate_files(
            after, infra_blocking=infra_blocking,
            enforce_coverage=enforce_coverage)
        verdict.files_changed = changed
        if not self._structure_enforced:
            revert = False
        if not verdict.accepted and revert:
            # Revert only the isolated App Builder workspace snapshot. Do not
            # use git clean/checkout here: generated app workspaces may sit
            # inside the project repository and are usually untracked.
            restore_snapshot(workspace, before)
            verdict.reverted = True
        return verdict

    def _coverage(self, files: dict[str, str]) -> dict[str, Any]:
        base = self.meters.evaluate_requirements(
            entities=self._req_entities,
            features=self._req_features,
            files=files,
            services=self._req_services,
            kind=self._kind,
            threshold=self._target_coverage,
        )
        if not self._fidelity_driven:
            return base
        app_files = {p: c for p, c in files.items()
                     if not (p.lower().endswith(".md") or p.startswith("docs/"))}
        fid = self.meters.evaluate_fidelity(
            description=self._description, files=app_files,
            entities=self._req_entities,
        )
        score = min(base.get("score", 1.0), fid.get("score", 1.0))
        gaps = list(base.get("gaps", []))
        gaps += [f"requirement not yet reflected: {tok}"
                 for tok in fid.get("missing", [])]
        return {**base, "score": score, "gaps": gaps,
                "fully_covered": score >= self._target_coverage}
