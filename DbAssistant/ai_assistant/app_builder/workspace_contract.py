"""Workspace contract repairs for generated runnable apps."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


PROTECTED_FILES = (
    "src/settings.py",
    "src/db/__init__.py",
    "src/db/connection.py",
    "src/db/schema.py",
    "src/db/schema.sql",
    "tests/conftest.py",
)

_FORBIDDEN_MARKERS = (
    "DBASSIST_DB_PATH",
    "def init_db(",
    "sqlite3.connect(':memory:'",
    'sqlite3.connect(":memory:"',
)

_FORBIDDEN_APP_MARKERS = _FORBIDDEN_MARKERS + (
    "from src import db",
    "from src.database import",
    "from src import database",
    "import src.database",
)

_ALTERNATE_DB_MODULE_PATHS = (
    "src/database.py",
)


def _content_has_forbidden_db_layer(content: str) -> bool:
    return any(marker in content for marker in _FORBIDDEN_MARKERS)


def _app_needs_restore(app_current: str) -> bool:
    return any(marker in app_current for marker in _FORBIDDEN_APP_MARKERS)


def _remove_alternate_db_modules(workspace: Path, report: ReconcileReport) -> None:
    """Drop agent-invented alternate DB modules that collide with ``src/db/``."""
    db_package = workspace / "src" / "db"
    if not db_package.is_dir():
        return
    for rel in _ALTERNATE_DB_MODULE_PATHS:
        path = workspace / rel
        if not path.exists():
            continue
        try:
            path.unlink()
            report.removed.append(rel)
            report.changed = True
        except OSError as exc:
            report.notes.append(f"could not remove {rel}: {exc}")


@dataclass
class ReconcileReport:
    """Changes made to keep the generated app launchable."""

    changed: bool = False
    removed: list[str] = field(default_factory=list)
    restored: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "changed": self.changed,
            "removed": list(self.removed),
            "restored": list(self.restored),
            "notes": list(self.notes),
        }


def reconcile_data_layer(
    workspace: Path,
    baseline: dict[str, str],
    *,
    restore_app_on_forbidden_init: bool = True,
) -> ReconcileReport:
    """Repair data-layer drift introduced by agentic workspace writes.

    Session A may improve the app, but FROM_DATABASE/FastAPI prototypes must keep
    the deterministic SQLite runtime contract: ``src/db/`` package,
    ``APP_DB_PATH``, and file-backed tests. This function removes the known
    collision ``src/db.py`` and restores protected files from the baseline when
    they are missing or contain forbidden alternate DB-layer markers.
    """
    report = ReconcileReport()

    db_module = workspace / "src" / "db.py"
    db_package = workspace / "src" / "db"
    if db_module.exists() and db_package.is_dir():
        try:
            db_module.unlink()
            report.removed.append("src/db.py")
            report.changed = True
        except OSError as exc:
            report.notes.append(f"could not remove src/db.py: {exc}")

    _remove_alternate_db_modules(workspace, report)

    for rel in PROTECTED_FILES:
        expected = baseline.get(rel)
        if expected is None:
            report.notes.append(f"baseline missing protected file: {rel}")
            continue
        path = workspace / rel
        current = ""
        if path.exists():
            try:
                current = path.read_text(encoding="utf-8")
            except OSError:
                current = ""
        needs_restore = not path.exists() or _content_has_forbidden_db_layer(current)
        if needs_restore and current != expected:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(expected, encoding="utf-8")
            report.restored.append(rel)
            report.changed = True

    app_rel = "src/app.py"
    app_path = workspace / app_rel
    app_expected = baseline.get(app_rel)
    if restore_app_on_forbidden_init and app_expected and app_path.exists():
        try:
            app_current = app_path.read_text(encoding="utf-8")
        except OSError:
            app_current = ""
        if _app_needs_restore(app_current) and app_current != app_expected:
            app_path.write_text(app_expected, encoding="utf-8")
            report.restored.append(app_rel)
            report.changed = True
    elif restore_app_on_forbidden_init and app_path.exists() and not app_expected:
        report.notes.append("baseline missing src/app.py — cannot restore app entrypoint")

    return report


def reconcile_file_map(files: dict[str, str], baseline: dict[str, str]) -> dict[str, str]:
    """Apply the same data-layer contract to an in-memory file map."""
    out = dict(files)
    if "src/db.py" in out and any(p.startswith("src/db/") for p in out):
        out.pop("src/db.py", None)
    if any(p.startswith("src/db/") for p in out):
        for rel in _ALTERNATE_DB_MODULE_PATHS:
            out.pop(rel, None)
    for rel in PROTECTED_FILES:
        expected = baseline.get(rel)
        if expected is None:
            continue
        current = out.get(rel, "")
        if rel not in out or _content_has_forbidden_db_layer(current):
            out[rel] = expected
    app_expected = baseline.get("src/app.py")
    app_current = out.get("src/app.py", "")
    if app_expected and _app_needs_restore(app_current):
        out["src/app.py"] = app_expected
    return out
