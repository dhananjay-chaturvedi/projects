"""Final packaging — turn a built app workspace into a shippable bundle.

This is the shared, surface-neutral logic behind the "Approve" action: once a
user has reviewed/tested a build (whether it passed or failed the meter gates),
packaging makes the workspace fully self-installing and runnable on a clean
machine. It writes cross-platform install/run scripts (create a virtualenv,
install all dependencies, complete first-run setup) plus an ``INSTALL.md`` and,
optionally, a distributable ``.zip`` archive.

No model calls and no network: deterministic file generation over an existing
workspace, so UI, CLI, and API all share one code path.
"""

from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Mirrors ai_assistant.app_builder.webapp._requirements(); used only as a
# fallback when a build somehow produced no requirements.txt.
_DEFAULT_REQUIREMENTS = (
    "fastapi\n"
    "uvicorn[standard]\n"
    "jinja2\n"
    "python-multipart\n"
    "httpx\n"
    "pytest\n"
    "pyyaml\n"
)

# Never bundle environment/build cruft or per-run SQLite state in the archive.
_EXCLUDE_DIRS = frozenset({
    ".venv", "venv", "__pycache__", ".git", ".pytest_cache", ".mypy_cache",
    ".ruff_cache", "node_modules", ".idea", ".vscode", "var",
})
_EXCLUDE_SUFFIXES = (
    ".pyc", ".pyo",
    ".db", ".sqlite", ".sqlite3",
    ".db-journal", ".db-wal", ".db-shm",
)
_EXCLUDE_FILES = frozenset({
    "app.db", "store.db", "data.db", "test_runtime.db", ".DS_Store",
})


@dataclass
class PackageResult:
    """Outcome of packaging a workspace into a shippable bundle."""

    ok: bool
    workspace: str
    app_name: str
    entrypoint: str = "src.app:app"
    port: int = 8000
    created: list[str] = field(default_factory=list)
    archive: str = ""
    issues: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "workspace": self.workspace,
            "app_name": self.app_name,
            "entrypoint": self.entrypoint,
            "port": self.port,
            "created": list(self.created),
            "archive": self.archive,
            "issues": list(self.issues),
        }


def package_app(
    workspace: str | Path,
    *,
    app_name: str = "",
    port: int | None = None,
    make_archive: bool = True,
) -> PackageResult:
    """Make *workspace* shippable: install/run scripts, INSTALL.md, archive.

    Idempotent — re-running overwrites the generated scaffolding. Safe to call
    on a partial/failed build as long as ``src/app.py`` exists, so the user can
    Approve-and-ship whatever state they have validated.
    """
    ws = Path(workspace)
    name = app_name or ws.name or "app"
    if port is None:
        from ai_query import module_config as mc
        port = mc.get_int("ai.app_builder", "default_port", default=8000)
    _fallback_port = 8000
    safe_port = port if 1 <= int(port) <= 65535 else _fallback_port
    result = PackageResult(
        ok=False, workspace=str(ws), app_name=name, port=int(safe_port))

    if not ws.exists() or not ws.is_dir():
        result.issues.append(f"workspace does not exist: {ws}")
        return result
    if not (ws / "src" / "app.py").exists():
        result.issues.append(
            "no src/app.py in workspace — build the app before packaging")
        return result

    req_path = ws / "requirements.txt"
    if not req_path.exists() or not req_path.read_text(encoding="utf-8").strip():
        req_path.write_text(_DEFAULT_REQUIREMENTS, encoding="utf-8")
        result.created.append("requirements.txt")

    written = _write_scripts(ws, name, safe_port)
    result.created.extend(written)

    if make_archive:
        try:
            archive = _make_archive(ws, name)
            result.archive = str(archive)
        except OSError as exc:
            result.issues.append(f"could not create archive: {exc}")

    result.ok = True
    return result


def _write_scripts(ws: Path, name: str, port: int) -> list[str]:
    files: dict[str, str] = {
        "setup_db.py": _setup_db_py(),
        "install.sh": _install_sh(port),
        "install.bat": _install_bat(port),
        "run.sh": _run_sh(port),
        "run.bat": _run_bat(port),
        "INSTALL.md": _install_md(name, port),
    }
    created: list[str] = []
    for rel, content in files.items():
        path = ws / rel
        path.write_text(content, encoding="utf-8")
        if rel.endswith(".sh"):
            _make_executable(path)
        created.append(rel)
    return created


def _make_executable(path: Path) -> None:
    try:
        mode = path.stat().st_mode
        path.chmod(mode | 0o111)
    except OSError:
        pass


def _make_archive(ws: Path, name: str) -> Path:
    """Zip the workspace (minus venv/db/cache) for distribution."""
    archive = ws.parent / f"{name}-package.zip"
    if archive.exists():
        archive.unlink()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, fnames in os.walk(ws):
            dirs[:] = [d for d in dirs if d not in _EXCLUDE_DIRS]
            for fname in fnames:
                if fname in _EXCLUDE_FILES or fname.endswith(_EXCLUDE_SUFFIXES):
                    continue
                full = Path(root) / fname
                if full.resolve() == archive.resolve():
                    continue
                zf.write(full, Path(name) / full.relative_to(ws))
    return archive


def _install_sh(port: int) -> str:
    return (
        "#!/usr/bin/env bash\n"
        "# Install all dependencies and complete first-run setup.\n"
        "# Generated by App Builder packaging — safe to re-run.\n"
        "set -euo pipefail\n"
        'cd "$(dirname "$0")"\n\n'
        'PYTHON="${PYTHON:-python3}"\n'
        'echo "[install] creating virtual environment (.venv)…"\n'
        '"$PYTHON" -m venv .venv\n'
        '# shellcheck disable=SC1091\n'
        'source .venv/bin/activate\n\n'
        'echo "[install] upgrading pip…"\n'
        "python -m pip install --upgrade pip >/dev/null\n\n"
        'echo "[install] installing dependencies from requirements.txt…"\n'
        "python -m pip install -r requirements.txt\n\n"
        'echo "[install] completing application setup…"\n'
        "python setup_db.py\n\n"
        'echo "[install] done. Start the app with: ./run.sh"\n'
    )


def _install_bat(port: int) -> str:
    return (
        "@echo off\r\n"
        "REM Install all dependencies and complete first-run setup.\r\n"
        "REM Generated by App Builder packaging - safe to re-run.\r\n"
        "setlocal\r\n"
        'cd /d "%~dp0"\r\n'
        "if not defined PYTHON set PYTHON=python\r\n"
        "echo [install] creating virtual environment (.venv)...\r\n"
        "%PYTHON% -m venv .venv\r\n"
        "call .venv\\Scripts\\activate.bat\r\n"
        "echo [install] upgrading pip...\r\n"
        "python -m pip install --upgrade pip\r\n"
        "echo [install] installing dependencies from requirements.txt...\r\n"
        "python -m pip install -r requirements.txt\r\n"
        "echo [install] completing application setup...\r\n"
        "if not defined APP_DB_PATH set APP_DB_PATH=app.db\r\n"
        "python setup_db.py\r\n"
        "echo [install] done. Start the app with: run.bat\r\n"
        "endlocal\r\n"
    )


def _run_sh(port: int) -> str:
    return (
        "#!/usr/bin/env bash\n"
        "# Launch the packaged app (run ./install.sh first).\n"
        "set -euo pipefail\n"
        'cd "$(dirname "$0")"\n'
        "if [ ! -d .venv ]; then\n"
        '  echo "[run] .venv not found — running install first…"\n'
        "  ./install.sh\n"
        "fi\n"
        "# shellcheck disable=SC1091\n"
        "source .venv/bin/activate\n"
        f'PORT="${{PORT:-{port}}}"\n'
        'APP_DB_PATH="${APP_DB_PATH:-app.db}"\n'
        'export APP_DB_PATH\n'
        'echo "[run] starting on http://127.0.0.1:${PORT} (Ctrl+C to stop)…"\n'
        'exec python -m uvicorn src.app:app --host 127.0.0.1 --port "${PORT}"\n'
    )


def _run_bat(port: int) -> str:
    return (
        "@echo off\r\n"
        "REM Launch the packaged app (run install.bat first).\r\n"
        "setlocal\r\n"
        'cd /d "%~dp0"\r\n'
        "if not exist .venv (\r\n"
        "  echo [run] .venv not found - running install first...\r\n"
        "  call install.bat\r\n"
        ")\r\n"
        "call .venv\\Scripts\\activate.bat\r\n"
        f"if not defined PORT set PORT={port}\r\n"
        "if not defined APP_DB_PATH set APP_DB_PATH=app.db\r\n"
        "echo [run] starting on http://127.0.0.1:%PORT% (Ctrl+C to stop)...\r\n"
        "python -m uvicorn src.app:app --host 127.0.0.1 --port %PORT%\r\n"
        "endlocal\r\n"
    )


def _setup_db_py() -> str:
    return (
        '"""First-run setup: initialize the database (schema + seed).\n\n'
        "Generated by App Builder packaging. Best-effort and idempotent: the app\n"
        "also initializes its data layer on startup, so a failure here is not\n"
        "fatal — the database is created on the first request instead.\n"
        '"""\n\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "import sys\n\n\n"
        "def main() -> int:\n"
        '    os.environ.setdefault("APP_DB_PATH", "app.db")\n'
        "    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))\n"
        "    try:\n"
        "        from src.db.connection import get_connection\n\n"
        "        get_connection()\n"
        '        print("[setup] database initialized at", os.environ["APP_DB_PATH"])\n'
        "    except Exception as exc:  # noqa: BLE001\n"
        '        print("[setup] database will initialize on first run:", exc)\n'
        "    return 0\n\n\n"
        'if __name__ == "__main__":\n'
        "    raise SystemExit(main())\n"
    )


def _install_md(name: str, port: int) -> str:
    return (
        f"# {name} — install & run\n\n"
        "This bundle is self-contained. It creates an isolated virtual "
        "environment, installs every dependency, completes first-run setup "
        "(database schema + seed data), and launches the app.\n\n"
        "## Quick start (macOS / Linux)\n\n"
        "```bash\n"
        "./install.sh   # one-time: venv + dependencies + setup\n"
        "./run.sh       # start the app\n"
        "```\n\n"
        "## Quick start (Windows)\n\n"
        "```bat\n"
        "install.bat\n"
        "run.bat\n"
        "```\n\n"
        f"Then open http://127.0.0.1:{port}/ in a browser "
        f"(API docs at http://127.0.0.1:{port}/docs).\n\n"
        "## Configuration\n\n"
        "- `PORT` — override the listen port (default "
        f"{port}).\n"
        "- `APP_DB_PATH` — SQLite file path (default `app.db`). Point this at a "
        "different path to relocate the database.\n\n"
        "## What the installer does\n\n"
        "1. Creates a `.venv` virtual environment.\n"
        "2. Installs all packages from `requirements.txt`.\n"
        "3. Initializes the database (schema + seeded sample data).\n"
        "4. Leaves the app ready to start with the run script.\n"
    )
