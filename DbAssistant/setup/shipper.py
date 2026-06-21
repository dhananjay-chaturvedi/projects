#!/usr/bin/env python3
"""
DbManagementTool — release shipper.

Builds a distributable ZIP archive of the tool that the receiver can
unzip and install by double-clicking ``install.command`` (macOS),
``install.bat`` (Windows) or ``install.sh`` (Linux).

Two modes
=========

* **Lean** (default, ~5 MB) — source-only. Receiver runs
  ``install.command`` which creates a venv and calls ``pip install -r ...``
  with the receiver's online package index.

* **Offline** (``--offline``) — bundles wheel files for **macOS arm64,
  macOS x86_64, Linux x86_64 and Windows x86_64** so the receiver can
  install without internet access. Significantly larger
  (typically 80-400 MB depending on module).

Per-module shipping
===================

The ``--module`` flag picks which feature is included. Defaults to
``full``. Module definitions come from ``setup/module_manifest.py``::

    full     -> entire tool (CLI + all UIs + all APIs)
    core     -> connections / SQL editor only
    ai       -> AI Query Assistant (UI + CLI + API) only
    monitor  -> Monitoring (UI + CLI + API + daemon) only
    migrator -> Data Migration only

All builds strip ``tests/``, caches, dotfiles, and any ``.env`` /
``.venv`` directories so secrets and stale state never ship.

Usage
-----

::

    python setup/shipper.py                              # lean full bundle
    python setup/shipper.py --module ai                  # lean ai-only
    python setup/shipper.py --offline                    # full + wheels
    python setup/shipper.py --module monitor --offline   # monitor + wheels
    python setup/shipper.py --output ./releases          # custom output dir

"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

_SETUP_DIR = Path(__file__).resolve().parent
_ROOT = _SETUP_DIR.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from setup.module_manifest import ModuleBundle, all_module_keys, get_module  # noqa: E402

# ---------------------------------------------------------------------------
# Constants & policies
# ---------------------------------------------------------------------------

MIN_PYTHON = (3, 10)

# What never ships, ever. Applied to every file copied into the bundle.
EXCLUDED_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".nox",
    ".cache",
    ".venv",
    "venv",
    "env",
    "node_modules",
    ".git",
    ".github",
    ".idea",
    ".vscode",
    ".cursor",
    ".claude",
    "logs",
    "htmlcov",
    "dist",
    "build",
    "tests",  # explicit user request: strip tests/ from the ship
}

EXCLUDED_FILE_GLOBS = (
    "*.pyc",
    "*.pyo",
    "*.swp",
    "*.swo",
    "*.tmp",
    "*.bak",
    "*.log",
    "*.log.*",
    "*.pid",
    "*.lock",
    "*.core",
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
    ".coverage",
    ".coverage.*",
    "coverage.xml",
)

EXCLUDED_EXACT_FILES = {
    ".env",
    ".env.example",  # template carrying API key placeholders — receiver creates their own
    ".python-version",
}

# Offline-bundle wheel matrix. (label, pip platform tags)
WHEEL_TARGETS: list[tuple[str, list[str]]] = [
    ("macos-arm64", ["macosx_11_0_arm64"]),
    ("macos-x86_64", ["macosx_10_15_x86_64"]),
    ("linux-x86_64", ["manylinux2014_x86_64", "manylinux_2_17_x86_64"]),
    ("windows-x86_64", ["win_amd64"]),
]
# Bundle wheels for every Python version we support so the receiver's
# interpreter (3.10/3.11/3.12) can find a matching wheel inside the
# bundled find-links directory.
WHEEL_PYTHON_VERSIONS = ["3.10", "3.11", "3.12"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class C:
    _enabled = sys.stdout.isatty() and not sys.platform.startswith("win")

    @classmethod
    def _w(cls, code: str, text: str) -> str:
        return f"\033[{code}m{text}\033[0m" if cls._enabled else text

    @classmethod
    def b(cls, t: str) -> str:
        return cls._w("1", t)

    @classmethod
    def red(cls, t: str) -> str:
        return cls._w("31", t)

    @classmethod
    def green(cls, t: str) -> str:
        return cls._w("32", t)

    @classmethod
    def yellow(cls, t: str) -> str:
        return cls._w("33", t)

    @classmethod
    def dim(cls, t: str) -> str:
        return cls._w("2", t)


def _info(msg: str) -> None:
    print(f"  {msg}")


def _section(title: str) -> None:
    print()
    print(C.b(f"== {title} =="))


def _ok(msg: str) -> None:
    print(f"  {C.green('OK')}    {msg}")


def _warn(msg: str) -> None:
    print(f"  {C.yellow('warn')}  {msg}")


def _fail(msg: str) -> None:
    print(f"  {C.red('fail')}  {msg}")


def _read_version() -> str:
    version_file = _ROOT / "VERSION"
    if not version_file.exists():
        return "0.0.0"
    raw = version_file.read_text(encoding="utf-8").strip().splitlines()
    return (raw[0].strip() if raw else "0.0.0") or "0.0.0"


def _human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0 or unit == "TB":
            return f"{size:6.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def _dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


def _is_excluded_file(name: str) -> bool:
    if name in EXCLUDED_EXACT_FILES:
        return True
    from fnmatch import fnmatch

    return any(fnmatch(name, g) for g in EXCLUDED_FILE_GLOBS)


def _copy_filtered(src: Path, dst: Path) -> int:
    """Copy ``src`` (file or dir) into ``dst``, applying exclusion rules.

    Returns the count of files copied.
    """
    copied = 0
    if src.is_file():
        if _is_excluded_file(src.name):
            return 0
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return 1
    if not src.is_dir():
        _warn(f"not found: {src}")
        return 0

    for root, dirs, files in os.walk(src):
        # Prune excluded directories in-place so os.walk doesn't descend.
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIR_NAMES and not d.startswith(".")]
        rel = Path(root).relative_to(src)
        out_dir = dst / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            if _is_excluded_file(f):
                continue
            shutil.copy2(Path(root) / f, out_dir / f)
            copied += 1
    return copied


# ---------------------------------------------------------------------------
# Requirements graph (expands -r includes recursively)
# ---------------------------------------------------------------------------


def _expand_requirements(req_files: Iterable[str]) -> list[Path]:
    """Expand a list of requirements files following ``-r other.txt`` lines.

    Returns absolute paths in include-order, deduplicated.
    """
    seen: set[Path] = set()
    out: list[Path] = []
    stack = list(req_files)
    while stack:
        rel = stack.pop(0)
        candidate = (_ROOT / rel).resolve()
        if candidate in seen:
            continue
        if not candidate.exists():
            _warn(f"requirements file not found: {rel}")
            continue
        seen.add(candidate)
        out.append(candidate)
        # Parse for further -r includes
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"^-r\s+(.+)$", line)
            if m:
                inc_rel = m.group(1).strip()
                # -r paths are relative to the file that mentions them
                inc_abs = (candidate.parent / inc_rel).resolve()
                stack.append(str(inc_abs.relative_to(_ROOT)) if inc_abs.is_relative_to(_ROOT) else str(inc_abs))
    return out


# ---------------------------------------------------------------------------
# Templated installer / README emitted into the bundle
# ---------------------------------------------------------------------------


INSTALL_SH_TEMPLATE = r"""#!/usr/bin/env bash
# DbManagementTool installer (bundled by shipper).
#
# Receiver: unzip the bundle, then run:
#     bash install.sh                       # online install
#     bash install.sh --offline             # use bundled wheels (if present)
#     bash install.sh --python /path/to/python3
#
# Creates a .venv in the bundle directory, installs the dependencies
# for the @@MODULE_KEY@@ bundle, and verifies imports.

set -eu

BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$BUNDLE_DIR"

USE_OFFLINE=0
PYTHON_BIN=""
_PY_NEXT=0
for arg in "$@"; do
    if [ "$_PY_NEXT" = "1" ]; then
        PYTHON_BIN="$arg"
        _PY_NEXT=0
        continue
    fi
    case "$arg" in
        --offline)   USE_OFFLINE=1 ;;
        --python=*)  PYTHON_BIN="${arg#*=}" ;;
        --python)    _PY_NEXT=1 ;;
    esac
done

# Detect Python >= @@MIN_PY_MAJOR@@.@@MIN_PY_MINOR@@
if [ -z "$PYTHON_BIN" ]; then
    for candidate in python3.12 python3.11 python3.10 python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            ver=$("$candidate" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || true)
            case "$ver" in
                3.1[0-9]|3.[2-9][0-9]|[4-9].*) PYTHON_BIN="$candidate"; break ;;
            esac
        fi
    done
fi

if [ -z "$PYTHON_BIN" ]; then
    echo "[FAIL] Python >= @@MIN_PY_MAJOR@@.@@MIN_PY_MINOR@@ not found. Install from https://www.python.org/downloads/" >&2
    exit 1
fi

echo "[INFO] Python:  $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
echo "[INFO] Bundle:  $BUNDLE_DIR"
echo "[INFO] Module:  @@MODULE_KEY@@"
[ "$USE_OFFLINE" = "1" ] && echo "[INFO] Mode:    offline (using bundled wheels/)"

# Create venv
VENV_DIR="$BUNDLE_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "[INFO] Creating virtual environment at $VENV_DIR"
    "$PYTHON_BIN" -m venv "$VENV_DIR"
fi
VENV_PY="$VENV_DIR/bin/python"
[ -x "$VENV_PY" ] || VENV_PY="$VENV_DIR/Scripts/python.exe"

echo "[INFO] Upgrading pip / setuptools / wheel"
"$VENV_PY" -m pip install --upgrade pip setuptools wheel >/dev/null

# Install requirements
PIP_ARGS=()
if [ "$USE_OFFLINE" = "1" ]; then
    if [ ! -d "$BUNDLE_DIR/wheels" ]; then
        echo "[FAIL] --offline requested but wheels/ folder is missing from this bundle." >&2
        exit 1
    fi
    # Resolve the host platform tag dir (best-effort).
    plat="$(uname -s 2>/dev/null || echo unknown)"
    arch="$(uname -m 2>/dev/null || echo unknown)"
    case "$plat:$arch" in
        Darwin:arm64)   WHEEL_DIR="$BUNDLE_DIR/wheels/macos-arm64" ;;
        Darwin:x86_64)  WHEEL_DIR="$BUNDLE_DIR/wheels/macos-x86_64" ;;
        Linux:x86_64)   WHEEL_DIR="$BUNDLE_DIR/wheels/linux-x86_64" ;;
        Linux:aarch64)  WHEEL_DIR="$BUNDLE_DIR/wheels/linux-x86_64" ;; # fallback
        *)              WHEEL_DIR="$BUNDLE_DIR/wheels/linux-x86_64" ;;
    esac
    if [ ! -d "$WHEEL_DIR" ]; then
        echo "[FAIL] No wheels available for $plat:$arch. Tried $WHEEL_DIR" >&2
        exit 1
    fi
    PIP_ARGS+=(--no-index --find-links "$WHEEL_DIR")
    echo "[INFO] Wheel index: $WHEEL_DIR"
fi

for req in @@REQ_FILES@@; do
    if [ -f "$BUNDLE_DIR/$req" ]; then
        echo "[INFO] pip install -r $req"
        if [ "${#PIP_ARGS[@]}" -gt 0 ]; then
            "$VENV_PY" -m pip install "${PIP_ARGS[@]}" -r "$BUNDLE_DIR/$req"
        else
            "$VENV_PY" -m pip install -r "$BUNDLE_DIR/$req"
        fi
    else
        echo "[WARN] requirements file missing: $req"
    fi
done

# Bootstrap config + data dir and run import checks
"$VENV_PY" "$BUNDLE_DIR/setup/install.py" \
    --root "$BUNDLE_DIR" \
    --module "@@MODULE_KEY@@" \
    --python "$VENV_PY" \
    --skip-venv \
    --verify-only

echo ""
echo "[ OK ] Installation complete."
echo ""
echo "  Project root: $BUNDLE_DIR"
echo "  To launch:"
echo "    @@LAUNCH_HINT@@"
echo "  To uninstall later:"
echo "    bash uninstall.sh"
echo ""
"""


INSTALL_COMMAND_TEMPLATE = r"""#!/usr/bin/env bash
# DbManagementTool — macOS Finder installer (double-click).
#
# Keeps the Terminal window open after install so the user sees the
# summary. Forwards any extra arguments to install.sh.
set -eu
BUNDLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$BUNDLE_DIR"

bash "$BUNDLE_DIR/install.sh" "$@"
rc=$?

echo ""
echo "Press Return to close this window..."
read -r _ || true
exit "$rc"
"""


INSTALL_BAT_TEMPLATE = r"""@echo off
REM DbManagementTool installer (bundled by shipper) - Windows launcher.
REM
REM Usage:
REM   install.bat                  -- online install
REM   install.bat --offline        -- use bundled wheels\
REM   install.bat --python C:\path\to\python.exe

setlocal enableextensions enabledelayedexpansion

set "BUNDLE_DIR=%~dp0"
if "%BUNDLE_DIR:~-1%"=="\" set "BUNDLE_DIR=%BUNDLE_DIR:~0,-1%"

set "USE_OFFLINE=0"
set "PYTHON_BIN="

:parseargs
if "%~1"=="" goto endparse
if /I "%~1"=="--offline" (set "USE_OFFLINE=1" & shift & goto parseargs)
if /I "%~1"=="--python" (set "PYTHON_BIN=%~2" & shift & shift & goto parseargs)
shift
goto parseargs
:endparse

if "%PYTHON_BIN%"=="" (
    where py >nul 2>nul
    if !ERRORLEVEL!==0 (
        for %%V in (3.12 3.11 3.10) do (
            if "!PYTHON_BIN!"=="" (
                py -%%V -c "import sys" >nul 2>nul
                if !ERRORLEVEL!==0 set "PYTHON_BIN=py -%%V"
            )
        )
    )
    if "!PYTHON_BIN!"=="" (
        where python >nul 2>nul
        if !ERRORLEVEL!==0 set "PYTHON_BIN=python"
    )
)

if "%PYTHON_BIN%"=="" (
    echo [FAIL] Python ^>= @@MIN_PY_MAJOR@@.@@MIN_PY_MINOR@@ not found.
    echo Install Python from https://www.python.org/downloads/ and re-run this script.
    pause
    exit /b 1
)

echo [INFO] Python:  %PYTHON_BIN%
echo [INFO] Bundle:  %BUNDLE_DIR%
echo [INFO] Module:  @@MODULE_KEY@@
if "%USE_OFFLINE%"=="1" echo [INFO] Mode:    offline (using bundled wheels\)

set "VENV_DIR=%BUNDLE_DIR%\.venv"
if not exist "%VENV_DIR%" (
    echo [INFO] Creating virtual environment at %VENV_DIR%
    %PYTHON_BIN% -m venv "%VENV_DIR%"
    if errorlevel 1 (echo [FAIL] venv creation failed & pause & exit /b 1)
)

set "VENV_PY=%VENV_DIR%\Scripts\python.exe"
if not exist "%VENV_PY%" (echo [FAIL] %VENV_PY% missing & pause & exit /b 1)

echo [INFO] Upgrading pip / setuptools / wheel
"%VENV_PY%" -m pip install --upgrade pip setuptools wheel >nul
if errorlevel 1 (echo [FAIL] pip bootstrap failed & pause & exit /b 1)

set "PIP_EXTRA="
set "WHEEL_DIR="
if "%USE_OFFLINE%"=="1" (
    if not exist "%BUNDLE_DIR%\wheels\windows-x86_64" (
        echo [FAIL] --offline requested but wheels\windows-x86_64\ is missing.
        pause
        exit /b 1
    )
    set "WHEEL_DIR=%BUNDLE_DIR%\wheels\windows-x86_64"
    set "PIP_EXTRA=--no-index --find-links"
)

for %%R in (@@REQ_FILES@@) do (
    if exist "%BUNDLE_DIR%\%%~R" (
        echo [INFO] pip install -r %%~R
        if "%USE_OFFLINE%"=="1" (
            "%VENV_PY%" -m pip install %PIP_EXTRA% "%WHEEL_DIR%" -r "%BUNDLE_DIR%\%%~R"
        ) else (
            "%VENV_PY%" -m pip install -r "%BUNDLE_DIR%\%%~R"
        )
        if errorlevel 1 (echo [FAIL] pip install -r %%~R failed & pause & exit /b 1)
    ) else (
        echo [WARN] requirements file missing: %%~R
    )
)

echo [INFO] Running setup\install.py --verify-only
"%VENV_PY%" "%BUNDLE_DIR%\setup\install.py" --root "%BUNDLE_DIR%" --module "@@MODULE_KEY@@" --python "%VENV_PY%" --skip-venv --verify-only

echo.
echo [ OK ] Installation complete.
echo.
echo   Project root: %BUNDLE_DIR%
echo   To launch:
echo     @@LAUNCH_HINT_WIN@@
echo   To uninstall later:
echo     uninstall.bat
echo.
pause
"""


README_INSTALL_TEMPLATE = """\
# @@TITLE@@ — Install Guide

**Version:** @@VERSION@@
**Module:** `@@MODULE_KEY@@` — @@DESCRIPTION@@
**Bundle mode:** @@MODE_DESC@@

## Requirements

* Python **>= @@MIN_PY_MAJOR@@.@@MIN_PY_MINOR@@** (the strict minimum — older
  versions are not supported)
* A working `pip` (bundled with the official Python distribution)
* Internet access — *unless* you downloaded the offline bundle
* Tkinter (bundled with `python.org` builds; on Linux: `sudo apt install python3-tk`)

## macOS

> The `install.command` and `uninstall.command` scripts are unsigned.
> When you first double-click them, macOS Gatekeeper will warn:
> _"<file> can't be opened because it is from an unidentified developer."_
>
> Workaround (one-time):
>
> 1. **Finder**: right-click `install.command` -> **Open** -> **Open** in the dialog.
> 2. *Or* from Terminal:
>    ```bash
>    xattr -d com.apple.quarantine install.command
>    ```
>
> After that, double-click works normally.

Then:

1. Double-click **`install.command`** in this folder.
2. A Terminal window opens, creates `.venv/`, installs the
   dependencies, and verifies imports.
3. Press Return to close the window when done.

## Linux

```bash
bash install.sh                # online install
bash install.sh --offline      # use bundled wheels (if this is an offline bundle)
```

If your distro ships Tkinter separately, install it first:

```bash
# Debian / Ubuntu
sudo apt install python3-tk
# Fedora / RHEL
sudo dnf install python3-tkinter
```

## Windows

1. Make sure Python **>= {MIN_PY_MAJOR}.{MIN_PY_MINOR}** is installed
   from <https://www.python.org/downloads/>. During setup, tick
   **"Add python.exe to PATH"** and **"tcl/tk and IDLE"**.
2. Double-click **`install.bat`** in this folder.
3. The script creates `.venv\\`, installs the dependencies, and pauses
   so you can read the summary.

## Offline install

@@OFFLINE_BLOCK@@

## Launching

After install completes:

@@LAUNCH_BLOCK@@

## Uninstalling

```bash
# macOS / Linux
bash uninstall.sh
# Windows
uninstall.bat
```

Pass `--purge` to also delete this folder including `config.ini` and
`properties.ini`. Without `--purge` user data (`~/.dbassistant/`) and
caches are removed but the bundle itself is kept.

## Module contents

This bundle includes:

@@CONTENTS_BLOCK@@

## Troubleshooting

* **`pip` cannot reach the index** — use the `--offline` flag (offline
  bundles only) or set `HTTP_PROXY` / `HTTPS_PROXY`.
* **`tkinter` not found** — reinstall Python from python.org with the
  optional Tcl/Tk feature enabled.
* **macOS "killed: 9"** — Gatekeeper quarantine. See the Gatekeeper note
  above.
* **Permission denied creating `~/.dbassistant`** — make sure your home
  directory is writable; set `DBASSISTANT_HOME=/some/writable/path` to
  relocate the user-data root.
"""


# ---------------------------------------------------------------------------
# Build pipeline
# ---------------------------------------------------------------------------


@dataclass
class BuildPlan:
    bundle: ModuleBundle
    version: str
    offline: bool
    output_dir: Path
    requirement_files: list[Path]

    @property
    def bundle_name(self) -> str:
        suffix = "-offline" if self.offline else ""
        return f"dbassistant-{self.bundle.key}-{self.version}{suffix}"

    @property
    def zip_path(self) -> Path:
        return self.output_dir / f"{self.bundle_name}.zip"


def _plan_build(args: argparse.Namespace) -> BuildPlan:
    bundle = get_module(args.module)
    version = (args.version or _read_version()).strip()
    output_dir = (args.output or (_ROOT / "dist")).resolve()
    req_files = _expand_requirements(bundle.requirement_files)
    if not args.no_optional:
        req_files += _expand_requirements(bundle.optional_requirement_files)
    # Dedup preserving order
    seen: set[Path] = set()
    deduped: list[Path] = []
    for f in req_files:
        if f not in seen:
            seen.add(f)
            deduped.append(f)
    return BuildPlan(
        bundle=bundle,
        version=version,
        offline=args.offline,
        output_dir=output_dir,
        requirement_files=deduped,
    )


def _stage_source(plan: BuildPlan, staging: Path) -> None:
    """Copy the source paths required by ``plan.bundle`` into ``staging``."""
    paths_to_copy = list(plan.bundle.required_paths)

    # Every bundle also needs:
    #   - setup/install.py (used by install.sh --verify-only)
    #   - setup/module_manifest.py (imported by install.py)
    #   - setup/uninstall.py + launchers
    #   - VERSION
    extras: list[str] = [
        "VERSION",
        "setup/install.py",
        "setup/module_manifest.py",
        "setup/uninstall.py",
        "setup/__init__.py",
        "uninstall.sh",
        "uninstall.command",
        "uninstall.bat",
    ]

    # Per-module: include the entry-point script when the bundle is "full"
    if plan.bundle.key == "full":
        extras += [
            "dbtool.py",
            "conDbUi.py",
            "api.py",
            "run.sh",
            "run.bat",
            "install.bat",
        ]

    # Also include EACH expanded requirement file, even when the relative
    # path lives under a module folder (e.g. ai_query/requirements.txt).
    for req in plan.requirement_files:
        try:
            extras.append(str(req.relative_to(_ROOT)))
        except ValueError:
            continue

    total_files = 0
    for rel in paths_to_copy + extras:
        src = (_ROOT / rel).resolve()
        # Skip anything outside the project root or missing.
        try:
            src.relative_to(_ROOT)
        except ValueError:
            _warn(f"refusing to ship path outside project: {rel}")
            continue
        if not src.exists():
            _warn(f"missing source path: {rel}")
            continue
        dst = staging / rel.rstrip("/")
        count = _copy_filtered(src, dst)
        total_files += count
        _ok(f"{rel}  ({count} files)")

    # Move config / properties / threshold .ini.example to bundle root as
    # *.example so the receiver knows what to copy. We also seed
    # config.ini / properties.ini directly so the install can run.
    _seed_config_examples(staging, plan)

    _info(f"total source files staged: {total_files}")


def _seed_config_examples(staging: Path, plan: BuildPlan) -> None:
    """Promote example INI files to the bundle root and create seed copies."""
    pairs: list[tuple[Path, Path, str]] = [
        (
            _ROOT / "common" / "config" / "config.ini.example",
            staging / "config.ini",
            "config.ini",
        ),
        (
            _ROOT / "common" / "config" / "properties.ini.example",
            staging / "properties.ini",
            "properties.ini",
        ),
    ]
    if plan.bundle.key in ("full", "monitor"):
        pairs.append(
            (
                _ROOT / "monitoring" / "monitor_thresholds.ini.example",
                staging / "monitoring" / "monitor_thresholds.ini",
                "monitoring/monitor_thresholds.ini",
            )
        )
        pairs.append(
            (
                _ROOT / "monitoring" / "monitor_config.ini.example",
                staging / "monitoring" / "monitor_config.ini",
                "monitoring/monitor_config.ini",
            )
        )
    if plan.bundle.key in ("full", "ai"):
        pairs.append(
            (
                _ROOT / "ai_query" / "config.ini.example",
                staging / "ai_query" / "config.ini",
                "ai_query/config.ini",
            )
        )
    if plan.bundle.key in ("full", "migrator"):
        pairs.append(
            (
                _ROOT / "schema_converter" / "config.ini.example",
                staging / "schema_converter" / "config.ini",
                "schema_converter/config.ini",
            )
        )

    for src, dst, label in pairs:
        if not src.exists():
            _warn(f"example missing, skipping seed: {src}")
            continue
        if dst.exists():
            continue  # already copied by module's required_paths
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        _ok(f"seeded {label} from {src.name}")


def _render(template: str, mapping: dict[str, str]) -> str:
    """Substitute ``@@KEY@@`` placeholders in ``template``.

    Plain string replace avoids the brace-collision pain that
    ``str.format`` causes when the template contains literal bash/batch
    syntax such as ``${BASH_SOURCE[0]}`` or ``${PIP_ARGS[@]}``.
    """
    out = template
    for k, v in mapping.items():
        out = out.replace(f"@@{k}@@", str(v))
    return out


def _emit_installers(plan: BuildPlan, staging: Path) -> None:
    """Write the bundled install.sh / install.command / install.bat."""
    req_paths_rel = [str(p.relative_to(_ROOT)) for p in plan.requirement_files]
    req_files_sh = " ".join(f'"{p}"' for p in req_paths_rel)
    req_files_bat = " ".join(f'"{p.replace("/", chr(92))}"' for p in req_paths_rel)

    launch_hint, launch_hint_win = _launch_hints(plan.bundle)
    base_map = {
        "MODULE_KEY": plan.bundle.key,
        "MIN_PY_MAJOR": str(MIN_PYTHON[0]),
        "MIN_PY_MINOR": str(MIN_PYTHON[1]),
        "LAUNCH_HINT": launch_hint,
        "LAUNCH_HINT_WIN": launch_hint_win,
    }
    sh_map = dict(base_map, REQ_FILES=req_files_sh)
    bat_map = dict(base_map, REQ_FILES=req_files_bat)

    (staging / "install.sh").write_text(_render(INSTALL_SH_TEMPLATE, sh_map), encoding="utf-8")
    (staging / "install.sh").chmod(0o755)

    (staging / "install.command").write_text(INSTALL_COMMAND_TEMPLATE, encoding="utf-8")
    (staging / "install.command").chmod(0o755)

    (staging / "install.bat").write_text(_render(INSTALL_BAT_TEMPLATE, bat_map), encoding="utf-8")

    _ok("install.sh / install.command / install.bat")


def _launch_hints(bundle: ModuleBundle) -> tuple[str, str]:
    if bundle.key == "full":
        return ("bash run.sh", "run.bat")
    if bundle.ui_examples:
        return (bundle.ui_examples[0], bundle.ui_examples[0])
    if bundle.cli_examples:
        return (bundle.cli_examples[0], bundle.cli_examples[0])
    return ("python -m " + bundle.key, "python -m " + bundle.key)


def _emit_readme(plan: BuildPlan, staging: Path, wheel_summary: list[str] | None) -> None:
    contents_lines = []
    for rel in plan.bundle.required_paths:
        contents_lines.append(f"* `{rel}`")
    contents_lines.append("* `setup/install.py` (verification helper)")
    contents_lines.append("* `setup/module_manifest.py` (module registry)")
    contents_lines.append("* `setup/uninstall.py` + `uninstall.{sh,command,bat}` (uninstaller)")
    contents_lines.append("* `VERSION`")
    contents_lines.append("* `config.ini`, `properties.ini` (seeded from examples)")
    contents_block = "\n".join(contents_lines)

    if plan.bundle.cli_examples or plan.bundle.ui_examples:
        launch_lines: list[str] = []
        for ex in plan.bundle.ui_examples:
            launch_lines.append(f"  {ex}    # UI")
        for ex in plan.bundle.cli_examples:
            launch_lines.append(f"  {ex}    # CLI")
        launch_block = "\n".join(launch_lines)
    else:
        launch_block = "  python dbtool.py --help"

    if plan.offline:
        wheel_list = "\n".join(f"* `{w}`" for w in (wheel_summary or []))
        mode_desc = "**offline** — wheels are bundled under `wheels/`"
        offline_block = (
            "This is an *offline* bundle. Wheels for the following targets "
            "are included under `wheels/`:\n\n"
            f"{wheel_list}\n\n"
            "To use them, pass `--offline` to the installer:\n\n"
            "```bash\n"
            "bash install.sh --offline    # macOS / Linux\n"
            "install.bat --offline        # Windows\n"
            "```\n\n"
            "The installer auto-detects your platform and points pip at the "
            "matching wheel directory. If no matching wheels are present for "
            "your platform, omit `--offline` to fall back to the online index."
        )
    else:
        mode_desc = "**lean** — receivers run `pip install` from their own index"
        offline_block = (
            "This is the *lean* bundle (default). Run `bash install.sh` (no "
            "extra flags) and the installer will `pip install` the dependencies "
            "from PyPI. If you need a self-contained bundle that works without "
            "internet, rebuild with `python setup/shipper.py --offline`."
        )

    text = _render(
        README_INSTALL_TEMPLATE,
        {
            "TITLE": plan.bundle.title,
            "VERSION": plan.version,
            "MODULE_KEY": plan.bundle.key,
            "DESCRIPTION": plan.bundle.description,
            "MODE_DESC": mode_desc,
            "MIN_PY_MAJOR": str(MIN_PYTHON[0]),
            "MIN_PY_MINOR": str(MIN_PYTHON[1]),
            "OFFLINE_BLOCK": offline_block,
            "LAUNCH_BLOCK": launch_block,
            "CONTENTS_BLOCK": contents_block,
        },
    )
    (staging / "README_INSTALL.md").write_text(text, encoding="utf-8")
    _ok("README_INSTALL.md")


# ---------------------------------------------------------------------------
# Offline wheel downloader
# ---------------------------------------------------------------------------


def _download_wheels(plan: BuildPlan, staging: Path) -> list[str]:
    """Download wheels for every WHEEL_TARGETS entry. Returns target labels
    that had at least one wheel successfully downloaded."""
    if not plan.requirement_files:
        _warn("no requirements to download wheels for")
        return []

    wheels_root = staging / "wheels"
    wheels_root.mkdir(parents=True, exist_ok=True)

    successful: list[str] = []
    for label, pip_platforms in WHEEL_TARGETS:
        target_dir = wheels_root / label
        target_dir.mkdir(parents=True, exist_ok=True)
        platform_args: list[str] = []
        for p in pip_platforms:
            platform_args += ["--platform", p]

        any_success = False
        for py_ver in WHEEL_PYTHON_VERSIONS:
            _info(f"downloading wheels for {label} python {py_ver}")
            for req in plan.requirement_files:
                cmd = [
                    sys.executable,
                    "-m",
                    "pip",
                    "download",
                    "--dest",
                    str(target_dir),
                    "--python-version",
                    py_ver,
                    "--only-binary=:all:",
                    "--implementation",
                    "cp",
                    *platform_args,
                    "-r",
                    str(req),
                ]
                r = subprocess.run(cmd, capture_output=True, text=True)
                rel = req.relative_to(_ROOT)
                if r.returncode == 0:
                    _ok(f"  {label} py{py_ver}  {rel}")
                    any_success = True
                else:
                    last = (
                        r.stderr.strip().splitlines()[-1]
                        if r.stderr.strip()
                        else "unknown error"
                    )
                    _warn(f"  {label} py{py_ver}  {rel}: {last}")

            # Best-effort fetch of py3-none-any wheels (pure-python deps
            # the strict --implementation cp pass can refuse).
            for req in plan.requirement_files:
                cmd = [
                    sys.executable,
                    "-m",
                    "pip",
                    "download",
                    "--dest",
                    str(target_dir),
                    "--python-version",
                    py_ver,
                    "--only-binary=:all:",
                    "-r",
                    str(req),
                ]
                subprocess.run(cmd, capture_output=True, text=True)

        if any_success:
            successful.append(label)

    size = _dir_size(wheels_root)
    _info(f"wheels/ total size: {_human_size(size)}")
    return successful


# ---------------------------------------------------------------------------
# Zip
# ---------------------------------------------------------------------------


def _zip_bundle(plan: BuildPlan, staging: Path) -> int:
    plan.output_dir.mkdir(parents=True, exist_ok=True)
    if plan.zip_path.exists():
        plan.zip_path.unlink()

    file_count = 0
    with zipfile.ZipFile(plan.zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for root, _dirs, files in os.walk(staging):
            for f in files:
                fp = Path(root) / f
                arcname = Path(plan.bundle_name) / fp.relative_to(staging)
                # Preserve exec bits via external_attr.
                zinfo = zipfile.ZipInfo.from_file(fp, arcname=str(arcname))
                mode = fp.stat().st_mode
                zinfo.external_attr = (mode & 0xFFFF) << 16
                zinfo.compress_type = zipfile.ZIP_DEFLATED
                with fp.open("rb") as src:
                    zf.writestr(zinfo, src.read())
                file_count += 1
    return file_count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="dbassistant-shipper",
        description=(
            "Package DbManagementTool into a distributable ZIP that the "
            "receiver can install by double-clicking install.command "
            "(macOS), install.bat (Windows) or install.sh (Linux)."
        ),
    )
    p.add_argument(
        "--module",
        default="full",
        choices=all_module_keys(),
        help="Which bundle to build (default: full).",
    )
    p.add_argument(
        "--offline",
        action="store_true",
        help=(
            "Bundle wheel files for macOS+Linux+Windows so the receiver can "
            "install without internet access. Significantly larger output."
        ),
    )
    p.add_argument(
        "--no-optional",
        action="store_true",
        help="Skip optional requirement files (cloud SDKs on core-only, etc.).",
    )
    p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory for the zip (default: ./dist/).",
    )
    p.add_argument(
        "--version",
        default=None,
        help="Override the version baked into the bundle name (default: VERSION file).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if sys.version_info < (3, 9):
        print("[FAIL] shipper requires Python >= 3.9 (current: %d.%d)" % sys.version_info[:2])
        return 1

    plan = _plan_build(args)

    print()
    print(C.b("DbManagementTool — Shipper"))
    print()
    _info(f"version  : {plan.version}")
    _info(f"module   : {plan.bundle.key} -> {plan.bundle.title}")
    _info(f"mode     : {'offline (with wheels)' if plan.offline else 'lean (online install)'}")
    _info(f"output   : {plan.output_dir}")
    _info(f"req files: {len(plan.requirement_files)}")
    for f in plan.requirement_files:
        print(f"           - {f.relative_to(_ROOT)}")

    start = time.time()
    with tempfile.TemporaryDirectory(prefix="dbassistant_ship_") as td:
        staging = Path(td) / plan.bundle_name
        staging.mkdir(parents=True)

        _section("Staging source")
        _stage_source(plan, staging)

        _section("Generating installers")
        _emit_installers(plan, staging)

        wheel_summary: list[str] = []
        if plan.offline:
            _section("Downloading wheels")
            wheel_summary = _download_wheels(plan, staging)
            if not wheel_summary:
                _fail("no wheel targets succeeded; the offline bundle would be useless.")
                return 2

        _section("Writing README_INSTALL.md")
        _emit_readme(plan, staging, wheel_summary)

        _section("Compressing zip")
        files = _zip_bundle(plan, staging)
        size = plan.zip_path.stat().st_size
        _ok(f"{plan.zip_path}  ({files} files, {_human_size(size)})")

    elapsed = time.time() - start

    print()
    print(C.green(C.b("Bundle ready.")))
    _info(f"file       : {plan.zip_path}")
    _info(f"size       : {_human_size(plan.zip_path.stat().st_size)}")
    _info(f"elapsed    : {elapsed:.1f}s")
    print()
    print(C.b("Receiver instructions"))
    print(C.dim("  1. Unzip the file."))
    print(C.dim("  2. macOS: double-click install.command   (right-click -> Open the first time, see README_INSTALL.md)"))
    print(C.dim("  3. Windows: double-click install.bat"))
    print(C.dim("  4. Linux: bash install.sh"))
    if plan.offline:
        print(C.dim("  5. Pass --offline to use the bundled wheels/."))
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
