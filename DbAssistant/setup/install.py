#!/usr/bin/env python3
"""
Cross-platform installer for DbManagementTool.

Used by setup/install.sh (Linux/macOS) and setup/install.bat (Windows).
Requires Python 3.10+ only — creates venv, installs pip deps, config files, verifies imports.

Examples:
    python setup/install.py
    python setup/install.py --module migrator
    python setup/install.py --module full --no-optional
    python setup/install.py --python .venv/bin/python --skip-venv
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Allow running as script without package install
_SETUP_DIR = Path(__file__).resolve().parent
_ROOT = _SETUP_DIR.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from setup.module_manifest import ModuleBundle, get_module  # noqa: E402


class C:
    OK = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    INFO = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if sys.platform == "win32":
        return bool(os.environ.get("WT_SESSION") or os.environ.get("TERM"))
    return sys.stdout.isatty()


def ok(msg: str) -> None:
    p = f"{C.OK}[ OK ]{C.END} " if _supports_color() else "[ OK ] "
    print(f"{p}{msg}")


def warn(msg: str) -> None:
    p = f"{C.WARN}[WARN]{C.END} " if _supports_color() else "[WARN] "
    print(f"{p}{msg}")


def fail(msg: str) -> None:
    p = f"{C.FAIL}[FAIL]{C.END} " if _supports_color() else "[FAIL] "
    print(f"{p}{msg}")


def info(msg: str) -> None:
    p = f"{C.INFO}[INFO]{C.END} " if _supports_color() else "[INFO] "
    print(f"{p}{msg}")


def _target_is_venv(python: Path) -> bool:
    """True when *python* lives inside a venv (has a sibling pyvenv.cfg).

    Layout: <venv>/bin/python (POSIX) or <venv>\\Scripts\\python.exe (Windows),
    so the marker file is two levels up from the interpreter.
    """
    try:
        return (python.parent.parent / "pyvenv.cfg").exists()
    except Exception:
        return False


def _pip_env(python: Path) -> dict[str, str]:
    """Environment for pip subprocesses.

    When installing into the project venv we neutralise host settings that
    would otherwise redirect installs to the user site-packages (PIP_USER=1)
    or mask the venv with external packages (PYTHONPATH). This only affects
    the pip child process — the parent shell and global Python are untouched.
    For non-venv targets (e.g. --skip-venv into system Python) we leave the
    user's environment as-is.
    """
    env = os.environ.copy()
    if _target_is_venv(python):
        env["PIP_USER"] = "0"
        env.pop("PYTHONPATH", None)
    return env


def run_pip(python: Path, args: list[str], *, quiet: bool = True) -> bool:
    cmd = [str(python), "-m", "pip", *args]
    env = _pip_env(python)
    kw = {"check": False, "capture_output": quiet, "text": True, "env": env}
    r = subprocess.run(cmd, **kw)
    if r.returncode == 0:
        return True
    if quiet and "--quiet" in args:
        args2 = [a for a in args if a != "--quiet"]
        r2 = subprocess.run([str(python), "-m", "pip", *args2], check=False, env=env)
        return r2.returncode == 0
    return False


def create_venv(base_python: Path, venv_dir: Path) -> Path | None:
    info(f"Creating virtual environment at {venv_dir}/")
    r = subprocess.run([str(base_python), "-m", "venv", str(venv_dir)], check=False)
    if r.returncode != 0:
        fail("Failed to create .venv — install python3-venv (Linux) or use --skip-venv")
        return None
    if sys.platform == "win32":
        py = venv_dir / "Scripts" / "python.exe"
    else:
        py = venv_dir / "bin" / "python"
    if not py.exists():
        fail(f"venv python not found at {py}")
        return None
    ok(f"Virtual environment ready: {py}")
    run_pip(py, ["install", "--upgrade", "pip", "setuptools", "wheel"], quiet=True)
    return py


def resolve_python(explicit: str | None, root: Path, skip_venv: bool) -> Path:
    # NOTE: do NOT use Path.resolve() here — it follows symlinks, and a venv's
    # bin/python is a symlink to the base interpreter. Running the *resolved*
    # path deactivates the venv (sys.prefix becomes the base prefix), so pip
    # would install into the system/user site instead of the venv. We only
    # make the path absolute and keep the venv launcher intact.
    if explicit:
        return Path(os.path.abspath(os.path.expanduser(explicit)))
    if skip_venv:
        return Path(sys.executable)
    venv = root / ".venv"
    if sys.platform == "win32":
        candidate = venv / "Scripts" / "python.exe"
    else:
        candidate = venv / "bin" / "python"
    if candidate.exists():
        ok(f"Using existing venv: {candidate}")
        return candidate
    created = create_venv(Path(sys.executable), venv)
    if created:
        return created
    warn("Falling back to current Python interpreter.")
    return Path(sys.executable).resolve()


def copy_config_examples(root: Path) -> None:
    examples = [
        (root / "common" / "config" / "config.ini.example", root / "config.ini"),
        (root / "common" / "config" / "properties.ini.example", root / "properties.ini"),
    ]
    for src, dst in examples:
        if dst.exists():
            ok(f"{dst.name} already exists")
            continue
        if src.exists():
            shutil.copy(src, dst)
            ok(f"Created {dst.name} from example")
        else:
            warn(f"Example missing: {src}")


def ensure_data_dir(root: Path) -> None:
    try:
        sys.path.insert(0, str(root))
        from common import paths as _paths
        from common.layout_migration import migrate_if_needed

        home = _paths.ensure_layout()
        result = migrate_if_needed()
        if result.get("ok"):
            ok(f"Data directory ready: {home} ({result.get('status')})")
        else:
            warn(f"Layout migration reported issue: {result.get('message')}")
    except Exception as exc:
        fallback = Path.home() / ".dbassistant"
        fallback.mkdir(parents=True, exist_ok=True)
        warn(f"Using default data dir {fallback} ({exc})")


def verify_paths(bundle: ModuleBundle, root: Path) -> list[str]:
    missing = []
    for rel in bundle.required_paths:
        p = root / rel.rstrip("/")
        if not p.exists():
            missing.append(rel)
    return missing


def install_requirements(
    python: Path, root: Path, bundle: ModuleBundle, *, no_optional: bool
) -> list[str]:
    failed: list[str] = []
    seen: set[str] = set()
    files: list[str] = list(bundle.requirement_files)
    if not no_optional:
        files.extend(bundle.optional_requirement_files)

    for rel in files:
        path = (root / rel).resolve()
        key = str(path)
        if key in seen or not path.exists():
            if not path.exists():
                warn(f"Requirements file not found (skipped): {rel}")
            continue
        seen.add(key)
        info(f"Installing from {rel} ...")
        if run_pip(python, ["install", "-r", str(path)], quiet=False):
            ok(f"Installed: {rel}")
        else:
            failed.append(rel)
            fail(f"pip install failed: {rel}")
    return failed


# Tkinter cannot be pip-installed — it is a system/interpreter-level package.
# install.sh / install.bat attempt the version-matched install; here we verify
# and, if missing, point at the correct per-platform command.
def _tkinter_hint() -> str:
    minor = sys.version_info[1]
    return (
        "system package (no pip) — "
        f"macOS/brew: brew install python-tk@3.{minor}; "
        f"Debian/Ubuntu: sudo apt-get install python3.{minor}-tk (or python3-tk); "
        "Fedora/RHEL: sudo dnf install python3-tkinter; Arch: sudo pacman -S tk; "
        "openSUSE: sudo zypper install python3-tk; "
        "Windows: reinstall python.org build with 'tcl/tk and IDLE'"
    )


_TKINTER_HINT = _tkinter_hint()


def _can_import(python: Path, mod: str, env: dict[str, str]) -> tuple[bool, str]:
    r = subprocess.run(
        [str(python), "-c", f"import {mod}"],
        env=env,
        capture_output=True,
        text=True,
    )
    return r.returncode == 0, r.stderr.strip()


def verify_imports(
    python: Path, bundle: ModuleBundle, *, no_optional: bool = False
) -> tuple[list[str], list[str]]:
    """Single authoritative post-install verification.

    Returns (critical_issues, warnings). Covers core runtime dependencies,
    tkinter (system package), per-module package importability, the dbtool
    CLI, and optional cloud SDKs. This is the one place imports are checked —
    the shell shims no longer duplicate it.
    """
    # (import_expression, remediation) — failures here are CRITICAL.
    critical_checks: list[tuple[str, str]] = [
        ("cryptography", "pip install cryptography"),
        ("dotenv", "pip install python-dotenv"),
        ("psutil", "pip install psutil"),
        ("mysql.connector", "pip install mysql-connector-python"),
        ("psycopg2", "pip install psycopg2-binary"),
        ("tkinter", _TKINTER_HINT),
        ("common.config_loader", "ensure the project root is on PYTHONPATH"),
    ]

    key = bundle.key
    if key in ("full", "migrator"):
        critical_checks.append(("schema_converter", "schema_converter module missing"))
    if key in ("full", "ai"):
        critical_checks.append(("ai_query", "ai_query module missing"))
    if key in ("full", "monitor"):
        critical_checks.append(("monitoring", "monitoring module missing"))

    # Optional cloud SDKs — failures here are WARNINGS, only for cloud-capable
    # bundles and only when optional packages were not skipped.
    cloud_checks: list[tuple[str, str]] = []
    if not no_optional and key in ("full", "monitor"):
        cloud_checks = [
            ("boto3", "pip install boto3"),
            ("google.cloud.monitoring_v3", "pip install google-cloud-monitoring"),
            ("azure.identity", "pip install azure-identity"),
        ]

    issues: list[str] = []
    warnings: list[str] = []
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_ROOT)

    for mod, hint in critical_checks:
        good, err = _can_import(python, mod, env)
        if good:
            ok(f"import {mod}")
        else:
            issues.append(f"{mod} could not be imported  ({hint})")
            fail(f"{mod} — {err or 'import failed'}  ({hint})")

    # dbtool CLI smoke test (full bundle only)
    if key == "full":
        r = subprocess.run(
            [str(python), str(_ROOT / "dbtool.py"), "--help"],
            env=env,
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            ok("dbtool.py --help")
        else:
            issues.append(f"dbtool.py --help failed  ({r.stderr.strip() or 'error'})")
            fail(f"dbtool.py --help — {r.stderr.strip() or 'failed'}")

    for mod, hint in cloud_checks:
        good, _ = _can_import(python, mod, env)
        if good:
            ok(f"import {mod}")
        else:
            warnings.append(
                f"{mod} unavailable — cloud monitoring for this provider disabled  ({hint})"
            )
            warn(f"{mod} not importable — {hint}")

    return issues, warnings


def _write_posix(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    try:
        path.chmod(path.stat().st_mode | 0o111)
    except OSError:
        pass


def write_run_helpers(root: Path, python: Path) -> None:
    """Refresh ALL launcher scripts with the resolved venv python path.

    This is the single source of truth for launchers (POSIX + Windows); the
    install.sh / install.bat shims no longer generate them.
    """
    rel_py_posix = os.path.relpath(python, root).replace("\\", "/")
    rel_py_win = rel_py_posix.replace("/", "\\")

    # ── POSIX launchers ───────────────────────────────────────────────
    # setup/run.sh — main desktop UI, uses the resolved venv python directly.
    _write_posix(
        root / "setup" / "run.sh",
        f"""#!/usr/bin/env bash
# Launch DbManagementTool — generated by setup/install.py
ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/.." && pwd)"
cd "$ROOT"
exec "$ROOT/{rel_py_posix}" conDbUi.py "$@"
""",
    )

    # Root convenience wrapper → setup/run.sh
    _write_posix(
        root / "run.sh",
        """#!/usr/bin/env bash
# Generated by setup/install.py
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$ROOT/setup/run.sh" "$@"
""",
    )

    # Module shortcut wrappers under setup/scripts/ → canonical module scripts
    module_wrappers = {
        "run_monitor.sh": "monitoring/run_monitor.sh",
        "run_schema_converter.sh": "schema_converter/run_schema_converter.sh",
        "run_ai_query_assistant.sh": "ai_query/run_ai_query_assistant.sh",
    }
    for name, target in module_wrappers.items():
        _write_posix(
            root / "setup" / "scripts" / name,
            f"""#!/usr/bin/env bash
# Generated by setup/install.py
ROOT="$(cd "$(dirname "${{BASH_SOURCE[0]}}")/../.." && pwd)"
exec "$ROOT/{target}" "$@"
""",
        )
        # Make the canonical module script itself executable if present.
        canonical = root / target
        if canonical.exists():
            try:
                canonical.chmod(canonical.stat().st_mode | 0o111)
            except OSError:
                pass

    # ── Windows launchers ─────────────────────────────────────────────
    run_bat = root / "run.bat"
    run_bat.write_text(
        "@echo off\r\n"
        "REM Launch DbManagementTool GUI - generated by setup/install.py\r\n"
        "setlocal enableextensions\r\n"
        "set \"ROOT=%~dp0\"\r\n"
        "if \"%ROOT:~-1%\"==\"\\\" set \"ROOT=%ROOT:~0,-1%\"\r\n"
        f"set \"VENV_PY=%ROOT%\\{rel_py_win}\"\r\n"
        "if not exist \"%VENV_PY%\" (\r\n"
        "    echo [FAIL] %VENV_PY% not found.\r\n"
        "    echo Run install.bat to create the virtual environment first.\r\n"
        "    pause\r\n"
        "    exit /b 1\r\n"
        ")\r\n"
        "cd /d \"%ROOT%\"\r\n"
        "\"%VENV_PY%\" \"%ROOT%\\conDbUi.py\" %*\r\n"
        "set \"RC=%ERRORLEVEL%\"\r\n"
        "if not \"%RC%\"==\"0\" (\r\n"
        "    echo.\r\n"
        "    echo ERROR: DbManagementTool exited with code %RC%.\r\n"
        "    pause\r\n"
        ")\r\n"
        "exit /b %RC%\r\n",
        encoding="utf-8",
    )

    (root / "run_cli.bat").write_text(
        "@echo off\r\n"
        "REM DbManagementTool CLI - generated by setup/install.py\r\n"
        "setlocal enableextensions\r\n"
        "set \"ROOT=%~dp0\"\r\n"
        "if \"%ROOT:~-1%\"==\"\\\" set \"ROOT=%ROOT:~0,-1%\"\r\n"
        f"set \"VENV_PY=%ROOT%\\{rel_py_win}\"\r\n"
        "cd /d \"%ROOT%\"\r\n"
        "\"%VENV_PY%\" \"%ROOT%\\dbtool.py\" %*\r\n"
        "exit /b %ERRORLEVEL%\r\n",
        encoding="utf-8",
    )

    (root / "run_api.bat").write_text(
        "@echo off\r\n"
        "REM Start REST API server - generated by setup/install.py\r\n"
        "setlocal enableextensions\r\n"
        "set \"ROOT=%~dp0\"\r\n"
        "if \"%ROOT:~-1%\"==\"\\\" set \"ROOT=%ROOT:~0,-1%\"\r\n"
        f"set \"VENV_PY=%ROOT%\\{rel_py_win}\"\r\n"
        "cd /d \"%ROOT%\"\r\n"
        "echo Starting REST API on http://127.0.0.1:8000\r\n"
        "echo Docs: http://127.0.0.1:8000/docs\r\n"
        "\"%VENV_PY%\" \"%ROOT%\\dbtool.py\" api --host 127.0.0.1 --port 8000\r\n"
        "exit /b %ERRORLEVEL%\r\n",
        encoding="utf-8",
    )

    ok("Launchers written: run.sh, setup/run.sh, setup/scripts/*.sh, run.bat, run_cli.bat, run_api.bat")


def print_summary(
    issues: list[str], warnings: list[str], failed_req: list[str]
) -> None:
    """Cross-platform package/import summary (shared by install.sh & install.bat)."""
    bold = C.BOLD if _supports_color() else ""
    end = C.END if _supports_color() else ""
    print()
    print(f"{bold}── Package & import summary ──────────────────────────────────────{end}")
    if not issues and not warnings and not failed_req:
        ok("All Python packages installed and imports verified.")
        return

    if failed_req:
        fail(f"{len(failed_req)} requirement file(s) failed to install:")
        for rel in failed_req:
            print(f"    - {rel}")
    if issues:
        fail(f"{len(issues)} critical import issue(s):")
        for i in issues:
            print(f"    - {i}")
    if warnings:
        warn(f"{len(warnings)} optional warning(s):")
        for w in warnings:
            print(f"    - {w}")

    print()
    print(f"{bold}Remediation (run against the project venv):{end}")
    print("  • Re-run packages : .venv/bin/python -m pip install -r setup/requirements-<module>.txt")
    print("  • psycopg2 build  : .venv/bin/python -m pip install psycopg2-binary")
    print("  • Tkinter missing : system package — Linux: sudo apt-get install python3-tk")
    print("                      macOS: brew install python-tk; Windows: reinstall with tcl/tk")
    print("  • Oracle (thick)  : install Instant Client + pip install oracledb")


def print_next_steps(bundle: ModuleBundle, root: Path) -> None:
    print()
    print(f"{C.BOLD}Module installed: {bundle.title}{C.END}" if _supports_color() else f"Module installed: {bundle.title}")
    print(bundle.description)
    print()
    if bundle.key == "full":
        print("  bash run.sh              # Linux/macOS GUI")
        print("  run.bat                  # Windows GUI")
        print("  python dbtool.py --help  # CLI")
    else:
        for line in bundle.ui_examples:
            print(f"  {line}")
        for line in bundle.cli_examples:
            print(f"  {line}")
    print()
    print(f"  Project root: {root}")
    print("  Edit config.ini and properties.ini before connecting to databases.")
    print()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DbManagementTool installer")
    p.add_argument(
        "--module",
        default="full",
        choices=["full", "core", "migrator", "ai", "monitor"],
        help="Which bundle to install (default: full)",
    )
    p.add_argument("--root", type=Path, default=_ROOT, help="Project root directory")
    p.add_argument("--python", help="Python interpreter to use (venv or system)")
    p.add_argument(
        "--skip-venv",
        action="store_true",
        help="Do not create .venv; use --python or current interpreter",
    )
    p.add_argument(
        "--no-optional",
        action="store_true",
        help="Skip optional requirement files (e.g. extra cloud SDKs on core-only)",
    )
    p.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip pip install; only verify paths and imports",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    os.chdir(root)

    print()
    info(f"DbManagementTool installer — module={args.module}")
    info(f"Root: {root}")

    if sys.version_info < (3, 10):
        fail("Python 3.10+ required")
        return 1

    try:
        bundle = get_module(args.module)
    except ValueError as exc:
        fail(str(exc))
        return 1

    missing = verify_paths(bundle, root)
    if missing:
        fail("Missing paths for this module bundle (incomplete copy?):")
        for m in missing:
            print(f"    - {m}")
        info("See MODULES.md for what to ship with each module.")
        return 1
    ok("Bundle file layout OK")

    python = resolve_python(args.python, root, args.skip_venv)
    info(f"Target interpreter: {python}")

    failed_req: list[str] = []
    if not args.verify_only:
        failed_req = install_requirements(python, root, bundle, no_optional=args.no_optional)
        if failed_req:
            warn(f"{len(failed_req)} requirement file(s) had errors — see output above")

    copy_config_examples(root)
    ensure_data_dir(root)

    import_issues, import_warnings = verify_imports(
        python, bundle, no_optional=args.no_optional
    )
    write_run_helpers(root, python)

    print_summary(import_issues, import_warnings, failed_req)
    print_next_steps(bundle, root)

    if import_issues or failed_req:
        fail(
            f"{len(import_issues)} import issue(s), "
            f"{len(failed_req)} requirement failure(s)"
        )
        return 1
    ok("Installation complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
