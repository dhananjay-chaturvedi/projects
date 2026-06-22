#!/usr/bin/env python3
"""
DbManagementTool — Cross-platform uninstaller.

Behaviour
---------
* Always removes the user-data root (``~/.dbassistant/`` and any legacy
  variants), including encryption keys, saved connections, runtime logs,
  daemon pid/state and session files.
* Always removes project-local *generated* artefacts: ``.venv/``,
  ``__pycache__/``, ``.pytest_cache/``, ``logs/``, ``.DS_Store`` and
  compiled Python (``*.pyc`` / ``*.pyo``).
* On Linux, stops + disables + removes any ``dbtool-monitor`` /
  ``dbtool-api`` systemd unit files we installed. If removal fails for
  permission reasons, prints the exact commands the user should run as
  root.
* Prompts the user **once** (single confirmation): whether to also
  ``--purge`` the project source folder (which includes ``.ini`` files).
  Without ``--purge`` the source tree and ``.ini`` files are kept.

Designed to run with the *system* Python interpreter -- no third-party
dependencies. ``python3`` (>=3.9) on macOS/Linux, ``python`` on Windows.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Layout knowledge (kept inline so the uninstaller works even when the
# project source has already been partially removed).
# ---------------------------------------------------------------------------

USER_HOME = Path.home()

USER_DATA_DIRS = [
    USER_HOME / ".dbassistant",
    # Legacy directories from previous layouts. We always sweep these too
    # so an uninstall after an aborted migration leaves a truly clean home.
    USER_HOME / ".dbtool",
    USER_HOME / ".dbmanager",
    USER_HOME / ".dbtool.legacy",
    USER_HOME / ".dbmanager.legacy",
]

# Project-root generated artefacts (relative to ``PROJECT_ROOT``).
GENERATED_DIRS = [
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "logs",
    ".coverage_data",
]

GENERATED_FILES = [
    ".DS_Store",
    ".coverage",
    "coverage.xml",
]

# File-glob suffixes to recursively delete inside the project (excluding
# things in the dirs we already wipe wholesale).
GENERATED_GLOBS = ["*.pyc", "*.pyo"]

# systemd unit files this project installs.
SYSTEMD_UNITS = ["dbtool-monitor.service", "dbtool-api.service"]
SYSTEMD_DIR = Path("/etc/systemd/system")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_windows() -> bool:
    return sys.platform.startswith("win")


def _is_linux() -> bool:
    return sys.platform.startswith("linux")


class C:
    """ANSI color helpers, no-op on dumb terminals / Windows cmd."""

    _enabled = sys.stdout.isatty() and not _is_windows()

    @classmethod
    def _wrap(cls, code: str, text: str) -> str:
        if not cls._enabled:
            return text
        return f"\033[{code}m{text}\033[0m"

    @classmethod
    def b(cls, t: str) -> str:
        return cls._wrap("1", t)

    @classmethod
    def red(cls, t: str) -> str:
        return cls._wrap("31", t)

    @classmethod
    def green(cls, t: str) -> str:
        return cls._wrap("32", t)

    @classmethod
    def yellow(cls, t: str) -> str:
        return cls._wrap("33", t)

    @classmethod
    def dim(cls, t: str) -> str:
        return cls._wrap("2", t)


def _section(title: str) -> None:
    print()
    print(C.b("== " + title + " =="))


def _info(msg: str) -> None:
    print(f"  {msg}")


def _ok(msg: str) -> None:
    print(f"  {C.green('OK')}    {msg}")


def _skip(msg: str) -> None:
    print(f"  {C.dim('skip')}  {msg}")


def _warn(msg: str) -> None:
    print(f"  {C.yellow('warn')}  {msg}")


def _fail(msg: str) -> None:
    print(f"  {C.red('fail')}  {msg}")


def _human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:6.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def _dir_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    try:
        for root, _dirs, files in os.walk(path, onerror=lambda _e: None):
            for name in files:
                fp = Path(root) / name
                try:
                    total += fp.stat().st_size
                except OSError:
                    continue
    except OSError:
        pass
    return total


def _on_rm_error(func, path, exc_info):  # noqa: ANN001 — shutil.rmtree callback
    """Best-effort handler: make file writable then retry once."""
    try:
        Path(path).chmod(stat.S_IWRITE | stat.S_IREAD | stat.S_IWUSR | stat.S_IRUSR)
        func(path)
    except Exception:  # noqa: BLE001
        pass


def _rmtree_safe(path: Path) -> bool:
    """Remove ``path`` recursively if it exists. Returns True on success."""
    if not path.exists() and not path.is_symlink():
        return True
    try:
        shutil.rmtree(path, onerror=_on_rm_error)
        return not path.exists()
    except Exception as exc:  # noqa: BLE001
        _fail(f"could not remove {path}: {exc}")
        return False


def _unlink_safe(path: Path) -> bool:
    try:
        if path.is_symlink() or path.exists():
            path.unlink()
        return True
    except Exception as exc:  # noqa: BLE001
        _fail(f"could not remove {path}: {exc}")
        return False


# ---------------------------------------------------------------------------
# Daemon / process cleanup
# ---------------------------------------------------------------------------


def _read_daemon_pid() -> int | None:
    """Read the monitoring daemon pid, if a pid file exists."""
    candidates = [
        USER_HOME / ".dbassistant" / "runtime" / "daemon.pid",
        # Legacy locations
        USER_HOME / ".dbtool" / "daemon.pid",
        USER_HOME / ".dbmanager" / "runtime" / "daemon.pid",
    ]
    for c in candidates:
        if c.exists():
            try:
                txt = c.read_text().strip()
                if txt:
                    return int(txt)
            except (OSError, ValueError):
                continue
    return None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if _is_windows():
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return str(pid) in out.stdout
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:  # noqa: BLE001
        return False


def stop_daemon() -> None:
    pid = _read_daemon_pid()
    if pid is None:
        _skip("no daemon pid file found")
        return
    if not _pid_alive(pid):
        _skip(f"pid {pid} not running")
        return
    _info(f"stopping monitoring daemon (pid {pid})...")
    try:
        if _is_windows():
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], timeout=10)
        else:
            os.kill(pid, signal.SIGTERM)
        # Wait up to 10 seconds for graceful shutdown.
        for _ in range(20):
            time.sleep(0.5)
            if not _pid_alive(pid):
                break
        if _pid_alive(pid) and not _is_windows():
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.3)
        if _pid_alive(pid):
            _warn(f"daemon pid {pid} did not exit — please stop it manually")
        else:
            _ok(f"stopped daemon pid {pid}")
    except Exception as exc:  # noqa: BLE001
        _warn(f"could not stop daemon pid {pid}: {exc}")


# ---------------------------------------------------------------------------
# User-data cleanup
# ---------------------------------------------------------------------------


def remove_user_data() -> None:
    any_present = False
    for d in USER_DATA_DIRS:
        if d.exists() or d.is_symlink():
            any_present = True
            size = _dir_size(d) if d.is_dir() else 0
            label = f"{d}  ({_human_size(size)})"
            if _rmtree_safe(d):
                _ok(f"removed {label}")
            else:
                _fail(f"could NOT remove {label}")
    if not any_present:
        _skip("no user-data directories found")


# ---------------------------------------------------------------------------
# systemd cleanup (Linux only)
# ---------------------------------------------------------------------------


def cleanup_systemd() -> None:
    if not _is_linux():
        _skip("systemd cleanup not applicable on this platform")
        return

    present = [SYSTEMD_DIR / u for u in SYSTEMD_UNITS if (SYSTEMD_DIR / u).exists()]
    if not present:
        _skip("no dbtool systemd unit files found in /etc/systemd/system")
        return

    failed: list[str] = []
    for unit_path in present:
        unit_name = unit_path.stem
        _info(f"removing {unit_path}")

        # 1. systemctl stop & disable — try without sudo, then with sudo if
        #    interactive.
        for action in ("stop", "disable"):
            cmd = ["systemctl", action, unit_name]
            try:
                subprocess.run(cmd, check=False, capture_output=True, timeout=15)
            except FileNotFoundError:
                _warn("systemctl not found; cannot stop/disable service")
                break
            except Exception as exc:  # noqa: BLE001
                _warn(f"systemctl {action} {unit_name} failed: {exc}")

        # 2. Unlink the unit file. Need root.
        try:
            unit_path.unlink()
            _ok(f"deleted {unit_path}")
        except PermissionError:
            failed.append(str(unit_path))
            _warn(f"permission denied removing {unit_path}")
        except Exception as exc:  # noqa: BLE001
            failed.append(str(unit_path))
            _fail(f"could not remove {unit_path}: {exc}")

    if failed:
        print()
        print(C.yellow("  Manual cleanup required for systemd."))
        print(
            "  Run the following as root to finish removing the service "
            "units:"
        )
        print()
        for unit_path in failed:
            unit_name = Path(unit_path).stem
            print(f"    sudo systemctl stop    {unit_name}")
            print(f"    sudo systemctl disable {unit_name}")
            print(f"    sudo rm  -f {unit_path}")
        print("    sudo systemctl daemon-reload")
        print()
    else:
        try:
            subprocess.run(
                ["systemctl", "daemon-reload"],
                check=False,
                capture_output=True,
                timeout=10,
            )
        except Exception:  # noqa: BLE001
            pass


# ---------------------------------------------------------------------------
# Project-root cleanup
# ---------------------------------------------------------------------------


def remove_generated(project_root: Path) -> None:
    # Top-level wholesale removals (.venv, logs, etc.).
    for rel in GENERATED_DIRS:
        d = project_root / rel
        if d.exists():
            size = _dir_size(d)
            if _rmtree_safe(d):
                _ok(f"removed {rel}/  ({_human_size(size)})")

    for rel in GENERATED_FILES:
        f = project_root / rel
        if f.exists():
            if _unlink_safe(f):
                _ok(f"removed {rel}")

    # Nested cache directories at any depth (e.g. submodule __pycache__).
    nested_cache_names = {
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
    }
    removed_dirs = 0
    for path in sorted(project_root.rglob("*"), key=lambda p: -len(p.parts)):
        if path.is_dir() and path.name in nested_cache_names:
            if _rmtree_safe(path):
                removed_dirs += 1
    if removed_dirs:
        _ok(f"removed {removed_dirs} nested cache director(y/ies)")

    # Stray .pyc / .pyo files anywhere in the tree.
    removed_files = 0
    for pattern in GENERATED_GLOBS:
        for fp in project_root.rglob(pattern):
            if fp.is_file():
                if _unlink_safe(fp):
                    removed_files += 1
    if removed_files:
        _ok(f"removed {removed_files} compiled python file(s)")


def purge_project_root(project_root: Path) -> None:
    """Delete the entire project root directory.

    On POSIX (macOS/Linux) ``shutil.rmtree`` succeeds even while the
    running script lives inside the directory (the kernel keeps the
    executable mapped until the process exits). On Windows, we schedule a
    detached helper batch file that waits 2 seconds then removes the
    directory after this process terminates.
    """
    if not project_root.exists():
        _skip("project root already gone")
        return

    if _is_windows():
        helper_body = textwrap.dedent(
            f"""\
            @echo off
            timeout /t 2 /nobreak >nul
            rmdir /S /Q "{project_root}"
            del "%~f0"
            """
        )
        bat_path = Path(tempfile.gettempdir()) / "dbassistant_purge.bat"
        bat_path.write_text(helper_body, encoding="utf-8")
        try:
            DETACHED = 0x00000008  # DETACHED_PROCESS
            NEW_GROUP = 0x00000200  # CREATE_NEW_PROCESS_GROUP
            subprocess.Popen(  # noqa: S603 — controlled args
                ["cmd", "/c", str(bat_path)],
                close_fds=True,
                creationflags=DETACHED | NEW_GROUP,
            )
            _ok(
                f"scheduled deletion of {project_root} (runs in ~2s after "
                f"this process exits)"
            )
        except Exception as exc:  # noqa: BLE001
            _fail(f"could not schedule project deletion: {exc}")
            _info(f"please manually delete: {project_root}")
        return

    # POSIX path: we can rmtree the running script's parent directly.
    size = _dir_size(project_root)
    if _rmtree_safe(project_root):
        _ok(f"removed project root {project_root}  ({_human_size(size)})")
    else:
        _info(f"please manually delete: {project_root}")


# ---------------------------------------------------------------------------
# Environment variable advisory
# ---------------------------------------------------------------------------


def env_var_advice() -> None:
    flagged = []
    for var in ("DBASSISTANT_HOME", "DBTOOL_API_KEY", "ALERT_TEAMS_WEBHOOK_URL"):
        if os.environ.get(var):
            flagged.append(var)
    if not flagged:
        _skip("no DbManagementTool env vars set in this shell")
        return
    print(C.yellow("  Heads up: these env vars are still set in your shell."))
    print(
        "  They are harmless but you may want to remove them from your "
        "shell rc files:"
    )
    for v in flagged:
        print(f"    - {v}")


# ---------------------------------------------------------------------------
# Summary + confirmation
# ---------------------------------------------------------------------------


def _print_targets(project_root: Path, do_purge: bool) -> None:
    print()
    print(C.b("DbManagementTool — Uninstaller"))
    print()
    print("This will permanently delete:")
    print()
    print(C.b("  User data (always):"))
    any_user = False
    for d in USER_DATA_DIRS:
        if d.exists():
            size = _dir_size(d) if d.is_dir() else 0
            print(f"    {d}  ({_human_size(size)})")
            any_user = True
    if not any_user:
        print("    (none found)")

    print()
    print(C.b("  Project-local generated artefacts (always):"))
    for rel in GENERATED_DIRS:
        d = project_root / rel
        if d.exists():
            print(f"    {d}  ({_human_size(_dir_size(d))})")
    for rel in GENERATED_FILES:
        f = project_root / rel
        if f.exists():
            print(f"    {f}")

    if _is_linux():
        present_units = [SYSTEMD_DIR / u for u in SYSTEMD_UNITS if (SYSTEMD_DIR / u).exists()]
        if present_units:
            print()
            print(C.b("  systemd units (Linux, may require sudo):"))
            for u in present_units:
                print(f"    {u}")

    print()
    if do_purge:
        print(C.red(C.b("  --purge: also deletes the entire project source tree:")))
        print(f"    {project_root}  ({_human_size(_dir_size(project_root))})")
        print("    -> includes config.ini, properties.ini and everything else.")
    else:
        print(C.dim("  Project source tree and *.ini files will be KEPT."))
        print(C.dim("  Pass --purge to also delete the project folder."))
    print()


def _confirm_purge(default_purge: bool) -> bool:
    """The single user confirmation: do we also wipe the project folder?"""
    prompt_default = "yes" if default_purge else "no"
    while True:
        try:
            ans = input(
                f"  Also delete the project folder including .ini files "
                f"(PURGE)? [yes/no, default: {prompt_default}]: "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            print(C.yellow("  Cancelled."))
            sys.exit(2)
        if not ans:
            return default_purge
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        if ans in ("c", "cancel", "q", "quit"):
            print(C.yellow("  Cancelled."))
            sys.exit(2)
        print("  Please answer 'yes', 'no' or 'cancel'.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dbassistant-uninstall",
        description=(
            "Remove DbManagementTool. Always wipes ~/.dbassistant and "
            "project-local caches. With --purge also removes the project "
            "source folder (including config.ini / properties.ini)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="Also delete the project source folder (config.ini included).",
    )
    parser.add_argument(
        "--no-purge",
        action="store_true",
        help="Do NOT delete the project folder. Skip the confirmation.",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help=(
            "Skip the single confirmation prompt. Uses --purge / --no-purge "
            "if provided, otherwise defaults to --no-purge (keep source)."
        ),
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=None,
        help=(
            "Project root to clean up. Defaults to the parent of this "
            "script. Useful when the uninstaller has been copied elsewhere."
        ),
    )

    args = parser.parse_args(argv)

    if args.purge and args.no_purge:
        print(C.red("Cannot pass both --purge and --no-purge."), file=sys.stderr)
        return 2

    project_root = (args.project_root or Path(__file__).resolve().parent.parent).resolve()

    # Decide whether we'll purge before printing the summary so the user
    # sees what's actually about to happen.
    if args.purge:
        do_purge = True
    elif args.no_purge:
        do_purge = False
    elif args.yes:
        do_purge = False
    else:
        do_purge = False  # default value shown to user in prompt

    _print_targets(project_root, do_purge)

    if not args.yes and not args.purge and not args.no_purge:
        do_purge = _confirm_purge(default_purge=False)
        _print_targets(project_root, do_purge)  # re-print to confirm choice

    _section("Stopping monitoring daemon")
    stop_daemon()

    _section("Removing systemd services")
    cleanup_systemd()

    _section("Removing user data (~/.dbassistant + legacy)")
    remove_user_data()

    _section("Removing project-local generated files")
    remove_generated(project_root)

    _section("Environment variables")
    env_var_advice()

    if do_purge:
        _section("Purging project source folder")
        purge_project_root(project_root)

    print()
    print(C.green(C.b("Uninstall complete.")))
    if not do_purge:
        print(
            C.dim(
                "  Project source tree kept at:\n"
                f"    {project_root}\n"
                "  To remove it later, delete the folder manually or re-run "
                "with --purge."
            )
        )
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
