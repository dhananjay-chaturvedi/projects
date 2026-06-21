"""
Smoke-test for ``setup/uninstall.py``.

Builds an entirely fake home + project root in a temp dir, copies the
real uninstall.py in, and runs:

1. ``--no-purge -y``  — should wipe fake_home/.dbassistant and project
   caches but keep .ini files and the project source.
2. ``--purge   -y``   — should additionally remove the project root.

This script is intentionally NOT a pytest case so it can be invoked
directly (``python tests/_smoke_uninstall.py``). It exits non-zero on
the first assertion failure.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REAL_SCRIPT = Path(__file__).resolve().parent.parent / "setup" / "uninstall.py"
assert REAL_SCRIPT.exists(), f"missing {REAL_SCRIPT}"


def _seed_project(root: Path) -> None:
    """Create a mini project tree with the same kinds of artefacts the
    real project has, so the uninstaller has something to act on."""
    (root / "setup").mkdir(parents=True, exist_ok=True)
    shutil.copy2(REAL_SCRIPT, root / "setup" / "uninstall.py")
    # Source files (should survive non-purge).
    (root / "config.ini").write_text("[paths]\n")
    (root / "properties.ini").write_text("[ui.window]\n")
    (root / "dbtool.py").write_text("# entry stub\n")
    (root / "monitoring").mkdir()
    (root / "monitoring" / "monitor_thresholds.ini").write_text("# rules\n")
    # Generated artefacts (should be removed on non-purge).
    (root / ".venv").mkdir()
    (root / ".venv" / "marker.txt").write_text("venv")
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "foo.cpython-312.pyc").write_text("bytecode")
    (root / ".pytest_cache").mkdir()
    (root / ".pytest_cache" / "v").write_text("cache")
    (root / "logs").mkdir()
    (root / "logs" / "app.log").write_text("log line")
    (root / ".DS_Store").write_text("\x00")
    (root / "some_module" / "__pycache__").mkdir(parents=True)
    (root / "some_module" / "bar.pyc").write_text("bytecode")


def _seed_home(home: Path) -> None:
    """Create fake user-data dirs."""
    da = home / ".dbassistant"
    (da / "keys").mkdir(parents=True)
    (da / "keys" / "db.key").write_bytes(b"\x00" * 32)
    (da / "connections").mkdir()
    (da / "connections" / "db.json").write_text("{}")
    (da / "runtime").mkdir()
    (da / "runtime" / "alerts.jsonl").write_text("")
    (da / "session").mkdir()
    (da / "session" / "ai_state.json").write_text("{}")
    # Legacy: one of each
    (home / ".dbtool").mkdir()
    (home / ".dbtool" / "old.json").write_text("legacy")
    (home / ".dbmanager.legacy").mkdir()
    (home / ".dbmanager.legacy" / "stale.txt").write_text("legacy")


def _run(cmd: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, env=env, capture_output=True, text=True, timeout=60
    )


def main() -> int:
    failures: list[str] = []

    with tempfile.TemporaryDirectory(prefix="dbassistant_smoke_") as td:
        sandbox = Path(td).resolve()
        fake_home = sandbox / "home"
        fake_home.mkdir()

        # ---- Scenario 1: non-purge -------------------------------------
        proj1 = sandbox / "proj1"
        _seed_project(proj1)
        _seed_home(fake_home)

        env = os.environ.copy()
        env["HOME"] = str(fake_home)
        # Belt-and-braces: also unset DBASSISTANT_HOME so we hit defaults.
        env.pop("DBASSISTANT_HOME", None)
        # Coerce a non-tty stdout so colors are disabled.
        env["NO_COLOR"] = "1"

        r1 = _run(
            [
                sys.executable,
                str(proj1 / "setup" / "uninstall.py"),
                "--project-root",
                str(proj1),
                "--no-purge",
                "-y",
            ],
            env=env,
        )
        print("=== scenario 1: --no-purge ===")
        print(r1.stdout)
        if r1.returncode != 0:
            print(r1.stderr, file=sys.stderr)
            failures.append(f"non-purge exit code {r1.returncode}")

        # User data: gone.
        for missing in [
            fake_home / ".dbassistant",
            fake_home / ".dbtool",
            fake_home / ".dbmanager.legacy",
        ]:
            if missing.exists():
                failures.append(f"expected user-data dir gone: {missing}")
        # Project source: still here.
        for must_remain in [
            proj1,
            proj1 / "config.ini",
            proj1 / "properties.ini",
            proj1 / "monitoring" / "monitor_thresholds.ini",
            proj1 / "dbtool.py",
            proj1 / "setup" / "uninstall.py",
        ]:
            if not must_remain.exists():
                failures.append(f"expected to remain: {must_remain}")
        # Caches: gone.
        for must_be_gone in [
            proj1 / ".venv",
            proj1 / "__pycache__",
            proj1 / ".pytest_cache",
            proj1 / "logs",
            proj1 / ".DS_Store",
            proj1 / "some_module" / "__pycache__",
            proj1 / "some_module" / "bar.pyc",
        ]:
            if must_be_gone.exists():
                failures.append(f"expected gone after non-purge: {must_be_gone}")

        # ---- Scenario 2: purge -----------------------------------------
        proj2 = sandbox / "proj2"
        _seed_project(proj2)
        _seed_home(fake_home)

        r2 = _run(
            [
                sys.executable,
                str(proj2 / "setup" / "uninstall.py"),
                "--project-root",
                str(proj2),
                "--purge",
                "-y",
            ],
            env=env,
        )
        print("=== scenario 2: --purge ===")
        print(r2.stdout)
        if r2.returncode != 0:
            print(r2.stderr, file=sys.stderr)
            failures.append(f"purge exit code {r2.returncode}")

        if proj2.exists():
            # On non-Windows POSIX, project root should be fully removed.
            failures.append(f"expected project root gone after --purge: {proj2}")
        if (fake_home / ".dbassistant").exists():
            failures.append("expected ~/.dbassistant gone after --purge")

    if failures:
        print()
        print("FAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print()
    print("SMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
