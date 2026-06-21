"""Pytest wrapper for tests/verify_train_llm_runtime.py."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_verify_train_llm_runtime_script():
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tests/verify_train_llm_runtime.py")],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "All verification checks passed" in proc.stdout
