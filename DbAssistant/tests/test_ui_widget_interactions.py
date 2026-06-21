"""Subprocess-backed widget interaction coverage for the master Tk UI."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_master_ui_widget_interactions(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["DBASSISTANT_HOME"] = str(tmp_path / "dbassistant-home")
    env["PYTHONPATH"] = str(root)
    proc = subprocess.run(
        [sys.executable, str(root / "tests" / "_ui_widget_interactions.py")],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        timeout=90,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode < 0 and not output:
        pytest.skip("Tk unavailable/headless: UI subprocess aborted before output")
    assert proc.returncode == 0, output
    assert "UI_WIDGET_INTERACTIONS_OK" in output
