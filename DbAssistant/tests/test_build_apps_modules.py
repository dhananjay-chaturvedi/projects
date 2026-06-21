"""Module discovery and CLI smoke for app-builder."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_modules_discover_new_build_apps():
    from common.core import modules as m

    installed = m.discover(refresh=True)
    assert "llm_builder" not in installed  # replaced by ai_query rag/llm
    assert "app_builder" in installed
    assert installed["app_builder"].cli_commands == ["app-builder"]


def test_app_builder_scaffold_cli(tmp_path):
    import os

    env = {**os.environ, "DBASSISTANT_HOME": str(tmp_path)}
    p = subprocess.run(
        [sys.executable, "-m", "ai_assistant.app_builder", "app-builder",
         "scaffold", "--name", "clitest"],
        cwd=ROOT, capture_output=True, text=True, timeout=30, env=env,
    )
    assert p.returncode == 0, (p.stdout or "") + (p.stderr or "")
    assert (tmp_path / "ai_assistant" / "app_builder" / "clitest" / "src" / "app.py").is_file()


def test_ai_payload_has_build_apps():
    from common.ui.shared import specs

    payload = specs.ai_payload()
    ids = [a["id"] for a in payload["buildAppsActions"]]
    assert ids == ["app_builder"]
