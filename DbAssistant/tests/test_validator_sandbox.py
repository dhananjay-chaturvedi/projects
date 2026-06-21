"""Session C validator-owned test folder — structure helpers and sandbox."""

from __future__ import annotations

from pathlib import Path

from ai_assistant.app_builder.engine import VALIDATOR_TEST_DIR
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator


def _scaffold(ws: Path) -> None:
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text(
        "from fastapi import FastAPI\napp = FastAPI()\n", encoding="utf-8")
    (ws / "src" / "domain.py").write_text(
        "class Widget:\n    def ping(self):\n        return 'ok'\n",
        encoding="utf-8")
    (ws / "tests").mkdir()
    (ws / "tests" / "test_app.py").write_text("def test_x(): pass\n",
                                               encoding="utf-8")


def test_workspace_structure_lists_builder_files(tmp_path: Path) -> None:
    _scaffold(tmp_path)
    tree = AppBuildOrchestrator._workspace_structure(tmp_path)
    assert "src/app.py" in tree
    assert "src/domain.py" in tree
    assert VALIDATOR_TEST_DIR not in tree


def test_public_symbols_extracts_classes_and_methods(tmp_path: Path) -> None:
    _scaffold(tmp_path)
    syms = AppBuildOrchestrator._public_symbols(tmp_path)
    assert "src.domain" in syms
    assert "Widget" in syms
    assert "ping" in syms


def test_enforce_validator_sandbox_reverts_out_of_folder_writes(
    tmp_path: Path,
) -> None:
    _scaffold(tmp_path)
    before = {"src/app.py": (tmp_path / "src/app.py").read_text(encoding="utf-8"),
              "tests/test_app.py": (tmp_path / "tests/test_app.py").read_text(
                  encoding="utf-8")}
    # Simulate C writing in its folder (allowed) and tampering with A's code.
    vdir = tmp_path / VALIDATOR_TEST_DIR
    vdir.mkdir()
    (vdir / "test_acceptance.py").write_text("def test_health(): pass\n",
                                             encoding="utf-8")
    (tmp_path / "src" / "app.py").write_text("# tampered\n", encoding="utf-8")
    orch = AppBuildOrchestrator()
    reverted = orch._enforce_validator_sandbox(tmp_path, before)
    assert "src/app.py" in reverted
    assert "tampered" not in (tmp_path / "src" / "app.py").read_text()
    assert (vdir / "test_acceptance.py").exists()


def test_validator_test_dir_constant() -> None:
    assert VALIDATOR_TEST_DIR == "validator_generated_tests"
