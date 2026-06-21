"""Tests for the Approve → package step (shippable bundle generation)."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

from ai_assistant.app_builder.packaging import package_app


def _make_workspace(tmp_path: Path) -> Path:
    ws = tmp_path / "demoapp"
    (ws / "src" / "db").mkdir(parents=True)
    (ws / "src" / "__init__.py").write_text("", encoding="utf-8")
    (ws / "src" / "app.py").write_text("app = object()\n", encoding="utf-8")
    (ws / "src" / "db" / "__init__.py").write_text("", encoding="utf-8")
    (ws / "src" / "db" / "connection.py").write_text(
        "def get_connection():\n    return None\n", encoding="utf-8")
    (ws / "requirements.txt").write_text("fastapi\n", encoding="utf-8")
    return ws


def test_package_writes_install_run_scripts_and_archive(tmp_path):
    ws = _make_workspace(tmp_path)

    result = package_app(ws, app_name="demoapp", port=9001, make_archive=True)

    assert result.ok
    for rel in ("setup_db.py", "install.sh", "install.bat", "run.sh",
                "run.bat", "INSTALL.md"):
        assert (ws / rel).exists(), f"missing {rel}"
    # POSIX scripts are executable.
    assert os.access(ws / "install.sh", os.X_OK)
    assert os.access(ws / "run.sh", os.X_OK)
    # Port is baked into the run scripts and docs.
    assert "9001" in (ws / "run.sh").read_text(encoding="utf-8")
    assert "9001" in (ws / "INSTALL.md").read_text(encoding="utf-8")

    assert result.archive and Path(result.archive).exists()
    with zipfile.ZipFile(result.archive) as zf:
        names = zf.namelist()
    assert any(n.endswith("install.sh") for n in names)
    # Build/venv cruft must not be shipped.
    assert not any(".venv" in n for n in names)
    assert not any(n.endswith("app.db") for n in names)


def test_package_excludes_runtime_db_files_and_var_dir(tmp_path):
    ws = _make_workspace(tmp_path)
    (ws / "data.db").write_bytes(b"sqlite")
    (ws / "runtime.sqlite3").write_bytes(b"sqlite")
    (ws / "var").mkdir()
    (ws / "var" / "app.db").write_bytes(b"sqlite")

    result = package_app(ws, app_name="demoapp", make_archive=True)

    assert result.ok
    with zipfile.ZipFile(result.archive) as zf:
        names = zf.namelist()
    assert not any("data.db" in n for n in names)
    assert not any("runtime.sqlite3" in n for n in names)
    assert not any("/var/" in n for n in names)


def test_package_synthesizes_missing_requirements(tmp_path):
    ws = _make_workspace(tmp_path)
    (ws / "requirements.txt").unlink()

    result = package_app(ws, app_name="demoapp", make_archive=False)

    assert result.ok
    req = (ws / "requirements.txt").read_text(encoding="utf-8")
    assert "fastapi" in req and "uvicorn" in req
    assert "requirements.txt" in result.created


def test_package_refuses_without_runnable_app(tmp_path):
    ws = tmp_path / "empty"
    ws.mkdir()

    result = package_app(ws, app_name="empty")

    assert not result.ok
    assert any("src/app.py" in issue for issue in result.issues)


def test_no_archive_flag_skips_zip(tmp_path):
    ws = _make_workspace(tmp_path)

    result = package_app(ws, app_name="demoapp", make_archive=False)

    assert result.ok
    assert result.archive == ""
    assert not (ws.parent / "demoapp-package.zip").exists()
