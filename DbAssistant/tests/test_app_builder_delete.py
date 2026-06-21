"""Tests for the Delete build step (erase a build's workspace, no trace)."""

from __future__ import annotations

from pathlib import Path

from ai_assistant.app_builder import service as svc_mod
from ai_assistant.app_builder.service import AppBuilderService


def _service_rooted_at(tmp_path: Path, monkeypatch) -> tuple[AppBuilderService, Path]:
    root = tmp_path / "app_builder"
    root.mkdir(parents=True)
    monkeypatch.setattr(svc_mod.app_paths, "app_builder_dir", lambda: root)
    return AppBuilderService(), root


def _make_build(root: Path, name: str) -> Path:
    ws = root / name
    (ws / "src").mkdir(parents=True)
    (ws / "src" / "app.py").write_text("app = object()\n", encoding="utf-8")
    (ws / "app.db").write_text("data", encoding="utf-8")
    return ws


def test_delete_app_erases_workspace(tmp_path, monkeypatch):
    svc, root = _service_rooted_at(tmp_path, monkeypatch)
    ws = _make_build(root, "demoapp")
    assert ws.exists()

    result = svc.delete_app({"name": "demoapp"})

    assert result["ok"] and result["deleted"]
    assert not ws.exists()
    # The parent output dir stays; only this build is gone.
    assert root.exists()


def test_delete_app_missing_build_is_ok_noop(tmp_path, monkeypatch):
    svc, _root = _service_rooted_at(tmp_path, monkeypatch)

    result = svc.delete_app({"name": "ghost"})

    assert result["ok"] and not result["deleted"]
    assert any("nothing to delete" in i for i in result["issues"])


def test_delete_app_requires_name(tmp_path, monkeypatch):
    svc, _root = _service_rooted_at(tmp_path, monkeypatch)

    result = svc.delete_app({"name": "  "})

    assert not result["ok"]
    assert not result["deleted"]


def test_delete_app_refuses_path_escape(tmp_path, monkeypatch):
    svc, root = _service_rooted_at(tmp_path, monkeypatch)
    outside = tmp_path / "secret"
    outside.mkdir()
    (outside / "keep.txt").write_text("important", encoding="utf-8")

    # Try to climb out of the output dir via the build name.
    result = svc.delete_app({"name": "../secret"})

    assert not result["ok"]
    assert (outside / "keep.txt").exists()
    assert any("outside" in i for i in result["issues"])
