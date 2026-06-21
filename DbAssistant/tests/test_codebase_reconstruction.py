"""Deterministic from-codebase build performs reconstruction + reports meters."""

from __future__ import annotations

from pathlib import Path

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.flows import BuildFlows


def _make_codebase(root: Path):
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    (src / "app.py").write_text(
        "from fastapi import FastAPI\n"
        "app = FastAPI()\n\n"
        '@app.get("/users")\n'
        "def list_users():\n"
        '    """List users."""\n'
        "    return []\n\n\n"
        "class UserService:\n"
        '    """Domain service."""\n'
        "    def all(self):\n"
        "        return []\n",
        encoding="utf-8",
    )
    (root / "requirements.txt").write_text("fastapi\nuvicorn\n", encoding="utf-8")


def test_build_from_codebase_injects_brief_and_meters(tmp_path):
    code = tmp_path / "legacy_app"
    _make_codebase(code)
    ws = tmp_path / "out"

    bp = AppBlueprint(
        name="recovered",
        description="Recover this app",
        mode=BuildMode.FROM_CODEBASE,
        codebase_path=str(code),
    )
    out = BuildFlows().build_from_codebase(bp, ws)

    # Reconstruction brief was folded into the blueprint description.
    assert "PREDICTED APP DESIGN BRIEF" in bp.description
    assert "/users" in bp.description  # recovered route surfaced in the brief

    # Codebase meters were evaluated and attached.
    assert "meters" in out
    assert out["meters"].get("overall") is not None
    assert "architecture_recovery" in out["meters"].get("meters", {})
    assert isinstance(out.get("components"), list)
    assert out.get("insight", {}).get("profile", {}).get("routes")


def test_build_from_codebase_handles_missing_path(tmp_path):
    bp = AppBlueprint(
        name="nope",
        description="x",
        mode=BuildMode.FROM_CODEBASE,
        codebase_path=str(tmp_path / "does_not_exist"),
    )
    # Must not raise even when the codebase path is empty/missing.
    out = BuildFlows().build_from_codebase(bp, tmp_path / "out2")
    assert "files" in out
