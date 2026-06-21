"""Tests for AiAppEngine — code-enforced app building rules."""

from __future__ import annotations

from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint
from ai_assistant.app_builder.service import AppBuilderService


def test_blueprint_validation_rejects_empty_name():
    eng = AiAppEngine()
    v = eng.validate_blueprint(AppBlueprint(name=""))
    assert not v.accepted


def test_scaffold_from_scratch_passes_design_gate(tmp_path, monkeypatch):
    from common import paths as app_paths

    monkeypatch.setattr(app_paths, "app_builder_dir", lambda: tmp_path)
    svc = AppBuilderService()
    r = svc.scaffold_from_scratch("demoapp")
    assert r["ok"]
    assert (tmp_path / "demoapp" / "src" / "app.py").is_file()
    assert (tmp_path / "demoapp" / "requirements.txt").is_file()


def test_engine_metadata_packet_includes_rules():
    bp = AppBlueprint(name="x", services=["ci_cd", "database"])
    pkt = AiAppEngine().agent_metadata_packet(bp)
    assert pkt["engine"] == "AiAppEngine"
    assert "required_files" in pkt
    assert pkt["rules"]
