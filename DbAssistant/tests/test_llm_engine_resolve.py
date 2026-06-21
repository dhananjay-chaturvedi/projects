"""Tests for LLM engine resolution and monitor mixin shared exports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_assistant.llm.service import LlmService


def test_monitor_shared_exports_make_scrollable():
    import common.ui.tk.monitor.server_monitor.mixins._shared as shared

    assert "make_scrollable" in shared.__all__
    assert callable(shared.make_scrollable)


def test_resolve_for_model_raises_when_trained_engine_unavailable(tmp_path, monkeypatch):
    model_dir = tmp_path / "pt_model"
    model_dir.mkdir()
    (model_dir / "meta.json").write_text(
        json.dumps({"name": "pt_model", "engine": "pytorch"}),
        encoding="utf-8",
    )
    (model_dir / "model.pt").write_bytes(b"stub")

    svc = LlmService(models_dir=tmp_path)

    class _FakeEngine:
        name = "pytorch"

        def is_available(self):
            return False, "torch missing in test"

    monkeypatch.setattr(
        "ai_assistant.llm.service.get_engine",
        lambda name: _FakeEngine() if name == "pytorch" else None,
    )

    with pytest.raises(RuntimeError, match="trained with the 'pytorch' engine"):
        svc._resolve_for_model("pt_model", None)


def test_generate_reports_engine_unavailable_not_model_json(tmp_path, monkeypatch):
    model_dir = tmp_path / "pt_model"
    model_dir.mkdir()
    (model_dir / "meta.json").write_text(
        json.dumps({"name": "pt_model", "engine": "pytorch"}),
        encoding="utf-8",
    )
    (model_dir / "model.pt").write_bytes(b"stub")

    svc = LlmService(models_dir=tmp_path)

    class _FakeEngine:
        name = "pytorch"

        def is_available(self):
            return False, "torch missing in test"

    monkeypatch.setattr(
        "ai_assistant.llm.service.get_engine",
        lambda name: _FakeEngine() if name == "pytorch" else None,
    )

    out = svc.generate("list users", name="pt_model")
    assert out["ok"] is False
    assert "pytorch" in (out.get("error") or "")
    assert "model.json" not in (out.get("error") or "")


def test_trained_engine_name_reads_meta(tmp_path):
    model_dir = tmp_path / "numpy_model"
    model_dir.mkdir()
    (model_dir / "meta.json").write_text(
        json.dumps({"name": "numpy_model", "engine": "numpy"}),
        encoding="utf-8",
    )
    svc = LlmService(models_dir=tmp_path)
    assert svc._trained_engine_name("numpy_model", None) == "numpy"
