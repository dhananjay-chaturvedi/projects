"""Backend dropdown live-refresh after local model training."""

from __future__ import annotations

import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class _FakeCombo:
    def __init__(self):
        self.values = []

    def config(self, **kw):
        if "values" in kw:
            self.values = list(kw["values"])


class _FakeVar:
    def __init__(self, val=""):
        self._v = val

    def get(self):
        return self._v

    def set(self, v):
        self._v = v


class _FakeAgent:
    def __init__(self, options, active_value=""):
        self._options = options
        self._active = active_value

    def list_backend_options(self):
        return self._options

    def get_active_backend_value(self):
        return self._active


def _call_refresh(self_obj):
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    AIQueryUI.refresh_backend_options(self_obj)


def test_refresh_adds_new_model_and_keeps_selection():
    combo = _FakeCombo()
    var = _FakeVar("Cursor")
    agent = _FakeAgent(
        options=[
            {"label": "Cursor", "value": "cursor"},
            {"label": "mymodel (local pytorch)", "value": "local-llm::mymodel"},
        ],
        active_value="cursor",
    )
    self_obj = types.SimpleNamespace(
        root=None, ai_backend_combo=combo, ai_backend_var=var,
        ai_agent=agent, _backend_label_to_name={},
    )
    _call_refresh(self_obj)
    assert "mymodel (local pytorch)" in combo.values
    assert var.get() == "Cursor"  # existing selection preserved
    assert self_obj._backend_label_to_name["mymodel (local pytorch)"] == "local-llm::mymodel"


def test_refresh_falls_back_to_active_when_selection_gone():
    combo = _FakeCombo()
    var = _FakeVar("OldModel (local pytorch)")  # no longer present
    agent = _FakeAgent(
        options=[{"label": "Cursor", "value": "cursor"}],
        active_value="cursor",
    )
    self_obj = types.SimpleNamespace(
        root=None, ai_backend_combo=combo, ai_backend_var=var,
        ai_agent=agent, _backend_label_to_name={},
    )
    _call_refresh(self_obj)
    assert var.get() == "Cursor"


def test_refresh_wired_after_training():
    tk = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "refresh_backend_options" in tk
    ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    assert "def refresh_backend_options" in ui
