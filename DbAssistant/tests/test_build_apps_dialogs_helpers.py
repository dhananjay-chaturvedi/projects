"""Tests for App Builder Tk dialog helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from common.ui.tk.ai.build_apps_dialogs import (
    _default_app_name,
    _ensure_build_name,
    _is_auto_app_name,
)


class _FakeNameVar:
    def __init__(self, value: str = "") -> None:
        self._value = value

    def get(self) -> str:
        return self._value

    def set(self, value: str) -> None:
        self._value = value


def test_default_app_name_matches_pattern():
    name = _default_app_name()
    assert name.startswith("dbassist_app_")
    assert _is_auto_app_name(name)


@patch("common.ui.tk.ai.build_apps_dialogs._default_app_name")
def test_ensure_build_name_regenerates_auto_pattern(mock_name):
    mock_name.side_effect = [
        "dbassist_app_20260101_120001",
        "dbassist_app_20260101_120002",
    ]
    var = _FakeNameVar("dbassist_app_20260101_120000")
    first = _ensure_build_name(var)  # type: ignore[arg-type]
    second = _ensure_build_name(var)  # type: ignore[arg-type]
    assert first == "dbassist_app_20260101_120001"
    assert second == "dbassist_app_20260101_120002"


def test_ensure_build_name_keeps_custom_name():
    var = _FakeNameVar("my_custom_shop")
    assert _ensure_build_name(var) == "my_custom_shop"  # type: ignore[arg-type]
    assert var.get() == "my_custom_shop"


def test_baseline_write_does_not_clobber_existing_files(tmp_path):
    ws = tmp_path / "app"
    ws.mkdir()
    app_py = ws / "src" / "app.py"
    app_py.parent.mkdir(parents=True)
    custom = "# custom full app\nfrom fastapi import FastAPI\napp = FastAPI()\n"
    app_py.write_text(custom, encoding="utf-8")

    engine = AiAppEngine()
    orch = AppBuildOrchestrator(engine)
    bp = AppBlueprint(name="shop", mode=BuildMode.FROM_SCRATCH)
    stub = {f.path: f.content for f in orch._baseline.generate(
        orch._request(bp, None)).files}

    orch._write(ws, stub, overwrite=False)
    assert app_py.read_text(encoding="utf-8") == custom

    stub["src/app.py"] = "# stub baseline\n"
    orch._write(ws, stub, overwrite=True)
    assert "stub baseline" in app_py.read_text(encoding="utf-8")


def test_stop_running_app_terminates_process():
    from common.ui.tk.ai.build_apps_dialogs import _stop_running_app

    proc = MagicMock()
    proc.poll.return_value = None
    state = {"process": proc}
    _stop_running_app(state)
    proc.terminate.assert_called_once()
    assert state["process"] is None
