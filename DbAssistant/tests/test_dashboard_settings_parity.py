"""Dashboard and Settings parity tests (Web + TUI controls, offline-safe)."""

from __future__ import annotations

import pytest


def test_web_dashboard_settings_expose_tk_controls():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    html = c.get("/").text
    for token in (
        "dash-refresh",
        "dash-reset-layout",
        "Drag ⠿ header to rearrange",
        "settings-save",
        "settings-save-key",
        "Save changes",
        "settings-reload",
        "settings-restore-defaults",
        "settings-clear-cache",
        "settings-shortcuts",
    ):
        assert token in html


def test_web_dashboard_settings_appjs_wires_controls():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    for token in ("dash-reset-layout", "settings-reload", "settings-restore-defaults", "settings-save"):
        assert token in js
    assert 'api.post("/api/dashboard/layout/reset")' in js
    assert 'api.post("/api/config/settings"' in js


def test_web_settings_type_aware_and_secret_writeonly():
    """Web settings render per-type controls and treat secrets write-only
    (blank = unchanged), reading the API's `sensitive`/`type`/`options` (Tk
    parity), not the absent `secret` field."""
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    # Reads the real API metadata fields.
    assert "settingIsSecret" in js
    assert "s.sensitive" in js
    # Per-type controls: bool/enum -> <select>, secret -> masked write-only input.
    assert 'data-type="bool"' in js
    assert 'data-type="enum"' in js
    assert 'type="password"' in js and 'data-type="secret"' in js
    assert "(unchanged)" in js  # write-only secret placeholder
    # collectSettingEdits skips blank secrets (only sends typed values).
    assert "if (el.value) out[id] = el.value;" in js


def test_web_llm_trainer_has_eval_action():
    """Web LLM trainer exposes Evaluate model wired to /api/ai/llm/eval."""
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    assert '["eval", "Evaluate model"]' in js
    assert 'api.post("/api/ai/llm/eval"' in js


def test_web_rag_remove_document_uses_source_field():
    """RAG Manager remove posts the API's expected {scope, source} shape."""
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    js = c.get("/ui/app.js").text
    assert 'api.post("/api/ai/rag/remove-document", { scope, source: q })' in js


pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_dashboard_settings_expose_tk_controls():
    from textual.widgets import Button

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("dashboard")
        await pilot.pause()
        dash = app.screen
        for bid in ("dash-refresh", "dash-reset-layout"):
            assert dash.query_one("#" + bid, Button) is not None

        app.push_screen_by_name("settings")
        await pilot.pause()
        settings = app.screen
        for bid in (
            "set-save",
            "set-reload",
            "set-restore-defaults",
            "set-clear-cache",
            "set-shortcuts",
        ):
            assert settings.query_one("#" + bid, Button) is not None


@pytest.mark.anyio
async def test_tui_settings_secret_edit_is_write_only(monkeypatch):
    """Sensitive settings are editable in Textual (Tk parity) via a masked,
    write-only field: blank leaves the secret unchanged; a value persists."""
    from textual.widgets import Button, DataTable, Input

    from common.ui.textual.app import DbToolApp
    from common.config import settings_service as S

    saved: list[tuple[str, str]] = []
    monkeypatch.setattr(
        S, "set_value",
        lambda sid, value: saved.append((sid, value)) or {"ok": True, "message": "ok"},
    )

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("settings")
        await pilot.pause()
        scr = app.screen
        # Inject a sensitive setting and render it.
        scr._settings = [{"id": "secret.token", "type": "str", "sensitive": True,
                          "label": "API token", "value": "***", "group": "Secrets",
                          "description": "secret"}]
        scr._render_rows()
        scr.query_one("#set-grid", DataTable).move_cursor(row=0)
        await pilot.pause()

        # Blank submit leaves the secret unchanged (no set_value).
        scr.query_one("#set-save", Button).press()
        await pilot.pause()
        field = app.screen.query_one("#field-value", Input)
        assert field.password is True and field.value == ""
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        assert saved == []

        # A value submit persists it.
        scr.query_one("#set-save", Button).press()
        await pilot.pause()
        app.screen.query_one("#field-value", Input).value = "s3cr3t"
        app.screen.query_one("#form-submit", Button).press()
        await pilot.pause()
        assert saved == [("secret.token", "s3cr3t")]
