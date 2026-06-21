"""Web UI functional wiring tests — handlers must call backend APIs, not desktop stubs.

Presence parity (button exists in HTML) is covered by tab-specific parity tests.
These tests verify that critical Web controls invoke the same API routes the Tk
UI reaches through the headless service layer.
"""

from __future__ import annotations

import re

import pytest


def _app_js() -> str:
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    return c.get("/ui/app.js").text


def _handler_block(js: str, button_id: str) -> str:
    """Return the addEventListener block for a button id (best-effort slice)."""
    needle = f'$("#{button_id}")'
    start = js.find(needle)
    assert start >= 0, f"missing handler anchor for {button_id}"
    # Slice until next top-level $("#...") listener or EOF (app.js is flat).
    rest = js[start + len(needle) :]
    nxt = rest.find('\n$("#')
    return js[start : start + len(needle) + (nxt if nxt >= 0 else len(rest))]


@pytest.mark.parametrize(
    "button_id,api_pattern",
    [
        ("dash-reset-layout", r'api\.post\("/api/dashboard/layout/reset"\)'),
        ("mig-settings", r'api\.get\("/api/migrator/config"\)'),
        ("mig-compare-schema", r'api\.post\("/api/migrator/compare-schema"'),
        ("mig-compare", r'api\.post\("/api/migrator/compare-data"'),
        ("mig-dump", r'/api/migrator/.*/dump'),
        ("ai-review", r'api\.post\("/api/ai/review"'),
        ("ai-schema-show", r'api\.get\("/api/ai/cache/show'),
        ("ai-settings-open", r'api\.get\("/api/ai/config"\)'),
        ("mon-settings", r'api\.get\("/api/monitor/config"\)'),
        ("mon-notifications", r'api\.get\("/api/monitor/notifications"\)'),
        ("mon-add-ssh", r'api\.post\("/api/monitor/connections/saved"'),
        ("mon-add-db", r'api\.post\("/api/monitor/db-connections"'),
        ("mon-add-cloud", r'api\.post\("/api/monitor/cloud/connections"'),
        ("mon-edit-target", r'api\.call\("PUT", `/api/monitor/connections/saved/'),
        ("mon-test-target", r'api\.post\(`/api/monitor/db-connections/'),
        ("mon-thr-edit", r'api\.call\("PATCH", `/api/thresholds/'),
        ("mon-thr-check", r'api\.post\("/api/thresholds/check"'),
        ("ai-session-new", r'api\.post\("/api/ai/sessions"'),
        ("ai-session-refresh", r'loadAiSessions'),
        ("ai-session-delete", r'api\.del\(`/api/ai/sessions/'),
        ("ai-session-followup", r'askAiSession\("followup"\)'),
        ("ai-session-cross", r'/cross-tab'),
        ("dash-save-layout", r'api\.post\("/api/dashboard/layout"'),
        ("settings-save", r'api\.post\("/api/config/settings"'),
        ("settings-restore-defaults", r'api\.post\("/api/config/settings/restore"'),
    ],
)
def test_web_critical_handlers_call_api(button_id: str, api_pattern: str):
    block = _handler_block(_app_js(), button_id)
    assert re.search(api_pattern, block), (
        f"{button_id} handler should call {api_pattern!r}; got:\n{block[:400]}"
    )


def test_web_mon_remove_stops_then_deletes_by_source():
    """Remove stops an active target, else deletes the idle saved one by source."""
    js = _app_js()
    start = js.find("async function monStopOrDelete")
    assert start >= 0, "missing monStopOrDelete function"
    block = js[start : start + 1100]
    # First branch stops monitoring (no API call); the delete branch routes by source.
    assert "monActive[cat].delete" in block
    assert "/api/monitor/db-connections/" in block
    assert "/api/monitor/connections/saved/" in block
    assert "/api/monitor/cloud/connections/" in block
    # The handler is wired to every section's Remove button.
    for token in ("mon-server-remove", "mon-database-remove", "mon-cloud-remove"):
        assert token in js, token


def test_web_mig_apply_passes_index_and_drop_options():
    js = _app_js()
    block = _handler_block(js, "mig-apply")
    assert "create_indexes" in block
    assert "drop_if_exists" in block


def test_web_ai_send_editor_opens_sql_tab():
    block = _handler_block(_app_js(), "ai-send-editor")
    assert "sqlTabs.push" in block
    assert 'activateTab("sql")' in block


def _index_html() -> str:
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    return c.get("/").text


def test_web_uses_labelframe_groupings():
    """Tk LabelFrame parity: panels group controls in labelled fieldsets.

    Monitoring mirrors the Tk three-section layout (Server / Database / Cloud).
    """
    html = _index_html()
    assert html.count('class="labelframe"') >= 5
    for legend in ("Server monitoring", "Database Monitoring",
                   "Cloud Resource Monitoring", "Alerts",
                   "Editor actions", "Browse objects"):
        assert legend in html, legend


def test_monitor_threshold_section_id_not_on_migration():
    """The Alert Thresholds button must open the monitor section, not migration."""
    html = _index_html()
    # The monitor thresholds details owns mon-thresholds-sec; migration uses its own id.
    assert html.count('id="mon-thresholds-sec"') == 1
    assert 'id="mig-options-sec"' in html
    mon_idx = html.find('id="mon-thresholds-sec"')
    mig_idx = html.find('id="mig-options-sec"')
    panel_mon = html.find('id="panel-monitor"')
    panel_dash = html.find('id="panel-dashboard"')
    # mon-thresholds-sec must live inside the monitor panel.
    assert panel_mon < mon_idx < panel_dash


def test_web_ai_review_not_desktop_stub():
    block = _handler_block(_app_js(), "ai-review")
    assert "Review checks:" not in block
    assert "available in desktop" not in block.lower()


def test_web_mig_settings_not_desktop_stub():
    block = _handler_block(_app_js(), "mig-settings")
    assert "desktop UI" not in block


def test_web_mon_settings_not_desktop_stub():
    block = _handler_block(_app_js(), "mon-settings")
    assert "desktop UI" not in block


@pytest.mark.parametrize(
    "route,method",
    [
        ("/api/dashboard/layout/reset", "post"),
        ("/api/migrator/config", "get"),
        ("/api/ai/review", "post"),
        ("/api/ai/cache/show", "get"),
        ("/api/ai/config", "get"),
        ("/api/monitor/config", "get"),
        ("/api/monitor/connections/saved", "get"),
        ("/api/monitor/db-connections", "get"),
        ("/api/cloud/connections", "get"),
        ("/api/config/settings", "get"),
        ("/api/ai/sessions", "get"),
        ("/api/dashboard/layout", "get"),
    ],
)
def test_web_backend_exposes_wired_routes(route: str, method: str):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    if method == "get":
        r = c.get(route + ("?connection=" if "cache/show" in route else ""))
    else:
        body = {"sql": "SELECT 1"} if route.endswith("/review") else None
        r = c.post(route, json=body)
    assert r.status_code != 404, f"{method.upper()} {route} not mounted on web backend"


def test_settings_write_route_roundtrip(tmp_path, monkeypatch):
    """POST /api/config/settings persists a curated value end-to-end."""
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.ui.web.backend import build_web_backend
    from common.config import settings_service as S

    specs = S.describe_all(redact=True)
    editable = next(
        (s for s in specs if not s.get("secret") and not s.get("read_only")
         and (s.get("type") in (None, "str", "string", "text"))), None)
    if editable is None:
        pytest.skip("no editable curated setting available")
    sid = editable.get("id") or editable.get("key")
    current = str(editable.get("value", editable.get("current", "")))

    c = TestClient(build_web_backend())
    r = c.post("/api/config/settings", json={"values": {sid: current}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("ok") is True
    assert sid in (body.get("saved") or [])


def test_settings_read_now_writable():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    c = TestClient(build_web_backend())
    body = c.get("/api/config/settings").json()
    assert body.get("writable") is True


def test_migrator_apply_accepts_index_and_drop_flags():
    """The migrator apply route model accepts the Tk parity options."""
    from schema_converter.api import SchemaApplyRequest

    req = SchemaApplyRequest(target_conn="t", ddl="CREATE TABLE x(id int);",
                             create_indexes=False, drop_if_exists=True)
    assert req.create_indexes is False
    assert req.drop_if_exists is True


def test_bridge_apply_drop_and_index_filtering():
    """apply_ddl_to_target honours create_indexes/drop_if_exists via a fake core."""
    from schema_converter.bridge import SchemaBridge

    executed: list[str] = []

    class _FakeCore:
        def _split_sql_statements(self, ddl):
            return [s.strip() for s in ddl.split(";") if s.strip()]

        def execute(self, conn, sql):
            executed.append(sql)
            return {}

    bridge = SchemaBridge.__new__(SchemaBridge)
    bridge._core = _FakeCore()
    ddl = "CREATE TABLE users(id int); CREATE INDEX ix ON users(id);"
    bridge.apply_ddl_to_target("t", ddl, create_indexes=False, drop_if_exists=True)

    joined = " | ".join(executed)
    assert "DROP TABLE IF EXISTS users" in joined
    assert "CREATE TABLE users" in joined
    assert "CREATE INDEX" not in joined  # index filtered out
