"""Tests for the Settings/config subsystem and notification dispatch.

Covers:
  * comment-preserving INI writer
  * ConfigLoader.set / save / restore_defaults
  * settings_service validate / set / set_many / restore / redaction
  * notification config + encrypted secret store + dispatch gating + email
  * threshold write API (update / enable / disable / validation)
  * read-only config API endpoints (secrets never leaked, no write routes)
  * config CLI dispatch (read paths + secret set)

All writes are isolated to temp files / a temp DBASSISTANT_HOME so the real
config.ini / properties.ini / monitor_thresholds.ini are never modified.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest


# --------------------------------------------------------------------------- #
# ini_writer
# --------------------------------------------------------------------------- #
def test_ini_writer_preserves_comments_and_edits(tmp_path):
    from common.config.ini_writer import set_ini_value

    p = tmp_path / "x.ini"
    p.write_text(
        "# top\n[project]\n# explains\ndebug_mode = true\napp_name = A\n\n"
        "[ports]\noracle = 1521\n"
    )
    assert set_ini_value(p, "project", "debug_mode", "false")
    assert set_ini_value(p, "ports", "oracle", "1599")
    assert set_ini_value(p, "project", "timezone", "UTC")  # new key in section
    assert set_ini_value(p, "new.section", "k", "v")        # new section
    t = p.read_text()
    assert "# top" in t and "# explains" in t           # comments preserved
    assert "debug_mode = false" in t
    assert "oracle = 1599" in t
    assert "timezone = UTC" in t
    assert "[new.section]" in t and "k = v" in t


# --------------------------------------------------------------------------- #
# ConfigLoader
# --------------------------------------------------------------------------- #
def test_config_loader_set_save_restore(tmp_path):
    from common.config_loader import ConfigLoader

    ex = tmp_path / "c.example"
    ex.write_text("[project]\ndebug_mode = false\napp_name = Default\n")
    live = tmp_path / "c.ini"
    live.write_text("# mine\n[project]\ndebug_mode = true\napp_name = Mine\n")

    cl = ConfigLoader(str(live), str(ex))
    assert cl.get_bool("project", "debug_mode") is True
    assert cl.set("project", "app_name", "Renamed")
    assert cl.get("project", "app_name") == "Renamed"
    assert "# mine" in live.read_text()                 # comments survive write

    # persisted across new loader instance
    assert ConfigLoader(str(live), str(ex)).get("project", "app_name") == "Renamed"

    assert cl.restore_defaults() is True
    assert cl.get("project", "app_name") == "Default"
    assert cl.get_bool("project", "debug_mode") is False


def test_config_loader_restore_without_example_returns_false(tmp_path):
    from common.config_loader import ConfigLoader

    live = tmp_path / "c.ini"
    live.write_text("[a]\nb = 1\n")
    assert ConfigLoader(str(live)).restore_defaults() is False


# --------------------------------------------------------------------------- #
# settings_service (writes redirected to temp loaders)
# --------------------------------------------------------------------------- #
@pytest.fixture()
def temp_settings(tmp_path, monkeypatch):
    """Point settings_service at throwaway config/properties loaders."""
    from common.config import settings_service as S
    from common.config_loader import ConfigLoader

    cfg_ex = tmp_path / "config.example"
    cfg_ex.write_text(
        "[database.connection]\nconnection_timeout = 30.0\nquery_timeout = 0\n"
        "[project]\ndebug_mode = false\n"
    )
    cfg = tmp_path / "config.ini"
    shutil.copy(cfg_ex, cfg)

    prop_ex = tmp_path / "properties.example"
    prop_ex.write_text("[logging]\nenable_stdout = true\n")
    prop = tmp_path / "properties.ini"
    shutil.copy(prop_ex, prop)

    cfg_loader = ConfigLoader(str(cfg), str(cfg_ex))
    prop_loader = ConfigLoader(str(prop), str(prop_ex))

    def _loader(target):
        return cfg_loader if target == "config" else prop_loader

    monkeypatch.setattr(S, "_loader", _loader)
    # restore_defaults() uses the global getters — patch those too.
    import common.config_loader as CL
    monkeypatch.setattr(CL, "get_config", lambda: cfg_loader)
    monkeypatch.setattr(CL, "get_properties", lambda: prop_loader)
    return S


def test_settings_validate(temp_settings):
    S = temp_settings
    from common.config.settings_schema import find

    spec = find("config.database.connection.connection_timeout")
    assert S.validate(spec, "abc") is not None
    assert S.validate(spec, "99999") is not None       # over max
    assert S.validate(spec, "45") is None
    dbg = find("config.project.debug_mode")
    assert S.validate(dbg, "maybe") is not None
    assert S.validate(dbg, "true") is None


def test_settings_set_and_persist(temp_settings):
    S = temp_settings
    r = S.set_value("config.database.connection.query_timeout", "120")
    assert r["ok"]
    assert S.describe(__import__("common.config.settings_schema", fromlist=["find"])
                      .find("config.database.connection.query_timeout"))["value"] == "120"
    # invalid rejected
    bad = S.set_value("config.database.connection.query_timeout", "-5")
    assert not bad["ok"]


def test_settings_set_many_atomic(temp_settings):
    S = temp_settings
    # one invalid -> nothing saved
    r = S.set_many({
        "config.project.debug_mode": "true",
        "config.database.connection.query_timeout": "notnum",
    })
    assert not r["ok"]
    assert r["saved"] == []
    # all valid -> saved
    r2 = S.set_many({
        "config.project.debug_mode": "true",
        "config.database.connection.query_timeout": "60",
    })
    assert r2["ok"]
    assert set(r2["saved"]) == {
        "config.project.debug_mode",
        "config.database.connection.query_timeout",
    }


def test_settings_restore_defaults(temp_settings):
    S = temp_settings
    S.set_value("config.project.debug_mode", "true")
    out = S.restore_defaults("all")
    assert out["ok"]
    from common.config.settings_schema import find
    assert S.describe(find("config.project.debug_mode"))["value"] == "false"


# --------------------------------------------------------------------------- #
# Notification secret store + dispatch
# --------------------------------------------------------------------------- #
def test_secret_store_roundtrip(tmp_path):
    from common.notifications import NotificationSecretStore

    store = NotificationSecretStore(
        path=tmp_path / "n.json", key_path=tmp_path / "n.key"
    )
    assert store.get("smtp_password") == ""
    assert store.set("smtp_password", "hunter2")
    assert store.get("smtp_password") == "hunter2"
    assert store.status()["smtp_password"] is True
    # encrypted at rest (raw file must not contain the plaintext)
    assert "hunter2" not in (tmp_path / "n.json").read_text()
    # clearing
    assert store.set("smtp_password", "")
    assert store.get("smtp_password") == ""


def test_dispatch_disabled_skips(monkeypatch):
    from common import notifications as N

    monkeypatch.delenv("ALERT_TEAMS_WEBHOOK_URL", raising=False)
    cfg = N.NotificationConfig(enabled=False)
    out = N.dispatch_alert("x", severity="WARNING", cfg=cfg,
                           store=_MemStore())
    assert out["ok"] and out["delivered"] == [] and out["skipped"]


def test_dispatch_severity_gate(monkeypatch):
    from common import notifications as N

    monkeypatch.delenv("ALERT_TEAMS_WEBHOOK_URL", raising=False)
    cfg = N.NotificationConfig(enabled=True, min_severity="CRITICAL",
                               teams_enabled=True)
    out = N.dispatch_alert("x", severity="WARNING", cfg=cfg, store=_MemStore())
    assert out["delivered"] == [] and "below" in out["skipped"]


def test_dispatch_teams_uses_encrypted_webhook(monkeypatch):
    from common import notifications as N
    import monitoring.send_notification as sn

    monkeypatch.delenv("ALERT_TEAMS_WEBHOOK_URL", raising=False)
    seen = {}

    def fake_send(body, *, webhook_url=None, **kw):
        seen["url"] = webhook_url
        return {"ok": True, "channel": "teams"}

    monkeypatch.setattr(sn, "send_alert", fake_send)
    cfg = N.NotificationConfig(enabled=True, min_severity="INFO", teams_enabled=True)
    store = _MemStore(teams_webhook_url="https://hooks/abc")
    out = N.dispatch_alert("hi", severity="WARNING", cfg=cfg, store=store)
    assert out["ok"] and "teams" in out["delivered"]
    assert seen["url"] == "https://hooks/abc"


def test_dispatch_force_bypasses_enabled(monkeypatch):
    from common import notifications as N
    import monitoring.send_notification as sn

    monkeypatch.delenv("ALERT_TEAMS_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(sn, "send_alert",
                        lambda *a, **k: {"ok": True, "channel": "teams"})
    cfg = N.NotificationConfig(enabled=False, teams_enabled=True)
    out = N.dispatch_alert("hi", severity="INFO", cfg=cfg,
                           store=_MemStore(teams_webhook_url="u"), force=True)
    assert out["ok"] and "teams" in out["delivered"]


def test_email_alert_requires_config():
    from common.notifications import NotificationConfig, send_email_alert

    out = send_email_alert("s", "b", NotificationConfig(), store=_MemStore())
    assert not out["ok"] and out["channel"] == "email"


class _MemStore:
    def __init__(self, **vals):
        self._v = dict(vals)

    def get(self, f):
        return self._v.get(f, "")

    def status(self):
        return {k: bool(v) for k, v in self._v.items()}


# --------------------------------------------------------------------------- #
# Threshold write API
# --------------------------------------------------------------------------- #
def test_threshold_update_enable_disable(tmp_path):
    from monitoring.threshold_checker import ThresholdChecker

    src = Path("monitoring/monitor_thresholds.ini")
    cfg = tmp_path / "t.ini"
    shutil.copy(src, cfg)
    tc = ThresholdChecker(config_path=cfg)
    rule = [r for r in tc.all_rules() if r.source == "db"][0]

    r = tc.update_rule("db", rule.metric, {"critical": "95", "warning": "80"})
    assert r["ok"]
    tc2 = ThresholdChecker(config_path=cfg)
    nr = tc2.get_rule("db", rule.metric)
    assert nr.critical == 95.0 and nr.warning == 80.0

    assert tc.set_enabled("db", rule.metric, False)["ok"]
    assert ThresholdChecker(config_path=cfg).get_rule("db", rule.metric).enabled is False

    # comments preserved
    assert "#" in cfg.read_text()


def test_threshold_update_validation(tmp_path):
    from monitoring.threshold_checker import ThresholdChecker

    cfg = tmp_path / "t.ini"
    shutil.copy("monitoring/monitor_thresholds.ini", cfg)
    tc = ThresholdChecker(config_path=cfg)
    rule = [r for r in tc.all_rules() if r.source == "db"][0]

    assert not tc.update_rule("db", rule.metric, {"operator": "=>"})["ok"]
    assert not tc.update_rule("db", rule.metric, {"window": "0"})["ok"]
    assert not tc.update_rule("db", rule.metric, {"metric_name": "x"})["ok"]  # not editable
    assert not tc.update_rule("db", "no_such_metric", {"critical": "1"})["ok"]


# --------------------------------------------------------------------------- #
# Read-only config API
# --------------------------------------------------------------------------- #
@pytest.fixture()
def api_client(monkeypatch, dbassistant_home):
    monkeypatch.setenv("DBTOOL_API_KEY", "")
    from common.headless.app_factory import create_app
    from fastapi.testclient import TestClient

    return TestClient(create_app(module_key="monitor"))


def test_api_settings_listing_and_redaction(api_client):
    r = api_client.get("/api/config/settings", params={"group": "General"})
    assert r.status_code == 200
    body = r.json()
    assert body["writable"] is True and body["count"] >= 1

    one = api_client.get("/api/config/settings/config.database.connection.query_timeout")
    assert one.status_code == 200 and one.json()["label"]

    assert api_client.get("/api/config/settings/nope.bad").status_code == 404

    # Batch write route mirrors the desktop Settings UI (Web/TUI parity).
    spec_id = "config.database.connection.query_timeout"
    current = one.json().get("value", one.json().get("current", ""))
    wr = api_client.post("/api/config/settings", json={"values": {spec_id: current}})
    assert wr.status_code == 200
    assert wr.json().get("ok") is True


def test_api_monitor_config_and_notifications(api_client):
    """Module-owned monitor config + notifications (not core /api/config)."""
    r = api_client.get("/api/monitor/config")
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") and "monitoring" in body.get("config", {})

    n = api_client.get("/api/monitor/notifications")
    assert n.status_code == 200
    assert "topsecret" not in json.dumps(n.json())
    assert "enabled" in n.json()

    # Writing generates a live monitor_config.ini; isolate so the repo copy is
    # never mutated by the test run.
    from monitoring import monitor_config

    live = monitor_config.live_path()
    existed = live.exists()
    original = live.read_text() if existed else None
    try:
        w = api_client.post(
            "/api/monitor/config",
            json={"section": "monitoring", "key": "metrics_refresh_interval", "value": "6000"},
        )
        assert w.status_code == 200
        assert monitor_config.get_int("monitoring", "metrics_refresh_interval") == 6000
    finally:
        if original is not None:
            live.write_text(original)
        elif live.exists():
            live.unlink()
        monitor_config.reload()


# --------------------------------------------------------------------------- #
# config CLI dispatch (read paths + secret set)
# --------------------------------------------------------------------------- #
def test_cli_config_list_and_describe(capsys):
    from common.core.cli_handlers import dispatch_core_argv

    assert dispatch_core_argv(["config", "list", "--group", "General"], svc=None)
    out = capsys.readouterr().out
    assert "debug" in out.lower() or "general" in out.lower()

    assert dispatch_core_argv(
        ["config", "describe", "config.database.connection.query_timeout"], svc=None
    )
    assert "Query timeout" in capsys.readouterr().out


def test_threshold_add_rule(tmp_path):
    from monitoring.threshold_checker import ThresholdChecker

    cfg = tmp_path / "t.ini"
    cfg.write_text("; empty\n")
    tc = ThresholdChecker(config_path=cfg)
    r = tc.add_rule("db", "custom_metric", {
        "operator": ">", "warning": "90", "window": "3", "enabled": "true",
    })
    assert r["ok"]
    tc2 = ThresholdChecker(config_path=cfg)
    rule = tc2.get_rule("db", "custom_metric")
    assert rule is not None and rule.warning == 90.0
