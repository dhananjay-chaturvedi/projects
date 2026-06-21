from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


def test_api_key_create_verify_revoke_and_hash_storage(monkeypatch, tmp_path):
    from common.security import api_keys

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    created = api_keys.create_key("test")
    assert created["token"].startswith(created["key_id"] + ".")
    assert api_keys.verify_token(created["token"])["key_id"] == created["key_id"]

    raw = json.loads(api_keys.store_path().read_text(encoding="utf-8"))
    stored = raw["keys"][0]
    assert created["secret"] not in json.dumps(raw)
    assert stored["secret_hash"] and stored["salt"]

    assert api_keys.revoke_key(created["key_id"])["ok"] is True
    assert api_keys.verify_token(created["token"]) is None


def test_api_key_regenerate_invalidates_old_secret(monkeypatch, tmp_path):
    from common.security import api_keys

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    created = api_keys.create_key("regen")
    regenerated = api_keys.regenerate_key(created["key_id"])
    assert regenerated["ok"] is True
    assert regenerated["token"] != created["token"]
    assert api_keys.verify_token(created["token"]) is None
    assert api_keys.verify_token(regenerated["token"])["key_id"] == created["key_id"]


def test_public_api_accepts_kms_key_and_rejects_missing(monkeypatch, tmp_path):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    from common.security import api_keys
    from tests.test_headless_api import _DummySvc

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    token = api_keys.create_key("api")["token"]
    client = TestClient(create_app(svc=_DummySvc()))

    assert client.get("/api/health").status_code == 200
    assert client.get("/api/connections").status_code == 401
    ok = client.get("/api/connections", headers={"X-API-Key": token})
    assert ok.status_code == 200
    who = client.get("/api/auth/whoami", headers={"Authorization": f"Bearer {token}"})
    assert who.status_code == 200
    assert who.json()["authenticated"] is True


def test_apikey_cli_create_list_revoke(monkeypatch, tmp_path):
    env = {**os.environ, "DBASSISTANT_HOME": str(tmp_path), "PYTHONPATH": os.getcwd()}
    create = subprocess.run(
        [sys.executable, "app/dbtool.py", "apikey", "create", "--name", "cli", "--format", "json"],
        text=True, capture_output=True, env=env, check=True,
    )
    created = json.loads(create.stdout)
    assert created["token"].startswith(created["key_id"] + ".")

    listed = subprocess.run(
        [sys.executable, "app/dbtool.py", "apikey", "list", "--format", "json"],
        text=True, capture_output=True, env=env, check=True,
    )
    keys = json.loads(listed.stdout)
    assert keys[0]["key_id"] == created["key_id"]
    assert "secret" not in json.dumps(keys)

    revoke = subprocess.run(
        [sys.executable, "app/dbtool.py", "apikey", "revoke", created["key_id"], "--format", "json"],
        text=True, capture_output=True, env=env, check=True,
    )
    assert json.loads(revoke.stdout)["ok"] is True


def test_api_key_management_wired_across_ui_surfaces():
    root = os.getcwd()
    tk_settings = open(os.path.join(root, "common/ui/tk/settings_ui.py"), encoding="utf-8").read()
    textual_settings = open(
        os.path.join(root, "common/ui/textual/screens/settings.py"), encoding="utf-8"
    ).read()
    web_index = open(os.path.join(root, "common/ui/web/static/index.html"), encoding="utf-8").read()
    web_js = open(os.path.join(root, "common/ui/web/static/app.js"), encoding="utf-8").read()
    web_backend = open(os.path.join(root, "common/ui/web/backend.py"), encoding="utf-8").read()

    assert "Access Keys" in tk_settings and "_apikey_create" in tk_settings
    assert "Create API key" in textual_settings and "_apikey_create" in textual_settings
    assert "settings-access-keys" in web_index and "apikey-create" in web_index
    assert "loadApiKeys" in web_js and "/ui/apikeys" in web_js
    assert "ui_apikey_create" in web_backend and "api_keys.create_key" in web_backend
