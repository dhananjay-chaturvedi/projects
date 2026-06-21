from __future__ import annotations

import subprocess

import pytest


def test_monitor_ssh_password_not_in_argv_and_host_key_checked(monkeypatch, tmp_path):
    from monitoring.service import MonitorService

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    monkeypatch.setattr("shutil.which", lambda name: "/usr/bin/sshpass")
    seen = {}

    class _Mgr:
        def get_connection(self, _name):
            return {"host": "example.test", "username": "alice", "password": "top-secret"}

    def _run(cmd, **kwargs):
        seen["cmd"] = cmd
        seen["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    svc = MonitorService(None)
    monkeypatch.setattr(svc, "_monitor_mgr", lambda: _Mgr())
    monkeypatch.setattr(subprocess, "run", _run)

    assert svc.test_monitor_ssh("ssh-target")["ok"] is True
    assert "top-secret" not in seen["cmd"]
    assert seen["cmd"][:3] == ["sshpass", "-e", "ssh"]
    assert seen["env"]["SSHPASS"] == "top-secret"
    assert any("StrictHostKeyChecking=accept-new" in part for part in seen["cmd"])
    assert not any("UserKnownHostsFile=/dev/null" in part for part in seen["cmd"])


def test_ai_export_api_sandboxes_server_side_path(monkeypatch, tmp_path):
    fastapi = pytest.importorskip("fastapi")
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from ai_query.api import build_router

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    calls = {}

    class _Svc:
        def llm_export(self, path, *, include_sample=True, rag_connection=""):
            calls["path"] = path
            return {"ok": True, "path": path}

    app = FastAPI()
    app.include_router(build_router(_Svc()))
    client = TestClient(app)

    ok = client.post("/api/ai/llm/export", json={"path": "datasets/out.jsonl"})
    assert ok.status_code == 200
    assert calls["path"] == str(tmp_path / "exports" / "datasets" / "out.jsonl")

    bad = client.post("/api/ai/llm/export", json={"path": "../escape.jsonl"})
    assert bad.status_code == 400
    assert "exports" in bad.json()["detail"]


def test_ai_rag_document_path_uses_real_relative_check():
    from ai_query.api import build_router
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    class _Svc:
        def rag_add_document(self, *args, **kwargs):
            return {"ok": True}

    app = FastAPI()
    app.include_router(build_router(_Svc()))
    client = TestClient(app)
    resp = client.post(
        "/api/ai/rag/document",
        json={"scope": "s", "file_path": "/tmp/not-in-home.txt"},
    )
    assert resp.status_code == 400


def test_mysql_helpers_quote_identifiers_and_bind_like_pattern():
    from common.drivers import conMysql

    class _Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, query, params=None):
            self.calls.append((query, params))

        def fetchall(self):
            return []

        def close(self):
            pass

    class _Conn:
        def __init__(self):
            self.cursor_obj = _Cursor()

        def is_connected(self):
            return True

        def ping(self, **_kwargs):
            return None

        def cursor(self, buffered=True):
            return self.cursor_obj

    conn = _Conn()
    conMysql.getMysqlTables(conn, database="safe`db")
    conMysql.getMysqlIndexes(conn, "users", database="appdb")
    conMysql.getMysqlVariables(conn, pattern="version%'; DROP TABLE x; --")
    conMysql.getMysqlStatus(conn, pattern="Threads%'; DROP TABLE x; --")
    conMysql.getMysqlTableColumns(conn, "users", database="app`db")

    queries = conn.cursor_obj.calls
    assert queries[0] == ("SHOW TABLES FROM `safe``db`", None)
    assert queries[1] == ("SHOW INDEX FROM `appdb`.`users`", None)
    assert queries[2] == ("SHOW VARIABLES LIKE %s", ("version%'; DROP TABLE x; --",))
    assert queries[3] == ("SHOW STATUS LIKE %s", ("Threads%'; DROP TABLE x; --",))
    assert queries[4] == ("SHOW COLUMNS FROM `app``db`.`users`", None)


def test_mariadb_select_database_quotes_identifier():
    from common.drivers import conMariadb

    class _Cursor:
        def __init__(self):
            self.query = ""

        def execute(self, query):
            self.query = query

        def close(self):
            pass

    class _Conn:
        def __init__(self):
            self.cursor_obj = _Cursor()

        def cursor(self):
            return self.cursor_obj

    conn = _Conn()
    assert conMariadb.selectDatabase(conn, "safe`db") is True
    assert conn.cursor_obj.query == "USE `safe``db`"
