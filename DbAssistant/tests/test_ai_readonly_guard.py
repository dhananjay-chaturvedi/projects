"""AI surfaces must never execute mutating SQL (DROP/DELETE/UPDATE/...).

Covers the shared guard, the AI Query execution chokepoint, the no-session
``ai_execute_sql`` service path, app-builder schema deploy, and UI wiring across
Tk/Textual/Web.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from common.sql_guard import (
    AI_FORBIDDEN_KEYWORDS,
    assert_read_only,
    find_destructive,
    inspect_read_only,
)

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Shared guard classification                                                 #
# --------------------------------------------------------------------------- #

READ_ONLY = [
    "SELECT * FROM users LIMIT 10",
    "select id, name from t where x = 1",
    "WITH c AS (SELECT 1 AS n) SELECT n FROM c",
    "SHOW TABLES",
    "EXPLAIN SELECT * FROM t",
    "DESCRIBE users",
    "PRAGMA table_info(users)",
]

MUTATING = [
    "DROP TABLE users",
    "DELETE FROM users",
    "DELETE FROM users WHERE id = 1",
    "UPDATE users SET name = 'x'",
    "INSERT INTO users (id) VALUES (1)",
    "TRUNCATE TABLE users",
    "ALTER TABLE users ADD COLUMN x INT",
    "CREATE TABLE t (id INT)",
    "REPLACE INTO users (id) VALUES (1)",
    "GRANT ALL ON db.* TO 'u'@'%'",
    "  drop   table  users  ",
    "/* harmless */ DROP TABLE users",
    "-- comment\nDELETE FROM users",
    "SELECT 1; DROP TABLE users",
    "WITH d AS (DELETE FROM users RETURNING id) SELECT * FROM d",
    "UpDaTe users set a=1",
]


@pytest.mark.parametrize("sql", READ_ONLY)
def test_read_only_allowed(sql):
    ok, reason, offending = inspect_read_only(sql)
    assert ok, f"expected allowed: {sql!r} -> {reason}"
    assert offending == []
    assert assert_read_only(sql) == ""


@pytest.mark.parametrize("sql", MUTATING)
def test_mutating_blocked(sql):
    ok, reason, offending = inspect_read_only(sql)
    assert not ok, f"expected blocked: {sql!r}"
    assert offending, f"no offending keyword reported for {sql!r}"
    assert assert_read_only(sql) != ""


def test_empty_sql_blocked():
    ok, _, _ = inspect_read_only("")
    assert not ok
    ok, _, _ = inspect_read_only("   ")
    assert not ok


def test_explicit_three_in_forbidden_set():
    for kw in ("DROP", "DELETE", "UPDATE"):
        assert kw in AI_FORBIDDEN_KEYWORDS


def test_find_destructive():
    assert find_destructive("DROP TABLE x") == ["DROP"]
    assert find_destructive("CREATE TABLE IF NOT EXISTS t (id INT)") == []
    assert "DELETE" in find_destructive("CREATE TABLE t (id int); DELETE FROM t")


# --------------------------------------------------------------------------- #
# AI Query execution chokepoint                                               #
# --------------------------------------------------------------------------- #

class _FakeManager:
    db_type = "mysql"

    def __init__(self):
        self.calls: list[str] = []

    def execute_query(self, sql):
        self.calls.append(sql)
        return ([], None)


def test_execute_sql_after_gate_blocks_mutation():
    from ai_query.sql_execution_service import execute_sql_after_gate

    mgr = _FakeManager()
    out = execute_sql_after_gate("DELETE FROM users", mgr, {})
    assert out["blocked"] is True
    assert out["allowed"] is False
    assert out["result"] is None
    assert mgr.calls == [], "mutating SQL must never reach execute_query"


def test_execute_sql_after_gate_allows_select():
    from ai_query.sql_execution_service import execute_sql_after_gate

    mgr = _FakeManager()
    out = execute_sql_after_gate("SELECT 1", mgr, {})
    assert not out.get("blocked")
    assert mgr.calls == ["SELECT 1"]


def test_execute_sql_with_rules_blocks_mutation():
    from ai_query.sql_execution_service import execute_sql_with_rules

    mgr = _FakeManager()
    out = execute_sql_with_rules("DROP TABLE x", mgr, sql_mode="open")
    assert out["blocked"] is True
    assert mgr.calls == []


def test_agent_execute_in_session_blocks_mutation():
    from ai_query.agent import AIQueryAgent

    class _ConnectedManager(_FakeManager):
        conn = object()

    agent = AIQueryAgent()
    sess = agent.sessions.create(connection_name="c1")
    mgr = _ConnectedManager()

    out = agent.execute_in_session(sess.session_id, "DELETE FROM users", mgr)

    assert out["blocked"] is True
    assert out["result"]["blocked"] is True
    assert mgr.calls == [], "mutating SQL must never reach execute_query"


# --------------------------------------------------------------------------- #
# No-session AI execute service path                                          #
# --------------------------------------------------------------------------- #

class _FakeCore:
    def __init__(self):
        self.executed: list[str] = []

    def get_connection_profile(self, name):
        return {"type": "mysql"}

    def open_connection(self, name):
        return {"ok": True}

    def execute(self, name, sql):
        self.executed.append(sql)
        return {"columns": [], "rows": [], "rowcount": 0}


def test_ai_execute_sql_blocks_mutation():
    # Import the actual service class dynamically to avoid import-name drift.
    import ai_query.service as svc_mod

    svc_cls = None
    for attr in vars(svc_mod).values():
        if isinstance(attr, type) and hasattr(attr, "ai_execute_sql"):
            svc_cls = attr
            break
    assert svc_cls is not None, "service class with ai_execute_sql not found"

    core = _FakeCore()
    svc = svc_cls(core)
    out = svc.ai_execute_sql("c1", "UPDATE users SET x = 1")
    assert out.get("blocked") is True
    assert core.executed == [], "mutating SQL must never reach core.execute"

    out2 = svc.ai_execute_sql("c1", "SELECT 1")
    assert core.executed == ["SELECT 1"]


# --------------------------------------------------------------------------- #
# App builder schema deploy is additive-only and refuses destructive          #
# --------------------------------------------------------------------------- #

def test_schema_deploy_refuses_destructive():
    from ai_assistant.app_builder import schema_deploy

    class _M:
        def __init__(self):
            self.calls = []

        def execute_query(self, sql):
            self.calls.append(sql)
            return ([], None)

    # extract_ddl only keeps CREATE TABLE IF NOT EXISTS, so a DROP in schema.sql
    # is already filtered; deploy must still never run a destructive statement.
    files = {
        "src/db/schema.sql": (
            "CREATE TABLE IF NOT EXISTS users (id INT);\n"
            "DROP TABLE old_users;\n"
        )
    }
    mgr = _M()
    report = schema_deploy.deploy_schema(mgr, files)
    assert all("DROP" not in c.upper() for c in mgr.calls)
    assert report["executed"] == 1


# --------------------------------------------------------------------------- #
# UI wiring parity                                                            #
# --------------------------------------------------------------------------- #

def test_web_ai_exec_uses_guarded_endpoint():
    app_js = (ROOT / "common/ui/web/static/app.js").read_text(encoding="utf-8")
    m = re.search(r'#ai-exec"\)\.addEventListener\("click".*?\}\);', app_js, re.S)
    assert m, "could not locate #ai-exec handler"
    handler = m.group(0)
    assert "/api/ai/execute-sql" in handler
    assert '"/api/query"' not in handler


def test_textual_ai_execute_has_guard():
    src = (ROOT / "common/ui/textual/screens/ai_query.py").read_text(encoding="utf-8")
    assert "assert_read_only" in src


def test_api_route_registered():
    src = (ROOT / "ai_query/api.py").read_text(encoding="utf-8")
    assert '/api/ai/execute-sql' in src
