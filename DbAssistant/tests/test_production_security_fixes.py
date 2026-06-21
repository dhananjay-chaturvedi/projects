"""Regression tests for mass-production security hardening."""

from __future__ import annotations

from pathlib import Path

import pytest

from common.security.paths import (
    PathEscapeError,
    assert_safe_name,
    assert_safe_relative_file,
    resolve_under,
    resolve_user_path,
)


def test_assert_safe_name_rejects_traversal():
    with pytest.raises(PathEscapeError):
        assert_safe_name("../../escape")


def test_resolve_under_stays_in_root(tmp_path: Path):
    root = tmp_path / "apps"
    root.mkdir()
    out = resolve_under(root, "demo")
    assert out == (root / "demo").resolve()


def test_resolve_user_path_sandbox(tmp_path: Path):
    root = tmp_path / "exports"
    root.mkdir()
    allowed = resolve_user_path(root, "demo.csv")
    allowed.parent.mkdir(parents=True, exist_ok=True)
    allowed.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(PathEscapeError):
        resolve_user_path(root, str(tmp_path / "outside.csv"))


def test_assert_safe_relative_file_rejects_parent_segments():
    with pytest.raises(PathEscapeError):
        assert_safe_relative_file("../etc/passwd")


@pytest.mark.parametrize("sql", [
    "SELECT * FROM users INTO OUTFILE '/tmp/x.csv'",
    "SELECT col INTO new_table FROM old_table",
])
def test_mutating_select_side_effects_blocked(sql):
    from common.sql_guard import inspect_read_only

    ok, _, offending = inspect_read_only(sql)
    assert not ok
    assert offending


def test_app_builder_workspace_rejects_traversal():
    from ai_assistant.app_builder.service import AppBuilderService

    svc = AppBuilderService()
    with pytest.raises(PathEscapeError):
        svc._workspace("../../outside")


def test_llm_model_dir_rejects_traversal():
    from ai_assistant.llm.service import LlmService

    svc = LlmService()
    with pytest.raises(PathEscapeError):
        svc._model_dir("../outside")


def test_export_table_rejects_path_outside_exports(tmp_path, monkeypatch):
    from common.headless.db_service import CoreDBService
    from common import paths as app_paths

    monkeypatch.setattr(app_paths, "exports_dir", lambda: tmp_path / "exports")
    app_paths.exports_dir().mkdir(parents=True, exist_ok=True)

    svc = CoreDBService()

    class _Mgr:
        db_type = "sqlite"

        def execute_query(self, sql):
            return ({"columns": ["a"], "rows": [[1]]}, None)

    monkeypatch.setattr(svc, "_get_or_connect", lambda _name: _Mgr())
    out = svc.export_table("c", "users", str(tmp_path / "escape.csv"))
    assert out["ok"] is False


def test_web_backend_installs_http_guards():
    from common.ui.web.backend import build_web_backend

    app = build_web_backend()
    middlewares = [getattr(m, "cls", None) for m in app.user_middleware]
    assert any(m is not None for m in middlewares)
