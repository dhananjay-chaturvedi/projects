"""Deterministic compile + import dry-run gate (Session C's code-level check).

These tests prove that the App Builder catches the failure class that used to
only surface as a silent crash on "Start app": a generated app that does not
compile or import. They also verify the safe launch environment (local SQLite,
no DATABASE_URL) so a built prototype reliably boots.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_assistant.app_builder import preflight
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator


def _write_app(ws: Path, body: str) -> None:
    (ws / "src").mkdir(parents=True, exist_ok=True)
    (ws / "src" / "__init__.py").write_text("", encoding="utf-8")
    (ws / "src" / "app.py").write_text(body, encoding="utf-8")


_GOOD_APP = (
    "from fastapi import FastAPI\n"
    "app = FastAPI()\n"
    "@app.get('/health')\n"
    "def health():\n"
    "    return {'ok': True}\n"
)


def test_compile_check_in_memory_flags_syntax_error() -> None:
    errors = preflight.compile_check(files={"src/app.py": "def f(:\n    pass\n"})
    assert errors and "src/app.py" in errors[0]


def test_compile_check_passes_clean_code() -> None:
    assert preflight.compile_check(files={"src/app.py": _GOOD_APP}) == []


def test_launch_env_uses_local_sqlite_and_drops_database_url(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "mysql://user:pass@remote/db")
    env = preflight.launch_env(Path("/tmp/ws"))
    assert "DATABASE_URL" not in env
    assert env["APP_DB_PATH"].endswith("app.db")
    assert env["APP_WORKSPACE"] == "/tmp/ws"


def test_dry_run_detects_import_failure(tmp_path: Path) -> None:
    _write_app(tmp_path, "import this_module_does_not_exist_xyz\n" + _GOOD_APP)
    result = preflight.dry_run(tmp_path)
    assert result.compiled is True       # it parses fine
    assert result.imported is False      # but import explodes
    assert not result.ok
    assert result.issues()


def test_dry_run_passes_for_importable_app(tmp_path: Path) -> None:
    _write_app(tmp_path, _GOOD_APP)
    result = preflight.dry_run(tmp_path)
    assert result.compiled is True
    assert result.imported is True
    assert result.ok
    assert "src.app" in result.checked_modules


def test_dry_run_syntax_error_skips_import(tmp_path: Path) -> None:
    _write_app(tmp_path, "def broken(:\n")
    result = preflight.dry_run(tmp_path)
    assert result.compiled is False
    assert result.imported is False
    assert "skipped" in result.import_error


def test_evaluate_rejects_build_with_syntax_error() -> None:
    orch = AppBuildOrchestrator()
    blueprint = AppBlueprint(name="demo", mode=BuildMode.FROM_SCRATCH)
    files = {"src/app.py": "def broken(:\n", "requirements.txt": "fastapi\n"}
    verdict = orch._evaluate(blueprint, files)
    assert verdict.accepted is False
    assert any("syntax error" in i for i in verdict.issues)


# ── continuous (per-round) code gate, coordinated C→B→A ───────────────────────
class _StubVerdict:
    def feedback_text(self) -> str:
        return "round feedback"


def test_round_preflight_check_skips_without_app(tmp_path: Path) -> None:
    orch = AppBuildOrchestrator()
    assert orch._round_preflight_check(tmp_path) is None


def test_round_preflight_check_runs_when_app_present(tmp_path: Path) -> None:
    _write_app(tmp_path, _GOOD_APP)
    pf = AppBuildOrchestrator()._round_preflight_check(tmp_path)
    assert pf is not None and pf.ok
    # quick mode: app import is checked, per-module smoke is deferred to final
    assert pf.checked_modules == []


def test_forward_nudge_leads_with_code_gate_failure() -> None:
    orch = AppBuildOrchestrator()
    orch._round_preflight = preflight.PreflightResult(
        compiled=True, imported=False, import_error="NameError: boom")
    nudge = orch._forward_nudge(_StubVerdict(), {"gaps": []})
    assert "CODE GATE FAILED" in nudge
    assert "boom" in nudge


def test_forward_nudge_clean_when_code_gate_passes() -> None:
    orch = AppBuildOrchestrator()
    orch._round_preflight = preflight.PreflightResult()  # ok by default
    nudge = orch._forward_nudge(_StubVerdict(), {"gaps": []})
    assert "CODE GATE FAILED" not in nudge


# ── HTTP launch smoke (uvicorn + GET /health, /) ─────────────────────────────
_LAUNCH_APP = (
    "from fastapi import FastAPI\n"
    "from fastapi.responses import PlainTextResponse\n"
    "app = FastAPI()\n"
    "@app.get('/health')\n"
    "def health():\n"
    "    return {'ok': True}\n"
    "@app.get('/', response_class=PlainTextResponse)\n"
    "def root():\n"
    "    return 'ok'\n"
)


def test_http_smoke_passes_for_runnable_app(tmp_path: Path) -> None:
    _write_app(tmp_path, _LAUNCH_APP)
    result = preflight.http_smoke(tmp_path, timeout=30)
    if result.skipped:
        pytest.skip(result.skip_reason or "uvicorn unavailable")
    assert result.ok
    paths = {c["path"] for c in result.checks}
    assert "/health" in paths and "/" in paths


def test_http_smoke_fails_without_health_route(tmp_path: Path) -> None:
    body = (
        "from fastapi import FastAPI\n"
        "app = FastAPI()\n"
        "@app.get('/')\n"
        "def root():\n"
        "    return {'ok': True}\n"
    )
    _write_app(tmp_path, body)
    result = preflight.http_smoke(tmp_path, timeout=30)
    if result.skipped:
        pytest.skip(result.skip_reason or "uvicorn unavailable")
    assert not result.ok
    assert result.errors


def test_http_smoke_fails_on_5xx_route(tmp_path: Path) -> None:
    """A route that errors (5xx) is a broken flow and must fail the gate."""
    body = (
        "from fastapi import FastAPI\n"
        "from fastapi.responses import PlainTextResponse\n"
        "app = FastAPI()\n"
        "@app.get('/health')\n"
        "def health():\n"
        "    return {'ok': True}\n"
        "@app.get('/', response_class=PlainTextResponse)\n"
        "def root():\n"
        "    return 'home'\n"
        "@app.get('/boom')\n"
        "def boom():\n"
        "    raise RuntimeError('broken flow')\n"
    )
    _write_app(tmp_path, body)
    result = preflight.http_smoke(tmp_path, timeout=30)
    if result.skipped:
        pytest.skip(result.skip_reason or "uvicorn unavailable")
    assert not result.ok
    assert any("/boom" in e for e in result.errors)
    paths = {c["path"] for c in result.checks}
    assert "/boom" in paths  # the route crawl discovered and hit it


def test_http_smoke_fails_on_empty_index(tmp_path: Path) -> None:
    """A landing page that renders nothing is a non-functional UI."""
    body = (
        "from fastapi import FastAPI\n"
        "from fastapi.responses import PlainTextResponse\n"
        "app = FastAPI()\n"
        "@app.get('/health')\n"
        "def health():\n"
        "    return {'ok': True}\n"
        "@app.get('/', response_class=PlainTextResponse)\n"
        "def root():\n"
        "    return ''\n"
    )
    _write_app(tmp_path, body)
    result = preflight.http_smoke(tmp_path, timeout=30)
    if result.skipped:
        pytest.skip(result.skip_reason or "uvicorn unavailable")
    assert not result.ok
    assert any("empty page" in e for e in result.errors)


def test_http_smoke_skipped_when_uvicorn_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_app(tmp_path, _LAUNCH_APP)
    real_find_spec = preflight.importlib.util.find_spec

    def _no_uvicorn(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "uvicorn":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(preflight.importlib.util, "find_spec", _no_uvicorn)
    result = preflight.http_smoke(tmp_path)
    assert result.skipped
    assert result.ok  # skipped is not a failure
