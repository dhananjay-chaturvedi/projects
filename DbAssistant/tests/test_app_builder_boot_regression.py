"""Regression tests for App Builder FROM_DATABASE launchability."""

from __future__ import annotations

from pathlib import Path

from ai_assistant.app_builder import preflight
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from ai_assistant.app_builder.requirements import derive_spec
from ai_assistant.app_builder.service import AppBuilderService
from ai_assistant.app_builder.webapp import generate_app
from ai_assistant.app_builder.workspace_contract import reconcile_data_layer


def _write_files(workspace: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        path = workspace / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def test_reconcile_removes_db_module_collision_and_restores_boot(tmp_path: Path) -> None:
    spec = derive_spec(
        app_name="catalog",
        schema={"products": ["id", "name", "price"]},
        description="manage products",
    )
    baseline = generate_app(spec)
    ws = tmp_path / "ws"
    _write_files(ws, baseline)
    (ws / "src" / "db.py").write_text(
        "import os\n"
        "DB_PATH = os.environ.get('DBASSIST_DB_PATH', ':memory:')\n\n"
        "def init_db():\n"
        "    return None\n",
        encoding="utf-8",
    )
    (ws / "src" / "app.py").write_text(
        "from fastapi import FastAPI\n"
        "from src import db\n\n"
        "app = FastAPI()\n"
        "db.init_db()\n",
        encoding="utf-8",
    )

    before = preflight.boot_check(ws)
    assert not before.ok

    report = reconcile_data_layer(ws, baseline)

    assert report.changed
    assert not (ws / "src" / "db.py").exists()
    assert "src/app.py" in report.restored
    after = preflight.boot_check(ws)
    assert after.ok, after.digest()


def test_reconcile_removes_src_database_module_and_restores_app(tmp_path: Path) -> None:
    spec = derive_spec(
        app_name="catalog",
        schema={"products": ["id", "name", "price"]},
        description="manage products",
    )
    baseline = generate_app(spec)
    ws = tmp_path / "ws"
    _write_files(ws, baseline)
    (ws / "src" / "database.py").write_text(
        "import os\n"
        "import sqlite3\n"
        "DBASSIST_DB_PATH = os.environ.get('DBASSIST_DB_PATH', 'data.db')\n\n"
        "def init_db():\n"
        "    sqlite3.connect(DBASSIST_DB_PATH)\n",
        encoding="utf-8",
    )
    (ws / "src" / "app.py").write_text(
        "from fastapi import FastAPI\n"
        "from src.database import init_db\n\n"
        "app = FastAPI()\n"
        "init_db()\n",
        encoding="utf-8",
    )

    report = reconcile_data_layer(ws, baseline)

    assert report.changed
    assert not (ws / "src" / "database.py").exists()
    assert "src/database.py" in report.removed
    assert "src/app.py" in report.restored
    after = preflight.boot_check(ws)
    assert after.ok, after.digest()


def test_orchestrator_restores_minimal_launch_page_when_boot_fails(
    tmp_path: Path,
) -> None:
    spec = derive_spec(
        app_name="catalog",
        schema={"products": ["id", "name"]},
        description="manage products",
    )
    ws = tmp_path / "ws"
    _write_files(ws, generate_app(spec))
    (ws / "src" / "app.py").write_text(
        "raise RuntimeError('startup exploded')\n",
        encoding="utf-8",
    )
    assert not preflight.boot_check(ws).ok

    orch = AppBuildOrchestrator()
    blueprint = AppBlueprint(
        name="catalog",
        mode=BuildMode.FROM_DATABASE,
        connections=["selected_db"],
    )
    req = orch._request(blueprint, {"products": ["id", "name"]})
    orch._reconcile_data_layer_workspace(ws, blueprint, req)
    orch._maybe_stub_launch_fallback(ws, blueprint, None)

    boot = preflight.boot_check(ws)
    assert boot.ok, boot.digest()
    assert "Minimal launch fallback" in (ws / "src" / "app.py").read_text(
        encoding="utf-8")


def test_service_uses_only_selected_connection_and_reports_sqlite_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))

    class _Core:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def get_objects(self, connection: str, kind: str):
            self.calls.append((connection, kind))
            raise RuntimeError("selected connection unavailable")

    core = _Core()
    result = AppBuilderService(core).build({
        "name": "selected_only",
        "mode": "from_database",
        "connections": ["user_selected_db"],
        "description": "build an app from my selected DB",
    })

    assert core.calls == [("user_selected_db", "tables")]
    assert result["resolved_connection"] == "user_selected_db"
    assert result["introspection_status"]["ok"] is False
    assert result["introspection_status"]["runtime_fallback"] == "sqlite"
    assert "selected connection unavailable" in result["introspection_status"]["error"]
