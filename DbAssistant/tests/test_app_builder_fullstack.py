"""The App Builder must produce a real, runnable full-stack app (API + UI).

These tests go past "files exist": they build through the service, then run the
*generated app's own* pytest suite in a subprocess (which boots the FastAPI app
via TestClient and exercises the JSON API + HTML UI). This is the meaning of
"working app" — it starts and serves with no external database.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ai_assistant.app_builder.requirements import (
    derive_spec,
    detect_features,
)
from ai_assistant.app_builder.service import AppBuilderService
from ai_assistant.app_builder.spec import AppSpec, Entity
from ai_assistant.app_builder.webapp import generate_app


@pytest.fixture
def home(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    return tmp_path


def _run_generated_tests(workspace: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, "APP_DB_PATH": ":memory:"}
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        cwd=workspace, capture_output=True, text=True, timeout=120, env=env,
    )


# ── generator unit-level guarantees ──────────────────────────────────────────
def test_generated_app_is_full_stack():
    spec = AppSpec(
        app_name="shop",
        entities=[Entity("customers", ["id", "name", "email"])],
        services=["monitoring", "document"],
    )
    files = generate_app(spec)
    # API + UI + data + infra + tests all present.
    for required in (
        "src/app.py", "src/api.py", "src/web.py", "src/repository.py",
        "src/db/schema.py", "src/db/connection.py", "templates/base.html",
        "templates/index.html", "templates/list.html", "templates/form.html",
        "static/style.css", "tests/test_app.py", "config/infra.yaml",
        "src/infra/monitoring.py", "src/infra/document.py",
    ):
        assert required in files, required
    # Every generated Python file is syntactically valid.
    for path, content in files.items():
        if path.endswith(".py"):
            ast.parse(content)
    # API exposes per-entity CRUD; UI exposes routed pages.
    assert '@router.get("/customers")' in files["src/api.py"]
    assert '@router.post("/customers")' in files["src/api.py"]
    assert 'TemplateResponse' in files["src/web.py"]


def test_generated_app_ships_standardized_skeleton():
    """Every app ships the test taxonomy + docs skeleton on a clean structure."""
    from ai_assistant.app_builder.engine import (
        SKELETON_DOC_FILES,
        SKELETON_TEST_FILES,
        TEST_TAXONOMY,
    )

    spec = AppSpec(
        app_name="shop",
        description="manage products",
        entities=[Entity("products", ["id", "name", "price"])],
        services=["monitoring"],
    )
    files = generate_app(spec)
    # Each standardized test folder is a real package.
    for d in TEST_TAXONOMY:
        assert f"tests/{d}/__init__.py" in files, d
    # Sample data + docs skeleton are present.
    for required in SKELETON_TEST_FILES + SKELETON_DOC_FILES:
        assert required in files, required
    # Sample-data fixtures reference the real entity and expose rows_for().
    sample = files["tests/test_sample_data/sample_data.py"]
    assert "products" in sample and "def rows_for" in sample
    ast.parse(sample)
    # The requirement doc captures the description for Session B/C to read.
    assert "manage products" in files["docs/requirement.txt"]


def test_sample_data_does_not_inflate_test_coverage():
    """The sample-data folder must not count as per-entity test coverage."""
    from ai_assistant.meters import RequirementCoverageMeter

    spec = AppSpec(
        app_name="shop",
        entities=[Entity("alphas", ["id", "x"]), Entity("betas", ["id", "y"])],
        features=["list", "create"],
    )
    files = generate_app(spec)
    # Drop every real reference to "betas" from the test suite, leaving only the
    # sample-data fixtures mentioning it.
    files["tests/test_app.py"] = "\n".join(
        ln for ln in files["tests/test_app.py"].splitlines() if "betas" not in ln)
    m = RequirementCoverageMeter().measure(
        entities=["alphas", "betas"], features=["list", "create"], files=files)
    assert m.evidence["per_entity"]["betas"]["tests"] is False


def test_generated_app_runs_and_serves(tmp_path):
    """Build a from-scratch app and run its own test suite (boots the server)."""
    spec = derive_spec(app_name="notes", description="manage notes and tags")
    files = generate_app(spec)
    ws = tmp_path / "ws"
    for path, content in files.items():
        fp = ws / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content, encoding="utf-8")
    result = _run_generated_tests(ws)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "passed" in result.stdout


def test_generated_app_seeds_sample_data_on_empty(tmp_path):
    """A freshly launched app demonstrates flows with seeded data, not blank lists.

    Boots the generated app in a subprocess (isolated from the test process) and
    confirms its list endpoint returns seeded sample rows so the UI is functional
    on first launch even when the database does not already cover the app.
    """
    spec = derive_spec(app_name="tasks", description="manage tasks")
    files = generate_app(spec)
    ws = tmp_path / "ws"
    for path, content in files.items():
        fp = ws / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content, encoding="utf-8")
    table = spec.entities[0].table
    check = (
        "import os, json\n"
        "os.environ['APP_DB_PATH'] = ':memory:'\n"
        "from fastapi.testclient import TestClient\n"
        "from src.app import app\n"
        "c = TestClient(app)\n"
        f"rows = c.get('/api/{table}').json()\n"
        "print(json.dumps({'count': len(rows), "
        "'seeded': any('sample' in str(v) for r in rows for v in r.values())}))\n"
    )
    env = {**os.environ, "APP_DB_PATH": ":memory:"}
    result = subprocess.run(
        [sys.executable, "-c", check],
        cwd=ws, capture_output=True, text=True, timeout=120, env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["count"] >= 1
    assert payload["seeded"] is True


# ── requirements analysis ─────────────────────────────────────────────────────
def test_requirements_default_full_crud():
    # No explicit verbs -> full CRUD so the app is genuinely usable.
    assert detect_features("an app to manage books") == ["list", "create", "edit", "delete"]
    # Explicit verb is honored (plus listing).
    feats = detect_features("let users add and delete tickets")
    assert "create" in feats and "delete" in feats


def test_derive_spec_from_schema_builds_entities():
    spec = derive_spec(schema={"customers": ["id", "email"], "orders": ["id", "total"]})
    tables = sorted(e.table for e in spec.entities)
    assert tables == ["customers", "orders"]


# ── end-to-end through the service ───────────────────────────────────────────
def test_service_build_from_database_runs(home):
    schema = {"products": ["id", "name", "price"], "reviews": ["id", "product_id", "stars"]}
    r = AppBuilderService().build({
        "name": "catalog", "mode": "from_database", "schema": schema,
        "description": "manage products and reviews",
        "services": ["monitoring", "ci_cd", "database", "hosting"],
    })
    assert r["ok"] is True, r["verdict"]
    ws = Path(r["workspace"])
    result = _run_generated_tests(ws)
    assert result.returncode == 0, result.stdout + result.stderr
