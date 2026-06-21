"""The App Builder must build the *requested* app, not a CRUD mirror of tables.

For an ecommerce request it produces a real storefront (home, catalog, product
detail, cart, checkout placing orders) — verified by running the generated
app's own test suite — and uses the DB schema to *inform* the design rather than
simulating CRUD on every table.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode
from ai_assistant.app_builder.requirements import derive_spec, detect_archetype
from ai_assistant.app_builder.service import AppBuilderService
from ai_assistant.app_builder.webapp import generate_app
from ai_assistant.meters import RequirementCoverageMeter


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


# ── intent detection ──────────────────────────────────────────────────────────
def test_detect_storefront_from_prompt():
    assert detect_archetype("build an ecommerce app to sell electronic items") == "storefront"
    assert detect_archetype("an online shop with a cart and checkout") == "storefront"
    # "manage products" is an admin tool, not a storefront.
    assert detect_archetype("manage products and reviews") == "crud"
    assert detect_archetype("a todo app") == "crud"


def test_detect_storefront_from_schema():
    # A priced product table + an orders table implies a storefront.
    assert detect_archetype("", {"products": ["id", "name", "price"],
                                  "orders": ["id", "total"]}) == "storefront"
    assert detect_archetype("build from database", {
        "products": ["id", "name", "price"], "orders": ["id", "total"],
    }) == "storefront"
    # Once the user provides a real non-store prompt, the prompt wins over schema.
    assert detect_archetype("build a staff shift scheduling app", {
        "products": ["id", "name", "price"], "orders": ["id", "total"],
    }) == "crud"
    # customers + orders (no priced catalog) is a generic management app.
    assert detect_archetype("", {"customers": ["id", "email"],
                                 "orders": ["id", "total"]}) == "crud"


# ── generation shape ──────────────────────────────────────────────────────────
def test_storefront_generates_shopping_surfaces_not_crud():
    spec = derive_spec(app_name="voltmart",
                       description="ecommerce app to sell electronics")
    assert spec.kind == "storefront"
    files = generate_app(spec)
    # Storefront templates, not CRUD admin templates.
    for t in ("templates/catalog.html", "templates/product.html",
              "templates/cart.html", "templates/checkout.html",
              "templates/confirmation.html"):
        assert t in files, t
    assert "templates/list.html" not in files
    assert "templates/form.html" not in files
    # Real shopping API + pages.
    assert '@router.get("/products")' in files["src/api.py"]
    assert '@router.post("/orders")' in files["src/api.py"]
    assert '"/cart"' in files["src/web.py"]
    assert '"/checkout"' in files["src/web.py"]
    # Every generated Python file is valid.
    for path, content in files.items():
        if path.endswith(".py"):
            ast.parse(content)


def test_engine_manifest_is_archetype_aware():
    eng = AiAppEngine()
    store = AppBlueprint(
        name="shop", kind="storefront", mode=BuildMode.FROM_DATABASE,
        connections=["local"],
    )
    crud = AppBlueprint(
        name="admin", kind="crud", mode=BuildMode.FROM_DATABASE,
        connections=["local"],
    )
    store_m = eng.expected_manifest(store)
    crud_m = eng.expected_manifest(crud)
    assert "templates/catalog.html" in store_m
    assert "templates/checkout.html" in store_m
    assert "templates/list.html" not in store_m
    assert "templates/list.html" in crud_m
    assert "templates/catalog.html" not in crud_m


# ── it actually runs (cart + checkout) ────────────────────────────────────────
def test_storefront_runs_and_serves(tmp_path):
    spec = derive_spec(app_name="voltmart",
                       description="ecommerce app to sell electronic items",
                       schema={"products": ["id", "name", "price", "category"],
                               "orders": ["id", "total"]})
    files = generate_app(spec)
    ws = tmp_path / "ws"
    for path, content in files.items():
        fp = ws / path
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(content, encoding="utf-8")
    result = _run_generated_tests(ws)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "passed" in result.stdout


# ── end-to-end through the service ───────────────────────────────────────────
def test_service_builds_storefront_from_database(home):
    r = AppBuilderService().build({
        "name": "voltmart", "mode": "from_database",
        "schema": {"products": ["id", "name", "price", "category"],
                   "orders": ["id", "total", "customer_id"]},
        "description": "build an ecommerce app to sell electronic items",
        "services": ["monitoring", "ci_cd", "database", "hosting"],
    })
    assert r["ok"] is True, r["verdict"]
    ws = Path(r["workspace"])
    assert (ws / "templates" / "catalog.html").is_file()
    assert not (ws / "templates" / "list.html").exists()
    result = _run_generated_tests(ws)
    assert result.returncode == 0, result.stdout + result.stderr


# ── coverage meter (storefront surfaces) ──────────────────────────────────────
def test_storefront_coverage_full_and_gaps():
    spec = derive_spec(app_name="voltmart", description="ecommerce store")
    files = generate_app(spec)
    meter = RequirementCoverageMeter()
    full = meter.measure(entities=[], features=[], files=files,
                         services=["monitoring"], kind="storefront")
    assert full.score == 1.0
    assert full.evidence["fully_covered"] is True

    # Drop the cart page + the checkout API → coverage falls with clear gaps.
    broken = dict(files)
    broken["src/web.py"] = files["src/web.py"].replace('"/cart"', '"/basket"')
    broken["src/api.py"] = files["src/api.py"].replace(
        '@router.post("/orders")', '@router.get("/orders")')
    part = meter.measure(entities=[], features=[], files=broken,
                        services=[], kind="storefront")
    assert part.score < 1.0
    assert any("cart page" in g for g in part.issues)
    assert any("checkout/orders API" in g for g in part.issues)
