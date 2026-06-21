"""Validate a real App Builder workspace against the standard quality meters.

This test does NOT invoke a live AI backend. It either:
  1. Reads the most recently built ``e2e_todo`` workspace (if present), or
  2. Uses a representative in-repo fixture workspace.

Run a live build first (optional):
    python tools/e2e_app_builder.py e2e_todo "..." cursor 2
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_assistant.app_builder.meters import AppMeterRegistry
from common.paths import app_builder_dir


def _read_workspace(root: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    skip = {".venv", "__pycache__", ".pytest_cache", ".git"}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if any(part in skip for part in p.parts):
            continue
        rel = str(p.relative_to(root))
        if rel.startswith("."):
            continue
        try:
            files[rel] = p.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
    return files


def _todo_workspace() -> Path | None:
    ws = app_builder_dir() / "e2e_todo"
    return ws if ws.is_dir() and (ws / "src").is_dir() else None


@pytest.mark.parametrize(
    "description,features,entities,test_summary",
    [
        (
            "todo list app create list complete delete tasks web page",
            ["create", "list", "complete", "delete"],
            ["task"],
            "40 passed in 1.0s",
        ),
        (
            "expense tracker with categories and monthly reports",
            ["create", "list", "report"],
            ["expense", "category"],
            "10 passed in 0.5s",
        ),
        (
            "recipe book with ingredients and search",
            ["create", "list", "search"],
            ["recipe", "ingredient"],
            "8 passed in 0.4s",
        ),
        (
            "guestbook where visitors leave messages",
            ["create", "list"],
            ["message", "guest"],
            "6 passed in 0.3s",
        ),
        (
            "poll app with voting and results page",
            ["create", "list", "vote"],
            ["poll", "vote"],
            "12 passed in 0.6s",
        ),
    ],
    ids=["todo", "expense", "recipe", "guestbook", "poll"],
)
def test_e2e_quality_profiles(description, features, entities, test_summary):
    """Five app *profiles* must score well on the standard meter battery.

    When a live ``e2e_todo`` build exists, the todo profile uses its real files;
    the other profiles use the same structural baseline (FastAPI + tests) to
    confirm the meters discriminate requirements correctly without needing five
    separate live builds in CI.
    """
    reg = AppMeterRegistry()
    live = _todo_workspace()
    if live is not None and "todo" in description:
        files = _read_workspace(live)
        assert len(files) > 5, "live e2e_todo workspace looks empty"
    else:
        # Structural baseline representative of a small built app.
        files = {
            "requirements.txt": "fastapi\nuvicorn\npydantic\npytest\n",
            "src/app.py": (
                f"from fastapi import FastAPI\napp=FastAPI()\n"
                f"@app.get('/health')\ndef health(): return {{'ok':True}}\n"
                f"@app.get('/{entities[0]}s')\ndef list_{entities[0]}(): "
                f"return []\n"),
            "src/models.py": (
                f"# {entities[0]} model for {description}\n"
                f"class {entities[0].title()}:\n    id:int\n"),
            f"templates/{entities[0]}.html": f"<html>{description}</html>",
            "tests/test_app.py": "def test_health():\n    assert True\n",
        }
        for feat in features:
            files["src/app.py"] += f"# feature: {feat}\n"
            files[f"templates/{entities[0]}.html"] += f" {feat}"

    rep = reg.evaluate_quality(
        files, description=description, features=features, entities=entities,
        test_outcome={"passed": True, "summary": test_summary})
    assert rep["overall"] >= 0.75, rep
    assert "functional_correctness" not in rep["failing"]


# ── extensive app profile: API + CLI + CRUD + SOLID ─────────────────────────────
_EXTENSIVE_FILES = {
    "requirements.txt": "fastapi\nuvicorn\npydantic\nclick\npytest\n",
    "pyproject.toml": (
        "[project]\nname='inventory'\n\n[project.scripts]\n"
        "inventory = 'src.cli:cli'\n"),
    "src/__init__.py": "",
    "src/models.py": (
        "from pydantic import BaseModel, Field\n\n"
        "class Product(BaseModel):\n    id: int\n    name: str\n"
        "    stock: int = Field(0, ge=0)\n\n"
        "class Supplier(BaseModel):\n    id: int\n    name: str\n"),
    "schema.sql": (
        "CREATE TABLE IF NOT EXISTS suppliers (\n"
        "  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL,\n"
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\n\n"
        "CREATE TABLE IF NOT EXISTS products (\n"
        "  id INTEGER PRIMARY KEY,\n  name TEXT NOT NULL,\n"
        "  stock INTEGER NOT NULL DEFAULT 0,\n"
        "  supplier_id INTEGER REFERENCES suppliers(id),\n"
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\n"
        "CREATE INDEX idx_products_supplier ON products(supplier_id);\n\n"
        "CREATE TABLE IF NOT EXISTS stock_movements (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  product_id INTEGER REFERENCES products(id),\n"
        "  delta INTEGER NOT NULL,\n"
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n);\n"),
    "src/repository.py": (
        "from abc import ABC, abstractmethod\n\n"
        "class ProductRepository(ABC):\n"
        "    '''Abstract product store (DIP seam).'''\n"
        "    @abstractmethod\n    def add(self, product):\n        ...\n"
        "    @abstractmethod\n    def get(self, pid):\n        ...\n"
        "    @abstractmethod\n    def list(self):\n        ...\n"
        "    @abstractmethod\n    def update(self, product):\n        ...\n"
        "    @abstractmethod\n    def delete(self, pid):\n        ...\n\n"
        "class SqlProductRepository(ProductRepository):\n"
        "    def __init__(self, db):\n        self._db = db\n"
        "    def add(self, product):\n        return self._db.insert(product)\n"
        "    def get(self, pid):\n        return self._db.find(pid)\n"
        "    def list(self):\n        return self._db.all()\n"
        "    def update(self, product):\n        return self._db.save(product)\n"
        "    def delete(self, pid):\n        return self._db.remove(pid)\n"),
    "src/services.py": (
        "from .repository import ProductRepository\n\n"
        "class InventoryService:\n"
        "    '''Business logic with an injected repository (DIP).'''\n"
        "    def __init__(self, repo: ProductRepository):\n"
        "        self._repo = repo\n"
        "    def create_product(self, product):\n"
        "        return self._repo.add(product)\n"
        "    def list_products(self):\n        return self._repo.list()\n"
        "    def update_product(self, product):\n"
        "        return self._repo.update(product)\n"
        "    def delete_product(self, pid):\n        return self._repo.delete(pid)\n"
        "    def low_stock(self, threshold=5):\n"
        "        return [p for p in self._repo.list() if p.stock < threshold]\n"),
    "src/app.py": (
        "from fastapi import FastAPI, HTTPException\nfrom .models import Product\n"
        "app = FastAPI()\n"
        "@app.get('/health')\ndef health():\n    '''Health.'''\n    return {'ok': True}\n"
        "@app.post('/products')\ndef create_product(p: Product):\n"
        "    '''Create.'''\n    return p\n"
        "@app.get('/products')\ndef list_products():\n    '''List.'''\n    return []\n"
        "@app.put('/products/{pid}')\ndef update_product(pid: int, p: Product):\n"
        "    '''Update.'''\n    if pid < 0:\n        raise HTTPException(400)\n    return p\n"
        "@app.delete('/products/{pid}')\ndef delete_product(pid: int):\n"
        "    '''Delete.'''\n    return {'deleted': pid}\n"),
    "src/cli.py": (
        "import click\n\n"
        "@click.group()\ndef cli():\n    '''Inventory CLI.'''\n\n"
        "@cli.command()\n@click.option('--name')\ndef add(name):\n"
        "    '''Add a product.'''\n    click.echo(name)\n\n"
        "@cli.command(name='list')\ndef list_cmd():\n    '''List products.'''\n    pass\n\n"
        "@cli.command()\n@click.argument('pid')\ndef delete(pid):\n"
        "    '''Delete a product.'''\n    pass\n\n"
        "if __name__ == '__main__':\n    cli()\n"),
    "templates/index.html": (
        "<html><body><h1>Inventory</h1>"
        "<section>Products</section><section>Suppliers</section>"
        "<section>Stock levels and low-stock alerts</section>"
        "</body></html>"),
    "tests/test_inventory.py": (
        "def test_create():\n    assert True\n"
        "def test_low_stock():\n    assert True\n"),
    "docs/README.md": (
        "# Inventory\n\nManage products, suppliers and stock levels. "
        "Record stock movements and view low-stock alerts.\n"),
}


def test_extensive_app_scores_all_new_dimensions():
    """An app with a real API, CLI, full CRUD and SOLID layering must light up
    every new meter — not just the legacy battery."""
    reg = AppMeterRegistry()
    rep = reg.evaluate_quality(
        _EXTENSIVE_FILES,
        description=("inventory app to manage products, suppliers and stock "
                     "levels with low-stock alerts, via a REST API and a CLI"),
        features=["create", "list", "update", "delete", "low stock"],
        entities=["product", "supplier", "stock"],
        test_outcome={"passed": True, "summary": "2 passed in 0.1s"})

    meters = rep["meters"]
    # CLI is requested AND present → applicable and strong.
    assert meters["cli_interface"]["evidence"]["applicable"] is True
    assert meters["cli_interface"]["score"] >= 0.8
    # SOLID: abstractions + injection present → strong.
    assert meters["solid_principles"]["evidence"]["applicable"] is True
    assert meters["solid_principles"]["score"] >= 0.8
    # CRUD: all four operations present.
    assert meters["backend_logic"]["evidence"]["crud_coverage"] == 1.0
    assert rep["overall"] >= 0.85, rep
    assert not rep["failing"], rep["failing"]
