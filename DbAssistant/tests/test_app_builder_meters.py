"""Tests for the App Builder meters package (ai_assistant/app_builder/meters)."""

from __future__ import annotations

from ai_assistant.app_builder.meters import (
    AppMeterRegistry,
    MessageIntent,
    classify_intent,
    extract_plan,
)
from ai_assistant.app_builder.meters.intent_classifier import IntentClassifier


# ── intent classifier ─────────────────────────────────────────────────────────
def test_intent_classifier_questions():
    assert classify_intent("Which database should I use?") is MessageIntent.QUESTION
    assert classify_intent("How do I wire the router?") is MessageIntent.QUESTION
    assert classify_intent("Should we add auth now?") is MessageIntent.QUESTION
    assert classify_intent("ASK: confirm the schema") is MessageIntent.QUESTION


def test_intent_classifier_recommendations():
    assert classify_intent(
        "I recommend SQLite for simplicity.") is MessageIntent.RECOMMENDATION
    assert classify_intent(
        "It is better to use a service layer here.") is MessageIntent.RECOMMENDATION


def test_intent_classifier_progress_and_done():
    assert classify_intent(
        "I implemented the orders model and added tests.") is MessageIntent.PROGRESS
    assert classify_intent(
        "The application is complete and ready for review.") is MessageIntent.DONE


def test_intent_classifier_detail_confidence():
    d = IntentClassifier().detail("Which DB should I use?")
    assert d["intent"] == "question"
    assert d["signals"]["question"] >= 1


# ── design plan + similarity ────────────────────────────────────────────────────
def test_extract_plan_normalizes_components_and_entities():
    plan = extract_plan(
        "Create customer and order models, a router with /orders endpoint, "
        "and templates. Features: create and list.",
        role="builder", entities=["customer", "order"], features=["create"])
    assert "customer" in plan.entities and "order" in plan.entities
    # routes/router/endpoint all collapse to the canonical "route".
    assert "route" in plan.components
    assert "model" in plan.components
    assert "create" in plan.features and "list" in plan.features


def test_design_similarity_passes_for_aligned_plans():
    reg = AppMeterRegistry()
    ents, feats = ["customer", "order"], ["create", "list"]
    a = extract_plan("Build src/models.py for customer and order models, "
                     "src/routes.py with /customers and /orders, templates. "
                     "Features create and list.", entities=ents, features=feats)
    b = extract_plan("Entities: customer and order (models). Components: model, "
                     "router, template. Features: create and list.",
                     entities=ents, features=feats)
    c = extract_plan("Validate customer and order models and the create and list "
                     "flows. Components: model, router, test.",
                     entities=ents, features=feats)
    res = reg.evaluate_design_similarity([a, b, c], threshold=0.8)
    assert res["score"] >= 0.8
    assert res["passed"] is True


def test_design_similarity_fails_for_divergent_plans():
    reg = AppMeterRegistry()
    a = extract_plan("A blog with posts and comments models.",
                     entities=["post", "comment"])
    b = extract_plan("An inventory system with products and warehouses tables.",
                     entities=["product", "warehouse"])
    c = extract_plan("A chat app with messages and rooms models.",
                     entities=["message", "room"])
    res = reg.evaluate_design_similarity([a, b, c], threshold=0.8)
    assert res["passed"] is False
    assert res["score"] < 0.8


def test_design_similarity_single_plan_trivially_aligned():
    reg = AppMeterRegistry()
    a = extract_plan("orders model", entities=["order"])
    res = reg.evaluate_design_similarity([a])
    assert res["score"] == 1.0


# ── quality battery ─────────────────────────────────────────────────────────────
_GOOD_FILES = {
    "requirements.txt": "fastapi\nuvicorn\npydantic\npytest\n",
    "src/__init__.py": "",
    "src/app.py": (
        "from fastapi import FastAPI, HTTPException\n"
        "from .models import Order\n"
        "app = FastAPI()\n"
        "@app.get('/health')\n"
        "def health():\n    return {'ok': True}\n"
        "@app.get('/orders')\n"
        "def list_orders():\n    '''List orders.'''\n    return []\n"
        "@app.post('/orders')\n"
        "def create_order(o: Order):\n"
        "    '''Create an order.'''\n"
        "    if not o.item:\n        raise HTTPException(400, 'bad')\n"
        "    return o\n"),
    "src/models.py": (
        "from pydantic import BaseModel\n"
        "class Order(BaseModel):\n    id: int\n    item: str\n"
        "# CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT)\n"),
    "templates/orders.html": "<html><body>Grocery orders</body></html>",
    "tests/test_orders.py": "def test_create():\n    assert True\n",
    "README.md": "# Grocery order manager",
}


def test_quality_battery_scores_good_app_high():
    reg = AppMeterRegistry()
    rep = reg.evaluate_quality(
        _GOOD_FILES, description="grocery order manager",
        features=["create", "list"], entities=["order"],
        test_outcome={"summary": "2 passed in 0.1s"})
    assert rep["overall"] >= 0.8
    assert rep["meters"]["functional_correctness"]["score"] == 1.0
    assert rep["meters"]["feature_completeness"]["score"] >= 0.8


def test_quality_battery_flags_poor_app():
    reg = AppMeterRegistry()
    poor = {"app.py": "def f():\n    try:\n        x = 1\n    except:\n        pass\n"}
    rep = reg.evaluate_quality(
        poor, description="grocery store with orders",
        features=["checkout"], entities=["order"],
        test_outcome={"summary": "no tests"})
    assert rep["overall"] < 0.5
    assert "functional_correctness" in rep["failing"]
    assert "schema_design" in rep["failing"]


def test_functional_correctness_parses_pytest_summary():
    reg = AppMeterRegistry()
    m = reg.get("functional_correctness")
    good = m.measure({"summary": "8 passed in 0.4s"})
    assert good.score == 1.0
    mixed = m.measure({"summary": "2 failed, 6 passed in 0.5s"})
    assert 0.7 <= mixed.score < 0.8
    none = m.measure({"summary": "no tests"})
    assert none.score == 0.0
    # Boolean fallback when summary line is missing but pytest succeeded.
    ok_bool = m.measure({"passed": True, "summary": ""})
    assert ok_bool.score == 1.0


# ── CLI interface meter ─────────────────────────────────────────────────────────
def test_cli_meter_not_applicable_for_web_app():
    reg = AppMeterRegistry()
    m = reg.get("cli_interface")
    res = m.measure({"src/app.py": "from fastapi import FastAPI\napp=FastAPI()\n"},
                    description="a web todo app", features=["list"])
    assert res.evidence["applicable"] is False
    assert res.score == 1.0


def test_cli_meter_flags_requested_but_missing():
    reg = AppMeterRegistry()
    m = reg.get("cli_interface")
    res = m.measure({"src/app.py": "def main():\n    pass\n"},
                    description="a CLI tool to manage tasks from the terminal",
                    features=["add", "list"])
    assert res.evidence["applicable"] is True
    assert res.evidence["requested"] is True
    assert res.score < 0.5
    assert any("requested" in i for i in res.issues)


def test_cli_meter_scores_real_click_cli_high():
    reg = AppMeterRegistry()
    m = reg.get("cli_interface")
    files = {
        "src/cli.py": (
            "import click\n\n"
            "@click.group()\ndef cli():\n    '''Task manager CLI.'''\n\n"
            "@cli.command()\n@click.option('--name', help='task name')\n"
            "def add(name):\n    '''Add a task.'''\n    click.echo(name)\n\n"
            "@cli.command()\ndef list():\n    '''List tasks.'''\n    pass\n\n"
            "if __name__ == '__main__':\n    cli()\n"),
    }
    res = m.measure(files, description="a CLI task manager", features=["add"])
    assert res.evidence["applicable"] is True
    assert res.score >= 0.8
    assert res.passed


# ── SOLID principles meter ──────────────────────────────────────────────────────
def test_solid_meter_not_applicable_for_scripts():
    reg = AppMeterRegistry()
    m = reg.get("solid_principles")
    res = m.measure({"app.py": "def f():\n    return 1\n"})
    assert res.evidence["applicable"] is False
    assert res.score == 1.0


def test_solid_meter_rewards_abstractions_and_injection():
    reg = AppMeterRegistry()
    m = reg.get("solid_principles")
    files = {
        "src/repo.py": (
            "from abc import ABC, abstractmethod\n\n"
            "class Repository(ABC):\n"
            "    @abstractmethod\n    def get(self, id):\n        ...\n\n"
            "class SqlRepository(Repository):\n"
            "    def __init__(self, db):\n        self._db = db\n"
            "    def get(self, id):\n        return self._db.find(id)\n\n"
            "class TaskService:\n"
            "    def __init__(self, repo: Repository):\n        self._repo = repo\n"
            "    def fetch(self, id):\n        return self._repo.get(id)\n"),
    }
    res = m.measure(files)
    assert res.evidence["applicable"] is True
    assert res.evidence["abstractions"] >= 1
    assert res.evidence["injected_classes"] >= 2
    assert res.score >= 0.8


def test_solid_meter_flags_god_object():
    reg = AppMeterRegistry()
    m = reg.get("solid_principles")
    methods = "".join(
        f"    def m{i}(self):\n        x = {i}\n        return x\n" for i in range(20))
    files = {
        "src/god.py": "class God:\n" + methods,
        "src/other.py": "class Helper:\n    def go(self):\n        return 1\n",
    }
    res = m.measure(files)
    assert res.evidence["applicable"] is True
    assert "God" in res.evidence["oversized"]
    assert res.score < 0.7


# ── CRUD coverage in backend logic ──────────────────────────────────────────────
def test_backend_logic_flags_incomplete_crud():
    reg = AppMeterRegistry()
    m = reg.get("backend_logic")
    files = {"src/app.py": (
        "from fastapi import FastAPI, HTTPException\nfrom pydantic import BaseModel\n"
        "app=FastAPI()\n"
        "@app.post('/items')\ndef create_item(): ...\n"
        "@app.get('/items')\ndef list_items(): ...\n")}
    res = m.measure(files)
    assert "crud_coverage" in res.components
    assert res.evidence["crud_coverage"] < 1.0
    assert any("CRUD" in i for i in res.issues)


def test_backend_logic_full_crud_scores_high():
    reg = AppMeterRegistry()
    m = reg.get("backend_logic")
    files = {"src/app.py": (
        "from fastapi import FastAPI, HTTPException\nfrom pydantic import BaseModel\n"
        "app=FastAPI()\n"
        "@app.post('/items')\ndef create_item(): ...\n"
        "@app.get('/items')\ndef list_items(): ...\n"
        "@app.put('/items/{i}')\ndef update_item(i): ...\n"
        "@app.delete('/items/{i}')\ndef delete_item(i):\n"
        "    if not i:\n        raise HTTPException(404)\n")}
    res = m.measure(files)
    assert res.evidence["crud_coverage"] == 1.0
    assert set(res.evidence["crud_ops"]) == {"create", "read", "update", "delete"}


def test_registry_is_extensible():
    reg = AppMeterRegistry()
    from ai_assistant.meters.base import Meter, Measurement

    class DummyMeter(Meter):
        name = "dummy"

        def measure(self, *a, **k):
            return Measurement(meter="dummy", score=0.5)

    reg.register("dummy", DummyMeter())
    assert "dummy" in reg.names()
    assert reg.get("dummy").measure().score == 0.5
