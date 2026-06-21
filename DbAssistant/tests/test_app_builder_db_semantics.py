from __future__ import annotations

from ai_assistant.app_builder.db_profile import (
    ColumnProfile,
    DbProfile,
    DbProfiler,
    TableProfile,
)
from ai_assistant.app_builder.db_semantics import enrich_profile
from ai_assistant.app_builder.db_understanding import DbUnderstandingClient
from ai_assistant.app_builder.meters.registry import AppMeterRegistry


def test_semantics_infers_labeled_fk_roles_and_tags_from_samples():
    customers = TableProfile(
        "customers",
        columns=[
            ColumnProfile("id", "INTEGER", is_pk=True),
            ColumnProfile("email", "TEXT"),
            ColumnProfile("name", "TEXT"),
        ],
        row_count_estimate=2,
        sample_rows=[{"id": 1, "email": "a@example.com", "name": "Ada"}],
    )
    orders = TableProfile(
        "orders",
        columns=[
            ColumnProfile("id", "INTEGER", is_pk=True),
            ColumnProfile("customer_id", "INTEGER"),
            ColumnProfile("total_amount", "REAL"),
            ColumnProfile("status", "TEXT"),
            ColumnProfile("created_at", "TEXT"),
        ],
        row_count_estimate=5,
        sample_rows=[{"id": 10, "customer_id": 1, "total_amount": 12.5,
                      "status": "paid", "created_at": "2026-01-01"}],
    )
    profile = DbProfile(tables=[customers, orders])
    enrich_profile(profile)

    assert profile.relationships
    rel = profile.relationships[0]
    assert rel["source"] == "inferred"
    assert rel["from_table"] == "orders"
    assert rel["to_table"] == "customers"
    assert any("labeled inferred edges" in note for note in profile.advisory_notes)
    assert orders.role == "transaction"
    tags = {c.name: set(c.semantic_tags) for c in orders.columns}
    assert "foreign_key" in tags["customer_id"]
    assert "money" in tags["total_amount"]
    assert "temporal" in tags["created_at"]
    assert "status" in tags["status"]


class CatalogCore:
    db_type = "MariaDB"

    def open_connection(self, name, form=None):
        return {"ok": True}

    def get_manager(self, name, profile=None):
        return self

    def get_objects(self, name, kind="tables"):
        return ["customers", "orders"] if kind == "tables" else []

    def get_table_schema(self, name, table):
        if table == "customers":
            return {
                "columns": [
                    {"name": "id", "type": "INTEGER", "pk": True},
                    {"name": "email", "type": "TEXT", "Key": "UNI"},
                ],
                "primary_key": ["id"],
                "unique_keys": [["email"]],
            }
        return {
            "columns": [
                {"name": "id", "type": "INTEGER", "pk": True},
                {"name": "customer_id", "type": "INTEGER", "Key": "MUL"},
                {"name": "total", "type": "REAL"},
            ],
            "primary_key": ["id"],
            "foreign_keys": [
                {
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "id",
                    "constraint_name": "fk_orders_customers",
                }
            ],
        }

    def sample_table(self, name, table, limit=3):
        if table == "customers":
            return {"error": None, "columns": ["id", "email"],
                    "rows": [[1, "a@example.com"]]}
        return {"error": None, "columns": ["id", "customer_id", "total"],
                "rows": [[10, 1, 12.5]]}

    def execute(self, name, sql):
        return {"error": "catalog not needed", "rows": []}


def test_profiler_resolves_declared_keys_before_understanding_prompt():
    client = DbUnderstandingClient(core=CatalogCore(), connection_name="local")
    insight = client.understand({"customers": ["id", "email"],
                                 "orders": ["id", "customer_id", "total"]})
    profile = insight.profile.as_dict()
    assert profile["relationships"][0]["source"] == "declared"
    customers = next(t for t in profile["tables"] if t["name"] == "customers")
    email = next(c for c in customers["columns"] if c["name"] == "email")
    assert email["unique"] is True
    prompt = insight.prompt_block()
    assert "relationships (declared first; inferred labeled)" in prompt
    assert "orders.customer_id -> customers.id" in prompt
    assert "table roles:" in prompt


def test_profiler_refuses_mutating_live_sql():
    class Manager:
        db_type = "sqlite"
        conn = object()

        def __init__(self):
            self.calls = []

        def execute_query(self, sql):
            self.calls.append(sql)
            return ([], None)

    mgr = Manager()
    profiler = DbProfiler(db_manager=mgr)

    rows, err = profiler._run_select("UPDATE users SET name = 'x'")

    assert rows is None
    assert err
    assert mgr.calls == []


def test_db_semantic_meters_score_grounded_connected_app():
    profile = {
        "tables": [
            {"name": "customers", "role": "master", "columns": [
                {"name": "id", "semantic_tags": ["primary_key"]},
                {"name": "email", "semantic_tags": ["pii"]},
            ]},
            {"name": "orders", "role": "transaction", "columns": [
                {"name": "customer_id", "semantic_tags": ["foreign_key"]},
                {"name": "total", "semantic_tags": ["money"]},
                {"name": "status", "semantic_tags": ["status", "enum"]},
            ]},
        ],
        "relationships": [
            {"from_table": "orders", "from_column": "customer_id",
             "to_table": "customers", "to_column": "id",
             "kind": "N:1", "source": "declared", "confidence": 1.0}
        ],
    }
    insight = {
        "app_name": "Customer Orders",
        "app_summary": "Customers place orders and track order status.",
        "data_flow": "customers create orders",
        "app_features": ["filter order status", "view customer order detail"],
    }
    files = {
        "src/app.py": """
@app.get('/customers/{id}/orders')
def customer_order_detail(): pass
@app.get('/orders')
def orders(): pass
""",
        "templates/orders.html": (
            "customer orders detail dashboard filter status select option "
            "total currency price email privacy"
        ),
    }
    report = AppMeterRegistry().evaluate_db_build(
        files, profile=profile, schema={"customers": ["id"], "orders": ["id"]},
        archetype="ecommerce", insight=insight)
    assert report["meters"]["relationship_fidelity"]["passed"]
    assert report["meters"]["entity_role_fit"]["passed"]
    assert report["meters"]["data_semantics"]["passed"]
    assert report["meters"]["workflow_coverage"]["passed"]
    assert report["meters"]["prediction_grounding"]["passed"]
