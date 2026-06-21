"""AI-driven template enrichment: store, enrichment, corpus merge, surfaces.

Covers the persistent enriched-template store, the AI-driven enrichment service
(placeholder validation + accept/reject), the merge into the training corpus via
query_templates, and CLI/API/service/UI parity.
"""

from __future__ import annotations

import os
import tempfile
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def isolated_home(monkeypatch):
    """Point the LLM/session dir at a throwaway folder + reset the store cache."""
    home = tempfile.mkdtemp()
    monkeypatch.setenv("DBASSISTANT_HOME", home)
    from ai_assistant.llm import template_store

    template_store._cache["mtime"] = None
    template_store._cache["data"] = None
    yield Path(home)
    template_store._cache["mtime"] = None
    template_store._cache["data"] = None


# ── store ────────────────────────────────────────────────────────────────────

def test_store_add_load_dedupe_clear(isolated_home):
    from ai_assistant.llm import template_store as ts

    assert ts.summary()["catalog_templates"] == 0
    r = ts.add(
        catalog={"PostgreSQL": [{"question": "list tables",
                                 "sql": "SELECT 1", "category": "catalog"}]},
        objects=[{"question": "count rows",
                  "sql": "SELECT COUNT(*) FROM {table}", "category": "count"}],
    )
    assert r["added_catalog"] == 1
    assert r["added_object"] == 1
    # Normalised dialect tag.
    assert "postgresql" in ts.load()["catalog"]
    # requires auto-derived from placeholders is empty for {table}-only.
    assert ts.enriched_object_templates()[0]["requires"] == []

    # Re-adding identical entries is a no-op (dedupe).
    r2 = ts.add(catalog={"postgresql": [{"question": "list tables",
                                         "sql": "SELECT 1"}]})
    assert r2["added_catalog"] == 0

    ts.clear()
    assert ts.summary()["catalog_templates"] == 0
    assert ts.summary()["object_templates"] == 0


def test_store_requires_inferred_from_placeholders(isolated_home):
    from ai_assistant.llm import template_store as ts

    ts.add(objects=[{
        "question": "group", "sql":
        "SELECT {text_col_q}, SUM({num_col_q}) FROM {table} GROUP BY {text_col_q}",
    }])
    req = ts.enriched_object_templates()[0]["requires"]
    assert "text_col" in req and "num_col" in req


# ── enrichment service (pure) ──────────────────────────────────────────────────

def _catalog_then_object_gen(prompt: str) -> str:
    if "SCOPE: catalog" in prompt:
        return '{"sql": "SELECT COUNT(*) AS n FROM information_schema.tables"}'
    return '{"sql": "SELECT COUNT(*) AS total FROM {table}", "category": "count"}'


def test_enrich_accepts_valid_templates(isolated_home):
    from ai_assistant.llm import template_enrichment as te
    from ai_assistant.llm import template_store as ts

    res = te.enrich_templates(
        generate_text_fn=_catalog_then_object_gen, db_types=["PostgreSQL"])
    assert res["ok"] is True
    assert res["accepted"] > 0
    assert res["rejected"] == 0
    s = ts.summary()
    assert s["catalog_templates"] > 0
    assert s["object_templates"] > 0


def test_enrich_rejects_unknown_placeholders(isolated_home):
    from ai_assistant.llm import template_enrichment as te

    def bad(prompt):
        return '{"sql": "SELECT * FROM {mystery}"}'

    res = te.enrich_templates(
        generate_text_fn=bad, db_types=["PostgreSQL"],
        questions=[{"intent": "x", "scope": "object"}], persist=False)
    assert res["accepted"] == 0
    assert res["rejected"] == 1


def test_enrich_rejects_object_without_table(isolated_home):
    from ai_assistant.llm import template_enrichment as te

    def no_table(prompt):
        return '{"sql": "SELECT {col_list} FROM orders"}'

    res = te.enrich_templates(
        generate_text_fn=no_table, db_types=["MySQL"],
        questions=[{"intent": "x", "scope": "object"}], persist=False)
    assert res["accepted"] == 0


def test_enrich_rejects_catalog_with_placeholders(isolated_home):
    from ai_assistant.llm import template_enrichment as te

    def cat_with_ph(prompt):
        return '{"sql": "SELECT * FROM {table}"}'

    res = te.enrich_templates(
        generate_text_fn=cat_with_ph, db_types=["PostgreSQL"],
        questions=[{"intent": "list tables", "scope": "catalog"}], persist=False)
    assert res["accepted"] == 0


def test_enrich_limit_per_type(isolated_home):
    from ai_assistant.llm import template_enrichment as te

    res = te.enrich_templates(
        generate_text_fn=_catalog_then_object_gen,
        db_types=["PostgreSQL"], limit_per_type=2)
    # Only the first 2 intents are attempted (both catalog-scope).
    assert res["per_type"]["postgresql"]["accepted"] == 2


# ── corpus merge ───────────────────────────────────────────────────────────────

def test_enriched_templates_flow_into_corpus(isolated_home):
    from ai_assistant.llm import template_store as ts
    from ai_assistant.llm.query_templates import (
        all_catalog_pairs, _all_object_templates,
    )

    ts.add(
        catalog={"postgresql": [{"question": "count tables",
                                 "sql": "SELECT COUNT(*) FROM information_schema.tables"}]},
        objects=[{"question": "count rows", "sql": "SELECT COUNT(*) FROM {table}"}],
    )
    assert any(
        p["db_type"] == "postgresql" and "information_schema.tables" in p["sql"]
        for p in all_catalog_pairs())
    assert any(
        t.id.startswith("enriched") and "{table}" in t.sql
        for t in _all_object_templates())


def test_render_object_templates_skips_malformed_enriched(isolated_home):
    """A bad enriched template must not break rendering of valid ones."""
    from ai_assistant.llm import template_store as ts
    from ai_assistant.llm.query_templates import render_object_templates

    # Inject directly (bypass enrichment validation) to simulate corruption.
    data = ts.load()
    data["object"].append({
        "id": "enriched.bad", "question": "bad", "sql": "SELECT {nope} FROM {table}",
        "requires": [], "category": "x", "complexity": "moderate", "db_types": ["*"],
    })
    ts._save(data)

    col = types.SimpleNamespace(name="amount", type="int")
    info = types.SimpleNamespace(
        name="orders", columns=[col], text_cols=[], numeric_cols=[col])

    class _Dialect:
        def quote(self, n):
            return n

        def col(self, n):
            return n

        def bounded_select(self, n, limit):
            return f"SELECT * FROM {n} LIMIT {limit}"

        def limit(self, sql, n):
            return f"{sql} LIMIT {n}"

    # Must not raise despite the malformed enriched template.
    out = render_object_templates(info, _Dialect(), limit=5)
    assert all("{nope}" not in r["sql"] for r in out)


# ── service / CLI / API parity ─────────────────────────────────────────────────

class _FakeAgent:
    def __init__(self):
        self.prompts = []

    def is_available(self):
        return True

    def _call_ai(self, prompt, timeout=120):
        self.prompts.append(prompt)
        if "SCOPE: catalog" in prompt:
            return {"response":
                    '{"sql": "SELECT COUNT(*) AS n FROM information_schema.tables"}'}
        return {"response": '{"sql": "SELECT COUNT(*) AS total FROM {table}"}'}


class _FakeCore:
    def get_connection_profile(self, name):
        return {"db_type": "postgresql"}


def test_service_enrich_templates(isolated_home):
    from ai_query.service import AIService

    svc = AIService(_FakeCore())
    svc._ai = _FakeAgent()
    r = svc.llm_enrich_templates({"db_types": ["PostgreSQL"], "limit_per_type": 3})
    assert r["ok"] is True
    assert r["accepted"] > 0

    summary = svc.llm_template_store_summary()
    assert summary["ok"] is True
    assert summary["catalog_templates"] >= 1

    cleared = svc.llm_template_store_clear()
    assert cleared["catalog_templates"] == 0


def test_cli_exposes_enrich_templates():
    src = (ROOT / "ai_query/cli.py").read_text()
    assert '"enrich-templates"' in src
    assert "llm_enrich_templates" in src
    assert '"templates"' in src


def test_api_exposes_enrich_templates_route(isolated_home):
    from fastapi import FastAPI
    from ai_query.api import build_router
    from ai_query.service import AIService

    svc = AIService(_FakeCore())
    svc._ai = _FakeAgent()
    app = FastAPI()
    app.include_router(build_router(svc))
    paths = {r.path for r in app.routes}
    assert "/api/ai/llm/enrich-templates" in paths
    assert "/api/ai/llm/templates" in paths


def test_ui_wires_enrich_templates_button():
    panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "Enrich templates" in panel
    assert "def do_enrich" in panel
    assert "llm_enrich_templates" in panel
