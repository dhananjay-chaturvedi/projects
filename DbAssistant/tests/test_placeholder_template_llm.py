"""Placeholder-template training + runtime resolution."""

from __future__ import annotations

import sqlite3
import types
from pathlib import Path
from unittest import mock

import pytest

from ai_assistant.llm.placeholder_resolver import has_placeholders, resolve
from ai_assistant.llm.query_templates import (
    ALL_PH_TOKENS,
    normalize_template_mode,
    render_object_templates,
    render_object_templates_delex,
)
from ai_assistant.llm.tokenizer import tokenize
from tests.test_llm_harvest import FakeCore


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def shop_core(tmp_path: Path) -> FakeCore:
    path = str(tmp_path / "shop.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL, status TEXT
        );
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY, name TEXT, email TEXT
        );
        INSERT INTO orders (customer_id, total, status) VALUES (1, 10.5, 'open');
        """
    )
    con.close()
    return FakeCore(path)


def test_ph_tokens_are_single_tokenizer_tokens():
    toks = tokenize("SELECT COUNT(*) AS total FROM PH_TABLE LIMIT PH_LIMIT")
    assert "PH_TABLE" in toks
    assert "PH_LIMIT" in toks
    assert "{" not in toks


def test_normalize_template_mode_defaults():
    assert normalize_template_mode(None) == "both"
    assert normalize_template_mode("concrete") == "concrete"
    assert normalize_template_mode("bogus") == "both"


def test_render_object_templates_delex_keeps_real_labels_in_question():
    from ai_assistant.llm.db_query_miner import ColumnInfo, TableInfo

    info = TableInfo(
        name="orders",
        columns=[
            ColumnInfo("id", "INTEGER"),
            ColumnInfo("total", "REAL"),
            ColumnInfo("status", "TEXT"),
        ],
    )

    class _Dialect:
        def quote(self, n):
            return n

        def col(self, n):
            return n

        def bounded_select(self, n, limit):
            return f"SELECT * FROM {n} LIMIT {limit}"

        def limit(self, sql, n):
            return f"{sql} LIMIT {n}"

    delex = render_object_templates_delex(info, _Dialect(), limit=5)
    concrete = render_object_templates(info, _Dialect(), limit=5)
    assert delex
    count_delex = next(p for p in delex if p.get("category") == "count")
    count_conc = next(p for p in concrete if p.get("category") == "count")
    assert "orders" in count_delex["question"].lower()
    assert "PH_TABLE" in count_delex["sql"]
    assert "orders" in count_conc["sql"].lower()
    assert count_delex.get("delexicalized") is True


def test_seed_corpus_placeholder_mode(shop_core: FakeCore):
    from ai_assistant.llm.seed_corpus import render_seed_pairs

    out = render_seed_pairs(
        shop_core, "shop", template_mode="placeholder", validate=False)
    assert out.get("pairs")
    assert any("PH_" in (p.get("sql") or "") for p in out["pairs"])
    assert any(p.get("delexicalized") for p in out["pairs"])


def test_has_placeholders_detects_ph_tokens():
    assert has_placeholders("SELECT COUNT(*) FROM PH_TABLE")
    assert not has_placeholders("SELECT COUNT(*) FROM orders")


def test_resolver_maps_orders_table(shop_core: FakeCore):
    res = resolve(
        "SELECT COUNT(*) AS total FROM PH_TABLE",
        "How many rows are in the orders table?",
        core=shop_core, connection="shop", db_type="SQLite",
    )
    assert res.get("ok") is True
    assert res.get("resolved") is True
    assert "orders" in (res.get("sql") or "").lower()
    assert res.get("mappings", {}).get("PH_TABLE")


def test_resolver_passthrough_without_placeholders(shop_core: FakeCore):
    sql = "SELECT COUNT(*) FROM orders"
    res = resolve(sql, "count orders", core=shop_core, connection="shop", db_type="SQLite")
    assert res.get("sql") == sql
    assert res.get("resolved") is False


def test_resolver_ai_fallback_picks_candidate(shop_core: FakeCore):
    picked = {"ok": False}

    def fake_pick(candidates, question, sql):
        picked["ok"] = True
        return candidates[0]

    res = resolve(
        "SELECT COUNT(*) AS total FROM PH_TABLE",
        "ambiguous table query",
        core=shop_core, connection="shop", db_type="SQLite",
        ai_pick_fn=fake_pick,
    )
    assert picked["ok"] is True
    assert res.get("ok") is True or res.get("resolved")


def test_generate_resolves_placeholders(shop_core: FakeCore, tmp_path: Path, monkeypatch):
    from ai_assistant.llm.service import LlmService

    svc = LlmService(models_dir=tmp_path / "llm")
    mdir = svc._model_dir("ph_test")
    mdir.mkdir(parents=True)
    (mdir / "meta.json").write_text('{"engine": "python"}', encoding="utf-8")
    (mdir / "dataset.jsonl").write_text(
        '{"question":"[sqlite] count orders","sql":"SELECT COUNT(*) AS total FROM PH_TABLE"}\n',
        encoding="utf-8",
    )

    class _Eng:
        def generate(self, question, mdir, params=None):
            return {"sql": "SELECT COUNT(*) AS total FROM PH_TABLE", "explanation": ""}

    monkeypatch.setattr(svc, "_resolve_for_model", lambda *a, **k: (_Eng(), "python"))
    monkeypatch.setattr(
        svc, "_is_trained", lambda mdir, eng: True,
    )

    r = svc.generate(
        "How many rows are in the orders table?",
        name="ph_test",
        connection="shop",
        db_type="SQLite",
        core=shop_core,
    )
    assert r.get("ok") is True
    assert "orders" in (r.get("sql") or "").lower()
    assert r.get("resolved") is True
    assert r.get("mappings", {}).get("PH_TABLE")


def test_apply_harvest_config_template_mode(monkeypatch):
    from ai_query.service import AIService

    monkeypatch.setattr(
        "ai_query.module_config.get",
        lambda sec, key, default="": "placeholder" if key == "template_mode" else default,
    )
    monkeypatch.setattr(
        "ai_query.module_config.get_bool",
        lambda *a, **k: k.get("default", False),
    )
    monkeypatch.setattr(
        "ai_query.module_config.get_int",
        lambda *a, **k: k.get("default", 0),
    )
    body = AIService._apply_harvest_config({})
    assert body.get("template_mode") == "placeholder"


def test_cli_harvest_has_template_mode():
    src = (ROOT / "ai_query/cli.py").read_text()
    assert "--template-mode" in src
    assert "template_mode" in src


def test_api_harvest_model_has_template_mode():
    from ai_query.api import LlmHarvestRequest

    assert "template_mode" in LlmHarvestRequest.model_fields


def test_ui_template_mode_selector():
    panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "template_mode_var" in panel
    assert '"template_mode"' in panel
