"""Tests for NL->SQL validation, decoding guards, sql_check, and eval meters."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from ai_assistant.llm.decode import (
    GenerationConfig,
    guarded_generate,
    has_repeated_ngram,
    trim_sql_output,
)
from ai_assistant.llm.validation import validate_pair, validate_pairs


LEAKED_SQL = (
    "SELECT 'public. You execute the queries. You can think of many very common "
    "and useful example SQL queries so that it is very common and useful to train LLM. "
    "SELECT' AS table_name"
)


class FakeCore:
    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"
        )
        self._conn.execute("INSERT INTO items (name) VALUES ('a'), ('b')")
        self._conn.commit()

    def execute(self, name: str, sql: str):
        try:
            cur = self._conn.execute(sql)
            if cur.description:
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                return {"columns": cols, "rows": rows}
            return {"columns": [], "rows": [], "rowcount": cur.rowcount}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def get_connection_profile(self, name: str):
        return {"db_type": "SQLite", "name": name}


def test_validate_pair_rejects_leaked_prose_sql():
    ok, _cleaned, reason = validate_pair(
        {"question": "how many tables", "sql": LEAKED_SQL},
        db_type="SQLite",
    )
    assert not ok
    assert reason


def test_validate_pair_accepts_real_sql():
    ok, cleaned, reason = validate_pair(
        {"question": "count items", "sql": "SELECT COUNT(*) FROM items"},
        db_type="SQLite",
    )
    assert ok, reason
    assert cleaned["sql"].upper().startswith("SELECT")


def test_collect_pairs_filters_bad_rag(monkeypatch):
    from ai_assistant.llm.service import LlmService

    svc = LlmService()
    monkeypatch.setattr(
        LlmService,
        "_rag_examples",
        staticmethod(lambda _conn: [
            {"question": "how many tables", "sql": LEAKED_SQL},
            {"question": "count items", "sql": "SELECT COUNT(*) FROM items"},
        ]),
    )
    pairs = svc.collect_pairs(include_sample=False, rag_connection="x", db_type="SQLite")
    assert len(pairs) == 1
    assert "COUNT" in pairs[0]["sql"].upper()


def test_trim_sql_output_strips_prose_prefix():
    raw = "You execute queries. SELECT COUNT(*) FROM items"
    assert trim_sql_output(raw).upper().startswith("SELECT")


def test_has_repeated_ngram_detects_loop():
    assert has_repeated_ngram([1, 2, 1, 2], 1, 2)


def test_guarded_generate_respects_ngram_block():
    vocab = 8
    calls = {"n": 0}

    def predict(_ctx):
        calls["n"] += 1
        probs = [0.01] * vocab
        probs[2] = 0.9
        return probs

    out = guarded_generate(
        [0, 1],
        predict_proba=predict,
        decode_token=lambda i: str(i),
        pad_id=0,
        eos_id=7,
        context=4,
        config=GenerationConfig(max_new=6, no_repeat_ngram=2, repetition_penalty=1.0, top_k=0),
    )
    assert len(out) <= 6


def test_check_sql_parse_and_explain_sqlite():
    from ai_assistant.llm.sql_check import check_sql

    core = FakeCore()
    r = check_sql(
        "SELECT COUNT(*) FROM items",
        db_type="SQLite",
        core=core,
        connection="test",
    )
    assert r["parse_ok"]
    assert r["valid"]


def test_eval_lightweight_on_fake_core():
    from ai_assistant.llm.eval import evaluate_model

    pairs = [
        {"question": "count items", "sql": "SELECT COUNT(*) FROM items"},
        {"question": "list items", "sql": "SELECT name FROM items"},
        {"question": "count rows", "sql": "SELECT COUNT(*) AS n FROM items"},
        {"question": "all items", "sql": "SELECT * FROM items LIMIT 5"},
    ]
    core = FakeCore()

    def gen(_q: str) -> str:
        return "SELECT COUNT(*) FROM items"

    r = evaluate_model(
        pairs=pairs,
        generate_fn=gen,
        connection="test",
        db_type="SQLite",
        core=core,
        depth="lightweight",
    )
    assert r["ok"]
    s = r["summary"]
    assert s["count"] >= 1
    assert "parse_ok_rate" in s


def test_llm_generate_returns_validity_fields(tmp_path, monkeypatch):
    from ai_assistant.llm.service import LlmService
    from ai_assistant.llm.dataset import SAMPLE_PAIRS

    svc = LlmService(models_dir=tmp_path / "llm")
    monkeypatch.setattr(svc, "collect_pairs", lambda **kw: SAMPLE_PAIRS[:4])
    train = svc.train(name="t", engine="python", include_sample=False, overrides={"connection": "test"})
    assert train.get("ok"), train.get("error")
    r = svc.generate(
        "count the number of orders",
        name="t",
        connection="test",
        db_type="SQLite",
        core=FakeCore(),
    )
    assert "valid" in r
    assert "parse_ok" in r
    assert "attempts" in r


def test_api_cli_wiring_strings():
    api = Path("ai_query/api.py").read_text(encoding="utf-8")
    cli = Path("ai_query/cli.py").read_text(encoding="utf-8")
    svc = Path("ai_query/service.py").read_text(encoding="utf-8")
    assert "/api/ai/llm/eval" in api
    assert "llm_eval" in api
    assert 'llm_sub.add_parser("eval"' in cli
    assert "llm_eval" in cli
    assert "/api/ai/llm/model-dataset" in api
    assert '"verify"' in cli and "llm_model_dataset" in cli
    assert "/api/ai/llm/harvest" in api and "llm_harvest" in api
    assert '"harvest"' in cli and "llm_harvest" in cli
    assert "def llm_harvest" in svc and "LlmHarvestService" in svc
    # Graceful harvest stop wired across service + API (+ CLI Ctrl-C handler).
    assert "/api/ai/llm/harvest/stop" in api and "llm_harvest_stop" in api
    assert "def llm_harvest_stop" in svc
    assert "should_stop" in svc and "_harvest_cancels" in svc
    assert "should_stop" in cli
    # Live training progress + background LLM jobs.
    assert "/api/ai/llm/jobs" in api and "llm_job_events" in api
    assert "/api/ai/llm/jobs/{job_id}/stop" in api
    assert "progress=progress" in cli and "training_epoch" in cli
    assert "_model_epoch_progress" in Path("ai_assistant/llm/training_service.py").read_text(
        encoding="utf-8"
    )
    # Incremental + parallel harvest knobs wired in service/CLI/config.
    assert "train_mode" in svc and "gen_workers" in svc
    assert "_apply_harvest_config" in svc and "gen_timeout" in svc
    assert "--train-mode" in cli and "--gen-workers" in cli
    assert "--gen-timeout" in cli and "--no-retry-backlog" in cli
    harvest_cfg = Path("ai_query/module_config.py").read_text(encoding="utf-8")
    assert "train_mode" in harvest_cfg and "gen_workers" in harvest_cfg
    assert "gen_retries" in harvest_cfg and "retry_backlog" in harvest_cfg


def test_normalize_question_for_match_case_insensitive():
    from ai_assistant.llm.validation import normalize_question_for_match

    assert normalize_question_for_match("  How   is DB health? ") == normalize_question_for_match(
        "how is db health?"
    )
    # Trailing punctuation should not block exact recall.
    assert normalize_question_for_match("How many employees are there?") == normalize_question_for_match(
        "how many employees are there"
    )


def test_dedupe_pairs_respects_mariadb_dialect():
    from ai_assistant.llm.data_sources import _dedupe_pairs

    pairs = [
        {
            "question": "Show DEPT_ID from DEPARTMENTS",
            "sql": "SELECT `DEPT_ID` FROM `DEPARTMENTS` LIMIT 5",
            "description": "projection",
        },
        {
            "question": "Count tables",
            "sql": "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = SCHEMA()",
            "description": "catalog",
        },
    ]
    kept_generic = _dedupe_pairs(pairs, db_type=None)
    kept_maria = _dedupe_pairs(pairs, db_type="MariaDB")
    assert len(kept_generic) == 0
    assert len(kept_maria) == 2


def test_tokenizer_preserves_backtick_identifiers():
    from ai_assistant.llm.tokenizer import WordTokenizer

    sql = "SELECT COUNT(*) FROM `EMPLOYEES`"
    tok = WordTokenizer().build([sql])
    decoded = tok.decode(tok.encode(sql))
    assert "` EMPLOYEES `" not in decoded
    assert "`EMPLOYEES`" in decoded or "EMPLOYEES" in decoded


def test_tokenizer_preserves_double_quoted_identifiers():
    from ai_assistant.llm.tokenizer import WordTokenizer

    sql = 'SELECT COUNT(*) FROM "public"."employees"'
    tok = WordTokenizer().build([sql])
    decoded = tok.decode(tok.encode(sql))
    assert '" public "' not in decoded
    assert "public" in decoded and "employees" in decoded


def test_exact_recall_returns_saved_sql(tmp_path):
    import json

    from ai_assistant.llm.service import LlmService

    root = tmp_path / "llm"
    mdir = root / "recall"
    mdir.mkdir(parents=True)
    pair = {
        "question": "How is the health of Database",
        "sql": 'SELECT COUNT(*) FROM "public"."departments"',
    }
    (mdir / "dataset.jsonl").write_text(json.dumps(pair) + "\n", encoding="utf-8")
    (mdir / "meta.json").write_text(json.dumps({"engine": "python", "trained": True}), encoding="utf-8")
    svc = LlmService(models_dir=root)
    r = svc.generate("how is the health of database", name="recall")
    assert r.get("ok")
    assert r.get("recalled") is True
    assert "departments" in (r.get("sql") or "")


def test_train_pairs_rejects_live_invalid_sql():
    from ai_assistant.llm.training_service import LlmTrainingService

    svc = LlmTrainingService(FakeCore())
    r = svc.train_pairs(
        [{"question": "missing table", "sql": "SELECT * FROM no_such_table_xyz"}],
        names=["bad_model"],
        connection="test",
    )
    assert not r.get("ok")
    assert "rejected" in (r.get("error") or "").lower() or "validation" in (r.get("error") or "").lower()


def test_local_backend_rejects_invalid_sql_when_connection_set():
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    class _FakeSvc:
        def generate(self, *a, **kw):
            return {
                "ok": True,
                "sql": 'SELECT " public ". " employees "',
                "valid": False,
                "reason": "syntax error",
            }

    LocalLlmBackend.set_runtime(connection="test", db_type="PostgreSQL", core=object())
    b = LocalLlmBackend()
    b._svc = _FakeSvc()
    out = b.call("USER QUESTION: count employees\n")
    assert out.get("error")
    assert out.get("response") is None


def test_ai_query_local_llm_generation_does_not_register_live_executor(monkeypatch):
    from ai_query.agent import AIQueryAgent
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    runtime: dict = {}

    def fake_set_runtime(**kwargs):
        runtime.update(kwargs)

    class _Backend:
        name = "local-llm"
        display_name = "Local LLM"

        def is_available(self):
            return True

    class _Db:
        db_type = "PostgreSQL"
        conn = object()

        def execute_query(self, _sql):
            raise AssertionError("Generate SQL must not execute live DB probes")

    monkeypatch.setattr(LocalLlmBackend, "set_runtime", staticmethod(fake_set_runtime))

    agent = AIQueryAgent()
    agent.cli_available = True
    agent._active_backend = _Backend()
    monkeypatch.setattr(agent, "get_cached_comprehensive_context", lambda *_a, **_kw: {
        "database_type": "PostgreSQL",
        "question_complexity": 0,
        "schema": {"tables": ["employees"], "table_schemas": {}},
    })
    monkeypatch.setattr(agent, "_build_intelligent_context", lambda *_a, **_kw: "")
    monkeypatch.setattr(agent, "_augment_with_rag", lambda db_context, *_a, **_kw: db_context)
    monkeypatch.setattr(agent, "_call_ai", lambda *_a, **_kw: {
        "response": (
            "SQL:\n```sql\nSELECT table_name FROM information_schema.tables\n```\n"
            "SUMMARY_SQL:\nSELECT table_name FROM information_schema.tables\n\n"
            "EXPLANATION:\nGenerated locally."
        ),
        "error": None,
    })

    result = agent.ask_question("list all tables", _Db(), "local_pg")

    assert not result.get("error")
    assert runtime["connection"] == "local_pg"
    assert runtime["db_type"] == "PostgreSQL"
    assert "executor" not in runtime


def test_local_backend_extracts_user_question_from_followup_prompt():
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    prompt = (
        "You are an INTELLIGENT DATABASE AGENT helping refine queries.\n\n"
        "Previous Conversation:\nUser: list all tables\n\n"
        "Current SQL Query:\nSELECT 1\n\n"
        "User's Follow-up Message: list all databases\n\n"
        "USER QUESTION: list all databases\n\n"
        "CRITICAL INSTRUCTIONS:\n1. Use ONLY tables...\n"
    )
    assert LocalLlmBackend._extract_question(prompt) == "list all databases"


def test_local_backend_rejects_long_followup_without_user_question_marker():
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    prompt = (
        "You are an INTELLIGENT DATABASE AGENT helping refine queries.\n" * 80
        + "User's Follow-up Message: list all databases\n"
    )
    assert LocalLlmBackend._extract_question(prompt) == ""


def test_ai_query_local_llm_followup_extracts_question(monkeypatch):
    from ai_query.agent import AIQueryAgent
    from ai_query.backends.local_llm_backend import LocalLlmBackend

    seen: dict = {}

    class _FakeSvc:
        def generate(self, question, **kw):
            seen["question"] = question
            seen["kwargs"] = kw
            return {
                "ok": True,
                "sql": "SELECT schema_name FROM information_schema.schemata",
                "valid": True,
                "reason": "",
            }

    class _Db:
        db_type = "PostgreSQL"
        conn = object()

    LocalLlmBackend.set_runtime(connection="local_pg", db_type="PostgreSQL")
    backend = LocalLlmBackend()
    backend._svc = _FakeSvc()
    monkeypatch.setattr(backend, "is_available", lambda: True)

    agent = AIQueryAgent()
    agent.cli_available = True
    agent._active_backend = backend
    agent.conversation_history = [
        {"role": "user", "content": "list all tables"},
        {"role": "assistant", "content": "SQL: SELECT 1"},
    ]
    agent.current_sql = "SELECT table_name FROM information_schema.tables"
    monkeypatch.setattr(agent, "get_cached_comprehensive_context", lambda *_a, **_kw: {
        "database_type": "PostgreSQL",
        "question_complexity": 0,
        "schema": {"tables": ["employees"], "table_schemas": {}},
    })
    monkeypatch.setattr(agent, "_build_intelligent_context", lambda *_a, **_kw: "")
    monkeypatch.setattr(agent, "_augment_with_rag", lambda db_context, *_a, **_kw: db_context)

    result = agent.send_follow_up("list all databases", _Db(), "local_pg")

    assert not result.get("error"), result.get("error")
    assert seen["question"] == "list all databases"
    assert seen["kwargs"]["connection"] == "local_pg"
    assert seen["kwargs"].get("executor") is None
    assert "SELECT schema_name" in (result.get("sql") or "")
    assert "USER QUESTION: list all databases" in (agent.last_prompt_sent or "")
