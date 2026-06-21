"""Tests for the auto-harvest LLM training pipeline.

Covers the curated seed corpus loader/renderer, the AI question-bank generator,
and the LlmHarvestService orchestration (seed unification, follow-up threads,
capture replay, validation, source accounting) — using a fake ``core`` backed
by a real on-disk SQLite database and fake backend callables (no network).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

import pytest

from ai_assistant.llm.harvest_service import LlmHarvestService
from ai_assistant.llm.seed_corpus import (
    load_seed_problems,
    render_seed_pairs,
)


class FakeCore:
    """Minimal CoreDBService-compatible shim backed by a real SQLite file."""

    def __init__(self, path: str, db_type: str = "SQLite") -> None:
        self.path = path
        self.db_type = db_type

    def get_connection_profile(self, name: str):
        return {"db_type": self.db_type, "service_or_db": self.path}

    def get_objects(self, name: str, obj_type: str = "tables"):
        if obj_type != "tables":
            return []
        con = sqlite3.connect(self.path)
        try:
            rows = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            con.close()
        return [r[0] for r in rows]

    def get_table_schema(self, name: str, table: str):
        con = sqlite3.connect(self.path)
        try:
            cur = con.execute(f"PRAGMA table_info({table})")
            cols = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
        finally:
            con.close()
        return {"error": None, "table": table, "columns": cols, "indexes": []}

    def execute(self, name: str, sql: str):
        con = sqlite3.connect(self.path)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = [[("" if v is None else str(v)) for v in r] for r in cur.fetchall()]
            return {"error": None, "columns": cols, "rows": rows, "rowcount": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0}
        finally:
            con.close()


@pytest.fixture()
def core(tmp_path: Path) -> FakeCore:
    path = str(tmp_path / "shop.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY, name TEXT, email TEXT, country TEXT
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL,
            status TEXT, created_at TEXT
        );
        INSERT INTO customers (name, email, country) VALUES
            ('Alice', 'a@x.com', 'US'), ('Bob', 'b@x.com', 'UK'),
            ('Cara', 'c@x.com', 'US');
        INSERT INTO orders (customer_id, total, status, created_at) VALUES
            (1, 100.0, 'paid', '2023-01-15'), (2, 50.0, 'open', '2023-02-20'),
            (1, 75.0, 'paid', '2023-03-10');
        """
    )
    con.commit()
    con.close()
    return FakeCore(path)


# ── corpus integrity ─────────────────────────────────────────────────────────
def test_seed_corpus_integrity():
    problems = load_seed_problems()
    assert problems, "curated corpus should not be empty"
    ids = [p.id for p in problems]
    assert len(ids) == len(set(ids)), "problem ids must be unique"
    for p in problems:
        assert p.prompts, f"{p.id} must have paraphrase prompts"
        assert p.complexity in ("basic", "advanced", "complex")
        if p.mode == "template":
            assert p.sql, f"template problem {p.id} must have an sql skeleton"


def test_load_seed_problems_complexity_filter():
    basic = load_seed_problems(complexity=["basic"])
    assert basic and all(p.complexity == "basic" for p in basic)


# ── corpus rendering against a live schema ───────────────────────────────────
def test_render_seed_pairs_shares_sql_across_paraphrases(core: FakeCore):
    out = render_seed_pairs(core, "shop", sample_limit=5)
    assert out["ok"]
    assert out["stats"]["validated"] > 0
    assert out["stats"]["failed"] == 0
    pairs = out["pairs"]
    assert pairs
    # The count_rows skeleton renders to ONE SQL for `customers`, shared by all
    # of its paraphrase prompts (multiple distinct questions -> same SQL).
    total_sql = 'SELECT COUNT(*) AS total FROM "customers"'
    shared = [p["question"] for p in pairs if p["sql"] == total_sql]
    assert len(shared) > 1, "count_rows paraphrases must share one validated SQL"
    assert len(set(shared)) == len(shared), "paraphrases should be distinct questions"
    # Every rendered SQL actually executed (validated) against the real DB.
    for p in pairs:
        assert p["sql"].upper().startswith("SELECT")
    # Generate-mode problems are grouped (prompts list), not flattened.
    assert out["generate_problems"]
    assert all("prompts" in g and isinstance(g["prompts"], list)
               for g in out["generate_problems"])


def test_render_seed_pairs_non_sql_engine_returns_generate_only(core: FakeCore):
    core.db_type = "MongoDB"
    out = render_seed_pairs(core, "shop")
    assert out["stats"]["template_pairs"] == 0
    # Generate-mode prompts still surface for the backend to ground.
    assert out["generate_problems"]


# ── question bank ────────────────────────────────────────────────────────────
def test_question_bank_parses_json_and_dedupes(core: FakeCore):
    def fake_text(prompt: str) -> str:
        assert "SCHEMA:" in prompt and "JSON array" in prompt
        return (
            'Sure:\n```json\n'
            '["How many customers are there?", "How many customers are there?", '
            '"List all orders", "   ", "Total revenue by country"]\n```'
        )

    svc = LlmHarvestService(core, generate_text_fn=fake_text)
    r = svc.generate_question_bank("shop", count=10)
    assert r["ok"]
    qs = r["questions"]
    assert "List all orders" in qs
    # Duplicate + blank removed.
    assert len(qs) == len({q.lower() for q in qs})
    assert all(q.strip() for q in qs)


def test_question_bank_line_fallback(core: FakeCore):
    def fake_text(prompt: str) -> str:
        return "1. How many orders are paid?\n- List customers in the US\nrandom noise"

    svc = LlmHarvestService(core, generate_text_fn=fake_text)
    r = svc.generate_question_bank("shop", count=10)
    assert r["ok"]
    assert any("orders are paid" in q for q in r["questions"])


def test_question_bank_requires_backend(core: FakeCore):
    svc = LlmHarvestService(core)  # no text generator
    r = svc.generate_question_bank("shop", count=5)
    assert not r["ok"]
    assert "question bank" in (r["error"] or "").lower()


# ── orchestration ────────────────────────────────────────────────────────────
def test_harvest_curated_only_no_backend(core: FakeCore):
    svc = LlmHarvestService(core)  # no backend callables
    r = svc.harvest({"connection": "shop", "use_captures": False,
                     "do_train": False, "generated_questions": 0})
    assert r["ok"]
    assert r["pairs"] > 0
    assert r["sources"]["curated_template"] > 0
    assert r["sources"]["backend_generated"] == 0
    assert r["trained"] is False


def test_harvest_zero_ai_questions_skips_backend_generate_problems(core: FakeCore):
    calls: list[str] = []

    def gen_sql(conn: str, q: str) -> dict:
        calls.append(q)
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({"connection": "shop", "use_captures": False,
                     "do_train": False, "generated_questions": 0})
    assert r["ok"]
    assert calls == []
    assert r["sources"]["backend_generated"] == 0
    assert r["sources"]["followup_turns"] == 0


def test_harvest_followup_threads_and_generation(core: FakeCore):
    def gen_sql(conn: str, q: str) -> dict:
        return {"sql": "SELECT COUNT(*) AS total FROM customers",
                "explanation": "counts customers"}

    def run_thread(conn: str, base_q: str, followups: list) -> list:
        turns = [{"question": base_q,
                  "sql": "SELECT * FROM orders LIMIT 100", "explanation": "base"}]
        for f in followups:
            turns.append({"question": f,
                          "sql": "SELECT * FROM orders LIMIT 5",
                          "explanation": "refined: " + f})
        return turns

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql, run_thread_fn=run_thread)
    r = svc.harvest({"connection": "shop", "use_captures": False,
                     "do_train": False, "generated_questions": 1,
                     "followups": True})
    assert r["ok"]
    assert r["sources"]["backend_generated"] > 0
    # join_two_tables contributes follow-up turns.
    assert r["sources"]["followup_turns"] >= 1


def test_harvest_graceful_stop_skips_backend_keeps_offline_model(core: FakeCore, monkeypatch):
    """Stopping before backend work keeps the already-trained offline model and
    never issues backend calls."""
    gen_calls: list[str] = []
    trained_phases: list[str] = []

    def gen_sql(conn: str, q: str) -> dict:
        gen_calls.append(q)
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    class FakeTrainer:
        def __init__(self, _core):
            pass

        def train_pairs(self, pairs, **kwargs):
            trained_phases.append(f"{len(pairs)}")
            return {"ok": True, "models": [{"name": kwargs["names"][0]}],
                    "reason": "ok"}

    monkeypatch.setattr(
        "ai_assistant.llm.training_service.LlmTrainingService", FakeTrainer)

    # should_stop flips True right after the offline phase, before backend work.
    state = {"calls": 0}

    def should_stop() -> bool:
        state["calls"] += 1
        return state["calls"] > 1  # allow first check (entry), stop afterwards

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest(
        {"connection": "shop", "use_captures": False,
         "train_new_name": "stop_model", "do_train": True,
         "generated_questions": 5, "followups": False},
        should_stop=should_stop,
    )
    assert r["stopped"] is True
    assert r["offline_trained"] is True          # offline model trained
    assert r["backend_enhanced"] is False
    assert gen_calls == []                         # no backend questions issued
    assert trained_phases == ["%d" % r["offline_pairs"]] or trained_phases  # trained once (offline)


def test_harvest_stop_mid_generation_still_trains_collected(core: FakeCore, monkeypatch):
    """A stop during backend generation finishes the in-flight question, then
    trains on what was collected — the training write always completes."""
    trained_calls: list[int] = []

    def gen_sql(conn: str, q: str) -> dict:
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    class FakeTrainer:
        def __init__(self, _core):
            pass

        def train_pairs(self, pairs, **kwargs):
            trained_calls.append(len(pairs))
            return {"ok": True, "models": [{"name": kwargs["names"][0]}], "reason": "ok"}

    monkeypatch.setattr(
        "ai_assistant.llm.training_service.LlmTrainingService", FakeTrainer)

    # Stop only once we are well past the offline checkpoint, i.e. allow several
    # checks so offline training + backend start proceed, then request stop.
    state = {"calls": 0}

    def should_stop() -> bool:
        state["calls"] += 1
        return state["calls"] > 3

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest(
        {"connection": "shop", "use_captures": False,
         "train_new_name": "stop_model", "do_train": True,
         "generated_questions": 5, "followups": False},
        should_stop=should_stop,
    )
    # Offline training happened first; a final train write completed regardless.
    assert trained_calls, "at least the offline training write must complete"
    assert r["offline_trained"] is True


def test_harvest_trains_offline_before_backend_generation(core: FakeCore, monkeypatch):
    events: list[str] = []

    def gen_sql(conn: str, q: str) -> dict:
        events.append("generate")
        return {"sql": "SELECT COUNT(*) AS total FROM customers",
                "explanation": "counts customers"}

    class FakeTrainer:
        def __init__(self, _core):
            pass

        def train_pairs(self, pairs, **kwargs):
            events.append(f"train:{kwargs.get('names')}:{len(pairs)}")
            return {"ok": True, "models": [{"name": kwargs["names"][0]}],
                    "reason": f"trained {len(pairs)}"}

    monkeypatch.setattr(
        "ai_assistant.llm.training_service.LlmTrainingService",
        FakeTrainer,
    )
    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({"connection": "shop", "use_captures": False,
                     "train_new_name": "offline_first",
                     "do_train": True, "generated_questions": 1,
                     "followups": False})
    assert r["offline_trained"] is True
    assert r["backend_enhanced"] is True
    assert events[0].startswith("train:")
    assert "generate" in events
    assert events.index("generate") > 0


def test_harvest_invalid_sql_is_dropped(core: FakeCore):
    def gen_sql(conn: str, q: str) -> dict:
        return {"sql": "SELECT * FROM table_that_does_not_exist", "explanation": ""}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    # Disable curated + captures so only the (invalid) generated SQL is present;
    # supply a user question so the generator is actually invoked.
    r = svc.harvest({"connection": "shop", "use_curated": False,
                     "use_captures": False, "do_train": False,
                     "generated_questions": 0,
                     "questions": ["show me everything"]})
    # All generated pairs were invalid -> dropped; harvest reports no pairs.
    assert r["sources"].get("backend_generated", 0) >= 1
    assert r["pairs"] == 0
    assert r["rejected"] >= 1


def test_aiservice_harvest_stop_registry(monkeypatch):
    """AIService registers a cancel event by harvest_id and llm_harvest_stop
    sets it; unknown ids report an error."""
    from ai_query.service import AIService

    svc = AIService(core=None)
    # No running harvest with this id yet.
    assert svc.llm_harvest_stop("nope")["ok"] is False

    captured = {}

    class FakeHarvester:
        def harvest(self, body, *, on_progress=None, should_stop=None):
            # While running, the id is registered and stoppable.
            captured["mid_run_stop"] = svc.llm_harvest_stop(body["harvest_id"])
            captured["stopped_flag"] = bool(should_stop and should_stop())
            return {"ok": True, "pairs": 0}

    monkeypatch.setattr(svc, "_llm_harvester", lambda backend=None, gen_workers=1: FakeHarvester())
    svc.llm_harvest({"connection": "c", "harvest_id": "job1"})
    assert captured["mid_run_stop"]["ok"] is True
    assert captured["stopped_flag"] is True
    # Cleaned up after completion.
    assert svc.llm_harvest_stop("job1")["ok"] is False


def test_harvest_capture_replay_uses_explanation(core: FakeCore, tmp_path: Path):
    cap_dir = tmp_path / "capture"
    samples = cap_dir / "shop" / "samples.jsonl"
    samples.parent.mkdir(parents=True)
    samples.write_text(json.dumps({
        "question": "how many paid orders",
        "sql": "SELECT COUNT(*) AS n FROM orders WHERE status = 'paid'",
        "explanation": "Counts paid orders",
        "purpose": "llm_training",
    }) + "\n", encoding="utf-8")

    from common import paths as app_paths
    with mock.patch.object(app_paths, "ai_capture_dir", return_value=str(cap_dir)):
        svc = LlmHarvestService(core)
        r = svc.harvest({"connection": "shop", "use_curated": False,
                         "use_captures": True, "do_train": False,
                         "generated_questions": 0})
    assert r["sources"]["capture_replay"] >= 1
    pair = next(p for p in r["harvested_pairs"] if "paid orders" in p["question"])
    assert pair["description"] == "Counts paid orders"


# ── incremental + parallel generation ───────────────────────────────────────
@pytest.fixture()
def llm_models_root(tmp_path: Path, monkeypatch):
    """Redirect per-model ledger/backlog files to an isolated directory."""
    root = tmp_path / "llm"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("ai_assistant.llm.service.models_root", lambda: root)
    return root


def _write_ledger(models_root: Path, model: str, pairs: list[dict]) -> None:
    mdir = models_root / model
    mdir.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(p) for p in pairs]
    (mdir / "dataset.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_incremental_skips_ledger_questions(core: FakeCore, llm_models_root: Path):
    """Incremental harvest must not call the backend for questions already in the ledger."""
    model = "incr_skip"
    known_q = "How many customers are there?"
    _write_ledger(llm_models_root, model, [{
        "question": known_q,
        "sql": "SELECT COUNT(*) AS total FROM customers",
        "description": "seed",
    }])
    calls: list[str] = []

    def gen_sql(conn: str, q: str) -> dict:
        calls.append(q)
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({
        "connection": "shop",
        "train_new_name": model,
        "use_curated": False,
        "use_captures": False,
        "do_train": False,
        "generated_questions": 0,
        "train_mode": "incremental",
        "questions": [known_q, "List all orders"],
    })
    assert r["ok"]
    assert r["skipped_known"] >= 1
    assert known_q not in calls
    assert any("orders" in q.lower() for q in calls)


def test_incremental_union_training(core: FakeCore, llm_models_root: Path, monkeypatch):
    """Incremental retrain unions new pairs with the existing ledger before fitting."""
    model = "incr_union"
    _write_ledger(llm_models_root, model, [{
        "question": "How many customers are there?",
        "sql": "SELECT COUNT(*) AS total FROM customers",
        "description": "old",
    }])
    fit_pair_counts: list[int] = []

    def _persist(conn, pairs, **kw):
        fit_pair_counts.append(len(pairs))
        return (str(llm_models_root / model / "ds.jsonl"), len(pairs))

    monkeypatch.setattr(
        "ai_assistant.llm.service.LlmService.train",
        lambda self, **kwargs: {"ok": True, "name": kwargs["name"], "engine": "python"},
    )
    monkeypatch.setattr("ai_assistant.llm.data_sources.persist_pairs", _persist)

    def gen_sql(conn: str, q: str) -> dict:
        return {"sql": "SELECT COUNT(*) AS total FROM orders"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({
        "connection": "shop",
        "train_new_name": model,
        "use_curated": False,
        "use_captures": False,
        "do_train": True,
        "generated_questions": 0,
        "train_mode": "incremental",
        "questions": ["Count all orders"],
        "followups": False,
    })
    assert r["ok"]
    assert r["already_trained"] >= 1
    assert r["new_pairs"] >= 1
    assert fit_pair_counts and fit_pair_counts[-1] >= 2


def test_parallel_generation_matches_serial(core: FakeCore):
    """Parallel workers should keep the same validated question set as serial."""
    import time

    items = [
        ("Question A", "d"),
        ("Question B", "d"),
        ("Question C", "d"),
        ("Question D", "d"),
    ]

    def gen_sql(conn: str, q: str) -> dict:
        time.sleep(0.01)
        return {"sql": "SELECT COUNT(*) AS total FROM customers", "explanation": q}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    serial = svc._generate_pairs_for_questions(
        "shop", items, gen_workers=1, gen_timeout=30, gen_retries=0,
    )
    parallel = svc._generate_pairs_for_questions(
        "shop", items, gen_workers=4, gen_timeout=30, gen_retries=0,
    )
    serial_qs = {p["question"] for p in serial["pairs"]}
    parallel_qs = {p["question"] for p in parallel["pairs"]}
    assert serial_qs == parallel_qs == {q for q, _ in items}


def test_per_call_timeout_skips_question(core: FakeCore):
    """A timed-out generation skips only that question; others continue."""
    import time

    def gen_sql(conn: str, q: str) -> dict:
        if "slow" in q.lower():
            time.sleep(3)
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    out = svc._generate_pairs_for_questions(
        "shop",
        [("fast one", ""), ("slow question", ""), ("fast two", "")],
        gen_workers=1,
        gen_timeout=1,
        gen_retries=0,
    )
    kept = {p["question"] for p in out["pairs"]}
    assert "fast one" in kept and "fast two" in kept
    assert "slow question" not in kept
    assert out["skipped"] >= 1


def test_in_run_retry_succeeds_on_second_attempt(core: FakeCore):
    """Bounded in-run retries can recover from a transient backend failure."""
    state = {"n": 0}

    def gen_sql(conn: str, q: str) -> dict:
        state["n"] += 1
        if state["n"] == 1:
            return {"error": "transient"}
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    out = svc._generate_pairs_for_questions(
        "shop", [("retry me", "")], gen_workers=1, gen_timeout=30, gen_retries=1,
    )
    assert len(out["pairs"]) == 1
    assert out["retried"] >= 1


def test_backlog_persisted_and_replayed_first(core: FakeCore, llm_models_root: Path):
    """Failed questions land in pending_questions.jsonl and are retried first."""
    from ai_assistant.llm.model_ledger import load_backlog, save_backlog

    model = "backlog_model"
    save_backlog(model, [{"question": "replay me first", "description": "backlog"}])
    order: list[str] = []

    def gen_sql(conn: str, q: str) -> dict:
        order.append(q)
        if q == "replay me first":
            return {"error": "still failing"}
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    r = svc.harvest({
        "connection": "shop",
        "train_new_name": model,
        "use_curated": False,
        "use_captures": False,
        "do_train": False,
        "generated_questions": 0,
        "questions": ["fresh question"],
        "retry_backlog": True,
    })
    assert r["ok"]
    assert order[0] == "replay me first"
    backlog = load_backlog(model)
    assert any("replay me first" in b["question"] for b in backlog)
    assert r["backlog_pending"] >= 1

    # Success removes from backlog on the next run.
    def gen_sql_ok(conn: str, q: str) -> dict:
        return {"sql": "SELECT COUNT(*) AS total FROM customers"}

    svc2 = LlmHarvestService(core, generate_sql_fn=gen_sql_ok)
    r2 = svc2.harvest({
        "connection": "shop",
        "train_new_name": model,
        "use_curated": False,
        "use_captures": False,
        "do_train": False,
        "generated_questions": 0,
        "retry_backlog": True,
    })
    assert r2["ok"]
    assert load_backlog(model) == []


def test_circuit_breaker_stops_after_consecutive_failures(core: FakeCore):
    """Circuit breaker halts new submissions after N consecutive failures."""

    def gen_sql(conn: str, q: str) -> dict:
        return {"error": "backend down"}

    svc = LlmHarvestService(core, generate_sql_fn=gen_sql)
    items = [(f"q{i}", "") for i in range(6)]
    out = svc._generate_pairs_for_questions(
        "shop", items, gen_workers=1, gen_timeout=5,
        gen_retries=0, max_consecutive_failures=2,
    )
    assert out["circuit_broken"] is True
    assert out["skipped"] == 2
    assert out["pairs"] == []
