#!/usr/bin/env python3
"""Runtime verification for Train LLM dropdown + panel wiring.

Exercises the same backend paths the Tk UI uses, plus static UI wiring checks.
Run: python tests/verify_train_llm_runtime.py [--connection NAME]
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONNECTION_DEFAULT = "PostgreSQL-db_assistant-1"
FALLBACK_CONN = "verify-shop"


class FakeCore:
    """Minimal CoreDBService shim backed by a real SQLite file."""

    def __init__(self, path: str, db_type: str = "SQLite") -> None:
        import sqlite3

        self.path = path
        self.db_type = db_type
        self._sqlite3 = sqlite3

    def get_connection_profile(self, name: str):
        return {"db_type": self.db_type, "service_or_db": self.path, "name": name}

    def get_manager(self, name: str, profile: dict | None = None):
        return self

    conn = property(lambda self: self._sqlite3.connect(self.path))

    def get_objects(self, name: str, obj_type: str = "tables"):
        if obj_type != "tables":
            return []
        con = self._sqlite3.connect(self.path)
        try:
            rows = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        finally:
            con.close()
        return [r[0] for r in rows]

    def get_table_schema(self, name: str, table: str):
        con = self._sqlite3.connect(self.path)
        try:
            cur = con.execute(f'PRAGMA table_info("{table}")')
            cols = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
        finally:
            con.close()
        return {"error": None, "table": table, "columns": cols, "indexes": []}

    def execute(self, name: str, sql: str):
        con = self._sqlite3.connect(self.path)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = [[("" if v is None else str(v)) for v in r] for r in cur.fetchall()]
            return {"error": None, "columns": cols, "rows": rows, "rowcount": len(rows)}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0}
        finally:
            con.close()

    def execute_query(self, sql: str):
        res = self.execute("", sql)
        if res.get("error"):
            return None, res["error"]
        return {"columns": res["columns"], "rows": res["rows"], "rowcount": res["rowcount"]}, None


def _make_fallback_core() -> tuple[FakeCore, str]:
    import sqlite3

    path = tempfile.mktemp(suffix=".db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE customers(id INTEGER PRIMARY KEY, name TEXT, active INTEGER);
        CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL);
        INSERT INTO customers(name, active) VALUES ('Alice', 1), ('Bob', 0);
        INSERT INTO orders(customer_id, total) VALUES (1, 100.0), (1, 50.0);
        """
    )
    con.commit()
    con.close()
    return FakeCore(path), FALLBACK_CONN


def _patch_llm_train(monkeypatch_fn=None) -> None:
    class _FakeLlm:
        def train(self, **kwargs):
            return {"ok": True, "name": kwargs.get("name", "default"), "engine": "python"}

    import ai_assistant.llm.service as ls
    ls.LlmService = _FakeLlm  # type: ignore[misc]

    import ai_assistant.llm.data_sources as ds

    ds.persist_pairs = lambda c, p, **kw: ("/tmp/verify.jsonl", len(p))  # type: ignore[assignment]


def _ok(msg: str) -> None:
    print(f"  OK  {msg}")


def _fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    raise SystemExit(1)


def check_ui_wiring() -> None:
    print("\n[1] UI wiring (static)")
    ai_ui = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text(encoding="utf-8")
    for needle in (
        "open_train_llm_dialog",
        "_aiqa_train_current",
        "_aiqa_train_chat",
        "_aiqa_train_model",
        "_aiqa_train_engine",
        "open_llm_trainer",
    ):
        if needle not in ai_ui:
            _fail(f"missing in ai_query_ui.py: {needle}")
    if 'engine="python"' in ai_ui:
        _fail("ai_query_ui.py still hard-codes engine=\"python\" for AI Query training")
    _ok("AI Query Train LLM session-config dialog wired")

    llm_panel = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text(encoding="utf-8")
    for needle in (
        "Rich DB training (from database)",
        "Preview mined queries",
        "do_preview_mined",
        "_training_service",
        "train_svc.train_llm",
    ):
        if needle not in llm_panel:
            _fail(f"missing in llm_panel.py: {needle}")
    validation = (ROOT / "ai_assistant/llm/validation.py").read_text(encoding="utf-8")
    for needle in ("validate_pair", "parse_sql", "_PROSE_MARKERS"):
        if needle not in validation:
            _fail(f"missing in validation.py: {needle}")
    _ok("Central NL->SQL validation gate present")

    eval_mod = (ROOT / "ai_assistant/llm/eval.py").read_text(encoding="utf-8")
    for needle in ("evaluate_model", "format_eval_summary", "execution_accuracy"):
        if needle not in eval_mod:
            _fail(f"missing in eval.py: {needle}")
    _ok("Training accuracy meters present")


def check_headless_cli(connection: str, core) -> None:
    print("\n[2] Headless service (mine-pairs, rag-status)")
    from ai_assistant.llm.training_service import LlmTrainingService

    svc = LlmTrainingService(core)
    r = svc.rag_status(connection)
    if not r.get("ok", True):
        _fail(f"rag-status: {r.get('error')}")
    _ok(f"rag-status for '{connection}': ok")

    mined = svc.mine_training_pairs({
        "connections": [connection],
        "train_sample_limit": 3,
        "train_max_tables": 10,
    })
    if not mined.get("ok"):
        _fail(f"mine-pairs: {mined.get('error')}")
    stats = mined.get("stats") or {}
    kept = stats.get("kept", 0)
    _ok(
        f"mine-pairs: kept={kept} "
        f"validated={stats.get('validated', 0)}/{stats.get('candidates', 0)} "
        f"db_type={mined.get('db_type', '')}"
    )
    if kept < 1:
        _fail("mine-pairs returned zero validated pairs")


def check_train_current_pair(connection: str, core) -> None:
    print("\n[3] Train on current Q->SQL (train_pairs)")
    from ai_assistant.llm.training_service import LlmTrainingService

    _patch_llm_train()
    svc = LlmTrainingService(core)
    pairs = [{
        "question": "How many customers are there?",
        "sql": "SELECT COUNT(*) FROM customers",
        "description": "verify runtime current turn",
    }]
    r = svc.train_pairs(
        pairs, names=["verify_runtime"], engine="python", connection=connection,
    )
    if not r.get("ok"):
        _fail(f"train_pairs: {r.get('error') or r.get('reason')}")
    if r.get("pairs", 0) < 1:
        _fail("train_pairs reported zero pairs")
    _ok(f"train_pairs: {r.get('pairs')} pair(s), source={r.get('source')}")


def check_train_from_chat(connection: str, core) -> None:
    print("\n[4] Train from chat (capture store path)")
    from ai_assistant.capture.pipeline import CapturePipeline, CaptureTurn
    from ai_assistant.llm.data_sources import collect_connection_pairs
    from ai_assistant.llm.training_service import LlmTrainingService
    from common import paths as app_paths

    _patch_llm_train()

    with tempfile.TemporaryDirectory() as tmp:
        store_root = Path(tmp) / "capture"
        pipe = CapturePipeline(store=type(
            "S",
            (),
            {
                "root": store_root,
                "append": lambda self, rec: None,
            },
        )())
        # Use real store
        from ai_assistant.capture.store import IsolatedCaptureStore

        store = IsolatedCaptureStore(store_root)
        pipe.store = store

        mgr = core
        pipe.record_turn(CaptureTurn(
            question="How many rows in customers?",
            prompt="schema context",
            raw_response="SELECT COUNT(*) FROM customers",
            parsed={"summary_sql": "SELECT COUNT(*) FROM customers", "sql": "SELECT COUNT(*) FROM customers"},
            context={"schema": {"table_schemas": {"customers": ["id", "name"]}}},
            connection_name=connection,
            db_manager=mgr,
            backend="verify",
            session_id="verify-session",
            is_followup=False,
        ))
        pipe.record_turn(CaptureTurn(
            question="Show only active ones",
            prompt="follow-up context",
            raw_response="SELECT * FROM customers WHERE active = true LIMIT 10",
            parsed={"summary_sql": "SELECT * FROM customers WHERE active = true LIMIT 10",
                    "sql": "SELECT * FROM customers WHERE active = true LIMIT 10"},
            context={"schema": {"table_schemas": {"customers": ["id", "name", "active"]}}},
            connection_name=connection,
            db_manager=mgr,
            backend="verify",
            session_id="verify-session",
            is_followup=True,
            previous_sql="SELECT COUNT(*) FROM customers",
        ))

        # Patch capture dir read via collect_connection_pairs on real store path
        import ai_assistant.llm.data_sources as ds

        orig_dir = app_paths.ai_capture_dir

        def _tmp_capture_dir():
            return store_root

        app_paths.ai_capture_dir = _tmp_capture_dir  # type: ignore[method-assign]
        try:
            captured = collect_connection_pairs(connection, use_rag=False, include_capture=True)
            svc = LlmTrainingService(core)
            r = svc.train_llm({
                "mode": "from_database",
                "connections": [connection],
                "train_llm": ["verify_chat"],
                "mine_db": False,
                "use_rag": False,
                "include_sample": False,
            })
        finally:
            app_paths.ai_capture_dir = orig_dir  # type: ignore[method-assign]

        if len(captured) < 1:
            _fail(f"collect_connection_pairs found {len(captured)} pairs from synthetic capture")
        if not r.get("ok"):
            _fail(f"train_llm from chat path: {r.get('error') or r.get('reason')}")
        _ok(
            f"train from chat path: pairs={r.get('pairs')} source={r.get('source')} "
            f"(captured={len(captured)} in store)"
        )


def check_rich_panel_train(connection: str, core) -> None:
    print("\n[5] Rich DB train (panel backend path)")
    from ai_assistant.llm.training_service import LlmTrainingService

    _patch_llm_train()
    svc = LlmTrainingService(core)
    preview = svc.mine_training_pairs({
        "connections": [connection],
        "train_sample_limit": 3,
        "train_max_tables": 8,
        "train_max_pairs": 50,
    })
    if not preview.get("ok"):
        _fail(f"preview mine: {preview.get('error')}")
    _ok(f"preview: {preview.get('stats', {}).get('kept', 0)} pairs")

    r = svc.train_llm({
        "mode": "from_database",
        "connections": [connection],
        "train_new_name": "verify_rich",
        "mine_db": True,
        "train_sample_limit": 3,
        "train_max_tables": 8,
        "use_rag": False,
        "index_rag": False,
    })
    if not r.get("ok"):
        _fail(f"rich train: {r.get('error') or r.get('reason')}")
    _ok(
        f"rich train: pairs={r.get('pairs')} source={r.get('source')} "
        f"reason={r.get('reason', '')[:80]}"
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Verify Train LLM runtime wiring")
    p.add_argument("--connection", default=CONNECTION_DEFAULT)
    args = p.parse_args()
    conn = args.connection

    print(f"Verify Train LLM wiring (connection={conn})")
    check_ui_wiring()
    core = None
    using_fallback = False
    try:
        core = __import__("ai_query.service", fromlist=["make_service"]).make_service()._core
        core.get_manager(conn)
        _ok(f"Using saved connection profile: {conn}")
    except Exception as exc:
        print(f"\n  NOTE  Live profile '{conn}' unavailable headlessly ({exc}).")
        core, conn = _make_fallback_core()
        using_fallback = True
        _ok(f"Using in-memory SQLite fallback as '{conn}'")

    check_headless_cli(conn, core)
    check_train_current_pair(conn, core)
    check_train_from_chat(conn, core)
    check_rich_panel_train(conn, core)
    if using_fallback:
        print("\n  NOTE  GUI restart + live PostgreSQL still recommended for manual UI check.")
    print("\nAll verification checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
