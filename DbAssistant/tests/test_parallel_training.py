"""Parallel / concurrent LLM training safety.

Covers per-connection shard staging + locked merge-on-commit, concurrency-safe
``LlmService.train`` (no clobbered artifacts when the same model trains twice at
once), the ``LlmTrainingService.train_from_connections`` orchestration, and the
CLI / API / UI wiring for "train one model from several connections".
"""

from __future__ import annotations

import json
import threading
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _pairs(prefix: str, n: int) -> list[dict]:
    return [{"question": f"{prefix} q{i}", "sql": f"SELECT {i} FROM t WHERE k={i}"}
            for i in range(n)]


# ── shard staging + commit ────────────────────────────────────────────────────
def test_stage_and_commit_merges_union_and_clears_shards():
    from ai_assistant.llm.service import LlmService

    svc = LlmService()
    svc.stage_shard("m1", "connA", _pairs("a", 5))
    svc.stage_shard("m1", "connB", _pairs("b", 5))
    assert sorted(svc.list_shards("m1")) == ["connA", "connB"]

    res = svc.commit_shards("m1", engine="python")
    assert res.get("ok"), res
    assert res.get("merged_pairs") == 10
    assert sorted(res.get("committed_shards")) == ["connA", "connB"]
    assert svc.list_shards("m1") == []  # shards cleared after a successful commit


def test_parallel_staging_is_lossless():
    from ai_assistant.llm.service import LlmService

    svc = LlmService()

    def stage(conn):
        svc.stage_shard("m2", conn, _pairs(conn, 8))

    threads = [threading.Thread(target=stage, args=(f"c{i}",)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    res = svc.commit_shards("m2", engine="python")
    assert res.get("ok"), res
    # 6 connections x 8 unique pairs each = 48 unique pairs.
    assert res.get("merged_pairs") == 48


# ── concurrency-safe train() ────────────────────────────────────────────────────
def test_concurrent_same_model_train_keeps_valid_artifacts():
    from ai_assistant.llm.service import LlmService

    svc = LlmService()
    results: list[dict] = []
    lock = threading.Lock()

    def do_train(tag):
        # Each writes its own dataset via stage->commit on the SAME model name.
        svc.stage_shard("shared", tag, _pairs(tag, 6))
        r = svc.commit_shards("shared", engine="python")
        with lock:
            results.append(r)

    threads = [threading.Thread(target=do_train, args=(f"t{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results and all(r.get("ok") for r in results)
    # meta.json + dataset.jsonl must be intact (never torn by interleaving).
    mdir = svc._model_dir("shared")  # noqa: SLF001
    meta = json.loads((mdir / "meta.json").read_text())
    assert meta.get("name") == "shared"
    for line in (mdir / "dataset.jsonl").read_text().splitlines():
        if line.strip():
            json.loads(line)  # raises if torn


# ── training-service orchestration ───────────────────────────────────────────
def test_train_from_connections_orchestration(monkeypatch):
    from ai_assistant.llm.training_service import LlmTrainingService

    svc = LlmTrainingService(core=None)

    def fake_mine(self, body):
        conn = (body.get("connections") or [""])[0]
        return {"ok": True, "pairs": _pairs(conn, 4), "stats": {}}

    svc.mine_training_pairs = types.MethodType(fake_mine, svc)
    svc._live_validate_pairs = types.MethodType(  # noqa: SLF001
        lambda self, pairs, connection="": (pairs, None), svc)

    out = svc.train_from_connections({
        "connections": ["connX", "connY"],
        "train_new_name": "multimodel",
        "train_engine": "python",
        "gen_workers": 2,
    })
    assert out.get("ok"), out
    assert out.get("source") == "multi_connection"
    assert sorted(out.get("connections")) == ["connX", "connY"]
    models = out.get("models") or []
    assert models and models[0].get("ok")
    assert models[0].get("merged_pairs") == 8


def test_train_from_connections_requires_connection_and_model():
    from ai_assistant.llm.training_service import LlmTrainingService

    svc = LlmTrainingService(core=None)
    assert svc.train_from_connections({"train_new_name": "x"}).get("ok") is False
    assert svc.train_from_connections({"connections": ["c"]}).get("ok") is False


# ── API ───────────────────────────────────────────────────────────────────────
def test_api_train_multi_route_validation(api_client):
    # Missing connections -> 4xx error (route is wired and reachable).
    r = api_client.post("/api/ai/llm/train-multi", json={"train_new_name": "m"})
    assert r.status_code >= 400


# ── parity wiring ────────────────────────────────────────────────────────────
def test_parallel_training_wired_across_surfaces():
    svc = (ROOT / "ai_assistant/llm/service.py").read_text()
    assert "def stage_shard" in svc and "def commit_shards" in svc
    assert "file_lock" in svc
    ts = (ROOT / "ai_assistant/llm/training_service.py").read_text()
    assert "def train_from_connections" in ts
    ai_svc = (ROOT / "ai_query/service.py").read_text()
    assert "def llm_train_multi" in ai_svc
    api = (ROOT / "ai_query/api.py").read_text()
    assert "/api/ai/llm/train-multi" in api
    cli = (ROOT / "ai_query/cli.py").read_text()
    assert "train-multi" in cli and "train_from_connections" not in cli  # cli calls service
    assert "llm_train_multi" in cli or "train-multi" in cli
    tk = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    # The dedicated "Train multi-connection" button was unified into the
    # auto-harvest flow: selecting several connections in the multi-select makes
    # advanced training span them all (real objects + per-dialect validation).
    assert "multi_conn_list" in tk and '"connections": harvest_conns' in tk
    tui = (ROOT / "common/ui/textual/screens/build_apps.py").read_text()
    assert "train_multi" in tui and "llm-multi-conns" in tui
    web = (ROOT / "common/ui/web/static/app.js").read_text()
    assert "train_multi" in web and "/api/ai/llm/train-multi" in web
