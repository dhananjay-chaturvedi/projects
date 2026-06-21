"""Model versioning surfaces (snapshot list / restore) across service+CLI+API+UI."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _service_with_temp_models(tmp_path: Path):
    from ai_assistant.llm.service import LlmService
    from ai_query.service import AIService

    llm = LlmService(models_dir=str(tmp_path / "models"))
    svc = AIService(core=None)
    svc._llm_service = llm  # inject isolated model store
    return svc, llm


def test_service_versions_and_restore_roundtrip(tmp_path: Path):
    svc, llm = _service_with_temp_models(tmp_path)
    mdir = llm._model_dir("m1")
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "meta.json").write_text(json.dumps({"v": 1}), encoding="utf-8")
    llm._snapshot_model(mdir, reason="pre-train")
    (mdir / "meta.json").write_text(json.dumps({"v": 2}), encoding="utf-8")

    listed = svc.llm_model_versions(name="m1")
    assert listed["ok"] and listed["count"] >= 1
    ver = listed["versions"][0]["version"]

    restored = svc.llm_model_restore(name="m1", version=ver)
    assert restored["ok"] and restored["restored"] == ver
    assert json.loads((mdir / "meta.json").read_text())["v"] == 1


def test_service_restore_requires_version(tmp_path: Path):
    svc, _ = _service_with_temp_models(tmp_path)
    r = svc.llm_model_restore(name="m1", version="")
    assert r["ok"] is False


def test_service_versions_empty_model(tmp_path: Path):
    svc, _ = _service_with_temp_models(tmp_path)
    r = svc.llm_model_versions(name="does-not-exist")
    assert r["ok"] is True and r["count"] == 0


# ── API ──────────────────────────────────────────────────────────────────────
def test_api_versions_and_restore_routes(api_client):
    r = api_client.get("/api/ai/llm/versions", params={"name": "default"})
    assert r.status_code == 200
    assert "versions" in r.json()
    # Restoring a missing version -> 4xx (route wired and reachable).
    r2 = api_client.post("/api/ai/llm/restore",
                         json={"name": "default", "version": "nope"})
    assert r2.status_code >= 400


# ── parity wiring ─────────────────────────────────────────────────────────────
def test_versioning_wired_across_surfaces():
    svc = (ROOT / "ai_query/service.py").read_text()
    assert "def llm_model_versions" in svc and "def llm_model_restore" in svc
    api = (ROOT / "ai_query/api.py").read_text()
    assert "/api/ai/llm/versions" in api and "/api/ai/llm/restore" in api
    cli = (ROOT / "ai_query/cli.py").read_text()
    assert '"versions"' in cli and '"restore"' in cli
    tk = (ROOT / "common/ui/tk/ai/llm_panel.py").read_text()
    assert "llm_model_versions" in tk and "llm_model_restore" in tk
