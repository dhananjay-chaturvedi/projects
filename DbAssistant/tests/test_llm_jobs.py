"""Tests for LLM training progress forwarding and background jobs."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from ai_assistant.llm.jobs import LlmJobManager, get_llm_job_manager
from ai_assistant.llm.training_service import LlmTrainingService


def test_model_epoch_progress_adapter():
    events: list[dict] = []
    cb = LlmTrainingService._model_epoch_progress(events.append, "local_db_numpy")
    cb({"epoch": 5, "loss": 0.2})
    cb({"status": "ignored"})
    assert events == [{
        "type": "training_epoch",
        "model": "local_db_numpy",
        "epoch": 5,
        "loss": 0.2,
    }]


def test_train_pairs_forwards_training_epoch(monkeypatch):
    class _FakeLlm:
        def train(self, **kwargs):
            progress = kwargs.get("progress")
            if progress:
                progress({"epoch": 1, "loss": 0.42})
                progress({"epoch": 10, "loss": 0.11})
            return {"ok": True, "name": kwargs.get("name")}

    monkeypatch.setattr(
        "ai_assistant.llm.service.LlmService",
        lambda: _FakeLlm(),
    )
    monkeypatch.setattr(
        "ai_assistant.llm.data_sources.persist_pairs",
        lambda *a, **k: ("/tmp/dataset.jsonl", 1),
    )

    events: list[dict] = []
    svc = LlmTrainingService(None)
    r = svc.train_pairs(
        [{"question": "count rows", "sql": "SELECT COUNT(*) FROM t"}],
        names=["demo"],
        include_sample=False,
        on_progress=events.append,
    )
    assert r["ok"]
    epoch_events = [e for e in events if e.get("type") == "training_epoch"]
    assert len(epoch_events) == 2
    assert epoch_events[0]["model"] == "demo"
    assert epoch_events[0]["epoch"] == 1


def test_llm_job_manager_train_emits_progress():
    svc = MagicMock()
    captured: list[dict] = []

    def _train_rich(body, *, progress=None):
        if progress:
            progress({"type": "training_capture", "status": "collecting"})
            progress({"type": "training_epoch", "model": "demo", "epoch": 1, "loss": 0.5})
        return {"ok": True, "pairs": 3, "models": [{"name": "demo", "ok": True}]}

    svc.llm_train_rich = _train_rich
    mgr = LlmJobManager(svc)
    started = mgr.start({"kind": "train", "train_new_name": "demo"})
    assert started["ok"] and started["job_id"]
    job_id = started["job_id"]
    for _ in range(50):
        st = mgr.status(job_id)
        if st["status"] in ("finished", "error", "stopped"):
            break
        time.sleep(0.05)
    events = mgr.events(job_id, 0)
    types = {e.get("type") for e in events}
    assert "training_capture" in types
    assert "training_epoch" in types
    assert st["status"] == "finished"
    assert st["result"]["ok"]


def test_llm_job_manager_harvest_stop():
    svc = MagicMock()
    stop_checks: list[bool] = []

    def _harvest(body, *, progress=None, should_stop=None):
        if progress:
            progress({"type": "harvest_offline_collected", "pairs": 2})
        for _ in range(20):
            if should_stop and should_stop():
                stop_checks.append(True)
                return {"ok": True, "pairs": 2, "stopped": True}
            time.sleep(0.01)
        return {"ok": True, "pairs": 5, "stopped": False}

    svc.llm_harvest = _harvest
    svc.llm_harvest_stop = MagicMock(return_value={"ok": True})
    mgr = LlmJobManager(svc)
    started = mgr.start({"kind": "harvest", "connection": "db1"})
    job_id = started["job_id"]
    time.sleep(0.05)
    mgr.stop(job_id)
    for _ in range(50):
        st = mgr.status(job_id)
        if st["status"] in ("finished", "stopped", "error"):
            break
        time.sleep(0.05)
    assert st["status"] in ("finished", "stopped")
    svc.llm_harvest_stop.assert_called_once_with(job_id)


def test_get_llm_job_manager_singleton():
    svc = MagicMock()
    assert get_llm_job_manager(svc) is get_llm_job_manager(svc)
