"""Concurrency fixes for shared read-modify-write state.

Verifies the lock-safe rewrites of: LLM retry backlog, AI state (PII/backend),
monitoring alert clearing, and AI session persistence.
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# ── LLM retry backlog (model_ledger) ──────────────────────────────────────────
def test_backlog_remove_is_lock_safe_and_intact():
    from ai_assistant.llm import model_ledger as ml

    items = [{"question": f"q{i}", "description": f"d{i}"} for i in range(30)]
    ml.save_backlog("bl_model", items)

    def remove(i):
        ml.remove_from_backlog("bl_model", f"q{i}")

    threads = [threading.Thread(target=remove, args=(i,)) for i in range(0, 30, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    remaining = ml.load_backlog("bl_model")
    qs = sorted(int(r["question"][1:]) for r in remaining)
    # Odd-numbered questions survive; file is valid JSONL throughout.
    assert qs == [i for i in range(30) if i % 2 == 1]


# ── AI state (PII toggle / backend selection) ─────────────────────────────────
def test_update_ai_state_no_lost_keys():
    from ai_query import service as ai_service

    def set_pii(v):
        ai_service._update_ai_state({"mask_pii": v})

    def set_backend(b):
        ai_service._update_ai_state({"active_backend": b})

    threads = [
        threading.Thread(target=set_pii, args=(True,)),
        threading.Thread(target=set_backend, args=("cursor",)),
    ]
    for _ in range(10):
        threads.append(threading.Thread(target=set_pii, args=(False,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = ai_service._read_ai_state()
    # Both keys persist (neither writer clobbered the other's key).
    assert "mask_pii" in state and state.get("active_backend") == "cursor"


# ── monitoring alerts: clear vs concurrent appends ────────────────────────────
def test_clear_alerts_does_not_lose_concurrent_appends():
    from monitoring.service import MonitorService

    svc = MonitorService(None)
    # Seed CRITICAL alerts that the clear will target.
    for i in range(20):
        svc.log_alert("CRITICAL", f"crit {i}", source="db", instance="i1")

    stop = threading.Event()
    appended = {"n": 0}
    lock = threading.Lock()

    def appender():
        # Append INFO alerts (not targeted by the clear) during the clear.
        while not stop.is_set():
            svc.log_alert("INFO", "keep me", source="db", instance="i2")
            with lock:
                appended["n"] += 1

    t = threading.Thread(target=appender)
    t.start()
    try:
        svc.clear_alerts(severity="CRITICAL")
    finally:
        stop.set()
        t.join()

    alerts = svc.list_alerts().get("alerts", [])
    info_kept = [a for a in alerts if a.get("severity") == "INFO"]
    crit_kept = [a for a in alerts if a.get("severity") == "CRITICAL"]
    # No CRITICAL remain; every INFO append is still present (none lost).
    assert not crit_kept
    assert len(info_kept) == appended["n"]


# ── AI session persistence wiring ─────────────────────────────────────────────
def test_session_persistence_uses_file_lock():
    src = (ROOT / "ai_query/session_manager.py").read_text()
    assert "from common.concurrency import file_lock" in src
    assert src.count("with file_lock(path):") >= 2


def test_race_helpers_used_across_hotspots():
    assert "file_lock" in (ROOT / "ai_assistant/llm/model_ledger.py").read_text()
    assert "_update_ai_state" in (ROOT / "ai_query/service.py").read_text()
    mon = (ROOT / "monitoring/service.py").read_text()
    assert "append_jsonl_locked" in mon and "atomic_write_text" in mon


def test_threshold_checker_supports_all_saved_operators(tmp_path):
    from monitoring.monitoring_utils import reset_all
    from monitoring.threshold_checker import ThresholdChecker

    cfg = tmp_path / "thresholds.ini"
    cfg.write_text(
        "\n".join([
            "[metric.os.cpu_utilization]",
            "operator = >=",
            "critical = 80",
            "window = 1",
            "enabled = true",
        ]),
        encoding="utf-8",
    )
    reset_all()
    checker = ThresholdChecker(config_path=cfg, reload_on_check=False)
    alert = checker.check("os", "cpu_utilization", 80)
    assert alert is not None
    assert alert.severity == "CRITICAL"


def test_capture_store_fsyncs_and_streams(monkeypatch, tmp_path):
    from ai_assistant.capture.record import CaptureRecord
    from ai_assistant.capture.store import IsolatedCaptureStore

    fsynced = {"n": 0}
    monkeypatch.setattr(os, "fsync", lambda _fd: fsynced.__setitem__("n", fsynced["n"] + 1))
    store = IsolatedCaptureStore(tmp_path)
    rec = CaptureRecord(
        project_id="p", connection_name="c", database="d",
        question="q", sql="SELECT 1", quality_accepted=True,
    )
    store.append(rec)
    assert fsynced["n"] == 1
    got = list(store.iter_records("p", accepted_only=True))
    assert len(got) == 1 and got[0].question == "q"


def test_agent_shared_state_has_locks():
    src = (ROOT / "ai_query/agent.py").read_text()
    assert "self._cache_lock = threading.RLock()" in src
    assert "self._session_bind_lock = threading.RLock()" in src
    assert "with self._cache_lock:" in src
    assert "with self._session_bind_lock:" in src
