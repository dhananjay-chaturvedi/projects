"""Scheduled RAG re-index runner.

Runs ``reindex_stale`` automatically once per day inside a configurable
start-time + duration window, modelled on
:class:`ai_assistant.llm.scheduler.LlmHarvestScheduler`. Re-indexing is
*incremental* (only changed schema objects are re-embedded) so a scheduled run
is cheap and never disrupts a live index — stale/drifted connections are
refreshed, fresh ones are skipped.

The schedule is fully offline: it connects to the configured connections,
hashes the live schema, and re-embeds only what changed. Set
``connections`` to a comma list (or leave blank to cover every indexed
connection) and flip ``enabled`` to opt in.
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Callable

from ai_assistant.llm.scheduler import LlmHarvestScheduler


class RagReindexScheduler:
    """Background scheduler that re-indexes stale RAG connections once/day."""

    def __init__(
        self,
        run_reindex: Callable[..., dict],
        *,
        get_config: Callable[[], dict] | None = None,
    ) -> None:
        self._run_reindex = run_reindex
        self._get_config = get_config or (lambda: {})
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_run_date = ""
        self._last_result: dict[str, Any] = {}
        self._running_now = False

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="rag-reindex-scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict[str, Any]:
        cfg = self._get_config()
        start_t, duration = LlmHarvestScheduler._window_spec(cfg)
        return {
            "enabled": bool(cfg.get("enabled")),
            "start_time": start_t,
            "duration_hours": duration,
            "window_end": LlmHarvestScheduler._window_end_str(start_t, duration),
            "running": bool(self._thread and self._thread.is_alive()),
            "reindex_in_progress": self._running_now,
            "connections": list(cfg.get("connections") or []),
            "force": bool(cfg.get("force")),
            "last_run_date": self._last_run_date,
            "next_run": LlmHarvestScheduler._next_run_str(start_t),
            "last_result": dict(self._last_result),
        }

    # ── internals ────────────────────────────────────────────────────────────
    def _loop(self) -> None:
        while not self._stop.is_set():
            cfg = self._get_config()
            if cfg.get("enabled") and LlmHarvestScheduler._in_window(cfg):
                today = datetime.now().strftime("%Y-%m-%d")
                if self._last_run_date != today:
                    self._run_once(cfg, today)
            self._stop.wait(30)

    def _run_once(self, cfg: dict, today: str) -> None:
        connections = list(cfg.get("connections") or []) or None
        force = bool(cfg.get("force"))
        self._running_now = True
        try:
            res = self._run_reindex(connections, force=force)
            self._last_result = {
                "date": today,
                "ok": bool((res or {}).get("ok")),
                "reindexed": (res or {}).get("reindexed"),
                "error": (res or {}).get("error"),
            }
        except Exception as exc:  # noqa: BLE001
            self._last_result = {"date": today, "ok": False, "error": str(exc)}
        finally:
            self._running_now = False
            self._last_run_date = today


_schedulers: dict[int, RagReindexScheduler] = {}


def get_reindex_scheduler(service: Any) -> RagReindexScheduler:
    """Return the per-service singleton RAG re-index scheduler."""
    key = id(service)

    def _cfg() -> dict:
        try:
            from ai_query import module_config as mc

            raw_conns = mc.get(
                "ai.rag.reindex.schedule", "connections", default="") or ""
            conns = [c.strip() for c in raw_conns.split(",") if c.strip()]
            return {
                "enabled": mc.get_bool(
                    "ai.rag.reindex.schedule", "enabled", default=False),
                "start_time": mc.get(
                    "ai.rag.reindex.schedule", "start_time", default="02:00"),
                "duration_hours": mc.get(
                    "ai.rag.reindex.schedule", "duration_hours", default=""),
                "window_end": mc.get(
                    "ai.rag.reindex.schedule", "window_end", default=""),
                "connections": conns,
                "force": mc.get_bool(
                    "ai.rag.reindex.schedule", "force", default=False),
            }
        except Exception:
            return {"enabled": False, "connections": []}

    if key not in _schedulers:
        _schedulers[key] = RagReindexScheduler(
            lambda connections, force=False: service.reindex_stale(
                connections, force=force),
            get_config=_cfg,
        )
    return _schedulers[key]
