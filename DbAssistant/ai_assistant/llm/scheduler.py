"""Scheduled LLM harvest/training runner.

Runs an automatic harvest+train once per day, starting at a configurable
time of day and stopping gracefully after a configurable number of hours
(the duration cap). Stops happen only at safe harvest checkpoints, so a model
write is never interrupted. The default schedule is *advanced incremental,
offline* (multi-dialect template training, no backend AI) so a nightly run is
self-contained and never blocks on an external AI backend; set
``training_depth = online`` to also use the backend AI agent.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable


class LlmHarvestScheduler:
    """Background scheduler that runs harvest once/day in a start+duration window."""

    def __init__(
        self,
        run_harvest: Callable[..., dict],
        *,
        get_config: Callable[[], dict] | None = None,
    ) -> None:
        self._run_harvest = run_harvest
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
            target=self._loop, daemon=True, name="llm-harvest-scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict[str, Any]:
        cfg = self._get_config()
        start_t, duration = self._window_spec(cfg)
        return {
            "enabled": bool(cfg.get("enabled")),
            "start_time": start_t,
            "duration_hours": duration,
            "window_end": self._window_end_str(start_t, duration),
            "running": bool(self._thread and self._thread.is_alive()),
            "harvest_in_progress": self._running_now,
            "last_run_date": self._last_run_date,
            "next_run": self._next_run_str(start_t),
            "last_result": dict(self._last_result),
        }

    # ── internals ────────────────────────────────────────────────────────────
    def _loop(self) -> None:
        while not self._stop.is_set():
            cfg = self._get_config()
            if cfg.get("enabled") and self._in_window(cfg):
                today = datetime.now().strftime("%Y-%m-%d")
                if self._last_run_date != today:
                    self._run_once(cfg, today)
            self._stop.wait(30)

    def _run_once(self, cfg: dict, today: str) -> None:
        body = dict(cfg.get("body") or {})
        if not (body.get("connection") or body.get("connections")):
            return
        start_t, duration = self._window_spec(cfg)
        deadline = self._window_end_dt(start_t, duration)

        def _should_stop() -> bool:
            # Graceful duration cap: ask harvest to wrap up once the window
            # closes (or the scheduler itself is stopping).
            return self._stop.is_set() or datetime.now() >= deadline

        self._running_now = True
        try:
            res = self._run_harvest(body, should_stop=_should_stop)
            self._last_result = {
                "date": today,
                "ok": bool((res or {}).get("ok")),
                "pairs": (res or {}).get("pairs"),
                "stopped": (res or {}).get("stopped"),
                "training_depth": (res or {}).get("training_depth"),
                "error": (res or {}).get("error"),
            }
        except Exception as exc:  # noqa: BLE001
            self._last_result = {"date": today, "ok": False, "error": str(exc)}
        finally:
            self._running_now = False
            self._last_run_date = today

    @staticmethod
    def _window_spec(cfg: dict) -> tuple[str, float]:
        """Return (start_time 'HH:MM', duration_hours).

        Back-compat: if a legacy ``window_end`` is configured and no explicit
        ``duration_hours``, derive the duration from start/end.
        """
        start_t = str(cfg.get("start_time") or cfg.get("window_start") or "01:00").strip()
        dur = cfg.get("duration_hours")
        if dur in (None, ""):
            end = str(cfg.get("window_end") or "").strip()
            if end:
                try:
                    sh, sm = (int(x) for x in start_t.split(":"))
                    eh, em = (int(x) for x in end.split(":"))
                    minutes = (eh * 60 + em) - (sh * 60 + sm)
                    if minutes <= 0:
                        minutes += 24 * 60
                    return start_t, round(minutes / 60.0, 3)
                except Exception:
                    pass
            return start_t, 4.0
        try:
            return start_t, max(0.1, float(dur))
        except Exception:
            return start_t, 4.0

    @classmethod
    def _window_end_dt(cls, start_t: str, duration_hours: float) -> datetime:
        now = datetime.now()
        try:
            sh, sm = (int(x) for x in start_t.split(":"))
        except Exception:
            sh, sm = 1, 0
        start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        # If we're already past the start today, the window anchored to today.
        if now < start_dt and now.strftime("%H:%M") <= start_t:
            start_dt -= timedelta(days=1)
        return start_dt + timedelta(hours=duration_hours)

    @classmethod
    def _window_end_str(cls, start_t: str, duration_hours: float) -> str:
        try:
            sh, sm = (int(x) for x in start_t.split(":"))
        except Exception:
            return ""
        end = (datetime(2000, 1, 1, sh, sm) + timedelta(hours=duration_hours))
        return end.strftime("%H:%M")

    @classmethod
    def _in_window(cls, cfg: dict) -> bool:
        start_t, duration = cls._window_spec(cfg)
        end_t = cls._window_end_str(start_t, duration)
        now = datetime.now().strftime("%H:%M")
        if start_t <= end_t:
            return start_t <= now <= end_t
        # Window wraps past midnight.
        return now >= start_t or now <= end_t

    @staticmethod
    def _next_run_str(start_t: str) -> str:
        now = datetime.now()
        try:
            sh, sm = (int(x) for x in start_t.split(":"))
        except Exception:
            return ""
        nxt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        if nxt <= now:
            nxt += timedelta(days=1)
        return nxt.strftime("%Y-%m-%d %H:%M")


_schedulers: dict[int, LlmHarvestScheduler] = {}


def get_harvest_scheduler(service: Any) -> LlmHarvestScheduler:
    key = id(service)

    def _cfg() -> dict:
        try:
            from ai_query import module_config as mc

            raw_conns = mc.get("ai.llm.harvest.schedule", "connections", default="") or ""
            conns = [c.strip() for c in raw_conns.split(",") if c.strip()]
            return {
                "enabled": mc.get_bool("ai.llm.harvest.schedule", "enabled", default=False),
                "start_time": mc.get(
                    "ai.llm.harvest.schedule", "start_time",
                    default=mc.get("ai.llm.harvest.schedule", "window_start", default="01:00")),
                "duration_hours": mc.get(
                    "ai.llm.harvest.schedule", "duration_hours", default=""),
                "window_end": mc.get("ai.llm.harvest.schedule", "window_end", default=""),
                "body": {
                    "connection": mc.get("ai.llm.harvest.schedule", "connection", default=""),
                    "connections": conns or None,
                    "train_mode": mc.get(
                        "ai.llm.harvest.schedule", "train_mode", default="incremental"),
                    # Default to offline (template) depth so a nightly run never
                    # blocks on an external AI backend.
                    "training_depth": mc.get(
                        "ai.llm.harvest.schedule", "training_depth", default="offline"),
                    "advanced_training": mc.get_bool(
                        "ai.llm.harvest.schedule", "advanced_training", default=True),
                    "multi_dialect": mc.get_bool(
                        "ai.llm.harvest.schedule", "multi_dialect", default=True),
                    "train_new_name": mc.get(
                        "ai.llm.harvest.schedule", "train_new_name", default="default"),
                },
            }
        except Exception:
            return {"enabled": False, "body": {}}

    if key not in _schedulers:
        _schedulers[key] = LlmHarvestScheduler(
            lambda body, should_stop=None: service.llm_harvest(
                body, should_stop=should_stop),
            get_config=_cfg,
        )
    return _schedulers[key]
