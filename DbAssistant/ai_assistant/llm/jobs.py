"""Background LLM train/harvest jobs with SSE progress streaming.

Mirrors :mod:`ai_assistant.app_builder.jobs` for Web/TUI live progress parity.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Iterator

_JOB_TTL_SEC = 3600
_MAX_JOBS = 32


@dataclass
class LlmJob:
    """One in-flight or recently finished LLM train/harvest job."""

    id: str
    kind: str  # train | harvest
    body: dict
    status: str = "pending"  # pending | running | stopped | finished | error
    events: list[dict] = field(default_factory=list)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None
    error: str = ""
    created_at: float = field(default_factory=time.time)
    finished_at: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append_event(self, payload: dict) -> int:
        with self._lock:
            seq = len(self.events)
            self.events.append({"seq": seq, "ts": time.time(), **payload})
            return seq

    def events_since(self, cursor: int) -> list[dict]:
        with self._lock:
            return [e for e in self.events if e.get("seq", 0) >= cursor]


class LlmJobManager:
    """Manages background LLM training/harvest jobs for a single AIService."""

    def __init__(self, service: Any) -> None:
        self._service = service
        self._jobs: dict[str, LlmJob] = {}
        self._lock = threading.Lock()

    def start(self, body: dict) -> dict[str, Any]:
        self._cleanup_old()
        kind = str((body or {}).get("kind") or "train").strip().lower()
        if kind not in ("train", "harvest"):
            return {"ok": False, "error": f"Unknown job kind '{kind}'."}
        job_id = uuid.uuid4().hex[:12]
        payload = dict(body or {})
        payload.pop("kind", None)
        job = LlmJob(id=job_id, kind=kind, body=payload)
        with self._lock:
            if len(self._jobs) >= _MAX_JOBS:
                self._cleanup_old(force=True)
            self._jobs[job_id] = job
        thread = threading.Thread(
            target=self._run_job, args=(job,), daemon=True, name=f"llm-{kind}-{job_id}",
        )
        job.status = "running"
        thread.start()
        return {"ok": True, "job_id": job_id, "status": job.status, "kind": kind}

    def status(self, job_id: str) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        return {
            "ok": True,
            "job_id": job.id,
            "kind": job.kind,
            "status": job.status,
            "result": job.result,
            "error": job.error,
        }

    def events(self, job_id: str, cursor: int = 0) -> list[dict]:
        job = self._get(job_id)
        if job is None:
            return []
        return job.events_since(cursor)

    def iter_events_sse(
        self, job_id: str, cursor: int = 0, *, timeout: float = 30.0,
    ) -> Iterator[str]:
        """Yield SSE-formatted event strings until the job finishes."""
        del timeout  # reserved for future long-poll tuning
        job = self._get(job_id)
        if job is None:
            yield f"data: {json.dumps({'error': 'job not found'})}\n\n"
            return
        pos = cursor
        while True:
            batch = job.events_since(pos)
            for ev in batch:
                pos = ev.get("seq", pos) + 1
                yield f"data: {json.dumps(ev, default=str)}\n\n"
            if job.status in ("finished", "stopped", "error"):
                final = job.events_since(pos)
                for ev in final:
                    pos = ev.get("seq", pos) + 1
                    yield f"data: {json.dumps(ev, default=str)}\n\n"
                yield f"data: {json.dumps({'type': 'job_done', 'status': job.status, 'result': job.result})}\n\n"
                return
            time.sleep(0.25)

    def stop(self, job_id: str) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        job.cancel_event.set()
        if job.kind == "harvest":
            self._service.llm_harvest_stop(job.id)
        if job.status == "running":
            job.status = "stopped"
        job.append_event({"type": "stopped", "text": "Stop requested"})
        return {"ok": True, "job_id": job_id, "status": job.status}

    def _get(self, job_id: str) -> LlmJob | None:
        with self._lock:
            return self._jobs.get(job_id)

    def _run_job(self, job: LlmJob) -> None:
        def on_progress(ev: dict) -> None:
            job.append_event(ev)

        try:
            if job.kind == "harvest":
                body = dict(job.body)
                body.setdefault("harvest_id", job.id)
                result = self._service.llm_harvest(
                    body,
                    progress=on_progress,
                    should_stop=job.cancel_event.is_set,
                )
            else:
                result = self._service.llm_train_rich(job.body, progress=on_progress)
            job.result = result
            if result.get("ok"):
                job.status = "finished"
            elif job.cancel_event.is_set() or result.get("stopped"):
                job.status = "stopped"
            else:
                job.status = "error"
                job.error = str(result.get("error") or result.get("reason") or "failed")
        except Exception as exc:  # noqa: BLE001
            job.status = "error"
            job.error = str(exc)
            job.append_event({"type": "error", "text": str(exc)})
        finally:
            job.finished_at = time.time()
            job.append_event({
                "type": "job_finished",
                "status": job.status,
                "ok": bool((job.result or {}).get("ok")),
            })

    def _cleanup_old(self, *, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            expired = [
                jid for jid, j in self._jobs.items()
                if j.status in ("finished", "stopped", "error")
                and (now - (j.finished_at or j.created_at)) > _JOB_TTL_SEC
            ]
            for jid in expired:
                del self._jobs[jid]
            if force and len(self._jobs) >= _MAX_JOBS:
                oldest = sorted(self._jobs.items(), key=lambda x: x[1].created_at)
                for jid, _ in oldest[: len(self._jobs) - _MAX_JOBS + 1]:
                    if self._jobs[jid].status in ("finished", "stopped", "error"):
                        del self._jobs[jid]


_managers: dict[int, LlmJobManager] = {}


def get_llm_job_manager(service: Any) -> LlmJobManager:
    key = id(service)
    if key not in _managers:
        _managers[key] = LlmJobManager(service)
    return _managers[key]
