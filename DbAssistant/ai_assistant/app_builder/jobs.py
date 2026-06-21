"""Server-side build job manager for Web/TUI real-time App Builder parity.

Runs agentic builds in background threads, streams progress/agent events via
an in-memory queue, and exposes control hooks (stop, take-control, message,
answer pending decisions) over HTTP.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import Any, Iterator, Optional

def _cfg_int(key: str, default: int) -> int:
    try:
        from ai_query import module_config as mc
        return mc.get_int("ai.app_builder", key, default=default)
    except Exception:
        return default


_JOB_TTL_SEC = _cfg_int("job_ttl_seconds", 3600)
_MAX_JOBS = _cfg_int("max_jobs", 32)


@dataclass
class BuildJob:
    """One in-flight or recently finished agentic build."""

    id: str
    body: dict
    status: str = "pending"  # pending | running | stopped | finished | error
    events: list[dict] = field(default_factory=list)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    answer_queue: Queue = field(default_factory=Queue)
    pending_decision: Optional[dict] = None
    result: Optional[dict] = None
    error: str = ""
    decider: Any = None
    coordinator: Any = None
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


class BuildJobManager:
    """Manages background agentic build jobs for a single service instance."""

    def __init__(self, service: Any) -> None:
        self._service = service
        self._jobs: dict[str, BuildJob] = {}
        self._lock = threading.Lock()

    def start(self, body: dict) -> dict[str, Any]:
        self._cleanup_old()
        job_id = uuid.uuid4().hex[:12]
        job = BuildJob(id=job_id, body=dict(body))
        with self._lock:
            if len(self._jobs) >= _MAX_JOBS:
                self._cleanup_old(force=True)
            self._jobs[job_id] = job
        thread = threading.Thread(
            target=self._run_job, args=(job,), daemon=True, name=f"build-{job_id}")
        job.status = "running"
        thread.start()
        return {"ok": True, "job_id": job_id, "status": job.status}

    def status(self, job_id: str) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        return {
            "ok": True,
            "job_id": job.id,
            "status": job.status,
            "result": job.result,
            "error": job.error,
            "pending_decision": job.pending_decision,
            "workspace": (job.result or {}).get("workspace", ""),
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
                # Drain any remaining events then send terminal marker.
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
        # Unblock any pending decision wait.
        try:
            job.answer_queue.put_nowait("skip")
        except Exception:
            pass
        if job.status == "running":
            job.status = "stopped"
        job.append_event({"type": "stopped", "text": "Build stop requested"})
        return {"ok": True, "job_id": job_id, "status": job.status}

    def answer(self, job_id: str, value: str) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        job.answer_queue.put(value)
        job.pending_decision = None
        job.append_event({"type": "decision_answered", "value": value})
        return {"ok": True}

    def take_control(self, job_id: str) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        decider = job.decider or getattr(self._service, "last_decider", None)
        if decider is None:
            return {"ok": False, "error": "no active decider for this job"}
        decider.take_control()
        job.append_event({"type": "take_control", "text": "Switched to interactive"})
        return {"ok": True}

    def send_message(
        self,
        job_id: str,
        text: str,
        *,
        target: str = "auto",
        interactive: bool = False,
    ) -> dict[str, Any]:
        job = self._get(job_id)
        if job is None:
            return {"ok": False, "error": "job not found"}
        coord = job.coordinator or getattr(self._service, "_job_coordinators", {}).get(job_id)
        if coord is None:
            coord = getattr(self._service, "last_coordinator", None)
        if coord is None:
            return {"ok": False, "error": "no live coordinator — build not agentic"}
        text = (text or "").strip()
        if not text:
            return {"ok": False, "error": "empty message"}
        job.append_event({
            "type": "user_message",
            "target": target,
            "text": text,
        })
        try:
            if target.startswith("auto") or target == "auto (B→A)":
                if interactive or job.status != "running":
                    reply = coord.route_user_request(text, interactive=interactive)
                else:
                    reply = coord.queue_user_message(text)
            elif target == "builder":
                sess = getattr(coord, "builder", None)
                reply = sess.send(text) if sess else ""
            elif target == "answerer":
                sess = getattr(coord, "answerer", None)
                payload = text
                if hasattr(coord, "status_preface"):
                    preface = coord.status_preface()
                    if preface:
                        payload = f"{preface}\n\nUSER: {text}"
                reply = sess.send(payload) if sess else ""
            elif target == "validator":
                sess = getattr(coord, "validator", None)
                payload = text
                if hasattr(coord, "status_preface"):
                    preface = coord.status_preface()
                    if preface:
                        payload = f"{preface}\n\nUSER: {text}"
                reply = sess.send(payload) if sess else ""
            else:
                return {"ok": False, "error": f"unknown target: {target}"}
            job.append_event({"type": "message_reply", "target": target,
                              "text": str(reply or "")})
            return {"ok": True, "reply": str(reply or "")}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def _get(self, job_id: str) -> Optional[BuildJob]:
        with self._lock:
            return self._jobs.get(job_id)

    def _run_job(self, job: BuildJob) -> None:
        def on_progress(payload: Any) -> None:
            if isinstance(payload, dict):
                ptype = payload.get("type", "")
                if ptype.startswith("training_"):
                    job.append_event({"type": ptype, "payload": payload})
                elif payload.get("agent_event"):
                    job.append_event({"type": "agent_event",
                                      "payload": payload["agent_event"]})
                else:
                    job.append_event({"type": "round", "payload": payload})
            else:
                job.append_event({"type": "progress", "payload": payload})

        def ask(decision: Any) -> Any:
            d = {
                "id": getattr(decision, "id", ""),
                "question": getattr(decision, "question", ""),
                "detail": getattr(decision, "detail", ""),
                "options": list(getattr(decision, "options", []) or []),
                "allow_multiple": bool(getattr(decision, "allow_multiple", False)),
            }
            job.pending_decision = d
            job.append_event({"type": "decision", "decision": d})
            if job.cancel_event.is_set():
                return "skip"
            try:
                answer = job.answer_queue.get(timeout=3600)
            except Empty:
                return "skip"
            return answer

        try:
            body = dict(job.body)
            body["_internal_job_id"] = job.id
            result = self._service.run_agentic_build(
                body,
                on_progress=on_progress,
                ask=ask,
                cancel_event=job.cancel_event,
            )
            job.result = result
            if job.cancel_event.is_set() or result.get("aborted"):
                job.status = "stopped"
            elif result.get("ok"):
                job.status = "finished"
            else:
                job.status = "error"
                job.error = result.get("error") or result.get("stop_reason") or ""
        except Exception as exc:  # noqa: BLE001
            job.status = "error"
            job.error = str(exc)
            job.append_event({"type": "error", "text": str(exc)})
        finally:
            job.finished_at = time.time()
            job.pending_decision = None
            job.decider = getattr(self._service, "last_decider", None)
            job.coordinator = getattr(self._service, "_job_coordinators", {}).get(job.id)
            job.append_event({"type": "job_finished", "status": job.status})

    def attach_decider(self, job_id: str, decider: Any) -> None:
        job = self._get(job_id)
        if job is not None:
            job.decider = decider

    def _cleanup_old(self, *, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            stale = [
                jid for jid, j in self._jobs.items()
                if j.status in ("finished", "stopped", "error")
                and (now - (j.finished_at or j.created_at)) > _JOB_TTL_SEC
            ]
            for jid in stale:
                del self._jobs[jid]
            if force and len(self._jobs) >= _MAX_JOBS:
                oldest = sorted(
                    self._jobs.items(),
                    key=lambda x: x[1].created_at,
                )
                for jid, _ in oldest[: len(self._jobs) - _MAX_JOBS + 1]:
                    if self._jobs[jid].status in ("finished", "stopped", "error"):
                        del self._jobs[jid]


# Module-level singleton keyed by service id()
_managers: dict[int, BuildJobManager] = {}


def get_job_manager(service: Any) -> BuildJobManager:
    key = id(service)
    if key not in _managers:
        _managers[key] = BuildJobManager(service)
    return _managers[key]
