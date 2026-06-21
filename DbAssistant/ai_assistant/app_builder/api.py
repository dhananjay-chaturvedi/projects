"""REST routes for AppBuilderAssistant."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
try:
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v1 fallback
    ConfigDict = None


class BuildTrainLlmRequest(BaseModel):
    if ConfigDict is not None:
        model_config = ConfigDict(extra="allow")
    else:  # pragma: no cover - pydantic v1 fallback
        class Config:
            extra = "allow"


def build_router(svc: Any = None) -> APIRouter:
    from ai_assistant.app_builder.jobs import get_job_manager
    from ai_assistant.app_builder.service import make_service

    service = svc or make_service()
    jobs = get_job_manager(service)
    router = APIRouter(prefix="/api/app-builder", tags=["app-builder"])

    def _call(action, *, status_code: int = 500):
        try:
            return action()
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=status_code, detail=str(exc)) from exc

    def _model_dump(req) -> dict:
        if hasattr(req, "model_dump"):
            return req.model_dump()
        return req.dict()

    @router.post("/init")
    def init_blueprint(body: dict):
        return _call(lambda: service.init_blueprint(body))

    @router.post("/scaffold")
    def scaffold(body: dict):
        return _call(lambda: service.scaffold_from_scratch(body.get("name", "myapp")))

    @router.post("/build")
    def build(body: dict):
        return _call(lambda: service.build(body))

    @router.post("/auto-build")
    def auto_build(body: dict):
        return _call(lambda: service.auto_build(body))

    @router.post("/package")
    def package(body: dict):
        """Approve + package a built app into a shippable bundle."""
        return _call(lambda: service.package_app(body))

    @router.post("/delete")
    def delete(body: dict):
        """Erase a build's workspace and all artifacts (leaves no trace)."""
        return _call(lambda: service.delete_app(body))

    @router.get("/services")
    def list_services():
        from ai_assistant.app_builder.engine import SERVICE_TEMPLATES
        return {"services": list(SERVICE_TEMPLATES)}

    # ── background jobs (real-time agentic builds) ───────────────────────────
    @router.post("/jobs")
    def start_job(body: dict):
        """Start an agentic build as a background job with SSE event stream."""
        return _call(lambda: jobs.start(body))

    @router.get("/jobs/{job_id}")
    def job_status(job_id: str):
        return _call(lambda: jobs.status(job_id))

    @router.get("/jobs/{job_id}/events")
    def job_events(job_id: str, cursor: int = 0):
        """SSE stream of build progress and agent transcript events."""
        def _gen():
            yield from jobs.iter_events_sse(job_id, cursor)
        return StreamingResponse(_gen(), media_type="text/event-stream")

    @router.get("/jobs/{job_id}/events/poll")
    def job_events_poll(job_id: str, cursor: int = 0):
        """Polling fallback: return events since *cursor*."""
        return _call(lambda: {"events": jobs.events(job_id, cursor),
                              **jobs.status(job_id)})

    @router.post("/jobs/{job_id}/stop")
    def job_stop(job_id: str):
        return _call(lambda: jobs.stop(job_id))

    @router.post("/jobs/{job_id}/take-control")
    def job_take_control(job_id: str):
        return _call(lambda: jobs.take_control(job_id))

    @router.post("/jobs/{job_id}/answer")
    def job_answer(job_id: str, body: dict):
        return _call(lambda: jobs.answer(job_id, str(body.get("value", "skip"))))

    @router.post("/jobs/{job_id}/message")
    def job_message(job_id: str, body: dict):
        return _call(lambda: jobs.send_message(
            job_id,
            str(body.get("text", "")),
            target=str(body.get("target", "auto")),
            interactive=bool(body.get("interactive", False)),
        ))

    # ── generated app runtime ─────────────────────────────────────────────────
    @router.post("/start-app")
    def start_app(body: dict):
        return _call(lambda: service.start_app(body))

    @router.post("/stop-app")
    def stop_app(body: dict):
        return _call(lambda: service.stop_app(body))

    @router.get("/app-status")
    def app_status(name: str):
        return _call(lambda: service.app_status({"name": name}))

    @router.get("/pii")
    def pii_setting():
        return _call(lambda: service.get_pii_masking())

    @router.get("/llm-models")
    def llm_models():
        return _call(lambda: service.llm_models())

    @router.post("/train-llm")
    def train_llm(body: dict):
        return _call(lambda: service.train_llm(body))

    @router.post("/build-train-llm")
    def build_train_llm(body: BuildTrainLlmRequest):
        """Train LLM(s) from a build's OWN data (generated schema/queries + DB
        insight), execution-validated — works even for from_scratch builds."""
        return _call(lambda: service.build_train_llm(_model_dump(body)))

    @router.get("/rag-status")
    def rag_status(connection: str = ""):
        return _call(lambda: service.rag_status(connection))

    @router.post("/index-rag")
    def index_rag(body: dict):
        return _call(lambda: service.index_rag(
            str(body.get("connection") or ""),
            rebuild=bool(body.get("rebuild", False)),
        ))

    @router.post("/mine-training-pairs")
    def mine_training_pairs(body: dict):
        """Preview the validated NL->SQL corpus mined from a DB connection."""
        return _call(lambda: service.mine_training_pairs(body))

    return router
