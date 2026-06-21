from __future__ import annotations

import builtins
from pathlib import Path


def test_standard_edition_excludes_advanced_modules(tmp_path):
    from scripts.build_edition import build_edition

    result = build_edition(Path.cwd(), tmp_path / "standard", "standard", dry_run=True)
    skipped = "\n".join(result["skipped"])
    assert "ai_assistant/app_builder" in skipped
    assert "ai_assistant/llm" in skipped
    assert "ai_assistant/rag" in skipped
    assert "common/ui/web/static/app_builder_ui.js" in skipped


def test_ai_service_gracefully_handles_missing_llm_and_rag(monkeypatch):
    from ai_query.service import AIService

    real_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("ai_assistant.llm") or name.startswith("ai_assistant.rag"):
            raise ImportError(name)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    svc = AIService(core=None)
    assert svc.llm_status("default")["ok"] is False
    assert "not available" in svc.llm_status("default")["error"]
    assert svc.rag_status("conn")["ok"] is False
    assert "not available" in svc.rag_status("conn")["error"]


def test_ai_router_mounts_when_llm_jobs_package_missing(monkeypatch):
    from ai_query.api import build_router

    real_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("ai_assistant.llm.jobs"):
            raise ImportError(name)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    class _Svc:
        pass

    router = build_router(_Svc())
    paths = {route.path for route in router.routes}
    assert "/api/ai/llm/jobs" in paths
