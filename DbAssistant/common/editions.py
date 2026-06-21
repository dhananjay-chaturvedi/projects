"""Build-time edition definitions."""

from __future__ import annotations

STANDARD_EXCLUDES = {
    "ai_assistant/app_builder",
    "ai_assistant/llm",
    "ai_assistant/rag",
    "common/ui/web/static/app_builder_ui.js",
    "common/ui/tk/ai/build_apps_dialogs.py",
}

ADVANCED_EXCLUDES: set[str] = set()


def advanced_modules_installed() -> bool:
    """True when App Builder / LLM / RAG packages are present in this build."""
    from pathlib import Path as _Path

    root = _Path(__file__).resolve().parents[1]
    return (root / "ai_assistant" / "app_builder").is_dir()


def excludes_for(edition: str) -> set[str]:
    edition = (edition or "advanced").strip().lower()
    if edition == "standard":
        return set(STANDARD_EXCLUDES)
    if edition == "advanced":
        return set(ADVANCED_EXCLUDES)
    raise ValueError("edition must be 'standard' or 'advanced'")
