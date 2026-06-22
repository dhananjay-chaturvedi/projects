"""
Registry of pluggable LLM engines (Stage 0–3).

    python  — pure-Python MLP (zero deps, always available)
    numpy   — NumPy-vectorized MLP (Stage 1)
    pytorch — nano-GPT transformer (Stage 2, default)
    ollama  — Ollama HTTP serving + Modelfile customization (Stage 3)
"""

from __future__ import annotations

from typing import Optional

from ai_assistant.llm.engines.base import LlmEngine

_ENGINES: dict[str, type[LlmEngine]] = {}
_INSTANCES: dict[str, LlmEngine] = {}


def _register(cls: type[LlmEngine]) -> type[LlmEngine]:
    _ENGINES[cls.name] = cls
    return cls


def _ensure_registered() -> None:
    if _ENGINES:
        return
    from ai_assistant.llm.engines.python_engine import PythonEngine
    from ai_assistant.llm.engines.numpy_engine import NumpyEngine
    from ai_assistant.llm.engines.pytorch_engine import PytorchEngine
    from ai_assistant.llm.engines.ollama_engine import OllamaEngine

    for cls in (PythonEngine, NumpyEngine, PytorchEngine, OllamaEngine):
        _register(cls)


def get_engine(name: str) -> Optional[LlmEngine]:
    """Return a cached engine instance by name, or None if unknown."""
    _ensure_registered()
    key = (name or "").strip().lower()
    if key not in _ENGINES:
        return None
    if key not in _INSTANCES:
        _INSTANCES[key] = _ENGINES[key]()
    return _INSTANCES[key]


def list_engines() -> list[LlmEngine]:
    """Return all registered engine instances."""
    _ensure_registered()
    return [get_engine(n) for n in sorted(_ENGINES)]


def available_engines() -> list[dict]:
    """Return info dicts for all engines (includes availability)."""
    return [e.info() for e in list_engines() if e is not None]


def resolve_engine(
    preferred: str,
    fallback: str = "python",
) -> tuple[LlmEngine, str, bool]:
    """Pick *preferred* engine, falling back if unavailable.

    Returns (engine, engine_used_name, did_fallback).
    """
    _ensure_registered()
    pref = (preferred or "pytorch").strip().lower()
    fb = (fallback or "python").strip().lower()

    eng = get_engine(pref)
    if eng is not None:
        ok, _ = eng.is_available()
        if ok:
            return eng, pref, False

    eng_fb = get_engine(fb)
    if eng_fb is not None:
        ok, _ = eng_fb.is_available()
        if ok:
            return eng_fb, fb, pref != fb

    # Last resort: python (always available)
    py = get_engine("python")
    assert py is not None
    return py, "python", pref != "python"


__all__ = [
    "LlmEngine",
    "get_engine",
    "list_engines",
    "available_engines",
    "resolve_engine",
]
