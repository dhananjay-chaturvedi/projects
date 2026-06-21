"""Estimate and record prompt token usage for AI Query Assistant."""

from __future__ import annotations

import threading
from typing import Any, Callable

_lock = threading.Lock()
_last_record: dict[str, Any] | None = None
_capture_hooks: list[Callable[[dict[str, Any]], None]] = []


def estimate_tokens(text: str) -> int:
    """Return an estimated token count for *text* (tiktoken when available)."""
    if not text:
        return 0
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")
        return len(enc.encode(text))
    except Exception:
        return max(1, len(text) // 4)


def meter_enabled() -> bool:
    from ai_query import module_config as mc

    return mc.get_bool("ai.meter", "enabled", default=True)


def record_prompt(
    *,
    path: str,
    prompt: str,
    backend: str = "",
    tier: int = 1,
    **extra: Any,
) -> dict[str, Any]:
    """Record a prompt measurement and invoke test capture hooks."""
    tokens = estimate_tokens(prompt)
    rec: dict[str, Any] = {
        "path": path,
        "prompt_tokens_est": tokens,
        "prompt_chars": len(prompt),
        "backend": backend,
        "tier": tier,
        **extra,
    }
    with _lock:
        global _last_record
        _last_record = rec
    if meter_enabled():
        from common.config_loader import console_print

        console_print(
            f"[TokenMeter] {path} tier={tier} ~{tokens} tokens "
            f"({len(prompt)} chars) backend={backend or '?'}"
        )
    for hook in list(_capture_hooks):
        try:
            hook(rec)
        except Exception:
            pass
    return rec


def get_last_record() -> dict[str, Any] | None:
    with _lock:
        return dict(_last_record) if _last_record else None


def register_capture_hook(fn: Callable[[dict[str, Any]], None]) -> Callable:
    _capture_hooks.append(fn)
    return fn


def clear_capture_hooks() -> None:
    _capture_hooks.clear()
