"""Per-build PII masking helper for App Builder AI calls."""

from __future__ import annotations


def mask_if_enabled(text: str, enabled: bool) -> str:
    """Mask PII in *text* when *enabled* is True."""
    if not enabled or not text:
        return text
    try:
        from ai_query.pii_masker import mask_pii

        return mask_pii(text).text
    except Exception:
        return text
