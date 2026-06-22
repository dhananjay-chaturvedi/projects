"""Map unified SSL connection parameters for DatabaseManager."""

from __future__ import annotations

SSL_PARAM_KEYS = ("ssl_mode", "ssl_ca", "ssl_cert", "ssl_key", "wallet_location")


def ssl_connect_kwargs(source: dict) -> dict:
    """Extract optional SSL/TLS fields from a connection profile or kwargs dict."""
    out: dict = {}
    for key in SSL_PARAM_KEYS:
        value = source.get(key)
        if value not in (None, ""):
            out[key] = value
    mode = source.get("ssl_mode")
    if mode and str(mode).strip().lower() == "disable":
        out["ssl_mode"] = "disable"
    return out
