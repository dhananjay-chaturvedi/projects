"""Schema cache TTL expiry + reconnect invalidation."""

from __future__ import annotations

import types
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cache_expiry_logic():
    from ai_query.agent import AIQueryAgent

    now = datetime.now()
    # TTL disabled -> never expired even with an ancient timestamp.
    s0 = types.SimpleNamespace(
        cache_ttl_seconds=0,
        cache_metadata={"c": {"timestamp": now - timedelta(hours=5)}})
    assert AIQueryAgent._cache_is_expired(s0, "c") is False

    # TTL 10s, entry 60s old -> expired.
    s1 = types.SimpleNamespace(
        cache_ttl_seconds=10,
        cache_metadata={"c": {"timestamp": now - timedelta(seconds=60)}})
    assert AIQueryAgent._cache_is_expired(s1, "c") is True

    # TTL 600s, fresh entry -> not expired.
    s2 = types.SimpleNamespace(
        cache_ttl_seconds=600,
        cache_metadata={"c": {"timestamp": now}})
    assert AIQueryAgent._cache_is_expired(s2, "c") is False

    # No timestamp recorded -> not expired (treated as live).
    s3 = types.SimpleNamespace(cache_ttl_seconds=10, cache_metadata={"c": {}})
    assert AIQueryAgent._cache_is_expired(s3, "c") is False


class _RecordingAgent:
    def __init__(self):
        self.invalidated: list = []

    def invalidate_cache(self, name=None):
        self.invalidated.append(name)


def test_reconnect_invalidates_changed_connection():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    agent = _RecordingAgent()
    mgr_old = object()
    self_obj = types.SimpleNamespace(
        ai_agent=agent,
        active_connections={"db1": mgr_old},
        _cached_conn_ids={"db1": id(mgr_old)},
        ai_conn_combo=None,  # bail out after invalidation bookkeeping
    )
    # No change -> no invalidation.
    AIQueryUI.refresh_ai_connections(self_obj)
    assert agent.invalidated == []

    # Reconnect: same name, new manager object -> invalidate that connection.
    mgr_new = object()
    self_obj.active_connections = {"db1": mgr_new}
    AIQueryUI.refresh_ai_connections(self_obj)
    assert "db1" in agent.invalidated


def test_reconnect_invalidates_removed_connection():
    from common.ui.tk.ai.ai_query_ui import AIQueryUI

    agent = _RecordingAgent()
    mgr = object()
    self_obj = types.SimpleNamespace(
        ai_agent=agent,
        active_connections={},  # db1 went away
        _cached_conn_ids={"db1": id(mgr)},
        ai_conn_combo=None,
    )
    AIQueryUI.refresh_ai_connections(self_obj)
    assert "db1" in agent.invalidated


def test_ttl_config_documented():
    cfg = (ROOT / "ai_query/config.ini.example").read_text()
    assert "ttl_seconds" in cfg
