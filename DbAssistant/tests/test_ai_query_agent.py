"""AI query agent unit tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ai_query.agent import AIQueryAgent


def test_agent_constructs_without_probing():
    with patch("ai_query.backends.AIBackendRegistry") as reg_cls:
        reg = MagicMock()
        reg.get_default_name.return_value = ""
        reg.get.return_value = None
        reg_cls.return_value = reg
        agent = AIQueryAgent()
    assert agent.cli_available is False


def test_list_all_backends():
    with patch("ai_query.backends.AIBackendRegistry") as reg_cls:
        reg = MagicMock()
        reg.get_default_name.return_value = ""
        reg.list_all_names.return_value = ["claude", "cursor"]
        reg_cls.return_value = reg
        agent = AIQueryAgent()
    assert "claude" in agent.list_all_backends()


def test_cache_metadata_initialized():
    with patch("ai_query.backends.AIBackendRegistry") as reg_cls:
        reg_cls.return_value = MagicMock(get_default_name=lambda: "", get=lambda: None)
        agent = AIQueryAgent()
    assert isinstance(agent.schema_cache, dict)
    assert isinstance(agent.cache_metadata, dict)
