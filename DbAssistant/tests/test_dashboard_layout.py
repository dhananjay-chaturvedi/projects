"""Tests for dashboard grid layout persistence."""

from __future__ import annotations

from common.dashboard.layout_store import (
    DEFAULT_LAYOUT,
    find_position,
    load_layout,
    merge_visible_into_layout,
    move_panel,
    reset_layout,
)


def test_move_panel_swap():
    layout = [row[:] for row in DEFAULT_LAYOUT]
    layout = move_panel(layout, "monitor", 2, 1)  # monitor (0,1) <-> objects (2,1)
    assert find_position(layout, "monitor") == (2, 1)
    assert find_position(layout, "objects") == (0, 1)


def test_move_panel_to_empty():
    layout = [row[:] for row in DEFAULT_LAYOUT]
    layout[2][0] = None
    layout = move_panel(layout, "connections", 2, 0)
    assert find_position(layout, "connections") == (2, 0)
    assert layout[0][0] is None


def test_merge_visible_adds_missing():
    layout = [row[:] for row in DEFAULT_LAYOUT]
    merged = merge_visible_into_layout(layout, {"monitor", "connections", "ai"})
    placed = {pid for row in merged for pid in row if pid}
    assert {"monitor", "connections", "ai"}.issubset(placed)


def _isolate_layout(monkeypatch, tmp_path):
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    from common import paths as _paths

    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()


def test_reset_layout_returns_default(tmp_path, monkeypatch):
    _isolate_layout(monkeypatch, tmp_path)
    layout = reset_layout({"monitor", "connections"})
    assert layout[0] == ["connections", "monitor"]


def test_load_layout_without_file(tmp_path, monkeypatch):
    _isolate_layout(monkeypatch, tmp_path)
    layout = load_layout({"sql_editor", "objects"})
    assert find_position(layout, "sql_editor") is not None
    assert find_position(layout, "objects") is not None
