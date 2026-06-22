"""Persisted 2-column dashboard grid layout."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Optional

from common import paths as _paths


def _layout_file() -> Path:
    return _paths.dashboard_layout_path()


_GRID_COLS = 2

DEFAULT_LAYOUT: list[list[Optional[str]]] = [
    ["connections", "monitor"],
    ["ai", "schema"],
    ["sql_editor", "objects"],
]

ALL_PANEL_IDS = frozenset(
    pid for row in DEFAULT_LAYOUT for pid in row if pid is not None
)


def load_layout(visible_ids: set[str] | None = None) -> list[list[Optional[str]]]:
    """Load grid from disk or return default, merged with *visible_ids*."""
    layout = deepcopy(DEFAULT_LAYOUT)
    lf = _layout_file()
    if lf.exists():
        try:
            data = json.loads(lf.read_text())
            rows = data.get("rows")
            if isinstance(rows, list) and rows:
                parsed = _normalize_rows(rows)
                if parsed:
                    layout = parsed
        except Exception:
            pass

    if visible_ids:
        layout = merge_visible_into_layout(layout, visible_ids)
    return layout


def save_layout(layout: list[list[Optional[str]]]) -> None:
    lf = _layout_file()
    lf.parent.mkdir(parents=True, exist_ok=True)
    lf.write_text(
        json.dumps({"rows": layout}, indent=2),
        encoding="utf-8",
    )


def reset_layout(visible_ids: set[str] | None = None) -> list[list[Optional[str]]]:
    layout = deepcopy(DEFAULT_LAYOUT)
    if visible_ids:
        layout = merge_visible_into_layout(layout, visible_ids)
    save_layout(layout)
    return layout


def find_position(
    layout: list[list[Optional[str]]], panel_id: str
) -> tuple[int, int] | None:
    for r, row in enumerate(layout):
        for c, pid in enumerate(row):
            if pid == panel_id:
                return r, c
    return None


def move_panel(
    layout: list[list[Optional[str]]],
    panel_id: str,
    to_row: int,
    to_col: int,
) -> list[list[Optional[str]]]:
    """Move *panel_id* to (to_row, to_col), swapping with occupant if any."""
    grid = deepcopy(layout)
    if not grid:
        return grid
    if to_row < 0 or to_col < 0 or to_col >= _GRID_COLS:
        return grid
    while to_row >= len(grid):
        grid.append([None, None])

    from_pos = find_position(grid, panel_id)
    if from_pos is None:
        return grid

    fr, fc = from_pos
    if fr == to_row and fc == to_col:
        return grid

    displaced = grid[to_row][to_col]
    grid[to_row][to_col] = panel_id
    grid[fr][fc] = displaced
    return grid


def merge_visible_into_layout(
    layout: list[list[Optional[str]]],
    visible_ids: set[str],
) -> list[list[Optional[str]]]:
    """Ensure every visible panel id appears exactly once in the grid."""
    grid = deepcopy(layout)
    placed: set[str] = set()
    for row in grid:
        for i, pid in enumerate(row):
            if pid and pid not in visible_ids:
                row[i] = None
            elif pid:
                if pid in placed:
                    row[i] = None
                else:
                    placed.add(pid)

    for pid in sorted(visible_ids):
        if pid in placed:
            continue
        slot = _first_empty_slot(grid)
        if slot:
            r, c = slot
            grid[r][c] = pid
        else:
            grid.append([pid, None])
        placed.add(pid)

    return _trim_trailing_empty_rows(grid)


def _normalize_rows(rows: list) -> list[list[Optional[str]]]:
    out: list[list[Optional[str]]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        cells: list[Optional[str]] = [None, None]
        for i in range(min(_GRID_COLS, len(row))):
            val = row[i]
            if val is None or val == "":
                cells[i] = None
            elif isinstance(val, str) and val in ALL_PANEL_IDS:
                cells[i] = val
        out.append(cells)
    return out if out else deepcopy(DEFAULT_LAYOUT)


def _first_empty_slot(grid: list[list[Optional[str]]]) -> tuple[int, int] | None:
    for r, row in enumerate(grid):
        for c, pid in enumerate(row):
            if pid is None:
                return r, c
    return None


def _trim_trailing_empty_rows(
    grid: list[list[Optional[str]]],
) -> list[list[Optional[str]]]:
    """Drop trailing empty rows; keep a compact grid for partial installs."""
    while len(grid) > 1 and all(cell is None for cell in grid[-1]):
        grid.pop()
    if not grid:
        return [[None, None]]
    return grid
