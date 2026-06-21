"""Shared helpers for exporting query result sets to files."""

from __future__ import annotations

import csv
import json
from typing import Any


def cell_to_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (bytearray, bytes)):
        if not val:
            return ""
        for encoding in ("utf-8", "windows-1252", "iso-8859-1", "latin1"):
            try:
                return val.decode(encoding)
            except (UnicodeDecodeError, AttributeError):
                continue
        return val.decode("utf-8", errors="replace")
    if isinstance(val, (dict, list)):
        return json.dumps(val, default=str)
    return str(val)


def export_result_to_csv(filename: str, result_data: dict) -> None:
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(result_data.get("columns") or [])
        for row in result_data.get("rows") or []:
            writer.writerow([cell_to_str(v) for v in row])


def export_rows_to_json(filename: str, rows: list, *, columns: list | None = None) -> None:
    payload = {"columns": columns or [], "rows": rows}
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
