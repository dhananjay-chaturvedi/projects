"""Human-readable migration preview text (Tk parity for TUI/Web)."""

from __future__ import annotations

import json
from typing import Any


def format_schema_preview(result: dict[str, Any]) -> str:
    """Format ``convert_schema_multi`` output like the Tk preview pane."""
    tables = result.get("tables") or []
    lines = [
        "=" * 80,
        f"SCHEMA CONVERSION PREVIEW ({len(tables)} table(s))",
        "=" * 80,
        "",
    ]
    if result.get("error"):
        lines.append(f"ERROR: {result['error']}\n")
    for row in tables:
        table = row.get("table") or "?"
        target = row.get("target_table") or table
        lines.extend([f"Table: {table}  ->  {target}", "-" * 80])
        if row.get("error"):
            lines.append(f"  ERROR: {row['error']}\n")
            continue
        issues = row.get("issues") or []
        if issues:
            lines.append("VALIDATION WARNINGS:")
            for issue in issues:
                lines.append(f"  - {issue}")
            lines.append("")
        ddl = (row.get("ddl") or "").strip()
        if ddl:
            lines.extend(["GENERATED DDL:", ddl, ""])
        for extra in row.get("indexes_ddl") or []:
            stmt = (extra or "").strip()
            if stmt:
                lines.append(stmt)
        if row.get("all_ddl"):
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def format_sample_data(result: dict[str, Any]) -> str:
    """Format ``sample_rows_multi`` output like the Tk sample-data pane."""
    tables = result.get("tables") or []
    lines = [
        "=" * 80,
        "SAMPLE DATA (first row from each table)",
        f"Checking {len(tables)} table(s)",
        "=" * 80,
        "",
    ]
    if result.get("error"):
        lines.append(f"ERROR: {result['error']}\n")
    for row in tables:
        table = row.get("table") or "?"
        lines.extend([f"Table: {table}", "-" * 80])
        if row.get("error"):
            lines.append(f"  ERROR: {row['error']}\n")
            continue
        cols = row.get("columns") or []
        rows = row.get("rows") or []
        if not rows:
            lines.append("  (no rows)\n")
            continue
        if cols:
            lines.append("  Columns: " + ", ".join(str(c) for c in cols))
        for i, sample in enumerate(rows, 1):
            if isinstance(sample, dict):
                lines.append(f"  Row {i}: {json.dumps(sample, default=str)}")
            else:
                lines.append(f"  Row {i}: {sample}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
