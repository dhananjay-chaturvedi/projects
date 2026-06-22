"""Load NL questions from user-supplied files for harvest/training."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path


def load_questions_from_file(path: str | Path) -> list[str]:
    """Read questions from text, CSV, JSONL, or markdown list files."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Questions file not found: {p}")
    text = p.read_text(encoding="utf-8", errors="replace")
    suffix = p.suffix.lower()

    if suffix == ".jsonl":
        return _from_jsonl(text)
    if suffix == ".json":
        return _from_json(text)
    if suffix == ".csv":
        return _from_csv(text)
    return _from_lines(text)


def _clean_line(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^\s*(?:[-*\u2022]|\d+[.)])\s*", "", line)
    return line.strip().strip('"').strip("'")


def _from_lines(text: str) -> list[str]:
    out: list[str] = []
    for line in text.splitlines():
        q = _clean_line(line)
        if len(q) >= 3 and not q.startswith("#"):
            out.append(q)
    return out


def _from_csv(text: str) -> list[str]:
    out: list[str] = []
    reader = csv.reader(text.splitlines())
    for row in reader:
        if not row:
            continue
        q = _clean_line(row[0])
        if q and q.lower() not in ("question", "questions"):
            out.append(q)
    return out


def _from_json(text: str) -> list[str]:
    data = json.loads(text)
    if isinstance(data, list):
        out: list[str] = []
        for item in data:
            if isinstance(item, str):
                out.append(item.strip())
            elif isinstance(item, dict):
                q = item.get("question") or item.get("prompt") or item.get("text")
                if q:
                    out.append(str(q).strip())
        return [q for q in out if q]
    if isinstance(data, dict):
        items = data.get("questions") or data.get("prompts") or []
        return [str(q).strip() for q in items if str(q).strip()]
    return []


def _from_jsonl(text: str) -> list[str]:
    out: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            q = _clean_line(line)
            if q:
                out.append(q)
            continue
        if isinstance(obj, dict):
            q = obj.get("question") or obj.get("prompt") or obj.get("text")
            if q:
                out.append(str(q).strip())
        elif isinstance(obj, str):
            out.append(obj.strip())
    return [q for q in out if q]
