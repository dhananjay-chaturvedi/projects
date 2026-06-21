"""Per-model training ledger and retry backlog (dataset.jsonl + pending_questions.jsonl)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PENDING_FILENAME = "pending_questions.jsonl"
DATASET_FILENAME = "dataset.jsonl"


def model_dir(name: str) -> Path:
    from ai_assistant.llm.service import LlmService

    return LlmService()._model_dir(name or "default")


def load_ledger_pairs(model_name: str) -> list[dict]:
    """Return all NL->SQL pairs saved for a trained model."""
    from ai_assistant.llm.service import LlmService

    r = LlmService().dataset(model_name or "default")
    if not r.get("available"):
        return []
    return list(r.get("pairs") or [])


def known_question_keys(model_names: list[str]) -> set[str]:
    """Normalized question keys already present in one or more model ledgers."""
    from ai_assistant.llm.validation import normalize_question_for_match

    known: set[str] = set()
    for name in model_names:
        for p in load_ledger_pairs(name):
            key = normalize_question_for_match(str(p.get("question") or ""))
            if key:
                known.add(key)
    return known


def load_backlog(model_name: str) -> list[dict]:
    """Load pending questions awaiting retry for *model_name*."""
    path = model_dir(model_name) / PENDING_FILENAME
    if not path.exists():
        return []
    out: list[dict] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            q = str(obj.get("question") or "").strip()
            if q:
                out.append({
                    "question": q,
                    "description": str(obj.get("description") or ""),
                })
    except Exception:
        return []
    return out


def _serialize_backlog(items: list[dict]) -> str:
    """Dedupe + serialise backlog items to JSONL text (empty string if none)."""
    from ai_assistant.llm.validation import normalize_question_for_match

    seen: set[str] = set()
    lines: list[str] = []
    for item in items:
        q = str(item.get("question") or "").strip()
        if not q:
            continue
        key = normalize_question_for_match(q)
        if not key or key in seen:
            continue
        seen.add(key)
        row = {"question": q}
        desc = str(item.get("description") or "").strip()
        if desc:
            row["description"] = desc
        lines.append(json.dumps(row))
    return ("\n".join(lines) + "\n") if lines else ""


def save_backlog(model_name: str, items: list[dict]) -> None:
    """Persist the retry backlog for *model_name* (replaces existing file).

    Serialised under a per-file lock + atomic replace so concurrent writers
    never tear the file or lose each other's writes.
    """
    from common.concurrency import atomic_write_text, file_lock

    mdir = model_dir(model_name)
    mdir.mkdir(parents=True, exist_ok=True)
    path = mdir / PENDING_FILENAME
    text = _serialize_backlog(items)
    with file_lock(path):
        if text:
            atomic_write_text(path, text, lock=False)
        elif path.exists():
            path.unlink()


def remove_from_backlog(model_name: str, question: str) -> None:
    """Drop one question from the backlog after it succeeds (lock-safe RMW)."""
    from ai_assistant.llm.validation import normalize_question_for_match
    from common.concurrency import atomic_write_text, file_lock

    key = normalize_question_for_match(question)
    if not key:
        return
    path = model_dir(model_name) / PENDING_FILENAME
    with file_lock(path):
        current: list[dict] = []
        if path.exists():
            try:
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    q = str(obj.get("question") or "").strip()
                    if q:
                        current.append({
                            "question": q,
                            "description": str(obj.get("description") or ""),
                        })
            except Exception:
                current = []
        remaining = [
            item for item in current
            if normalize_question_for_match(item.get("question", "")) != key
        ]
        text = _serialize_backlog(remaining)
        if text:
            atomic_write_text(path, text, lock=False)
        elif path.exists():
            path.unlink()


def merge_incremental_pairs(
    incoming: list[dict],
    model_name: str,
    *,
    db_type: str | None = None,
) -> tuple[list[dict], int, int]:
    """Union *incoming* with the model ledger; return (pairs, already_trained, new_pairs)."""
    from ai_assistant.llm.data_sources import _dedupe_pairs
    from ai_assistant.llm.validation import normalize_question_for_match

    ledger = load_ledger_pairs(model_name)
    already = len(ledger)
    ledger_keys = {
        normalize_question_for_match(p.get("question", "")) for p in ledger
    }
    ledger_keys.discard("")
    new_count = 0
    for p in incoming:
        key = normalize_question_for_match(p.get("question", ""))
        if key and key not in ledger_keys:
            new_count += 1
    merged = _dedupe_pairs(list(ledger) + list(incoming), db_type=db_type)
    return merged, already, new_count
