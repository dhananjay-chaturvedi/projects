"""Persistent store of AI-enriched NL->SQL query templates.

The built-in templates in :mod:`ai_assistant.llm.query_templates` are static.
The "Enrich template" feature lets the selected AI backend *grow* the library:
it produces reusable, placeholder-parameterised templates per dialect that the
corpus builders render with real object names at training time. Accepted
templates are persisted here and merged into the corpus on the next train.

A single object template (e.g. ``SELECT COUNT(*) FROM {table}``) therefore lets
a locally-trained model answer the same question across every connection of a
dialect: at generation time the placeholders are filled with the real object
names from the connected database's live schema — no per-table re-training.

This module deliberately depends only on :mod:`ai_assistant.llm.dataset` (for
``normalize_db_type``) so :mod:`query_templates` can import it lazily without an
import cycle. It returns plain dicts; ``query_templates`` wraps object templates
into ``QueryTemplate`` instances.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from ai_assistant.llm.dataset import normalize_db_type

# Placeholder tokens an object template may reference. These MUST match the keys
# produced by ``query_templates.build_template_values`` so every enriched object
# template renders against real schema metadata exactly like a built-in one.
ALLOWED_OBJECT_PLACEHOLDERS: frozenset[str] = frozenset({
    "table", "table_label", "limit", "bounded_select", "limit_clause",
    "limit_50_clause", "col_list", "col_list_label", "text_col", "text_col_q",
    "num_col", "num_col_q", "first_col", "first_col_q",
})

_VERSION = 1
_lock = threading.RLock()
_cache: dict[str, Any] = {"mtime": None, "data": None}


def store_path() -> Path:
    from common import paths as _paths

    return _paths.session_dir() / "llm" / "enriched_templates.json"


def _empty() -> dict[str, Any]:
    return {"version": _VERSION, "catalog": {}, "object": []}


def load() -> dict[str, Any]:
    """Return the stored templates, or an empty structure when none exist."""
    path = store_path()
    with _lock:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            _cache["mtime"], _cache["data"] = None, None
            return _empty()
        if _cache["data"] is not None and _cache["mtime"] == mtime:
            return json.loads(json.dumps(_cache["data"]))  # defensive copy
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            data = _empty()
        if not isinstance(data, dict):
            data = _empty()
        data.setdefault("version", _VERSION)
        data.setdefault("catalog", {})
        data.setdefault("object", [])
        _cache["mtime"], _cache["data"] = mtime, data
        return json.loads(json.dumps(data))


def _save(data: dict[str, Any]) -> None:
    path = store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    import os

    os.replace(tmp, path)
    with _lock:
        try:
            _cache["mtime"] = path.stat().st_mtime
        except OSError:
            _cache["mtime"] = None
        _cache["data"] = json.loads(json.dumps(data))


def _requires_from_sql(sql: str) -> list[str]:
    req: list[str] = []
    if "{text_col" in sql:
        req.append("text_col")
    if "{num_col" in sql:
        req.append("num_col")
    if "{col_list" in sql or "{first_col" in sql:
        req.append("columns")
    return req


def add(
    *,
    catalog: dict[str, list[dict]] | None = None,
    objects: list[dict] | None = None,
) -> dict[str, Any]:
    """Merge new templates into the store (de-duplicated). Returns a summary."""
    with _lock:
        data = load()
        added_catalog = 0
        added_object = 0

        for db_type, items in (catalog or {}).items():
            tag = normalize_db_type(db_type)
            bucket = data["catalog"].setdefault(tag, [])
            seen = {(e.get("question", ""), e.get("sql", "")) for e in bucket}
            for item in items or []:
                q = (item.get("question") or "").strip()
                sql = (item.get("sql") or "").strip()
                if not q or not sql or (q, sql) in seen:
                    continue
                bucket.append({
                    "id": item.get("id") or f"enriched.{tag}.{len(bucket)}",
                    "question": q,
                    "sql": sql,
                    "category": item.get("category") or "catalog",
                })
                seen.add((q, sql))
                added_catalog += 1

        obj_seen = {
            (e.get("question", ""), e.get("sql", "")) for e in data["object"]
        }
        for item in objects or []:
            q = (item.get("question") or "").strip()
            sql = (item.get("sql") or "").strip()
            if not q or not sql or (q, sql) in obj_seen:
                continue
            data["object"].append({
                "id": item.get("id") or f"enriched.object.{len(data['object'])}",
                "question": q,
                "sql": sql,
                "requires": list(item.get("requires") or _requires_from_sql(sql)),
                "category": item.get("category") or "object",
                "complexity": item.get("complexity") or "moderate",
                "db_types": list(item.get("db_types") or ["*"]),
            })
            obj_seen.add((q, sql))
            added_object += 1

        if added_catalog or added_object:
            _save(data)
        return {
            "added_catalog": added_catalog,
            "added_object": added_object,
            **summary(data),
        }


def enriched_catalog_pairs() -> list[dict]:
    """Catalog/metadata templates per dialect (mirrors all_catalog_pairs)."""
    data = load()
    out: list[dict] = []
    for tag, items in (data.get("catalog") or {}).items():
        for e in items:
            out.append({
                "question": e.get("question", ""),
                "sql": e.get("sql", ""),
                "db_type": tag,
                "category": e.get("category") or "catalog",
                "scope": "catalog",
            })
    return out


def enriched_object_templates() -> list[dict]:
    """Object templates as plain dicts (query_templates wraps them)."""
    data = load()
    return [dict(e) for e in (data.get("object") or [])]


def summary(data: dict[str, Any] | None = None) -> dict[str, Any]:
    data = data if data is not None else load()
    catalog = data.get("catalog") or {}
    return {
        "catalog_dialects": len(catalog),
        "catalog_templates": sum(len(v) for v in catalog.values()),
        "object_templates": len(data.get("object") or []),
    }


def clear() -> dict[str, Any]:
    with _lock:
        _save(_empty())
        return {"ok": True, **summary()}
