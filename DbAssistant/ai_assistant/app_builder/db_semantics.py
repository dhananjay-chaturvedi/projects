"""Deterministic DB semantics for from_database app prediction.

This module turns catalog facts and bounded samples into an app-facing semantic
model before Session B is prompted. Declared catalog metadata is authoritative;
sample-only inference is used only to label plausible relationships when the
source DB does not declare referential integrity.
"""

from __future__ import annotations

import re
from typing import Any

_ID_RE = re.compile(r"(^id$|_id$|id$)", re.IGNORECASE)
_MONEY_RE = re.compile(r"amount|price|cost|total|balance|fee|salary|rate", re.IGNORECASE)
_TEMPORAL_RE = re.compile(r"(^|_)(date|time|timestamp)|_at$|created|updated", re.IGNORECASE)
_PII_RE = re.compile(r"email|phone|mobile|ssn|name|address|dob|birth", re.IGNORECASE)
_STATUS_RE = re.compile(r"status|state|type|category|kind", re.IGNORECASE)
_AUDIT_RE = re.compile(r"audit|log|history|event|activity", re.IGNORECASE)


def enrich_profile(profile: Any) -> None:
    """Populate semantic relationships, roles, and column tags in-place."""
    _mark_declared_relationship_columns(profile)
    _infer_missing_relationships(profile)
    _tag_columns(profile)
    _classify_table_roles(profile)
    _add_advisories(profile)


def hub_tables(profile: Any, *, limit: int = 5) -> list[str]:
    counts: dict[str, int] = {}
    for rel in getattr(profile, "relationships", []) or []:
        target = str(rel.get("to_table") or "").lower()
        if target:
            counts[target] = counts.get(target, 0) + 1
    by_actual = {t.name.lower(): t.name for t in getattr(profile, "tables", [])}
    return [
        by_actual.get(name, name)
        for name, _count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]
    ]


def relationship_summary(profile: Any, *, limit: int = 12) -> list[str]:
    out: list[str] = []
    for rel in (getattr(profile, "relationships", []) or [])[:limit]:
        src = rel.get("source", "inferred")
        conf = rel.get("confidence")
        suffix = f", confidence {conf:.0%}" if isinstance(conf, (int, float)) and src != "declared" else ""
        out.append(
            f"{rel.get('from_table')}.{rel.get('from_column')} -> "
            f"{rel.get('to_table')}.{rel.get('to_column')} "
            f"({rel.get('kind', 'N:1')}, {src}{suffix})"
        )
    return out


def table_role_summary(profile: Any, *, limit: int = 15) -> list[str]:
    lines: list[str] = []
    for table in (getattr(profile, "tables", []) or [])[:limit]:
        role = getattr(table, "role", "") or "unclassified"
        conf = getattr(table, "role_confidence", 0.0) or 0.0
        lines.append(f"{table.name}: {role} ({conf:.0%})")
    return lines


def semantic_column_summary(profile: Any, *, limit: int = 20) -> list[str]:
    lines: list[str] = []
    for table in getattr(profile, "tables", []) or []:
        for col in getattr(table, "columns", []) or []:
            tags = list(getattr(col, "semantic_tags", []) or [])
            if tags:
                lines.append(f"{table.name}.{col.name}: {', '.join(tags)}")
                if len(lines) >= limit:
                    return lines
    return lines


def _mark_declared_relationship_columns(profile: Any) -> None:
    tables = {t.name.lower(): t for t in getattr(profile, "tables", [])}
    for rel in getattr(profile, "relationships", []) or []:
        table = tables.get(str(rel.get("from_table") or "").lower())
        if table is None:
            continue
        col = _column(table, str(rel.get("from_column") or ""))
        if col is not None:
            col.is_fk = True


def _infer_missing_relationships(profile: Any) -> None:
    tables = list(getattr(profile, "tables", []) or [])
    existing = {
        (
            str(r.get("from_table") or "").lower(),
            str(r.get("from_column") or "").lower(),
            str(r.get("to_table") or "").lower(),
            str(r.get("to_column") or "").lower(),
        )
        for r in getattr(profile, "relationships", []) or []
    }
    pk_targets = []
    for target in tables:
        for col in getattr(target, "columns", []) or []:
            if getattr(col, "is_pk", False) or col.name.lower() == "id":
                pk_targets.append((target, col))
    for source in tables:
        for scol in getattr(source, "columns", []) or []:
            if getattr(scol, "is_pk", False):
                continue
            if not _ID_RE.search(scol.name):
                continue
            best = None
            best_score = 0.0
            for target, tcol in pk_targets:
                if target.name == source.name:
                    continue
                score = _relationship_score(source, scol, target, tcol)
                if score > best_score:
                    best = (target, tcol)
                    best_score = score
            if best is None or best_score < 0.55:
                continue
            target, tcol = best
            key = (source.name.lower(), scol.name.lower(), target.name.lower(), tcol.name.lower())
            if key in existing:
                continue
            scol.is_fk = True
            profile.relationships.append({
                "from_table": source.name,
                "from_column": scol.name,
                "to_table": target.name,
                "to_column": tcol.name,
                "kind": "N:1",
                "source": "inferred",
                "confidence": round(best_score, 3),
                "evidence": "sample/name/pk heuristic",
            })
            existing.add(key)


def _relationship_score(source: Any, scol: Any, target: Any, tcol: Any) -> float:
    score = 0.0
    sname = scol.name.lower()
    target_singular = _singular(target.name.lower())
    if sname in (f"{target_singular}_id", f"{target.name.lower()}_id"):
        score += 0.35
    elif target_singular in sname:
        score += 0.22
    if getattr(tcol, "is_pk", False) or tcol.name.lower() == "id":
        score += 0.2
    if _compatible_types(getattr(scol, "data_type", ""), getattr(tcol, "data_type", "")):
        score += 0.15
    overlap = _sample_overlap(source, scol.name, target, tcol.name)
    if overlap is not None:
        score += 0.3 * overlap
    return min(score, 0.95)


def _sample_overlap(source: Any, source_col: str, target: Any, target_col: str) -> float | None:
    svals = {
        str(r.get(source_col))
        for r in (getattr(source, "sample_rows", []) or [])
        if r.get(source_col) not in (None, "")
    }
    tvals = {
        str(r.get(target_col))
        for r in (getattr(target, "sample_rows", []) or [])
        if r.get(target_col) not in (None, "")
    }
    if not svals or not tvals:
        return None
    return len(svals & tvals) / len(svals)


def _compatible_types(left: str, right: str) -> bool:
    l = (left or "").lower()
    r = (right or "").lower()
    if not l or not r:
        return True
    intish = ("int", "number", "serial", "bigint")
    textish = ("char", "text", "uuid", "varchar")
    return (any(x in l for x in intish) and any(x in r for x in intish)) or (
        any(x in l for x in textish) and any(x in r for x in textish)
    )


def _tag_columns(profile: Any) -> None:
    for table in getattr(profile, "tables", []) or []:
        for col in getattr(table, "columns", []) or []:
            tags: set[str] = set(getattr(col, "semantic_tags", []) or [])
            name = col.name.lower()
            dtype = (getattr(col, "data_type", "") or "").lower()
            if _MONEY_RE.search(name):
                tags.add("money")
            if _TEMPORAL_RE.search(name) or any(x in dtype for x in ("date", "time")):
                tags.add("temporal")
            if _PII_RE.search(name):
                tags.add("pii")
            if _STATUS_RE.search(name):
                tags.add("status")
            if _is_enum_like(table, col):
                tags.add("enum")
            if getattr(col, "is_fk", False):
                tags.add("foreign_key")
            if getattr(col, "is_pk", False):
                tags.add("primary_key")
            col.semantic_tags = sorted(tags)


def _is_enum_like(table: Any, col: Any) -> bool:
    distinct = getattr(col, "distinct_estimate", None)
    rows = getattr(table, "row_count_estimate", None)
    if distinct is None:
        return bool(_STATUS_RE.search(col.name))
    if rows and rows > 0:
        return distinct <= 12 and distinct <= max(2, int(rows * 0.3))
    return distinct <= 8 and not getattr(col, "is_pk", False)


def _classify_table_roles(profile: Any) -> None:
    outgoing: dict[str, list[dict]] = {}
    incoming: dict[str, list[dict]] = {}
    for rel in getattr(profile, "relationships", []) or []:
        outgoing.setdefault(str(rel.get("from_table") or "").lower(), []).append(rel)
        incoming.setdefault(str(rel.get("to_table") or "").lower(), []).append(rel)
    for table in getattr(profile, "tables", []) or []:
        key = table.name.lower()
        cols = list(getattr(table, "columns", []) or [])
        fk_count = len(outgoing.get(key, []))
        in_count = len(incoming.get(key, []))
        non_key_cols = [
            c for c in cols
            if not getattr(c, "is_pk", False) and not getattr(c, "is_fk", False)
        ]
        if _AUDIT_RE.search(table.name):
            table.role, table.role_confidence = "audit", 0.85
        elif fk_count >= 2 and len(non_key_cols) <= 2:
            table.role, table.role_confidence = "junction", 0.85
        elif fk_count >= 1 and _has_event_signal(table, cols):
            table.role, table.role_confidence = "transaction", 0.78
        elif _lookup_like(table, cols):
            table.role, table.role_confidence = "lookup", 0.8
        elif in_count >= 1 or len(non_key_cols) >= 2:
            table.role, table.role_confidence = "master", 0.72
        else:
            table.role, table.role_confidence = "master", 0.55


def _lookup_like(table: Any, cols: list[Any]) -> bool:
    rows = getattr(table, "row_count_estimate", None)
    names = {c.name.lower() for c in cols}
    return (
        rows is not None and rows <= 50 and len(cols) <= 5
        and bool(names & {"code", "name", "label", "status", "type"})
    )


def _has_event_signal(table: Any, cols: list[Any]) -> bool:
    blob = " ".join([table.name] + [c.name for c in cols])
    return bool(_TEMPORAL_RE.search(blob) or _MONEY_RE.search(blob) or _AUDIT_RE.search(blob))


def _add_advisories(profile: Any) -> None:
    declared = [r for r in getattr(profile, "relationships", []) or [] if r.get("source") == "declared"]
    inferred = [r for r in getattr(profile, "relationships", []) or [] if r.get("source") == "inferred"]
    if not declared and inferred:
        profile.advisory_notes.append(
            "source DB did not expose declared foreign keys; relationship graph includes labeled inferred edges"
        )
    elif not declared and len(getattr(profile, "tables", []) or []) > 1:
        profile.advisory_notes.append(
            "source DB did not expose declared foreign keys; Session B receives table roles without authoritative RI"
        )


def _column(table: Any, name: str) -> Any | None:
    low = name.lower()
    for col in getattr(table, "columns", []) or []:
        if col.name.lower() == low:
            return col
    return None


def _singular(name: str) -> str:
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("s") and len(name) > 3:
        return name[:-1]
    return name
