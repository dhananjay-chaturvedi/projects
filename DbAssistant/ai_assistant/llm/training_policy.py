"""Shared policy for building high-quality NL->SQL training corpora.

The policy follows current text-to-SQL synthesis practice: schema grounded,
execution verified, complexity tagged, and deduplicated before training.
"""

from __future__ import annotations

from typing import Any


def build_training_corpus(
    core: Any,
    connection: str,
    *,
    sample_limit: int = 5,
    max_tables: int = 40,
    max_pairs: int = 400,
    validate: bool = True,
    include_capture: bool = True,
    include_rag: bool = False,
    insight: Any = None,
    on_progress: Any = None,
) -> dict[str, Any]:
    """Return a vetted corpus from DB mining plus optional captures/RAG."""
    from ai_assistant.llm.data_sources import (
        _dedupe_pairs,
        collect_connection_pairs,
    )
    from ai_assistant.llm.db_query_miner import mine_connection_pairs

    pairs: list[dict] = []
    mined = mine_connection_pairs(
        core,
        connection,
        sample_limit=sample_limit,
        max_tables=max_tables,
        max_pairs=max_pairs,
        validate=validate,
        on_progress=on_progress,
    )
    if mined.get("ok"):
        pairs.extend(mined.get("pairs") or [])
    if include_capture or include_rag or insight is not None:
        pairs.extend(collect_connection_pairs(
            connection,
            insight,
            use_rag=include_rag,
            include_capture=include_capture,
        ))
    pairs = _dedupe_pairs(_annotate_policy(pairs))
    stats = dict(mined.get("stats") or {})
    stats["policy"] = {
        "execution_validated": bool(validate),
        "include_capture": bool(include_capture),
        "include_rag": bool(include_rag),
        "dedupe": "question+normalized_sql",
    }
    return {
        "ok": bool(pairs),
        "pairs": pairs,
        "db_type": mined.get("db_type", ""),
        "error": None if pairs else (mined.get("error") or "No training pairs generated."),
        "stats": stats,
    }


def _annotate_policy(pairs: list[dict]) -> list[dict]:
    out: list[dict] = []
    for p in pairs:
        q = dict(p)
        desc = (q.get("description") or "").strip()
        if desc and "complexity=" not in desc:
            complexity = _complexity(q.get("sql", ""))
            q["description"] = f"{desc}; complexity={complexity}"
        elif not desc:
            q["description"] = f"complexity={_complexity(q.get('sql', ''))}"
        out.append(q)
    return out


def _complexity(sql: str) -> str:
    s = (sql or "").lower()
    score = 0
    for token in (" join ", " group by ", " over ", " having ", " union ", " intersect "):
        if token in s:
            score += 1
    if "select" in s and s.count("select") > 1:
        score += 1
    if score >= 2:
        return "complex"
    if score == 1 or any(fn in s for fn in ("count(", "sum(", "avg(", "min(", "max(")):
        return "moderate"
    return "simple"
