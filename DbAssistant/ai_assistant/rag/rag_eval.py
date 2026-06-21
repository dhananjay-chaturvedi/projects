"""
Retrieval evaluation harness for the RAG pipeline.

Turns a small *gold set* of ``question -> expected tables`` cases into hard
numbers so retrieval tuning (``lexical_alpha`` / ``top_k`` / provider / rerank)
is data-driven rather than guesswork. Metrics are the standard IR set:

* **recall@k**       — fraction of expected tables present in the top-k hits.
* **MRR**            — mean reciprocal rank of the first relevant hit.
* **context precision** — fraction of returned hits that are relevant.

Gold cases can be authored explicitly or seeded automatically from the indexed
NL->SQL examples (the SQL's ``FROM``/``JOIN`` tables are the expected set).

This module holds only pure functions; :class:`~ai_assistant.rag.service.RagService`
wires them to the live retriever so UI/CLI/API share one code path.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE)\s+([A-Za-z_][\w.\"`\[\]]*)",
    re.IGNORECASE,
)


def extract_tables(sql: str, *, dialect: str = "") -> set[str]:
    """Return the set of table names referenced by *sql* (lower-cased, unqualified).

    Uses ``sqlglot`` when available for accuracy and falls back to a regex scan
    so the harness works even without the optional parser.
    """
    sql = (sql or "").strip()
    if not sql:
        return set()
    tables: set[str] = set()
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse(sql, read=dialect or None)
        for stmt in parsed:
            if stmt is None:
                continue
            for tbl in stmt.find_all(exp.Table):
                if tbl.name:
                    tables.add(tbl.name.lower())
        if tables:
            return tables
    except Exception:  # noqa: BLE001
        pass
    for m in _TABLE_RE.finditer(sql):
        raw = m.group(1).strip().strip('"`[]')
        name = raw.split(".")[-1].strip('"`[]')
        if name:
            tables.add(name.lower())
    return tables


def gold_from_examples(
    examples: Iterable[dict[str, Any]], *, dialect: str = ""
) -> list[dict[str, Any]]:
    """Seed gold cases from indexed NL->SQL examples.

    *examples* are rows with ``metadata.question`` / ``metadata.sql`` (as
    returned by ``store.list_by_kind(scope, "example")``).
    """
    gold: list[dict[str, Any]] = []
    for row in examples:
        meta = row.get("metadata") or {}
        question = (meta.get("question") or row.get("ref") or "").strip()
        sql = (meta.get("sql") or "").strip()
        if not question or not sql:
            continue
        tables = extract_tables(sql, dialect=dialect)
        if tables:
            gold.append({"question": question, "tables": sorted(tables)})
    return gold


def _norm(name: str) -> str:
    return (name or "").strip().strip('"`[]').split(".")[-1].lower()


def score_case(
    expected_tables: Iterable[str],
    hits: list[Any],
    *,
    k: int,
) -> dict[str, Any]:
    """Compute per-case recall@k / reciprocal rank / context precision.

    *hits* are :class:`RetrievalHit`-like objects with ``kind``/``ref``/``text``.
    """
    expected = {_norm(t) for t in expected_tables if t}
    top = hits[: max(1, k)]
    retrieved_tables: list[str] = []
    relevant_flags: list[bool] = []
    for h in top:
        kind = getattr(h, "kind", "")
        ref = _norm(getattr(h, "ref", ""))
        text_low = (getattr(h, "text", "") or "").lower()
        is_table = kind in ("table", "view")
        if is_table:
            retrieved_tables.append(ref)
        # A hit is "relevant" if it is an expected table/view, or any other doc
        # (relationship/example/etc.) that names an expected table.
        relevant = (is_table and ref in expected) or any(
            t and t in text_low for t in expected
        )
        relevant_flags.append(bool(relevant))

    found = expected & set(retrieved_tables)
    recall = (len(found) / len(expected)) if expected else 0.0

    rr = 0.0
    for rank, flag in enumerate(relevant_flags, start=1):
        if flag:
            rr = 1.0 / rank
            break

    precision = (sum(relevant_flags) / len(top)) if top else 0.0
    return {
        "expected": sorted(expected),
        "retrieved_tables": retrieved_tables,
        "found": sorted(found),
        "recall_at_k": round(recall, 4),
        "reciprocal_rank": round(rr, 4),
        "context_precision": round(precision, 4),
    }


def aggregate(case_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Average per-case metrics into recall@k / MRR / mean context precision."""
    n = len(case_metrics)
    if not n:
        return {"cases": 0, "recall_at_k": 0.0, "mrr": 0.0,
                "context_precision": 0.0}
    return {
        "cases": n,
        "recall_at_k": round(sum(c["recall_at_k"] for c in case_metrics) / n, 4),
        "mrr": round(sum(c["reciprocal_rank"] for c in case_metrics) / n, 4),
        "context_precision": round(
            sum(c["context_precision"] for c in case_metrics) / n, 4
        ),
    }
