"""Training-data collection for the standalone local NL->SQL LLM.

Harvests vetted ``{question, sql, description}`` triples from generated app
files, DB understanding insights, capture-store records, and existing RAG
examples. Persists pairs into the per-connection RAG index so
:class:`~ai_assistant.llm.service.LlmService` can train on them.
"""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any, Iterable

_SQL_KEYWORDS = re.compile(
    r"^\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|REPLACE)\b",
    re.IGNORECASE,
)
# Triple/single-quoted SQL string literals in Python source.
_PY_SQL_STRING = re.compile(
    r'(?P<q>"""|\'\'\'|"|\')(?P<body>(?:\\.|(?!\1).)*?)(?P=q)',
    re.DOTALL,
)
_FUNC_DEF = re.compile(r"def\s+(\w+)\s*\([^)]*\)\s*(?:->[^:]+)?:\s*(?:\"\"\"(.*?)\"\"\")?", re.DOTALL)


from ai_assistant.llm.validation import normalize_sql, validate_pair, validate_pairs

# Back-compat alias used by older tests/shims.
_normalize_sql = normalize_sql


def _valid_sql(sql: str, *, db_type: str | None = None) -> bool:
    ok, _, _ = validate_pair({"question": "q", "sql": sql}, db_type=db_type)
    return ok


def _fold_question(question: str, description: str = "") -> str:
    """Return NL question only — descriptions are kept separate for metadata."""
    from ai_assistant.llm.validation import clean_question

    return clean_question(question, description)


def _dedupe_pairs(pairs: Iterable[dict], *, db_type: str | None = None) -> list[dict]:
    kept, _stats = validate_pairs(list(pairs), db_type=db_type)
    return kept


def _pairs_from_insight(insight: Any) -> list[dict]:
    """Seed NL questions from table insights and design brief."""
    pairs: list[dict] = []
    if insight is None:
        return pairs
    tables = getattr(insight, "tables", None) or (insight or {}).get("tables") or []
    for t in tables:
        if isinstance(t, dict):
            name = t.get("name", "")
            note = t.get("note", "")
            cols = t.get("columns") or []
        else:
            name = getattr(t, "name", "")
            note = getattr(t, "note", "")
            cols = getattr(t, "columns", []) or []
        if not name:
            continue
        desc = note or f"Table {name} in the connected database"
        q = f"List all rows from the {name} table"
        sql = f"SELECT * FROM {name}"
        if cols:
            col_list = ", ".join(str(c) for c in cols[:12])
            q = f"Show {col_list} from {name}"
            sql = f"SELECT {col_list} FROM {name}"
        pairs.append({"question": _fold_question(q, desc), "sql": sql, "description": desc})
    summary = getattr(insight, "app_summary", "") or ""
    if isinstance(insight, dict):
        summary = summary or insight.get("app_summary") or ""
    if summary:
        pairs.append({
            "question": _fold_question("Summarize the database for a DBA dashboard", summary),
            "sql": "SELECT name FROM sqlite_master WHERE type='table'",
            "description": summary,
        })
    features = getattr(insight, "app_features", None) or []
    if isinstance(insight, dict):
        features = features or insight.get("app_features") or []
    for feat in (features or [])[:8]:
        feat = str(feat).strip()
        if not feat or not tables:
            continue
        first = tables[0]
        tname = first.get("name") if isinstance(first, dict) else getattr(first, "name", "")
        if not tname:
            continue
        pairs.append({
            "question": _fold_question(f"Query data to support: {feat}", summary),
            "sql": f"SELECT * FROM {tname} LIMIT 10",
            "description": feat,
        })
    return pairs


def _pairs_from_workspace(workspace: Path) -> list[dict]:
    """Extract SQL strings from generated app source files."""
    pairs: list[dict] = []
    if not workspace or not workspace.exists():
        return pairs
    scan_roots = [
        workspace / "src",
        workspace,
    ]
    seen_files: set[Path] = set()
    for root in scan_roots:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if path in seen_files or "test" in path.name.lower():
                continue
            seen_files.add(path)
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            rel = path.relative_to(workspace).as_posix()
            for m in _PY_SQL_STRING.finditer(text):
                body = m.group("body")
                if not _SQL_KEYWORDS.search(body):
                    continue
                # Find nearest function name / docstring before this match.
                prefix = text[: m.start()]
                funcs = list(_FUNC_DEF.finditer(prefix))
                fname = funcs[-1].group(1) if funcs else ""
                doc = (funcs[-1].group(2) or "").strip() if funcs else ""
                desc = doc or f"Query from {rel}" + (f" ({fname})" if fname else "")
                q = fname.replace("_", " ") if fname else f"Run query in {rel}"
                if doc:
                    q = doc.split("\n")[0].strip() or q
                pairs.append({
                    "question": _fold_question(q, desc),
                    "sql": body.strip(),
                    "description": desc,
                })
    return pairs


def _pairs_from_capture(connection: str) -> list[dict]:
    """Load NL->SQL pairs from the capture store for a connection."""
    pairs: list[dict] = []
    if not connection:
        return pairs
    try:
        from ai_assistant.capture.store import IsolatedCaptureStore
        from common import paths as app_paths

        store = IsolatedCaptureStore(app_paths.ai_capture_dir())
        conn_slug = re.sub(r"\W+", "_", connection.lower())[:60]
        for path in store.root.rglob("samples.jsonl"):
            if conn_slug not in str(path).lower() and connection.lower() not in str(path).lower():
                continue
            for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                q = rec.get("question") or rec.get("nl_question") or ""
                parsed = rec.get("parsed") or {}
                sql = parsed.get("summary_sql") or parsed.get("sql") or rec.get("sql") or ""
                # Preserve the AI's explanation as the pair description so it
                # travels into RAG (retrievable context) and the dataset view;
                # this is how chat/follow-up turns train "from the explanation"
                # too, not just the Generated SQL.
                desc = (
                    rec.get("explanation")
                    or parsed.get("explanation")
                    or rec.get("purpose", "")
                    or ""
                )
                if q and sql:
                    pairs.append({"question": q, "sql": sql, "description": desc})
    except Exception:
        pass
    return pairs


def _pairs_from_rag(connection: str) -> list[dict]:
    try:
        from ai_assistant.llm.service import LlmService

        return [
            {**p, "description": ""}
            for p in LlmService._rag_examples(connection)  # noqa: SLF001
        ]
    except Exception:
        return []


def collect_build_pairs(
    workspace: str | Path,
    connection: str = "",
    insight: Any = None,
    *,
    db_type: str | None = None,
) -> list[dict]:
    """Collect training pairs after an app build completes."""
    ws = Path(workspace)
    pairs: list[dict] = []
    pairs.extend(_pairs_from_insight(insight))
    pairs.extend(_pairs_from_workspace(ws))
    if connection:
        pairs.extend(_pairs_from_capture(connection))
        pairs.extend(_pairs_from_rag(connection))
    return _dedupe_pairs(pairs, db_type=db_type)


def collect_connection_pairs(
    connection: str,
    insight: Any = None,
    *,
    use_rag: bool = True,
    include_capture: bool = True,
    db_type: str | None = None,
) -> list[dict]:
    """Collect training pairs without building an app (manual Train LLM)."""
    pairs: list[dict] = []
    pairs.extend(_pairs_from_insight(insight))
    if connection:
        if include_capture:
            pairs.extend(_pairs_from_capture(connection))
        if use_rag:
            pairs.extend(_pairs_from_rag(connection))
    return _dedupe_pairs(pairs, db_type=db_type)


def collect_scratch_pairs(description: str = "") -> list[dict]:
    """Seed pairs from a scratch description when no DB/codebase is available."""
    desc = (description or "").strip()
    if not desc:
        return []
    pairs = [{
        "question": _fold_question("Describe the data model implied by this app", desc),
        "sql": "SELECT name FROM sqlite_master WHERE type='table'",
        "description": desc,
    }]
    return _dedupe_pairs(pairs)


def collect_codebase_pairs(codebase_path: str | Path) -> list[dict]:
    """Collect training pairs by scanning a codebase directory for SQL strings."""
    path = Path(codebase_path)
    if not path.exists():
        return []
    return _dedupe_pairs(_pairs_from_workspace(path))


def persist_pairs(
    connection: str,
    pairs: list[dict],
    *,
    core: Any = None,
) -> tuple[str, int]:
    """Write pairs to RAG examples and a temp JSONL dataset file.

    Returns ``(dataset_path, count)``.
    """
    if not pairs:
        return "", 0
    rag = None
    try:
        from ai_assistant.rag.service import RagService

        rag = RagService(core)
    except Exception:
        rag = None
    for p in pairs:
        if rag is not None and connection:
            rag.add_example(
                connection,
                p["question"],
                p["sql"],
                description=p.get("description") or "",
            )
    fd, path = tempfile.mkstemp(suffix=".jsonl", prefix="ab_train_")
    import os

    os.close(fd)
    out_path = Path(path)
    with out_path.open("w", encoding="utf-8") as fh:
        for p in pairs:
            fh.write(json.dumps({"question": p["question"], "sql": p["sql"]}) + "\n")
    return str(out_path), len(pairs)


def resolve_train_names(body: dict) -> list[str]:
    """Merge selected existing model names + optional new model name."""
    names = [str(n).strip() for n in (body.get("train_llm") or []) if str(n).strip()]
    new_name = str(body.get("train_new_name") or "").strip()
    if new_name and new_name not in names:
        names.append(new_name)
    return names
