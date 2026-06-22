"""Build-data NL->SQL training corpus.

Unlike :mod:`ai_assistant.llm.harvest_service` (the generic DB-mining + AI
question-bank harvest used by *Build and Train LLM*), this module derives a
training corpus from the **app build's own rich data**:

* the generated app's schema (``src/db/schema.sql``) and the queries baked into
  the generated source files,
* the DB-understanding ``insight`` (tables, roles, relationships, sample rows,
  design brief, app features, data flow),
* the build transcript / decisions (what the agent generated during the build),
* the codebase profile (``from_codebase`` mode).

Every generated pair is **execution-validated** for accuracy:

* ``from_database`` — validated against the selected live connection,
* ``from_scratch`` / ``from_codebase`` — validated against a throwaway SQLite
  database materialized from the generated ``src/db/schema.sql`` so even a
  from-scratch build yields grounded, executable training data.

The output is a deduped list of ``{question, sql, description}`` triples ready
for :class:`ai_assistant.llm.service.LlmService` to train on.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any, Callable

from ai_assistant.llm.data_sources import (
    _dedupe_pairs,
    _fold_question,
    _pairs_from_capture,
    _pairs_from_insight,
    _pairs_from_rag,
    _pairs_from_workspace,
)
from ai_assistant.llm.sql_check import check_sql

# ── schema parsing ───────────────────────────────────────────────────────────
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"'`\[]?(?P<name>\w+)[\"'`\]]?\s*\((?P<body>.*?)\)\s*;",
    re.IGNORECASE | re.DOTALL,
)
_TEXTY = ("char", "text", "clob", "string", "varchar", "uuid")
_NUMY = ("int", "real", "float", "double", "decimal", "numeric", "serial")


def _split_columns(body: str) -> list[str]:
    """Split a CREATE TABLE body on top-level commas (ignoring nested parens)."""
    cols: list[str] = []
    depth = 0
    buf = ""
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            cols.append(buf.strip())
            buf = ""
        else:
            buf += ch
    if buf.strip():
        cols.append(buf.strip())
    return cols


def parse_schema_sql(text: str) -> dict[str, list[tuple[str, str]]]:
    """Parse ``CREATE TABLE`` statements into ``{table: [(column, type), ...]}``."""
    tables: dict[str, list[tuple[str, str]]] = {}
    if not text:
        return tables
    for m in _CREATE_TABLE_RE.finditer(text):
        name = m.group("name")
        columns: list[tuple[str, str]] = []
        for raw in _split_columns(m.group("body")):
            if not raw:
                continue
            head = raw.split("(")[0].strip()
            first = head.split()[0].strip("\"'`[]") if head.split() else ""
            upper = first.upper()
            if upper in {
                "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "KEY", "INDEX",
            }:
                continue
            parts = head.split()
            col = first
            col_type = parts[1] if len(parts) > 1 else "TEXT"
            if col:
                columns.append((col, col_type))
        if name and columns:
            tables[name] = columns
    return tables


def _columns_from_insight(insight: Any) -> dict[str, list[tuple[str, str]]]:
    """Pull ``{table: [(column, type), ...]}`` from a DataInsight-like object."""
    out: dict[str, list[tuple[str, str]]] = {}
    if insight is None:
        return out
    tables = getattr(insight, "tables", None)
    if tables is None and isinstance(insight, dict):
        tables = insight.get("tables")
    for t in tables or []:
        if isinstance(t, dict):
            name = t.get("name", "")
            cols = t.get("columns") or []
        else:
            name = getattr(t, "name", "")
            cols = getattr(t, "columns", []) or []
        if not name:
            continue
        parsed: list[tuple[str, str]] = []
        for c in cols:
            if isinstance(c, dict):
                cname = c.get("name") or c.get("column") or ""
                ctype = c.get("data_type") or c.get("type") or "TEXT"
            else:
                cname = str(c)
                ctype = "TEXT"
            if cname:
                parsed.append((str(cname), str(ctype)))
        if parsed:
            out[name] = parsed
    return out


def _classify(columns: list[tuple[str, str]]) -> dict[str, Any]:
    names = [c for c, _ in columns]
    pk = next((c for c, _ in columns if c.lower() == "id"), names[0] if names else "")
    text_cols = [c for c, t in columns if any(k in t.lower() for k in _TEXTY) and c != pk]
    num_cols = [c for c, t in columns if any(k in t.lower() for k in _NUMY) and c != pk]
    return {"names": names, "pk": pk, "text_cols": text_cols, "num_cols": num_cols}


# ── grounded pair generation ──────────────────────────────────────────────────
def _humanize(table: str) -> str:
    return table.replace("_", " ").strip() or table


def grounded_pairs_for_table(
    table: str, columns: list[tuple[str, str]],
) -> list[dict]:
    """Generate schema-grounded NL->SQL candidate pairs for one table.

    Pairs are *candidates*: callers must execution-validate them before use.
    """
    info = _classify(columns)
    names = info["names"]
    pk = info["pk"]
    human = _humanize(table)
    desc = f"Generated from the built app's '{table}' table"
    pairs: list[dict] = [
        {"question": f"List all {human}", "sql": f"SELECT * FROM {table}"},
        {"question": f"Show the first 10 {human}",
         "sql": f"SELECT * FROM {table} LIMIT 10"},
        {"question": f"How many {human} are there",
         "sql": f"SELECT COUNT(*) FROM {table}"},
    ]
    if pk:
        pairs.append({
            "question": f"Show the most recent {human}",
            "sql": f"SELECT * FROM {table} ORDER BY {pk} DESC LIMIT 10",
        })
        pairs.append({
            "question": f"Find the {human} with {pk} 1",
            "sql": f"SELECT * FROM {table} WHERE {pk} = 1",
        })
    visible = [c for c in names if c != pk][:6]
    if visible:
        col_list = ", ".join(visible)
        pairs.append({
            "question": f"Show {col_list} for each {human}",
            "sql": f"SELECT {col_list} FROM {table}",
        })
    for tc in info["text_cols"][:2]:
        pairs.append({
            "question": f"List distinct {tc} values in {human}",
            "sql": f"SELECT DISTINCT {tc} FROM {table}",
        })
        pairs.append({
            "question": f"Count {human} grouped by {tc}",
            "sql": f"SELECT {tc}, COUNT(*) FROM {table} GROUP BY {tc}",
        })
    for nc in info["num_cols"][:2]:
        pairs.append({
            "question": f"What is the total {nc} across {human}",
            "sql": f"SELECT SUM({nc}) FROM {table}",
        })
        pairs.append({
            "question": f"What is the average {nc} of {human}",
            "sql": f"SELECT AVG({nc}) FROM {table}",
        })
    for p in pairs:
        p["question"] = _fold_question(p["question"], desc)
        p["description"] = desc
    return pairs


def _pairs_from_transcript(build_result: Any) -> list[dict]:
    """Best-effort NL->SQL extraction from the build transcript/decisions."""
    if not isinstance(build_result, dict):
        return []
    pairs: list[dict] = []
    blobs: list[str] = []
    for item in build_result.get("transcript") or []:
        ev = item.get("event") if isinstance(item, dict) else None
        if isinstance(ev, dict):
            for key in ("text", "detail"):
                v = ev.get(key)
                if isinstance(v, str) and v.strip():
                    blobs.append(v)
    for blob in blobs:
        for m in re.finditer(
            r"```(?:sql)?\s*(?P<sql>SELECT\b.*?)```", blob,
            re.IGNORECASE | re.DOTALL,
        ):
            sql = m.group("sql").strip().rstrip(";")
            if sql:
                pairs.append({
                    "question": _fold_question("Run the query produced during the build",
                                               "build transcript"),
                    "sql": sql,
                    "description": "build transcript",
                })
    return pairs


def _pairs_from_codebase_profile(profile: Any) -> list[dict]:
    """NL->SQL seeds from a codebase profile's discovered DB tables."""
    if not profile:
        return []
    data = profile.as_dict() if hasattr(profile, "as_dict") else profile
    if not isinstance(data, dict):
        return []
    pairs: list[dict] = []
    for tbl in data.get("db_tables") or []:
        name = tbl if isinstance(tbl, str) else (tbl.get("name") if isinstance(tbl, dict) else "")
        if not name:
            continue
        pairs.append({
            "question": _fold_question(f"List all {_humanize(name)}",
                                       "discovered in the codebase"),
            "sql": f"SELECT * FROM {name}",
            "description": f"Table '{name}' discovered in the codebase",
        })
    return pairs


# ── execution validators ──────────────────────────────────────────────────────
def build_sqlite_executor(workspace: str | Path) -> Callable[[str], tuple[Any, str]] | None:
    """Materialize the generated ``src/db/schema.sql`` into a throwaway in-memory
    SQLite database and return an executor compatible with :func:`check_sql`.

    Returns ``None`` when no generated schema is available.
    """
    ws = Path(workspace) if workspace else None
    if ws is None:
        return None
    schema_path = ws / "src" / "db" / "schema.sql"
    ddl = ""
    if schema_path.exists():
        try:
            ddl = schema_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            ddl = ""
    if not ddl.strip():
        return None
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    try:
        conn.executescript(ddl)
        conn.commit()
    except sqlite3.Error:
        conn.close()
        return None

    def _executor(sql: str) -> tuple[Any, str]:
        try:
            conn.execute(sql)
            return None, ""
        except sqlite3.Error as exc:
            return None, str(exc)

    return _executor


def _validate(
    pairs: list[dict],
    *,
    core: Any,
    connection: str,
    db_type: str,
    executor: Callable[[str], tuple[Any, str]] | None,
) -> tuple[list[dict], int]:
    """Keep only pairs whose SQL parses and dry-runs (EXPLAIN + LIMIT 0)."""
    kept: list[dict] = []
    rejected = 0
    seen: set[str] = set()
    for p in pairs:
        sql = (p.get("sql") or "").strip()
        if not sql:
            continue
        chk = check_sql(
            sql, db_type=db_type, core=core, connection=connection,
            explain=True, limit_zero=True, executor=executor,
        )
        if not chk.get("valid"):
            rejected += 1
            continue
        norm = chk.get("normalized") or sql
        key = norm.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        kept.append({**p, "sql": norm})
    return kept, rejected


# ── public API ─────────────────────────────────────────────────────────────────
def collect_build_corpus(
    workspace: str | Path = "",
    *,
    insight: Any = None,
    build_result: Any = None,
    codebase_profile: Any = None,
    connection: str = "",
    core: Any = None,
    db_type: str | None = None,
    validate: bool = True,
    max_pairs: int = 600,
) -> dict[str, Any]:
    """Build an execution-validated NL->SQL corpus from app-build data.

    Returns ``{"pairs": [...], "stats": {...}}``. Validation target:

    * a live ``connection`` (via ``core``) when available — used for
      ``from_database`` builds;
    * otherwise a throwaway SQLite built from the generated schema — used for
      ``from_scratch`` / ``from_codebase`` builds.
    """
    ws = Path(workspace) if workspace else None
    candidates: list[dict] = []

    # 1) Schema-grounded generation (the most accurate, execution-checkable seed).
    schema_tables: dict[str, list[tuple[str, str]]] = {}
    if ws is not None:
        schema_path = ws / "src" / "db" / "schema.sql"
        if schema_path.exists():
            try:
                schema_tables = parse_schema_sql(
                    schema_path.read_text(encoding="utf-8", errors="replace"))
            except OSError:
                schema_tables = {}
    if not schema_tables:
        schema_tables = _columns_from_insight(insight)
    for table, cols in schema_tables.items():
        candidates.extend(grounded_pairs_for_table(table, cols))

    # 2) Existing build/insight/workspace/capture/rag/codebase/transcript seeds.
    candidates.extend(_pairs_from_insight(insight))
    if ws is not None:
        candidates.extend(_pairs_from_workspace(ws))
    candidates.extend(_pairs_from_transcript(build_result))
    candidates.extend(_pairs_from_codebase_profile(codebase_profile))
    if connection:
        candidates.extend(_pairs_from_capture(connection))
        candidates.extend(_pairs_from_rag(connection))

    # 3) Choose a validation target: live connection or generated SQLite.
    use_connection = bool(connection and core is not None)
    executor = None
    eff_db_type = (db_type or "").strip()
    if not use_connection:
        executor = build_sqlite_executor(ws) if ws is not None else None
        if executor is not None:
            eff_db_type = "sqlite"

    pre_dedup = _dedupe_pairs(candidates, db_type=eff_db_type or None)

    rejected = 0
    if validate and (use_connection or executor is not None):
        pairs, rejected = _validate(
            pre_dedup,
            core=core if use_connection else None,
            connection=connection if use_connection else "",
            db_type=eff_db_type,
            executor=executor,
        )
    else:
        pairs = pre_dedup

    if max_pairs and len(pairs) > max_pairs:
        pairs = pairs[:max_pairs]

    stats = {
        "tables": len(schema_tables),
        "candidates": len(pre_dedup),
        "validated": len(pairs),
        "rejected": rejected,
        "validation": (
            "connection" if use_connection
            else "generated_sqlite" if executor is not None
            else "parse_only"
        ),
        "connection": connection,
    }
    return {"pairs": pairs, "stats": stats}
