"""AI-driven enrichment of the reusable NL->SQL query-template library.

Given the selected AI backend (as a text generator) and a set of generic query
*intents*, this asks the backend to produce one reusable, placeholder-
parameterised SQL template per dialect. Accepted templates are persisted to
:mod:`ai_assistant.llm.template_store` and merged into the training corpus on
the next harvest/train, so a locally-trained model can answer the same question
across every connection of a dialect by substituting the real object names from
the connected database's live schema into the template placeholders.

Backend access is injected as a callable (``generate_text_fn``) so the
dependency direction stays ai_query -> ai_assistant (like harvest_service).
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable

from ai_assistant.llm.dataset import normalize_db_type
from ai_assistant.llm.query_templates import supported_sql_db_types
from ai_assistant.llm.template_store import ALLOWED_OBJECT_PLACEHOLDERS

GenerateTextFn = Callable[[str], str]

_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)
_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

# "Our set of questions": generic query intents covering catalog/metadata and
# per-object analytics. The AI turns each into a dialect-specific template.
DEFAULT_INTENTS: list[dict] = [
    {"intent": "List all tables in the database", "scope": "catalog"},
    {"intent": "List all columns with their data types", "scope": "catalog"},
    {"intent": "Count how many tables exist", "scope": "catalog"},
    {"intent": "List all views", "scope": "catalog"},
    {"intent": "List all indexes", "scope": "catalog"},
    {"intent": "List all foreign-key relationships", "scope": "catalog"},
    {"intent": "List all primary-key columns", "scope": "catalog"},
    {"intent": "Show estimated row counts per table", "scope": "catalog"},
    {"intent": "Show a sample of rows from a table", "scope": "object"},
    {"intent": "Count the rows in a table", "scope": "object"},
    {"intent": "Show min, max, average and sum of a numeric column in a table",
     "scope": "object"},
    {"intent": "Count rows grouped by a text column in a table", "scope": "object"},
    {"intent": "List the top rows of a table ordered by a numeric column",
     "scope": "object"},
    {"intent": "Count distinct values of a text column in a table", "scope": "object"},
    {"intent": "Total of a numeric column by a text column in a table",
     "scope": "object"},
    {"intent": "Count rows where the first column is NULL in a table",
     "scope": "object"},
]

# Dummy identifiers used to parse-validate a placeholder template skeleton.
_DUMMY_VALUES: dict[str, str] = {
    "table": "t1", "table_label": "t1", "limit": "10",
    "bounded_select": "SELECT * FROM t1 LIMIT 10",
    "limit_clause": "LIMIT 10", "limit_50_clause": "LIMIT 50",
    "col_list": "c1, c2", "col_list_label": "c1, c2",
    "text_col": "c1", "text_col_q": "c1",
    "num_col": "c2", "num_col_q": "c2",
    "first_col": "c1", "first_col_q": "c1",
}

_OBJECT_PLACEHOLDER_HELP = (
    "Parameterise object names using ONLY these placeholders (verbatim, with "
    "braces): {table} (the table; always reference the table this way), "
    "{col_list} (a comma list of columns), {text_col_q} (a text column), "
    "{num_col_q} (a numeric column), {first_col_q} (the first column), "
    "{limit} (a row limit integer), {limit_clause} (a dialect LIMIT clause), "
    "{limit_50_clause} (a LIMIT 50 clause). Do NOT invent real table or column "
    "names. The template MUST reference {table}."
)
_CATALOG_PLACEHOLDER_HELP = (
    "Use the dialect's system catalog / information_schema views. Do NOT use any "
    "{placeholder} tokens — this query must be runnable as-is."
)


def _enrich_prompt(db_type: str, intent: str, scope: str) -> str:
    help_text = (
        _OBJECT_PLACEHOLDER_HELP if scope == "object" else _CATALOG_PLACEHOLDER_HELP
    )
    return (
        f"You are extending a library of reusable {db_type} SQL templates for an "
        f"NL->SQL model.\n"
        f"Produce ONE reusable, READ-ONLY {db_type} SQL query for this intent.\n\n"
        f"INTENT: {intent}\n"
        f"SCOPE: {scope}\n\n"
        f"{help_text}\n\n"
        f"Rules:\n"
        f"- The SQL must be valid {db_type} syntax and strictly read-only.\n"
        f"- Output ONLY a JSON object with keys: sql, category, explanation.\n\n"
        f'Output (JSON): {{"sql": "...", "category": "...", "explanation": "..."}}'
    )


def _parse_template_response(raw: str) -> dict | None:
    text = (raw or "").strip()
    if not text:
        return None
    fence = _CODE_FENCE_RE.search(text)
    blob = fence.group(1) if fence else text
    m = _JSON_OBJ_RE.search(blob)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    sql = (obj.get("sql") or "").strip()
    if not sql:
        return None
    return {
        "sql": sql,
        "category": (obj.get("category") or "").strip(),
        "explanation": (obj.get("explanation") or "").strip(),
    }


def _validate_template(
    sql: str,
    *,
    db_type: str,
    scope: str,
    val_conn: str = "",
    core: Any = None,
) -> dict:
    """Validate a produced template. Returns ``{"ok", "reason"}``.

    Catalog templates are literal SQL (parse-checked, live dry-run when a
    matching connection exists). Object templates are skeletons whose
    placeholders are dummy-substituted and parse-checked for the dialect; their
    live execution validation happens later when the corpus renders them with
    real object names.
    """
    from ai_assistant.llm.sql_check import check_sql

    placeholders = set(_PLACEHOLDER_RE.findall(sql))

    if scope == "object":
        unknown = placeholders - set(ALLOWED_OBJECT_PLACEHOLDERS)
        if unknown:
            return {"ok": False, "reason": f"unknown placeholders: {sorted(unknown)}"}
        if "table" not in placeholders:
            return {"ok": False, "reason": "object template must reference {table}"}
        try:
            probe = sql.format(**_DUMMY_VALUES)
        except (KeyError, IndexError, ValueError) as exc:
            return {"ok": False, "reason": f"render failed: {exc}"}
        chk = check_sql(probe, db_type=db_type)
        if not chk.get("parse_ok") and not chk.get("valid"):
            return {"ok": False, "reason": chk.get("error") or "parse failed"}
        return {"ok": True, "reason": ""}

    # catalog scope
    if placeholders:
        return {"ok": False, "reason": "catalog template must not use placeholders"}
    live = bool(val_conn and core is not None)
    chk = check_sql(
        sql, db_type=db_type,
        core=core if live else None, connection=val_conn if live else "",
        explain=live, limit_zero=live,
    )
    if live:
        if not chk.get("valid"):
            return {"ok": False, "reason": chk.get("error") or "dry-run failed"}
    elif not chk.get("parse_ok") and not chk.get("valid"):
        return {"ok": False, "reason": chk.get("error") or "parse failed"}
    return {"ok": True, "reason": "", "normalized": chk.get("normalized") or sql}


def enrich_templates(
    *,
    generate_text_fn: GenerateTextFn,
    questions: list[dict] | list[str] | None = None,
    db_types: list[str] | None = None,
    conn_by_dbtype: dict[str, str] | None = None,
    core: Any = None,
    limit_per_type: int = 0,
    persist: bool = True,
    on_progress: Any = None,
    should_stop: Any = None,
) -> dict[str, Any]:
    """Ask the backend for reusable templates per dialect; validate; persist.

    ``questions`` accepts the built-in intent dicts (``{"intent","scope"}``) or
    plain strings (treated as object-scope intents). When omitted, the curated
    :data:`DEFAULT_INTENTS` set is used.
    """
    if generate_text_fn is None:
        return {"ok": False, "error": "No AI backend text generator available."}

    intents = _normalize_intents(questions)
    if not intents:
        return {"ok": False, "error": "No intents to enrich."}

    targets = [d for d in (db_types or supported_sql_db_types()) if str(d).strip()]
    if not targets:
        return {"ok": False, "error": "No database types to enrich."}

    stop = should_stop if callable(should_stop) else (lambda: False)
    routing = dict(conn_by_dbtype or {})
    cap = max(0, int(limit_per_type or 0))

    catalog_out: dict[str, list[dict]] = {}
    object_out: list[dict] = []
    per_type: dict[str, dict] = {}
    accepted = 0
    rejected = 0

    for raw_db in targets:
        if stop():
            break
        tag = normalize_db_type(raw_db)
        per_type[tag] = {"accepted": 0, "rejected": 0}
        val_conn = routing.get(tag, "")
        used = 0
        for spec in intents:
            if stop():
                break
            if cap and used >= cap:
                break
            used += 1
            intent, scope = spec["intent"], spec["scope"]
            if on_progress:
                on_progress({"type": "enrich_template", "status": "asking",
                             "db_type": tag, "intent": intent})
            try:
                raw = generate_text_fn(_enrich_prompt(raw_db, intent, scope)) or ""
            except Exception as exc:  # noqa: BLE001
                rejected += 1
                per_type[tag]["rejected"] += 1
                if on_progress:
                    on_progress({"type": "enrich_template", "status": "error",
                                 "db_type": tag, "intent": intent, "error": str(exc)})
                continue
            parsed = _parse_template_response(raw)
            if not parsed:
                rejected += 1
                per_type[tag]["rejected"] += 1
                continue
            verdict = _validate_template(
                parsed["sql"], db_type=tag, scope=scope,
                val_conn=val_conn, core=core,
            )
            if not verdict.get("ok"):
                rejected += 1
                per_type[tag]["rejected"] += 1
                if on_progress:
                    on_progress({"type": "enrich_template", "status": "rejected",
                                 "db_type": tag, "intent": intent,
                                 "reason": verdict.get("reason")})
                continue
            sql = verdict.get("normalized") or parsed["sql"]
            category = parsed["category"] or ("catalog" if scope == "catalog" else "object")
            if scope == "catalog":
                catalog_out.setdefault(tag, []).append({
                    "question": intent, "sql": sql, "category": category,
                })
            else:
                object_out.append({
                    "question": intent, "sql": sql, "category": category,
                    "db_types": ["*"],
                })
            accepted += 1
            per_type[tag]["accepted"] += 1
            if on_progress:
                on_progress({"type": "enrich_template", "status": "accepted",
                             "db_type": tag, "intent": intent})

    store_summary: dict[str, Any] = {}
    if persist and (catalog_out or object_out):
        from ai_assistant.llm import template_store

        store_summary = template_store.add(catalog=catalog_out, objects=object_out)

    return {
        "ok": accepted > 0,
        "accepted": accepted,
        "rejected": rejected,
        "db_types": [normalize_db_type(d) for d in targets],
        "per_type": per_type,
        "catalog": catalog_out,
        "object": object_out,
        "store": store_summary,
        "error": None if accepted else "No templates were accepted.",
    }


def _normalize_intents(questions: Any) -> list[dict]:
    if not questions:
        return list(DEFAULT_INTENTS)
    out: list[dict] = []
    for q in questions:
        if isinstance(q, dict):
            intent = (q.get("intent") or q.get("question") or "").strip()
            scope = (q.get("scope") or "object").strip().lower()
            if intent:
                out.append({"intent": intent,
                            "scope": "catalog" if scope == "catalog" else "object"})
        elif isinstance(q, str) and q.strip():
            out.append({"intent": q.strip(), "scope": "object"})
    return out
