"""Resolve PH_* placeholder tokens in locally-generated SQL using live schema.

When a placeholder-trained local LLM emits SQL like ``SELECT COUNT(*) FROM
PH_TABLE``, this module binds each token to real object names from the
connected database, validates with a dry-run, and backtracks across candidate
tables/columns when the first binding fails.
"""

from __future__ import annotations

import difflib
import re
from typing import Any, Callable

from ai_assistant.llm.query_templates import ALL_PH_TOKENS, PLACEHOLDER_TOKENS

PH_TOKEN_RE = re.compile(
    r"\b(" + "|".join(sorted(ALL_PH_TOKENS, key=len, reverse=True)) + r")\b"
)

# Minimum confidence gap between top-2 table candidates to avoid ambiguity.
_AMBIGUITY_GAP = 0.12

AiPickFn = Callable[[list[dict], str, str], dict | None]


def has_placeholders(sql: str) -> bool:
    return bool(PH_TOKEN_RE.search(sql or ""))


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (name or "").lower())


def _plural_variants(name: str) -> set[str]:
    base = _normalize_name(name)
    out = {base}
    if base.endswith("s") and len(base) > 2:
        out.add(base[:-1])
    elif base:
        out.add(base + "s")
    if base.endswith("ies") and len(base) > 3:
        out.add(base[:-3] + "y")
    elif base.endswith("y") and len(base) > 1:
        out.add(base[:-1] + "ies")
    return out


def _question_tokens(question: str) -> set[str]:
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", (question or "").lower())
    out: set[str] = set()
    for w in words:
        out |= _plural_variants(w)
    return out


def _score_table(label: str, question: str) -> float:
    """Rank how well *label* matches entities mentioned in *question*."""
    qtok = _question_tokens(question)
    label_norm = _normalize_name(label)
    if not label_norm:
        return 0.0
    label_vars = _plural_variants(label)
    if label_norm in qtok or label_vars & qtok:
        return 1.0
    best = 0.0
    for q in qtok:
        if len(q) < 3:
            continue
        ratio = difflib.SequenceMatcher(None, label_norm, q).ratio()
        if label_norm in q or q in label_norm:
            ratio = max(ratio, 0.85)
        best = max(best, ratio)
    return best


class _ManagerAsCore:
    """Minimal core façade so :class:`DbTrainingMiner` can use a live manager."""

    def __init__(self, mgr: Any, connection: str) -> None:
        self._mgr = mgr
        self._connection = connection

    def get_connection_profile(self, name: str) -> dict:
        return {"db_type": getattr(self._mgr, "db_type", "") or ""}

    def get_objects(self, name: str, obj_type: str = "tables") -> list:
        if obj_type != "tables":
            return []
        try:
            from common.database_registry import DatabaseRegistry

            return DatabaseRegistry.execute_operation(self._mgr, "list_tables") or []
        except Exception:
            return []

    def execute(self, name: str, sql: str) -> dict:
        try:
            from common.database_registry import DatabaseRegistry

            return DatabaseRegistry.execute_operation(self._mgr, "execute", sql) or {}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def get_manager(self, name: str):
        return self._mgr

    def get_table_schema(self, name: str, table: str) -> dict:
        try:
            from common.database_registry import DatabaseRegistry

            return DatabaseRegistry.execute_operation(
                self._mgr, "describe_table", table) or {}
        except Exception:
            return {}


def _resolve_core(core: Any, db_manager: Any, connection: str) -> Any:
    if core is not None:
        return core
    if db_manager is not None and connection:
        return _ManagerAsCore(db_manager, connection)
    return None


def _load_table_catalog(core: Any, connection: str, *, max_tables: int = 40) -> list[dict]:
    from ai_assistant.llm.db_query_miner import DbTrainingMiner

    miner = DbTrainingMiner(core, connection, max_tables=max_tables, validate=False)
    dialect = miner._detect_dialect()  # noqa: SLF001
    if dialect is None:
        return []
    catalog: list[dict] = []
    for t in miner._tables():  # noqa: SLF001
        info = miner._table_info(t)  # noqa: SLF001
        if not info.columns:
            continue
        catalog.append({
            "info": info,
            "name": info.name,
            "label": info.name.split(".")[-1],
            "dialect": dialect,
        })
    return catalog


def _build_mapping(entry: dict, question: str, *, limit: int) -> dict[str, str]:
    from ai_assistant.llm.query_templates import build_template_values

    info = entry["info"]
    dialect = entry["dialect"]
    values = build_template_values(info, dialect, limit=limit) or {}
    mapping: dict[str, str] = {}
    for key, ph in PLACEHOLDER_TOKENS.items():
        if key in values and ph in ALL_PH_TOKENS:
            mapping[ph] = str(values[key])
    return mapping


def _extract_limit(question: str, default: int = 10) -> int:
    m = re.search(r"\b(?:top|first|limit)\s+(\d+)\b", (question or "").lower())
    if m:
        return max(1, min(int(m.group(1)), 1000))
    m = re.search(r"\b(\d+)\s+(?:rows?|records?)\b", (question or "").lower())
    if m:
        return max(1, min(int(m.group(1)), 1000))
    return default


def _apply_mapping(sql: str, mapping: dict[str, str]) -> str:
    out = sql or ""

    def _repl(m: re.Match) -> str:
        return mapping.get(m.group(1), m.group(1))

    return PH_TOKEN_RE.sub(_repl, out)


def _rank_tables(catalog: list[dict], question: str) -> list[tuple[float, dict]]:
    scored = [( _score_table(e["label"], question), e) for e in catalog]
    scored.sort(key=lambda x: (-x[0], x[1]["label"]))
    return scored


def resolve(
    sql: str,
    question: str,
    *,
    core: Any = None,
    connection: str = "",
    db_type: str = "",
    executor: Any = None,
    db_manager: Any = None,
    ai_pick_fn: AiPickFn | None = None,
    max_candidates: int = 8,
) -> dict[str, Any]:
    """Bind PH_* tokens and return concrete SQL plus metadata."""
    from ai_assistant.llm.sql_check import check_sql

    if not has_placeholders(sql):
        return {
            "ok": True, "sql": sql, "mappings": {}, "confidence": 1.0,
            "ambiguous": False, "candidates": [], "resolved": False,
        }

    resolved_core = _resolve_core(core, db_manager, connection)
    if not resolved_core or not connection:
        return {
            "ok": False, "sql": sql, "mappings": {}, "confidence": 0.0,
            "ambiguous": False, "candidates": [],
            "error": "A live connection is required to resolve placeholders.",
            "resolved": False,
        }

    catalog = _load_table_catalog(resolved_core, connection)
    if not catalog:
        return {
            "ok": False, "sql": sql, "mappings": {}, "confidence": 0.0,
            "ambiguous": False, "candidates": [],
            "error": "Could not read schema to resolve placeholders.",
            "resolved": False,
        }

    ranked = _rank_tables(catalog, question)[:max_candidates]
    if not ranked:
        return {
            "ok": False, "sql": sql, "mappings": {}, "confidence": 0.0,
            "ambiguous": False, "candidates": [],
            "error": "No tables found for placeholder resolution.",
            "resolved": False,
        }

    top_score = ranked[0][0]
    second_score = ranked[1][0] if len(ranked) > 1 else 0.0
    ambiguous = (
        top_score < 0.55
        or (len(ranked) > 1 and (top_score - second_score) < _AMBIGUITY_GAP)
    )

    candidate_payloads: list[dict] = []
    limit = _extract_limit(question)
    for score, entry in ranked:
        mapping = _build_mapping(entry, question, limit=limit)
        concrete = _apply_mapping(sql, mapping)
        candidate_payloads.append({
            "table": entry["name"],
            "label": entry["label"],
            "score": score,
            "mapping": mapping,
            "sql": concrete,
        })

    if ambiguous and ai_pick_fn is not None:
        picked = ai_pick_fn(candidate_payloads, question, sql)
        if picked and picked.get("sql"):
            chk = check_sql(
                picked["sql"], db_type=db_type, core=resolved_core,
                connection=connection, executor=executor,
                explain=True, limit_zero=True,
            )
            if chk.get("valid"):
                return {
                    "ok": True,
                    "sql": chk.get("normalized") or picked["sql"],
                    "mappings": picked.get("mapping") or {},
                    "confidence": picked.get("score", top_score),
                    "ambiguous": False,
                    "candidates": candidate_payloads,
                    "resolved": True,
                    "resolution": "ai_fallback",
                }

    live = bool(connection and resolved_core)
    for cand in candidate_payloads:
        concrete = cand["sql"]
        chk = check_sql(
            concrete,
            db_type=db_type,
            core=resolved_core if live else None,
            connection=connection if live else "",
            executor=executor,
            explain=live,
            limit_zero=live,
        )
        if chk.get("valid") or (not live and chk.get("parse_ok")):
            return {
                "ok": True,
                "sql": chk.get("normalized") or concrete,
                "mappings": cand["mapping"],
                "confidence": cand["score"],
                "ambiguous": ambiguous and cand is candidate_payloads[0],
                "candidates": candidate_payloads,
                "resolved": True,
                "resolution": "deterministic",
            }

    if ambiguous:
        return {
            "ok": False,
            "sql": sql,
            "mappings": {},
            "confidence": top_score,
            "ambiguous": True,
            "candidates": candidate_payloads,
            "error": "Could not confidently map placeholders to schema objects.",
            "resolved": False,
        }

    return {
        "ok": False,
        "sql": sql,
        "mappings": candidate_payloads[0]["mapping"] if candidate_payloads else {},
        "confidence": top_score,
        "ambiguous": False,
        "candidates": candidate_payloads,
        "error": "Placeholder resolution produced SQL that failed validation.",
        "resolved": False,
    }
