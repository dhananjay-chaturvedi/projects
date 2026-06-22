"""Curated NL->SQL seed corpus loader and renderer.

Loads ``data/seed_problems.yaml`` — a hand-curated set of problems that each map
many paraphrase prompts onto one canonical intent, tagged by difficulty. Two
problem modes:

* ``template`` — the ``sql`` skeleton is rendered against the *real* schema using
  the same placeholder scheme as :mod:`ai_assistant.llm.query_templates`, then
  live-validated. Every paraphrase shares the one validated SQL, so each phrasing
  becomes an exact-recall training pair.
* ``generate`` — no SQL is templated; the canonical prompts are returned for the
  harvester to send to the backend AI agent (Cursor) for grounding.

The renderer reuses :class:`ai_assistant.llm.db_query_miner.DbTrainingMiner` for
dialect detection, table discovery, schema introspection, and read-only
execution so it stays in lockstep with the miner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_CORPUS_PATH = Path(__file__).parent / "data" / "seed_problems.yaml"


@dataclass(frozen=True)
class SeedProblem:
    id: str
    complexity: str = "basic"          # basic | advanced | complex
    category: str = ""
    mode: str = "template"             # template | generate
    sql: str = ""                      # placeholder skeleton (template mode)
    prompts: tuple[str, ...] = ()
    followups: tuple[str, ...] = ()
    requires: tuple[str, ...] = ()     # e.g. ("text_col",), ("num_col", "text_col")
    db_types: tuple[str, ...] = ("*",)


def _coerce_problem(raw: dict) -> SeedProblem | None:
    if not isinstance(raw, dict):
        return None
    pid = str(raw.get("id") or "").strip()
    prompts = tuple(str(p).strip() for p in (raw.get("prompts") or []) if str(p).strip())
    if not pid or not prompts:
        return None
    return SeedProblem(
        id=pid,
        complexity=str(raw.get("complexity") or "basic").strip().lower(),
        category=str(raw.get("category") or "").strip(),
        mode=str(raw.get("mode") or "template").strip().lower(),
        sql=str(raw.get("sql") or "").strip(),
        prompts=prompts,
        followups=tuple(str(f).strip() for f in (raw.get("followups") or []) if str(f).strip()),
        requires=tuple(str(r).strip() for r in (raw.get("requires") or []) if str(r).strip()),
        db_types=tuple(str(d).strip() for d in (raw.get("db_types") or ["*"]) if str(d).strip()) or ("*",),
    )


def load_seed_problems(
    db_type: str | None = None,
    *,
    complexity: list[str] | None = None,
    path: str | Path | None = None,
) -> list[SeedProblem]:
    """Load curated problems, optionally filtered by db_type and complexity."""
    import yaml  # lazy: keeps PyYAML optional for callers that never use the corpus

    p = Path(path) if path else _CORPUS_PATH
    if not p.exists():
        return []
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    problems: list[SeedProblem] = []
    allow = {c.strip().lower() for c in (complexity or [])} or None
    for raw in (data.get("problems") or []):
        prob = _coerce_problem(raw)
        if prob is None:
            continue
        if db_type and prob.db_types != ("*",) and db_type not in prob.db_types:
            continue
        if allow and prob.complexity not in allow:
            continue
        problems.append(prob)
    return problems


def _safe_format(text: str, values: dict) -> str | None:
    """Format *text* with *values*; return None if a placeholder is missing."""
    try:
        return text.format(**values)
    except (KeyError, IndexError, ValueError):
        return None


def _requires_met(requires: tuple[str, ...], values: dict) -> bool:
    if "text_col" in requires and not values.get("_has_text_col"):
        return False
    if "num_col" in requires and not values.get("_has_num_col"):
        return False
    return True


def render_seed_pairs(
    core: Any,
    connection: str,
    *,
    db_type: str | None = None,
    sample_limit: int = 5,
    max_tables: int = 40,
    complexity: list[str] | None = None,
    validate: bool = True,
    template_mode: str = "concrete",
) -> dict[str, Any]:
    """Render the curated corpus against a live connection.

    Returns a dict with:
      * ``pairs`` — validated template-mode NL->SQL pairs (paraphrases share SQL)
      * ``generate_problems`` — generate-mode problems for the harvester to send
        to the backend, with placeholders best-effort filled from the schema
      * ``stats`` — coverage counters
    """
    from ai_assistant.llm.db_query_miner import DbTrainingMiner
    from ai_assistant.llm.query_templates import (
        build_delex_sql_values,
        build_template_values,
        normalize_template_mode,
    )
    from ai_assistant.llm.sql_check import check_sql

    mode = normalize_template_mode(template_mode)
    emit_concrete = mode in ("concrete", "both")
    emit_delex = mode in ("placeholder", "both")

    out: dict[str, Any] = {"ok": False, "pairs": [], "generate_problems": [], "stats": {}}
    if core is None or not connection:
        out["error"] = "core and connection required."
        return out

    miner = DbTrainingMiner(
        core, connection,
        sample_limit=sample_limit, max_tables=max_tables, validate=validate,
    )
    dialect = miner._detect_dialect()  # noqa: SLF001 - same package, intentional reuse
    resolved_db_type = db_type or miner._db_type or ""
    problems = load_seed_problems(resolved_db_type or None, complexity=complexity)
    if dialect is None:
        # Non-SQL engine (e.g. MongoDB): only generate-mode prompts are usable,
        # and those need a backend, so return them unrendered (grouped per
        # problem so all paraphrases can share one generated SQL).
        gen = [
            {"id": p.id, "prompts": list(p.prompts), "followups": list(p.followups),
             "category": p.category, "complexity": p.complexity}
            for p in problems if p.mode == "generate"
        ]
        out.update(ok=bool(gen), generate_problems=gen,
                   stats={"db_type": resolved_db_type, "tables": 0,
                          "template_pairs": 0,
                          "generate_prompts": sum(len(g["prompts"]) for g in gen)})
        return out

    table_infos = []
    for t in miner._tables():  # noqa: SLF001
        info = miner._table_info(t)  # noqa: SLF001
        vals = build_template_values(info, dialect, limit=sample_limit)
        if vals is not None:
            table_infos.append((info, vals))

    pairs: list[dict] = []
    rendered = 0
    validated = 0
    failed = 0
    template_problems = [p for p in problems if p.mode == "template" and p.sql]
    for prob in template_problems:
        for _info, values in table_infos:
            if not _requires_met(prob.requires, values):
                continue
            if emit_concrete:
                sql = _safe_format(prob.sql, values)
                if not sql:
                    continue
                sql = sql.strip()
                rendered += 1
                if validate:
                    ok, _err = miner._run(sql)  # noqa: SLF001
                    if not ok:
                        failed += 1
                        continue
                    validated += 1
                for prompt in prob.prompts:
                    q = _safe_format(prompt, values)
                    if not q:
                        continue
                    pairs.append({
                        "question": q.strip(),
                        "sql": sql,
                        "description": prob.category or prob.id,
                        "delexicalized": False,
                    })
            if emit_delex:
                delex_vals = build_delex_sql_values(values)
                sql = _safe_format(prob.sql, delex_vals)
                if not sql:
                    continue
                sql = sql.strip()
                rendered += 1
                db_type_tag = resolved_db_type or miner._db_type or ""  # noqa: SLF001
                chk = check_sql(sql, db_type=db_type_tag)
                if not chk.get("parse_ok") and not chk.get("valid"):
                    failed += 1
                    continue
                validated += 1
                for prompt in prob.prompts:
                    q = _safe_format(prompt, values)
                    if not q:
                        continue
                    pairs.append({
                        "question": q.strip(),
                        "sql": sql,
                        "description": prob.category or prob.id,
                        "delexicalized": True,
                    })

    # Generate-mode: fill placeholders best-effort from the first usable table,
    # grouping all paraphrases under one problem so the harvester can generate
    # SQL once (from the canonical prompt) and share it across every phrasing.
    rep_values = table_infos[0][1] if table_infos else {}
    gen_problems: list[dict] = []
    for prob in problems:
        if prob.mode != "generate":
            continue
        filled = []
        for prompt in prob.prompts:
            q = _safe_format(prompt, rep_values) if rep_values else prompt
            filled.append((q or prompt).strip())
        gen_problems.append({
            "id": prob.id,
            "prompts": filled,
            "followups": list(prob.followups),
            "category": prob.category,
            "complexity": prob.complexity,
        })

    out.update(
        ok=bool(pairs or gen_problems),
        pairs=pairs,
        generate_problems=gen_problems,
        stats={
            "db_type": resolved_db_type,
            "tables": len(table_infos),
            "template_problems": len(template_problems),
            "rendered": rendered,
            "validated": validated,
            "failed": failed,
            "template_pairs": len(pairs),
            "generate_problems": len(gen_problems),
            "generate_prompts": sum(len(g["prompts"]) for g in gen_problems),
        },
    )
    return out
