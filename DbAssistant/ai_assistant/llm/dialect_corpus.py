"""Multi-dialect NL->SQL corpus builder.

Seeds catalog and object templates for *every* supported SQL dialect while
using object names from the connected database.  The connected dialect is
live-validated; other dialects are parse-validated via sqlglot so a model
trained on one connection still learns correct syntax for all dialects.
"""

from __future__ import annotations

from typing import Any

from ai_assistant.llm.dataset import normalize_db_type, tag_question
from ai_assistant.llm.query_templates import (
    all_catalog_pairs,
    mongo_catalog_pairs,
    normalize_template_mode,
    render_object_templates,
    render_object_templates_delex,
    template_explanation,
)
from ai_assistant.llm.sql_check import check_sql


def _canonical_sql_db_type(db_type: str) -> str:
    """Map profile db_type strings to CATALOG_TEMPLATES keys."""
    key = normalize_db_type(db_type)
    mapping = {
        "postgresql": "PostgreSQL",
        "mysql": "MySQL",
        "mariadb": "MariaDB",
        "sqlite": "SQLite",
        "sqlserver": "SQLServer",
        "oracle": "Oracle",
    }
    return mapping.get(key, db_type)


def collect_multi_dialect_pairs(
    core: Any,
    connection: str,
    *,
    connected_db_type: str = "",
    sample_limit: int = 5,
    max_tables: int = 40,
    include_mongo: bool = True,
    conn_by_dbtype: dict[str, str] | None = None,
    template_mode: str = "concrete",
) -> dict[str, Any]:
    """Build dialect-tagged training pairs for all SQL dialects (+ Mongo).

    ``conn_by_dbtype`` maps a normalized db_type -> a live connection of that
    type. When a dialect has a matching live connection, its SQL is verified
    with a real dry-run (EXPLAIN / LIMIT 0) against that connection; otherwise
    it is parse-validated via sqlglot. This lets selecting several connections
    raise syntax accuracy for every dialect that has a live target.
    """
    from ai_assistant.llm.db_query_miner import (
        DbTrainingMiner,
        get_dialect_for_db_type,
        supported_sql_db_types,
    )

    # Always include the schema-source connection in the validation routing so
    # the connected dialect is live-validated even when no explicit map passed.
    routing: dict[str, str] = dict(conn_by_dbtype or {})
    if connection and connected_db_type:
        routing.setdefault(normalize_db_type(connected_db_type), connection)

    mode = normalize_template_mode(template_mode)
    emit_concrete = mode in ("concrete", "both")
    emit_delex = mode in ("placeholder", "both")

    miner = DbTrainingMiner(
        core, connection, sample_limit=sample_limit, max_tables=max_tables, validate=False,
    )
    connected = _canonical_sql_db_type(connected_db_type or miner._db_type or "")  # noqa: SLF001
    database = miner._detect_dialect().database if miner._detect_dialect() else ""  # noqa: SLF001

    table_infos = []
    dialect = miner._detect_dialect()  # noqa: SLF001
    if dialect is not None:
        for t in miner._tables():  # noqa: SLF001
            info = miner._table_info(t)  # noqa: SLF001
            if info.columns:
                table_infos.append(info)

    pairs: list[dict] = []
    stats = {
        "connected_db_type": connected,
        "dialects": 0,
        "catalog": 0,
        "object": 0,
        "mongo": 0,
        "rejected": 0,
        "live_validated": 0,
    }

    def _accept(
        question: str,
        sql: str,
        *,
        target_db_type: str,
        category: str = "",
        val_conn: str = "",
        delexicalized: bool = False,
    ) -> None:
        live_for_pair = bool(val_conn and core is not None and not delexicalized)
        tagged_q = tag_question(question, target_db_type)
        chk = check_sql(
            sql,
            db_type=target_db_type,
            core=core if live_for_pair else None,
            connection=val_conn if live_for_pair else "",
            explain=live_for_pair,
            limit_zero=live_for_pair,
        )
        # With a live target, demand executable; otherwise accept parse-clean.
        if live_for_pair:
            if not chk.get("valid"):
                stats["rejected"] += 1
                return
            stats["live_validated"] += 1
        elif not chk.get("parse_ok") and not chk.get("valid"):
            stats["rejected"] += 1
            return
        norm_sql = chk.get("normalized") or sql
        pairs.append({
            "question": tagged_q,
            "sql": norm_sql,
            "db_type": normalize_db_type(target_db_type),
            "description": category,
            "explanation": template_explanation(
                question, norm_sql, db_type=target_db_type, category=category,
            ),
            "delexicalized": delexicalized,
        })
        if category == "catalog" or (category or "").endswith("catalog"):
            stats["catalog"] += 1
        else:
            stats["object"] += 1

    for raw_db in supported_sql_db_types():
        canon = _canonical_sql_db_type(raw_db)
        target_tag = normalize_db_type(raw_db)
        d = get_dialect_for_db_type(canon, database)
        if d is None:
            continue
        stats["dialects"] += 1
        val_conn = routing.get(target_tag, "")

        for cat in all_catalog_pairs():
            if _canonical_sql_db_type(cat.get("db_type", "")) != canon:
                continue
            _accept(
                cat["question"],
                cat["sql"],
                target_db_type=target_tag,
                category=cat.get("category", "catalog"),
                val_conn=val_conn,
            )

        for info in table_infos:
            if emit_concrete:
                for rendered in render_object_templates(info, d, limit=sample_limit):
                    _accept(
                        rendered["question"],
                        rendered["sql"],
                        target_db_type=target_tag,
                        category=rendered.get("category", "object"),
                        val_conn=val_conn,
                        delexicalized=False,
                    )
            if emit_delex:
                for rendered in render_object_templates_delex(info, d, limit=sample_limit):
                    _accept(
                        rendered["question"],
                        rendered["sql"],
                        target_db_type=target_tag,
                        category=rendered.get("category", "object"),
                        val_conn=val_conn,
                        delexicalized=True,
                    )

    if include_mongo:
        mongo_conn = routing.get("mongodb") or routing.get("documentdb") or ""
        for cat in mongo_catalog_pairs():
            tagged_q = tag_question(cat["question"], "mongodb")
            if mongo_conn and core is not None:
                chk = check_sql(
                    cat["sql"], db_type="mongodb", core=core,
                    connection=mongo_conn, explain=True, limit_zero=True,
                )
                if not chk.get("valid") and not chk.get("parse_ok"):
                    stats["rejected"] += 1
                    continue
            pairs.append({
                "question": tagged_q,
                "sql": cat["sql"],
                "db_type": "mongodb",
                "description": cat.get("category", "catalog"),
                "explanation": template_explanation(
                    cat["question"], cat["sql"], db_type="mongodb", category="catalog",
                ),
            })
            stats["mongo"] += 1

    return {"ok": bool(pairs), "pairs": pairs, "stats": stats}
