"""Reusable NL->SQL training query templates by database type.

Templates are skeletons: the miner fills them with real table/column names and
executes them against the selected connection before training on them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Identifier-style placeholder tokens for delexicalized (schema-agnostic) training.
# These are single WordTokenizer tokens (unlike ``{table}`` which splits into three).
PLACEHOLDER_TOKENS: dict[str, str] = {
    "table": "PH_TABLE",
    "bounded_select": "PH_BOUNDED_SELECT",
    "limit_clause": "PH_LIMIT_CLAUSE",
    "limit_50_clause": "PH_LIMIT_50_CLAUSE",
    "col_list": "PH_COLLIST",
    "text_col_q": "PH_TEXTCOL",
    "num_col_q": "PH_NUMCOL",
    "first_col_q": "PH_FIRSTCOL",
    "limit": "PH_LIMIT",
}

ALL_PH_TOKENS: frozenset[str] = frozenset(PLACEHOLDER_TOKENS.values())


def normalize_template_mode(mode: str | None) -> str:
    """Return ``concrete``, ``placeholder``, or ``both`` (default)."""
    m = (mode or "both").strip().lower()
    if m in ("concrete", "placeholder", "both"):
        return m
    return "both"


def build_delex_sql_values(values: dict) -> dict:
    """Copy *values* but replace SQL-bound keys with PH_* tokens."""
    out = dict(values)
    for key, ph in PLACEHOLDER_TOKENS.items():
        if key in out:
            out[key] = ph
    return out


@dataclass(frozen=True)
class QueryTemplate:
    id: str
    question: str
    sql: str
    db_types: tuple[str, ...] = ("*",)
    requires: tuple[str, ...] = ()
    complexity: str = "simple"
    scope: str = "object"  # catalog | object
    category: str = ""


CATALOG_TEMPLATES: dict[str, list[QueryTemplate]] = {
    "SQLite": [
        QueryTemplate("sqlite.tables", "List every table in the database",
                      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
                      scope="catalog", category="catalog"),
        QueryTemplate("sqlite.ddl", "Show the CREATE statement (DDL) for every table",
                      "SELECT name, sql FROM sqlite_master WHERE type='table'",
                      scope="catalog", category="catalog"),
        QueryTemplate("sqlite.indexes", "List all indexes defined in the database",
                      "SELECT name, tbl_name FROM sqlite_master WHERE type='index'",
                      scope="catalog", category="catalog"),
        QueryTemplate("sqlite.views", "List all views in the database",
                      "SELECT name FROM sqlite_master WHERE type='view'",
                      scope="catalog", category="catalog"),
        QueryTemplate("sqlite.table_count", "Count how many tables exist in the database",
                      "SELECT COUNT(*) AS table_count FROM sqlite_master WHERE type='table'",
                      scope="catalog", category="catalog"),
    ],
    "MySQL": [],
    "MariaDB": [],
    "PostgreSQL": [
        QueryTemplate("pg.tables", "List all tables in the public schema",
                      "SELECT table_name FROM information_schema.tables "
                      "WHERE table_schema = 'public' ORDER BY table_name",
                      scope="catalog", category="catalog"),
        QueryTemplate("pg.columns", "List all columns and their data types",
                      "SELECT table_name, column_name, data_type "
                      "FROM information_schema.columns WHERE table_schema = 'public' "
                      "ORDER BY table_name, ordinal_position",
                      scope="catalog", category="catalog"),
        QueryTemplate("pg.rows", "Show live row-count estimates per table",
                      "SELECT relname AS table_name, n_live_tup AS row_estimate "
                      "FROM pg_stat_user_tables ORDER BY n_live_tup DESC",
                      scope="catalog", category="catalog"),
        QueryTemplate("pg.fks", "List all foreign-key constraints",
                      "SELECT tc.table_name, kcu.column_name, ccu.table_name AS references_table "
                      "FROM information_schema.table_constraints tc "
                      "JOIN information_schema.key_column_usage kcu "
                      "ON tc.constraint_name = kcu.constraint_name "
                      "JOIN information_schema.constraint_column_usage ccu "
                      "ON ccu.constraint_name = tc.constraint_name "
                      "WHERE tc.constraint_type = 'FOREIGN KEY'",
                      scope="catalog", category="catalog"),
        QueryTemplate("pg.table_count", "Count tables in the public schema",
                      "SELECT COUNT(*) AS table_count FROM information_schema.tables "
                      "WHERE table_schema = 'public'",
                      scope="catalog", category="catalog"),
    ],
    "SQLServer": [
        QueryTemplate("mssql.tables", "List all user tables",
                      "SELECT name FROM sys.tables ORDER BY name",
                      scope="catalog", category="catalog"),
        QueryTemplate("mssql.columns", "List all columns with their data types",
                      "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                      "FROM INFORMATION_SCHEMA.COLUMNS "
                      "ORDER BY TABLE_NAME, ORDINAL_POSITION",
                      scope="catalog", category="catalog"),
        QueryTemplate("mssql.rows", "Show row counts for every table",
                      "SELECT t.name AS table_name, SUM(p.rows) AS row_count "
                      "FROM sys.tables t JOIN sys.partitions p ON t.object_id = p.object_id "
                      "WHERE p.index_id IN (0,1) GROUP BY t.name ORDER BY row_count DESC",
                      scope="catalog", category="catalog"),
        QueryTemplate("mssql.fks", "List all foreign-key relationships",
                      "SELECT fk.name AS fk_name, "
                      "OBJECT_NAME(fk.parent_object_id) AS table_name, "
                      "OBJECT_NAME(fk.referenced_object_id) AS references_table "
                      "FROM sys.foreign_keys fk",
                      scope="catalog", category="catalog"),
    ],
    "Oracle": [
        QueryTemplate("oracle.tables", "List all of my tables",
                      "SELECT table_name FROM user_tables ORDER BY table_name",
                      scope="catalog", category="catalog"),
        QueryTemplate("oracle.rows", "Show row-count statistics for every table",
                      "SELECT table_name, num_rows FROM user_tables ORDER BY num_rows DESC",
                      scope="catalog", category="catalog"),
        QueryTemplate("oracle.columns", "List all columns and their data types",
                      "SELECT table_name, column_name, data_type "
                      "FROM user_tab_columns ORDER BY table_name, column_id",
                      scope="catalog", category="catalog"),
        QueryTemplate("oracle.fks", "List all foreign-key constraints",
                      "SELECT table_name, constraint_name FROM user_constraints "
                      "WHERE constraint_type = 'R'",
                      scope="catalog", category="catalog"),
    ],
}

CATALOG_TEMPLATES["MySQL"] = [
    QueryTemplate("mysql.tables", "List all tables in the current database",
                  "SELECT TABLE_NAME FROM information_schema.TABLES "
                  "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME",
                  scope="catalog", category="catalog"),
    QueryTemplate("mysql.rows", "Show estimated row counts for every table",
                  "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES "
                  "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_ROWS DESC",
                  scope="catalog", category="catalog"),
    QueryTemplate("mysql.columns", "List all columns and their data types",
                  "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                  "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                  "ORDER BY TABLE_NAME, ORDINAL_POSITION",
                  scope="catalog", category="catalog"),
    QueryTemplate("mysql.fks", "List all foreign-key relationships",
                  "SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, "
                  "REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                  "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL",
                  scope="catalog", category="catalog"),
    QueryTemplate("mysql.pks", "List primary key columns for every table",
                  "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                  "WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'",
                  scope="catalog", category="catalog"),
    QueryTemplate("mysql.table_count", "Count tables in the current database",
                  "SELECT COUNT(*) AS table_count FROM information_schema.TABLES "
                  "WHERE TABLE_SCHEMA = DATABASE()",
                  scope="catalog", category="catalog"),
]
CATALOG_TEMPLATES["MariaDB"] = CATALOG_TEMPLATES["MySQL"]

OBJECT_TEMPLATES: list[QueryTemplate] = [
    QueryTemplate("object.sample", "Show a sample of {limit} rows from the {table_label} table",
                  "{bounded_select}", category="sample"),
    QueryTemplate("object.projection", "Show {col_list_label} from {table_label}",
                  "SELECT {col_list} FROM {table} {limit_clause}",
                  requires=("columns",), category="projection"),
    QueryTemplate("object.count", "How many rows are in the {table_label} table?",
                  "SELECT COUNT(*) AS total FROM {table}", category="count"),
    QueryTemplate("object.group_by", "Count {table_label} rows grouped by {text_col}",
                  "SELECT {text_col_q} AS {text_col}_value, COUNT(*) AS n "
                  "FROM {table} GROUP BY {text_col_q} ORDER BY n DESC {limit_50_clause}",
                  requires=("text_col",), complexity="moderate", category="group_by"),
    QueryTemplate("object.distinct", "How many distinct values of {text_col} exist in {table_label}?",
                  "SELECT COUNT(DISTINCT {text_col_q}) AS distinct_{text_col} FROM {table}",
                  requires=("text_col",), complexity="moderate", category="distinct"),
    QueryTemplate("object.aggregate", "Show min, max, average and sum of {num_col} in {table_label}",
                  "SELECT MIN({num_col_q}) AS min_{num_col}, MAX({num_col_q}) AS max_{num_col}, "
                  "AVG({num_col_q}) AS avg_{num_col}, SUM({num_col_q}) AS sum_{num_col} FROM {table}",
                  requires=("num_col",), complexity="moderate", category="aggregate"),
    QueryTemplate("object.top_n", "List the top {limit} {table_label} rows with the highest {num_col}",
                  "SELECT * FROM {table} ORDER BY {num_col_q} DESC {limit_clause}",
                  requires=("num_col",), complexity="moderate", category="top_n"),
    QueryTemplate("object.group_aggregate", "Total {num_col} by {text_col} in {table_label}",
                  "SELECT {text_col_q} AS {text_col}, SUM({num_col_q}) AS total_{num_col} "
                  "FROM {table} GROUP BY {text_col_q} ORDER BY total_{num_col} DESC {limit_50_clause}",
                  requires=("num_col", "text_col"), complexity="complex", category="group_aggregate"),
    QueryTemplate("object.null_profile", "How many rows have a NULL {first_col} in {table_label}?",
                  "SELECT COUNT(*) AS null_{first_col} FROM {table} WHERE {first_col_q} IS NULL",
                  requires=("columns",), category="null_profile"),
    QueryTemplate("object.window_rank", "Rank {table_label} rows by {num_col} within each {text_col}",
                  "SELECT {text_col_q} AS {text_col}, {num_col_q} AS {num_col}, "
                  "RANK() OVER (PARTITION BY {text_col_q} ORDER BY {num_col_q} DESC) AS rnk "
                  "FROM {table} {limit_clause}",
                  requires=("num_col", "text_col"), complexity="complex", category="window"),
]


def catalog_pairs_for(db_type: str) -> list[tuple[str, str]]:
    return [(t.question, t.sql) for t in CATALOG_TEMPLATES.get(db_type, [])]


def all_catalog_pairs() -> list[dict]:
    """Catalog/metadata templates for every supported SQL dialect.

    Includes any AI-enriched catalog templates persisted via the template store
    so "Enrich template" output is trained on the next harvest.
    """
    out: list[dict] = []
    for db_type, templates in CATALOG_TEMPLATES.items():
        tag = db_type.lower()
        for tmpl in templates:
            out.append({
                "question": tmpl.question,
                "sql": tmpl.sql,
                "db_type": tag,
                "category": tmpl.category or tmpl.id,
                "scope": tmpl.scope,
            })
    try:
        from ai_assistant.llm import template_store

        out.extend(template_store.enriched_catalog_pairs())
    except Exception:
        pass
    return out


def _all_object_templates() -> list[QueryTemplate]:
    """Built-in object templates plus any AI-enriched ones from the store."""
    out = list(OBJECT_TEMPLATES)
    try:
        from ai_assistant.llm import template_store

        for e in template_store.enriched_object_templates():
            out.append(QueryTemplate(
                id=e.get("id") or "enriched.object",
                question=e.get("question", ""),
                sql=e.get("sql", ""),
                requires=tuple(e.get("requires") or ()),
                complexity=e.get("complexity") or "moderate",
                scope="object",
                category=e.get("category") or "object",
            ))
    except Exception:
        pass
    return out


# MongoDB / document-store catalog templates (query language, not SQL).
MONGO_CATALOG_TEMPLATES: list[QueryTemplate] = [
    QueryTemplate(
        "mongo.collections",
        "List all collections in the database",
        'db.getCollectionNames()',
        db_types=("mongodb", "documentdb"),
        scope="catalog",
        category="catalog",
    ),
    QueryTemplate(
        "mongo.collection_count",
        "Count how many collections exist",
        "db.getCollectionNames().length",
        db_types=("mongodb", "documentdb"),
        scope="catalog",
        category="catalog",
    ),
    QueryTemplate(
        "mongo.sample",
        "Show a sample of {limit} documents from {table_label}",
        "db.{table_label}.find().limit({limit})",
        db_types=("mongodb", "documentdb"),
        scope="object",
        category="sample",
    ),
    QueryTemplate(
        "mongo.count",
        "How many documents are in the {table_label} collection?",
        "db.{table_label}.countDocuments()",
        db_types=("mongodb", "documentdb"),
        scope="object",
        category="count",
    ),
]


def mongo_catalog_pairs() -> list[dict]:
    return [
        {
            "question": t.question,
            "sql": t.sql,
            "db_type": "mongodb",
            "category": t.category or t.id,
        }
        for t in MONGO_CATALOG_TEMPLATES
        if t.scope == "catalog"
    ]


def supported_sql_db_types() -> list[str]:
    return sorted(CATALOG_TEMPLATES.keys())


def template_explanation(question: str, sql: str, *, db_type: str = "", category: str = "") -> str:
    """Short human-readable explanation for a template-derived pair."""
    db = (db_type or "SQL").strip()
    cat = (category or "query").replace("_", " ")
    return (
        f"{db} {cat}: answers '{question}' using validated template SQL."
    )


def build_template_values(table_info: Any, dialect: Any, *, limit: int) -> dict | None:
    """Build the placeholder substitution map for one table.

    Returns ``None`` when the table has no usable columns. The same map powers
    :func:`render_object_templates` and the curated seed corpus renderer so both
    stay in lockstep with the dialect's quoting / limit syntax.
    """
    cols = list(getattr(table_info, "columns", []) or [])
    if not cols:
        return None
    text_cols = list(getattr(table_info, "text_cols", []) or [])
    num_cols = list(getattr(table_info, "numeric_cols", []) or [])
    table = dialect.quote(table_info.name)
    table_label = table_info.name.split(".")[-1]
    first_col = cols[0]
    text = text_cols[0] if text_cols else first_col
    num = num_cols[0] if num_cols else first_col
    col_list_names = [c.name for c in cols[:6]]
    return {
        "table": table,
        "table_label": table_label,
        "limit": int(limit),
        "bounded_select": dialect.bounded_select(table_info.name, limit),
        "limit_clause": _limit_clause(dialect, limit),
        "limit_50_clause": _limit_clause(dialect, 50),
        "col_list": ", ".join(dialect.col(c) for c in col_list_names),
        "col_list_label": ", ".join(col_list_names),
        "text_col": text.name,
        "text_col_q": dialect.col(text.name),
        "num_col": num.name,
        "num_col_q": dialect.col(num.name),
        "first_col": first_col.name,
        "first_col_q": dialect.col(first_col.name),
        "_has_text_col": bool(text_cols),
        "_has_num_col": bool(num_cols),
    }


def render_object_templates(table_info: Any, dialect: Any, *, limit: int) -> list[dict]:
    """Render generic object templates using real table/column metadata."""
    values = build_template_values(table_info, dialect, limit=limit)
    if values is None:
        return []
    out: list[dict] = []
    for tmpl in _all_object_templates():
        if "text_col" in tmpl.requires and not values["_has_text_col"]:
            continue
        if "num_col" in tmpl.requires and not values["_has_num_col"]:
            continue
        try:
            rendered_q = tmpl.question.format(**values)
            rendered_sql = tmpl.sql.format(**values).strip()
        except (KeyError, IndexError, ValueError):
            # A malformed enriched template referencing an unknown placeholder
            # must never break rendering for the well-formed ones.
            continue
        out.append({
            "question": rendered_q,
            "sql": rendered_sql,
            "category": tmpl.category or tmpl.id,
            "complexity": tmpl.complexity,
            "template_id": tmpl.id,
        })
    return out


def render_object_templates_delex(table_info: Any, dialect: Any, *, limit: int) -> list[dict]:
    """Render object templates with real labels in the question and PH_* in SQL."""
    values = build_template_values(table_info, dialect, limit=limit)
    if values is None:
        return []
    delex = build_delex_sql_values(values)
    out: list[dict] = []
    for tmpl in _all_object_templates():
        if "text_col" in tmpl.requires and not values["_has_text_col"]:
            continue
        if "num_col" in tmpl.requires and not values["_has_num_col"]:
            continue
        try:
            rendered_q = tmpl.question.format(**values)
            rendered_sql = tmpl.sql.format(**delex).strip()
        except (KeyError, IndexError, ValueError):
            continue
        out.append({
            "question": rendered_q,
            "sql": rendered_sql,
            "category": tmpl.category or tmpl.id,
            "complexity": tmpl.complexity,
            "template_id": tmpl.id,
            "delexicalized": True,
        })
    return out


def _limit_clause(dialect: Any, n: int) -> str:
    sample = dialect.limit("SELECT * FROM __x__", int(n))
    if " LIMIT " in sample:
        return f"LIMIT {int(n)}"
    if "FETCH FIRST" in sample:
        return f"FETCH FIRST {int(n)} ROWS ONLY"
    # SQL Server TOP cannot be appended to arbitrary SELECTs; the validator will
    # keep only queries that execute, and the main miner also has SQLServer-
    # specific bounded_select for table samples.
    return ""
