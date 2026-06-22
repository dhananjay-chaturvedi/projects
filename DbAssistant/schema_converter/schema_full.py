"""
Full table schema introspection and cross-database DDL generation.

Extends the basic SchemaConverter model with comments, constraints, rich indexes,
ENUM/SET, generated columns, partitioning, charset/collation, sequences, and
related objects (triggers/views) metadata.
"""

from __future__ import annotations

import re
from typing import Any

from common.config_loader import properties

from .converter import DataTypeMapper, DefaultValueFormatter
from .type_overrides import apply_type_override, parse_base_type


def _ensure_str(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _escape_mysql_comment(text: str) -> str:
    return str(text).replace("'", "''")


def get_zero_date_strategy() -> str:
    try:
        from schema_converter import module_config as mc
        return mc.get(
            "schema.conversion", "zero_date_strategy", default="quote"
        ).strip().lower()
    except Exception:
        return properties.get(
            "schema.conversion", "zero_date_strategy", default="quote"
        ).strip().lower()


def apply_zero_date_strategy(default_sql: str | None) -> str | None:
    """Adjust zero-date defaults for strict target engines."""
    if not default_sql:
        return default_sql
    strategy = get_zero_date_strategy()
    inner = default_sql.strip().strip("'")
    if inner not in ("0000-00-00", "0000-00-00 00:00:00"):
        return default_sql
    if strategy == "null":
        return "NULL"
    if strategy == "omit":
        return None
    return default_sql if default_sql.startswith("'") else _sql_quote(inner)


def empty_extended_schema(table_name: str) -> dict[str, Any]:
    return {
        "table_name": table_name,
        "source_schema": None,
        "table_comment": None,
        "table_collation": None,
        "table_charset": None,
        "columns": [],
        "primary_key": [],
        "indexes": [],
        "unique_constraints": [],
        "foreign_keys": [],
        "check_constraints": [],
        "partition": None,
        "sequences": [],
        "related_objects": {"views": [], "triggers": [], "procedures": [], "functions": []},
        "conversion_warnings": [],
    }


# ---------------------------------------------------------------------------
# Introspection – MySQL / MariaDB
# ---------------------------------------------------------------------------

def enrich_mysql_schema(schema: dict, conn, table_name: str) -> dict:
    cursor = conn.cursor()
    db_name = _mysql_current_db(cursor)

    cursor.execute(
        """
        SELECT TABLE_COMMENT, TABLE_COLLATION, ENGINE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        [db_name, table_name],
    )
    row = cursor.fetchone()
    table_charset = None
    table_collation = None
    if row:
        schema["table_comment"] = _ensure_str(row[0]) or None
        schema["table_collation"] = _ensure_str(row[1])
        schema["table_engine"] = _ensure_str(row[2])
        table_collation = schema["table_collation"]
        if table_collation and "_" in table_collation:
            table_charset = table_collation.split("_", 1)[0]
            schema["table_charset"] = table_charset

    col_by_name = {c["name"]: c for c in schema["columns"]}
    _mysql_enrich_columns(
        cursor, db_name, table_name, col_by_name, table_charset, table_collation
    )

    schema["unique_constraints"] = _mysql_unique_constraints(cursor, db_name, table_name)
    schema["foreign_keys"] = _mysql_foreign_keys(cursor, db_name, table_name)
    schema["check_constraints"] = _mysql_check_constraints(cursor, db_name, table_name)
    schema["indexes"] = _mysql_indexes(cursor, db_name, table_name, schema["primary_key"])
    schema["partition"] = _mysql_partition(cursor, db_name, table_name)
    schema["sequences"] = []
    schema["related_objects"]["triggers"] = _mysql_triggers(cursor, table_name)
    schema["related_objects"]["views"] = _mysql_views_on_table(cursor, db_name, table_name)
    cursor.close()
    return schema


def _mysql_current_db(cursor) -> str:
    cursor.execute("SELECT DATABASE()")
    row = cursor.fetchone()
    return _ensure_str(row[0]) if row else ""


def _mysql_information_schema_columns(cursor, table_name: str) -> set[str]:
    """Return upper-case column names available for an information_schema table."""
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = %s
            """,
            [table_name.upper()],
        )
        return {_ensure_str(r[0]).upper() for r in cursor.fetchall() if r[0]}
    except Exception:
        return set()


def _mysql_normalize_column_charset(col: dict, table_charset, table_collation):
    """Drop charset/collation that merely inherit the table default."""
    if not _mysql_type_supports_charset(col.get("type") or ""):
        col.pop("charset", None)
        col.pop("collation", None)
        return
    cset = col.get("charset")
    coll = col.get("collation")
    if cset and table_charset and cset == table_charset:
        col["charset"] = None
    if coll and table_collation and coll == table_collation:
        col["collation"] = None


def _mysql_enrich_columns(
    cursor, db_name, table_name, col_by_name, table_charset=None, table_collation=None
):
    """Enrich column metadata; tolerate older MySQL/MariaDB information_schema."""
    isc = _mysql_information_schema_columns(cursor, "COLUMNS")
    gen_col = "GENERATION_EXPRESSION" if "GENERATION_EXPRESSION" in isc else None
    gen_select = "GENERATION_EXPRESSION" if gen_col else "NULL"

    cursor.execute(
        f"""
        SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT, CHARACTER_SET_NAME,
               COLLATION_NAME, {gen_select}, EXTRA, IS_NULLABLE,
               COLUMN_DEFAULT, COLUMN_KEY
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        [db_name, table_name],
    )
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        col = col_by_name.get(name)
        if not col:
            continue
        col["type"] = _ensure_str(row[1]) or col["type"]
        col["comment"] = _ensure_str(row[2]) or None
        col["charset"] = _ensure_str(row[3])
        col["collation"] = _ensure_str(row[4])
        gen = _ensure_str(row[5]) if gen_col else None
        if gen:
            stored = "STORED GENERATED" in (_ensure_str(row[6]) or "").upper()
            col["generated"] = {"expression": gen, "stored": stored}
        enum_vals = _parse_mysql_enum_type(col["type"])
        if enum_vals:
            col["enum_values"] = enum_vals
        if "unsigned" in (col["type"] or "").lower():
            col["unsigned"] = True
        _mysql_normalize_column_charset(col, table_charset, table_collation)


def _parse_mysql_enum_type(type_str: str) -> list[str] | None:
    if not type_str:
        return None
    upper = type_str.upper()
    if not (upper.startswith("ENUM(") or upper.startswith("SET(")):
        return None
    inner = type_str[type_str.index("(") + 1 : type_str.rindex(")")]
    return [p.strip().strip("'") for p in re.findall(r"'((?:[^'\\]|\\.)*)'", inner)]


def _mysql_unique_constraints(cursor, db_name, table_name):
    cursor.execute(
        """
        SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION
        FROM information_schema.TABLE_CONSTRAINTS tc
        JOIN information_schema.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
         AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_SCHEMA = %s AND tc.TABLE_NAME = %s
          AND tc.CONSTRAINT_TYPE = 'UNIQUE'
        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        """,
        [db_name, table_name],
    )
    grouped: dict[str, list] = {}
    for name, col, _ in cursor.fetchall():
        grouped.setdefault(_ensure_str(name), []).append(_ensure_str(col))
    return [{"name": k, "columns": v} for k, v in grouped.items()]


def _mysql_foreign_keys(cursor, db_name, table_name):
    cursor.execute(
        """
        SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME,
               kcu.REFERENCED_COLUMN_NAME, rc.UPDATE_RULE, rc.DELETE_RULE,
               kcu.ORDINAL_POSITION
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
          ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
         AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        WHERE kcu.CONSTRAINT_SCHEMA = %s AND kcu.TABLE_NAME = %s
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        """,
        [db_name, table_name],
    )
    grouped: dict[str, dict] = {}
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        fk = grouped.setdefault(
            name,
            {
                "name": name,
                "columns": [],
                "referenced_table": _ensure_str(row[2]),
                "referenced_columns": [],
                "on_update": _ensure_str(row[4]),
                "on_delete": _ensure_str(row[5]),
            },
        )
        fk["columns"].append(_ensure_str(row[1]))
        fk["referenced_columns"].append(_ensure_str(row[3]))
    return list(grouped.values())


def _mysql_check_constraints(cursor, db_name, table_name):
    try:
        cursor.execute(
            """
            SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.CHECK_CONSTRAINTS cc
              ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
             AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_SCHEMA = %s AND tc.TABLE_NAME = %s
              AND tc.CONSTRAINT_TYPE = 'CHECK'
            """,
            [db_name, table_name],
        )
        return [
            {"name": _ensure_str(r[0]), "expression": _ensure_str(r[1])}
            for r in cursor.fetchall()
        ]
    except Exception:
        return []


def _mysql_indexes(cursor, db_name, table_name, primary_key):
    isc = _mysql_information_schema_columns(cursor, "STATISTICS")
    expr_select = "EXPRESSION" if "EXPRESSION" in isc else "NULL"
    comment_select = "INDEX_COMMENT" if "INDEX_COMMENT" in isc else "NULL"

    cursor.execute(
        f"""
        SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, COLLATION,
               INDEX_TYPE, {expr_select}, {comment_select}
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        [db_name, table_name],
    )
    grouped: dict[str, dict] = {}
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        if name == "PRIMARY":
            continue
        idx = grouped.setdefault(
            name,
            {
                "name": name,
                "columns": [],
                "unique": row[1] == 0,
                "type": (_ensure_str(row[5]) or "BTREE").upper(),
                "expression": None,
                "partial_predicate": None,
                "comment": _ensure_str(row[7]) if "INDEX_COMMENT" in isc else None,
            },
        )
        expr = _ensure_str(row[6]) if "EXPRESSION" in isc else None
        col = _ensure_str(row[3])
        order = "DESC" if (_ensure_str(row[4]) or "A") == "D" else "ASC"
        if expr:
            idx["expression"] = expr
            idx["columns"].append({"name": expr, "order": order, "expression": True})
        elif col:
            idx["columns"].append({"name": col, "order": order})
    return list(grouped.values())


def _mysql_partition(cursor, db_name, table_name):
    cursor.execute(
        """
        SELECT PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION,
               PARTITION_NAME, PARTITION_ORDINAL_POSITION
        FROM information_schema.PARTITIONS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
          AND PARTITION_NAME IS NOT NULL
        ORDER BY PARTITION_ORDINAL_POSITION
        """,
        [db_name, table_name],
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    method = _ensure_str(rows[0][0])
    expr = _ensure_str(rows[0][1])
    parts = []
    for row in rows:
        parts.append(
            {
                "name": _ensure_str(row[3]),
                "description": _ensure_str(row[2]),
            }
        )
    return {"method": method, "expression": expr, "partitions": parts}


def _mysql_triggers(cursor, table_name):
    try:
        cursor.execute("SHOW TRIGGERS WHERE `Table` = %s", [table_name])
        triggers = []
        for row in cursor.fetchall():
            triggers.append(
                {
                    "name": _ensure_str(row[0]),
                    "timing": _ensure_str(row[4]),
                    "event": _ensure_str(row[1]),
                    "body": _ensure_str(row[3]),
                }
            )
        return triggers
    except Exception:
        return []


def _mysql_views_on_table(cursor, db_name, table_name):
    try:
        cursor.execute(
            """
            SELECT TABLE_NAME, VIEW_DEFINITION
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = %s AND VIEW_DEFINITION LIKE %s
            """,
            [db_name, f"%{table_name}%"],
        )
        return [
            {"name": _ensure_str(r[0]), "definition": _ensure_str(r[1])}
            for r in cursor.fetchall()
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Introspection – PostgreSQL
# ---------------------------------------------------------------------------

def enrich_postgres_schema(schema: dict, conn, table_name: str, pg_schema: str = "public") -> dict:
    if "." in table_name:
        pg_schema, table_only = table_name.split(".", 1)
    else:
        table_only = table_name
    schema["source_schema"] = pg_schema
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT obj_description(c.oid), current_setting('server_encoding')
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        [pg_schema, table_only],
    )
    row = cursor.fetchone()
    if row:
        schema["table_comment"] = _ensure_str(row[0])
        schema["table_charset"] = _ensure_str(row[1])

    col_by_name = {c["name"]: c for c in schema["columns"]}
    cursor.execute(
        """
        SELECT a.attname, pg_catalog.col_description(c.oid, a.attnum),
               pg_catalog.format_type(a.atttypid, a.atttypmod),
               a.attnotnull, pg_get_expr(ad.adbin, ad.adrelid),
               a.attgenerated
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        [pg_schema, table_only],
    )
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        col = col_by_name.get(name)
        if not col:
            continue
        col["comment"] = _ensure_str(row[1])
        col["type"] = _ensure_str(row[2]) or col["type"]
        gen_kind = _ensure_str(row[5]) or ""
        gen_expr = _ensure_str(row[4])
        if gen_kind in ("s", "p") and gen_expr:
            col["generated"] = {"expression": gen_expr, "stored": gen_kind == "s"}

    schema["unique_constraints"] = _pg_unique_constraints(cursor, pg_schema, table_only)
    schema["foreign_keys"] = _pg_foreign_keys(cursor, pg_schema, table_only)
    schema["check_constraints"] = _pg_check_constraints(cursor, pg_schema, table_only)
    schema["indexes"] = _pg_indexes(cursor, pg_schema, table_only)
    schema["partition"] = _pg_partition(cursor, pg_schema, table_only)
    schema["sequences"] = _pg_sequences(cursor, pg_schema, table_only)
    schema["related_objects"]["triggers"] = _pg_triggers(cursor, pg_schema, table_only)
    cursor.close()
    return schema


def _pg_unique_constraints(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT c.conname, a.attname, u.ord
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY u(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
        WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'u'
        ORDER BY c.conname, u.ord
        """,
        [pg_schema, table_name],
    )
    grouped: dict[str, list] = {}
    for name, col, _ in cursor.fetchall():
        grouped.setdefault(_ensure_str(name), []).append(_ensure_str(col))
    return [{"name": k, "columns": v} for k, v in grouped.items()]


def _pg_foreign_keys(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT c.conname, a.attname, nr.nspname, tr.relname, af.attname,
               c.confupdtype, c.confdeltype, u.ord
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN unnest(c.conkey) WITH ORDINALITY u(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = u.attnum
        JOIN unnest(c.confkey) WITH ORDINALITY uf(attnum, ord) ON uf.ord = u.ord
        JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = uf.attnum
        JOIN pg_class tr ON tr.oid = c.confrelid
        JOIN pg_namespace nr ON nr.oid = tr.relnamespace
        WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'f'
        ORDER BY c.conname, u.ord
        """,
        [pg_schema, table_name],
    )
    grouped: dict[str, dict] = {}
    upd_map = {"a": "NO ACTION", "r": "RESTRICT", "c": "CASCADE", "n": "SET NULL", "d": "SET DEFAULT"}
    del_map = upd_map
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        fk = grouped.setdefault(
            name,
            {
                "name": name,
                "columns": [],
                "referenced_schema": _ensure_str(row[2]),
                "referenced_table": _ensure_str(row[3]),
                "referenced_columns": [],
                "on_update": upd_map.get(_ensure_str(row[5]), "NO ACTION"),
                "on_delete": del_map.get(_ensure_str(row[6]), "NO ACTION"),
            },
        )
        fk["columns"].append(_ensure_str(row[1]))
        fk["referenced_columns"].append(_ensure_str(row[4]))
    return list(grouped.values())


def _pg_check_constraints(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT c.conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'c'
        """,
        [pg_schema, table_name],
    )
    out = []
    for name, defn in cursor.fetchall():
        expr = _ensure_str(defn) or ""
        expr = re.sub(r"^CHECK\s*\((.*)\)\s*$", r"\1", expr, flags=re.I | re.S)
        out.append({"name": _ensure_str(name), "expression": expr})
    return out


def _pg_indexes(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        """,
        [pg_schema, table_name],
    )
    indexes = []
    for name, defn in cursor.fetchall():
        name = _ensure_str(name)
        defn = _ensure_str(defn) or ""
        if " UNIQUE " in defn.upper():
            unique = True
        else:
            unique = defn.upper().startswith("CREATE UNIQUE")
        expr_match = re.search(r"\((.+)\)", defn)
        expr_body = expr_match.group(1) if expr_match else ""
        partial = None
        where_match = re.search(r"\bWHERE\b(.+)$", defn, re.I)
        if where_match:
            partial = where_match.group(1).strip()
        using_match = re.search(r"USING\s+(\w+)", defn, re.I)
        idx_type = (using_match.group(1) if using_match else "BTREE").upper()
        columns = []
        if expr_body and not partial:
            for part in _split_index_columns(expr_body):
                columns.append({"name": part.strip(), "order": "ASC"})
        indexes.append(
            {
                "name": name,
                "columns": columns,
                "unique": unique,
                "type": idx_type,
                "expression": defn if "(" not in expr_body[:1] else None,
                "partial_predicate": partial,
                "raw_definition": defn,
            }
        )
    return indexes


def _split_index_columns(body: str) -> list[str]:
    parts, current, depth = [], [], 0
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def _pg_partition(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT pg_get_partkeydef(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'p'
        """,
        [pg_schema, table_name],
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        return None
    return {"method": "PARTITION BY", "expression": _ensure_str(row[0]), "partitions": []}


def _pg_sequences(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT seq.relname, a.attname
        FROM pg_class seq
        JOIN pg_depend dep ON dep.objid = seq.oid
        JOIN pg_class tbl ON tbl.oid = dep.refobjid
        JOIN pg_namespace n ON n.oid = tbl.relnamespace
        JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = dep.refobjsubid
        WHERE seq.relkind = 'S' AND n.nspname = %s AND tbl.relname = %s
        """,
        [pg_schema, table_name],
    )
    return [
        {"name": _ensure_str(r[0]), "column": _ensure_str(r[1])}
        for r in cursor.fetchall()
    ]


def _pg_triggers(cursor, pg_schema, table_name):
    cursor.execute(
        """
        SELECT tgname, pg_get_triggerdef(t.oid, true)
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
        """,
        [pg_schema, table_name],
    )
    return [{"name": _ensure_str(r[0]), "definition": _ensure_str(r[1])} for r in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Introspection – Oracle
# ---------------------------------------------------------------------------

def enrich_oracle_schema(schema: dict, conn, table_name: str) -> dict:
    tname = table_name.upper()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT comments FROM user_tab_comments WHERE table_name = :1", [tname]
    )
    row = cursor.fetchone()
    if row:
        schema["table_comment"] = _ensure_str(row[0])

    col_by_name = {c["name"].upper(): c for c in schema["columns"]}
    cursor.execute(
        "SELECT column_name, comments FROM user_col_comments WHERE table_name = :1",
        [tname],
    )
    for col_name, comment in cursor.fetchall():
        col = col_by_name.get(_ensure_str(col_name).upper())
        if col:
            col["comment"] = _ensure_str(comment)

    cursor.execute(
        """
        SELECT cc.column_name, cc.comments
        FROM user_col_comments cc
        JOIN user_tab_cols utc ON utc.table_name = cc.table_name
          AND utc.column_name = cc.column_name
        WHERE cc.table_name = :1 AND utc.virtual_column = 'YES'
        """,
        [tname],
    )
    for col_name, _ in cursor.fetchall():
        col = col_by_name.get(_ensure_str(col_name).upper())
        if col:
            col["generated"] = {"expression": "VIRTUAL", "stored": False}

    schema["unique_constraints"] = _ora_unique_constraints(cursor, tname)
    schema["foreign_keys"] = _ora_foreign_keys(cursor, tname)
    schema["check_constraints"] = _ora_check_constraints(cursor, tname)
    schema["indexes"] = _ora_indexes(cursor, tname, schema["primary_key"])
    schema["partition"] = _ora_partition(cursor, tname)
    schema["sequences"] = _ora_sequences(cursor, tname)
    schema["related_objects"]["triggers"] = _ora_triggers(cursor, tname)
    cursor.close()
    return schema


def _ora_unique_constraints(cursor, table_name):
    cursor.execute(
        """
        SELECT c.constraint_name, cols.column_name, cols.position
        FROM user_constraints c
        JOIN user_cons_columns cols ON c.constraint_name = cols.constraint_name
        WHERE c.table_name = :1 AND c.constraint_type = 'U'
        ORDER BY c.constraint_name, cols.position
        """,
        [table_name],
    )
    grouped: dict[str, list] = {}
    for name, col, _ in cursor.fetchall():
        grouped.setdefault(_ensure_str(name), []).append(_ensure_str(col))
    return [{"name": k, "columns": v} for k, v in grouped.items()]


def _ora_foreign_keys(cursor, table_name):
    cursor.execute(
        """
        SELECT c.constraint_name, cols.column_name, r.table_name, r_cols.column_name,
               c.delete_rule, c.constraint_name, cols.position
        FROM user_constraints c
        JOIN user_cons_columns cols ON c.constraint_name = cols.constraint_name
        JOIN user_constraints r ON c.r_constraint_name = r.constraint_name
        JOIN user_cons_columns r_cols ON r.constraint_name = r_cols.constraint_name
          AND cols.position = r_cols.position
        WHERE c.table_name = :1 AND c.constraint_type = 'R'
        ORDER BY c.constraint_name, cols.position
        """,
        [table_name],
    )
    grouped: dict[str, dict] = {}
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        fk = grouped.setdefault(
            name,
            {
                "name": name,
                "columns": [],
                "referenced_table": _ensure_str(row[2]),
                "referenced_columns": [],
                "on_update": "NO ACTION",
                "on_delete": _ensure_str(row[4]) or "NO ACTION",
            },
        )
        fk["columns"].append(_ensure_str(row[1]))
        fk["referenced_columns"].append(_ensure_str(row[3]))
    return list(grouped.values())


def _ora_check_constraints(cursor, table_name):
    cursor.execute(
        """
        SELECT constraint_name, search_condition
        FROM user_constraints
        WHERE table_name = :1 AND constraint_type = 'C'
          AND search_condition IS NOT NULL
        """,
        [table_name],
    )
    out = []
    for name, expr in cursor.fetchall():
        expr_s = _ensure_str(expr) or ""
        if expr_s.upper().startswith("(") and " IS NOT NULL" not in expr_s.upper():
            out.append({"name": _ensure_str(name), "expression": expr_s.strip("()")})
    return out


def _ora_indexes(cursor, table_name, primary_key):
    cursor.execute(
        """
        SELECT i.index_name, ic.column_name, ic.column_position, i.uniqueness,
               ie.column_expression
        FROM user_indexes i
        JOIN user_ind_columns ic ON i.index_name = ic.index_name
        LEFT JOIN user_ind_expressions ie ON ie.index_name = ic.index_name
          AND ie.column_position = ic.column_position
        WHERE i.table_name = :1
          AND i.index_name NOT IN (
            SELECT constraint_name FROM user_constraints
            WHERE table_name = :1 AND constraint_type IN ('P','U')
          )
        ORDER BY i.index_name, ic.column_position
        """,
        [table_name, table_name],
    )
    grouped: dict[str, dict] = {}
    for row in cursor.fetchall():
        name = _ensure_str(row[0])
        idx = grouped.setdefault(
            name,
            {
                "name": name,
                "columns": [],
                "unique": (_ensure_str(row[3]) or "") == "UNIQUE",
                "type": "BTREE",
                "expression": None,
                "partial_predicate": None,
            },
        )
        expr = _ensure_str(row[4])
        col = _ensure_str(row[1])
        if expr:
            idx["expression"] = expr
            idx["columns"].append({"name": expr, "order": "ASC", "expression": True})
        elif col:
            idx["columns"].append({"name": col, "order": "ASC"})
    return list(grouped.values())


def _ora_partition(cursor, table_name):
    cursor.execute(
        """
        SELECT partitioning_type, partition_count
        FROM user_part_tables WHERE table_name = :1
        """,
        [table_name],
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {
        "method": _ensure_str(row[0]),
        "expression": None,
        "partitions": [],
        "partition_count": row[1],
    }


def _ora_sequences(cursor, table_name):
    return []


def _ora_triggers(cursor, table_name):
    cursor.execute(
        """
        SELECT trigger_name, trigger_type, triggering_event, trigger_body
        FROM user_triggers WHERE table_name = :1
        """,
        [table_name],
    )
    return [
        {
            "name": _ensure_str(r[0]),
            "timing": _ensure_str(r[1]),
            "event": _ensure_str(r[2]),
            "body": _ensure_str(r[3]),
        }
        for r in cursor.fetchall()
    ]


# ---------------------------------------------------------------------------
# Enrichment entry + conversion
# ---------------------------------------------------------------------------

def enrich_table_schema(schema: dict, db_type: str, conn, table_name: str) -> dict:
    if not schema:
        return schema
    for key, default in empty_extended_schema(table_name).items():
        if key not in schema:
            schema[key] = default if not isinstance(default, list) else list(default)
    if db_type in ("MySQL", "MariaDB"):
        return enrich_mysql_schema(schema, conn, table_name)
    if db_type == "PostgreSQL":
        return enrich_postgres_schema(schema, conn, table_name)
    if db_type == "Oracle":
        return enrich_oracle_schema(schema, conn, table_name)
    return schema


def convert_extended_schema(
    schema: dict,
    source_type: str,
    target_type: str,
    table_name_map: dict[str, str] | None = None,
    type_overrides: dict[str, str] | None = None,
) -> dict:
    table_name_map = table_name_map or {}
    converted = {
        "table_name": schema.get("table_name"),
        "source_schema": schema.get("source_schema"),
        "table_comment": schema.get("table_comment"),
        "table_collation": schema.get("table_collation"),
        "table_charset": schema.get("table_charset"),
        "columns": [],
        "primary_key": list(schema.get("primary_key") or []),
        "indexes": [],
        "unique_constraints": [],
        "foreign_keys": [],
        "check_constraints": [],
        "partition": schema.get("partition"),
        "sequences": list(schema.get("sequences") or []),
        "related_objects": schema.get("related_objects") or {},
        "conversion_warnings": list(schema.get("conversion_warnings") or []),
    }

    overrides = type_overrides or {}
    for col in schema.get("columns") or []:
        converted_col = _convert_column(
            col, source_type, target_type, type_overrides=overrides
        )
        converted["columns"].append(converted_col)

    converted["unique_constraints"] = [
        {
            "name": uc["name"],
            "columns": list(uc.get("columns") or []),
        }
        for uc in schema.get("unique_constraints") or []
    ]

    for fk in schema.get("foreign_keys") or []:
        ref_table = fk.get("referenced_table")
        mapped_ref = table_name_map.get(ref_table, ref_table)
        converted["foreign_keys"].append(
            {
                "name": fk.get("name"),
                "columns": list(fk.get("columns") or []),
                "referenced_table": mapped_ref,
                "referenced_columns": list(fk.get("referenced_columns") or []),
                "on_update": fk.get("on_update") or "NO ACTION",
                "on_delete": fk.get("on_delete") or "NO ACTION",
            }
        )

    for chk in schema.get("check_constraints") or []:
        converted["check_constraints"].append(
            {
                "name": chk.get("name"),
                "expression": _convert_check_expression(
                    chk.get("expression"), source_type, target_type, converted["columns"]
                ),
            }
        )

    for enum_col in [c for c in converted["columns"] if c.get("enum_values")]:
        if target_type in ("MySQL", "MariaDB"):
            vals = ",".join(_sql_quote(v) for v in enum_col["enum_values"])
            kind = "ENUM" if "SET(" not in (enum_col.get("source_type") or "").upper() else "SET"
            if kind == "SET":
                enum_col["type"] = f"SET({vals})"
            else:
                enum_col["type"] = f"ENUM({vals})"
        elif target_type == "PostgreSQL":
            vals = ", ".join(_sql_quote(v) for v in enum_col["enum_values"])
            enum_col["check_expression"] = f"{enum_col['name']} IN ({vals})"
        else:
            converted["conversion_warnings"].append(
                f"ENUM column {enum_col['name']} converted to string; add CHECK manually."
            )

    converted["indexes"] = _convert_indexes(schema.get("indexes") or [], source_type, target_type)
    converted["partition"] = _convert_partition(schema.get("partition"), source_type, target_type)
    converted["related_objects"] = _convert_related_objects(
        schema.get("related_objects") or {}, source_type, target_type, table_name_map
    )
    return converted


def _convert_column(
    col: dict,
    source_type: str,
    target_type: str,
    type_overrides: dict[str, str] | None = None,
) -> dict:
    ctype = col.get("type") or ""
    enum_values = col.get("enum_values")
    if not enum_values:
        enum_values = _parse_mysql_enum_type(ctype)
    converted_type = ctype
    if not enum_values:
        overrides = type_overrides or {}
        source_base, _ = parse_base_type(ctype)
        if source_base in overrides:
            converted_type = apply_type_override(ctype, overrides[source_base])
        else:
            converted_type = DataTypeMapper.convert_type(ctype, source_type, target_type)

    auto_increment = bool(
        col.get("extra") == "auto_increment"
        or col.get("auto_increment")
    )
    on_update = None
    if col.get("extra"):
        extra_bits = DefaultValueFormatter.parse_mysql_extra(col["extra"])
        auto_increment = auto_increment or extra_bits.get("auto_increment", False)
        if extra_bits.get("on_update"):
            on_update = DefaultValueFormatter.format_on_update(
                extra_bits["on_update"], target_type
            )

    default = DefaultValueFormatter.convert_default(
        col.get("default"),
        converted_type,
        source_type,
        target_type,
        auto_increment=auto_increment,
    )
    default = apply_zero_date_strategy(default)

    out = {
        "name": col["name"],
        "type": converted_type,
        "nullable": col.get("nullable", True),
        "default": default,
        "auto_increment": auto_increment,
        "comment": col.get("comment"),
        "charset": col.get("charset"),
        "collation": col.get("collation"),
        "generated": col.get("generated"),
        "enum_values": enum_values,
        "source_type": ctype,
        "on_update": on_update,
        "unsigned": col.get("unsigned", False),
    }
    if out.get("generated") and target_type not in ("MySQL", "MariaDB", "PostgreSQL"):
        out["generated"] = None
    return out


def _convert_check_expression(expr, source_type, target_type, columns):
    if not expr:
        return expr
    return str(expr)


def _convert_indexes(indexes, source_type, target_type):
    out = []
    for idx in indexes:
        item = {
            "name": idx.get("name"),
            "columns": [dict(c) for c in idx.get("columns") or []],
            "unique": bool(idx.get("unique")),
            "type": idx.get("type") or "BTREE",
            "expression": idx.get("expression"),
            "partial_predicate": idx.get("partial_predicate"),
            "raw_definition": idx.get("raw_definition"),
        }
        if item["expression"] and target_type not in ("MySQL", "MariaDB", "PostgreSQL", "Oracle"):
            item["expression"] = None
        out.append(item)
    return out


def _convert_partition(partition, source_type, target_type):
    if not partition:
        return None
    if source_type == target_type or (
        source_type in ("MySQL", "MariaDB") and target_type in ("MySQL", "MariaDB")
    ):
        return partition
    return {
        "method": partition.get("method"),
        "expression": partition.get("expression"),
        "partitions": partition.get("partitions") or [],
        "requires_manual_review": True,
    }


def _convert_related_objects(related, source_type, target_type, table_name_map):
    same_family = source_type == target_type or (
        source_type in ("MySQL", "MariaDB") and target_type in ("MySQL", "MariaDB")
    )
    out = {"views": [], "triggers": [], "procedures": [], "functions": []}
    for trig in related.get("triggers") or []:
        entry = dict(trig)
        entry["requires_manual_review"] = not same_family
        out["triggers"].append(entry)
    for view in related.get("views") or []:
        entry = dict(view)
        entry["requires_manual_review"] = not same_family
        out["views"].append(entry)
    return out


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

def _mysql_type_supports_charset(type_str: str) -> bool:
    if not type_str:
        return False
    base = type_str.upper().split("(")[0].strip()
    return base in {
        "CHAR",
        "VARCHAR",
        "TINYTEXT",
        "TEXT",
        "MEDIUMTEXT",
        "LONGTEXT",
        "ENUM",
        "SET",
    }


def _indexes_for_ddl(schema: dict, target_type: str) -> list[dict]:
    """Return indexes to emit separately (skip those already inline as constraints)."""
    indexes = list(schema.get("indexes") or [])
    if target_type not in ("MySQL", "MariaDB"):
        return indexes

    uc_names = {uc.get("name") for uc in schema.get("unique_constraints") or [] if uc.get("name")}
    filtered = []
    seen: set[str] = set()
    for idx in indexes:
        name = idx.get("name")
        if not name or name in seen:
            continue
        if name in uc_names:
            continue
        if idx.get("unique"):
            continue
        seen.add(name)
        filtered.append(idx)
    return filtered


def generate_all_table_ddl(schema: dict, target_type: str) -> list[str]:
    """Return ordered DDL statements to recreate *schema* on *target_type*."""
    if target_type not in ("MySQL", "MariaDB", "Oracle", "PostgreSQL"):
        return []
    statements: list[str] = []
    create_sql = _generate_create_table(schema, target_type)
    if create_sql:
        statements.append(create_sql)

    table = schema["table_name"]
    for idx in _indexes_for_ddl(schema, target_type):
        sql = _generate_index_ddl(table, idx, target_type)
        if sql:
            statements.append(sql)

    for fk in schema.get("foreign_keys") or []:
        sql = _generate_foreign_key_ddl(table, fk, target_type)
        if sql and sql not in create_sql:
            statements.append(sql)

    for seq in schema.get("sequences") or []:
        sql = _generate_sequence_ddl(seq, target_type)
        if sql:
            statements.append(sql)

    comment_sql = _generate_comment_ddl(schema, target_type)
    statements.extend(comment_sql)

    trigger_sql = _generate_trigger_ddl(schema, target_type)
    statements.extend(trigger_sql)

    return [s for s in statements if s]


def _table_options_mysql(schema: dict) -> str:
    parts = []
    engine = schema.get("table_engine")
    if engine:
        parts.append(f"ENGINE={engine}")
    charset = schema.get("table_charset")
    collation = schema.get("table_collation")
    if not charset:
        from schema_converter.charset import get_conversion_charset

        if get_conversion_charset().lower().replace("-", "") in ("utf8", "utf"):
            charset = "utf8mb4"
            collation = collation or "utf8mb4_unicode_ci"
    elif str(charset).upper() in ("UTF8", "UTF-8"):
        charset = "utf8mb4"
        collation = collation or "utf8mb4_unicode_ci"
    if charset:
        parts.append(f"DEFAULT CHARSET={charset}")
    if collation:
        parts.append(f"COLLATE={collation}")
    if schema.get("table_comment"):
        parts.append(f"COMMENT='{_escape_mysql_comment(schema['table_comment'])}'")
    return (" " + " ".join(parts)) if parts else ""


def _column_suffix_mysql(col: dict) -> str:
    parts = []
    col_type = col.get("type") or ""
    if _mysql_type_supports_charset(col_type):
        if col.get("charset"):
            parts.append(f"CHARACTER SET {col['charset']}")
        if col.get("collation"):
            parts.append(f"COLLATE {col['collation']}")
    if col.get("comment"):
        parts.append(f"COMMENT '{_escape_mysql_comment(col['comment'])}'")
    return (" " + " ".join(parts)) if parts else ""


def _append_default(col_def: str, col: dict) -> str:
    default = col.get("default")
    if default and str(default).upper() != "NULL":
        col_def += f" DEFAULT {default}"
    if col.get("on_update"):
        col_def += f" ON UPDATE {col['on_update']}"
    return col_def


def _generate_create_table(schema: dict, target_type: str) -> str:
    table = schema["table_name"]
    columns = schema.get("columns") or []
    pk = schema.get("primary_key") or []
    lines: list[str] = []

    for col in columns:
        line = _generate_column_def(col, target_type)
        lines.append(f"  {line}")

    for uc in schema.get("unique_constraints") or []:
        cols = ", ".join(uc.get("columns") or [])
        if cols:
            name = uc.get("name") or f"uk_{table}_{cols.replace(', ', '_')}"
            lines.append(f"  CONSTRAINT {name} UNIQUE ({cols})")

    for chk in schema.get("check_constraints") or []:
        expr = chk.get("expression") or chk.get("check_expression")
        if expr:
            name = chk.get("name") or f"chk_{table}_{len(lines)}"
            lines.append(f"  CONSTRAINT {name} CHECK ({expr})")

    for col in columns:
        if col.get("check_expression") and target_type == "PostgreSQL":
            name = f"chk_{table}_{col['name']}_enum"
            lines.append(f"  CONSTRAINT {name} CHECK ({col['check_expression']})")

    if pk:
        if target_type == "Oracle":
            lines.append(f"  CONSTRAINT pk_{table} PRIMARY KEY ({', '.join(pk)})")
        else:
            lines.append(f"  PRIMARY KEY ({', '.join(pk)})")

    if target_type in ("MySQL", "MariaDB"):
        for fk in schema.get("foreign_keys") or []:
            fk_line = _inline_foreign_key_mysql(fk)
            if fk_line:
                lines.append(f"  {fk_line}")

    body = ",\n".join(lines)
    partition_sql = _partition_clause(schema.get("partition"), target_type)

    if target_type in ("MySQL", "MariaDB"):
        return f"CREATE TABLE {table} (\n{body}\n){_table_options_mysql(schema)}{partition_sql};"
    if target_type == "Oracle":
        ddl = f"CREATE TABLE {table} (\n{body}\n){partition_sql or ''}"
        if any(c.get("auto_increment") for c in columns) and pk:
            ddl += _oracle_autoincrement_extras(table, pk[0])
        return ddl
    if target_type == "PostgreSQL":
        return f"CREATE TABLE {table} (\n{body}\n){partition_sql or ''}"
    return f"CREATE TABLE {table} (\n{body}\n)"


def _generate_column_def(col: dict, target_type: str) -> str:
    name = col["name"]
    if col.get("auto_increment") and target_type == "PostgreSQL":
        ctype = "BIGSERIAL" if "BIGINT" in col["type"].upper() else "SERIAL"
        line = f"{name} {ctype}"
    else:
        line = f"{name} {col['type']}"

    if col.get("generated") and target_type in ("MySQL", "MariaDB"):
        gen = col["generated"]
        kind = "STORED" if gen.get("stored") else "VIRTUAL"
        line += f" GENERATED ALWAYS AS ({gen['expression']}) {kind}"
    elif col.get("generated") and target_type == "PostgreSQL":
        gen = col["generated"]
        kind = "STORED" if gen.get("stored") else "VIRTUAL"
        line += f" GENERATED ALWAYS AS ({gen['expression']}) {kind}"

    if not col.get("nullable", True):
        line += " NOT NULL"
    if not col.get("auto_increment") and not col.get("generated"):
        line = _append_default(line, col)

    if col.get("auto_increment") and target_type in ("MySQL", "MariaDB"):
        line += " AUTO_INCREMENT"

    if target_type in ("MySQL", "MariaDB"):
        line += _column_suffix_mysql(col)
    return line


def _inline_foreign_key_mysql(fk: dict) -> str | None:
    cols = ", ".join(fk.get("columns") or [])
    ref_cols = ", ".join(fk.get("referenced_columns") or [])
    if not cols or not fk.get("referenced_table"):
        return None
    name = fk.get("name") or f"fk_{cols.replace(', ', '_')}"
    sql = (
        f"CONSTRAINT {name} FOREIGN KEY ({cols}) "
        f"REFERENCES {fk['referenced_table']} ({ref_cols})"
    )
    if fk.get("on_delete") and fk["on_delete"] != "NO ACTION":
        sql += f" ON DELETE {fk['on_delete']}"
    if fk.get("on_update") and fk["on_update"] != "NO ACTION":
        sql += f" ON UPDATE {fk['on_update']}"
    return sql


def _generate_foreign_key_ddl(table: str, fk: dict, target_type: str) -> str | None:
    if target_type in ("MySQL", "MariaDB"):
        return None
    inline = _inline_foreign_key_mysql(fk)
    if not inline:
        return None
    sep = ";" if target_type != "Oracle" else ""
    return f"ALTER TABLE {table} ADD {inline}{sep}"


def _normalize_index_columns(cols):
    normalized = []
    for c in cols or []:
        if isinstance(c, str):
            normalized.append({"name": c, "order": "ASC"})
        else:
            normalized.append(c)
    return normalized


def _generate_index_ddl(table: str, idx: dict, target_type: str) -> str | None:
    name = idx.get("name")
    if not name:
        return None
    unique = "UNIQUE " if idx.get("unique") else ""
    if idx.get("raw_definition") and target_type == "PostgreSQL":
        raw = idx["raw_definition"]
        return raw if raw.endswith(";") else raw + ";"

    cols = _normalize_index_columns(idx.get("columns"))
    if idx.get("expression"):
        body = idx["expression"]
    elif cols:
        parts = []
        for c in cols:
            if c.get("expression"):
                parts.append(c["name"])
            else:
                order = f" {c['order']}" if c.get("order") == "DESC" else ""
                parts.append(f"{c['name']}{order}")
        body = ", ".join(parts)
    else:
        return None

    idx_type = ""
    if target_type in ("MySQL", "MariaDB") and idx.get("type") not in (None, "BTREE"):
        idx_type = f" USING {idx['type']}"

    partial = ""
    if idx.get("partial_predicate") and target_type == "PostgreSQL":
        partial = f" WHERE {idx['partial_predicate']}"

    sep = ";" if target_type != "Oracle" else ""
    return f"CREATE {unique}INDEX {name} ON {table}{idx_type} ({body}){partial}{sep}"


def _partition_clause(partition, target_type: str) -> str:
    if not partition:
        return ""
    if partition.get("requires_manual_review"):
        return ""
    method = (partition.get("method") or "").upper()
    expr = partition.get("expression")
    if target_type in ("MySQL", "MariaDB") and method and expr:
        parts = [f"\nPARTITION BY {method}({expr})"]
        for p in partition.get("partitions") or []:
            pname = p.get("name")
            desc = p.get("description")
            if pname and desc:
                parts.append(f" PARTITION {pname} VALUES LESS THAN ({desc})")
            elif pname:
                parts.append(f" PARTITION {pname}")
        return "".join(parts)
    if target_type == "PostgreSQL" and expr:
        return f"\nPARTITION BY {expr}"
    if target_type == "Oracle" and method:
        return f"\nPARTITION BY {method}"
    return ""


def _oracle_autoincrement_extras(table: str, pk_col: str) -> str:
    return (
        f"\n\nCREATE SEQUENCE {table}_seq START WITH 1 INCREMENT BY 1"
        f"\n\nCREATE OR REPLACE TRIGGER {table}_trg\n"
        f"BEFORE INSERT ON {table}\nFOR EACH ROW\nBEGIN\n"
        f"  IF :new.{pk_col} IS NULL THEN\n"
        f"    SELECT {table}_seq.NEXTVAL INTO :new.{pk_col} FROM dual;\n"
        f"  END IF;\nEND"
    )


def _generate_sequence_ddl(seq: dict, target_type: str) -> str | None:
    name = seq.get("name")
    if not name or target_type != "PostgreSQL":
        return None
    return f"CREATE SEQUENCE IF NOT EXISTS {name};"


def _generate_comment_ddl(schema: dict, target_type: str) -> list[str]:
    table = schema["table_name"]
    out: list[str] = []
    if schema.get("table_comment"):
        c = schema["table_comment"].replace("'", "''")
        if target_type in ("MySQL", "MariaDB"):
            pass  # inline in CREATE TABLE
        elif target_type == "PostgreSQL":
            out.append(f"COMMENT ON TABLE {table} IS '{c}';")
        elif target_type == "Oracle":
            out.append(f"COMMENT ON TABLE {table} IS '{c}'")

    for col in schema.get("columns") or []:
        if not col.get("comment"):
            continue
        c = col["comment"].replace("'", "''")
        if target_type in ("MySQL", "MariaDB"):
            continue
        elif target_type == "PostgreSQL":
            out.append(f"COMMENT ON COLUMN {table}.{col['name']} IS '{c}';")
        elif target_type == "Oracle":
            out.append(f"COMMENT ON COLUMN {table}.{col['name']} IS '{c}'")
    return out


def _generate_trigger_ddl(schema: dict, target_type: str) -> list[str]:
    out: list[str] = []
    for trig in (schema.get("related_objects") or {}).get("triggers") or []:
        if trig.get("requires_manual_review"):
            continue
        if trig.get("definition"):
            defn = trig["definition"]
            out.append(defn if defn.endswith(";") else defn + ";")
        elif target_type in ("MySQL", "MariaDB") and trig.get("body"):
            timing = trig.get("timing", "AFTER")
            event = trig.get("event", "INSERT")
            name = trig.get("name")
            table = schema["table_name"]
            out.append(
                f"CREATE TRIGGER {name} {timing} {event} ON {table} "
                f"FOR EACH ROW {trig['body']};"
            )
    return out
