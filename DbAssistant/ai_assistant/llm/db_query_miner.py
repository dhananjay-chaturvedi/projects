"""Standalone database-driven NL->SQL training-data miner.

A sophisticated, centralized channel that turns a *real* database connection
into a vetted corpus of natural-language -> SQL training pairs for the local
NL->SQL LLM.

Design goals (per product requirements):

* **Real data, no fakes.** Every generated query is executed against the
  selected connection. Only queries that run successfully are kept, so the
  corpus reflects the actual schema, dialect, and data — never invented tables.
* **System-catalog first.** The richest, safest queries read from system
  views/tables (``information_schema``, ``pg_catalog``, ``sqlite_master``,
  ``sys.*``, ``user_*``) to teach metadata/profiling patterns without scanning
  user data.
* **Bounded data access.** Any query that touches user tables is row-limited
  (``LIMIT`` / ``TOP`` / ``FETCH FIRST``) and the sample size is configurable,
  so mining never pulls large result sets.
* **Broad coverage.** Generates metadata, schema-introspection, sampling,
  aggregation, grouping, filtering, ordering/top-N, distinct/null profiling,
  date-bucketing, join, and window/analytical patterns — the common, regular,
  widely-used SQL idioms a DBA assistant must know.
* **Dialect aware.** SQLite, MySQL, MariaDB, PostgreSQL, SQL Server and Oracle
  each get the right catalog views, quoting, and row-limit syntax.

The miner is read-only: it constructs only ``SELECT``/``WITH`` statements and
refuses to execute anything else.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

_READ_ONLY_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|MERGE|CALL|EXEC)\b",
    re.IGNORECASE,
)

_NUMERIC_HINTS = (
    "int", "dec", "num", "float", "double", "real", "money", "bit",
    "serial", "year",
)
_DATE_HINTS = ("date", "time", "timestamp", "datetime")
_TEXT_HINTS = ("char", "text", "string", "clob", "enum", "uuid", "json")

# Engines we can mine via SQL. Document stores are skipped.
_SQL_ENGINES = frozenset({
    "SQLite", "MySQL", "MariaDB", "PostgreSQL", "SQLServer", "Oracle",
})


@dataclass
class ColumnInfo:
    name: str
    type: str = ""

    @property
    def kind(self) -> str:
        t = (self.type or "").lower()
        if any(h in t for h in _DATE_HINTS):
            return "date"
        if any(h in t for h in _NUMERIC_HINTS):
            return "numeric"
        if any(h in t for h in _TEXT_HINTS):
            return "text"
        return "other"


@dataclass
class TableInfo:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def numeric_cols(self) -> list[ColumnInfo]:
        return [c for c in self.columns if c.kind == "numeric"]

    @property
    def date_cols(self) -> list[ColumnInfo]:
        return [c for c in self.columns if c.kind == "date"]

    @property
    def text_cols(self) -> list[ColumnInfo]:
        return [c for c in self.columns if c.kind == "text"]


class _Dialect:
    """Per-engine identifier quoting, row-limit syntax, and catalog queries."""

    def __init__(self, db_type: str, database: str = "") -> None:
        self.db_type = db_type
        self.database = database or ""

    # ── identifiers / limits ────────────────────────────────────────────
    def quote(self, identifier: str) -> str:
        parts = [p for p in str(identifier).split(".") if p]
        if self.db_type == "SQLServer":
            return ".".join(f"[{p}]" for p in parts)
        if self.db_type in ("MySQL", "MariaDB"):
            return ".".join(f"`{p}`" for p in parts)
        return ".".join('"' + p.replace('"', '""') + '"' for p in parts)

    def col(self, name: str) -> str:
        if self.db_type == "SQLServer":
            return f"[{name}]"
        if self.db_type in ("MySQL", "MariaDB"):
            return f"`{name}`"
        return '"' + name.replace('"', '""') + '"'

    def limit(self, sql: str, n: int) -> str:
        n = int(n)
        if self.db_type in ("MySQL", "MariaDB", "PostgreSQL", "SQLite"):
            return f"{sql} LIMIT {n}"
        if self.db_type == "SQLServer":
            return re.sub(r"^\s*SELECT\b", f"SELECT TOP {n}", sql, count=1,
                          flags=re.IGNORECASE)
        if self.db_type == "Oracle":
            return f"{sql} FETCH FIRST {n} ROWS ONLY"
        return f"{sql} LIMIT {n}"

    def bounded_source(self, table: str, n: int) -> str:
        """A row-limited subquery over a user table (caps scanned rows)."""
        return f"(SELECT * FROM {self.quote(table)} {self._inner_limit(n)}) sub"

    def _inner_limit(self, n: int) -> str:
        n = int(n)
        if self.db_type == "Oracle":
            return f"FETCH FIRST {n} ROWS ONLY"
        if self.db_type == "SQLServer":
            # TOP must sit in the SELECT; bounded_source rewrites instead.
            return ""
        return f"LIMIT {n}"

    def bounded_select(self, table: str, n: int) -> str:
        """``SELECT * FROM table`` capped to *n* rows, dialect-correct."""
        return self.limit(f"SELECT * FROM {self.quote(table)}", n)

    # ── catalog (system view) queries ───────────────────────────────────
    def catalog_pairs(self) -> list[dict]:
        from ai_assistant.llm.query_templates import catalog_pairs_for

        templated = catalog_pairs_for(self.db_type)
        if templated:
            return templated
        db = self.database
        if self.db_type == "SQLite":
            return [
                ("List every table in the database",
                 "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"),
                ("Show the CREATE statement (DDL) for every table",
                 "SELECT name, sql FROM sqlite_master WHERE type='table'"),
                ("List all indexes defined in the database",
                 "SELECT name, tbl_name FROM sqlite_master WHERE type='index'"),
                ("List all views in the database",
                 "SELECT name FROM sqlite_master WHERE type='view'"),
                ("Count how many tables exist in the database",
                 "SELECT COUNT(*) AS table_count FROM sqlite_master WHERE type='table'"),
            ]
        if self.db_type in ("MySQL", "MariaDB"):
            return [
                ("List all tables in the current database",
                 "SELECT TABLE_NAME FROM information_schema.TABLES "
                 "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME"),
                ("Show estimated row counts for every table",
                 "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES "
                 "WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_ROWS DESC"),
                ("List all columns and their data types",
                 "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                 "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                 "ORDER BY TABLE_NAME, ORDINAL_POSITION"),
                ("List all foreign-key relationships",
                 "SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, "
                 "REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                 "WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL"),
                ("List primary key columns for every table",
                 "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                 "WHERE TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'"),
                ("Count tables in the current database",
                 "SELECT COUNT(*) AS table_count FROM information_schema.TABLES "
                 "WHERE TABLE_SCHEMA = DATABASE()"),
            ]
        if self.db_type == "PostgreSQL":
            return [
                ("List all tables in the public schema",
                 "SELECT table_name FROM information_schema.tables "
                 "WHERE table_schema = 'public' ORDER BY table_name"),
                ("List all columns and their data types",
                 "SELECT table_name, column_name, data_type "
                 "FROM information_schema.columns WHERE table_schema = 'public' "
                 "ORDER BY table_name, ordinal_position"),
                ("Show live row-count estimates per table",
                 "SELECT relname AS table_name, n_live_tup AS row_estimate "
                 "FROM pg_stat_user_tables ORDER BY n_live_tup DESC"),
                ("List all foreign-key constraints",
                 "SELECT tc.table_name, kcu.column_name, ccu.table_name AS references_table "
                 "FROM information_schema.table_constraints tc "
                 "JOIN information_schema.key_column_usage kcu "
                 "ON tc.constraint_name = kcu.constraint_name "
                 "JOIN information_schema.constraint_column_usage ccu "
                 "ON ccu.constraint_name = tc.constraint_name "
                 "WHERE tc.constraint_type = 'FOREIGN KEY'"),
                ("Count tables in the public schema",
                 "SELECT COUNT(*) AS table_count FROM information_schema.tables "
                 "WHERE table_schema = 'public'"),
            ]
        if self.db_type == "SQLServer":
            return [
                ("List all user tables",
                 "SELECT name FROM sys.tables ORDER BY name"),
                ("List all columns with their data types",
                 "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
                 "FROM INFORMATION_SCHEMA.COLUMNS "
                 "ORDER BY TABLE_NAME, ORDINAL_POSITION"),
                ("Show row counts for every table",
                 "SELECT t.name AS table_name, SUM(p.rows) AS row_count "
                 "FROM sys.tables t JOIN sys.partitions p ON t.object_id = p.object_id "
                 "WHERE p.index_id IN (0,1) GROUP BY t.name ORDER BY row_count DESC"),
                ("List all foreign-key relationships",
                 "SELECT fk.name AS fk_name, "
                 "OBJECT_NAME(fk.parent_object_id) AS table_name, "
                 "OBJECT_NAME(fk.referenced_object_id) AS references_table "
                 "FROM sys.foreign_keys fk"),
            ]
        if self.db_type == "Oracle":
            return [
                ("List all of my tables",
                 "SELECT table_name FROM user_tables ORDER BY table_name"),
                ("Show row-count statistics for every table",
                 "SELECT table_name, num_rows FROM user_tables ORDER BY num_rows DESC"),
                ("List all columns and their data types",
                 "SELECT table_name, column_name, data_type "
                 "FROM user_tab_columns ORDER BY table_name, column_id"),
                ("List all foreign-key constraints",
                 "SELECT table_name, constraint_name FROM user_constraints "
                 "WHERE constraint_type = 'R'"),
            ]
        return []


def _coerce_columns(raw_cols: Any) -> list[ColumnInfo]:
    out: list[ColumnInfo] = []
    for c in raw_cols or []:
        if isinstance(c, dict):
            name = (c.get("name") or c.get("column") or c.get("Field")
                    or c.get("COLUMN_NAME") or c.get("column_name") or "")
            ctype = (c.get("type") or c.get("data_type") or c.get("Type")
                     or c.get("DATA_TYPE") or c.get("dtype") or "")
        elif isinstance(c, (list, tuple)) and c:
            name = c[0]
            ctype = c[1] if len(c) > 1 else ""
        else:
            name = c
            ctype = ""
        name = str(name or "").strip()
        if name:
            out.append(ColumnInfo(name=name, type=str(ctype or "").strip()))
    return out


class DbTrainingMiner:
    """Mine vetted NL->SQL pairs from a live database connection."""

    def __init__(
        self,
        core: Any,
        connection: str,
        *,
        sample_limit: int = 5,
        max_tables: int = 40,
        max_pairs: int = 400,
        validate: bool = True,
        on_progress: Any = None,
    ) -> None:
        self._core = core
        self._connection = connection
        self.sample_limit = max(1, min(int(sample_limit or 5), 1000))
        self.max_tables = max(1, int(max_tables or 40))
        self.max_pairs = max(1, int(max_pairs or 400))
        self.validate = bool(validate)
        self._on_progress = on_progress
        self._db_type = ""
        self._dialect: _Dialect | None = None

    # ── helpers ─────────────────────────────────────────────────────────
    def _progress(self, **payload: Any) -> None:
        if self._on_progress:
            try:
                self._on_progress({"type": "training_mine", **payload})
            except Exception:
                pass

    def _detect_dialect(self) -> _Dialect | None:
        db_type = ""
        database = ""
        try:
            profile = self._core.get_connection_profile(self._connection) or {}
            db_type = profile.get("db_type", "") or ""
            database = (profile.get("service_or_db") or profile.get("database")
                        or "")
        except Exception:
            profile = {}
        if not db_type:
            try:
                mgr = self._core.get_manager(self._connection)
                db_type = getattr(mgr, "db_type", "") or ""
            except Exception:
                db_type = ""
        self._db_type = db_type
        if db_type not in _SQL_ENGINES:
            return None
        return _Dialect(db_type, database)

    def _is_read_only(self, sql: str) -> bool:
        return bool(_READ_ONLY_RE.match(sql)) and not _FORBIDDEN_RE.search(
            # ignore matches inside the leading SELECT list keywords by scanning
            # the statement body; our generated SQL never contains DML, this is
            # a defense-in-depth guard.
            re.sub(r"'[^']*'", "", sql))

    def _run(self, sql: str) -> tuple[bool, str]:
        """Execute *sql* read-only; return ``(ok, error)``."""
        if not self._is_read_only(sql):
            return False, "non read-only SQL rejected"
        try:
            res = self._core.execute(self._connection, sql)
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)
        if isinstance(res, dict) and res.get("error"):
            return False, str(res.get("error"))
        return True, ""

    def _tables(self) -> list[str]:
        try:
            raw = self._core.get_objects(self._connection, "tables") or []
        except Exception:
            return []
        names: list[str] = []
        for item in raw:
            if isinstance(item, dict):
                if item.get("error"):
                    continue
                nm = item.get("name") or item.get("table") or ""
            elif isinstance(item, (list, tuple)) and item:
                nm = item[0]
            else:
                nm = item
            nm = str(nm or "").strip()
            if nm:
                names.append(nm)
        return names[: self.max_tables]

    def _table_info(self, table: str) -> TableInfo:
        cols: list[ColumnInfo] = []
        try:
            info = self._core.get_table_schema(self._connection, table) or {}
            cols = _coerce_columns(info.get("columns"))
        except Exception:
            cols = []
        return TableInfo(name=table, columns=cols)

    # ── query generators ────────────────────────────────────────────────
    def _table_pairs(self, t: TableInfo, d: _Dialect) -> list[dict]:
        from ai_assistant.llm.query_templates import render_object_templates

        templated = render_object_templates(t, d, limit=self.sample_limit)
        if templated:
            return templated
        pairs: list[dict] = []
        tname = t.name.split(".")[-1]
        n = self.sample_limit
        col_names = [c.name for c in t.columns]

        # Sampling (bounded).
        pairs.append({
            "question": f"Show a sample of {n} rows from the {tname} table",
            "sql": d.bounded_select(t.name, n),
            "category": "sample",
        })
        if col_names:
            picked = ", ".join(d.col(c) for c in col_names[:6])
            pairs.append({
                "question": f"Show {', '.join(col_names[:6])} from {tname}",
                "sql": d.limit(f"SELECT {picked} FROM {d.quote(t.name)}", n),
                "category": "projection",
            })

        # Row count (full count is a single cheap aggregate row).
        pairs.append({
            "question": f"How many rows are in the {tname} table?",
            "sql": f"SELECT COUNT(*) AS total FROM {d.quote(t.name)}",
            "category": "count",
        })

        # Grouping / aggregation on a categorical (text) column.
        for c in t.text_cols[:2]:
            pairs.append({
                "question": f"Count {tname} rows grouped by {c.name}",
                "sql": d.limit(
                    f"SELECT {d.col(c.name)} AS {c.name}_value, COUNT(*) AS n "
                    f"FROM {d.quote(t.name)} GROUP BY {d.col(c.name)} "
                    f"ORDER BY n DESC", 50),
                "category": "group_by",
            })
            pairs.append({
                "question": f"How many distinct values of {c.name} exist in {tname}?",
                "sql": f"SELECT COUNT(DISTINCT {d.col(c.name)}) AS distinct_{c.name} "
                       f"FROM {d.quote(t.name)}",
                "category": "distinct",
            })

        # Numeric aggregates / analytics.
        for c in t.numeric_cols[:2]:
            pairs.append({
                "question": f"Show min, max, average and sum of {c.name} in {tname}",
                "sql": f"SELECT MIN({d.col(c.name)}) AS min_{c.name}, "
                       f"MAX({d.col(c.name)}) AS max_{c.name}, "
                       f"AVG({d.col(c.name)}) AS avg_{c.name}, "
                       f"SUM({d.col(c.name)}) AS sum_{c.name} FROM {d.quote(t.name)}",
                "category": "aggregate",
            })
            pairs.append({
                "question": f"List the top {n} {tname} rows with the highest {c.name}",
                "sql": d.limit(
                    f"SELECT * FROM {d.quote(t.name)} "
                    f"ORDER BY {d.col(c.name)} DESC", n),
                "category": "top_n",
            })
            # Group a numeric measure by a category (analytical).
            if t.text_cols:
                g = t.text_cols[0]
                pairs.append({
                    "question": f"Total {c.name} by {g.name} in {tname}",
                    "sql": d.limit(
                        f"SELECT {d.col(g.name)} AS {g.name}, "
                        f"SUM({d.col(c.name)}) AS total_{c.name} "
                        f"FROM {d.quote(t.name)} GROUP BY {d.col(g.name)} "
                        f"ORDER BY total_{c.name} DESC", 50),
                    "category": "group_aggregate",
                })

        # NULL profiling on the first column.
        if col_names:
            c0 = col_names[0]
            pairs.append({
                "question": f"How many rows have a NULL {c0} in {tname}?",
                "sql": f"SELECT COUNT(*) AS null_{c0} FROM {d.quote(t.name)} "
                       f"WHERE {d.col(c0)} IS NULL",
                "category": "null_profile",
            })

        # Date bucketing (analytical) on a date column.
        for c in t.date_cols[:1]:
            expr = self._year_expr(d, c.name)
            if expr:
                pairs.append({
                    "question": f"Count {tname} rows per year of {c.name}",
                    "sql": d.limit(
                        f"SELECT {expr} AS yr, COUNT(*) AS n FROM {d.quote(t.name)} "
                        f"GROUP BY {expr} ORDER BY yr", 100),
                    "category": "time_bucket",
                })

        # Window / ranking analytic (modern engines).
        if t.numeric_cols and t.text_cols:
            m = t.numeric_cols[0]
            g = t.text_cols[0]
            pairs.append({
                "question": f"Rank {tname} rows by {m.name} within each {g.name}",
                "sql": d.limit(
                    f"SELECT {d.col(g.name)} AS {g.name}, {d.col(m.name)} AS {m.name}, "
                    f"RANK() OVER (PARTITION BY {d.col(g.name)} "
                    f"ORDER BY {d.col(m.name)} DESC) AS rnk "
                    f"FROM {d.quote(t.name)}", n),
                "category": "window",
            })
        return pairs

    def _year_expr(self, d: _Dialect, col: str) -> str:
        c = d.col(col)
        if d.db_type in ("MySQL", "MariaDB", "PostgreSQL"):
            return f"EXTRACT(YEAR FROM {c})"
        if d.db_type == "SQLite":
            return f"strftime('%Y', {c})"
        if d.db_type == "SQLServer":
            return f"YEAR({c})"
        if d.db_type == "Oracle":
            return f"EXTRACT(YEAR FROM {c})"
        return ""

    def _join_pairs(self, tables: list[TableInfo], d: _Dialect) -> list[dict]:
        """Heuristic FK-style joins (``*_id`` matching another table name)."""
        pairs: list[dict] = []
        by_name = {t.name.split(".")[-1].lower(): t for t in tables}
        for t in tables:
            tshort = t.name.split(".")[-1]
            for c in t.columns:
                cl = c.name.lower()
                if not cl.endswith("_id") and cl != "id":
                    continue
                base = cl[:-3] if cl.endswith("_id") else ""
                if not base:
                    continue
                target = None
                for cand in (base, base + "s", base + "es"):
                    if cand in by_name and by_name[cand].name != t.name:
                        target = by_name[cand]
                        break
                if target is None:
                    continue
                tgt_short = target.name.split(".")[-1]
                pairs.append({
                    "question": f"Join {tshort} with {tgt_short} on {c.name}",
                    "sql": d.limit(
                        f"SELECT a.*, b.* FROM {d.quote(t.name)} a "
                        f"JOIN {d.quote(target.name)} b "
                        f"ON a.{d.col(c.name)} = b.{d.col('id')}",
                        self.sample_limit),
                    "category": "join",
                })
                break  # one join per table keeps the corpus balanced
        return pairs

    # ── orchestration ───────────────────────────────────────────────────
    def mine(self) -> dict:
        d = self._detect_dialect()
        if d is None:
            return {
                "ok": False,
                "pairs": [],
                "db_type": self._db_type,
                "error": (f"DB type '{self._db_type}' is not supported for SQL "
                          "mining." if self._db_type else
                          "Could not determine the database type."),
                "stats": {},
            }
        self._dialect = d
        self._progress(status="profiling", connection=self._connection,
                       db_type=self._db_type)

        candidates: list[dict] = []
        # 1) System-catalog / metadata queries first.
        for q, sql in d.catalog_pairs():
            candidates.append({"question": q, "sql": sql, "category": "catalog"})

        # 2) Per-table data/aggregation/analytics.
        tables = [self._table_info(t) for t in self._tables()]
        for t in tables:
            candidates.extend(self._table_pairs(t, d))
        # 3) Cross-table joins.
        candidates.extend(self._join_pairs(tables, d))

        self._progress(status="generated", candidates=len(candidates),
                       tables=len(tables))

        kept: list[dict] = []
        validated = 0
        failed = 0
        seen: set[str] = set()
        for cand in candidates:
            if len(kept) >= self.max_pairs:
                break
            sql = cand["sql"].strip()
            key = re.sub(r"\s+", " ", sql.lower())
            if key in seen:
                continue
            seen.add(key)
            if self.validate:
                ok, err = self._run(sql)
                if not ok:
                    failed += 1
                    continue
                validated += 1
            from ai_assistant.llm.validation import validate_pair

            vok, cleaned, _reason = validate_pair(
                {"question": cand["question"], "sql": sql, "description": cand.get("category", "")},
                db_type=self._db_type,
            )
            if not vok:
                failed += 1
                continue
            kept.append({
                "question": cleaned["question"],
                "sql": cleaned["sql"],
                "description": cleaned.get("description") or cand.get("category", ""),
            })

        self._progress(status="validated", kept=len(kept),
                       validated=validated, failed=failed)

        by_cat: dict[str, int] = {}
        for p in kept:
            by_cat[p["description"]] = by_cat.get(p["description"], 0) + 1

        return {
            "ok": bool(kept),
            "pairs": kept,
            "db_type": self._db_type,
            "error": None if kept else "No queries validated against the database.",
            "stats": {
                "tables": len(tables),
                "candidates": len(candidates),
                "validated": validated,
                "failed": failed,
                "kept": len(kept),
                "by_category": by_cat,
                "sample_limit": self.sample_limit,
                "validated_against_db": self.validate,
            },
        }


def supported_sql_db_types() -> list[str]:
    return sorted(_SQL_ENGINES)


def get_dialect_for_db_type(db_type: str, database: str = "") -> _Dialect | None:
    if db_type not in _SQL_ENGINES:
        return None
    return _Dialect(db_type, database)


def mine_connection_pairs(
    core: Any,
    connection: str,
    *,
    sample_limit: int = 5,
    max_tables: int = 40,
    max_pairs: int = 400,
    validate: bool = True,
    on_progress: Any = None,
) -> dict:
    """Convenience wrapper around :class:`DbTrainingMiner`."""
    if core is None or not connection:
        return {"ok": False, "pairs": [], "error": "core and connection required.",
                "stats": {}}
    return DbTrainingMiner(
        core, connection,
        sample_limit=sample_limit, max_tables=max_tables, max_pairs=max_pairs,
        validate=validate, on_progress=on_progress,
    ).mine()
