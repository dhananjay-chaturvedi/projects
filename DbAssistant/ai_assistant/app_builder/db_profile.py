"""Deterministic database profiling for from_database app builds.

Facts are gathered via the database registry (metadata) and bounded read-only
queries (profiling + sampling). No model calls — interpretation happens later in
:class:`~ai_assistant.app_builder.db_understanding.DbUnderstandingClient`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from ai_query import module_config as mc


@dataclass
class ColumnProfile:
    name: str
    data_type: str = ""
    nullable: bool = True
    is_pk: bool = False
    is_fk: bool = False
    unique: bool = False
    indexed: bool = False
    null_ratio: Optional[float] = None
    distinct_estimate: Optional[int] = None
    sample_values: list[Any] = field(default_factory=list)
    semantic_tags: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "is_pk": self.is_pk,
            "is_fk": self.is_fk,
            "unique": self.unique,
            "indexed": self.indexed,
            "null_ratio": self.null_ratio,
            "distinct_estimate": self.distinct_estimate,
            "sample_values": list(self.sample_values),
            "semantic_tags": list(self.semantic_tags),
        }


@dataclass
class TableProfile:
    name: str
    columns: list[ColumnProfile] = field(default_factory=list)
    row_count_estimate: Optional[int] = None
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    indexes: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    unique_keys: list[list[str]] = field(default_factory=list)
    role: str = ""
    role_confidence: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.as_dict() for c in self.columns],
            "row_count_estimate": self.row_count_estimate,
            "sample_rows": list(self.sample_rows),
            "indexes": list(self.indexes),
            "constraints": list(self.constraints),
            "primary_key": list(self.primary_key),
            "unique_keys": [list(u) for u in self.unique_keys],
            "role": self.role,
            "role_confidence": self.role_confidence,
        }


@dataclass
class DbProfile:
    """Consolidated deterministic understanding of a database."""

    connection: str = ""
    db_type: str = ""
    tables: list[TableProfile] = field(default_factory=list)
    views: list[str] = field(default_factory=list)
    procedures: list[str] = field(default_factory=list)
    triggers: list[str] = field(default_factory=list)
    sequences: list[str] = field(default_factory=list)
    indexes: list[str] = field(default_factory=list)
    constraints: list[str] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    advisory_notes: list[str] = field(default_factory=list)
    metadata_kinds: dict[str, bool] = field(default_factory=dict)
    phases_completed: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "connection": self.connection,
            "db_type": self.db_type,
            "tables": [t.as_dict() for t in self.tables],
            "views": list(self.views),
            "procedures": list(self.procedures),
            "triggers": list(self.triggers),
            "sequences": list(self.sequences),
            "indexes": list(self.indexes),
            "constraints": list(self.constraints),
            "relationships": [dict(r) for r in self.relationships],
            "advisory_notes": list(self.advisory_notes),
            "metadata_kinds": dict(self.metadata_kinds),
            "phases_completed": list(self.phases_completed),
        }


@dataclass(frozen=True)
class DbProfilerConfig:
    """Runtime/config inputs for deterministic DB profiling."""

    core: Any = None
    db_manager: Any = None
    connection_name: str = ""
    sample_rows: int | None = None
    max_tables: int | None = None
    profile_row_cap: int | None = None
    deep_column_profiling: bool | None = None
    exact_row_counts: bool | None = None
    use_system_views: bool | None = None


class DbProfiler:
    """Gather deterministic DB facts via catalog ops + bounded queries."""

    _META_OPS = (
        ("tables", "getTables"),
        ("views", "getViews"),
        ("procedures", "getProcedures"),
        ("triggers", "getTriggers"),
        ("sequences", "getSequences"),
        ("indexes", "getIndexes"),
        ("constraints", "getConstraints"),
    )

    def __init__(
        self,
        config: DbProfilerConfig | None = None,
        **legacy,
    ) -> None:
        config = config or DbProfilerConfig(**legacy)
        self._core = config.core
        self._db = config.db_manager
        self._connection = config.connection_name
        self._sample_rows = config.sample_rows or mc.get_int(
            "ai.app_builder", "db_sample_rows", default=3)
        self._max_tables = config.max_tables or mc.get_int(
            "ai.app_builder", "db_max_tables", default=25)
        self._profile_row_cap = config.profile_row_cap or mc.get_int(
            "ai.app_builder", "db_profile_row_cap", default=1000)
        # Heavy per-column aggregation is opt-in; default off. When off we still
        # report approximate null/distinct, derived from the sampled rows.
        self._deep_column_profiling = (
            config.deep_column_profiling if config.deep_column_profiling is not None
            else mc.get_bool(
                "ai.app_builder", "db_deep_column_profiling", default=False))
        # Catalog/system-view row estimates are always preferred. An exact
        # COUNT(*) is only a fallback (engines without a catalog stat) and only
        # when this flag is on.
        self._exact_row_counts = (
            config.exact_row_counts if config.exact_row_counts is not None
            else mc.get_bool(
                "ai.app_builder", "db_exact_row_counts", default=True))
        self._use_system_views = (
            config.use_system_views if config.use_system_views is not None
            else mc.get_bool(
                "ai.app_builder", "use_system_views", default=True))
        self._db_type_cache: Optional[str] = None

    def profile(self, schema: dict[str, list[str]] | None = None) -> DbProfile:
        profile = DbProfile(connection=self._connection)
        profile.phases_completed.append("metadata")
        self._gather_metadata(profile, schema or {})
        if self._db is not None or self._core is not None:
            profile.phases_completed.append("profiling")
            self._active_profile = profile
            try:
                self._profile_tables(profile)
            finally:
                self._active_profile = profile
            profile.phases_completed.append("sampling")
            self._sample_tables(profile)
        profile.phases_completed.append("semantics")
        self._resolve_system_catalog(profile)
        from ai_assistant.app_builder.db_semantics import enrich_profile

        enrich_profile(profile)
        return profile

    # ── metadata ─────────────────────────────────────────────────────────────
    def _gather_metadata(
        self, profile: DbProfile, schema: dict[str, list[str]]
    ) -> None:
        if self._core is not None and self._connection:
            profile.db_type = self._db_type()
            for kind, _op in self._META_OPS:
                profile.metadata_kinds[kind] = self._fetch_objects(kind, profile)
            if not schema:
                schema = self._schema_from_core()
        else:
            for kind, _ in self._META_OPS:
                profile.metadata_kinds[kind] = bool(schema) if kind == "tables" else False

        existing = {t.name: t for t in profile.tables}
        for tname, cols in list((schema or {}).items())[: self._max_tables]:
            tname = str(tname).split(".")[-1]
            col_profiles = [
                ColumnProfile(name=str(c)) for c in (cols or []) if str(c).strip()
            ]
            if tname in existing:
                if col_profiles and not existing[tname].columns:
                    existing[tname].columns = col_profiles
                continue
            table = TableProfile(name=tname, columns=col_profiles)
            profile.tables.append(table)
            existing[tname] = table

    def _db_type(self) -> str:
        if self._db is not None:
            return str(getattr(self._db, "db_type", "") or "")
        try:
            mgr = self._core.get_manager(self._connection)
            return str(getattr(mgr, "db_type", "") or "")
        except Exception:
            return ""

    def _fetch_objects(self, kind: str, profile: DbProfile) -> bool:
        try:
            rows = self._core.get_objects(self._connection, kind) or []
        except Exception:
            return False
        if rows and isinstance(rows[0], dict) and rows[0].get("error"):
            return False
        names = [_row_name(r) for r in rows if _row_name(r)]
        if kind == "tables" and not profile.tables:
            for n in names[: self._max_tables]:
                profile.tables.append(TableProfile(name=n.split(".")[-1]))
        elif kind == "views":
            profile.views = names[:200]
        elif kind == "procedures":
            profile.procedures = names[:200]
        elif kind == "triggers":
            profile.triggers = names[:200]
        elif kind == "sequences":
            profile.sequences = names[:200]
        elif kind == "indexes":
            profile.indexes = [_short_label(r) for r in rows[:500]]
        elif kind == "constraints":
            profile.constraints = [_short_label(r) for r in rows[:500]]
        return bool(names)

    def _schema_from_core(self) -> dict[str, list[str]]:
        from ai_assistant.app_builder.service import _column_names

        out: dict[str, list[str]] = {}
        try:
            tables = self._core.get_objects(self._connection, "tables") or []
        except Exception:
            return out
        for table in list(tables)[: self._max_tables]:
            if isinstance(table, dict):
                continue
            tname = str(table).split(".")[-1]
            try:
                info = self._core.get_table_schema(self._connection, tname)
                cols = _column_names(info.get("columns") or [])
            except Exception:
                cols = []
            out[tname] = cols or ["id"]
        return out

    # ── profiling + sampling ─────────────────────────────────────────────────
    def _profile_tables(self, profile: DbProfile) -> None:
        for table in profile.tables[: self._max_tables]:
            self._enrich_columns(table)
            table.row_count_estimate = self._row_count(table.name)

    def _enrich_columns(self, table: TableProfile) -> None:
        if self._core is not None and self._connection:
            try:
                info = self._core.get_table_schema(self._connection, table.name)
                from ai_assistant.app_builder.service import _column_names

                raw_cols = info.get("columns") or []
                if raw_cols and not table.columns:
                    table.columns = [
                        ColumnProfile(name=n) for n in _column_names(raw_cols)
                    ]
                for rc in raw_cols:
                    if isinstance(rc, dict):
                        cname = str(
                            rc.get("name") or rc.get("column") or rc.get("Field") or ""
                        ).strip()
                        for col in table.columns:
                            if col.name == cname:
                                col.data_type = str(
                                    rc.get("type") or rc.get("data_type")
                                    or rc.get("Type") or ""
                                )
                                nullable = rc.get("nullable", rc.get("Null"))
                                if nullable is not None:
                                    col.nullable = str(nullable).upper() not in (
                                        "NO", "FALSE", "0")
                                col.is_pk = bool(
                                    rc.get("pk") or rc.get("primary_key")
                                    or str(rc.get("Key", "")).upper() == "PRI")
                                col.unique = bool(
                                    rc.get("unique") or rc.get("unique_key")
                                    or str(rc.get("Key", "")).upper() == "UNI")
                                col.indexed = bool(
                                    col.unique or col.is_pk or rc.get("indexed")
                                    or str(rc.get("Key", "")).upper() in ("MUL", "UNI"))
                                break
                self._apply_table_schema_details(table, info)
            except Exception:
                pass
        # Heavy full-table aggregations (NULL ratio + COUNT(DISTINCT)) only run
        # when deep profiling is explicitly enabled. Otherwise these stats are
        # derived for free from the sampled rows in :meth:`_sample_tables`.
        if self._deep_column_profiling:
            for col in table.columns[:20]:
                col.null_ratio = self._null_ratio(table.name, col.name)
                col.distinct_estimate = self._distinct_estimate(
                    table.name, col.name)

    def _apply_table_schema_details(self, table: TableProfile, info: dict[str, Any]) -> None:
        """Normalize per-table PK/FK/UK/index details returned by CoreDBService."""
        pk = _names_from_any(info.get("primary_key") or info.get("primary_keys"))
        if pk:
            table.primary_key = pk
            for name in pk:
                col = _find_col(table, name)
                if col:
                    col.is_pk = True
                    col.indexed = True

        for fk in _foreign_keys_from_any(info.get("foreign_keys") or info.get("fks")):
            if not fk.get("from_column"):
                continue
            if not fk.get("from_table"):
                fk["from_table"] = table.name
            if not fk.get("to_column"):
                fk["to_column"] = "id"
            fk.setdefault("kind", "N:1")
            fk.setdefault("source", "declared")
            fk.setdefault("confidence", 1.0)
            self._add_relationship(fk)
            col = _find_col(table, fk["from_column"])
            if col:
                col.is_fk = True
                col.indexed = True

        unique_groups = _unique_groups_from_any(info.get("unique_keys") or info.get("uniques"))
        for group in unique_groups:
            if group and group not in table.unique_keys:
                table.unique_keys.append(group)
            for name in group:
                col = _find_col(table, name)
                if col:
                    col.unique = True
                    col.indexed = True

        for idx in list(info.get("indexes") or []):
            label = _short_label(idx)
            if label and label not in table.indexes:
                table.indexes.append(label)
            cols = _index_columns(idx)
            unique = _index_is_unique(idx)
            for name in cols:
                col = _find_col(table, name)
                if col:
                    col.indexed = True
                    col.unique = col.unique or unique
            if unique and cols and cols not in table.unique_keys:
                table.unique_keys.append(cols)

    def _sample_tables(self, profile: DbProfile) -> None:
        for table in profile.tables[: self._max_tables]:
            rows = self._sample(table.name)
            table.sample_rows = rows
            if rows and table.columns:
                for col in table.columns:
                    col.sample_values = [
                        r.get(col.name) for r in rows[:3]
                        if col.name in r
                    ][:3]
                if not self._deep_column_profiling:
                    self._approx_stats_from_sample(table, rows)

    @staticmethod
    def _approx_stats_from_sample(
        table: TableProfile, rows: list[dict[str, Any]]
    ) -> None:
        """Approximate null ratio + distinct count from sampled rows (no query).

        Cheap, bounded by the sample size — used when deep profiling is off so
        the build still gets directional column stats without scanning tables.
        """
        for col in table.columns:
            present = [r[col.name] for r in rows if col.name in r]
            if not present:
                continue
            nulls = sum(1 for v in present if v is None or v == "")
            col.null_ratio = round(nulls / len(present), 4)
            col.distinct_estimate = len({str(v) for v in present})

    def _sample(self, table: str) -> list[dict[str, Any]]:
        if self._core is not None and self._connection:
            try:
                r = self._core.sample_table(
                    self._connection, table, limit=self._sample_rows)
                if r.get("error"):
                    return []
                out = []
                cols = r.get("columns") or []
                for row in r.get("rows") or []:
                    if isinstance(row, dict):
                        out.append(row)
                    elif isinstance(row, (list, tuple)):
                        out.append({
                            str(cols[i]) if i < len(cols) else f"c{i}": v
                            for i, v in enumerate(row)
                        })
                return out[: self._sample_rows]
            except Exception:
                pass
        if self._db is None:
            return []
        sql = f"SELECT * FROM {table} LIMIT {self._sample_rows}"
        rows, err = self._safe_query(sql)
        if err or not rows:
            return []
        return _rows_to_dicts(rows)[: self._sample_rows]

    def _row_count(self, table: str) -> Optional[int]:
        """Row count, preferring the system catalog (approximate, no scan).

        Order: (1) system-view estimate (TABLE_ROWS / reltuples / NUM_ROWS /
        partition stats) — fast, last-analyzed, no aggregation; (2) an exact
        COUNT(*) ONLY as a fallback for engines without a catalog stat (e.g.
        SQLite) and only when ``db_exact_row_counts`` is enabled.
        """
        if self._use_system_views:
            est = self._catalog_row_estimate(table)
            if est is not None:
                return est
        if self._exact_row_counts:
            return self._exact_row_count(table)
        return None

    def _catalog_row_estimate(self, table: str) -> Optional[int]:
        """Approximate row count from the engine's system catalog (no scan)."""
        sql = self._catalog_count_sql(self._effective_db_type(), table)
        if not sql:
            return None
        rows, err = self._run_select(sql)
        if err or not rows:
            return None
        return _first_int(rows)

    @staticmethod
    def _catalog_count_sql(db_type: str, table: str) -> str:
        """Per-engine system-catalog row-estimate query (or '' if unsupported).

        Table names originate from catalog introspection (not user free-text);
        single quotes are escaped defensively.
        """
        base = str(table).split(".")[-1]
        esc = base.replace("'", "''")
        if db_type in ("MySQL", "MariaDB"):
            return (
                "SELECT TABLE_ROWS FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{esc}'"
            )
        if db_type == "PostgreSQL":
            if "." in str(table):
                schema, tbl = str(table).split(".", 1)
            else:
                schema, tbl = "public", base
            schema_e = schema.replace("'", "''")
            tbl_e = tbl.replace("'", "''")
            return (
                "SELECT reltuples::bigint FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE n.nspname = '{schema_e}' AND c.relname = '{tbl_e}'"
            )
        if db_type == "Oracle":
            return (
                "SELECT NUM_ROWS FROM ALL_TABLES "
                f"WHERE TABLE_NAME = '{esc.upper()}'"
            )
        if db_type == "SQLServer":
            return (
                "SELECT SUM(p.rows) FROM sys.partitions p "
                f"WHERE p.object_id = OBJECT_ID('{esc}') "
                "AND p.index_id IN (0, 1)"
            )
        return ""

    def _exact_row_count(self, table: str) -> Optional[int]:
        """Exact COUNT(*) — fallback only; one query per table, capped."""
        if self._core is not None and self._connection \
                and hasattr(self._core, "count_table"):
            try:
                r = self._core.count_table(self._connection, table)
                if not r.get("error") and r.get("count") is not None:
                    return min(int(r["count"]), self._profile_row_cap)
            except Exception:
                pass
        rows, err = self._run_select(f"SELECT COUNT(*) FROM {table}")
        if err or not rows:
            return None
        val = _first_int(rows)
        return None if val is None else min(val, self._profile_row_cap)

    # ── system catalog semantics ─────────────────────────────────────────────
    def _resolve_system_catalog(self, profile: DbProfile) -> None:
        """Read declared keys/constraints/indexes from engine system catalogs."""
        if not self._use_system_views or not profile.tables:
            return
        db_type = self._effective_db_type()
        for sql in self._catalog_semantic_sql(db_type):
            rows, err = self._run_select(sql)
            if err or not rows:
                continue
            for row in rows:
                self._apply_catalog_row(profile, row)
        if db_type in ("SQLite", "sqlite"):
            self._resolve_sqlite_pragmas(profile)

    def _catalog_semantic_sql(self, db_type: str) -> list[str]:
        if db_type in ("MySQL", "MariaDB"):
            return [
                (
                    "SELECT k.TABLE_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, "
                    "k.REFERENCED_COLUMN_NAME, k.CONSTRAINT_NAME, t.CONSTRAINT_TYPE "
                    "FROM information_schema.KEY_COLUMN_USAGE k "
                    "JOIN information_schema.TABLE_CONSTRAINTS t "
                    "ON t.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA "
                    "AND t.TABLE_NAME = k.TABLE_NAME "
                    "AND t.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                    "WHERE k.TABLE_SCHEMA = DATABASE() "
                    "AND t.CONSTRAINT_TYPE IN ('PRIMARY KEY','FOREIGN KEY','UNIQUE')"
                ),
                (
                    "SELECT TABLE_NAME, COLUMN_NAME, NULL, NULL, INDEX_NAME, "
                    "CASE WHEN NON_UNIQUE = 0 THEN 'UNIQUE INDEX' ELSE 'INDEX' END "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE()"
                ),
            ]
        if db_type == "PostgreSQL":
            return [
                (
                    "SELECT tc.table_name, kcu.column_name, ccu.table_name, "
                    "ccu.column_name, tc.constraint_name, tc.constraint_type "
                    "FROM information_schema.table_constraints tc "
                    "LEFT JOIN information_schema.key_column_usage kcu "
                    "ON tc.constraint_name = kcu.constraint_name "
                    "AND tc.table_schema = kcu.table_schema "
                    "LEFT JOIN information_schema.constraint_column_usage ccu "
                    "ON ccu.constraint_name = tc.constraint_name "
                    "AND ccu.constraint_schema = tc.table_schema "
                    "WHERE tc.table_schema NOT IN ('pg_catalog','information_schema') "
                    "AND tc.constraint_type IN ('PRIMARY KEY','FOREIGN KEY','UNIQUE')"
                )
            ]
        if db_type == "Oracle":
            return [
                (
                    "SELECT cc.table_name, cc.column_name, rcc.table_name, "
                    "rcc.column_name, c.constraint_name, "
                    "CASE c.constraint_type WHEN 'P' THEN 'PRIMARY KEY' "
                    "WHEN 'R' THEN 'FOREIGN KEY' WHEN 'U' THEN 'UNIQUE' "
                    "WHEN 'C' THEN 'CHECK' ELSE c.constraint_type END "
                    "FROM all_constraints c "
                    "JOIN all_cons_columns cc ON c.owner = cc.owner "
                    "AND c.constraint_name = cc.constraint_name "
                    "LEFT JOIN all_cons_columns rcc ON c.r_owner = rcc.owner "
                    "AND c.r_constraint_name = rcc.constraint_name "
                    "WHERE c.constraint_type IN ('P','R','U','C')"
                )
            ]
        if db_type == "SQLServer":
            return [
                (
                    "SELECT OBJECT_NAME(fkc.parent_object_id), pc.name, "
                    "OBJECT_NAME(fkc.referenced_object_id), rc.name, fk.name, "
                    "'FOREIGN KEY' "
                    "FROM sys.foreign_key_columns fkc "
                    "JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id "
                    "JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id "
                    "AND pc.column_id = fkc.parent_column_id "
                    "JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id "
                    "AND rc.column_id = fkc.referenced_column_id"
                ),
                (
                    "SELECT OBJECT_NAME(kc.parent_object_id), c.name, NULL, NULL, "
                    "kc.name, kc.type_desc "
                    "FROM sys.key_constraints kc "
                    "JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id "
                    "AND ic.index_id = kc.unique_index_id "
                    "JOIN sys.columns c ON c.object_id = ic.object_id "
                    "AND c.column_id = ic.column_id"
                ),
            ]
        return []

    def _resolve_sqlite_pragmas(self, profile: DbProfile) -> None:
        for table in profile.tables:
            rows, err = self._run_select(f"PRAGMA foreign_key_list('{table.name}')")
            if not err and rows:
                for row in rows:
                    vals = _row_values(row)
                    if len(vals) >= 5:
                        self._add_relationship({
                            "from_table": table.name,
                            "from_column": str(vals[3]),
                            "to_table": str(vals[2]),
                            "to_column": str(vals[4]),
                            "constraint": f"sqlite_fk_{vals[0]}",
                            "kind": "N:1",
                            "source": "declared",
                            "confidence": 1.0,
                        })
            rows, err = self._run_select(f"PRAGMA index_list('{table.name}')")
            if err or not rows:
                continue
            for row in rows:
                vals = _row_values(row)
                if len(vals) < 3:
                    continue
                idx_name = str(vals[1])
                is_unique = str(vals[2]) in ("1", "True", "true")
                cols, _ = self._run_select(f"PRAGMA index_info('{idx_name}')")
                names = [str(v[2]) for v in (_row_values(c) for c in (cols or [])) if len(v) >= 3]
                for name in names:
                    col = _find_col(table, name)
                    if col:
                        col.indexed = True
                        col.unique = col.unique or is_unique
                if is_unique and names and names not in table.unique_keys:
                    table.unique_keys.append(names)

    def _apply_catalog_row(self, profile: DbProfile, row: Any) -> None:
        vals = _row_values(row)
        if len(vals) < 6:
            return
        table_name, column_name, ref_table, ref_col, constraint, ctype = [str(v or "") for v in vals[:6]]
        table = _find_table(profile, table_name)
        if table is None or not column_name:
            return
        col = _find_col(table, column_name)
        ctype_u = ctype.upper()
        if constraint and constraint not in table.constraints:
            table.constraints.append(constraint)
        if "PRIMARY" in ctype_u:
            if column_name not in table.primary_key:
                table.primary_key.append(column_name)
            if col:
                col.is_pk = True
                col.indexed = True
        elif "FOREIGN" in ctype_u and ref_table:
            self._add_relationship({
                "from_table": table.name,
                "from_column": column_name,
                "to_table": str(ref_table).split(".")[-1],
                "to_column": ref_col or "id",
                "constraint": constraint,
                "kind": "N:1",
                "source": "declared",
                "confidence": 1.0,
            })
            if col:
                col.is_fk = True
                col.indexed = True
        elif "UNIQUE" in ctype_u:
            if [column_name] not in table.unique_keys:
                table.unique_keys.append([column_name])
            if col:
                col.unique = True
                col.indexed = True
        elif "INDEX" in ctype_u:
            if constraint and constraint not in table.indexes:
                table.indexes.append(constraint)
            if col:
                col.indexed = True
                col.unique = col.unique or "UNIQUE" in ctype_u

    def _add_relationship(self, rel: dict[str, Any]) -> None:
        key = (
            str(rel.get("from_table") or "").lower(),
            str(rel.get("from_column") or "").lower(),
            str(rel.get("to_table") or "").lower(),
            str(rel.get("to_column") or "").lower(),
        )
        if not all(key):
            return
        profile = getattr(self, "_active_profile", None)
        if profile is None:
            # _apply_table_schema_details runs before a profile-scoped catalog
            # pass, so stash on the instance and merge after the profile is known.
            pending = getattr(self, "_pending_relationships", [])
            if key not in {
                (
                    str(r.get("from_table") or "").lower(),
                    str(r.get("from_column") or "").lower(),
                    str(r.get("to_table") or "").lower(),
                    str(r.get("to_column") or "").lower(),
                )
                for r in pending
            }:
                pending.append(rel)
                self._pending_relationships = pending
            return
        if key not in {
            (
                str(r.get("from_table") or "").lower(),
                str(r.get("from_column") or "").lower(),
                str(r.get("to_table") or "").lower(),
                str(r.get("to_column") or "").lower(),
            )
            for r in profile.relationships
        }:
            profile.relationships.append(rel)

    def _effective_db_type(self) -> str:
        if self._db_type_cache is None:
            self._db_type_cache = self._db_type()
        return self._db_type_cache

    def _run_select(self, sql: str):
        """Run a read-only SELECT via the live manager or the core service.

        Returns ``(rows, error)`` where ``rows`` is always a plain list of row
        tuples/dicts. Normalizes both manager shapes — the real
        ``DatabaseManager.execute_query`` returns ``({columns, rows}, error)``
        while lightweight stubs return ``([rows], error)`` — and the core
        service's normalized ``execute`` dict.
        """
        from common.sql_guard import assert_read_only

        guard_err = assert_read_only(sql, db_type=self._effective_db_type())
        if guard_err:
            return None, guard_err
        if self._db is not None:
            raw, err = self._safe_query(sql)
            if err:
                return None, str(err)
            return self._rows_of(raw), ""
        if self._core is not None and self._connection \
                and hasattr(self._core, "execute"):
            try:
                r = self._core.execute(self._connection, sql)
                if r.get("error"):
                    return None, r["error"]
                return r.get("rows") or [], ""
            except Exception as exc:  # noqa: BLE001
                return None, str(exc)
        return None, "no-connection"

    @staticmethod
    def _rows_of(raw: Any) -> list:
        if raw is None:
            return []
        if isinstance(raw, dict):
            return raw.get("rows") or []
        if isinstance(raw, (list, tuple)):
            return list(raw)
        return []

    def _null_ratio(self, table: str, column: str) -> Optional[float]:
        if not column:
            return None
        sql = (
            f"SELECT SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END), "
            f"COUNT(*) FROM {table}"
        )
        rows, err = self._run_select(sql)
        if err or not rows:
            return None
        try:
            row = rows[0]
            nulls = row[0] if isinstance(row, (list, tuple)) else list(row.values())[0]
            total = row[1] if isinstance(row, (list, tuple)) else list(row.values())[1]
            total = min(int(total or 0), self._profile_row_cap)
            if total <= 0:
                return None
            return round(int(nulls or 0) / total, 4)
        except (ValueError, TypeError, IndexError, KeyError):
            return None

    def _distinct_estimate(self, table: str, column: str) -> Optional[int]:
        if not column:
            return None
        cap = min(self._profile_row_cap, 500)
        sql = f"SELECT COUNT(DISTINCT {column}) FROM {table}"
        rows, err = self._run_select(sql)
        if err or not rows:
            return None
        val = _first_int(rows)
        return None if val is None else min(val, cap)

    def _safe_query(self, sql: str):
        try:
            return self._db.execute_query(sql)
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)


def _first_int(rows: Any) -> Optional[int]:
    """First scalar of the first row coerced to int, or None."""
    try:
        first = rows[0]
        if isinstance(first, (list, tuple)):
            val = first[0]
        elif isinstance(first, dict):
            val = list(first.values())[0]
        else:
            val = first
        if val is None or str(val).strip() == "":
            return None
        return int(float(val))
    except (ValueError, TypeError, IndexError, KeyError):
        return None


def _row_name(row: Any) -> str:
    if isinstance(row, str):
        return row.strip()
    if isinstance(row, (list, tuple)) and row:
        return str(row[0]).strip()
    if isinstance(row, dict):
        for k in ("name", "table", "TABLE_NAME", "view", "trigger"):
            if row.get(k):
                return str(row[k]).strip()
    return str(row or "").strip()


def _find_table(profile: DbProfile, name: str) -> TableProfile | None:
    low = str(name or "").split(".")[-1].lower()
    for table in profile.tables:
        if table.name.lower() == low:
            return table
    return None


def _find_col(table: TableProfile, name: str) -> ColumnProfile | None:
    low = str(name or "").lower()
    for col in table.columns:
        if col.name.lower() == low:
            return col
    return None


def _row_values(row: Any) -> list[Any]:
    if isinstance(row, dict):
        preferred = [
            "table_name", "TABLE_NAME", "table", "column_name", "COLUMN_NAME",
            "referenced_table_name", "REFERENCED_TABLE_NAME",
            "referenced_column_name", "REFERENCED_COLUMN_NAME",
            "constraint_name", "CONSTRAINT_NAME",
            "constraint_type", "CONSTRAINT_TYPE",
        ]
        if any(k in row for k in preferred):
            return [
                row.get("table_name") or row.get("TABLE_NAME") or row.get("table"),
                row.get("column_name") or row.get("COLUMN_NAME") or row.get("column"),
                row.get("referenced_table_name") or row.get("REFERENCED_TABLE_NAME")
                or row.get("ref_table"),
                row.get("referenced_column_name") or row.get("REFERENCED_COLUMN_NAME")
                or row.get("ref_column"),
                row.get("constraint_name") or row.get("CONSTRAINT_NAME")
                or row.get("index_name") or row.get("INDEX_NAME"),
                row.get("constraint_type") or row.get("CONSTRAINT_TYPE")
                or row.get("type") or row.get("TYPE"),
            ]
        return list(row.values())
    if isinstance(row, (list, tuple)):
        return list(row)
    return [row]


def _names_from_any(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, dict):
        return _names_from_any(value.get("columns") or value.get("name"))
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            if isinstance(item, dict):
                name = item.get("name") or item.get("column") or item.get("column_name")
                if name:
                    out.append(str(name).strip())
            elif isinstance(item, (list, tuple)) and item:
                out.append(str(item[0]).strip())
            elif item is not None:
                out.append(str(item).strip())
        return [n for n in out if n]
    return [str(value).strip()]


def _foreign_keys_from_any(value: Any) -> list[dict[str, Any]]:
    if not value:
        return []
    rows = value if isinstance(value, (list, tuple)) else [value]
    out: list[dict[str, Any]] = []
    for item in rows:
        if isinstance(item, dict):
            out.append({
                "from_table": item.get("from_table") or item.get("table"),
                "from_column": item.get("from_column") or item.get("column")
                or item.get("column_name"),
                "to_table": item.get("to_table") or item.get("ref_table")
                or item.get("referenced_table"),
                "to_column": item.get("to_column") or item.get("ref_column")
                or item.get("referenced_column") or "id",
                "constraint": item.get("name") or item.get("constraint_name"),
            })
        elif isinstance(item, (list, tuple)) and len(item) >= 4:
            out.append({
                "from_column": item[0],
                "to_table": item[1],
                "to_column": item[2],
                "constraint": item[3],
            })
        elif isinstance(item, str):
            # Best-effort parse of labels like "orders.customer_id -> customers.id".
            parts = item.replace("REFERENCES", "->").split("->")
            if len(parts) == 2:
                left = parts[0].strip().split(".")
                right = parts[1].strip().split(".")
                out.append({
                    "from_table": left[-2] if len(left) >= 2 else "",
                    "from_column": left[-1],
                    "to_table": right[-2] if len(right) >= 2 else right[0],
                    "to_column": right[-1] if len(right) >= 2 else "id",
                    "constraint": "",
                })
    return out


def _unique_groups_from_any(value: Any) -> list[list[str]]:
    if not value:
        return []
    rows = value if isinstance(value, (list, tuple)) else [value]
    groups: list[list[str]] = []
    for item in rows:
        names = _names_from_any(item)
        if names:
            groups.append(names)
    return groups


def _index_columns(item: Any) -> list[str]:
    if isinstance(item, dict):
        return _names_from_any(item.get("columns") or item.get("column")
                               or item.get("column_name"))
    if isinstance(item, str):
        if "(" in item and ")" in item:
            inside = item.split("(", 1)[1].split(")", 1)[0]
            return [p.strip(" `\"") for p in inside.split(",") if p.strip()]
        return []
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        return [str(item[-1]).strip()]
    return []


def _index_is_unique(item: Any) -> bool:
    if isinstance(item, dict):
        return bool(item.get("unique") or item.get("non_unique") == 0
                    or str(item.get("type", "")).upper() == "UNIQUE")
    return "unique" in str(item).lower()


def _short_label(row: Any) -> str:
    if isinstance(row, str):
        return row
    if isinstance(row, (list, tuple)):
        return " | ".join(str(x) for x in row[:4])
    if isinstance(row, dict):
        return " | ".join(f"{k}={v}" for k, v in list(row.items())[:4])
    return str(row)


def _rows_to_dicts(rows: Any) -> list[dict]:
    out: list[dict] = []
    for r in list(rows)[:50]:
        if isinstance(r, dict):
            out.append({str(k): _scalar(v) for k, v in r.items()})
        elif isinstance(r, (list, tuple)):
            out.append({f"c{i}": _scalar(v) for i, v in enumerate(r)})
        else:
            out.append({"value": _scalar(r)})
    return out


def _scalar(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)
