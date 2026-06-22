"""
Schema converter service layer.

Owns the actual conversion / DDL-dump logic.  Connections are resolved through
the shared core (a callable that maps a saved connection name to a connected
``DatabaseManager``); by default it uses the core ``DBService``.
"""

from __future__ import annotations

from typing import Callable, Optional

from .converter import SchemaConverter, ConversionValidator, SchemaComparer, DataComparer
from common.config_loader import get_compare_sample_size
from schema_converter.compare_options import DataCompareOptions


class SchemaService:
    """Headless API for schema conversion and native DDL dumps."""

    def __init__(self, connect: Optional[Callable] = None):
        # connect(name) -> DatabaseManager (connected).  Injected by the caller
        # so the module never hard-depends on a particular core service object.
        self._connect = connect
        self._svc = None

    def _resolve(self, name: str):
        if self._connect is not None:
            return self._connect(name)
        if self._svc is None:
            from common.headless.db_service import CoreDBService

            self._svc = CoreDBService()
        return self._svc.get_manager(name)

    # ------------------------------------------------------------------
    def convert(
        self,
        source_conn: str,
        target_db_type: str,
        table: str,
        naming: "TargetNaming | None" = None,
        table_name_map: dict | None = None,
        type_map: str = "",
    ) -> dict:
        """Convert *table* from *source_conn*'s engine to *target_db_type* DDL.

        *naming* (a :class:`schema_converter.table_naming.TargetNaming`) carries
        the target database/schema + prefix/suffix; when given, the generated DDL
        is qualified for the target (``source_schema.t`` -> ``target_db.prefix_t``),
        matching the UI's "Target database/schema" + prefix/suffix behaviour. An
        empty/omitted *naming* leaves names unqualified.

        *table_name_map* (optional) qualifies cross-table references (e.g. FKs)
        in a batch; when omitted a single-table map is derived from *naming*.
        """
        try:
            from common.db_manager import DatabaseManager
            from schema_converter.table_naming import TargetNaming
            from schema_converter.type_overrides import resolve_type_overrides

            naming = TargetNaming.from_source(naming)

            src_mgr = self._resolve(source_conn)
            target_mgr = DatabaseManager(target_db_type)

            converter = SchemaConverter(src_mgr, target_mgr)
            source_schema = converter.get_table_schema(table)
            if not source_schema:
                return {"error": f"Table '{table}' not found.", "ddl": None,
                        "indexes_ddl": [], "issues": []}

            target_table = naming.qualify(table)
            full_map = dict(table_name_map or {})
            full_map.setdefault(table, target_table)
            convert_kwargs = {
                "type_overrides": resolve_type_overrides(type_map or None),
            }
            if any(v != k for k, v in full_map.items()):
                convert_kwargs["table_name_map"] = full_map
            converted = converter.convert_schema(source_schema, **convert_kwargs)
            if target_table != table:
                converted["table_name"] = target_table
            ddl_list = converter.generate_all_table_ddl(converted)
            ddl = ddl_list[0] if ddl_list else ""
            idx_ddl = ddl_list[1:] if len(ddl_list) > 1 else []
            issues = ConversionValidator.validate_schema_conversion(
                source_schema, converted
            )
            return {"error": None, "ddl": ddl, "indexes_ddl": idx_ddl,
                    "all_ddl": ddl_list, "issues": issues,
                    "target_table": target_table}
        except Exception as exc:
            return {"error": str(exc), "ddl": None, "indexes_ddl": [], "issues": []}

    # ------------------------------------------------------------------
    def dump(self, name: str, table: str | None = None) -> dict:
        """Return native CREATE TABLE/INDEX DDL for *table* (or all tables)."""
        try:
            from common.db_manager import DatabaseManager

            mgr = self._resolve(name)
            db_type = getattr(mgr, "db_type", "")
            target = DatabaseManager(db_type)

            converter = SchemaConverter(mgr, target)
            tables: list[str] = [table] if table else (
                mgr.execute_operation("getTables") or []
            )
            statements: list[str] = []
            for tbl in tables:
                src = converter.get_table_schema(tbl)
                if not src:
                    continue
                ddl = converter.generate_create_table_ddl(src)
                if ddl:
                    statements.append(ddl.strip())
                for idx in (converter.generate_indexes_ddl(src) or []):
                    if idx:
                        statements.append(idx.strip())
            return {"error": None, "ddl": "\n\n".join(statements),
                    "table_count": len(tables)}
        except Exception as exc:
            return {"error": str(exc), "ddl": "", "table_count": 0}

    # ------------------------------------------------------------------
    def compare_schema(
        self,
        source_conn: str,
        target_conn: str,
        table: str,
        target_table: str | None = None,
    ) -> dict:
        """Compare live schema of *table* on source vs target connection."""
        try:
            src_mgr = self._resolve(source_conn)
            tgt_mgr = self._resolve(target_conn)
            tgt_table = target_table or table
            result = SchemaComparer.compare_tables(
                src_mgr, tgt_mgr, table, tgt_table
            )
            return {"error": result.get("error"), **result}
        except Exception as exc:
            return {"error": str(exc), "match": False, "issues": []}

    # ------------------------------------------------------------------
    def compare_data(
        self,
        source_conn: str,
        target_conn: str,
        table: str,
        target_table: str | None = None,
        options: DataCompareOptions | None = None,
        **legacy_options,
    ) -> dict:
        """Compare row-by-row data for *table* between source and target."""
        options = DataCompareOptions.from_source(options or {
            **legacy_options,
            "target_table": target_table,
        })
        mode = options.mode
        try:
            src_mgr = self._resolve(source_conn)
            tgt_mgr = self._resolve(target_conn)
            tgt_table = options.target_table or table
            comparer = DataComparer(src_mgr, tgt_mgr)
            sample_size = options.sample_size
            if sample_size is None:
                sample_size = get_compare_sample_size()
            result = comparer.compare_table_data(
                table,
                options=DataCompareOptions(
                    target_table=tgt_table,
                    mode=mode,
                    sample_size=sample_size,
                    stop_event=options.stop_event,
                    batch_size=options.batch_size,
                ),
            )
            return {"error": result.get("error"), **result}
        except Exception as exc:
            return {"error": str(exc), "match": False, "mode": mode}
