"""
Schema conversion bridge for full-tool service composition.

Schema logic lives in :class:`schema_converter.service.SchemaService`; this
thin adapter exposes ``convert_schema`` / ``dump_schema`` on a core service.
"""

from __future__ import annotations

from typing import Any

from schema_converter.compare_options import DataCompareOptions
from schema_converter.table_naming import TargetNaming
from schema_converter.transfer_options import (
    ParallelTransferContext,
    TransferMultiRequest,
    TransferOptions,
    TransferRequest,
    TransferRuntime,
    merge_options,
    options_from_config,
)


def make_service(core=None):
    """Core + schema composite for module-only CLI/API."""
    from common.headless.composite import composite_service
    from common.headless.db_service import CoreDBService

    core = core or CoreDBService()
    return composite_service(core, SchemaBridge(core))


class SchemaBridge:
    """Expose schema convert/dump on a core connection service."""

    def __init__(self, core: Any):
        self._core = core
        self._schema = None

    def _schema_service(self):
        if self._schema is None:
            from schema_converter import SchemaService

            self._schema = SchemaService(connect=self._core.get_manager)
        return self._schema

    @staticmethod
    def _build_transfer_options(
        override: TransferOptions | None = None,
    ) -> TransferOptions:
        """Merge per-run transfer flags onto saved migrator config defaults."""
        return merge_options(options_from_config(), override)

    def validate_migration(
        self,
        source_conn: str,
        target_conn: str,
        tables: list[str],
        *,
        naming: TargetNaming | None = None,
        type_map: str = "",
    ) -> dict:
        """Pre-migration dry-run validation report (G5). Read-only.

        *naming* (:class:`schema_converter.table_naming.TargetNaming`) supplies
        the target database/prefix/suffix used to qualify each target table name.
        """
        try:
            from schema_converter.migration_validation import validate_migration
            from schema_converter.type_overrides import resolve_type_overrides

            naming = TargetNaming.from_source(naming)
            src_mgr = self._core.get_manager(source_conn)
            tgt_mgr = self._core.get_manager(target_conn)
            overrides = resolve_type_overrides(type_map or None)
            pairs = [
                (t, naming.qualify(t))
                for t in (tables or [])
                if str(t).strip()
            ]
            report = validate_migration(
                src_mgr, tgt_mgr, pairs, type_overrides=overrides
            )
            report["source_conn"] = source_conn
            report["target_conn"] = target_conn
            return report
        except Exception as exc:
            return {"ok": False, "error": str(exc), "tables": []}

    def convert_schema(
        self,
        source_conn: str,
        target_db_type: str,
        table: str,
        naming: TargetNaming | None = None,
        table_name_map: dict | None = None,
        type_map: str = "",
    ) -> dict:
        return self._schema_service().convert(
            source_conn, target_db_type, table,
            naming=TargetNaming.from_source(naming),
            table_name_map=table_name_map,
            type_map=type_map,
        )

    def dump_schema(self, name: str, table: str | None = None) -> dict:
        return self._schema_service().dump(name, table)

    def compare_schema(
        self,
        source_conn: str,
        target_conn: str,
        table: str,
        target_table: str | None = None,
    ) -> dict:
        return self._schema_service().compare_schema(
            source_conn, target_conn, table, target_table
        )

    def compare_data(
        self,
        source_conn: str,
        target_conn: str,
        table: str,
        target_table: str | None = None,
        options: DataCompareOptions | None = None,
        **legacy_options,
    ) -> dict:
        if options is None:
            options = DataCompareOptions.from_source({
                **legacy_options,
                "target_table": target_table,
            })
        elif target_table and not options.target_table:
            options = DataCompareOptions(
                target_table=target_table,
                mode=options.mode,
                sample_size=options.sample_size,
                stop_event=options.stop_event,
                batch_size=options.batch_size,
            )
        return self._schema_service().compare_data(
            source_conn,
            target_conn,
            table,
            target_table=target_table,
            options=options,
        )

    def get_table_schema(self, name: str, table: str) -> dict:
        return self._core.get_table_schema(name, table)

    # ------------------------------------------------------------------
    # Parity additions (Phase 4) — batch convert, apply DDL on target,
    # transfer data, multi-table row counts and sample data.
    # ------------------------------------------------------------------

    def convert_schema_multi(
        self,
        source_conn: str,
        target_db_type: str,
        tables: list[str],
        naming: TargetNaming | None = None,
        type_map: str = "",
    ) -> dict:
        """Convert several tables in one call. Returns
        ``{error, target_type, tables: [{table, target_table, ddl, indexes_ddl,
        all_ddl, error, issues}], joined_ddl}``.

        When *naming* qualifies target names, all target table names are
        qualified and a shared table-name map is passed to each conversion so
        cross-table references (foreign keys) resolve to the qualified names.
        """
        naming = TargetNaming.from_source(naming)

        name_map = {
            t: naming.qualify(t)
            for t in (tables or [])
        }
        out: list[dict] = []
        for table in tables or []:
            r = self.convert_schema(
                source_conn, target_db_type, table,
                naming=naming,
                table_name_map=name_map,
                type_map=type_map,
            )
            out.append({
                "table": table,
                "target_table": r.get("target_table") or name_map.get(table, table),
                "ddl": r.get("ddl") or "",
                "indexes_ddl": r.get("indexes_ddl") or [],
                "all_ddl": r.get("all_ddl") or [],
                "issues": r.get("issues") or [],
                "error": r.get("error"),
            })
        joined_parts: list[str] = []
        for row in out:
            for part in row.get("all_ddl") or []:
                p = (part or "").strip()
                if p:
                    # Terminate each statement so the apply-step splitter (which
                    # splits on ';') can separate multi-statement DDL correctly.
                    if not p.endswith(";"):
                        p += ";"
                    joined_parts.append(p)
        first_err = next((row["error"] for row in out if row.get("error")), None)
        return {
            "error": first_err,
            "target_type": target_db_type,
            "tables": out,
            "joined_ddl": "\n\n".join(joined_parts),
        }

    @staticmethod
    def _is_index_ddl(stmt: str) -> bool:
        u = stmt.strip().upper()
        return u.startswith("CREATE INDEX") or u.startswith("CREATE UNIQUE INDEX")

    @staticmethod
    def _create_table_target(stmt: str) -> str:
        """Extract the table name from a CREATE TABLE statement, or ''."""
        import re

        m = re.match(
            r"\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
            stmt, re.IGNORECASE,
        )
        return m.group(1).strip() if m else ""

    def apply_ddl_to_target(
        self,
        target_conn: str,
        ddl: str,
        stop_on_error: bool = True,
        create_indexes: bool = True,
        drop_if_exists: bool = False,
    ) -> dict:
        """Execute a (possibly multi-statement) DDL blob against *target_conn*.

        Mirrors the UI's "Create target schemas" step. Splits on ``;`` via the
        core's SQL splitter so multi-statement input is supported even when the
        driver itself doesn't. Returns ``{error, executed, failed,
        statements: [{sql, ok, error}]}``.

        * ``create_indexes=False`` skips ``CREATE INDEX`` / ``CREATE UNIQUE
          INDEX`` statements (mirrors the Tk "Create Indexes" checkbox).
        * ``drop_if_exists=True`` runs ``DROP TABLE IF EXISTS <t>`` before each
          ``CREATE TABLE`` (mirrors the Tk "Drop Table If Exists" checkbox).
        """
        if not (ddl or "").strip():
            return {
                "error": "No DDL to apply.",
                "executed": 0,
                "failed": 0,
                "statements": [],
                "ok": False,
                "partial": False,
            }
        try:
            statements = self._core._split_sql_statements(ddl)
        except Exception:
            statements = [s for s in (ddl or "").split(";") if s.strip()]

        # Expand statements with optional DROP-before-CREATE and index filter.
        prepared: list[str] = []
        for stmt in statements:
            if not create_indexes and self._is_index_ddl(stmt):
                continue
            if drop_if_exists:
                target = self._create_table_target(stmt)
                if target:
                    prepared.append(f"DROP TABLE IF EXISTS {target}")
            prepared.append(stmt)

        results: list[dict] = []
        executed = 0
        failed = 0
        first_err: str | None = None
        for stmt in prepared:
            res = self._core.execute(target_conn, stmt)
            err = res.get("error")
            results.append({"sql": stmt, "ok": not err, "error": err})
            if err:
                failed += 1
                if first_err is None:
                    first_err = err
                if stop_on_error and not stmt.strip().upper().startswith("DROP TABLE IF EXISTS"):
                    break
            else:
                executed += 1
        return {
            "error": first_err,
            "executed": executed,
            "failed": failed,
            "statements": results,
            "ok": failed == 0,
            "partial": executed > 0 and failed > 0,
        }

    def transfer_data(
        self,
        request: TransferRequest,
        options: TransferOptions | None = None,
    ) -> dict:
        """Copy rows from ``table`` on *source_conn* to ``target_table`` on
        *target_conn*. Mirrors the UI's "Transfer data only" button.

        If *target_table* is not given but request naming options are present,
        the target name is qualified the same way the UI does.

        Optional keyword arguments enable row filtering (G1), column
        subset/rename (G2), continue-on-error (G3), overflow policy (G4),
        null/bool normalization (G6), timezone handling (G7), sequence reset
        (G8), checkpoint/resume (G9) and a report artifact (G10).

        Returns ``{ok, rows_transferred, skipped, source_table, target_table,
        message}``.
        """
        target_table = request.target_table
        if not target_table:
            target_table = request.naming.qualify(request.table)
        target_table = target_table or request.table
        stats: dict = {}
        try:
            src_mgr = self._core.get_manager(request.source_conn)
            tgt_mgr = self._core.get_manager(request.target_conn)
            from schema_converter.adapters import transfer_object, validate_migration_pair

            err = validate_migration_pair(
                src_mgr.db_type, tgt_mgr.db_type, operation="transfer"
            )
            if err:
                return {
                    "ok": False,
                    "rows_transferred": 0,
                    "source_table": request.table,
                    "target_table": target_table,
                    "message": err,
                }

            options = self._build_transfer_options(options)
            checkpoint_store = None
            if options.checkpoint:
                from schema_converter.migration_report import CheckpointStore

                checkpoint_store = CheckpointStore(
                    CheckpointStore.default_path(
                        request.source_conn, request.target_conn)
                )

            runtime = TransferRuntime(
                batch_size=request.batch_size,
                options=options,
                checkpoint_store=checkpoint_store,
                stats_out=stats,
            )
            rows = transfer_object(
                src_mgr,
                tgt_mgr,
                request.table,
                target_table,
                runtime=runtime,
            )
            count = int(rows or 0)
            skipped = int(stats.get("skipped") or 0)
            # Only clear the checkpoint when the transfer completed without
            # interruption. A stop_event mid-run means rows are only partially
            # transferred; keeping the checkpoint lets the next run resume.
            stop_ev = runtime.stop_event
            was_stopped = stop_ev is not None and stop_ev.is_set()
            if checkpoint_store is not None and not was_stopped:
                checkpoint_store.clear(request.table, target_table)

            result = {
                "ok": True,
                "rows_transferred": count,
                "skipped": skipped,
                "errors": stats.get("error_count", 0),
                "source_table": request.table,
                "target_table": target_table,
                "message": (
                    f"Transferred {count} row(s) from "
                    f"{request.table} → {target_table}."
                    + (f" Skipped {skipped}." if skipped else "")
                ),
            }

            if options.report_path:
                from schema_converter.migration_report import MigrationReport

                report = MigrationReport(
                    options.report_path,
                    source_conn=request.source_conn,
                    target_conn=request.target_conn,
                )
                report.add_table(
                    {
                        "source_table": request.table,
                        "target_table": target_table,
                        "ok": True,
                        "rows_transferred": count,
                        "skipped": skipped,
                        "error_count": stats.get("error_count", 0),
                        "duration_seconds": stats.get("duration_seconds"),
                        "errors": (stats.get("errors") or [])[:50],
                    }
                )
                result["report_path"] = report.write()
            return result
        except Exception as exc:
            rows_transferred = int(stats.get("rows_transferred") or 0)
            skipped = int(stats.get("skipped") or 0)
            return {
                "ok": False,
                "rows_transferred": rows_transferred,
                "skipped": skipped,
                "errors": int(stats.get("error_count") or 0),
                "source_table": request.table,
                "target_table": target_table,
                "message": str(exc),
                "partial": rows_transferred > 0,
            }

    def transfer_data_multi(
        self,
        request: TransferMultiRequest,
        options: TransferOptions | None = None,
    ) -> dict:
        """Copy rows for multiple tables, optionally in parallel.

        Parallel workers open fresh, task-scoped source/target DB sessions
        through ``CoreDBService.open_session`` so transactions stay isolated and
        the cached connection for SQL Editor/API requests is not shared.

        Row filter (WHERE) and column subset are single-table only and are not
        accepted here. Column rename (``column_map``) and the row ``limit`` are
        applied to every table (a rename is a no-op for tables that lack a
        listed source column). Fixed-value policies (continue-on-error,
        overflow, null/bool, timezone, sequence reset) layer onto saved migrator
        config defaults.
        """
        try:
            from schema_converter import module_config
            from schema_converter.parallel_transfer import (
                build_transfer_specs,
                run_parallel_transfer,
            )
            if not request.tables:
                return {"ok": False, "error": "No tables supplied.", "tables": []}
            worker_count = int(
                request.workers
                if request.workers is not None
                else module_config.get_int(
                    "schema.conversion", "parallel_workers", default=1
                )
            )
            if not request.parallel:
                worker_count = 1
            specs = build_transfer_specs(
                request.tables,
                request.naming.qualify,
            )

            options = self._build_transfer_options(options)
            checkpoint_store = None
            if options.checkpoint:
                from schema_converter.migration_report import CheckpointStore

                checkpoint_store = CheckpointStore(
                    CheckpointStore.default_path(
                        request.source_conn, request.target_conn)
                )

            def _open(core_name: str):
                if hasattr(self._core, "open_session"):
                    return self._core.open_session(core_name)
                # Fallback for custom test composites; not parallel-safe for real
                # use, but keeps legacy injected cores working in serial mode.
                return self._core.get_manager(core_name)

            context = ParallelTransferContext(
                source_conn=request.source_conn,
                target_conn=request.target_conn,
                source_manager_factory=_open,
                target_manager_factory=_open,
                batch_size=request.batch_size,
                workers=worker_count,
                checkpoint_store=checkpoint_store,
            )
            result = run_parallel_transfer(
                specs,
                context,
                options=options,
            )
            result["source_conn"] = request.source_conn
            result["target_conn"] = request.target_conn
            result["parallel"] = bool(request.parallel and worker_count > 1)

            if options.report_path:
                from schema_converter.migration_report import MigrationReport

                report = MigrationReport(
                    options.report_path,
                    source_conn=request.source_conn,
                    target_conn=request.target_conn,
                )
                for row in result.get("tables") or []:
                    report.add_table(
                        {
                            "source_table": row.get("source_table"),
                            "target_table": row.get("target_table"),
                            "ok": row.get("ok"),
                            "rows_transferred": row.get("rows_transferred", 0),
                            "skipped": row.get("skipped", 0),
                            "error_count": row.get("error_count", 0),
                            "duration_seconds": row.get("duration_seconds"),
                            "source_count": row.get("source_count"),
                            "target_count": row.get("target_count"),
                            "error": row.get("error"),
                        }
                    )
                result["report_path"] = report.write()
            return result
        except Exception as exc:
            return {
                "ok": False,
                "error": str(exc),
                "tables": [],
                "successful": 0,
                "failed": len(request.tables or []),
                "total_rows": 0,
                "workers": 0,
                "parallel": bool(request.parallel),
            }

    def count_rows_multi(self, name: str, tables: list[str]) -> dict:
        """Row-count report for many tables. Mirrors the UI's
        "Show row counts" button. Reuses :meth:`CoreDBService.count_table`.
        """
        if not hasattr(self._core, "count_table"):
            return {"error": "count_table not supported by core service.",
                    "tables": []}
        out: list[dict] = []
        total = 0
        first_err: str | None = None
        for tbl in tables or []:
            r = self._core.count_table(name, tbl)
            err = r.get("error")
            if err and first_err is None:
                first_err = err
            count = int(r.get("count") or 0) if not err else 0
            total += count
            out.append({"table": tbl, "count": count, "error": err})
        return {"error": first_err, "tables": out, "total": total}

    def sample_rows_multi(
        self,
        name: str,
        tables: list[str],
        limit: int = 1,
    ) -> dict:
        """Sample rows for many tables in one call (UI's "Show sample data")."""
        if not hasattr(self._core, "sample_table"):
            return {"error": "sample_table not supported by core service.",
                    "tables": []}
        out: list[dict] = []
        first_err: str | None = None
        for tbl in tables or []:
            r = self._core.sample_table(name, tbl, limit)
            err = r.get("error")
            if err and first_err is None:
                first_err = err
            out.append({
                "table": tbl,
                "columns": r.get("columns") or [],
                "rows": r.get("rows") or [],
                "rowcount": int(r.get("rowcount") or 0),
                "error": err,
            })
        return {"error": first_err, "tables": out}
