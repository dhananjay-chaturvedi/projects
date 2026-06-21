"""
REST API surface for the Data Migration module (package: schema_converter).

Routes are served under the ``/api/migrator`` prefix and cover schema
conversion, DDL apply, data transfer, and schema/data validation.
Module-only API (``common/`` + ``schema_converter/``) — no app/ required.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from schema_converter.compare_options import DataCompareOptions
from schema_converter.table_naming import TargetNaming
from schema_converter.transfer_options import (
    TransferMultiRequest,
    TransferRequest,
    options_from_mapping,
)


class SchemaConvertRequest(BaseModel):
    source_conn: str = Field(..., examples=["my_mysql"])
    target_type: str = Field(..., examples=["PostgreSQL"])
    table: str = Field(..., examples=["users"])
    target_db: str = Field(
        "", examples=["test"],
        description="Target database/schema to qualify generated table names "
                    "(test -> CREATE TABLE test.users). Required for "
                    "MySQL/MariaDB targets without a default database.",
    )
    prefix: str = Field("", examples=["mig_"])
    suffix: str = Field("", examples=["_copy"])
    type_map: str = Field(
        "",
        examples=['varchar2:text,int:decimal'],
        description='Optional type override rules: "source_type:target_type, ..."',
    )


class SchemaCompareRequest(BaseModel):
    source_conn: str = Field(..., examples=["source_mysql"])
    target_conn: str = Field(..., examples=["target_pg"])
    table: str = Field(..., examples=["users"])
    target_table: str = Field("", examples=["users_migrated"])


class DataCompareRequest(BaseModel):
    source_conn: str = Field(..., examples=["source_mysql"])
    target_conn: str = Field(..., examples=["target_pg"])
    table: str = Field(..., examples=["users"])
    target_table: str = Field("", examples=["users_migrated"])
    mode: str = Field("sample", examples=["sample", "full"])
    sample_size: int | None = Field(
        None,
        ge=1,
        le=10000,
        description=(
            "Rows per table in sample mode; omit to use "
            "properties.ini schema.conversion.compare_sample_size"
        ),
    )


class SchemaConvertMultiRequest(BaseModel):
    source_conn: str = Field(..., examples=["my_mysql"])
    target_type: str = Field(..., examples=["PostgreSQL"])
    tables: list[str] = Field(..., min_length=1, examples=[["users", "orders"]])
    target_db: str = Field(
        "", examples=["test"],
        description="Target database/schema to qualify generated table names. "
                    "Cross-table references (FKs) are qualified to match.",
    )
    prefix: str = Field("", examples=["mig_"])
    suffix: str = Field("", examples=["_copy"])
    type_map: str = Field(
        "",
        examples=['varchar2:text,int:decimal'],
        description='Optional type override rules: "source_type:target_type, ..."',
    )


class SchemaApplyRequest(BaseModel):
    target_conn: str = Field(..., examples=["target_pg"])
    ddl: str = Field(..., examples=["CREATE TABLE t(id INT);"])
    stop_on_error: bool = Field(True, examples=[True])
    create_indexes: bool = Field(
        True, description="When False, skip CREATE INDEX / CREATE UNIQUE INDEX "
                          "statements (mirrors the Tk 'Create Indexes' option).",
    )
    drop_if_exists: bool = Field(
        False, description="When True, run DROP TABLE IF EXISTS before each "
                           "CREATE TABLE (mirrors the Tk 'Drop Table If Exists').",
    )


class DataTransferRequest(BaseModel):
    source_conn: str = Field(..., examples=["my_mysql"])
    target_conn: str = Field(..., examples=["target_pg"])
    table: str = Field(..., examples=["users"])
    target_table: str = Field("", examples=["users_migrated"])
    target_db: str = Field(
        "", examples=["test"],
        description="Qualify the target table (test -> test.users) when "
                    "target_table is not given.",
    )
    prefix: str = Field("", examples=["mig_"])
    suffix: str = Field("", examples=["_copy"])
    batch_size: int | None = Field(None, ge=1, le=100000, examples=[1000])
    where: str = Field("", examples=["status = 'active'"], description="G1 row filter")
    limit: int | None = Field(None, ge=1, examples=[1000], description="G1 max rows")
    columns: str = Field("", examples=["id,name,email"], description="G2 column subset")
    column_map: str = Field(
        "", examples=["name:full_name"], description="G2 rename rules src:tgt,..."
    )
    continue_on_error: bool = Field(False, description="G3 keep going on row errors")
    overflow_policy: str = Field("", examples=["truncate"], description="G4 fail|truncate|skip")
    null_policy: str = Field("", examples=["empty_to_null"], description="G6 null/empty policy")
    bool_policy: str = Field("", examples=["int"], description="G6 bool normalization")
    timezone_policy: str = Field("", examples=["utc"], description="G7 preserve|naive|utc|target")
    target_timezone: str = Field("", examples=["Asia/Kolkata"], description="G7 target tz")
    reset_sequences: bool = Field(False, description="G8 reset auto-increment after load")
    checkpoint: bool = Field(False, description="G9 enable resume/checkpoint")
    report_path: str = Field("", examples=["/tmp/migrate_report.json"], description="G10 report file")


class DataTransferMultiRequest(BaseModel):
    source_conn: str = Field(..., examples=["source_pg"])
    target_conn: str = Field(..., examples=["target_mariadb"])
    tables: list[str] = Field(..., min_length=1, examples=[["public.users", "public.orders"]])
    target_db: str = Field(
        "", examples=["test"],
        description="Target database/schema to qualify each target table.",
    )
    prefix: str = Field("", examples=["mig_"])
    suffix: str = Field("", examples=["_copy"])
    batch_size: int | None = Field(None, ge=1, le=100000, examples=[1000])
    parallel: bool = Field(False, examples=[True])
    workers: int | None = Field(
        None,
        ge=1,
        le=64,
        examples=[4],
        description="Parallel worker count; omit to use schema_converter config.",
    )
    limit: int | None = Field(
        None, ge=1, description="G1 max rows applied per table"
    )
    column_map: str = Field(
        "",
        examples=["name:full_name"],
        description="G2 rename rules src:tgt,...; applied to every selected table",
    )
    continue_on_error: bool = Field(False, description="G3 keep going on row errors")
    overflow_policy: str = Field("", description="G4 fail|truncate|skip")
    null_policy: str = Field("", description="G6 null/empty policy")
    bool_policy: str = Field("", description="G6 bool normalization")
    timezone_policy: str = Field("", description="G7 preserve|naive|utc|target")
    target_timezone: str = Field("", description="G7 target tz")
    reset_sequences: bool = Field(False, description="G8 reset auto-increment after load")
    checkpoint: bool = Field(False, description="G9 enable resume/checkpoint")
    report_path: str = Field("", description="G10 report file")


class MigrationValidateRequest(BaseModel):
    source_conn: str = Field(..., examples=["source_pg"])
    target_conn: str = Field(..., examples=["target_mariadb"])
    tables: list[str] = Field(..., min_length=1, examples=[["public.users"]])
    target_db: str = Field("", examples=["test"])
    prefix: str = Field("", examples=["mig_"])
    suffix: str = Field("", examples=["_copy"])
    type_map: str = Field("", examples=["varchar2:text,int:decimal"])


class MultiTableRequest(BaseModel):
    tables: list[str] = Field(..., min_length=1, examples=[["users", "orders"]])
    limit: int = Field(1, ge=1, le=1000)


class MigratorConfigSet(BaseModel):
    section: str
    key: str
    value: str


def build_router(svc=None):
    from fastapi import APIRouter, HTTPException, Query

    if svc is None:
        from schema_converter.bridge import make_service

        svc = make_service()

    router = APIRouter(tags=["Data Migration"])

    def _error(detail: str, status: int = 400):
        raise HTTPException(status_code=status, detail=detail)

    def _call(action):
        try:
            return action()
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            _error(str(exc), 500)

    @router.post("/api/migrator/convert")
    def convert_schema(req: SchemaConvertRequest):
        r = _call(lambda: svc.convert_schema(
            req.source_conn, req.target_type, req.table,
            naming=TargetNaming(
                target_db=req.target_db, prefix=req.prefix, suffix=req.suffix
            ),
            type_map=req.type_map,
        ))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/migrator/compare-schema")
    def compare_schema(req: SchemaCompareRequest):
        r = _call(lambda: svc.compare_schema(
            req.source_conn,
            req.target_conn,
            req.table,
            req.target_table or None,
        ))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/migrator/compare-data")
    def compare_data(req: DataCompareRequest):
        mode = req.mode if req.mode in ("sample", "full") else "sample"
        sample_size = req.sample_size if req.sample_size is not None else None
        r = _call(lambda: svc.compare_data(
            req.source_conn,
            req.target_conn,
            req.table,
            options=DataCompareOptions(
                target_table=req.target_table or None,
                mode=mode,
                sample_size=sample_size,
            ),
        ))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/migrator/{connection}/dump")
    def schema_dump(
        connection: str,
        table: str = Query("", description="Specific table; blank = all"),
    ):
        r = _call(lambda: svc.dump_schema(connection, table=table or None))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.get("/api/migrator/{connection}/{table}")
    def schema_show(connection: str, table: str):
        r = _call(lambda: svc.get_table_schema(connection, table))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/migrator/convert-multi")
    def convert_schema_multi(req: SchemaConvertMultiRequest):
        """Convert several tables in one request."""
        if not hasattr(svc, "convert_schema_multi"):
            _error("convert_schema_multi not supported by this service.", 501)
        r = _call(lambda: svc.convert_schema_multi(
            req.source_conn, req.target_type, req.tables,
            naming=TargetNaming(
                target_db=req.target_db, prefix=req.prefix, suffix=req.suffix
            ),
            type_map=req.type_map,
        ))
        return r

    @router.post("/api/migrator/apply")
    def schema_apply(req: SchemaApplyRequest):
        """Execute a (multi-statement) DDL blob against a target connection."""
        if not hasattr(svc, "apply_ddl_to_target"):
            _error("apply_ddl_to_target not supported by this service.", 501)
        r = _call(lambda: svc.apply_ddl_to_target(
            req.target_conn, req.ddl, stop_on_error=req.stop_on_error,
            create_indexes=req.create_indexes, drop_if_exists=req.drop_if_exists,
        ))
        return r

    @router.post("/api/migrator/transfer-data")
    def schema_transfer_data(req: DataTransferRequest):
        """Copy rows from a source table into a target table."""
        if not hasattr(svc, "transfer_data"):
            _error("transfer_data not supported by this service.", 501)
        request = TransferRequest(
            source_conn=req.source_conn,
            target_conn=req.target_conn,
            table=req.table,
            target_table=req.target_table or None,
            batch_size=req.batch_size,
            naming=TargetNaming(
                target_db=req.target_db, prefix=req.prefix, suffix=req.suffix
            ),
        )
        options = options_from_mapping(req.model_dump())
        r = _call(lambda: svc.transfer_data(
            request,
            options,
        ))
        if not r["ok"]:
            _error(r["message"])
        return r

    @router.post("/api/migrator/transfer-data-multi")
    def schema_transfer_data_multi(req: DataTransferMultiRequest):
        """Copy rows for multiple source tables, optionally in parallel."""
        if not hasattr(svc, "transfer_data_multi"):
            _error("transfer_data_multi not supported by this service.", 501)
        request = TransferMultiRequest(
            source_conn=req.source_conn,
            target_conn=req.target_conn,
            tables=req.tables,
            batch_size=req.batch_size,
            naming=TargetNaming(
                target_db=req.target_db, prefix=req.prefix, suffix=req.suffix
            ),
            parallel=req.parallel,
            workers=req.workers,
        )
        options = options_from_mapping(req.model_dump())
        r = _call(lambda: svc.transfer_data_multi(
            request,
            options,
        ))
        if not r.get("ok"):
            _error(r.get("error") or "Transfer failed")
        return r

    @router.post("/api/migrator/validate")
    def schema_validate_migration(req: MigrationValidateRequest):
        """G5: pre-migration dry-run report. No rows are moved."""
        if not hasattr(svc, "validate_migration"):
            _error("validate_migration not supported by this service.", 501)
        r = _call(lambda: svc.validate_migration(
            req.source_conn,
            req.target_conn,
            req.tables,
            naming=TargetNaming(
                target_db=req.target_db, prefix=req.prefix, suffix=req.suffix
            ),
            type_map=req.type_map,
        ))
        if r.get("error"):
            _error(r["error"])
        return r

    @router.post("/api/migrator/{connection}/row-counts")
    def schema_row_counts(connection: str, req: MultiTableRequest):
        """Row counts for many tables on one connection."""
        if not hasattr(svc, "count_rows_multi"):
            _error("count_rows_multi not supported by this service.", 501)
        return _call(lambda: svc.count_rows_multi(connection, req.tables))

    @router.post("/api/migrator/{connection}/sample-multi")
    def schema_sample_multi(connection: str, req: MultiTableRequest):
        """Sample rows for many tables on one connection."""
        if not hasattr(svc, "sample_rows_multi"):
            _error("sample_rows_multi not supported by this service.", 501)
        return _call(lambda: svc.sample_rows_multi(connection, req.tables, limit=req.limit))

    @router.get("/api/migrator/config", tags=["Config"])
    def migrator_config_get():
        from schema_converter import module_config as mc
        return _call(lambda: {
            "ok": True,
            "config": {s: {k: mc.get(s, k) for k in keys} for s, keys in mc.DEFAULTS.items()},
            "path": str(mc.config_path() or mc.live_path()),
        })

    @router.post("/api/migrator/config", tags=["Config"])
    def migrator_config_set(req: MigratorConfigSet):
        from schema_converter import module_config as mc
        if req.section not in mc.DEFAULTS or req.key not in mc.DEFAULTS[req.section]:
            _error(f"Unknown setting {req.section}.{req.key}")
        _call(lambda: mc.set_value(req.section, req.key, req.value))
        return {"ok": True, "message": f"{req.section}.{req.key} saved."}

    @router.post("/api/migrator/config/restore", tags=["Config"])
    def migrator_config_restore():
        from schema_converter import module_config as mc
        _call(mc.restore_defaults)
        return {"ok": True, "message": "config.ini restored."}

    return router
