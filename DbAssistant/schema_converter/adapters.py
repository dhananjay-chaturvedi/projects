"""Migration source/target adapters for relational and document engines."""

from __future__ import annotations

from common.database_registry import DatabaseRegistry
from schema_converter.transfer_options import TransferRuntime

DOCUMENT_TYPES = frozenset({"MongoDB", "DocumentDB"})


def migration_pair_kind(source_type: str, target_type: str) -> str:
    src_doc = source_type in DOCUMENT_TYPES
    tgt_doc = target_type in DOCUMENT_TYPES
    if src_doc and tgt_doc:
        return "document"
    if not src_doc and not tgt_doc:
        return "relational"
    return "mixed"


def validate_migration_pair(source_type: str, target_type: str, *, operation: str = "transfer") -> str | None:
    kind = migration_pair_kind(source_type, target_type)
    if kind == "mixed":
        return (
            f"Cross-engine migration between {source_type} and {target_type} "
            "is not supported yet. Use relational-to-relational or "
            "document-to-document pairs."
        )
    if operation == "schema" and kind == "document":
        return (
            "Schema conversion for document databases is limited to collection "
            "inference. Use Transfer Data to copy MongoDB/DocumentDB collections."
        )
    return None


def transfer_relational_table(
    source_manager,
    target_manager,
    source_table: str,
    target_table: str,
    *,
    runtime: TransferRuntime | None = None,
    **legacy_runtime,
) -> int:
    from schema_converter.converter import DataConverter

    runtime = TransferRuntime.from_source(runtime or legacy_runtime)
    converter = DataConverter(source_manager, target_manager)
    try:
        return converter.transfer_table_data(
            source_table,
            target_table,
            runtime=runtime,
        )
    finally:
        if runtime.stats_out is not None:
            runtime.stats_out.update(
                getattr(converter, "last_transfer_stats", {}) or {}
            )


def transfer_document_collection(
    source_manager,
    target_manager,
    source_collection: str,
    target_collection: str,
    *,
    runtime: TransferRuntime | None = None,
    **legacy_runtime,
) -> int:
    from common.config_loader import config

    runtime = TransferRuntime.from_source(runtime or legacy_runtime)
    batch_size = runtime.batch_size
    if batch_size is None:
        batch_size = config.get_int(
            "database.performance", "transfer_batch_size", default=1000
        )

    read_fn = DatabaseRegistry.get_operation(source_manager.db_type, "readMongoDocuments")
    write_fn = DatabaseRegistry.get_operation(target_manager.db_type, "insertMongoDocuments")
    if not read_fn or not write_fn:
        raise NotImplementedError(
            f"Document transfer not available for "
            f"{source_manager.db_type} -> {target_manager.db_type}."
        )

    transferred = 0
    skip = 0
    while True:
        if runtime.stop_event is not None and runtime.stop_event.is_set():
            break
        docs = read_fn(source_manager.conn, source_collection, batch_size, skip)
        if not docs:
            break
        if not runtime.keep_ids:
            for doc in docs:
                doc.pop("_id", None)
        inserted = write_fn(target_manager.conn, target_collection, docs)
        transferred += int(inserted or 0)
        skip += len(docs)
        if runtime.progress_callback:
            runtime.progress_callback(transferred, None)
        if len(docs) < batch_size:
            break
    return transferred


def transfer_object(
    source_manager,
    target_manager,
    source_name: str,
    target_name: str,
    *,
    runtime: TransferRuntime | None = None,
    **legacy_runtime,
) -> int:
    runtime = TransferRuntime.from_source(runtime or legacy_runtime)
    err = validate_migration_pair(
        source_manager.db_type, target_manager.db_type, operation="transfer"
    )
    if err:
        raise ValueError(err)

    kind = migration_pair_kind(source_manager.db_type, target_manager.db_type)
    if kind == "document":
        return transfer_document_collection(
            source_manager,
            target_manager,
            source_name,
            target_name,
            runtime=runtime,
        )
    return transfer_relational_table(
        source_manager,
        target_manager,
        source_name,
        target_name,
        runtime=runtime,
    )


def list_migration_objects(manager) -> list[str]:
    caps = DatabaseRegistry.get_capabilities(manager.db_type)
    op = "getTables"
    if caps.supports_document_query:
        op = "getTables"
    fn = DatabaseRegistry.get_operation(manager.db_type, op)
    if not fn:
        return []
    return list(fn(manager.conn) or [])
