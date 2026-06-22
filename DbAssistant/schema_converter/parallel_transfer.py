"""Shared parallel data-transfer runner for the Data Migration module."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Iterable

from schema_converter.converter import ConversionValidator
from schema_converter.transfer_options import ParallelTransferContext, TransferRuntime


TargetTableFunc = Callable[[str], str]


@dataclass(frozen=True)
class TransferSpec:
    source_table: str
    target_table: str


def build_transfer_specs(
    source_tables: Iterable[str],
    target_table_for: TargetTableFunc,
) -> list[TransferSpec]:
    """Return normalized source/target table pairs for a transfer run."""
    return [
        TransferSpec(source_table=str(table), target_table=target_table_for(str(table)))
        for table in (source_tables or [])
        if str(table).strip()
    ]


def run_parallel_transfer(
    specs: list[TransferSpec],
    context: ParallelTransferContext,
    options=None,
) -> dict:
    """Transfer many tables with bounded worker concurrency.

    Each worker opens its own source and target manager through the supplied
    factories and closes them after that table finishes. This keeps transactions
    isolated and avoids holding idle sessions beyond the transfer operation.
    """
    specs = list(specs or [])
    if not specs:
        return {
            "ok": True,
            "error": None,
            "tables": [],
            "successful": 0,
            "failed": 0,
            "total_rows": 0,
            "workers": 0,
        }

    max_workers = max(1, int(context.workers or 1))
    max_workers = min(max_workers, len(specs))
    results: list[dict] = []

    def _transfer_one(index: int, spec: TransferSpec) -> dict:
        if context.stop_event is not None and context.stop_event.is_set():
            return {
                "ok": False,
                "source_table": spec.source_table,
                "target_table": spec.target_table,
                "rows_transferred": 0,
                "source_count": None,
                "target_count": None,
                "error": "Transfer stopped before this table started.",
                "index": index,
            }

        source_mgr = None
        target_mgr = None
        try:
            source_mgr = context.source_manager_factory(context.source_conn)
            target_mgr = context.target_manager_factory(context.target_conn)
            from schema_converter.adapters import transfer_object, validate_migration_pair

            err = validate_migration_pair(
                source_mgr.db_type, target_mgr.db_type, operation="transfer"
            )
            if err:
                return {
                    "ok": False,
                    "source_table": spec.source_table,
                    "target_table": spec.target_table,
                    "rows_transferred": 0,
                    "source_count": None,
                    "target_count": None,
                    "error": err,
                    "index": index,
                }

            def _progress(rows: int, total: int | None):
                if context.progress_callback:
                    context.progress_callback(spec.source_table, rows, total)

            stats: dict = {}
            rows = transfer_object(
                source_mgr,
                target_mgr,
                spec.source_table,
                spec.target_table,
                runtime=TransferRuntime(
                    batch_size=context.batch_size,
                    progress_callback=_progress if context.progress_callback else None,
                    stop_event=context.stop_event,
                    options=options,
                    checkpoint_store=context.checkpoint_store,
                    stats_out=stats,
                ),
            )
            source_count = None
            target_count = None
            validation_msg = None
            try:
                from schema_converter.converter import DataConverter

                converter = DataConverter(source_mgr, target_mgr)
                source_count = converter.get_row_count(spec.source_table, is_source=True)
                target_count = converter.get_row_count(spec.target_table, is_source=False)
                # When row filtering / skip policies are active, a count
                # difference is expected and not a failure.
                filtered = bool(
                    options is not None
                    and (
                        getattr(options, "where", "")
                        or getattr(options, "limit", None) is not None
                        or int(stats.get("skipped") or 0) > 0
                    )
                )
                if not filtered:
                    validation_msg = ConversionValidator.validate_data_transfer(
                        source_count, target_count
                    )
            except Exception:
                validation_msg = None
            return {
                "ok": not bool(validation_msg),
                "source_table": spec.source_table,
                "target_table": spec.target_table,
                "rows_transferred": int(rows or 0),
                "skipped": int(stats.get("skipped") or 0),
                "error_count": int(stats.get("error_count") or 0),
                "duration_seconds": stats.get("duration_seconds"),
                "source_count": source_count,
                "target_count": target_count,
                "error": validation_msg or None,
                "index": index,
            }
        except Exception as exc:
            return {
                "ok": False,
                "source_table": spec.source_table,
                "target_table": spec.target_table,
                "rows_transferred": 0,
                "source_count": None,
                "target_count": None,
                "error": str(exc),
                "index": index,
            }
        finally:
            for mgr in (target_mgr, source_mgr):
                if mgr is not None:
                    try:
                        mgr.disconnect()
                    except Exception:
                        pass

    if max_workers == 1:
        for index, spec in enumerate(specs, 1):
            results.append(_transfer_one(index, spec))
            if context.stop_event is not None and context.stop_event.is_set():
                break
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_transfer_one, index, spec): spec
                for index, spec in enumerate(specs, 1)
            }
            for future in as_completed(future_map):
                results.append(future.result())
                if context.stop_event is not None and context.stop_event.is_set():
                    # Already-running workers finish their current batch/table;
                    # queued futures that have not started are cancelled.
                    for pending in future_map:
                        pending.cancel()
                    break

    results.sort(key=lambda row: row.get("index", 0))
    failed = [row for row in results if not row.get("ok")]
    total_rows = sum(int(row.get("rows_transferred") or 0) for row in results)
    return {
        "ok": not failed,
        "error": failed[0].get("error") if failed else None,
        "tables": results,
        "successful": len(results) - len(failed),
        "failed": len(failed),
        "total_rows": total_rows,
        "workers": max_workers,
    }
