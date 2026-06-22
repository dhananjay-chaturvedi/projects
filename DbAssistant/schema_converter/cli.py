"""
CLI surface for the Data Migration module (package: schema_converter).

Exposes the ``migrator`` command with ``convert``, ``apply``, ``transfer-data``,
``compare-schema``, ``compare-data``, ``show``, ``dump``, ``row-counts`` and
``sample`` actions.
Runnable standalone via ``python -m schema_converter`` (uses ``common/`` only).
"""

from __future__ import annotations

from pathlib import Path

from common.core import cliutil
from common.config_loader import get_compare_sample_size


def register_cli(subparsers) -> None:
    """Add the ``migrator`` command and its actions to *subparsers*."""
    sch_p = subparsers.add_parser(
        "migrator",
        help="Data migration: schema convert, data transfer, and validation",
    )
    sch_sub = sch_p.add_subparsers(dest="schema_action", metavar="<action>")
    sch_sub.required = True

    conv_p = sch_sub.add_parser("convert", help="Convert a table schema to another DB type")
    conv_p.add_argument("--source-conn", required=True, dest="source_conn")
    conv_p.add_argument("--target-type", required=True, dest="target_type",
                        help="MySQL | MariaDB | PostgreSQL | Oracle")
    conv_t = conv_p.add_mutually_exclusive_group(required=True)
    conv_t.add_argument("--table", default="", help="Single table to convert")
    conv_t.add_argument(
        "--tables", default="",
        help="Comma-separated list of tables for a batch convert",
    )
    conv_p.add_argument(
        "--target-db", default="", dest="target_db",
        help="Target database/schema to qualify generated table names "
             "(e.g. test -> CREATE TABLE test.orders). Required for "
             "MySQL/MariaDB targets without a default database.",
    )
    conv_p.add_argument("--prefix", default="", help="Target table name prefix")
    conv_p.add_argument("--suffix", default="", help="Target table name suffix")
    conv_p.add_argument(
        "--type-map", default="", dest="type_map",
        help='Custom type rules, e.g. "varchar2:text,int:decimal"',
    )
    conv_p.add_argument("--output", default="", metavar="FILE.sql")
    conv_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    apply_p = sch_sub.add_parser(
        "apply", help="Run a (multi-statement) DDL blob against a target connection",
    )
    apply_p.add_argument("--target-conn", required=True, dest="target_conn")
    src_grp = apply_p.add_mutually_exclusive_group(required=True)
    src_grp.add_argument("--ddl", default="", help="DDL string to execute")
    src_grp.add_argument("--ddl-file", default="", dest="ddl_file",
                         help="Path to a .sql file with DDL")
    apply_p.add_argument(
        "--continue-on-error", action="store_true",
        help="Keep going after a failed statement (default: stop)",
    )

    xfer_p = sch_sub.add_parser(
        "transfer-data",
        help="Copy rows from a source table into a target table",
    )
    xfer_p.add_argument("--source-conn", required=True, dest="source_conn")
    xfer_p.add_argument("--target-conn", required=True, dest="target_conn")
    xfer_t = xfer_p.add_mutually_exclusive_group(required=True)
    xfer_t.add_argument("--table", default="", help="Single source table to transfer")
    xfer_t.add_argument(
        "--tables", default="",
        help="Comma-separated list of source tables to transfer",
    )
    xfer_p.add_argument(
        "--target-table", default="",
        help="Target table name when it differs from source (overrides --target-db)",
    )
    xfer_p.add_argument(
        "--target-db", default="", dest="target_db",
        help="Target database/schema to qualify the target table when "
             "--target-table is not given (e.g. test -> test.orders).",
    )
    xfer_p.add_argument("--prefix", default="", help="Target table name prefix")
    xfer_p.add_argument("--suffix", default="", help="Target table name suffix")
    xfer_p.add_argument(
        "--batch-size", type=int, default=0,
        help="Rows per executemany batch (0 = config default)",
    )
    xfer_p.add_argument(
        "--parallel", action="store_true",
        help="Transfer multiple tables concurrently (schema conversion remains serial)",
    )
    xfer_p.add_argument(
        "--workers", type=int, default=0,
        help="Parallel transfer worker count (0 = schema_converter config)",
    )
    xfer_p.add_argument(
        "--where", default="",
        help="G1: row filter (SQL WHERE, no keyword); single --table only",
    )
    xfer_p.add_argument(
        "--limit", type=int, default=None,
        help="G1: max rows per table (applies to every selected table)",
    )
    xfer_p.add_argument(
        "--columns", default="",
        help="G2: comma-separated source column subset (single --table only)",
    )
    xfer_p.add_argument(
        "--column-map", dest="column_map", default="",
        help='G2: column rename rules, e.g. "src1:tgt1,src2:tgt2"; '
             "applies to every selected table",
    )
    xfer_p.add_argument(
        "--continue-on-error", dest="continue_on_error", action="store_true",
        help="G3: keep going on row errors and report them (vs abort)",
    )
    xfer_p.add_argument(
        "--overflow-policy", dest="overflow_policy",
        choices=["fail", "truncate", "skip"], default="",
        help="G4: behaviour when a value exceeds the target column",
    )
    xfer_p.add_argument(
        "--null-policy", dest="null_policy",
        choices=["keep", "empty_to_null", "null_to_empty"], default="",
        help="G6: NULL/empty-string normalization",
    )
    xfer_p.add_argument(
        "--bool-policy", dest="bool_policy",
        choices=["auto", "int", "true_false"], default="",
        help="G6: boolean normalization across engines",
    )
    xfer_p.add_argument(
        "--timezone-policy", dest="timezone_policy",
        choices=["preserve", "naive", "utc", "target"], default="",
        help="G7: datetime/timestamp timezone handling",
    )
    xfer_p.add_argument(
        "--target-timezone", dest="target_timezone", default="",
        help="G7: target timezone name when --timezone-policy=target",
    )
    xfer_p.add_argument(
        "--reset-sequences", dest="reset_sequences", action="store_true",
        help="G8: reset target auto-increment/sequence after load",
    )
    xfer_p.add_argument(
        "--checkpoint", action="store_true",
        help="G9: enable resume/checkpoint for interrupted transfers",
    )
    xfer_p.add_argument(
        "--report", dest="report_path", default="",
        help="G10: write a JSON migration report to this file",
    )

    val_p = sch_sub.add_parser(
        "validate",
        help="G5: pre-migration dry-run report (no rows are moved)",
    )
    val_p.add_argument("--source-conn", required=True, dest="source_conn")
    val_p.add_argument("--target-conn", required=True, dest="target_conn")
    val_p.add_argument(
        "--tables", required=True,
        help="Comma-separated list of source tables to validate",
    )
    val_p.add_argument("--target-db", default="", dest="target_db")
    val_p.add_argument("--prefix", default="")
    val_p.add_argument("--suffix", default="")
    val_p.add_argument(
        "--type-map", dest="type_map", default="",
        help='Type override rules, e.g. "varchar2:text,int:decimal"',
    )
    val_p.add_argument("--format", choices=["table", "json"], default="table")

    rc_p = sch_sub.add_parser(
        "row-counts", help="Row counts for one or many tables on a single connection",
    )
    rc_p.add_argument("--conn", required=True)
    rc_p.add_argument(
        "--tables", required=True,
        help="Comma-separated list of table names",
    )
    rc_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    samp_p = sch_sub.add_parser(
        "sample", help="Sample rows for one or many tables on a single connection",
    )
    samp_p.add_argument("--conn", required=True)
    samp_p.add_argument(
        "--tables", required=True,
        help="Comma-separated list of table names",
    )
    samp_p.add_argument("--limit", type=int, default=1)
    samp_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    show_p = sch_sub.add_parser("show", help="Show columns/indexes of a table")
    show_p.add_argument("--conn", required=True, metavar="NAME")
    show_p.add_argument("--table", required=True)
    show_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    dump_p = sch_sub.add_parser("dump", help="Dump CREATE TABLE/INDEX DDL")
    dump_p.add_argument("--conn", required=True, metavar="NAME")
    dump_p.add_argument("--table", default="", help="Specific table; omit for all tables")
    dump_p.add_argument("--output", default="", metavar="FILE.sql")

    cmp_sch_p = sch_sub.add_parser(
        "compare-schema", help="Compare table schema between source and target connections"
    )
    cmp_sch_p.add_argument("--source-conn", required=True, dest="source_conn")
    cmp_sch_p.add_argument("--target-conn", required=True, dest="target_conn")
    cmp_sch_p.add_argument("--table", required=True)
    cmp_sch_p.add_argument(
        "--target-table",
        default="",
        help="Target table name when it differs from source",
    )
    cmp_sch_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    cmp_dat_p = sch_sub.add_parser(
        "compare-data", help="Compare table data row-by-row between connections"
    )
    cmp_dat_p.add_argument("--source-conn", required=True, dest="source_conn")
    cmp_dat_p.add_argument("--target-conn", required=True, dest="target_conn")
    cmp_dat_p.add_argument("--table", required=True)
    cmp_dat_p.add_argument(
        "--target-table",
        default="",
        help="Target table name when it differs from source",
    )
    cmp_dat_p.add_argument(
        "--mode",
        choices=["sample", "full"],
        default="sample",
        help="sample = first N rows; full = all rows",
    )
    cmp_dat_p.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help=(
            "Rows to compare when mode=sample "
            f"(default from properties.ini: {get_compare_sample_size()})"
        ),
    )
    cmp_dat_p.add_argument("--format", choices=["table", "json", "csv"], default="table")

    cfg_p = sch_sub.add_parser("config", help="View/edit schema_converter/config.ini")
    cfg_sub = cfg_p.add_subparsers(dest="migrator_config_action", required=True)
    cfg_show = cfg_sub.add_parser("show", help="Show module config")
    cfg_show.add_argument("--format", choices=["table", "json"], default="table")
    cfg_set = cfg_sub.add_parser("set", help="Set one config value")
    cfg_set.add_argument("section")
    cfg_set.add_argument("key")
    cfg_set.add_argument("value")
    cfg_sub.add_parser("restore", help="Restore config.ini from .example")


def _schema_service():
    from schema_converter.bridge import make_service

    return make_service()


def _migrator_config_cli(args) -> int:
    import json
    from schema_converter import module_config as mc

    act = args.migrator_config_action
    if act == "show":
        if args.format == "json":
            print(json.dumps(
                {s: {k: mc.get(s, k) for k in keys} for s, keys in mc.DEFAULTS.items()},
                indent=2,
            ))
            return 0
        for sec, keys in sorted(mc.DEFAULTS.items()):
            cliutil.info(f"[{sec}]")
            cliutil.print_table(
                [[k, mc.get(sec, k)] for k in sorted(keys)], ["key", "value"], "table"
            )
        return 0
    if act == "set":
        mc.set_value(args.section, args.key, args.value)
        cliutil.ok(f"{args.section}.{args.key} saved.")
        return 0
    if act == "restore":
        mc.restore_defaults()
        cliutil.ok("schema_converter/config.ini restored from .example.")
        return 0
    return 2


def dispatch_cli(args) -> int:
    """Handle a parsed ``migrator`` namespace. Returns an exit code."""
    action = getattr(args, "schema_action", None)
    if action == "config":
        return _migrator_config_cli(args)
    if action == "convert":
        return _convert(args)
    if action == "show":
        return _show(args)
    if action == "dump":
        return _dump(args)
    if action == "compare-schema":
        return _compare_schema(args)
    if action == "compare-data":
        return _compare_data(args)
    if action == "apply":
        return _apply(args)
    if action == "transfer-data":
        return _transfer_data(args)
    if action == "validate":
        return _validate(args)
    if action == "row-counts":
        return _row_counts(args)
    if action == "sample":
        return _sample(args)
    cliutil.err("Unknown migrator action.")
    return 2


def _naming_from_args(args):
    """Build a TargetNaming from parsed argparse fields."""
    from schema_converter.table_naming import TargetNaming

    return TargetNaming(
        target_db=getattr(args, "target_db", "") or "",
        prefix=getattr(args, "prefix", "") or "",
        suffix=getattr(args, "suffix", "") or "",
    )


def _convert(args) -> int:
    svc = _schema_service()
    # Multi-table batch convert (UI's "Preview / Convert schema" with N tables).
    if getattr(args, "tables", ""):
        names = [t.strip() for t in args.tables.split(",") if t.strip()]
        if not names:
            cliutil.err("--tables requires at least one table name.")
            return 1
        cliutil.info(
            f"Batch-converting {len(names)} table(s) from '{args.source_conn}' "
            f"→ {args.target_type} ..."
        )
        if not hasattr(svc, "convert_schema_multi"):
            cliutil.err("convert_schema_multi not supported by this service.")
            return 1
        r = svc.convert_schema_multi(
            args.source_conn, args.target_type, names,
            naming=_naming_from_args(args),
            type_map=getattr(args, "type_map", "") or "",
        )
        joined = r.get("joined_ddl") or ""
        if args.output:
            Path(args.output).write_text(joined)
            cliutil.ok(f"DDL ({len(names)} tables) written to {args.output}")
        else:
            print(joined)
        per_table_errors = 0
        for row in r.get("tables") or []:
            for iss in row.get("issues") or []:
                cliutil.warn(f"[{row['table']}] {iss}")
            if row.get("error"):
                per_table_errors += 1
                cliutil.err(f"[{row['table']}] {row['error']}")
        if r.get("error") and per_table_errors == 0:
            cliutil.err(r["error"])
            return 1
        return 0 if per_table_errors == 0 else 1

    cliutil.info(
        f"Converting '{args.table}' from '{args.source_conn}' → {args.target_type} ..."
    )
    r = svc.convert_schema(
        args.source_conn, args.target_type, args.table,
        naming=_naming_from_args(args),
        type_map=getattr(args, "type_map", "") or "",
    )
    if r["error"]:
        cliutil.err(r["error"])
        return 1
    parts = []
    if r["ddl"]:
        parts.append(r["ddl"])
    parts.extend(r.get("indexes_ddl") or [])
    full_ddl = "\n\n".join(parts)
    if args.output:
        Path(args.output).write_text(full_ddl)
        cliutil.ok(f"DDL written to {args.output}")
    else:
        print(full_ddl)
    for iss in (r.get("issues") or []):
        cliutil.warn(str(iss))
    return 0


def _apply(args) -> int:
    svc = _schema_service()
    if not hasattr(svc, "apply_ddl_to_target"):
        cliutil.err("apply_ddl_to_target not supported by this service.")
        return 1
    ddl = args.ddl
    if args.ddl_file:
        try:
            ddl = Path(args.ddl_file).read_text(encoding="utf-8")
        except OSError as exc:
            cliutil.err(str(exc))
            return 1
    if not (ddl or "").strip():
        cliutil.err("No DDL provided (use --ddl or --ddl-file).")
        return 1
    cliutil.info(f"Applying DDL to '{args.target_conn}' …")
    r = svc.apply_ddl_to_target(
        args.target_conn, ddl, stop_on_error=not args.continue_on_error,
    )
    for i, st in enumerate(r.get("statements") or [], 1):
        head = (st.get("sql") or "").strip().splitlines()[0]
        if len(head) > 80:
            head = head[:77] + "…"
        if st.get("ok"):
            cliutil.ok(f"[{i}] {head}")
        else:
            cliutil.err(f"[{i}] {head}  -> {st.get('error')}")
    msg = (
        f"executed={r.get('executed', 0)}  failed={r.get('failed', 0)}  "
        f"total={len(r.get('statements') or [])}"
    )
    if r.get("error") and r.get("failed", 0) > 0:
        cliutil.err(msg)
        return 1
    cliutil.ok(msg)
    return 0


def _transfer_data(args) -> int:
    svc = _schema_service()
    if not hasattr(svc, "transfer_data"):
        cliutil.err("transfer_data not supported by this service.")
        return 1
    from schema_converter.table_naming import qualify_target_table
    from schema_converter.transfer_options import (
        TransferMultiRequest,
        TransferRequest,
        options_from_mapping,
    )

    if getattr(args, "tables", ""):
        if args.target_table:
            cliutil.err("--target-table can only be used with --table, not --tables.")
            return 1
        single_only = [
            flag
            for flag, val in (
                ("--where", getattr(args, "where", "")),
                ("--columns", getattr(args, "columns", "")),
            )
            if val
        ]
        if single_only:
            cliutil.err(
                f"{', '.join(single_only)} only applies to a single table "
                "(--table), not --tables."
            )
            return 1
        if not hasattr(svc, "transfer_data_multi"):
            cliutil.err("transfer_data_multi not supported by this service.")
            return 1
        names = [t.strip() for t in args.tables.split(",") if t.strip()]
        if not names:
            cliutil.err("--tables requires at least one table name.")
            return 1
        cliutil.info(
            f"Transferring {len(names)} table(s) from '{args.source_conn}' "
            f"→ '{args.target_conn}' "
            f"({'parallel' if args.parallel else 'serial'}) ..."
        )
        request = TransferMultiRequest(
            source_conn=args.source_conn,
            target_conn=args.target_conn,
            tables=names,
            batch_size=args.batch_size or None,
            naming=_naming_from_args(args),
            parallel=bool(args.parallel),
            workers=args.workers or None,
        )
        options = options_from_mapping(vars(args))
        r = svc.transfer_data_multi(
            request,
            options,
        )
        rows = [
            [
                row.get("source_table"),
                row.get("target_table"),
                row.get("rows_transferred", 0),
                "OK" if row.get("ok") else (row.get("error") or "FAILED"),
            ]
            for row in (r.get("tables") or [])
        ]
        cliutil.print_table(rows, ["source", "target", "rows", "status"], "table")
        msg = (
            f"successful={r.get('successful', 0)} failed={r.get('failed', 0)} "
            f"rows={r.get('total_rows', 0)} workers={r.get('workers', 0)}"
        )
        (cliutil.ok if r.get("ok") else cliutil.err)(msg)
        return 0 if r.get("ok") else 1

    display_target = args.target_table or qualify_target_table(
        args.table,
        getattr(args, "target_db", "") or "",
        getattr(args, "prefix", "") or "",
        getattr(args, "suffix", "") or "",
    )
    cliutil.info(
        f"Transferring data '{args.table}' ({args.source_conn}) → "
        f"{display_target} ({args.target_conn}) ..."
    )
    request = TransferRequest(
        source_conn=args.source_conn,
        target_conn=args.target_conn,
        table=args.table,
        target_table=args.target_table or None,
        batch_size=args.batch_size or None,
        naming=_naming_from_args(args),
    )
    options = options_from_mapping(vars(args))
    r = svc.transfer_data(
        request,
        options,
    )
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    if r.get("report_path"):
        cliutil.info(f"Report written to {r['report_path']}")
    return 0 if r["ok"] else 1


def _validate(args) -> int:
    svc = _schema_service()
    if not hasattr(svc, "validate_migration"):
        cliutil.err("validate_migration not supported by this service.")
        return 1
    names = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not names:
        cliutil.err("--tables requires at least one table name.")
        return 1
    cliutil.info(
        f"Validating {len(names)} table(s): '{args.source_conn}' → "
        f"'{args.target_conn}' (dry-run, no rows moved) ..."
    )
    r = svc.validate_migration(
        args.source_conn,
        args.target_conn,
        names,
        naming=_naming_from_args(args),
        type_map=getattr(args, "type_map", "") or "",
    )
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    if getattr(args, "format", "table") == "json":
        import json

        print(json.dumps(r, indent=2, default=str))
        return 0 if r.get("ok") else 1
    rows = []
    for t in r.get("tables") or []:
        for issue in t.get("issues") or []:
            rows.append(
                [
                    t.get("source_table"),
                    t.get("target_table"),
                    issue.get("severity"),
                    issue.get("category"),
                    issue.get("column") or "",
                    issue.get("message"),
                ]
            )
    if rows:
        cliutil.print_table(
            rows,
            ["source", "target", "severity", "category", "column", "message"],
            "table",
        )
    summary = r.get("summary") or {}
    msg = (
        f"tables={summary.get('tables', 0)} "
        f"errors={summary.get('errors', 0)} "
        f"warnings={summary.get('warnings', 0)}"
    )
    (cliutil.ok if r.get("ok") else cliutil.err)(msg)
    return 0 if r.get("ok") else 1


def _row_counts(args) -> int:
    svc = _schema_service()
    if not hasattr(svc, "count_rows_multi"):
        cliutil.err("count_rows_multi not supported by this service.")
        return 1
    names = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not names:
        cliutil.err("--tables requires at least one table name.")
        return 1
    r = svc.count_rows_multi(args.conn, names)
    rows = [
        [t["table"], t.get("count", 0),
         t.get("error", "") or ""]
        for t in (r.get("tables") or [])
    ]
    cliutil.print_table(rows, ["table", "count", "error"], args.format)
    cliutil.info(f"Total rows across {len(names)} table(s): {r.get('total', 0):,}")
    return 0 if not r.get("error") else 1


def _sample(args) -> int:
    svc = _schema_service()
    if not hasattr(svc, "sample_rows_multi"):
        cliutil.err("sample_rows_multi not supported by this service.")
        return 1
    names = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not names:
        cliutil.err("--tables requires at least one table name.")
        return 1
    r = svc.sample_rows_multi(args.conn, names, limit=args.limit or 1)
    if args.format == "json":
        import json as _json
        print(_json.dumps(r, indent=2, default=str))
        return 0 if not r.get("error") else 1
    bad = 0
    for entry in (r.get("tables") or []):
        print(f"\n— {entry['table']} ({entry.get('rowcount', 0)} row(s)) —")
        if entry.get("error"):
            cliutil.err(entry["error"])
            bad += 1
            continue
        cols = entry.get("columns") or []
        rows = entry.get("rows") or []
        if rows:
            cliutil.print_table(rows, cols, "table")
        else:
            cliutil.info("(no rows)")
    return 0 if bad == 0 else 1


def _show(args) -> int:
    svc = _schema_service()
    r = svc.get_table_schema(args.conn, args.table)
    if r["error"]:
        cliutil.err(r["error"])
        return 1
    cols = r.get("columns") or []
    if not cols:
        cliutil.info(f"Table '{args.table}' has no columns or doesn't exist.")
        return 0
    if isinstance(cols[0], dict):
        headers = sorted({k for c in cols for k in c.keys()})
        rows = [[c.get(h, "") for h in headers] for c in cols]
    else:
        headers = ["column"]
        rows = [[c] for c in cols]
    cliutil.print_table(rows, headers, args.format)
    idx = r.get("indexes") or []
    if idx:
        print()
        cliutil.info(f"{len(idx)} index(es): " + ", ".join(str(x) for x in idx))
    return 0


def _dump(args) -> int:
    svc = _schema_service()
    r = svc.dump_schema(args.conn, table=args.table or None)
    if r["error"]:
        cliutil.err(r["error"])
        return 1
    if args.output:
        Path(args.output).write_text(r["ddl"])
        cliutil.ok(f"Wrote {r['table_count']} table(s) of DDL to {args.output}")
    else:
        print(r["ddl"])
    return 0


def _compare_schema(args) -> int:
    svc = _schema_service()
    target_table = args.target_table or args.table
    cliutil.info(
        f"Comparing schema '{args.table}' ({args.source_conn}) "
        f"vs '{target_table}' ({args.target_conn}) ..."
    )
    r = svc.compare_schema(
        args.source_conn, args.target_conn, args.table, target_table
    )
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    if args.format == "json":
        import json

        print(json.dumps(r, indent=2, default=str))
    elif args.format == "csv":
        rows = [[i] for i in (r.get("issues") or [])]
        cliutil.print_table(rows, ["issue"], "csv")
    else:
        if r.get("match"):
            cliutil.ok("Schema match")
        else:
            cliutil.warn("Schema mismatch")
        for iss in r.get("issues") or []:
            print(f"  - {iss}")
    return 0 if r.get("match") else 1


def _compare_data(args) -> int:
    from schema_converter.compare_options import DataCompareOptions

    svc = _schema_service()
    target_table = args.target_table or args.table
    cliutil.info(
        f"Comparing data '{args.table}' ({args.source_conn}) "
        f"vs '{target_table}' ({args.target_conn}) mode={args.mode} ..."
    )
    r = svc.compare_data(
        args.source_conn,
        args.target_conn,
        args.table,
        options=DataCompareOptions(
            target_table=target_table,
            mode=args.mode,
            sample_size=args.sample_size if args.sample_size is not None else None,
        ),
    )
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    if args.format == "json":
        import json

        print(json.dumps(r, indent=2, default=str))
    elif args.format == "csv":
        rows = []
        for mm in r.get("mismatched_rows") or []:
            for d in mm.get("differences") or []:
                rows.append([mm["row_number"], d["column"], d["source"], d["target"]])
        cliutil.print_table(rows, ["row", "column", "source", "target"], "csv")
    else:
        print(
            f"Source rows: {r.get('source_row_count')}  "
            f"Target rows: {r.get('target_row_count')}  "
            f"Compared: {r.get('rows_compared')}"
        )
        if r.get("row_count_message"):
            cliutil.warn(r["row_count_message"])
        if r.get("match"):
            cliutil.ok("Data match")
        else:
            cliutil.warn("Data mismatch")
        for mm in r.get("mismatched_rows") or []:
            print(f"  Row {mm['row_number']}:")
            for d in mm.get("differences") or []:
                print(
                    f"    {d['column']}: source={d['source']!r} "
                    f"target={d['target']!r}"
                )
    return 0 if r.get("match") else 1
