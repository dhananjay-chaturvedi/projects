"""Pre-migration validation / dry-run report (G5).

Inspects each source table against the target *before* any rows are moved and
reports:

* type incompatibilities (source type with no clean mapping)
* oversized columns (source longer/wider than an existing target column)
* unsupported / non-portable column defaults (functions, sequences)
* missing target tables / column-count mismatches

Nothing is written to the target; this is purely a read-only assessment.
"""

from __future__ import annotations


from schema_converter.converter import DataConverter, SchemaConverter
from schema_converter.type_overrides import parse_base_type

_UNPORTABLE_DEFAULT_TOKENS = (
    "sysdate",
    "systimestamp",
    "now(",
    "current_timestamp",
    "current_date",
    "current_time",
    "nextval",
    "newid(",
    "uuid(",
    "getdate(",
    "::",  # postgres cast in default
)


def _length_of(type_str: str) -> int | None:
    _, size = parse_base_type(type_str or "")
    if not size:
        return None
    first = size.split(",")[0].strip()
    try:
        return int(first)
    except ValueError:
        return None


def _is_unportable_default(default) -> bool:
    if default is None:
        return False
    text = str(default).strip().lower()
    if not text or text in ("null", "''", '""'):
        return False
    return any(token in text for token in _UNPORTABLE_DEFAULT_TOKENS)


def validate_table(
    source_manager,
    target_manager,
    source_table: str,
    target_table: str,
    *,
    type_overrides: dict | None = None,
) -> dict:
    """Validate a single source->target table pair. Read-only."""
    issues: list[dict] = []
    converter = SchemaConverter(source_manager, target_manager)

    source_schema = converter.get_table_schema(source_table)
    if not source_schema:
        return {
            "source_table": source_table,
            "target_table": target_table,
            "target_exists": False,
            "ok": False,
            "issues": [
                {
                    "severity": "error",
                    "category": "missing_source",
                    "column": "",
                    "message": f"Source table '{source_table}' not found.",
                }
            ],
        }

    try:
        converter.convert_schema(source_schema, type_overrides=type_overrides)
    except Exception as exc:
        issues.append(
            {
                "severity": "error",
                "category": "conversion_failed",
                "column": "",
                "message": f"Schema conversion failed: {exc}",
            }
        )

    # Detect unportable defaults on the source side.
    for col in source_schema.get("columns", []):
        if _is_unportable_default(col.get("default")):
            issues.append(
                {
                    "severity": "warning",
                    "category": "unsupported_default",
                    "column": col.get("name", ""),
                    "message": (
                        f"Column '{col.get('name')}' default "
                        f"'{col.get('default')}' may not translate to the target."
                    ),
                }
            )

    # Inspect existing target (if present) for oversize / count mismatch.
    target_exists = False
    target_meta: dict = {}
    try:
        dc = DataConverter(target_manager, target_manager)
        target_meta = dc._fetch_target_column_meta(target_table) or {}
        target_exists = bool(target_meta)
    except Exception:
        target_meta = {}

    if target_exists:
        lower_meta = {k.lower(): v for k, v in target_meta.items()}
        src_cols = source_schema.get("columns", [])
        if len(src_cols) != len(target_meta):
            issues.append(
                {
                    "severity": "warning",
                    "category": "column_count",
                    "column": "",
                    "message": (
                        f"Column count differs: source has {len(src_cols)}, "
                        f"target has {len(target_meta)}."
                    ),
                }
            )
        for col in src_cols:
            name = col.get("name", "")
            tgt = target_meta.get(name) or lower_meta.get(str(name).lower())
            if not tgt:
                issues.append(
                    {
                        "severity": "warning",
                        "category": "missing_target_column",
                        "column": name,
                        "message": f"Column '{name}' has no matching target column.",
                    }
                )
                continue
            src_len = _length_of(col.get("type", ""))
            tgt_max = tgt.get("char_max")
            if src_len and tgt_max and src_len > tgt_max:
                issues.append(
                    {
                        "severity": "error",
                        "category": "oversized_column",
                        "column": name,
                        "message": (
                            f"Source column '{name}' length {src_len} exceeds "
                            f"target capacity {tgt_max}."
                        ),
                    }
                )
    else:
        issues.append(
            {
                "severity": "info",
                "category": "target_missing",
                "column": "",
                "message": (
                    f"Target table '{target_table}' does not exist yet; "
                    "it will be created during schema conversion."
                ),
            }
        )

    has_error = any(i["severity"] == "error" for i in issues)
    return {
        "source_table": source_table,
        "target_table": target_table,
        "target_exists": target_exists,
        "column_count": len(source_schema.get("columns", [])),
        "ok": not has_error,
        "issues": issues,
    }


def validate_migration(
    source_manager,
    target_manager,
    table_pairs: list[tuple[str, str]],
    *,
    type_overrides: dict | None = None,
) -> dict:
    """Validate many source->target pairs. Returns a structured dry-run report."""
    tables = []
    for source_table, target_table in table_pairs:
        try:
            tables.append(
                validate_table(
                    source_manager,
                    target_manager,
                    source_table,
                    target_table,
                    type_overrides=type_overrides,
                )
            )
        except Exception as exc:
            tables.append(
                {
                    "source_table": source_table,
                    "target_table": target_table,
                    "target_exists": False,
                    "ok": False,
                    "issues": [
                        {
                            "severity": "error",
                            "category": "validation_error",
                            "column": "",
                            "message": str(exc),
                        }
                    ],
                }
            )

    errors = sum(
        1 for t in tables for i in t["issues"] if i["severity"] == "error"
    )
    warnings = sum(
        1 for t in tables for i in t["issues"] if i["severity"] == "warning"
    )
    return {
        "ok": errors == 0,
        "tables": tables,
        "summary": {
            "tables": len(tables),
            "errors": errors,
            "warnings": warnings,
            "tables_ok": sum(1 for t in tables if t["ok"]),
        },
    }
