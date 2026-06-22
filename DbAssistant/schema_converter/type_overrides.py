"""User-defined data-type mapping overrides for schema conversion."""

from __future__ import annotations

import re

_TYPE_SIZE_RE = re.compile(
    r"(\w+(?:\s+\w+)?)\s*(?:\(([^)]+)\))?", re.IGNORECASE
)

# Target types that never carry a size/precision suffix.
_NO_SIZE_TYPES = frozenset({
    "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
    "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB",
    "CLOB", "NCLOB", "BYTEA", "JSON", "JSONB", "XML",
    "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT",
    "SERIAL", "BIGSERIAL", "SMALLSERIAL",
    "DATE", "DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "TIMETZ",
    "BOOLEAN", "BOOL", "REAL", "DOUBLE", "FLOAT", "UUID",
})

# Source/target types that accept (size) or (precision,scale).
_SIZE_TAKING_TYPES = frozenset({
    "VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CHARACTER",
    "CHARACTER VARYING", "DECIMAL", "NUMERIC", "NUMBER",
    "BINARY", "VARBINARY", "RAW", "BIT", "FLOAT", "DOUBLE",
})


def parse_base_type(source_type_str: str) -> tuple[str, str | None]:
    """Return (BASE_TYPE, size_spec) from a column type string."""
    text = str(source_type_str or "").strip()
    if isinstance(source_type_str, bytes):
        text = source_type_str.decode("utf-8", errors="replace")
    match = _TYPE_SIZE_RE.match(text.upper())
    if not match:
        return text.upper(), None
    return match.group(1), match.group(2)


def _type_accepts_size(type_name: str) -> bool:
    base = type_name.split("(")[0].strip().upper()
    if base in _NO_SIZE_TYPES:
        return False
    return base in _SIZE_TAKING_TYPES


def parse_type_overrides(text: str) -> dict[str, str]:
    """Parse ``"varchar2:text, int:decimal"`` into ``{VARCHAR2: TEXT, INT: DECIMAL}``."""
    result: dict[str, str] = {}
    if not text or not str(text).strip():
        return result
    raw = str(text).strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        raw = raw[1:-1]
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        src, tgt = part.split(":", 1)
        src_key = src.strip().upper()
        tgt_val = tgt.strip().upper()
        if src_key and tgt_val:
            result[src_key] = tgt_val
    return result


def resolve_type_overrides(override_text: str | None = None) -> dict[str, str]:
    """Merge config defaults with an optional per-run override string."""
    from schema_converter import module_config

    defaults = parse_type_overrides(
        module_config.get("schema.conversion", "type_overrides", default="")
    )
    if override_text is not None and str(override_text).strip():
        merged = dict(defaults)
        merged.update(parse_type_overrides(override_text))
        return merged
    return defaults


def apply_type_override(source_type_str: str, target_type_name: str) -> str:
    """Map *source_type_str* to *target_type_name*, preserving size when applicable."""
    _base, size_spec = parse_base_type(source_type_str)
    target = str(target_type_name or "").strip().upper()
    if not target:
        return str(source_type_str)

    if not _type_accepts_size(target):
        return target

    if size_spec:
        return f"{target}({size_spec})"

    source_base, _ = parse_base_type(source_type_str)
    if target in ("DECIMAL", "NUMERIC", "NUMBER") and source_base in (
        "INT", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",
    ):
        return f"{target}(10,0)"

    return target
