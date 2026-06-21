"""Character-set / encoding helpers for data migration transfers."""

from __future__ import annotations

from decimal import Decimal


def get_conversion_charset() -> str:
    from schema_converter import module_config

    return (
        module_config.get("schema.conversion", "conversion_charset", default="utf-8")
        .strip()
        or "utf-8"
    )


def mysql_charset_name(charset: str) -> str:
    c = (charset or "utf-8").strip().lower().replace("-", "")
    if c in ("utf8", "utf"):
        return "utf8mb4"
    return charset


def postgres_encoding_name(charset: str) -> str:
    c = (charset or "utf-8").strip().lower()
    if c in ("utf8", "utf-8"):
        return "UTF8"
    return charset.upper()


def apply_connection_charset(manager, charset: str | None = None) -> None:
    """Set client encoding on an open connection used for data transfer."""
    if manager is None or getattr(manager, "conn", None) is None:
        return
    cs = (charset or get_conversion_charset()).strip()
    if not cs:
        return

    db_type = getattr(manager, "db_type", "")
    conn = manager.conn
    try:
        if db_type in ("MySQL", "MariaDB"):
            cursor = conn.cursor()
            cursor.execute(
                f"SET NAMES {mysql_charset_name(cs)} COLLATE "
                f"{mysql_charset_name(cs)}_unicode_ci"
            )
            cursor.close()
        elif db_type == "PostgreSQL":
            conn.set_client_encoding(postgres_encoding_name(cs))
        elif db_type == "Oracle":
            # oracledb uses environment encoding; best-effort session hint.
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "ALTER SESSION SET NLS_LANG='AMERICAN_AMERICA."
                    + postgres_encoding_name(cs)
                    + "'"
                )
            except Exception:
                pass
            cursor.close()
    except Exception:
        pass


def column_binary_flags(cursor, db_type: str) -> list[bool]:
    """Return True per column when the driver reports a binary type."""
    desc = getattr(cursor, "description", None) or []
    flags: list[bool] = []
    if db_type in ("MySQL", "MariaDB"):
        try:
            from mysql.connector.constants import FieldType

            binary_types = {
                FieldType.BLOB,
                FieldType.TINY_BLOB,
                FieldType.MEDIUM_BLOB,
                FieldType.LONG_BLOB,
                FieldType.BINARY,
                FieldType.VARBINARY,
                FieldType.GEOMETRY,
            }
            for col in desc:
                flags.append(getattr(col, "type", None) in binary_types)
            return flags
        except Exception:
            pass
    if db_type == "PostgreSQL":
        try:
            import psycopg2

            bytea_oid = psycopg2.extensions.BYTEA.values[0]
            for col in desc:
                flags.append(col[1] == bytea_oid)
            return flags
        except Exception:
            pass
    if db_type == "Oracle":
        for col in desc:
            name = (col[0] or "").upper()
            flags.append("BLOB" in name or "RAW" in name or name.endswith("_BIN"))
        return flags
    for col in desc:
        name = (col[0] or "").upper()
        flags.append(
            "BLOB" in name or "BINARY" in name or "BYTEA" in name or name.endswith("_BIN")
        )
    return flags


def convert_cell_value(
    value,
    *,
    charset: str,
    is_binary: bool = False,
    target_db_type: str = "",
):
    """Normalize a single cell for target insert."""
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        if is_binary:
            return bytes(value)
        try:
            return bytes(value).decode(charset, errors="replace")
        except Exception:
            return bytes(value)
    if isinstance(value, bool) and target_db_type == "Oracle":
        return 1 if value else 0
    if isinstance(value, Decimal):
        return value
    return value
