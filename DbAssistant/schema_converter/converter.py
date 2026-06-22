# ---------------------------------------------------------------------
# description: Schema converter for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""
Universal Schema and Data Converter
Supports: Oracle, MySQL, MariaDB, PostgreSQL
Converts database schemas and transfers data between any supported databases
"""

import re
from datetime import datetime
from common.config_loader import config, get_compare_sample_size
from schema_converter import module_config as _mod_cfg
from schema_converter.compare_options import DataCompareOptions
from schema_converter.charset import (
    apply_connection_charset,
    column_binary_flags,
    convert_cell_value,
    get_conversion_charset,
)
from schema_converter.transfer_options import (
    RowSkip,
    TransferOptions,
    TransferRuntime,
    ValueOverflow,
    build_select_sql,
    transform_value,
)


class DataTypeMapper:
    """Universal data type mapper for all supported databases"""

    # Canonical type mappings - each database type maps to these canonical types
    # Then we convert from canonical to target database

    # Oracle to Canonical
    ORACLE_CANONICAL = {
        "VARCHAR2": "VARCHAR",
        "NVARCHAR2": "VARCHAR",
        "CHAR": "CHAR",
        "NCHAR": "CHAR",
        "NUMBER": "NUMERIC",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "SMALLINT": "SMALLINT",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "DOUBLE PRECISION": "DOUBLE",
        "DATE": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "CLOB": "TEXT",
        "NCLOB": "TEXT",
        "BLOB": "BLOB",
        "RAW": "BINARY",
        "LONG": "TEXT",
        "LONG RAW": "BLOB",
    }

    # MySQL/MariaDB to Canonical (same mapping for both)
    MYSQL_CANONICAL = {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "TINYTEXT": "TEXT",
        "MEDIUMTEXT": "TEXT",
        "LONGTEXT": "TEXT",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "TINYINT": "SMALLINT",
        "SMALLINT": "SMALLINT",
        "MEDIUMINT": "INTEGER",
        "BIGINT": "BIGINT",
        "DECIMAL": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "FLOAT": "FLOAT",
        "DOUBLE": "DOUBLE",
        "REAL": "REAL",
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "TIME",
        "YEAR": "SMALLINT",
        "BLOB": "BLOB",
        "TINYBLOB": "BLOB",
        "MEDIUMBLOB": "BLOB",
        "LONGBLOB": "BLOB",
        "BINARY": "BINARY",
        "VARBINARY": "BINARY",
        "ENUM": "VARCHAR",
        "SET": "VARCHAR",
        "JSON": "TEXT",
    }

    # PostgreSQL to Canonical
    POSTGRES_CANONICAL = {
        "VARCHAR": "VARCHAR",
        "CHARACTER VARYING": "VARCHAR",
        "CHAR": "CHAR",
        "CHARACTER": "CHAR",
        "TEXT": "TEXT",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "INT4": "INTEGER",
        "SMALLINT": "SMALLINT",
        "INT2": "SMALLINT",
        "BIGINT": "BIGINT",
        "INT8": "BIGINT",
        "DECIMAL": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "REAL": "REAL",
        "FLOAT4": "REAL",
        "DOUBLE PRECISION": "DOUBLE",
        "FLOAT8": "DOUBLE",
        "FLOAT": "FLOAT",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMPTZ": "TIMESTAMP",
        "TIME": "TIME",
        "TIMETZ": "TIME",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
        "BYTEA": "BLOB",
        "UUID": "CHAR",
        "JSON": "TEXT",
        "JSONB": "TEXT",
        "XML": "TEXT",
        "SERIAL": "INTEGER",
        "BIGSERIAL": "BIGINT",
        "SMALLSERIAL": "SMALLINT",
    }

    # Canonical to Oracle
    CANONICAL_ORACLE = {
        "VARCHAR": "VARCHAR2",
        "CHAR": "CHAR",
        "TEXT": "CLOB",
        "INTEGER": "NUMBER(10)",
        "SMALLINT": "NUMBER(5)",
        "BIGINT": "NUMBER(19)",
        "NUMERIC": "NUMBER",
        "FLOAT": "FLOAT",
        "REAL": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "VARCHAR2(8)",
        "BOOLEAN": "NUMBER(1)",
        "BLOB": "BLOB",
        "BINARY": "RAW",
    }

    # Canonical to MySQL/MariaDB
    CANONICAL_MYSQL = {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "INTEGER": "INT",
        "SMALLINT": "SMALLINT",
        "BIGINT": "BIGINT",
        "NUMERIC": "DECIMAL",
        "FLOAT": "FLOAT",
        "REAL": "FLOAT",
        "DOUBLE": "DOUBLE",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "TIME",
        "BOOLEAN": "TINYINT(1)",
        "BLOB": "BLOB",
        "BINARY": "VARBINARY",
    }

    # Canonical to PostgreSQL
    CANONICAL_POSTGRES = {
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "TEXT": "TEXT",
        "INTEGER": "INTEGER",
        "SMALLINT": "SMALLINT",
        "BIGINT": "BIGINT",
        "NUMERIC": "NUMERIC",
        "FLOAT": "REAL",
        "REAL": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "TIME": "TIME",
        "BOOLEAN": "BOOLEAN",
        "BLOB": "BYTEA",
        "BINARY": "BYTEA",
    }

    @staticmethod
    def convert_type(source_type_str, source_db, target_db):
        """
        Universal type conversion between any supported databases
        source_db and target_db: 'Oracle', 'MySQL', 'MariaDB', 'PostgreSQL'
        """
        # Convert bytes to string if needed
        if isinstance(source_type_str, bytes):
            source_type_str = source_type_str.decode("utf-8")

        # Convert to string if not already
        source_type_str = str(source_type_str)

        # Extract base type and size/precision
        match = re.match(
            r"(\w+(?:\s+\w+)?)\s*(?:\(([^)]+)\))?", source_type_str.upper()
        )
        if not match:
            return source_type_str

        base_type = match.group(1)
        size_spec = match.group(2)

        # If source and target are the same family, preserve native type definitions
        if source_db == target_db:
            return source_type_str
        if source_db in ("MySQL", "MariaDB") and target_db in ("MySQL", "MariaDB"):
            return source_type_str
        if source_db == "PostgreSQL" and target_db == "PostgreSQL":
            return source_type_str
        if source_db == "Oracle" and target_db == "Oracle":
            return source_type_str

        # MySQL and MariaDB use same mappings
        if source_db == "MariaDB":
            source_db = "MySQL"
        if target_db == "MariaDB":
            target_db = "MySQL"

        # Step 1: Convert source type to canonical type
        if source_db == "Oracle":
            canonical_map = DataTypeMapper.ORACLE_CANONICAL
        elif source_db == "MySQL":
            canonical_map = DataTypeMapper.MYSQL_CANONICAL
        elif source_db == "PostgreSQL":
            canonical_map = DataTypeMapper.POSTGRES_CANONICAL
        else:
            return source_type_str

        canonical_type = canonical_map.get(base_type, base_type)

        # Special handling for Oracle NUMBER type
        if source_db == "Oracle" and base_type == "NUMBER":
            if size_spec:
                parts = size_spec.split(",")
                if len(parts) == 2:
                    canonical_type = "NUMERIC"
                elif len(parts) == 1:
                    precision = int(parts[0])
                    if precision <= 5:
                        canonical_type = "SMALLINT"
                    elif precision <= 10:
                        canonical_type = "INTEGER"
                    elif precision <= 19:
                        canonical_type = "BIGINT"
                    else:
                        canonical_type = "NUMERIC"
            else:
                canonical_type = "NUMERIC"

        # Step 2: Convert canonical type to target type
        if target_db == "Oracle":
            target_map = DataTypeMapper.CANONICAL_ORACLE
        elif target_db == "MySQL":
            target_map = DataTypeMapper.CANONICAL_MYSQL
        elif target_db == "PostgreSQL":
            target_map = DataTypeMapper.CANONICAL_POSTGRES
        else:
            return source_type_str

        target_type = target_map.get(canonical_type, canonical_type)

        # Step 3: Handle size specifications
        if size_spec:
            # VARCHAR, CHAR types
            if canonical_type in ["VARCHAR", "CHAR"]:
                size = int(size_spec)
                # Oracle VARCHAR2 max 4000
                if target_db == "Oracle" and size > 4000:
                    return "CLOB"
                # PostgreSQL VARCHAR can have size
                # MySQL VARCHAR max 65535
                if target_db == "MySQL" and size > 65535:
                    return "TEXT"
                return f"{target_type}({size_spec})"

            # NUMERIC/DECIMAL types
            elif canonical_type == "NUMERIC":
                if target_db == "Oracle":
                    return f"NUMBER({size_spec})"
                else:
                    return f"{target_type}({size_spec})"

            # BINARY types
            elif canonical_type == "BINARY":
                if target_db == "Oracle":
                    return f"RAW({size_spec})"
                elif target_db == "PostgreSQL":
                    return "BYTEA"
                else:
                    return f"{target_type}({size_spec})"

        return target_type


class DefaultValueFormatter:
    """Convert and format column DEFAULT clauses across database engines."""

    _DATE_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    _DATETIME_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")
    _TIME_LITERAL = re.compile(r"^\d{2}:\d{2}:\d{2}$")
    _NUMERIC_LITERAL = re.compile(r"^-?\d+(\.\d+)?$")

    _FUNCTION_ALIASES = {
        "NOW": "CURRENT_TIMESTAMP",
        "NOW()": "CURRENT_TIMESTAMP",
        "LOCALTIME": "CURRENT_TIMESTAMP",
        "LOCALTIME()": "CURRENT_TIMESTAMP",
        "LOCALTIMESTAMP": "CURRENT_TIMESTAMP",
        "LOCALTIMESTAMP()": "CURRENT_TIMESTAMP",
        "UTC_TIMESTAMP": "CURRENT_TIMESTAMP",
        "UTC_TIMESTAMP()": "CURRENT_TIMESTAMP",
        "GETDATE()": "CURRENT_TIMESTAMP",
        "GETDATE": "CURRENT_TIMESTAMP",
        "SYSDATE": "CURRENT_TIMESTAMP",
        "SYSTIMESTAMP": "CURRENT_TIMESTAMP",
        "CURRENT_TIMESTAMP()": "CURRENT_TIMESTAMP",
        "CURRENT_DATE()": "CURRENT_DATE",
        "CURRENT_TIME()": "CURRENT_TIME",
    }

    @classmethod
    def convert_default(
        cls,
        default,
        col_type,
        source_db,
        target_db,
        auto_increment=False,
    ):
        """Return a DEFAULT expression for *target_db*, or None to omit."""
        if auto_increment:
            return None
        if default is None:
            return None

        value = str(default).strip()
        if not value or value.upper() == "NULL":
            return None

        if "nextval(" in value.lower():
            return None

        value = cls._strip_pg_cast(value)
        value = cls._strip_oracle_string_wrapper(value)

        if cls._is_function_default(value):
            return cls._format_function_default(value, target_db)

        if cls._is_quoted(value):
            return cls._format_quoted_default(value, col_type, target_db)

        col_type_upper = (col_type or "").upper()

        if cls._is_temporal_type(col_type_upper):
            if (
                cls._DATE_LITERAL.match(value)
                or cls._DATETIME_LITERAL.match(value)
                or cls._TIME_LITERAL.match(value)
            ):
                return cls._quote(value.replace("T", " "), target_db)

        if cls._is_bit_type(col_type_upper):
            if value.lower().startswith("b'"):
                return value
            return f"b'{value}'"

        if cls._is_boolean_type(col_type_upper):
            return cls._format_bool(value, target_db)

        if cls._is_numeric_type(col_type_upper) and cls._NUMERIC_LITERAL.match(value):
            return value

        if cls._is_string_type(col_type_upper) or not cls._NUMERIC_LITERAL.match(value):
            if cls._DATE_LITERAL.match(value) or cls._DATETIME_LITERAL.match(value):
                return cls._quote(value.replace("T", " "), target_db)
            if cls._TIME_LITERAL.match(value):
                return cls._quote(value, target_db)
            return cls._quote(value.replace("'", "''"), target_db)

        return value

    @classmethod
    def parse_mysql_extra(cls, extra):
        """Extract AUTO_INCREMENT and ON UPDATE from MySQL/MariaDB column extra."""
        if not extra:
            return {}
        text = str(extra).strip()
        lower = text.lower()
        parsed = {}
        if "auto_increment" in lower:
            parsed["auto_increment"] = True
        match = re.search(
            r"on update\s+(current_timestamp(?:\(\d*\))?|now\s*\(\s*\))",
            text,
            re.IGNORECASE,
        )
        if match:
            token = match.group(1).upper().replace("NOW()", "CURRENT_TIMESTAMP")
            parsed["on_update"] = token
        return parsed

    @classmethod
    def format_on_update(cls, on_update, target_db):
        if not on_update:
            return None
        if target_db not in ("MySQL", "MariaDB"):
            return None
        return cls._format_function_default(str(on_update), target_db)

    @classmethod
    def _strip_pg_cast(cls, value):
        if "::" in value:
            return value.split("::", 1)[0].strip()
        return value

    @classmethod
    def _strip_oracle_string_wrapper(cls, value):
        upper = value.upper()
        if upper.startswith("TO_DATE(") or upper.startswith("TO_TIMESTAMP("):
            match = re.search(r"'((?:[^']|'')*)'", value)
            if match:
                inner = match.group(1).replace("''", "'")
                if cls._DATE_LITERAL.match(inner):
                    return inner
                if cls._DATETIME_LITERAL.match(inner.replace("T", " ")):
                    return inner.replace("T", " ")
        return value

    @classmethod
    def _is_function_default(cls, value):
        upper = value.upper().strip().rstrip("()")
        normalized = value.upper().strip()
        if normalized in cls._FUNCTION_ALIASES:
            return True
        if normalized.rstrip("()") in {
            k.rstrip("()") for k in cls._FUNCTION_ALIASES
        }:
            return True
        if "(" in value and upper.split("(")[0] in {
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "NOW",
            "SYSDATE",
            "SYSTIMESTAMP",
            "UUID",
            "GEN_RANDOM_UUID",
        }:
            return True
        return False

    @classmethod
    def _format_function_default(cls, value, target_db):
        normalized = value.upper().strip()
        canonical = cls._FUNCTION_ALIASES.get(normalized)
        if canonical is None:
            canonical = cls._FUNCTION_ALIASES.get(normalized.rstrip("()"))
        if canonical is None:
            canonical = normalized.rstrip("()")

        if target_db in ("MySQL", "MariaDB"):
            if canonical in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
                return canonical
            if canonical == "UUID":
                return "(UUID())"
            return canonical

        if target_db == "Oracle":
            if canonical == "CURRENT_TIMESTAMP":
                return "SYSTIMESTAMP"
            if canonical == "CURRENT_DATE":
                return "SYSDATE"
            if canonical == "CURRENT_TIME":
                return "SYSDATE"
            return value

        if target_db == "PostgreSQL":
            if canonical == "CURRENT_TIMESTAMP":
                return "CURRENT_TIMESTAMP"
            if canonical in {"CURRENT_DATE", "CURRENT_TIME"}:
                return canonical
            if canonical == "UUID":
                return "gen_random_uuid()"
            return value.lower() if canonical == "CURRENT_TIMESTAMP" else value

        return value

    @classmethod
    def _format_quoted_default(cls, value, col_type, target_db):
        inner = value[1:-1]
        if target_db == "Oracle" and cls._is_temporal_type((col_type or "").upper()):
            if cls._DATE_LITERAL.match(inner):
                return f"TO_DATE('{inner}', 'YYYY-MM-DD')"
            if cls._DATETIME_LITERAL.match(inner.replace("T", " ")):
                dt = inner.replace("T", " ")
                return f"TO_TIMESTAMP('{dt}', 'YYYY-MM-DD HH24:MI:SS')"
        return value if value.startswith("'") else cls._quote(inner, target_db)

    @classmethod
    def _format_bool(cls, value, target_db):
        truthy = str(value).strip().lower() in {"1", "true", "t", "yes", "y"}
        if target_db in ("MySQL", "MariaDB"):
            return "1" if truthy else "0"
        if target_db == "Oracle":
            return "1" if truthy else "0"
        return "TRUE" if truthy else "FALSE"

    @classmethod
    def _quote(cls, value, target_db):
        if target_db == "Oracle":
            return f"'{value}'"
        return f"'{value}'"

    @classmethod
    def _is_quoted(cls, value):
        return (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        )

    @classmethod
    def _is_temporal_type(cls, col_type):
        return any(
            token in col_type
            for token in (
                "DATE",
                "TIME",
                "TIMESTAMP",
                "DATETIME",
                "YEAR",
            )
        )

    @classmethod
    def _is_string_type(cls, col_type):
        return any(
            token in col_type
            for token in (
                "CHAR",
                "TEXT",
                "CLOB",
                "JSON",
                "ENUM",
                "SET",
                "UUID",
            )
        )

    @classmethod
    def _is_numeric_type(cls, col_type):
        return any(
            token in col_type
            for token in (
                "INT",
                "DECIMAL",
                "NUMERIC",
                "NUMBER",
                "FLOAT",
                "DOUBLE",
                "REAL",
                "BIT",
            )
        )

    @classmethod
    def _is_bit_type(cls, col_type):
        return "BIT" in col_type or col_type.startswith("BINARY")

    @classmethod
    def _is_boolean_type(cls, col_type):
        return "BOOL" in col_type


class SchemaConverter:
    """Universal database schema converter"""

    def __init__(self, source_db_manager, target_db_manager):
        self.source_manager = source_db_manager
        self.target_manager = target_db_manager
        self.source_type = source_db_manager.db_type
        self.target_type = target_db_manager.db_type

    @staticmethod
    def _ensure_str(value):
        """Convert bytes to string if needed, handle None"""
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value) if value is not None else None

    def get_table_schema(self, table_name):
        """Get full table schema from source database."""
        if self.source_type == "Oracle":
            schema = self._get_oracle_table_schema(table_name)
        elif self.source_type in ["MySQL", "MariaDB"]:
            schema = self._get_mysql_table_schema(table_name)
        elif self.source_type == "PostgreSQL":
            schema = self._get_postgres_table_schema(table_name)
        else:
            schema = None
        if schema:
            from .schema_full import enrich_table_schema

            schema = enrich_table_schema(
                schema, self.source_type, self.source_manager.conn, table_name
            )
        return schema

    def _get_oracle_table_schema(self, table_name):
        """Get Oracle table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        schema = {
            "table_name": table_name,
            "columns": [],
            "primary_key": [],
            "indexes": [],
            "foreign_keys": [],
        }

        # Get columns
        cursor.execute(
            """
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, data_default
            FROM user_tab_columns
            WHERE table_name = :1
            ORDER BY column_id
        """,
            [table_name.upper()],
        )

        for row in cursor.fetchall():
            (
                col_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable,
                default,
            ) = row

            # Ensure all string values are properly decoded
            col_name = self._ensure_str(col_name)
            data_type = self._ensure_str(data_type)
            nullable = self._ensure_str(nullable)

            # Build full type
            if data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR", "RAW"):
                full_type = f"{data_type}({data_length})"
            elif data_type == "NUMBER":
                if data_precision:
                    if data_scale and data_scale > 0:
                        full_type = f"NUMBER({data_precision},{data_scale})"
                    else:
                        full_type = f"NUMBER({data_precision})"
                else:
                    full_type = "NUMBER"
            else:
                full_type = data_type

            schema["columns"].append(
                {
                    "name": col_name,
                    "type": full_type,
                    "nullable": nullable == "Y",
                    "default": self._ensure_str(default),
                }
            )

        # Get primary key
        cursor.execute(
            """
            SELECT cols.column_name
            FROM user_constraints cons, user_cons_columns cols
            WHERE cons.constraint_name = cols.constraint_name
            AND cons.constraint_type = 'P'
            AND cons.table_name = :1
            ORDER BY cols.position
        """,
            [table_name.upper()],
        )

        schema["primary_key"] = [self._ensure_str(row[0]) for row in cursor.fetchall()]

        # Get indexes
        cursor.execute(
            """
            SELECT index_name, column_name
            FROM user_ind_columns
            WHERE table_name = :1
            AND index_name NOT IN (
                SELECT constraint_name FROM user_constraints
                WHERE table_name = :1 AND constraint_type = 'P'
            )
            ORDER BY index_name, column_position
        """,
            [table_name.upper(), table_name.upper()],
        )

        indexes = {}
        for row in cursor.fetchall():
            idx_name, col_name = row
            idx_name = self._ensure_str(idx_name)
            col_name = self._ensure_str(col_name)
            if idx_name not in indexes:
                indexes[idx_name] = []
            indexes[idx_name].append(col_name)

        schema["indexes"] = [{"name": k, "columns": v} for k, v in indexes.items()]

        cursor.close()
        return schema

    def _get_mysql_table_schema(self, table_name):
        """Get MySQL/MariaDB table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        schema = {
            "table_name": table_name,
            "columns": [],
            "primary_key": [],
            "indexes": [],
            "foreign_keys": [],
        }

        # Get columns
        cursor.execute(f"DESCRIBE {table_name}")

        for row in cursor.fetchall():
            field, type_str, null, key, default, extra = row

            # Ensure all string values are properly decoded
            field = self._ensure_str(field)
            type_str = self._ensure_str(type_str)
            null = self._ensure_str(null)
            key = self._ensure_str(key)

            schema["columns"].append(
                {
                    "name": field,
                    "type": type_str,
                    "nullable": null == "YES",
                    "default": self._ensure_str(default),
                    "extra": self._ensure_str(extra),
                }
            )

            if key == "PRI":
                schema["primary_key"].append(field)

        # Get indexes
        cursor.execute(f"SHOW INDEX FROM {table_name}")

        indexes = {}
        for row in cursor.fetchall():
            _, _, key_name, _, col_name = (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
            )
            key_name = self._ensure_str(key_name)
            col_name = self._ensure_str(col_name)
            if key_name != "PRIMARY":
                if key_name not in indexes:
                    indexes[key_name] = []
                indexes[key_name].append(col_name)

        schema["indexes"] = [{"name": k, "columns": v} for k, v in indexes.items()]

        cursor.close()
        return schema

    def _get_postgres_table_schema(self, table_name):
        """Get PostgreSQL table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        # Handle schema.table format
        if "." in table_name:
            schema_name, table_only = table_name.split(".", 1)
        else:
            schema_name = "public"
            table_only = table_name

        schema = {
            "table_name": table_only,
            "columns": [],
            "primary_key": [],
            "indexes": [],
            "foreign_keys": [],
        }

        # Get columns
        cursor.execute(
            """
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """,
            [schema_name, table_only],
        )

        for row in cursor.fetchall():
            (
                col_name,
                data_type,
                char_length,
                num_precision,
                num_scale,
                nullable,
                default,
            ) = row

            # Ensure all string values are properly decoded
            col_name = self._ensure_str(col_name)
            data_type = self._ensure_str(data_type)
            nullable = self._ensure_str(nullable)

            # Build full type
            if data_type in ("character varying", "character", "bit", "bit varying"):
                if char_length:
                    full_type = f"{data_type}({char_length})"
                else:
                    full_type = data_type
            elif data_type in ("numeric", "decimal"):
                if num_precision:
                    if num_scale:
                        full_type = f"{data_type}({num_precision},{num_scale})"
                    else:
                        full_type = f"{data_type}({num_precision})"
                else:
                    full_type = data_type
            else:
                full_type = data_type

            # Check for auto-increment (serial types or sequences)
            default_str = self._ensure_str(default)
            is_auto_increment = default_str and "nextval" in default_str.lower()

            schema["columns"].append(
                {
                    "name": col_name,
                    "type": full_type,
                    "nullable": nullable == "YES",
                    "default": default_str,
                    "auto_increment": is_auto_increment,
                }
            )

        # Get primary key
        cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """,
            [schema_name, table_only],
        )

        schema["primary_key"] = [self._ensure_str(row[0]) for row in cursor.fetchall()]

        # Get indexes
        cursor.execute(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
        """,
            [schema_name, table_only],
        )

        for row in cursor.fetchall():
            idx_name, idx_def = row
            idx_name = self._ensure_str(idx_name)
            idx_def = self._ensure_str(idx_def)
            # Skip primary key indexes
            if idx_def and "PRIMARY KEY" not in idx_def.upper():
                # Extract column names from index definition (simplified)
                schema["indexes"].append({"name": idx_name, "columns": []})

        cursor.close()
        return schema

    def convert_schema(self, source_schema, table_name_map=None, type_overrides=None):
        """Convert schema from source to target database format."""
        from .schema_full import convert_extended_schema

        return convert_extended_schema(
            source_schema,
            self.source_type,
            self.target_type,
            table_name_map=table_name_map,
            type_overrides=type_overrides,
        )

    def generate_all_table_ddl(self, schema):
        """Generate ordered DDL statements for a converted table schema."""
        from .schema_full import generate_all_table_ddl as build_all_table_ddl

        statements = build_all_table_ddl(schema, self.target_type)
        expanded = []
        for stmt in statements:
            if self.target_type == "Oracle" and "CREATE SEQUENCE" in stmt:
                for part in stmt.split("\n\n"):
                    part = part.strip()
                    if part:
                        expanded.append(part)
            else:
                expanded.append(stmt)
        return expanded

    def generate_create_table_ddl(self, schema):
        """Generate CREATE TABLE statement for target database."""
        ddl_list = self.generate_all_table_ddl(schema)
        return ddl_list[0] if ddl_list else None

    def generate_indexes_ddl(self, schema):
        """Generate CREATE INDEX and post-create statements (excluding CREATE TABLE)."""
        from .schema_full import (
            _generate_index_ddl,
            _generate_foreign_key_ddl,
            _generate_comment_ddl,
            _generate_trigger_ddl,
            _generate_sequence_ddl,
            _normalize_index_columns,
        )

        if self.target_type not in ("MySQL", "MariaDB", "Oracle", "PostgreSQL"):
            return []

        table = schema.get("table_name")
        statements: list[str] = []
        for idx in schema.get("indexes") or []:
            idx_norm = dict(idx)
            idx_norm["columns"] = _normalize_index_columns(idx.get("columns"))
            sql = _generate_index_ddl(table, idx_norm, self.target_type)
            if sql:
                statements.append(sql)
        for fk in schema.get("foreign_keys") or []:
            sql = _generate_foreign_key_ddl(table, fk, self.target_type)
            if sql:
                statements.append(sql)
        for seq in schema.get("sequences") or []:
            sql = _generate_sequence_ddl(seq, self.target_type)
            if sql:
                statements.append(sql)
        statements.extend(_generate_comment_ddl(schema, self.target_type))
        statements.extend(_generate_trigger_ddl(schema, self.target_type))
        return statements


class DataConverter:
    """Universal data converter for all supported databases"""

    def __init__(self, source_db_manager, target_db_manager):
        self.source_manager = source_db_manager
        self.target_manager = target_db_manager
        self.source_type = source_db_manager.db_type
        self.target_type = target_db_manager.db_type

    def transfer_table_data(
        self,
        source_table,
        target_table,
        runtime=None,
        progress_callback=None,
        stop_event=None,
        **legacy_runtime,
    ):
        """
        Transfer data from source table to target table.

        Args:
            source_table: Source table name
            target_table: Target table name
            runtime: Optional :class:`TransferRuntime` carrying batch size,
                     progress callback, stop event, transfer options and
                     checkpoint store. Legacy keyword controls are also accepted
                     and reconstructed into a runtime object.

        Returns the number of rows transferred. Detailed per-table stats
        (skipped rows, per-row errors, duration) are recorded on
        ``self.last_transfer_stats``.
        """
        import time

        if runtime is not None and not isinstance(runtime, (TransferRuntime, dict)):
            legacy_runtime = {
                **legacy_runtime,
                "batch_size": runtime,
                "progress_callback": progress_callback,
                "stop_event": stop_event,
            }
            runtime = None
        runtime = TransferRuntime.from_source(runtime or legacy_runtime)
        options = runtime.options or TransferOptions()
        batch_size = runtime.batch_size
        progress_callback = runtime.progress_callback
        stop_event = runtime.stop_event
        checkpoint_store = runtime.checkpoint_store

        if batch_size is None:
            batch_size = config.get_int(
                "database.performance", "transfer_batch_size", default=1000
            )

        source_conn = self.source_manager.conn
        target_conn = self.target_manager.conn
        charset = get_conversion_charset()
        apply_connection_charset(self.source_manager, charset)
        apply_connection_charset(self.target_manager, charset)

        start_perf = time.perf_counter()
        errors: list[dict] = []
        skipped = 0

        # Resume offset (G9): how many already-committed rows to skip.
        resume_offset = 0
        order_by = None
        if checkpoint_store is not None:
            resume_offset = checkpoint_store.get(source_table, target_table)
            if resume_offset:
                order_by = self._resolve_order_key(source_table, options)

        # Estimated row count for progress reporting.
        total_rows = None
        min_rows_for_estimation = config.get_int(
            "database.performance", "batch_min_rows", default=10
        )
        if progress_callback:
            try:
                total_rows = self.get_estimated_row_count(source_table, is_source=True)
                if total_rows < min_rows_for_estimation:
                    total_rows = None
            except Exception:
                total_rows = None

        if self.source_type in ["MySQL", "MariaDB"]:
            source_cursor = source_conn.cursor(buffered=True)
        else:
            source_cursor = source_conn.cursor()

        target_cursor = None
        rows_transferred = 0
        rows_read = 0
        batch = []

        try:
            select_sql = build_select_sql(
                source_table,
                options.columns or None,
                options.where,
                options.limit,
                self.source_type,
                order_by=order_by,
            )
            source_cursor.execute(select_sql)

            source_columns = [desc[0] for desc in source_cursor.description]
            binary_flags = column_binary_flags(source_cursor, self.source_type)
            if len(binary_flags) < len(source_columns):
                binary_flags.extend([False] * (len(source_columns) - len(binary_flags)))

            # G2: rename target columns where a mapping is supplied.
            target_columns = [options.column_map.get(c, c) for c in source_columns]
            col_names = ", ".join(target_columns)

            # G4: target column limits for overflow handling.
            col_limits = self._get_target_column_limits(target_table, target_columns)

            if self.target_type == "Oracle":
                placeholders = ", ".join([f":{i+1}" for i in range(len(target_columns))])
            else:  # MySQL / MariaDB / PostgreSQL
                placeholders = ", ".join(["%s"] * len(target_columns))

            insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})"

            target_cursor = target_conn.cursor()

            def _flush(current_batch):
                nonlocal rows_transferred, skipped
                if not current_batch:
                    return
                if options.continue_on_error:
                    committed = self._insert_batch_resilient(
                        target_cursor, target_conn, insert_sql, current_batch, errors
                    )
                    rows_transferred += committed
                else:
                    target_cursor.executemany(insert_sql, current_batch)
                    target_conn.commit()
                    rows_transferred += len(current_batch)
                if checkpoint_store is not None:
                    checkpoint_store.set(
                        source_table, target_table, resume_offset + rows_transferred
                    )

            for row in source_cursor:
                if stop_event and stop_event.is_set():
                    _flush(batch)
                    batch = []
                    break

                rows_read += 1
                # G9: skip already-committed rows on resume.
                if rows_read <= resume_offset:
                    continue

                try:
                    converted_row = self._convert_row_data(
                        row,
                        binary_flags=binary_flags,
                        charset=charset,
                        options=options,
                        col_limits=col_limits,
                        column_names=target_columns,
                    )
                except RowSkip as skip_exc:
                    skipped += 1
                    if len(errors) < _mod_cfg.get_int(
                        "schema.conversion", "transfer_error_limit", default=1000
                    ):
                        errors.append({"type": "skip", "message": str(skip_exc)})
                    continue
                except ValueOverflow as overflow_exc:
                    if options.continue_on_error:
                        skipped += 1
                        errors.append({"type": "overflow", "message": str(overflow_exc)})
                        continue
                    raise

                batch.append(converted_row)

                if len(batch) >= batch_size:
                    _flush(batch)
                    batch = []
                    if progress_callback:
                        progress_callback(rows_transferred, total_rows)
                    if stop_event and stop_event.is_set():
                        break

            if batch:
                _flush(batch)
                batch = []
                if progress_callback:
                    progress_callback(rows_transferred, total_rows)

            self.last_transfer_stats = {
                "rows_transferred": rows_transferred,
                "skipped": skipped,
                "errors": errors,
                "error_count": len(errors),
                "duration_seconds": round(time.perf_counter() - start_perf, 3),
            }

            # G8: reset target auto-increment / sequence after a full load.
            if options.reset_sequences and not (stop_event and stop_event.is_set()):
                try:
                    self.reset_target_sequence(target_table)
                    self.last_transfer_stats["sequence_reset"] = True
                except Exception as exc:
                    self.last_transfer_stats["sequence_reset_error"] = str(exc)

            return rows_transferred
        except Exception:
            self.last_transfer_stats = {
                "rows_transferred": rows_transferred,
                "skipped": skipped,
                "errors": errors,
                "error_count": len(errors),
                "duration_seconds": round(time.perf_counter() - start_perf, 3),
            }
            raise
        finally:
            self._close_cursor_quietly(source_cursor)
            self._close_cursor_quietly(target_cursor)

    @staticmethod
    def _close_cursor_quietly(cursor) -> None:
        """Close a DB cursor without masking the original transfer outcome."""
        if cursor is None:
            return
        try:
            cursor.close()
        except Exception:
            pass

    def _insert_batch_resilient(self, cursor, conn, insert_sql, batch, errors):
        """Insert a batch; on failure fall back to per-row inserts (G3)."""
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            return len(batch)
        except Exception:
            conn.rollback()
        committed = 0
        for row in batch:
            try:
                cursor.execute(insert_sql, row)
                conn.commit()
                committed += 1
            except Exception as exc:
                conn.rollback()
                if len(errors) < _mod_cfg.get_int(
                    "schema.conversion", "transfer_error_limit", default=1000
                ):
                    errors.append({"type": "row", "message": str(exc)})
        return committed

    def _convert_row_data(
        self,
        row,
        binary_flags=None,
        charset=None,
        options=None,
        col_limits=None,
        column_names=None,
    ):
        """Convert row data types for compatibility, applying transfer policies."""
        flags = binary_flags or []
        cs = charset or get_conversion_charset()
        limits = col_limits or []
        names = column_names or []

        # Fast path: no extra policies requested -> preserve historical behaviour.
        if options is None or not options.has_value_policies:
            converted = []
            for idx, value in enumerate(row):
                is_binary = flags[idx] if idx < len(flags) else False
                if isinstance(value, datetime):
                    converted.append(value)
                    continue
                converted.append(
                    convert_cell_value(
                        value,
                        charset=cs,
                        is_binary=is_binary,
                        target_db_type=self.target_type,
                    )
                )
            return tuple(converted)

        converted = []
        for idx, value in enumerate(row):
            is_binary = flags[idx] if idx < len(flags) else False
            col_limit = limits[idx] if idx < len(limits) else None
            column_name = names[idx] if idx < len(names) else ""
            converted.append(
                transform_value(
                    value,
                    col_limit=col_limit,
                    options=options,
                    target_db_type=self.target_type,
                    charset=cs,
                    is_binary=is_binary,
                    column_name=column_name,
                )
            )
        return tuple(converted)

    def _resolve_order_key(self, source_table, options):
        """Stable ORDER BY columns for resumeable reads (G9)."""
        try:
            sc = SchemaConverter(self.source_manager, self.target_manager)
            schema = sc.get_table_schema(source_table)
            pk = (schema or {}).get("primary_key") or []
            if pk:
                if options.columns:
                    pk = [c for c in pk if c in options.columns]
                if pk:
                    return list(pk)
        except Exception:
            pass
        if options.columns:
            return list(options.columns)
        return None

    def _get_target_column_limits(self, target_table, target_columns):
        """Return per-column limit dicts aligned to *target_columns* (G4)."""
        limits = [None] * len(target_columns)
        try:
            meta = self._fetch_target_column_meta(target_table)
        except Exception:
            return limits
        if not meta:
            return limits
        lower_meta = {k.lower(): v for k, v in meta.items()}
        for idx, col in enumerate(target_columns):
            info = meta.get(col) or lower_meta.get(str(col).lower())
            if info:
                limits[idx] = info
        return limits

    def _fetch_target_column_meta(self, target_table):
        """Query target catalog for char length / numeric precision per column."""
        db_type = self.target_type
        cursor = self.target_manager.conn.cursor()
        meta: dict = {}
        try:
            if db_type in ["MySQL", "MariaDB"]:
                if "." in target_table:
                    schema_name, table_only = target_table.split(".", 1)
                    where = "TABLE_SCHEMA = %s AND TABLE_NAME = %s"
                    params = [schema_name, table_only]
                else:
                    where = "TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
                    params = [target_table]
                cursor.execute(
                    "SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH, "
                    "NUMERIC_PRECISION, NUMERIC_SCALE, DATA_TYPE "
                    "FROM information_schema.COLUMNS WHERE " + where,
                    params,
                )
            elif db_type == "PostgreSQL":
                if "." in target_table:
                    schema_name, table_only = target_table.split(".", 1)
                else:
                    schema_name, table_only = "public", target_table
                cursor.execute(
                    "SELECT column_name, character_maximum_length, "
                    "numeric_precision, numeric_scale, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    [schema_name, table_only],
                )
            elif db_type == "Oracle":
                cursor.execute(
                    "SELECT column_name, char_length, data_precision, "
                    "data_scale, data_type FROM user_tab_columns "
                    "WHERE table_name = :1",
                    [target_table.upper()],
                )
            else:
                cursor.close()
                return meta

            for row in cursor.fetchall():
                name = row[0]
                char_max = row[1]
                num_precision = row[2]
                num_scale = row[3]
                data_type = (row[4] or "").lower() if len(row) > 4 else ""
                is_text = char_max is not None or any(
                    t in data_type for t in ("char", "text", "clob", "string")
                )
                meta[name] = {
                    "char_max": int(char_max) if char_max else None,
                    "num_precision": int(num_precision) if num_precision else None,
                    "num_scale": int(num_scale) if num_scale is not None else 0,
                    "is_text": is_text,
                }
        finally:
            cursor.close()
        return meta

    def reset_target_sequence(self, target_table):
        """Reset auto-increment / sequence on the target after load (G8)."""
        db_type = self.target_type
        cursor = self.target_manager.conn.cursor()
        try:
            if db_type in ["MySQL", "MariaDB"]:
                auto_col = self._find_autoincrement_column(target_table)
                if not auto_col:
                    return False
                cursor.execute(f"SELECT MAX({auto_col}) FROM {target_table}")
                max_val = cursor.fetchone()[0] or 0
                cursor.execute(
                    f"ALTER TABLE {target_table} AUTO_INCREMENT = {int(max_val) + 1}"
                )
                self.target_manager.conn.commit()
                return True
            if db_type == "PostgreSQL":
                serial_col = self._find_autoincrement_column(target_table)
                if not serial_col:
                    return False
                cursor.execute(
                    "SELECT setval(pg_get_serial_sequence(%s, %s), "
                    "COALESCE((SELECT MAX(" + serial_col + ") FROM " + target_table
                    + "), 1), true)",
                    [target_table, serial_col],
                )
                self.target_manager.conn.commit()
                return True
            if db_type == "Oracle":
                # Oracle sequences are independent objects; nothing safe to reset
                # generically without identity-column metadata.
                return False
            return False
        finally:
            cursor.close()

    def _find_autoincrement_column(self, target_table):
        """Best-effort detection of the auto-increment / serial column."""
        db_type = self.target_type
        cursor = self.target_manager.conn.cursor()
        try:
            if db_type in ["MySQL", "MariaDB"]:
                if "." in target_table:
                    schema_name, table_only = target_table.split(".", 1)
                    where = "TABLE_SCHEMA = %s AND TABLE_NAME = %s"
                    params = [schema_name, table_only]
                else:
                    where = "TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s"
                    params = [target_table]
                cursor.execute(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE "
                    + where + " AND EXTRA LIKE '%auto_increment%'",
                    params,
                )
                row = cursor.fetchone()
                return row[0] if row else None
            if db_type == "PostgreSQL":
                if "." in target_table:
                    schema_name, table_only = target_table.split(".", 1)
                else:
                    schema_name, table_only = "public", target_table
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s "
                    "AND (column_default LIKE 'nextval%%' OR is_identity = 'YES') "
                    "ORDER BY ordinal_position LIMIT 1",
                    [schema_name, table_only],
                )
                row = cursor.fetchone()
                return row[0] if row else None
            return None
        finally:
            cursor.close()

    def get_estimated_row_count(self, table_name, is_source=True):
        """
        Get estimated row count from table statistics (fast, doesn't scan table)
        Uses information_schema or system catalogs for quick estimates
        """
        manager = self.source_manager if is_source else self.target_manager
        db_type = manager.db_type
        cursor = manager.conn.cursor()

        try:
            if db_type in ["MySQL", "MariaDB"]:
                # Use information_schema.TABLES (very fast, estimated)
                cursor.execute(
                    """
                    SELECT TABLE_ROWS
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s
                """,
                    [table_name],
                )
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            elif db_type == "PostgreSQL":
                # Use pg_class statistics (very fast, estimated)
                # Handle schema.table format
                if "." in table_name:
                    schema_name, table_only = table_name.split(".", 1)
                else:
                    schema_name = "public"
                    table_only = table_name

                cursor.execute(
                    """
                    SELECT reltuples::bigint
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                """,
                    [schema_name, table_only],
                )
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            elif db_type == "Oracle":
                # Use USER_TABLES statistics (very fast, estimated)
                cursor.execute(
                    """
                    SELECT NUM_ROWS
                    FROM USER_TABLES
                    WHERE TABLE_NAME = :1
                """,
                    [table_name.upper()],
                )
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            else:
                # Fallback to actual count for unknown database types
                count = self.get_row_count(table_name, is_source)

            cursor.close()
            return int(count) if count else 0

        except Exception:
            # If statistics query fails, return 0 (will show rows without total)
            cursor.close()
            return 0

    def get_row_count(self, table_name, is_source=True):
        """Get exact row count from a table (can be slow on large tables)"""
        manager = self.source_manager if is_source else self.target_manager
        cursor = manager.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count


class ConversionValidator:
    """Validates schema conversion and data transfer"""

    @staticmethod
    def validate_schema_conversion(source_schema, converted_schema):
        """Validate that schema conversion is complete"""
        issues = []

        # Check column count
        if len(source_schema["columns"]) != len(converted_schema["columns"]):
            issues.append(
                f"Column count mismatch: source has {len(source_schema['columns'])}, converted has {len(converted_schema['columns'])}"
            )

        # Check primary key preservation
        if source_schema["primary_key"] != converted_schema["primary_key"]:
            issues.append("Primary key definition changed during conversion")

        return issues

    @staticmethod
    def validate_data_transfer(source_count, target_count):
        """Validate data transfer completeness.

        Returns None on success, or an error string on mismatch.
        If either count is None (e.g. source row-count was unavailable),
        comparison is skipped and None is returned (not a mismatch).
        """
        if source_count is None or target_count is None:
            return None
        if source_count != target_count:
            return f"Row count mismatch: source={source_count}, target={target_count}"
        return None


def _normalize_type_for_compare(type_str, db_type):
    """Map a column type to canonical form for cross-database comparison."""
    if not type_str:
        return ""
    match = re.match(
        r"(\w+(?:\s+\w+)?)", str(type_str).strip().upper()
    )
    if not match:
        return str(type_str).strip().upper()
    base_type = match.group(1)
    db = "MySQL" if db_type == "MariaDB" else db_type
    if db == "Oracle":
        canonical_map = DataTypeMapper.ORACLE_CANONICAL
    elif db == "MySQL":
        canonical_map = DataTypeMapper.MYSQL_CANONICAL
    elif db == "PostgreSQL":
        canonical_map = DataTypeMapper.POSTGRES_CANONICAL
    else:
        return base_type
    return canonical_map.get(base_type, base_type)


def _normalize_cell(value):
    """Normalize a cell value for row-by-row comparison."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return ("__binary__", len(value))
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, float):
        return round(value, 6)
    return value


def _index_columns_key(columns):
    """Normalize index columns (str or dict) to a hashable comparison key."""
    keys = []
    for col in columns or []:
        if isinstance(col, dict):
            keys.append((col.get("name"), col.get("order", "ASC")))
        else:
            keys.append((str(col), "ASC"))
    return tuple(keys)


class SchemaComparer:
    """Compare live table schemas between source and target databases."""

    @staticmethod
    def compare_tables(source_mgr, target_mgr, source_table, target_table):
        converter = SchemaConverter(source_mgr, target_mgr)
        target_reader = SchemaConverter(target_mgr, target_mgr)

        source_schema = converter.get_table_schema(source_table)
        if not source_schema:
            return {
                "match": False,
                "error": f"Source table '{source_table}' not found.",
                "issues": [],
            }

        target_schema = target_reader.get_table_schema(target_table)
        if not target_schema:
            return {
                "match": False,
                "error": f"Target table '{target_table}' not found.",
                "issues": [],
            }

        expected = converter.convert_schema(source_schema)
        issues = []

        src_cols = {c["name"]: c for c in source_schema["columns"]}
        tgt_cols = {c["name"]: c for c in target_schema["columns"]}
        exp_cols = {c["name"]: c for c in expected["columns"]}

        missing_in_target = sorted(set(src_cols) - set(tgt_cols))
        extra_in_target = sorted(set(tgt_cols) - set(src_cols))
        if missing_in_target:
            issues.append(
                f"Columns missing in target: {', '.join(missing_in_target)}"
            )
        if extra_in_target:
            issues.append(f"Extra columns in target: {', '.join(extra_in_target)}")

        for name in sorted(set(src_cols) & set(tgt_cols)):
            exp = exp_cols.get(name, src_cols[name])
            tgt = tgt_cols[name]
            exp_type = _normalize_type_for_compare(exp["type"], target_mgr.db_type)
            tgt_type = _normalize_type_for_compare(tgt["type"], target_mgr.db_type)
            if exp_type != tgt_type:
                issues.append(
                    f"Column '{name}' type mismatch: expected {exp['type']} "
                    f"(canonical {exp_type}), target has {tgt['type']} "
                    f"(canonical {tgt_type})"
                )
            if bool(exp.get("nullable", True)) != bool(tgt.get("nullable", True)):
                issues.append(
                    f"Column '{name}' nullable mismatch: expected "
                    f"{exp.get('nullable', True)}, target has {tgt.get('nullable', True)}"
                )

        src_pk = list(source_schema.get("primary_key") or [])
        tgt_pk = list(target_schema.get("primary_key") or [])
        if src_pk != tgt_pk:
            issues.append(
                f"Primary key mismatch: source={src_pk or 'none'}, "
                f"target={tgt_pk or 'none'}"
            )

        src_idx = {
            _index_columns_key(idx.get("columns")): idx.get("name")
            for idx in source_schema.get("indexes") or []
        }
        tgt_idx = {
            _index_columns_key(idx.get("columns")): idx.get("name")
            for idx in target_schema.get("indexes") or []
        }
        if set(src_idx) != set(tgt_idx):
            issues.append(
                f"Index column sets differ: source={list(src_idx.keys())}, "
                f"target={list(tgt_idx.keys())}"
            )

        return {
            "match": len(issues) == 0,
            "error": None,
            "issues": issues,
            "source_table": source_table,
            "target_table": target_table,
            "source_column_count": len(source_schema["columns"]),
            "target_column_count": len(target_schema["columns"]),
        }


class DataComparer:
    """Compare table data row-by-row between source and target databases."""

    def __init__(self, source_mgr, target_mgr):
        self.source_mgr = source_mgr
        self.target_mgr = target_mgr
        self.data_converter = DataConverter(source_mgr, target_mgr)

    def compare_table_data(
        self,
        source_table,
        target_table=None,
        options=None,
        **legacy_options,
    ):
        options = DataCompareOptions.from_source(options or {
            **legacy_options,
            "target_table": target_table,
        })
        target_table = options.target_table or source_table
        mode = options.mode
        sample_size = options.sample_size
        stop_event = options.stop_event
        batch_size = options.batch_size
        if sample_size is None:
            sample_size = get_compare_sample_size()
        if batch_size is None:
            batch_size = config.get_int(
                "database.performance", "transfer_batch_size", default=1000
            )

        converter = SchemaConverter(self.source_mgr, self.target_mgr)
        source_schema = converter.get_table_schema(source_table)
        if not source_schema:
            return {
                "match": False,
                "error": f"Source table '{source_table}' not found.",
                "mode": mode,
            }

        target_reader = SchemaConverter(self.target_mgr, self.target_mgr)
        target_schema = target_reader.get_table_schema(target_table)
        if not target_schema:
            return {
                "match": False,
                "error": f"Target table '{target_table}' not found.",
                "mode": mode,
            }

        src_names = [c["name"] for c in source_schema["columns"]]
        tgt_names = [c["name"] for c in target_schema["columns"]]
        common_cols = [c for c in src_names if c in tgt_names]
        if not common_cols:
            return {
                "match": False,
                "error": "No common columns between source and target tables.",
                "mode": mode,
            }

        order_cols = source_schema.get("primary_key") or common_cols
        order_cols = [c for c in order_cols if c in common_cols]
        if not order_cols:
            order_cols = common_cols

        source_count = self.data_converter.get_row_count(source_table, is_source=True)
        target_count = self.data_converter.get_row_count(target_table, is_source=False)

        result = {
            "match": True,
            "error": None,
            "mode": mode,
            "source_table": source_table,
            "target_table": target_table,
            "source_row_count": source_count,
            "target_row_count": target_count,
            "rows_compared": 0,
            "mismatched_rows": [],
            "row_count_match": source_count == target_count,
        }

        if source_count != target_count:
            result["match"] = False
            result["row_count_message"] = (
                f"Row count mismatch: source={source_count}, target={target_count}"
            )

        rows_to_compare = (
            min(sample_size, source_count, target_count)
            if mode == "sample"
            else min(source_count, target_count)
        )
        if rows_to_compare == 0:
            result["rows_compared"] = 0
            return result

        col_list = ", ".join(common_cols)
        order_by = ", ".join(order_cols)
        offset = 0
        compared = 0
        max_mismatches = _mod_cfg.get_int(
            "schema.conversion", "max_compare_mismatches", default=20
        )

        while compared < rows_to_compare:
            if stop_event and stop_event.is_set():
                result["stopped"] = True
                break

            chunk = min(batch_size, rows_to_compare - compared)
            source_rows = self._fetch_rows(
                self.source_mgr,
                source_table,
                col_list,
                order_by,
                chunk,
                offset,
            )
            target_rows = self._fetch_rows(
                self.target_mgr,
                target_table,
                col_list,
                order_by,
                chunk,
                offset,
            )

            for row_idx, (src_row, tgt_row) in enumerate(zip(source_rows, target_rows)):
                if stop_event and stop_event.is_set():
                    result["stopped"] = True
                    break

                converted = self.data_converter._convert_row_data(src_row)
                src_norm = tuple(_normalize_cell(v) for v in converted)
                tgt_norm = tuple(_normalize_cell(v) for v in tgt_row)
                compared += 1
                result["rows_compared"] = compared

                if src_norm != tgt_norm:
                    result["match"] = False
                    if len(result["mismatched_rows"]) < max_mismatches:
                        diffs = []
                        for col_name, s_val, t_val in zip(
                            common_cols, src_norm, tgt_norm
                        ):
                            if s_val != t_val:
                                diffs.append(
                                    {
                                        "column": col_name,
                                        "source": s_val,
                                        "target": t_val,
                                    }
                                )
                        result["mismatched_rows"].append(
                            {
                                "row_number": offset + row_idx + 1,
                                "differences": diffs,
                            }
                        )

            if stop_event and stop_event.is_set():
                break
            if len(source_rows) < chunk:
                break
            offset += chunk

        if mode == "full" and not result.get("stopped"):
            if compared < min(source_count, target_count):
                result["match"] = False
                if not result.get("row_count_message"):
                    result["row_count_message"] = (
                        f"Compared {compared} rows but expected "
                        f"{min(source_count, target_count)}"
                    )

        return result

    @staticmethod
    def _fetch_rows(manager, table, col_list, order_by, limit, offset):
        db_type = manager.db_type
        cursor = manager.conn.cursor()
        try:
            if db_type == "Oracle":
                sql = (
                    f"SELECT {col_list} FROM ("
                    f"  SELECT {col_list}, ROW_NUMBER() OVER (ORDER BY {order_by}) rn "
                    f"  FROM {table}"
                    f") WHERE rn > {offset} AND rn <= {offset + limit}"
                )
            elif db_type == "PostgreSQL":
                sql = (
                    f"SELECT {col_list} FROM {table} "
                    f"ORDER BY {order_by} LIMIT {limit} OFFSET {offset}"
                )
            else:
                sql = (
                    f"SELECT {col_list} FROM {table} "
                    f"ORDER BY {order_by} LIMIT {limit} OFFSET {offset}"
                )
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
