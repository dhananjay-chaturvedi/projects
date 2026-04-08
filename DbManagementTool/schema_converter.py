#-------------------------------------------------------------------------------
#description: Schema and data converter for the tool
#initial version: 08-APR-2026
#Author: Dhananjay Chaturvedi
#Copyright 2026 Dhananjay Chaturvedi
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#-------------------------------------------------------------------------------

"""
Universal Schema and Data Converter
Supports: Oracle, MySQL, MariaDB, PostgreSQL
Converts database schemas and transfers data between any supported databases
"""

import re
from datetime import datetime
from config_loader import config


class DataTypeMapper:
    """Universal data type mapper for all supported databases"""

    # Canonical type mappings - each database type maps to these canonical types
    # Then we convert from canonical to target database

    # Oracle to Canonical
    ORACLE_CANONICAL = {
        'VARCHAR2': 'VARCHAR',
        'NVARCHAR2': 'VARCHAR',
        'CHAR': 'CHAR',
        'NCHAR': 'CHAR',
        'NUMBER': 'NUMERIC',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'SMALLINT': 'SMALLINT',
        'FLOAT': 'FLOAT',
        'REAL': 'REAL',
        'DOUBLE PRECISION': 'DOUBLE',
        'DATE': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'CLOB': 'TEXT',
        'NCLOB': 'TEXT',
        'BLOB': 'BLOB',
        'RAW': 'BINARY',
        'LONG': 'TEXT',
        'LONG RAW': 'BLOB',
    }

    # MySQL/MariaDB to Canonical (same mapping for both)
    MYSQL_CANONICAL = {
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'TEXT': 'TEXT',
        'TINYTEXT': 'TEXT',
        'MEDIUMTEXT': 'TEXT',
        'LONGTEXT': 'TEXT',
        'INT': 'INTEGER',
        'INTEGER': 'INTEGER',
        'TINYINT': 'SMALLINT',
        'SMALLINT': 'SMALLINT',
        'MEDIUMINT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'DECIMAL': 'NUMERIC',
        'NUMERIC': 'NUMERIC',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'REAL': 'REAL',
        'DATE': 'DATE',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME',
        'YEAR': 'SMALLINT',
        'BLOB': 'BLOB',
        'TINYBLOB': 'BLOB',
        'MEDIUMBLOB': 'BLOB',
        'LONGBLOB': 'BLOB',
        'BINARY': 'BINARY',
        'VARBINARY': 'BINARY',
        'ENUM': 'VARCHAR',
        'SET': 'VARCHAR',
        'JSON': 'TEXT',
    }

    # PostgreSQL to Canonical
    POSTGRES_CANONICAL = {
        'VARCHAR': 'VARCHAR',
        'CHARACTER VARYING': 'VARCHAR',
        'CHAR': 'CHAR',
        'CHARACTER': 'CHAR',
        'TEXT': 'TEXT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'INT4': 'INTEGER',
        'SMALLINT': 'SMALLINT',
        'INT2': 'SMALLINT',
        'BIGINT': 'BIGINT',
        'INT8': 'BIGINT',
        'DECIMAL': 'NUMERIC',
        'NUMERIC': 'NUMERIC',
        'REAL': 'REAL',
        'FLOAT4': 'REAL',
        'DOUBLE PRECISION': 'DOUBLE',
        'FLOAT8': 'DOUBLE',
        'FLOAT': 'FLOAT',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIMESTAMPTZ': 'TIMESTAMP',
        'TIME': 'TIME',
        'TIMETZ': 'TIME',
        'BOOLEAN': 'BOOLEAN',
        'BOOL': 'BOOLEAN',
        'BYTEA': 'BLOB',
        'UUID': 'CHAR',
        'JSON': 'TEXT',
        'JSONB': 'TEXT',
        'XML': 'TEXT',
        'SERIAL': 'INTEGER',
        'BIGSERIAL': 'BIGINT',
        'SMALLSERIAL': 'SMALLINT',
    }

    # Canonical to Oracle
    CANONICAL_ORACLE = {
        'VARCHAR': 'VARCHAR2',
        'CHAR': 'CHAR',
        'TEXT': 'CLOB',
        'INTEGER': 'NUMBER(10)',
        'SMALLINT': 'NUMBER(5)',
        'BIGINT': 'NUMBER(19)',
        'NUMERIC': 'NUMBER',
        'FLOAT': 'FLOAT',
        'REAL': 'REAL',
        'DOUBLE': 'DOUBLE PRECISION',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'VARCHAR2(8)',
        'BOOLEAN': 'NUMBER(1)',
        'BLOB': 'BLOB',
        'BINARY': 'RAW',
    }

    # Canonical to MySQL/MariaDB
    CANONICAL_MYSQL = {
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'TEXT': 'TEXT',
        'INTEGER': 'INT',
        'SMALLINT': 'SMALLINT',
        'BIGINT': 'BIGINT',
        'NUMERIC': 'DECIMAL',
        'FLOAT': 'FLOAT',
        'REAL': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME',
        'BOOLEAN': 'TINYINT(1)',
        'BLOB': 'BLOB',
        'BINARY': 'VARBINARY',
    }

    # Canonical to PostgreSQL
    CANONICAL_POSTGRES = {
        'VARCHAR': 'VARCHAR',
        'CHAR': 'CHAR',
        'TEXT': 'TEXT',
        'INTEGER': 'INTEGER',
        'SMALLINT': 'SMALLINT',
        'BIGINT': 'BIGINT',
        'NUMERIC': 'NUMERIC',
        'FLOAT': 'REAL',
        'REAL': 'REAL',
        'DOUBLE': 'DOUBLE PRECISION',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'TIME',
        'BOOLEAN': 'BOOLEAN',
        'BLOB': 'BYTEA',
        'BINARY': 'BYTEA',
    }

    @staticmethod
    def convert_type(source_type_str, source_db, target_db):
        """
        Universal type conversion between any supported databases
        source_db and target_db: 'Oracle', 'MySQL', 'MariaDB', 'PostgreSQL'
        """
        # Convert bytes to string if needed
        if isinstance(source_type_str, bytes):
            source_type_str = source_type_str.decode('utf-8')

        # Convert to string if not already
        source_type_str = str(source_type_str)

        # Extract base type and size/precision
        match = re.match(r'(\w+(?:\s+\w+)?)\s*(?:\(([^)]+)\))?', source_type_str.upper())
        if not match:
            return source_type_str

        base_type = match.group(1)
        size_spec = match.group(2)

        # If source and target are the same, return as-is
        if source_db == target_db:
            return source_type_str

        # MySQL and MariaDB use same mappings
        if source_db == 'MariaDB':
            source_db = 'MySQL'
        if target_db == 'MariaDB':
            target_db = 'MySQL'

        # Step 1: Convert source type to canonical type
        if source_db == 'Oracle':
            canonical_map = DataTypeMapper.ORACLE_CANONICAL
        elif source_db == 'MySQL':
            canonical_map = DataTypeMapper.MYSQL_CANONICAL
        elif source_db == 'PostgreSQL':
            canonical_map = DataTypeMapper.POSTGRES_CANONICAL
        else:
            return source_type_str

        canonical_type = canonical_map.get(base_type, base_type)

        # Special handling for Oracle NUMBER type
        if source_db == 'Oracle' and base_type == 'NUMBER':
            if size_spec:
                parts = size_spec.split(',')
                if len(parts) == 2:
                    canonical_type = 'NUMERIC'
                elif len(parts) == 1:
                    precision = int(parts[0])
                    if precision <= 5:
                        canonical_type = 'SMALLINT'
                    elif precision <= 10:
                        canonical_type = 'INTEGER'
                    elif precision <= 19:
                        canonical_type = 'BIGINT'
                    else:
                        canonical_type = 'NUMERIC'
            else:
                canonical_type = 'NUMERIC'

        # Step 2: Convert canonical type to target type
        if target_db == 'Oracle':
            target_map = DataTypeMapper.CANONICAL_ORACLE
        elif target_db == 'MySQL':
            target_map = DataTypeMapper.CANONICAL_MYSQL
        elif target_db == 'PostgreSQL':
            target_map = DataTypeMapper.CANONICAL_POSTGRES
        else:
            return source_type_str

        target_type = target_map.get(canonical_type, canonical_type)

        # Step 3: Handle size specifications
        if size_spec:
            # VARCHAR, CHAR types
            if canonical_type in ['VARCHAR', 'CHAR']:
                size = int(size_spec)
                # Oracle VARCHAR2 max 4000
                if target_db == 'Oracle' and size > 4000:
                    return 'CLOB'
                # PostgreSQL VARCHAR can have size
                # MySQL VARCHAR max 65535
                if target_db == 'MySQL' and size > 65535:
                    return 'TEXT'
                return f"{target_type}({size_spec})"

            # NUMERIC/DECIMAL types
            elif canonical_type == 'NUMERIC':
                if target_db == 'Oracle':
                    return f"NUMBER({size_spec})"
                else:
                    return f"{target_type}({size_spec})"

            # BINARY types
            elif canonical_type == 'BINARY':
                if target_db == 'Oracle':
                    return f"RAW({size_spec})"
                elif target_db == 'PostgreSQL':
                    return 'BYTEA'
                else:
                    return f"{target_type}({size_spec})"

        return target_type


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
            return value.decode('utf-8')
        return str(value) if value is not None else None

    def get_table_schema(self, table_name):
        """Get table schema from source database"""
        if self.source_type == "Oracle":
            return self._get_oracle_table_schema(table_name)
        elif self.source_type in ["MySQL", "MariaDB"]:
            return self._get_mysql_table_schema(table_name)
        elif self.source_type == "PostgreSQL":
            return self._get_postgres_table_schema(table_name)
        return None

    def _get_oracle_table_schema(self, table_name):
        """Get Oracle table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        schema = {
            'table_name': table_name,
            'columns': [],
            'primary_key': [],
            'indexes': [],
            'foreign_keys': []
        }

        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, data_length, data_precision, data_scale,
                   nullable, data_default
            FROM user_tab_columns
            WHERE table_name = :1
            ORDER BY column_id
        """, [table_name.upper()])

        for row in cursor.fetchall():
            col_name, data_type, data_length, data_precision, data_scale, nullable, default = row

            # Ensure all string values are properly decoded
            col_name = self._ensure_str(col_name)
            data_type = self._ensure_str(data_type)
            nullable = self._ensure_str(nullable)

            # Build full type
            if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR', 'RAW'):
                full_type = f"{data_type}({data_length})"
            elif data_type == 'NUMBER':
                if data_precision:
                    if data_scale and data_scale > 0:
                        full_type = f"NUMBER({data_precision},{data_scale})"
                    else:
                        full_type = f"NUMBER({data_precision})"
                else:
                    full_type = "NUMBER"
            else:
                full_type = data_type

            schema['columns'].append({
                'name': col_name,
                'type': full_type,
                'nullable': nullable == 'Y',
                'default': self._ensure_str(default)
            })

        # Get primary key
        cursor.execute("""
            SELECT cols.column_name
            FROM user_constraints cons, user_cons_columns cols
            WHERE cons.constraint_name = cols.constraint_name
            AND cons.constraint_type = 'P'
            AND cons.table_name = :1
            ORDER BY cols.position
        """, [table_name.upper()])

        schema['primary_key'] = [self._ensure_str(row[0]) for row in cursor.fetchall()]

        # Get indexes
        cursor.execute("""
            SELECT index_name, column_name
            FROM user_ind_columns
            WHERE table_name = :1
            AND index_name NOT IN (
                SELECT constraint_name FROM user_constraints
                WHERE table_name = :1 AND constraint_type = 'P'
            )
            ORDER BY index_name, column_position
        """, [table_name.upper(), table_name.upper()])

        indexes = {}
        for row in cursor.fetchall():
            idx_name, col_name = row
            idx_name = self._ensure_str(idx_name)
            col_name = self._ensure_str(col_name)
            if idx_name not in indexes:
                indexes[idx_name] = []
            indexes[idx_name].append(col_name)

        schema['indexes'] = [{'name': k, 'columns': v} for k, v in indexes.items()]

        cursor.close()
        return schema

    def _get_mysql_table_schema(self, table_name):
        """Get MySQL/MariaDB table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        schema = {
            'table_name': table_name,
            'columns': [],
            'primary_key': [],
            'indexes': [],
            'foreign_keys': []
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

            schema['columns'].append({
                'name': field,
                'type': type_str,
                'nullable': null == 'YES',
                'default': self._ensure_str(default),
                'extra': self._ensure_str(extra)
            })

            if key == 'PRI':
                schema['primary_key'].append(field)

        # Get indexes
        cursor.execute(f"SHOW INDEX FROM {table_name}")

        indexes = {}
        for row in cursor.fetchall():
            table, non_unique, key_name, seq, col_name = row[0], row[1], row[2], row[3], row[4]
            key_name = self._ensure_str(key_name)
            col_name = self._ensure_str(col_name)
            if key_name != 'PRIMARY':
                if key_name not in indexes:
                    indexes[key_name] = []
                indexes[key_name].append(col_name)

        schema['indexes'] = [{'name': k, 'columns': v} for k, v in indexes.items()]

        cursor.close()
        return schema

    def _get_postgres_table_schema(self, table_name):
        """Get PostgreSQL table schema"""
        conn = self.source_manager.conn
        cursor = conn.cursor()

        # Handle schema.table format
        if '.' in table_name:
            schema_name, table_only = table_name.split('.', 1)
        else:
            schema_name = 'public'
            table_only = table_name

        schema = {
            'table_name': table_only,
            'columns': [],
            'primary_key': [],
            'indexes': [],
            'foreign_keys': []
        }

        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, [schema_name, table_only])

        for row in cursor.fetchall():
            col_name, data_type, char_length, num_precision, num_scale, nullable, default = row

            # Ensure all string values are properly decoded
            col_name = self._ensure_str(col_name)
            data_type = self._ensure_str(data_type)
            nullable = self._ensure_str(nullable)

            # Build full type
            if data_type in ('character varying', 'character', 'bit', 'bit varying'):
                if char_length:
                    full_type = f"{data_type}({char_length})"
                else:
                    full_type = data_type
            elif data_type in ('numeric', 'decimal'):
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
            is_auto_increment = default_str and 'nextval' in default_str.lower()

            schema['columns'].append({
                'name': col_name,
                'type': full_type,
                'nullable': nullable == 'YES',
                'default': default_str,
                'auto_increment': is_auto_increment
            })

        # Get primary key
        cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %s
            AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
        """, [schema_name, table_only])

        schema['primary_key'] = [self._ensure_str(row[0]) for row in cursor.fetchall()]

        # Get indexes
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = %s AND tablename = %s
        """, [schema_name, table_only])

        for row in cursor.fetchall():
            idx_name, idx_def = row
            idx_name = self._ensure_str(idx_name)
            idx_def = self._ensure_str(idx_def)
            # Skip primary key indexes
            if 'PRIMARY KEY' not in idx_def.upper():
                # Extract column names from index definition (simplified)
                schema['indexes'].append({'name': idx_name, 'columns': []})

        cursor.close()
        return schema

    def convert_schema(self, source_schema):
        """Convert schema from source to target database format"""
        converted = {
            'table_name': source_schema['table_name'],
            'columns': [],
            'primary_key': source_schema['primary_key'],
            'indexes': source_schema['indexes'],
            'foreign_keys': source_schema['foreign_keys']
        }

        for col in source_schema['columns']:
            converted_col = {
                'name': col['name'],
                'nullable': col['nullable'],
                'default': col.get('default')
            }

            # Convert data type using universal mapper
            converted_col['type'] = DataTypeMapper.convert_type(
                col['type'],
                self.source_type,
                self.target_type
            )

            # Handle AUTO_INCREMENT / SERIAL
            if col.get('extra') == 'auto_increment' or col.get('auto_increment'):
                converted_col['auto_increment'] = True

            converted['columns'].append(converted_col)

        return converted

    def generate_create_table_ddl(self, schema):
        """Generate CREATE TABLE statement for target database"""
        table_name = schema['table_name']
        columns = schema['columns']
        primary_key = schema['primary_key']

        if self.target_type in ["MySQL", "MariaDB"]:
            return self._generate_mysql_create_table(table_name, columns, primary_key)
        elif self.target_type == "Oracle":
            return self._generate_oracle_create_table(table_name, columns, primary_key)
        elif self.target_type == "PostgreSQL":
            return self._generate_postgres_create_table(table_name, columns, primary_key)

        return None

    def _generate_mysql_create_table(self, table_name, columns, primary_key):
        """Generate MySQL/MariaDB CREATE TABLE statement"""
        ddl = f"CREATE TABLE {table_name} (\n"

        col_defs = []
        for col in columns:
            col_def = f"  {col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            if col.get('default') and col['default'] not in ['NULL', None]:
                col_def += f" DEFAULT {col['default']}"
            if col.get('auto_increment'):
                col_def += " AUTO_INCREMENT"
            col_defs.append(col_def)

        if primary_key:
            pk_cols = ', '.join(primary_key)
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        ddl += ',\n'.join(col_defs)
        ddl += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        return ddl

    def _generate_oracle_create_table(self, table_name, columns, primary_key):
        """
        Generate Oracle CREATE TABLE statement.

        Note: No semicolons! cx_Oracle driver doesn't accept semicolons in cursor.execute().
        Semicolons are only for SQL*Plus command-line tool.
        """
        ddl = f"CREATE TABLE {table_name} (\n"

        col_defs = []
        has_auto_increment = False

        for col in columns:
            col_def = f"  {col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            if col.get('default') and col['default'] not in ['NULL', None]:
                col_def += f" DEFAULT {col['default']}"
            if col.get('auto_increment'):
                has_auto_increment = True
            col_defs.append(col_def)

        if primary_key:
            pk_cols = ', '.join(primary_key)
            col_defs.append(f"  CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})")

        ddl += ',\n'.join(col_defs)
        ddl += "\n)"  # No semicolon for cx_Oracle!

        # Add sequence and trigger for auto_increment columns
        # Note: In cx_Oracle, semicolons should NOT be used with cursor.execute()
        if has_auto_increment and primary_key:
            pk_col = primary_key[0]
            ddl += f"\n\nCREATE SEQUENCE {table_name}_seq START WITH 1 INCREMENT BY 1"
            ddl += f"\n\nCREATE OR REPLACE TRIGGER {table_name}_trg\n"
            ddl += f"BEFORE INSERT ON {table_name}\n"
            ddl += f"FOR EACH ROW\n"
            ddl += f"BEGIN\n"
            ddl += f"  IF :new.{pk_col} IS NULL THEN\n"
            ddl += f"    SELECT {table_name}_seq.NEXTVAL INTO :new.{pk_col} FROM dual;\n"
            ddl += f"  END IF;\n"
            ddl += f"END"  # No semicolon!

        return ddl

    def _generate_postgres_create_table(self, table_name, columns, primary_key):
        """Generate PostgreSQL CREATE TABLE statement"""
        ddl = f"CREATE TABLE {table_name} (\n"

        col_defs = []
        for col in columns:
            # Convert auto_increment to SERIAL
            if col.get('auto_increment'):
                if 'BIGINT' in col['type'].upper():
                    col_type = 'BIGSERIAL'
                elif 'SMALLINT' in col['type'].upper():
                    col_type = 'SMALLSERIAL'
                else:
                    col_type = 'SERIAL'
                col_def = f"  {col['name']} {col_type}"
            else:
                col_def = f"  {col['name']} {col['type']}"

            if not col['nullable']:
                col_def += " NOT NULL"
            if col.get('default') and col['default'] not in ['NULL', None] and not col.get('auto_increment'):
                col_def += f" DEFAULT {col['default']}"
            col_defs.append(col_def)

        if primary_key:
            pk_cols = ', '.join(primary_key)
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        ddl += ',\n'.join(col_defs)
        ddl += "\n)"  # No semicolon - cx_Oracle doesn't accept them in cursor.execute()

        return ddl

    def generate_indexes_ddl(self, schema):
        """Generate CREATE INDEX statements"""
        ddls = []
        table_name = schema['table_name']

        for idx in schema['indexes']:
            idx_name = idx['name']
            if idx['columns']:
                cols = ', '.join(idx['columns'])
                ddl = f"CREATE INDEX {idx_name} ON {table_name} ({cols});"
                ddls.append(ddl)

        return ddls


class DataConverter:
    """Universal data converter for all supported databases"""

    def __init__(self, source_db_manager, target_db_manager):
        self.source_manager = source_db_manager
        self.target_manager = target_db_manager
        self.source_type = source_db_manager.db_type
        self.target_type = target_db_manager.db_type

    def transfer_table_data(self, source_table, target_table, batch_size=None, progress_callback=None, stop_event=None):
        """
        Transfer data from source table to target table

        Args:
            source_table: Source table name
            target_table: Target table name
            batch_size: Number of rows to insert per batch (None = use config default)
            progress_callback: Optional callback function(rows_transferred, total_rows)
                             Called after each batch is committed
            stop_event: Optional threading.Event to check for stop signal
        """
        # Use configured batch size if not provided
        if batch_size is None:
            batch_size = config.get_int('database.performance', 'transfer_batch_size', default=1000)

        source_conn = self.source_manager.conn
        target_conn = self.target_manager.conn

        # Get estimated row count for progress reporting (fast, uses table statistics)
        total_rows = None
        min_rows_for_estimation = config.get_int('database.performance', 'batch_min_rows', default=10)
        if progress_callback:
            try:
                total_rows = self.get_estimated_row_count(source_table, is_source=True)
                # If estimate is 0 or very small, don't show it (might be inaccurate)
                if total_rows < min_rows_for_estimation:
                    total_rows = None
            except:
                total_rows = None  # If estimation fails, continue without it

        # Get source data with appropriate cursor
        if self.source_type in ["MySQL", "MariaDB"]:
            source_cursor = source_conn.cursor(buffered=True)
        else:
            source_cursor = source_conn.cursor()

        source_cursor.execute(f"SELECT * FROM {source_table}")

        # Get column names
        columns = [desc[0] for desc in source_cursor.description]
        col_names = ', '.join(columns)

        # Generate placeholders based on target database
        if self.target_type == "Oracle":
            placeholders = ', '.join([f":{i+1}" for i in range(len(columns))])
        elif self.target_type == "PostgreSQL":
            placeholders = ', '.join(['%s'] * len(columns))
        else:  # MySQL/MariaDB
            placeholders = ', '.join(['%s'] * len(columns))

        insert_sql = f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})"

        target_cursor = target_conn.cursor()

        rows_transferred = 0
        batch = []

        for row in source_cursor:
            # Check if stop was requested
            if stop_event and stop_event.is_set():
                # Commit any remaining batch before stopping
                if batch:
                    target_cursor.executemany(insert_sql, batch)
                    target_conn.commit()
                    rows_transferred += len(batch)
                break

            # Convert data types if necessary
            converted_row = self._convert_row_data(row)
            batch.append(converted_row)

            if len(batch) >= batch_size:
                target_cursor.executemany(insert_sql, batch)
                target_conn.commit()
                rows_transferred += len(batch)
                batch = []

                # Report progress after each batch
                if progress_callback:
                    progress_callback(rows_transferred, total_rows)

                # Check stop event after each batch commit
                if stop_event and stop_event.is_set():
                    break

        # Insert remaining rows
        if batch:
            target_cursor.executemany(insert_sql, batch)
            target_conn.commit()
            rows_transferred += len(batch)

            # Report final progress
            if progress_callback:
                progress_callback(rows_transferred, total_rows)

        source_cursor.close()
        target_cursor.close()

        return rows_transferred

    def _convert_row_data(self, row):
        """Convert row data types for compatibility"""
        converted = []

        for value in row:
            if value is None:
                converted.append(None)
            elif isinstance(value, (bytearray, bytes)):
                # Keep binary data as is
                converted.append(value)
            elif isinstance(value, datetime):
                # Date/time handling
                converted.append(value)
            elif isinstance(value, bool) and self.target_type == "Oracle":
                # Convert boolean to 0/1 for Oracle
                converted.append(1 if value else 0)
            else:
                converted.append(value)

        return tuple(converted)

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
                cursor.execute("""
                    SELECT TABLE_ROWS
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = %s
                """, [table_name])
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            elif db_type == "PostgreSQL":
                # Use pg_class statistics (very fast, estimated)
                # Handle schema.table format
                if '.' in table_name:
                    schema_name, table_only = table_name.split('.', 1)
                else:
                    schema_name = 'public'
                    table_only = table_name

                cursor.execute("""
                    SELECT reltuples::bigint
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                """, [schema_name, table_only])
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            elif db_type == "Oracle":
                # Use USER_TABLES statistics (very fast, estimated)
                cursor.execute("""
                    SELECT NUM_ROWS
                    FROM USER_TABLES
                    WHERE TABLE_NAME = :1
                """, [table_name.upper()])
                result = cursor.fetchone()
                count = result[0] if result and result[0] else 0

            else:
                # Fallback to actual count for unknown database types
                count = self.get_row_count(table_name, is_source)

            cursor.close()
            return int(count) if count else 0

        except Exception as e:
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
        if len(source_schema['columns']) != len(converted_schema['columns']):
            issues.append(f"Column count mismatch: source has {len(source_schema['columns'])}, converted has {len(converted_schema['columns'])}")

        # Check primary key preservation
        if source_schema['primary_key'] != converted_schema['primary_key']:
            issues.append("Primary key definition changed during conversion")

        return issues

    @staticmethod
    def validate_data_transfer(source_count, target_count):
        """Validate data transfer completeness"""
        if source_count != target_count:
            return f"Row count mismatch: source={source_count}, target={target_count}"
        return None
