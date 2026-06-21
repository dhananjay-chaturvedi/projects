"""
PostgreSQL Database Connection Module
"""

import logging
import sys

import psycopg2
from psycopg2 import Error

from common.autocommit import default_autocommit, set_autocommit
from common.config_loader import console_debug, get_db_port

from common.drivers.connection_options import DriverConnectionParams
from common.drivers.ssl_support import SslParams, postgres_ssl_connect_kwargs

# logging.basicConfig(filename="conPostgres.log", level=logging.INFO)


def log(message):
    """Driver INFO trace — see :func:`common.drivers.conMariadb.log`."""
    logging.info(message)
    console_debug(message)
    return True


def logError(message):
    logging.error(message)
    print(message, file=sys.stderr)
    return True


def connectPostgres(params=None, **kwargs):
    """
    Connect to PostgreSQL database.

    Note: No statement timeout is set, allowing long-running operations
    like schema conversions and large data transfers to complete without timeout.
    The connect_timeout parameter (not set here) would only apply to initial
    connection establishment, not to queries.

    SSL options are supplied via the ``ssl`` parameter (a
    :class:`common.drivers.ssl_support.SslParams`); for backward compatibility
    they are also accepted as loose ``ssl_mode``/``ssl_ca``/... keyword
    arguments and reconstructed from there.
    """
    params = DriverConnectionParams.from_call(params, kwargs)
    # Use configured port if not provided
    database = params.database
    host = params.host
    user = params.user
    password = params.password
    port = params.port
    ssl = params.ssl or SslParams()
    if port is None:
        port = get_db_port("postgresql")

    # connect_timeout applies only to initial connection establishment, not queries.
    connect_timeout = kwargs.get("connect_timeout")
    pg_timeout_kw = {}
    if connect_timeout and int(connect_timeout) > 0:
        pg_timeout_kw["connect_timeout"] = int(connect_timeout)

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            **postgres_ssl_connect_kwargs(
                ssl.ssl_mode, ssl.ssl_ca, ssl.ssl_cert, ssl.ssl_key
            ),
            **pg_timeout_kw,
        )
        charset = kwargs.get("charset")
        if charset:
            from common.charset_util import postgres_encoding_name

            conn.set_client_encoding(postgres_encoding_name(charset))
        set_autocommit(conn, "PostgreSQL", default_autocommit())
        log(f"Connected to PostgreSQL: {database}@{host}:{port}")
        return conn
    except Error as e:
        logError(f"Failed to connect to PostgreSQL: {e}")
        return None


def disconnectPostgres(conn):
    """Disconnect from PostgreSQL database"""
    if conn:
        conn.close()
        log("Disconnected from PostgreSQL")
    return True


def pingPostgres(conn):
    """Return True when the PostgreSQL connection is alive."""
    if not conn or getattr(conn, "closed", 0):
        return False
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True
    except Exception:
        return False
    finally:
        if cursor:
            cursor.close()


def reconnectPostgres(conn, params=None, **kwargs):
    """Reconnect to PostgreSQL using the supplied connection parameters."""
    try:
        disconnectPostgres(conn)
    except Exception:
        pass
    return connectPostgres(
        params=DriverConnectionParams.from_call(params, kwargs),
    )


def getPostgresVersion(conn):
    """Get PostgreSQL database version"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.close()
        # Extract just the version number
        version_parts = version.split()
        if len(version_parts) >= 2:
            return version_parts[1]
        return version
    except Error as e:
        logError(f"Failed to get PostgreSQL version: {e}")
        return None


def isSuperuser(conn):
    """Check if user has superuser privileges"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT usesuper FROM pg_user WHERE usename = current_user")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else False
    except Error:
        return False


def get_cursor(conn):
    """Get a cursor"""
    return conn.cursor()


# Table and Schema Operations
def getPostgresTables(conn):
    """Get all tables in current database"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename
        """)
        tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return tables
    except Error as e:
        logError(f"Failed to get tables: {e}")
        return []


def getPostgresViews(conn):
    """Get all views in current database"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT schemaname, viewname
            FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, viewname
        """)
        views = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return views
    except Error as e:
        logError(f"Failed to get views: {e}")
        return []


def getPostgresFunctions(conn):
    """Get all user-defined functions"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT n.nspname, p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND p.prokind = 'f'
            ORDER BY n.nspname, p.proname
        """)
        functions = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return functions
    except Error as e:
        logError(f"Failed to get functions: {e}")
        return []


def getPostgresProcedures(conn):
    """Get all stored procedures"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT n.nspname, p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND p.prokind = 'p'
            ORDER BY n.nspname, p.proname
        """)
        procedures = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return procedures
    except Error:
        # Older PostgreSQL versions don't have prokind
        try:
            cursor = get_cursor(conn)
            cursor.execute("""
                SELECT n.nspname, p.proname
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                ORDER BY n.nspname, p.proname
            """)
            procedures = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
            cursor.close()
            return procedures
        except Exception:
            return []


def getPostgresTriggers(conn):
    """Get all triggers"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT trigger_schema, trigger_name
            FROM information_schema.triggers
            WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY trigger_schema, trigger_name
        """)
        triggers = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except Error as e:
        logError(f"Failed to get triggers: {e}")
        return []


def getPostgresIndexes(conn):
    """Get all indexes"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT schemaname, indexname
            FROM pg_indexes
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, indexname
        """)
        indexes = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except Error as e:
        logError(f"Failed to get indexes: {e}")
        return []


def getPostgresSequences(conn):
    """Get all sequences"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT schemaname, sequencename
            FROM pg_sequences
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, sequencename
        """)
        sequences = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        cursor.close()
        return sequences
    except Error as e:
        logError(f"Failed to get sequences: {e}")
        return []


def getPostgresConstraints(conn):
    """Get all constraints"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT table_schema, table_name, constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, constraint_name
        """)
        constraints = [
            f"{row[0]}.{row[1]}.{row[2]} ({row[3]})" for row in cursor.fetchall()
        ]
        cursor.close()
        return constraints
    except Error as e:
        logError(f"Failed to get constraints: {e}")
        return []


def getPostgresSchemas(conn):
    """Get all schemas"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return schemas
    except Error as e:
        logError(f"Failed to get schemas: {e}")
        return []


def getPostgresDatabases(conn):
    """Get all databases"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT datname
            FROM pg_database
            WHERE datistemplate = false
            ORDER BY datname
        """)
        databases = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return databases
    except Error as e:
        logError(f"Failed to get databases: {e}")
        return []


def getPostgresUsers(conn):
    """Get all database users/roles"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT usename
            FROM pg_user
            ORDER BY usename
        """)
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return users
    except Error as e:
        logError(f"Failed to get users: {e}")
        return []


def getPostgresRoles(conn):
    """Get all roles"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT rolname
            FROM pg_roles
            ORDER BY rolname
        """)
        roles = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return roles
    except Error as e:
        logError(f"Failed to get roles: {e}")
        return []


def getPostgresTablespaces(conn):
    """Get all tablespaces"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT spcname
            FROM pg_tablespace
            ORDER BY spcname
        """)
        tablespaces = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tablespaces
    except Error as e:
        logError(f"Failed to get tablespaces: {e}")
        return []


def getPostgresExtensions(conn):
    """Get installed extensions"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT extname, extversion
            FROM pg_extension
            ORDER BY extname
        """)
        extensions = [f"{row[0]} (v{row[1]})" for row in cursor.fetchall()]
        cursor.close()
        return extensions
    except Error as e:
        logError(f"Failed to get extensions: {e}")
        return []


def getPostgresActivity(conn):
    """Get current activity/processes"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT pid, usename, application_name, client_addr, state
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
            ORDER BY pid
        """)
        activities = [
            f"PID:{row[0]} User:{row[1]} App:{row[2]} Client:{row[3]} State:{row[4]}"
            for row in cursor.fetchall()
        ]
        cursor.close()
        return activities
    except Error as e:
        logError(f"Failed to get activity: {e}")
        return []


def getPostgresTableSchema(conn, table_name):
    """
    Get detailed schema for a table including column names, types, and constraints

    Args:
        conn: PostgreSQL connection
        table_name: Table name (can be schema.table or just table)

    Returns:
        List of dicts with column information
    """
    try:
        # Check if connection is valid
        if not conn or conn.closed:
            logError(f"PostgreSQL connection not available for table {table_name}")
            return []

        cursor = get_cursor(conn)

        # Parse table name to handle schema.table format
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            schema = "public"
            table = table_name

        # Get column information
        cursor.execute(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                (
                    SELECT COUNT(*)
                    FROM information_schema.key_column_usage k
                    JOIN information_schema.table_constraints t
                        ON k.constraint_name = t.constraint_name
                        AND k.table_schema = t.table_schema
                        AND k.table_name = t.table_name
                    WHERE k.table_schema = c.table_schema
                        AND k.table_name = c.table_name
                        AND k.column_name = c.column_name
                        AND t.constraint_type = 'PRIMARY KEY'
                ) as is_primary_key
            FROM information_schema.columns c
            WHERE c.table_schema = %s
              AND c.table_name = %s
            ORDER BY c.ordinal_position
        """,
            (schema, table),
        )

        columns = []
        for row in cursor.fetchall():
            col_name = row[0]
            data_type = row[1]
            max_length = row[2]
            precision = row[3]
            scale = row[4]
            is_nullable = row[5] == "YES"
            default_val = row[6]
            is_pk = row[7] > 0

            # Format type with size/precision
            if (
                data_type in ("character varying", "character", "varchar", "char")
                and max_length
            ):
                type_str = f"{data_type}({max_length})"
            elif data_type == "numeric" and precision is not None:
                if scale is not None and scale > 0:
                    type_str = f"numeric({precision},{scale})"
                else:
                    type_str = f"numeric({precision})"
            else:
                type_str = data_type

            # Add primary key info
            if is_pk:
                type_str += " PRIMARY KEY"

            columns.append(
                {
                    "name": col_name,
                    "type": type_str,
                    "nullable": is_nullable,
                    "default": default_val,
                }
            )

        cursor.close()
        return columns

    except Error as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []
