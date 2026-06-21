"""
Microsoft SQL Server / Azure SQL connection module (pymssql).

See: https://pymssql.readthedocs.io/en/stable/
"""

from __future__ import annotations

import logging
import sys

from common.autocommit import default_autocommit, set_autocommit
from common.config_loader import config, get_db_port, console_debug

from common.drivers.connection_options import DriverConnectionParams
from common.drivers.ssl_support import SslParams, sqlserver_ssl_connect_kwargs

try:
    import pymssql
except ImportError:  # pragma: no cover - optional driver
    pymssql = None  # type: ignore

SqlServerError = pymssql.Error if pymssql else Exception


def log(message: str) -> bool:
    """Driver INFO trace — see :func:`common.drivers.conMariadb.log`."""
    logging.info(message)
    console_debug(message)
    return True


def logError(message: str) -> bool:
    logging.error(message)
    print(message, file=sys.stderr)
    return True


def connectSqlServer(params=None, **kwargs):
    if pymssql is None:
        logError("pymssql is not installed. Run: pip install pymssql")
        return None
    params = DriverConnectionParams.from_call(params, kwargs)
    database = params.database
    host = params.host
    user = params.user
    password = params.password
    port = params.port
    ssl = params.ssl or SslParams()
    if port is None:
        port = get_db_port("sqlserver")
    try:
        ssl_kwargs = sqlserver_ssl_connect_kwargs(
            ssl_mode=ssl.ssl_mode, ssl_ca=ssl.ssl_ca
        )
        login_timeout = max(
            1,
            int(config.get_float("database.connection", "connection_timeout", default=30.0)),
        )
        conn = pymssql.connect(
            server=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            login_timeout=login_timeout,
            **ssl_kwargs,
        )
        set_autocommit(conn, "SQLServer", default_autocommit())
        log(f"Connected to SQL Server: {database}@{host}:{port}")
        return conn
    except SqlServerError as e:
        logError(f"Failed to connect to SQL Server: {e}")
        return None


def disconnectSqlServer(conn):
    if conn:
        conn.close()
        log("Disconnected from SQL Server")
    return True


def pingSqlServer(conn):
    if not conn:
        return False
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return True
    except Exception:
        return False
    finally:
        if cur:
            cur.close()


def reconnectSqlServer(conn, params=None, **kwargs):
    try:
        disconnectSqlServer(conn)
    except Exception:
        pass
    return connectSqlServer(
        params=DriverConnectionParams.from_call(params, kwargs),
    )


def getSqlServerVersion(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT @@VERSION")
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except SqlServerError as e:
        logError(f"Failed to get SQL Server version: {e}")
        return None


def isRoot(conn):
    """True when login has sysadmin server role."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT IS_SRVROLEMEMBER('sysadmin')")
        row = cur.fetchone()
        cur.close()
        return bool(row and row[0] == 1)
    except SqlServerError:
        return False


def getCurrentDatabase(conn):
    try:
        cur = conn.cursor()
        cur.execute("SELECT DB_NAME()")
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None
    except SqlServerError:
        return None


def _fetch_names(conn, sql: str) -> list[str]:
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = [row[0] for row in cur.fetchall()]
        cur.close()
        return rows
    except SqlServerError as e:
        logError(f"Query failed: {e}")
        return []


def getSqlServerTables(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
    )


def getSqlServerViews(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)
        FROM INFORMATION_SCHEMA.VIEWS
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """,
    )


def getSqlServerProcedures(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(ROUTINE_SCHEMA) + '.' + QUOTENAME(ROUTINE_NAME)
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'PROCEDURE'
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """,
    )


def getSqlServerFunctions(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(ROUTINE_SCHEMA) + '.' + QUOTENAME(ROUTINE_NAME)
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_TYPE = 'FUNCTION'
        ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
        """,
    )


def getSqlServerTriggers(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(OBJECT_SCHEMA_NAME(parent_id)) + '.' + QUOTENAME(name)
        FROM sys.triggers
        WHERE parent_class = 1
        ORDER BY 1
        """,
    )


def getSqlServerIndexes(conn):
    return _fetch_names(
        conn,
        """
        SELECT QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + '.' + QUOTENAME(name)
        FROM sys.indexes
        WHERE index_id > 0 AND name IS NOT NULL
        ORDER BY 1
        """,
    )


def getSqlServerSchemas(conn):
    return _fetch_names(
        conn,
        "SELECT name FROM sys.schemas ORDER BY name",
    )


def getSqlServerUsers(conn):
    return _fetch_names(
        conn,
        "SELECT name FROM sys.database_principals WHERE type IN ('S','U','G') ORDER BY name",
    )


def getSqlServerTableSchema(conn, table_name):
    try:
        if not conn:
            return []
        if "." in table_name:
            schema, table = table_name.replace("[", "").replace("]", "").split(".", 1)
        else:
            schema, table = "dbo", table_name

        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (schema, table),
        )
        columns = []
        for row in cur.fetchall():
            col_name, data_type, char_len, prec, scale, nullable, default = row
            if data_type in ("varchar", "nvarchar", "char", "nchar") and char_len:
                type_str = f"{data_type}({char_len})"
            elif data_type in ("decimal", "numeric") and prec is not None:
                type_str = f"{data_type}({prec},{scale or 0})"
            else:
                type_str = data_type
            columns.append(
                {
                    "name": col_name,
                    "type": type_str,
                    "nullable": nullable == "YES",
                    "default": str(default).strip() if default else None,
                }
            )
        cur.close()
        return columns
    except SqlServerError as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []
