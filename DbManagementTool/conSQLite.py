"""
SQLite Database Connection Module
Example of adding a new database type to the tool
"""

import sqlite3
import sys
import logging
import os
from config_loader import console_print

logging.basicConfig(filename='conSQLite.log', level=logging.INFO)

def log(message):
    logging.info(message)
    console_print(message)
    return True

def logError(message):
    logging.error(message)
    print(message, file=sys.stderr)
    return True

def connectSQLite(database, **kwargs):
    """
    Connect to SQLite database file

    Args:
        database: Path to SQLite database file
        **kwargs: Additional parameters (ignored for SQLite)
    """
    try:
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(database)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

        conn = sqlite3.connect(database)
        log(f"Connected to SQLite: {database}")
        return conn
    except Exception as e:
        logError(f"Failed to connect to SQLite: {e}")
        return None

def disconnectSQLite(conn):
    """Disconnect from SQLite database"""
    if conn:
        conn.close()
        log("Disconnected from SQLite")
    return True

def getSQLiteVersion(conn):
    """Get SQLite version"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]
        cursor.close()
        return f"SQLite {version}"
    except Exception as e:
        logError(f"Failed to get SQLite version: {e}")
        return None

def isRoot(conn):
    """Check if user has admin privileges (always True for SQLite)"""
    # SQLite doesn't have user-based permissions
    return True

def getCurrentDatabase(conn):
    """Get current database name"""
    # SQLite connections are to a single file
    return "main"

def getSQLiteTables(conn):
    """Get all tables in SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        log(f"Found {len(tables)} tables in SQLite database")
        return tables
    except Exception as e:
        logError(f"Failed to get tables: {e}")
        return []

def getSQLiteViews(conn):
    """Get all views in SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='view'
            ORDER BY name
        """)
        views = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return views
    except Exception as e:
        logError(f"Failed to get views: {e}")
        return []

def getSQLiteIndexes(conn):
    """Get all indexes in SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='index'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        indexes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except Exception as e:
        logError(f"Failed to get indexes: {e}")
        return []

def getSQLiteTriggers(conn):
    """Get all triggers in SQLite database"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='trigger'
            ORDER BY name
        """)
        triggers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except Exception as e:
        logError(f"Failed to get triggers: {e}")
        return []

def getSQLiteTableInfo(conn, table_name):
    """Get table structure information"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        cursor.close()
        return columns
    except Exception as e:
        logError(f"Failed to get table info for {table_name}: {e}")
        return []

def getSQLiteSchemas(conn):
    """Get all attached database schemas"""
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA database_list")
        schemas = [row[1] for row in cursor.fetchall()]
        cursor.close()
        return schemas
    except Exception as e:
        logError(f"Failed to get schemas: {e}")
        return []

def getSQLiteTableSchema(conn, table_name):
    """
    Get detailed schema for a table including column names, types, and constraints

    Args:
        conn: SQLite connection
        table_name: Table name

    Returns:
        List of dicts with column information
    """
    try:
        # Check if connection is valid
        if not conn:
            logError(f"SQLite connection not available for table {table_name}")
            return []

        cursor = conn.cursor()
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        cursor.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()

        columns = []
        for row in rows:
            cid, col_name, col_type, notnull, default_val, is_pk = row

            # Format type with PRIMARY KEY info
            type_str = col_type if col_type else "TEXT"
            if is_pk:
                type_str += " PRIMARY KEY"

            columns.append({
                'name': col_name,
                'type': type_str,
                'nullable': not bool(notnull),
                'default': default_val
            })

        cursor.close()
        return columns

    except Exception as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []
