"""
MariaDB Database Connection Module
MariaDB is MySQL-compatible but has some differences
"""

import mysql.connector
from mysql.connector import Error
import logging
from config_loader import config, get_db_port, console_print

logging.basicConfig(filename='conMariadb.log', level=logging.INFO)

def log(message):
    logging.info(message)
    console_print(message)
    return True

def logError(message):
    logging.error(message)
    print(message, file=sys.stderr)
    return True

def connectMariadb(database, host, user, password, port=None):
    """Connect to MariaDB database"""
    # Use configured port if not provided
    if port is None:
        port = get_db_port('mariadb')

    # Get autocommit default from config
    autocommit_default = config.get_bool('database.connection', 'default_autocommit', default=False)

    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=autocommit_default,
            consume_results=True
        )

        # Verify database selection
        cursor = conn.cursor()
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        cursor.close()

        if current_db:
            log(f"Connected to MariaDB: {current_db}@{host}:{port}")
        else:
            log(f"Connected to MariaDB server at {host}:{port} but no database selected")

        return conn
    except Error as e:
        logError(f"Failed to connect to MariaDB: {e}")
        return None

def disconnectMariadb(conn):
    """Disconnect from MariaDB database"""
    if conn:
        conn.close()
        log("Disconnected from MariaDB")
    return True

def getMariadbVersion(conn):
    """Get MariaDB database version"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()
        return version
    except Error as e:
        logError(f"Failed to get MariaDB version: {e}")
        return None

def isRoot(conn):
    """Check if user has root/admin privileges"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER()")
        current_user = cursor.fetchone()[0]

        # Check if user is root or has SUPER privilege
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = cursor.fetchall()
        cursor.close()

        for grant in grants:
            grant_text = grant[0].upper()
            if 'ALL PRIVILEGES' in grant_text or 'SUPER' in grant_text or 'root@' in current_user:
                return True
        return False
    except Error:
        return False

def get_cursor(conn):
    """Get a buffered cursor to avoid 'unread result found' errors"""
    return conn.cursor(buffered=True)

def decode_value(value):
    """Decode bytearray/bytes to string"""
    if isinstance(value, bytearray):
        return value.decode('utf-8')
    elif isinstance(value, bytes):
        return value.decode('utf-8')
    return value

def getCurrentDatabase(conn):
    """Get the currently selected database"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT DATABASE()")
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result and result[0] else None
    except Error as e:
        logError(f"Failed to get current database: {e}")
        return None

def selectDatabase(conn, database):
    """Select/Use a specific database"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE {database}")
        cursor.close()
        log(f"Selected database: {database}")
        return True
    except Error as e:
        logError(f"Failed to select database {database}: {e}")
        return False

# Table and Schema Operations
def getMariadbTables(conn):
    """Get all tables in current database"""
    try:
        # First check if a database is selected
        current_db = getCurrentDatabase(conn)
        if not current_db:
            error_msg = "No database selected. Please select a database first."
            logError(error_msg)
            print(f"ERROR: {error_msg}", file=sys.stderr)
            return []

        log(f"Fetching tables from database: {current_db}")
        console_print(f"MariaDB: Fetching tables from database: {current_db}")

        cursor = get_cursor(conn)
        cursor.execute("SHOW TABLES")
        rows = cursor.fetchall()
        tables = [decode_value(row[0]) for row in rows]
        cursor.close()

        log(f"Found {len(tables)} tables in {current_db}")
        console_print(f"MariaDB: Found {len(tables)} tables in {current_db}")

        if tables and len(tables) <= 20:
            console_print(f"MariaDB: Tables: {', '.join(tables)}")

        return tables
    except Error as e:
        error_msg = f"Failed to get tables: {e}"
        logError(error_msg)
        print(f"ERROR: {error_msg}", file=sys.stderr)
        import traceback
        traceback_str = traceback.format_exc()
        logError(traceback_str)
        print(traceback_str, file=sys.stderr)
        return []

def getMariadbViews(conn):
    """Get all views in current database"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT TABLE_NAME
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
        """)
        views = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return views
    except Error as e:
        logError(f"Failed to get views: {e}")
        return []

def getMariadbProcedures(conn):
    """Get all stored procedures"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT ROUTINE_NAME
            FROM information_schema.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            AND ROUTINE_SCHEMA = DATABASE()
            ORDER BY ROUTINE_NAME
        """)
        procedures = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return procedures
    except Error as e:
        logError(f"Failed to get procedures: {e}")
        return []

def getMariadbFunctions(conn):
    """Get all user-defined functions"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT ROUTINE_NAME
            FROM information_schema.ROUTINES
            WHERE ROUTINE_TYPE = 'FUNCTION'
            AND ROUTINE_SCHEMA = DATABASE()
            ORDER BY ROUTINE_NAME
        """)
        functions = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return functions
    except Error as e:
        logError(f"Failed to get functions: {e}")
        return []

def getMariadbTriggers(conn):
    """Get all triggers"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT TRIGGER_NAME
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
            ORDER BY TRIGGER_NAME
        """)
        triggers = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except Error as e:
        logError(f"Failed to get triggers: {e}")
        return []

def getMariadbAllIndexes(conn):
    """Get all indexes across all tables"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT DISTINCT TABLE_NAME, INDEX_NAME
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
            AND INDEX_NAME != 'PRIMARY'
            ORDER BY TABLE_NAME, INDEX_NAME
        """)
        indexes = [f"{decode_value(row[0])}.{decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except Error as e:
        logError(f"Failed to get indexes: {e}")
        return []

def getMariadbConstraints(conn):
    """Get all constraints (foreign keys, unique, etc.)"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE TABLE_SCHEMA = DATABASE()
            AND CONSTRAINT_TYPE != 'PRIMARY KEY'
            ORDER BY TABLE_NAME, CONSTRAINT_NAME
        """)
        constraints = [f"{decode_value(row[0])}.{decode_value(row[1])} ({decode_value(row[2])})"
                      for row in cursor.fetchall()]
        cursor.close()
        return constraints
    except Error as e:
        logError(f"Failed to get constraints: {e}")
        return []

def getMariadbEvents(conn):
    """Get all scheduled events"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT EVENT_NAME
            FROM information_schema.EVENTS
            WHERE EVENT_SCHEMA = DATABASE()
            ORDER BY EVENT_NAME
        """)
        events = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return events
    except Error as e:
        logError(f"Failed to get events: {e}")
        return []

def getMariadbDatabases(conn):
    """Get all databases (if user has permission)"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW DATABASES")
        databases = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return databases
    except Error as e:
        logError(f"Failed to get databases: {e}")
        return []

def getMariadbUsers(conn):
    """Get all database users (requires privileges)"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT User, Host FROM mysql.user ORDER BY User")
        users = [f"{decode_value(row[0])}@{decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return users
    except Error as e:
        logError(f"Failed to get users: {e}")
        return []

def getMariadbEngines(conn):
    """Get available storage engines"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW ENGINES")
        engines = [f"{decode_value(row[0])} ({decode_value(row[1])})" for row in cursor.fetchall()]
        cursor.close()
        return engines
    except Error as e:
        logError(f"Failed to get engines: {e}")
        return []

def getMariadbCharsets(conn):
    """Get available character sets"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW CHARACTER SET")
        charsets = [f"{decode_value(row[0])} - {decode_value(row[2])}" for row in cursor.fetchall()]
        cursor.close()
        return charsets
    except Error as e:
        logError(f"Failed to get charsets: {e}")
        return []

def getMariadbProcessList(conn):
    """Get current process list"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW PROCESSLIST")
        processes = [f"ID:{row[0]} User:{decode_value(row[1])} DB:{decode_value(row[3]) if row[3] else 'None'} Command:{decode_value(row[4])}"
                     for row in cursor.fetchall()]
        cursor.close()
        return processes
    except Error as e:
        logError(f"Failed to get process list: {e}")
        return []

def getMariadbSequences(conn):
    """Get all sequences (MariaDB supports sequences from 10.3+)"""
    try:
        cursor = get_cursor(conn)
        cursor.execute("""
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_TYPE = 'SEQUENCE'
            ORDER BY TABLE_NAME
        """)
        sequences = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return sequences
    except Error as e:
        # Sequences might not be supported in older versions
        return []

def getMariadbTableSchema(conn, table_name, database=None):  # Get table schema (columns)
    """
    Get detailed schema for a table including column names, types, and constraints

    Args:
        conn: MariaDB connection
        table_name: Table name
        database: Database name (optional, uses current if not specified)

    Returns:
        List of dicts with column information
    """
    try:
        # Check if connection is valid
        if not conn or not conn.is_connected():
            logError(f"MariaDB connection not available for table {table_name}")
            return []

        cursor = get_cursor(conn)

        if not database:
            database = getCurrentDatabase(conn)

        # Get column information from information_schema
        cursor.execute("""
            SELECT
                COLUMN_NAME,
                COLUMN_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY,
                EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (database, table_name))

        columns = []
        for row in cursor.fetchall():
            col_name = decode_value(row[0])
            col_type = decode_value(row[1])
            is_nullable = decode_value(row[2]) == 'YES'
            default_val = decode_value(row[3]) if row[3] is not None else None
            col_key = decode_value(row[4])
            extra = decode_value(row[5])

            # Add key information to the column info
            key_info = ''
            if col_key == 'PRI':
                key_info = ' PRIMARY KEY'
            elif col_key == 'UNI':
                key_info = ' UNIQUE'
            elif col_key == 'MUL':
                key_info = ' INDEX'

            if 'auto_increment' in extra.lower():
                key_info += ' AUTO_INCREMENT'

            columns.append({
                'name': col_name,
                'type': col_type + key_info,
                'nullable': is_nullable,
                'default': default_val
            })

        cursor.close()
        return columns

    except Error as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []
