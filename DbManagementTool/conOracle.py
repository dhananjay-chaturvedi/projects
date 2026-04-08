import cx_Oracle
import sys
import logging
import os
from config_loader import config, get_db_port, console_print

# Initialize Oracle Client library location
try:
    oracle_client_path = config.get_path('paths', 'oracle_client_path')

    # Check if already initialized
    if not hasattr(cx_Oracle, '_client_initialized'):
        if os.path.exists(oracle_client_path):
            # Convert Path object to string for cx_Oracle
            cx_Oracle.init_oracle_client(lib_dir=str(oracle_client_path))
            cx_Oracle._client_initialized = True
            console_print(f"Oracle Client initialized with path: {oracle_client_path}")
        else:
            print(f"Warning: Oracle Client path not found: {oracle_client_path}", file=sys.stderr)
except Exception as e:
    print(f"Note: Oracle Client initialization: {e}", file=sys.stderr)
    # This may fail if already initialized, which is fine

logging.basicConfig(filename='conOracle.log', level=logging.INFO)

def log(message):
    logging.info(message)
    console_print(message)
    sys.stdout.flush()
    sys.stderr.flush()
    return True
def logError(message):
    logging.error(message)
    print(message, file=sys.stderr)  # Errors always to stderr
    sys.stderr.flush()
    return True
def logWarning(message):
    logging.warning(message)
    print(message, file=sys.stderr)  # Warnings always to stderr
    sys.stderr.flush()
    return True
def logCritical(message):
    logging.critical(message)
    print(message, file=sys.stderr)  # Critical always to stderr
    sys.stderr.flush()
    return True
def logDebug(message):
    logging.debug(message)
    console_print(message)  # Debug respects config
    sys.stdout.flush()
    return True
def logException(message):
    logging.exception(message)
    print(message, file=sys.stderr)  # Exceptions always to stderr
    sys.stderr.flush()
    return True
def logFatal(message):
    logging.fatal(message)
    print(message, file=sys.stderr)  # Fatal always to stderr
    sys.stderr.flush()
    return True
def logInfo(message):
    logging.info(message)
    console_print(message)  # Info respects config
    sys.stdout.flush()

def connectOracle(db, host, user, password, port=None):  # Connect to Oracle database
    """
    Connect to Oracle database.

    Note: No statement timeout is set, allowing long-running operations
    like schema conversions and large data transfers to complete without timeout.
    Oracle's connection timeout would only apply to initial connection, not to queries.
    """
    # Use configured port if not provided
    if port is None:
        port = get_db_port('oracle')

    try:
        dsn = cx_Oracle.makedsn(host, port, service_name=db)
        conn = cx_Oracle.connect(user, password, dsn)
        # No timeout parameters set - allows long-running schema conversions and data transfers
        return conn
    except cx_Oracle.Error as e:
        logError(f"Failed to connect to Oracle: {e}")
        return None

def disconnectOracle(conn):  # Disconnect from Oracle database
    conn.close()
    return True

def validate_connection(conn):
    """
    Validate that Oracle connection is alive and ready for operations

    Returns:
        bool: True if connection is valid, False otherwise
    """
    try:
        if not conn:
            return False

        # Ping the connection to verify it's still alive
        conn.ping()
        return True
    except Exception:
        return False

def isDBA(conn):  # Check if user has DBA privileges
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM session_roles WHERE role = 'DBA'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except cx_Oracle.Error:
        return False

def getOracleVersion(conn):  # Get Oracle database version
    return conn.version

def getOracleTables(conn):  # Get Oracle database tables
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, table_name FROM dba_tables ORDER BY owner, table_name")
            tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    except cx_Oracle.Error as e:
        logError(f"Failed to get tables: {e}")
        return []

def getOracleViews(conn):  # Get Oracle database views
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, view_name FROM dba_views ORDER BY owner, view_name")
            views = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT view_name FROM user_views ORDER BY view_name")
            views = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return views
    except cx_Oracle.Error as e:
        logError(f"Failed to get views: {e}")
        return []

def getOracleProcedures(conn):  # Get Oracle database procedures
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, object_name FROM dba_procedures WHERE object_type = 'PROCEDURE' ORDER BY owner, object_name")
            procedures = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT object_name FROM user_procedures WHERE object_type = 'PROCEDURE' ORDER BY object_name")
            procedures = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return procedures
    except cx_Oracle.Error as e:
        logError(f"Failed to get procedures: {e}")
        return []

def getOracleFunctions(conn):  # Get Oracle database functions
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, object_name FROM dba_procedures WHERE object_type = 'FUNCTION' ORDER BY owner, object_name")
            functions = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT object_name FROM user_procedures WHERE object_type = 'FUNCTION' ORDER BY object_name")
            functions = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return functions
    except cx_Oracle.Error as e:
        logError(f"Failed to get functions: {e}")
        return []

def getOraclePackages(conn):  # Get Oracle database packages
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, object_name FROM dba_objects WHERE object_type = 'PACKAGE' ORDER BY owner, object_name")
            packages = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT object_name FROM user_objects WHERE object_type = 'PACKAGE' ORDER BY object_name")
            packages = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return packages
    except cx_Oracle.Error as e:
        logError(f"Failed to get packages: {e}")
        return []

def getOracleSequences(conn):  # Get Oracle database sequences
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT sequence_owner, sequence_name FROM dba_sequences ORDER BY sequence_owner, sequence_name")
            sequences = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT sequence_name FROM user_sequences ORDER BY sequence_name")
            sequences = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return sequences
    except cx_Oracle.Error as e:
        logError(f"Failed to get sequences: {e}")
        return []

def getOracleTriggers(conn):  # Get Oracle database triggers
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, trigger_name FROM dba_triggers ORDER BY owner, trigger_name")
            triggers = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT trigger_name FROM user_triggers ORDER BY trigger_name")
            triggers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except cx_Oracle.Error as e:
        logError(f"Failed to get triggers: {e}")
        return []

def getOracleIndexes(conn):  # Get Oracle database indexes
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, index_name FROM dba_indexes ORDER BY owner, index_name")
            indexes = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT index_name FROM user_indexes ORDER BY index_name")
            indexes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except cx_Oracle.Error as e:
        logError(f"Failed to get indexes: {e}")
        return []

def getOracleConstraints(conn):  # Get Oracle database constraints
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, constraint_name FROM dba_constraints ORDER BY owner, constraint_name")
            constraints = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT constraint_name FROM user_constraints ORDER BY constraint_name")
            constraints = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return constraints
    except cx_Oracle.Error as e:
        logError(f"Failed to get constraints: {e}")
        return []

def getOracleSynonyms(conn):  # Get Oracle database synonyms
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT owner, synonym_name FROM dba_synonyms ORDER BY owner, synonym_name")
            synonyms = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT synonym_name FROM user_synonyms ORDER BY synonym_name")
            synonyms = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return synonyms
    except cx_Oracle.Error as e:
        logError(f"Failed to get synonyms: {e}")
        return []

def getOracleTablespaces(conn):  # Get Oracle database tablespaces
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT tablespace_name FROM dba_tablespaces ORDER BY tablespace_name")
        else:
            cursor.execute("SELECT tablespace_name FROM user_tablespaces ORDER BY tablespace_name")
        tablespaces = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tablespaces
    except cx_Oracle.Error as e:
        logError(f"Failed to get tablespaces: {e}")
        return []

def getOracleUsers(conn):  # Get Oracle database users
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT username FROM dba_users ORDER BY username")
        else:
            cursor.execute("SELECT username FROM all_users ORDER BY username")
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return users
    except cx_Oracle.Error as e:
        logError(f"Failed to get users: {e}")
        return []

def getOracleRoles(conn):  # Get Oracle database roles
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT role FROM dba_roles ORDER BY role")
        else:
            cursor.execute("SELECT role FROM session_roles ORDER BY role")
        roles = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return roles
    except cx_Oracle.Error as e:
        logError(f"Failed to get roles: {e}")
        return []

def getOracleTableSchema(conn, table_name):  # Get table schema (columns)
    """
    Get detailed schema for a table including column names, types, and constraints

    Args:
        conn: Oracle connection
        table_name: Table name (can be OWNER.TABLE_NAME or just TABLE_NAME)

    Returns:
        List of dicts with column information
    """
    try:
        # Check if connection is valid
        if not conn:
            logError(f"Oracle connection not available for table {table_name}")
            return []

        cursor = conn.cursor()

        # Parse table name to handle OWNER.TABLE format
        if '.' in table_name:
            owner, table = table_name.split('.', 1)
            log(f"[Schema Debug] Parsed table: owner={owner}, table={table}")
        else:
            # Use current user if no owner specified
            cursor.execute("SELECT USER FROM DUAL")
            owner = cursor.fetchone()[0]
            table = table_name
            log(f"[Schema Debug] Using current user: owner={owner}, table={table}")

        owner_upper = owner.upper()
        table_upper = table.upper()

        log(f"[Schema Debug] Querying with: owner={owner_upper}, table={table_upper}")

        # Get column information
        is_dba = isDBA(conn)
        log(f"[Schema Debug] Is DBA: {is_dba}")

        if is_dba:
            query = """
                SELECT
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    data_default
                FROM dba_tab_columns
                WHERE owner = :1
                  AND table_name = :2
                ORDER BY column_id
            """
            log(f"[Schema Debug] Using dba_tab_columns")
        else:
            query = """
                SELECT
                    column_name,
                    data_type,
                    data_length,
                    data_precision,
                    data_scale,
                    nullable,
                    data_default
                FROM all_tab_columns
                WHERE owner = :1
                  AND table_name = :2
                ORDER BY column_id
            """
            log(f"[Schema Debug] Using all_tab_columns")

        cursor.execute(query, [owner_upper, table_upper])
        rows = cursor.fetchall()
        log(f"[Schema Debug] Found {len(rows)} columns")

        if len(rows) == 0:
            # Try to verify if table exists
            if is_dba:
                cursor.execute("""
                    SELECT COUNT(*) FROM dba_tables
                    WHERE owner = :1 AND table_name = :2
                """, [owner_upper, table_upper])
            else:
                cursor.execute("""
                    SELECT COUNT(*) FROM all_tables
                    WHERE owner = :1 AND table_name = :2
                """, [owner_upper, table_upper])

            table_exists = cursor.fetchone()[0] > 0
            log(f"[Schema Debug] Table exists check: {table_exists}")

            if not table_exists:
                logError(f"Table {owner_upper}.{table_upper} not found in Oracle")
            else:
                logError(f"Table {owner_upper}.{table_upper} exists but has no columns (unexpected)")

        columns = []
        for idx, row in enumerate(rows):
            col_name, data_type, length, precision, scale, nullable, default = row

            if idx < 3:  # Log first 3 columns for debugging
                log(f"[Schema Debug] Column {idx}: {col_name}, {data_type}, len={length}, prec={precision}, scale={scale}, null={nullable}")

            # Format type with size/precision
            if data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR'):
                type_str = f"{data_type}({length})"
            elif data_type == 'NUMBER' and precision is not None:
                if scale is not None and scale > 0:
                    type_str = f"NUMBER({precision},{scale})"
                else:
                    type_str = f"NUMBER({precision})"
            else:
                type_str = data_type

            columns.append({
                'name': col_name,
                'type': type_str,
                'nullable': nullable == 'Y',
                'default': str(default).strip() if default else None
            })

        cursor.close()
        log(f"[Schema Debug] Returning {len(columns)} columns for {table_name}")
        return columns

    except cx_Oracle.Error as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []

# test connection function
def testConnection(db, host, user, password, port):
    try:
        conn = connectOracle(db, host, user, password, port)
        if conn:
            log("Connection successful")
            disconnectOracle(conn)
            return True
        else:
            logError("Connection failed")
            return False
    except cx_Oracle.Error as e:
        logError(f"Connection error: {e}")
        return False

