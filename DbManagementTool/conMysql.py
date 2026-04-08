import mysql.connector
from mysql.connector import Error
import sys
import logging
from config_loader import config, get_db_port, console_print

logging.basicConfig(filename='conMysql.log', level=logging.INFO)

def log(message):
    """Log info message"""
    logging.info(message)
    console_print(message)
    return True

def logError(message):
    """Log error message"""
    logging.error(message)
    print(message, file=sys.stderr, flush=True)
    return True

def logWarning(message):
    """Log warning message"""
    logging.warning(message)
    print(message, file=sys.stderr, flush=True)
    return True

def logCritical(message):
    """Log critical message"""
    logging.critical(message)
    print(message, file=sys.stderr, flush=True)
    return True

def logDebug(message):
    """Log debug message"""
    logging.debug(message)
    console_print(message)
    return True

def logException(message):
    """Log exception with traceback"""
    logging.exception(message)
    print(message, file=sys.stderr, flush=True)
    return True

def logFatal(message):
    """Log fatal message"""
    logging.fatal(message)
    print(message, file=sys.stderr, flush=True)
    return True

def logInfo(message):
    """Log info message"""
    logging.info(message)
    console_print(message)

def decode_value(value):
    """Helper function to decode bytearray to string"""
    if isinstance(value, bytearray):
        return value.decode('utf-8')
    elif isinstance(value, bytes):
        return value.decode('utf-8')
    return value

def get_cursor(conn):
    """Helper function to get a buffered cursor"""
    return conn.cursor(buffered=True)

def validate_connection(conn):
    """
    Validate that connection is alive and ready for operations

    Returns:
        bool: True if connection is valid, False otherwise
    """
    try:
        if not conn or not conn.is_connected():
            return False

        # Ping to verify connection is responsive
        conn.ping(reconnect=False, attempts=1, delay=0)
        return True
    except Exception:
        return False

def connectMysql(database, host, user, password, port=None):  # Connect to MySQL database
    """
    Connect to MySQL database.

    Note: No statement timeout is set, allowing long-running operations
    like schema conversions and large data transfers to complete without timeout.
    """
    # Use configured port if not provided
    if port is None:
        port = get_db_port('mysql')

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
            consume_results=True,
            # connection_timeout applies only to initial connection, not to queries
            # No statement timeout - schema conversions and data transfers can run indefinitely
        )
        if conn.is_connected():
            return conn
        return None
    except Error as e:
        logError(f"Failed to connect to MySQL: {e}")
        return None

def disconnectMysql(conn):  # Disconnect from MySQL database
    if conn and conn.is_connected():
        conn.close()
    return True

def isRoot(conn):  # Check if user has root/admin privileges
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT CURRENT_USER()")
        user = cursor.fetchone()[0]
        cursor.close()

        # Check if user has SUPER privilege
        cursor = get_cursor(conn)
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        grants = cursor.fetchall()
        cursor.close()

        for grant in grants:
            grant_str = str(grant[0])
            if 'ALL PRIVILEGES' in grant_str or 'SUPER' in grant_str:
                return True
        return False
    except Error:
        return False

def getMysqlVersion(conn):  # Get MySQL database version
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.close()
        return version
    except Error as e:
        logError(f"Failed to get version: {e}")
        return None

def getMysqlTables(conn, database=None):  # Get MySQL database tables
    try:
        # Validate connection first
        if not validate_connection(conn):
            logError("MySQL connection not valid or not connected")
            return []

        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SHOW TABLES FROM {database}")
        else:
            # Get current database if not specified
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()
            if current_db and current_db[0]:
                cursor.execute(f"SHOW TABLES FROM {current_db[0]}")
            else:
                # No database selected, query information_schema for all databases
                cursor.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME")
        tables = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return tables
    except Error as e:
        logError(f"Failed to get tables: {e}")
        return []

def getMysqlViews(conn, database=None):  # Get MySQL database views
    try:
        # Validate connection first
        if not validate_connection(conn):
            logError("MySQL connection not valid or not connected")
            return []

        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA = '{database}' ORDER BY TABLE_NAME")
        else:
            cursor.execute("""
                SELECT TABLE_NAME FROM information_schema.VIEWS
                WHERE TABLE_SCHEMA = IFNULL(DATABASE(), TABLE_SCHEMA)
                AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY TABLE_NAME
            """)
        views = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return views
    except Error as e:
        logError(f"Failed to get views: {e}")
        return []

def getMysqlProcedures(conn, database=None):  # Get MySQL database procedures
    try:
        # Validate connection first
        if not validate_connection(conn):
            logError("MySQL connection not valid or not connected")
            return []

        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '{database}' ORDER BY ROUTINE_NAME")
        else:
            cursor.execute("""
                SELECT ROUTINE_NAME FROM information_schema.ROUTINES
                WHERE ROUTINE_TYPE = 'PROCEDURE'
                AND ROUTINE_SCHEMA = IFNULL(DATABASE(), ROUTINE_SCHEMA)
                AND ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY ROUTINE_NAME
            """)
        procedures = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return procedures
    except Error as e:
        logError(f"Failed to get procedures: {e}")
        return []

def getMysqlFunctions(conn, database=None):  # Get MySQL database functions
    try:
        # Validate connection first
        if not validate_connection(conn):
            logError("MySQL connection not valid or not connected")
            return []

        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_SCHEMA = '{database}' ORDER BY ROUTINE_NAME")
        else:
            cursor.execute("""
                SELECT ROUTINE_NAME FROM information_schema.ROUTINES
                WHERE ROUTINE_TYPE = 'FUNCTION'
                AND ROUTINE_SCHEMA = IFNULL(DATABASE(), ROUTINE_SCHEMA)
                AND ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY ROUTINE_NAME
            """)
        functions = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return functions
    except Error as e:
        logError(f"Failed to get functions: {e}")
        return []

def getMysqlTriggers(conn, database=None):  # Get MySQL database triggers
    try:
        # Validate connection first
        if not validate_connection(conn):
            logError("MySQL connection not valid or not connected")
            return []

        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = '{database}' ORDER BY TRIGGER_NAME")
        else:
            cursor.execute("""
                SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = IFNULL(DATABASE(), TRIGGER_SCHEMA)
                AND TRIGGER_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY TRIGGER_NAME
            """)
        triggers = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except Error as e:
        logError(f"Failed to get triggers: {e}")
        return []

def getMysqlIndexes(conn, table_name, database=None):  # Get MySQL indexes for a specific table
    try:
        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SHOW INDEX FROM {database}.{table_name}")
        else:
            cursor.execute(f"SHOW INDEX FROM {table_name}")
        indexes = list(set([decode_value(row[2]) for row in cursor.fetchall()]))  # Get unique index names
        cursor.close()
        return indexes
    except Error as e:
        logError(f"Failed to get indexes: {e}")
        return []

def getMysqlAllIndexes(conn, database=None):  # Get all indexes in database
    try:
        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = '{database}' ORDER BY TABLE_NAME, INDEX_NAME")
        else:
            cursor.execute("""
                SELECT DISTINCT INDEX_NAME, TABLE_NAME FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = IFNULL(DATABASE(), TABLE_SCHEMA)
                AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY TABLE_NAME, INDEX_NAME
            """)
        indexes = [f"{decode_value(row[1])}.{decode_value(row[0])}" for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except Error as e:
        logError(f"Failed to get all indexes: {e}")
        return []

def getMysqlDatabases(conn):  # Get MySQL databases
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW DATABASES")
        databases = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return databases
    except Error as e:
        logError(f"Failed to get databases: {e}")
        return []

def getMysqlUsers(conn):  # Get MySQL users
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT DISTINCT User, Host FROM mysql.user ORDER BY User, Host")
        users = [f"{decode_value(row[0])}@{decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return users
    except Error as e:
        logError(f"Failed to get users: {e}")
        return []

def getMysqlEvents(conn, database=None):  # Get MySQL scheduled events
    try:
        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA = '{database}' ORDER BY EVENT_NAME")
        else:
            cursor.execute("""
                SELECT EVENT_NAME FROM information_schema.EVENTS
                WHERE EVENT_SCHEMA = IFNULL(DATABASE(), EVENT_SCHEMA)
                AND EVENT_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY EVENT_NAME
            """)
        events = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return events
    except Error as e:
        logError(f"Failed to get events: {e}")
        return []

def getMysqlConstraints(conn, database=None):  # Get MySQL constraints
    try:
        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"""
                SELECT DISTINCT CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE
                FROM information_schema.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = '{database}'
                ORDER BY TABLE_NAME, CONSTRAINT_NAME
            """)
        else:
            cursor.execute("""
                SELECT DISTINCT CONSTRAINT_NAME, TABLE_NAME, CONSTRAINT_TYPE
                FROM information_schema.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = IFNULL(DATABASE(), TABLE_SCHEMA)
                AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                ORDER BY TABLE_NAME, CONSTRAINT_NAME
            """)
        constraints = [f"{decode_value(row[1])}.{decode_value(row[0])} ({decode_value(row[2])})" for row in cursor.fetchall()]
        cursor.close()
        return constraints
    except Error as e:
        logError(f"Failed to get constraints: {e}")
        return []

def getMysqlTableColumns(conn, table_name, database=None):  # Get columns for a specific table
    try:
        cursor = get_cursor(conn)
        if database:
            cursor.execute(f"SHOW COLUMNS FROM {database}.{table_name}")
        else:
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return columns
    except Error as e:
        logError(f"Failed to get columns: {e}")
        return []

def getMysqlCharsets(conn):  # Get MySQL character sets
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW CHARACTER SET")
        charsets = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return charsets
    except Error as e:
        logError(f"Failed to get charsets: {e}")
        return []

def getMysqlCollations(conn):  # Get MySQL collations
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW COLLATION")
        collations = [decode_value(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return collations
    except Error as e:
        logError(f"Failed to get collations: {e}")
        return []

def getMysqlEngines(conn):  # Get MySQL storage engines
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW ENGINES")
        engines = [f"{decode_value(row[0])} - {decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return engines
    except Error as e:
        logError(f"Failed to get engines: {e}")
        return []

def getMysqlVariables(conn, pattern=None):  # Get MySQL system variables
    try:
        cursor = get_cursor(conn)
        if pattern:
            cursor.execute(f"SHOW VARIABLES LIKE '{pattern}'")
        else:
            cursor.execute("SHOW VARIABLES")
        variables = [f"{decode_value(row[0])} = {decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return variables
    except Error as e:
        logError(f"Failed to get variables: {e}")
        return []

def getMysqlStatus(conn, pattern=None):  # Get MySQL status variables
    try:
        cursor = get_cursor(conn)
        if pattern:
            cursor.execute(f"SHOW STATUS LIKE '{pattern}'")
        else:
            cursor.execute("SHOW STATUS")
        status = [f"{decode_value(row[0])} = {decode_value(row[1])}" for row in cursor.fetchall()]
        cursor.close()
        return status
    except Error as e:
        logError(f"Failed to get status: {e}")
        return []

def getMysqlProcessList(conn):  # Get MySQL process list
    try:
        cursor = get_cursor(conn)
        cursor.execute("SHOW PROCESSLIST")
        processes = []
        for row in cursor.fetchall():
            processes.append(f"ID:{decode_value(row[0])} User:{decode_value(row[1])} Host:{decode_value(row[2])} DB:{decode_value(row[3])} Command:{decode_value(row[4])} Time:{decode_value(row[5])}")
        cursor.close()
        return processes
    except Error as e:
        logError(f"Failed to get process list: {e}")
        return []

def getCurrentDatabase(conn):  # Get current database name
    try:
        cursor = get_cursor(conn)
        cursor.execute("SELECT DATABASE()")
        db = cursor.fetchone()[0]
        cursor.close()
        return db
    except Error as e:
        logError(f"Failed to get current database: {e}")
        return None

def getMysqlTableSchema(conn, table_name, database=None):  # Get table schema (columns)
    """
    Get detailed schema for a table including column names, types, and constraints

    Args:
        conn: MySQL connection
        table_name: Table name
        database: Database name (optional, uses current if not specified)

    Returns:
        List of dicts with column information
    """
    try:
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

# Test connection function
def testConnection(database, host, user, password, port):
    try:
        conn = connectMysql(database, host, user, password, port)
        if conn:
            log("Connection successful")
            disconnectMysql(conn)
            return True
        else:
            logError("Connection failed")
            return False
    except Error as e:
        logError(f"Connection error: {e}")
        return False
