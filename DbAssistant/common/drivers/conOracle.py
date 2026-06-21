import sys
import logging
import os
from common.autocommit import default_autocommit, set_autocommit
from common.config_loader import config, get_db_port, console_debug, console_print

from common.drivers import oracle_driver as ora
from common.drivers.connection_options import DriverConnectionParams
from common.drivers.ssl_support import SslParams, oracle_ssl_connect_kwargs

ORACLE_DRIVER = ora.DRIVER_NAME

# Initialize Oracle Client when configured (Thick mode for 11g+ / legacy auth).
try:
    oracle_client_path = config.get_path_or_none("paths", "oracle_client_path")
    lib_dir = (
        str(oracle_client_path)
        if oracle_client_path and os.path.exists(oracle_client_path)
        else None
    )
    if lib_dir is None and oracle_client_path:
        print(
            f"Warning: Oracle Client path not found: {oracle_client_path}",
            file=sys.stderr,
        )
    ora.init_client(lib_dir, console_print=console_print)
except Exception as e:
    print(f"Note: Oracle Client initialization: {e}", file=sys.stderr)


def log(message):
    """Driver INFO trace — see :func:`common.drivers.conMariadb.log`."""
    logging.info(message)
    console_debug(message)
    sys.stdout.flush()
    sys.stderr.flush()
    return True


def logError(message):
    logging.error(message)
    print(message, file=sys.stderr)
    sys.stderr.flush()
    return True


def connectOracle(params=None, **kwargs):
    """
    Connect to Oracle database.

    Note: No statement timeout is set, allowing long-running operations
    like schema conversions and large data transfers to complete without timeout.
    """
    params = DriverConnectionParams.from_call(params, kwargs)
    db = params.database
    host = params.host
    user = params.user
    password = params.password
    port = params.port
    ssl = params.ssl or SslParams()
    if port is None:
        port = get_db_port("oracle")

    try:
        ssl_extra = oracle_ssl_connect_kwargs(
            host=host,
            port=port,
            service_name=db,
            ssl_mode=ssl.ssl_mode,
            ssl_ca=ssl.ssl_ca,
            wallet_location=ssl.wallet_location,
        )
        dsn = ssl_extra.pop("dsn", None) or ora.makedsn(host, port, service_name=db)
        conn = ora.connect(user, password, dsn, **ssl_extra)
        set_autocommit(conn, "Oracle", default_autocommit())
        return conn
    except ora.OracleError as e:
        logError(f"Failed to connect to Oracle: {e}")
        return None


def disconnectOracle(conn):
    conn.close()
    return True


def pingOracle(conn):
    """Return True when the Oracle connection is alive."""
    return validate_connection(conn)


def reconnectOracle(conn, params=None, **kwargs):
    """Reconnect to Oracle using the supplied connection parameters."""
    try:
        disconnectOracle(conn)
    except Exception:
        pass
    return connectOracle(
        params=DriverConnectionParams.from_call(params, kwargs),
    )


def validate_connection(conn):
    """Validate that Oracle connection is alive and ready for operations."""
    try:
        if not conn:
            return False
        conn.ping()
        return True
    except Exception:
        return False


def isDBA(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT role FROM session_roles WHERE role = 'DBA'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except ora.OracleError:
        return False


def getOracleVersion(conn):
    return conn.version


def getOracleTables(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, table_name FROM dba_tables ORDER BY owner, table_name"
            )
            tables = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    except ora.OracleError as e:
        logError(f"Failed to get tables: {e}")
        return []


def getOracleViews(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, view_name FROM dba_views ORDER BY owner, view_name"
            )
            views = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT view_name FROM user_views ORDER BY view_name")
            views = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return views
    except ora.OracleError as e:
        logError(f"Failed to get views: {e}")
        return []


def getOracleProcedures(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, object_name FROM dba_procedures WHERE object_type = 'PROCEDURE' ORDER BY owner, object_name"
            )
            procedures = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT object_name FROM user_procedures WHERE object_type = 'PROCEDURE' ORDER BY object_name"
            )
            procedures = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return procedures
    except ora.OracleError as e:
        logError(f"Failed to get procedures: {e}")
        return []


def getOracleFunctions(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, object_name FROM dba_procedures WHERE object_type = 'FUNCTION' ORDER BY owner, object_name"
            )
            functions = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT object_name FROM user_procedures WHERE object_type = 'FUNCTION' ORDER BY object_name"
            )
            functions = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return functions
    except ora.OracleError as e:
        logError(f"Failed to get functions: {e}")
        return []


def getOraclePackages(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, object_name FROM dba_objects WHERE object_type = 'PACKAGE' ORDER BY owner, object_name"
            )
            packages = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT object_name FROM user_objects WHERE object_type = 'PACKAGE' ORDER BY object_name"
            )
            packages = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return packages
    except ora.OracleError as e:
        logError(f"Failed to get packages: {e}")
        return []


def getOracleSequences(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT sequence_owner, sequence_name FROM dba_sequences ORDER BY sequence_owner, sequence_name"
            )
            sequences = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT sequence_name FROM user_sequences ORDER BY sequence_name"
            )
            sequences = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return sequences
    except ora.OracleError as e:
        logError(f"Failed to get sequences: {e}")
        return []


def getOracleTriggers(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, trigger_name FROM dba_triggers ORDER BY owner, trigger_name"
            )
            triggers = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT trigger_name FROM user_triggers ORDER BY trigger_name"
            )
            triggers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return triggers
    except ora.OracleError as e:
        logError(f"Failed to get triggers: {e}")
        return []


def getOracleIndexes(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, index_name FROM dba_indexes ORDER BY owner, index_name"
            )
            indexes = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute("SELECT index_name FROM user_indexes ORDER BY index_name")
            indexes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return indexes
    except ora.OracleError as e:
        logError(f"Failed to get indexes: {e}")
        return []


def getOracleConstraints(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, constraint_name FROM dba_constraints ORDER BY owner, constraint_name"
            )
            constraints = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT constraint_name FROM user_constraints ORDER BY constraint_name"
            )
            constraints = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return constraints
    except ora.OracleError as e:
        logError(f"Failed to get constraints: {e}")
        return []


def getOracleSynonyms(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT owner, synonym_name FROM dba_synonyms ORDER BY owner, synonym_name"
            )
            synonyms = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
        else:
            cursor.execute(
                "SELECT synonym_name FROM user_synonyms ORDER BY synonym_name"
            )
            synonyms = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return synonyms
    except ora.OracleError as e:
        logError(f"Failed to get synonyms: {e}")
        return []


def getOracleTablespaces(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute(
                "SELECT tablespace_name FROM dba_tablespaces ORDER BY tablespace_name"
            )
        else:
            cursor.execute(
                "SELECT tablespace_name FROM user_tablespaces ORDER BY tablespace_name"
            )
        tablespaces = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tablespaces
    except ora.OracleError as e:
        logError(f"Failed to get tablespaces: {e}")
        return []


def getOracleUsers(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT username FROM dba_users ORDER BY username")
        else:
            cursor.execute("SELECT username FROM all_users ORDER BY username")
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return users
    except ora.OracleError as e:
        logError(f"Failed to get users: {e}")
        return []


def getOracleRoles(conn):
    try:
        cursor = conn.cursor()
        if isDBA(conn):
            cursor.execute("SELECT role FROM dba_roles ORDER BY role")
        else:
            cursor.execute("SELECT role FROM session_roles ORDER BY role")
        roles = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return roles
    except ora.OracleError as e:
        logError(f"Failed to get roles: {e}")
        return []


def getOracleTableSchema(conn, table_name):
    """Get detailed schema for a table including column names, types, and constraints."""
    try:
        if not conn:
            logError(f"Oracle connection not available for table {table_name}")
            return []

        cursor = conn.cursor()

        if "." in table_name:
            owner, table = table_name.split(".", 1)
            log(f"[Schema Debug] Parsed table: owner={owner}, table={table}")
        else:
            cursor.execute("SELECT USER FROM DUAL")
            owner = cursor.fetchone()[0]
            table = table_name
            log(f"[Schema Debug] Using current user: owner={owner}, table={table}")

        owner_upper = owner.upper()
        table_upper = table.upper()

        log(f"[Schema Debug] Querying with: owner={owner_upper}, table={table_upper}")

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
            log("[Schema Debug] Using dba_tab_columns")
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
            log("[Schema Debug] Using all_tab_columns")

        cursor.execute(query, [owner_upper, table_upper])
        rows = cursor.fetchall()
        log(f"[Schema Debug] Found {len(rows)} columns")

        if len(rows) == 0:
            if is_dba:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM dba_tables
                    WHERE owner = :1 AND table_name = :2
                """,
                    [owner_upper, table_upper],
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM all_tables
                    WHERE owner = :1 AND table_name = :2
                """,
                    [owner_upper, table_upper],
                )

            table_exists = cursor.fetchone()[0] > 0
            log(f"[Schema Debug] Table exists check: {table_exists}")

            if not table_exists:
                logError(f"Table {owner_upper}.{table_upper} not found in Oracle")
            else:
                logError(
                    f"Table {owner_upper}.{table_upper} exists but has no columns (unexpected)"
                )

        columns = []
        for idx, row in enumerate(rows):
            col_name, data_type, length, precision, scale, nullable, default = row

            if idx < 3:
                log(
                    f"[Schema Debug] Column {idx}: {col_name}, {data_type}, len={length}, prec={precision}, scale={scale}, null={nullable}"
                )

            if data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR"):
                type_str = f"{data_type}({length})"
            elif data_type == "NUMBER" and precision is not None:
                if scale is not None and scale > 0:
                    type_str = f"NUMBER({precision},{scale})"
                else:
                    type_str = f"NUMBER({precision})"
            else:
                type_str = data_type

            columns.append(
                {
                    "name": col_name,
                    "type": type_str,
                    "nullable": nullable == "Y",
                    "default": str(default).strip() if default else None,
                }
            )

        cursor.close()
        log(f"[Schema Debug] Returning {len(columns)} columns for {table_name}")
        return columns

    except ora.OracleError as e:
        logError(f"Failed to get table schema for {table_name}: {e}")
        return []

