# ---------------------------------------------------------------------
# description: Database registry manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""
Database Registry System
Extensible architecture for supporting multiple database types
"""

from common.config_loader import get_db_port, console_debug, console_print

from typing import Dict, Any

from common.db_capabilities import (
    DBCapabilities,
    DEFAULT_SQL_CAPABILITIES,
    DOCUMENTDB_CAPABILITIES,
    MARIADB_CAPABILITIES,
    MONGODB_CAPABILITIES,
    MYSQL_CAPABILITIES,
    ORACLE_CAPABILITIES,
    POSTGRES_CAPABILITIES,
    SQLSERVER_CAPABILITIES,
)


class DatabaseRegistry:
    """Central registry for all database type operations"""

    _registry = {}
    _initialized = False
    _init_lock = None  # Created lazily to avoid import-time threading dependency

    @classmethod
    def _get_lock(cls):
        import threading
        if cls._init_lock is None:
            cls._init_lock = threading.Lock()
        return cls._init_lock

    @classmethod
    def _ensure_initialized(cls):
        """Lazy initialization - only load database drivers when first needed.

        Uses a double-checked lock to avoid both data races and the overhead
        of acquiring the lock on every call once initialized.
        """
        if not cls._initialized:
            with cls._get_lock():
                if not cls._initialized:
                    register_all_databases()
                    cls._initialized = True

    @classmethod
    def register(cls, db_type, config):
        """
        Register a database type with its operations

        Args:
            db_type (str): Database type identifier (e.g., "Oracle", "MySQL")
            config (dict): Configuration with:
                - module: The connection module
                - operations: Dict mapping operation names to function names
                - display_name: Human-readable name
                - default_port: Default port number
                - connection_params: Required connection parameters
        """
        cls._registry[db_type] = config
        console_debug(f"Registered database type: {db_type}")

    @classmethod
    def get_config(cls, db_type) -> Dict[str, Any]:
        """Get configuration for a database type

        Always returns a dict (empty if not registered) to avoid Optional
        return types which can cause static analysis warnings when
        subscripting the result.
        """
        cls._ensure_initialized()
        return cls._registry.get(db_type, {})

    @classmethod
    def get_all_types(cls):
        """Get list of all registered database types"""
        cls._ensure_initialized()
        return list(cls._registry.keys())

    @classmethod
    def get_operation(cls, db_type, operation_name):
        """
        Get operation function for a database type

        Args:
            db_type (str): Database type
            operation_name (str): Operation name (e.g., "getTables", "getViews")

        Returns:
            function: The operation function, or None if not found
        """
        cls._ensure_initialized()
        config = cls._registry.get(db_type)
        if not config:
            return None

        operations = config.get("operations", {})
        func_name = operations.get(operation_name)

        if not func_name:
            return None

        module = config.get("module")
        return getattr(module, func_name, None)

    @classmethod
    def execute_operation(cls, db_type, operation_name, conn, *args, **kwargs):
        """
        Execute an operation for a database type

        Args:
            db_type (str): Database type
            operation_name (str): Operation name
            conn: Database connection
            *args, **kwargs: Additional arguments for the operation

        Returns:
            Result of the operation, or None if operation not found
        """
        func = cls.get_operation(db_type, operation_name)
        if func:
            return func(conn, *args, **kwargs)
        return None

    @classmethod
    def get_display_name(cls, db_type):
        """Get display name for a database type"""
        config = cls._registry.get(db_type)
        return config.get("display_name", db_type) if config else db_type

    @classmethod
    def get_default_port(cls, db_type):
        """Get default port for a database type"""
        config = cls._registry.get(db_type)
        return config.get("default_port", 0) if config else 0

    @classmethod
    def get_capabilities(cls, db_type: str) -> DBCapabilities:
        """Return capability metadata for a database type."""
        cls._ensure_initialized()
        config = cls._registry.get(db_type, {})
        caps = config.get("capabilities")
        if isinstance(caps, DBCapabilities):
            return caps
        return DEFAULT_SQL_CAPABILITIES

    @classmethod
    def get_connection_params(cls, db_type):
        """Get required connection parameters for a database type"""
        config = cls._registry.get(db_type)
        return config.get("connection_params", []) if config else []

    @classmethod
    def supports_operation(cls, db_type, operation_name):
        """Check if a database type supports an operation"""
        return cls.get_operation(db_type, operation_name) is not None

    @classmethod
    def get_available_operations(cls, db_type):
        """
        Get list of available operations with display names for a database type

        Returns:
            List of tuples: [(display_name, function_name), ...]
        """
        config = cls._registry.get(db_type)
        if not config:
            return []

        operations = config.get("operations", {})
        caps = cls.get_capabilities(db_type)

        # Map operation names to display names
        display_names = {
            "getTables": "Tables",
            "getViews": "Views",
            "getProcedures": "Procedures",
            "getFunctions": "Functions",
            "getTriggers": "Triggers",
            "getSequences": "Sequences",
            "getIndexes": "Indexes",
            "getConstraints": "Constraints",
            "getEvents": "Events",
            "getDatabases": "Databases",
            "getUsers": "Users",
            "getSchemas": "Schemas",
            "getTablespaces": "Tablespaces",
            "getEngines": "Engines",
            "getCharsets": "Charsets",
            "getProcessList": "Process List",
            "getRoles": "Roles",
            "getExtensions": "Extensions",
            "getActivity": "Activity",
            # Database-specific operations
            "getSynonyms": "Synonyms",
            "getPackages": "Packages",
            "getTypes": "Types",
            "getMaterializedViews": "Materialized Views",
            "getDatabaseLinks": "Database Links",
            "getProfiles": "Profiles",
            "getSessions": "Sessions",
        }

        # Filter out internal operations that shouldn't appear as UI buttons
        # These operations are for connection management or programmatic use, not for browsing database objects
        internal_operations = {
            "connect",
            "disconnect",
            "ping",
            "reconnect",
            "getVersion",
            "isRoot",
            "getCurrentDatabase",
            "selectDatabase",
            "getTableSchema",
        }

        result = []
        for op_name, func_name in operations.items():
            # Skip internal operations
            if op_name in internal_operations:
                continue
            display_name = caps.label_for_operation(op_name, display_names.get(op_name, op_name))
            result.append((display_name, func_name))

        return result


def register_all_databases():
    """Register all available database types"""

    # Register Oracle
    try:
        from common.drivers import conOracle

        DatabaseRegistry.register(
            "Oracle",
            {
                "module": conOracle,
                "display_name": "Oracle Database",
                "default_port": get_db_port("oracle"),
                "connection_params": ["host", "port", "service", "user", "password"],
                "capabilities": ORACLE_CAPABILITIES,
                "operations": {
                    "connect": "connectOracle",
                    "disconnect": "disconnectOracle",
                    "ping": "pingOracle",
                    "reconnect": "reconnectOracle",
                    "getVersion": "getOracleVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getOracleTables",
                    "getTableSchema": "getOracleTableSchema",
                    "getViews": "getOracleViews",
                    "getProcedures": "getOracleProcedures",
                    "getFunctions": "getOracleFunctions",
                    "getTriggers": "getOracleTriggers",
                    "getSequences": "getOracleSequences",
                    "getIndexes": "getOracleAllIndexes",
                    "getConstraints": "getOracleConstraints",
                    "getSynonyms": "getOracleSynonyms",
                    "getPackages": "getOraclePackages",
                    "getTypes": "getOracleTypes",
                    "getMaterializedViews": "getOracleMaterializedViews",
                    "getDatabaseLinks": "getOracleDatabaseLinks",
                    "getTablespaces": "getOracleTablespaces",
                    "getUsers": "getOracleUsers",
                    "getRoles": "getOracleRoles",
                    "getProfiles": "getOracleProfiles",
                    "getSessions": "getOracleSessions",
                },
            },
        )
    except ImportError as e:
        console_print(
            f"Oracle support not available: {e} "
            "(install: pip install oracledb)"
        )

    # Register MySQL
    try:
        from common.drivers import conMysql

        DatabaseRegistry.register(
            "MySQL",
            {
                "module": conMysql,
                "display_name": "MySQL Database",
                "default_port": get_db_port("mysql"),
                "connection_params": ["host", "port", "database", "user", "password"],
                "capabilities": MYSQL_CAPABILITIES,
                "operations": {
                    "connect": "connectMysql",
                    "disconnect": "disconnectMysql",
                    "ping": "pingMysql",
                    "reconnect": "reconnectMysql",
                    "getVersion": "getMysqlVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "selectDatabase": "selectDatabase",
                    "getTables": "getMysqlTables",
                    "getTableSchema": "getMysqlTableSchema",
                    "getViews": "getMysqlViews",
                    "getProcedures": "getMysqlProcedures",
                    "getFunctions": "getMysqlFunctions",
                    "getTriggers": "getMysqlTriggers",
                    "getIndexes": "getMysqlAllIndexes",
                    "getConstraints": "getMysqlConstraints",
                    "getEvents": "getMysqlEvents",
                    "getDatabases": "getMysqlDatabases",
                    "getUsers": "getMysqlUsers",
                    "getEngines": "getMysqlEngines",
                    "getCharsets": "getMysqlCharsets",
                    "getProcessList": "getMysqlProcessList",
                },
            },
        )
    except ImportError as e:
        console_print(f"MySQL support not available: {e}")

    # Register MariaDB
    try:
        from common.drivers import conMariadb

        DatabaseRegistry.register(
            "MariaDB",
            {
                "module": conMariadb,
                "display_name": "MariaDB Database",
                "default_port": get_db_port("mariadb"),
                "connection_params": ["host", "port", "database", "user", "password"],
                "capabilities": MARIADB_CAPABILITIES,
                "operations": {
                    "connect": "connectMariadb",
                    "disconnect": "disconnectMariadb",
                    "ping": "pingMariadb",
                    "reconnect": "reconnectMariadb",
                    "getVersion": "getMariadbVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "selectDatabase": "selectDatabase",
                    "getTables": "getMariadbTables",
                    "getTableSchema": "getMariadbTableSchema",
                    "getViews": "getMariadbViews",
                    "getProcedures": "getMariadbProcedures",
                    "getFunctions": "getMariadbFunctions",
                    "getTriggers": "getMariadbTriggers",
                    "getSequences": "getMariadbSequences",
                    "getIndexes": "getMariadbAllIndexes",
                    "getConstraints": "getMariadbConstraints",
                    "getEvents": "getMariadbEvents",
                    "getDatabases": "getMariadbDatabases",
                    "getUsers": "getMariadbUsers",
                    "getEngines": "getMariadbEngines",
                    "getCharsets": "getMariadbCharsets",
                    "getProcessList": "getMariadbProcessList",
                },
            },
        )
    except ImportError as e:
        console_print(f"MariaDB support not available: {e}")

    # Register PostgreSQL
    try:
        from common.drivers import conPostgres

        DatabaseRegistry.register(
            "PostgreSQL",
            {
                "module": conPostgres,
                "display_name": "PostgreSQL Database",
                "default_port": get_db_port("postgresql"),
                "connection_params": ["host", "port", "database", "user", "password"],
                "capabilities": POSTGRES_CAPABILITIES,
                "operations": {
                    "connect": "connectPostgres",
                    "disconnect": "disconnectPostgres",
                    "ping": "pingPostgres",
                    "reconnect": "reconnectPostgres",
                    "getVersion": "getPostgresVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getPostgresTables",
                    "getTableSchema": "getPostgresTableSchema",
                    "getViews": "getPostgresViews",
                    "getProcedures": "getPostgresProcedures",
                    "getFunctions": "getPostgresFunctions",
                    "getTriggers": "getPostgresTriggers",
                    "getSequences": "getPostgresSequences",
                    "getIndexes": "getPostgresAllIndexes",
                    "getConstraints": "getPostgresConstraints",
                    "getSchemas": "getPostgresSchemas",
                    "getExtensions": "getPostgresExtensions",
                    "getDatabases": "getPostgresDatabases",
                    "getUsers": "getPostgresUsers",
                    "getRoles": "getPostgresRoles",
                    "getTablespaces": "getPostgresTablespaces",
                    "getActivity": "getPostgresActivity",
                },
            },
        )
    except ImportError as e:
        console_print(f"PostgreSQL support not available: {e}")

    # Register SQLite (Example of extensibility - file-based database)
    try:
        from common.drivers import conSQLite

        DatabaseRegistry.register(
            "SQLite",
            {
                "module": conSQLite,
                "display_name": "SQLite Database",
                "default_port": 0,  # File-based, no port
                "connection_params": ["database"],  # Only database file path needed
                "operations": {
                    "connect": "connectSQLite",
                    "disconnect": "disconnectSQLite",
                    "ping": "pingSQLite",
                    "reconnect": "reconnectSQLite",
                    "getVersion": "getSQLiteVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getSQLiteTables",
                    "getTableSchema": "getSQLiteTableSchema",
                    "getViews": "getSQLiteViews",
                    "getIndexes": "getSQLiteIndexes",
                    "getTriggers": "getSQLiteTriggers",
                    "getSchemas": "getSQLiteSchemas",
                },
            },
        )
    except ImportError as e:
        console_print(f"SQLite support not available: {e}")

    # Register SQL Server / Azure SQL
    try:
        from common.drivers import conSqlServer

        DatabaseRegistry.register(
            "SQLServer",
            {
                "module": conSqlServer,
                "display_name": "Microsoft SQL Server",
                "default_port": get_db_port("sqlserver"),
                "connection_params": ["host", "port", "database", "user", "password"],
                "capabilities": SQLSERVER_CAPABILITIES,
                "operations": {
                    "connect": "connectSqlServer",
                    "disconnect": "disconnectSqlServer",
                    "ping": "pingSqlServer",
                    "reconnect": "reconnectSqlServer",
                    "getVersion": "getSqlServerVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getSqlServerTables",
                    "getTableSchema": "getSqlServerTableSchema",
                    "getViews": "getSqlServerViews",
                    "getProcedures": "getSqlServerProcedures",
                    "getFunctions": "getSqlServerFunctions",
                    "getTriggers": "getSqlServerTriggers",
                    "getIndexes": "getSqlServerIndexes",
                    "getSchemas": "getSqlServerSchemas",
                    "getUsers": "getSqlServerUsers",
                },
            },
        )
    except ImportError as e:
        console_print(f"SQL Server support not available: {e} (install: pip install pymssql)")

    # Register MongoDB
    try:
        from common.drivers import conMongo

        DatabaseRegistry.register(
            "MongoDB",
            {
                "module": conMongo,
                "display_name": "MongoDB",
                "default_port": get_db_port("mongodb"),
                "connection_params": [
                    "host",
                    "port",
                    "database",
                    "user",
                    "password",
                    "tls",
                    "tls_ca_file",
                ],
                "capabilities": MONGODB_CAPABILITIES,
                "operations": {
                    "connect": "connectMongo",
                    "disconnect": "disconnectMongo",
                    "ping": "pingMongo",
                    "reconnect": "reconnectMongo",
                    "getVersion": "getMongoVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getMongoCollections",
                    "getTableSchema": "getMongoTableSchema",
                    "getIndexes": "getMongoIndexes",
                    "getUsers": "getMongoUsers",
                    "executeDocumentQuery": "executeMongoQuery",
                    "readMongoDocuments": "readMongoDocuments",
                    "insertMongoDocuments": "insertMongoDocuments",
                    "createMongoCollection": "createMongoCollection",
                    "dropMongoCollection": "dropMongoCollection",
                },
            },
        )
        DatabaseRegistry.register(
            "DocumentDB",
            {
                "module": conMongo,
                "display_name": "AWS DocumentDB",
                "default_port": get_db_port("documentdb"),
                "connection_params": [
                    "host",
                    "port",
                    "database",
                    "user",
                    "password",
                    "tls_ca_file",
                ],
                "capabilities": DOCUMENTDB_CAPABILITIES,
                "operations": {
                    "connect": "connectDocumentDB",
                    "disconnect": "disconnectMongo",
                    "ping": "pingMongo",
                    "reconnect": "reconnectMongo",
                    "getVersion": "getMongoVersion",
                    "isRoot": "isRoot",
                    "getCurrentDatabase": "getCurrentDatabase",
                    "getTables": "getMongoCollections",
                    "getTableSchema": "getMongoTableSchema",
                    "getIndexes": "getMongoIndexes",
                    "getUsers": "getMongoUsers",
                    "executeDocumentQuery": "executeMongoQuery",
                    "readMongoDocuments": "readMongoDocuments",
                    "insertMongoDocuments": "insertMongoDocuments",
                    "createMongoCollection": "createMongoCollection",
                    "dropMongoCollection": "dropMongoCollection",
                },
            },
        )
    except ImportError as e:
        console_print(f"MongoDB support not available: {e} (install: pip install pymongo)")

    console_debug(
        f"\nRegistered {len(DatabaseRegistry._registry)} database types: {', '.join(DatabaseRegistry._registry.keys())}"
    )


# Registry is initialized lazily on first use (via _ensure_initialized)
# This prevents loading all database drivers at import time
