"""
tests/test_full_suite.py
========================
Comprehensive test suite for DbManagementTool.

Sections
--------
  1.  conMysql       – helpers (decode_value, validate_connection)
  2.  conMysql       – connect / disconnect (mocked)
  3.  conMysql       – schema getters (mocked)
  4.  config_loader  – ConfigLoader methods + convenience helpers
  5.  monitoring_utils – sustained_breach logic
  6.  threshold_checker – ThresholdChecker load, check, check_many
  7.  connection_manager – CRUD + encryption round-trip
  8.  monitor_connection_manager – CRUD
  9.  database_registry – registration, operation dispatch
  10. schema_converter – DataTypeMapper type conversions
  11. MySQL INTEGRATION – real queries against local MariaDB
      host=localhost  port=3306  user=dheeru  password=dheeru  db=pushdb

Run from project root:
    pytest tests/test_full_suite.py -v
    pytest tests/test_full_suite.py -v -m "not integration"   # skip live DB
"""

import json
import types

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Local MariaDB / MySQL credentials  (change only here if they change)
# ─────────────────────────────────────────────────────────────────────────────
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "dheeru"
MYSQL_PASS = "dheeru"
MYSQL_DB   = "test"
TEST_TABLE = "pytest_full_suite_tmp"


# ═════════════════════════════════════════════════════════════════════════════
# SHARED MOCK HELPERS
# ═════════════════════════════════════════════════════════════════════════════

class DummyCursor:
    """Mock cursor.

    Parameters
    ----------
    rows : list
        Default rows returned by fetchall / fetchone when no query_map matches.
    query_map : dict {lowercase_substring: rows}
        If the executed query contains a key (case-insensitive), the
        associated rows are returned instead of the default.
    """

    def __init__(self, rows=None, query_map=None):
        self._default    = rows or []
        self._query_map  = {k.lower(): v for k, v in (query_map or {}).items()}
        self._current    = self._default

    def execute(self, query, params=None):
        q = (query or "").lower()
        for pattern, rows in self._query_map.items():
            if pattern in q:
                self._current = rows
                return
        self._current = self._default

    def fetchone(self):
        return self._current[0] if self._current else (None,)

    def fetchall(self):
        return self._current

    def close(self):
        pass


class DummyConn:
    """Mock MySQL connection.

    Parameters
    ----------
    connected : bool
    rows : list
        Default rows for every cursor that is created.
    query_map : dict
        Forwarded to each DummyCursor created by cursor().
    cursor_sequence : list[DummyCursor]
        Pre-built cursors returned in FIFO order.  When exhausted, falls back
        to creating fresh DummyCursors with rows/query_map.
    """

    def __init__(self, connected=True, rows=None, query_map=None,
                 cursor_sequence=None):
        self._connected = connected
        self._rows      = rows or []
        self._query_map = query_map or {}
        self._seq       = list(cursor_sequence) if cursor_sequence else None

    def is_connected(self):
        return self._connected

    def ping(self, reconnect=False, attempts=1, delay=0):
        if not self._connected:
            raise Exception("not connected")

    def cursor(self, buffered=True):
        if self._seq:
            return self._seq.pop(0)
        return DummyCursor(rows=self._rows, query_map=self._query_map)

    def close(self):
        self._connected = False

    def commit(self):
        pass


# ═════════════════════════════════════════════════════════════════════════════
# SHARED FIXTURES
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _clear_monitoring_store():
    """Reset the global breach-tracking store before every test."""
    import monitoring_utils
    monitoring_utils._store.clear()
    yield
    monitoring_utils._store.clear()


@pytest.fixture(scope="module")
def mysql_conn():
    """Real connection to local MariaDB used by all integration tests."""
    from common.drivers import conMysql
    conn = conMysql.connectMysql(
        database=MYSQL_DB,
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        port=MYSQL_PORT,
    )
    if conn is None:
        pytest.skip("Local MySQL/MariaDB not reachable – skipping integration tests")
    cur = conn.cursor(buffered=True)
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS `{TEST_TABLE}` (
            id INT PRIMARY KEY AUTO_INCREMENT,
            device_id VARCHAR(64),
            acc_device_ctrl_no VARCHAR(64)
        )"""
    )
    conn.commit()
    cur.close()
    yield conn
    # module-level teardown: drop any leftover temp tables and close
    for tbl in (TEST_TABLE, f"{TEST_TABLE}_vis", f"{TEST_TABLE}_schema",
                f"{TEST_TABLE}_idx"):
        try:
            cur = conn.cursor(buffered=True)
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            conn.commit()
            cur.close()
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def tmp_conn_manager(tmp_path, monkeypatch):
    """ConnectionManager backed by an isolated DBASSISTANT_HOME."""
    import common.connection_manager as cm
    from common import paths as _paths

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()
    return cm.ConnectionManager()


@pytest.fixture
def tmp_monitor_manager(tmp_path, monkeypatch):
    """MonitorConnectionManager backed by an isolated DBASSISTANT_HOME."""
    import monitoring.monitor_connection_manager as mcm
    from common import paths as _paths

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()
    return mcm.MonitorConnectionManager()


@pytest.fixture
def checker():
    """ThresholdChecker loaded from the project's monitor_thresholds.ini."""
    from monitoring.threshold_checker import ThresholdChecker
    return ThresholdChecker()


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 1 – conMysql: helpers
# ═════════════════════════════════════════════════════════════════════════════

class TestDecodeValue:
    def test_str_passthrough(self):
        from conMysql import decode_value
        assert decode_value("hello") == "hello"

    def test_bytearray(self):
        from conMysql import decode_value
        assert decode_value(bytearray(b"world")) == "world"

    def test_bytes(self):
        from conMysql import decode_value
        assert decode_value(b"bytes") == "bytes"

    def test_int_passthrough(self):
        from conMysql import decode_value
        assert decode_value(42) == 42

    def test_none_passthrough(self):
        from conMysql import decode_value
        assert decode_value(None) is None


class TestValidateConnection:
    def test_none_returns_false(self):
        from conMysql import validate_connection
        assert validate_connection(None) is False

    def test_disconnected_returns_false(self):
        from conMysql import validate_connection
        assert validate_connection(DummyConn(connected=False)) is False

    def test_ping_exception_returns_false(self):
        from conMysql import validate_connection

        class BadPing(DummyConn):
            def ping(self, **kw):
                raise OSError("network error")

        assert validate_connection(BadPing(connected=True)) is False

    def test_valid_conn_returns_true(self):
        from conMysql import validate_connection
        assert validate_connection(DummyConn(connected=True)) is True


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 2 – conMysql: connect / disconnect (mocked)
# ═════════════════════════════════════════════════════════════════════════════

class TestConnectDisconnect:

    def _patch_connector(self, monkeypatch, dummy_conn=None, raise_exc=None):
        import conMysql

        class FakeConnector:
            Error = conMysql.Error

            @staticmethod
            def connect(**kwargs):
                if raise_exc:
                    raise raise_exc
                return dummy_conn

        monkeypatch.setattr(
            conMysql, "mysql", types.SimpleNamespace(connector=FakeConnector)
        )

    def test_connect_success(self, monkeypatch):
        import conMysql
        dummy = DummyConn(connected=True)
        self._patch_connector(monkeypatch, dummy_conn=dummy)
        assert conMysql.connectMysql(
            database="db", host="h", user="u", password="p", port=3306
        ) is dummy

    def test_connect_mysql_error_returns_none(self, monkeypatch):
        import conMysql
        self._patch_connector(monkeypatch,
                              raise_exc=conMysql.Error("bad credentials"))
        assert conMysql.connectMysql(
            database="db", host="h", user="u", password="bad", port=3306
        ) is None

    def test_connect_not_connected_returns_none(self, monkeypatch):
        import conMysql
        self._patch_connector(monkeypatch, dummy_conn=DummyConn(connected=False))
        assert conMysql.connectMysql(
            database="db", host="h", user="u", password="p", port=3306
        ) is None

    def test_disconnect_live_conn(self):
        from conMysql import disconnectMysql
        c = DummyConn(connected=True)
        assert disconnectMysql(c) is True
        assert c.is_connected() is False

    def test_disconnect_already_closed(self):
        from conMysql import disconnectMysql
        assert disconnectMysql(DummyConn(connected=False)) is True

    def test_disconnect_none(self):
        from conMysql import disconnectMysql
        assert disconnectMysql(None) is True

    def test_connect_mysql_success(self, monkeypatch):
        import conMysql
        dummy = DummyConn(connected=True)
        self._patch_connector(monkeypatch, dummy_conn=dummy)
        conn = conMysql.connectMysql(
            database="db", host="h", user="u", password="p", port=3306
        )
        assert conn is dummy
        assert conn.is_connected()

    def test_connect_mysql_failure(self, monkeypatch):
        import conMysql
        self._patch_connector(monkeypatch,
                              raise_exc=conMysql.Error("fail"))
        assert conMysql.connectMysql(
            database="db", host="h", user="u", password="bad", port=3306
        ) is None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 3 – conMysql: schema getters (mocked)
# ═════════════════════════════════════════════════════════════════════════════

class TestMysqlSchemaGetters:

    # ── Tables ───────────────────────────────────────────────────────────────

    def test_get_tables_with_database(self):
        from conMysql import getMysqlTables
        conn = DummyConn(rows=[(b"orders",), (b"customers",)])
        tables = getMysqlTables(conn, database="mydb")
        assert "orders" in tables
        assert "customers" in tables

    def test_get_tables_no_database_with_current_db(self):
        from conMysql import getMysqlTables
        conn = DummyConn(query_map={
            "select database()": [("mydb",)],
            "show tables from":  [(b"t1",), (b"t2",)],
        })
        tables = getMysqlTables(conn)
        assert isinstance(tables, list)

    def test_get_tables_invalid_conn(self):
        from conMysql import getMysqlTables
        assert getMysqlTables(DummyConn(connected=False)) == []

    def test_get_tables_returns_strings_not_bytes(self):
        from conMysql import getMysqlTables
        conn = DummyConn(rows=[(b"byte_table",)])
        tables = getMysqlTables(conn, database="mydb")
        assert all(isinstance(t, str) for t in tables)

    # ── Views ─────────────────────────────────────────────────────────────────

    def test_get_views_with_database(self):
        from conMysql import getMysqlViews
        conn = DummyConn(rows=[(b"v_summary",)])
        assert "v_summary" in getMysqlViews(conn, database="mydb")

    def test_get_views_no_database(self):
        from conMysql import getMysqlViews
        assert isinstance(getMysqlViews(DummyConn(rows=[(b"v1",)])), list)

    def test_get_views_invalid_conn(self):
        from conMysql import getMysqlViews
        assert getMysqlViews(DummyConn(connected=False)) == []

    # ── Procedures ────────────────────────────────────────────────────────────

    def test_get_procedures_with_database(self):
        from conMysql import getMysqlProcedures
        conn = DummyConn(rows=[(b"sp_cleanup",)])
        assert "sp_cleanup" in getMysqlProcedures(conn, database="mydb")

    def test_get_procedures_invalid_conn(self):
        from conMysql import getMysqlProcedures
        assert getMysqlProcedures(DummyConn(connected=False)) == []

    # ── Functions ─────────────────────────────────────────────────────────────

    def test_get_functions_with_database(self):
        from conMysql import getMysqlFunctions
        conn = DummyConn(rows=[(b"fn_calc",)])
        assert "fn_calc" in getMysqlFunctions(conn, database="mydb")

    def test_get_functions_invalid_conn(self):
        from conMysql import getMysqlFunctions
        assert getMysqlFunctions(DummyConn(connected=False)) == []

    # ── Triggers ──────────────────────────────────────────────────────────────

    def test_get_triggers_with_database(self):
        from conMysql import getMysqlTriggers
        conn = DummyConn(rows=[(b"trg_after_insert",)])
        assert "trg_after_insert" in getMysqlTriggers(conn, database="mydb")

    def test_get_triggers_invalid_conn(self):
        from conMysql import getMysqlTriggers
        assert getMysqlTriggers(DummyConn(connected=False)) == []

    # ── Indexes ───────────────────────────────────────────────────────────────

    def test_get_indexes_deduplication(self):
        from conMysql import getMysqlIndexes
        # SHOW INDEX returns row[2] as the index name; duplicates must be deduped
        conn = DummyConn(rows=[
            (None, None, "PRIMARY"),
            (None, None, "idx_email"),
            (None, None, "idx_email"),  # duplicate
        ])
        indexes = getMysqlIndexes(conn, "users")
        assert "PRIMARY"   in indexes
        assert "idx_email" in indexes
        assert indexes.count("idx_email") == 1

    def test_get_all_indexes(self):
        from conMysql import getMysqlAllIndexes
        # Row format: (INDEX_NAME, TABLE_NAME)
        conn = DummyConn(rows=[("PRIMARY", "users"), ("idx_name", "orders")])
        indexes = getMysqlAllIndexes(conn, database="mydb")
        assert any("users.PRIMARY" in i for i in indexes)
        assert any("orders.idx_name" in i for i in indexes)

    # ── Databases & Users ────────────────────────────────────────────────────

    def test_get_databases(self):
        from conMysql import getMysqlDatabases
        conn = DummyConn(rows=[("information_schema",), ("pushdb",)])
        assert "pushdb" in getMysqlDatabases(conn)

    def test_get_users(self):
        from conMysql import getMysqlUsers
        conn = DummyConn(rows=[("root", "localhost"), ("dheeru", "localhost")])
        assert "dheeru@localhost" in getMysqlUsers(conn)

    # ── Events & Constraints ─────────────────────────────────────────────────

    def test_get_events_with_database(self):
        from conMysql import getMysqlEvents
        conn = DummyConn(rows=[(b"daily_cleanup",)])
        assert "daily_cleanup" in getMysqlEvents(conn, database="mydb")

    def test_get_constraints_format(self):
        from conMysql import getMysqlConstraints
        conn = DummyConn(rows=[("pk_users", "users", "PRIMARY KEY")])
        constraints = getMysqlConstraints(conn, database="mydb")
        assert any("pk_users" in c for c in constraints)
        assert any("PRIMARY KEY" in c for c in constraints)

    # ── Column helpers ────────────────────────────────────────────────────────

    def test_get_table_columns(self):
        from conMysql import getMysqlTableColumns
        conn = DummyConn(rows=[("id",), ("name",), ("email",)])
        assert getMysqlTableColumns(conn, "users") == ["id", "name", "email"]

    # ── Server info ───────────────────────────────────────────────────────────

    def test_get_charsets(self):
        from conMysql import getMysqlCharsets
        conn = DummyConn(rows=[("utf8mb4",), ("latin1",)])
        assert "utf8mb4" in getMysqlCharsets(conn)

    def test_get_collations(self):
        from conMysql import getMysqlCollations
        conn = DummyConn(rows=[("utf8mb4_general_ci",)])
        assert "utf8mb4_general_ci" in getMysqlCollations(conn)

    def test_get_engines(self):
        from conMysql import getMysqlEngines
        conn = DummyConn(rows=[("InnoDB", "DEFAULT"), ("MyISAM", "YES")])
        engines = getMysqlEngines(conn)
        assert any("InnoDB" in e for e in engines)

    def test_get_variables_all(self):
        from conMysql import getMysqlVariables
        conn = DummyConn(rows=[("max_connections", "151"), ("version", "10.6")])
        variables = getMysqlVariables(conn)
        assert any("max_connections" in v for v in variables)

    def test_get_variables_with_pattern(self):
        from conMysql import getMysqlVariables
        conn = DummyConn(rows=[("max_connections", "151")])
        variables = getMysqlVariables(conn, pattern="max%")
        assert len(variables) == 1
        assert "max_connections" in variables[0]

    def test_get_status(self):
        from conMysql import getMysqlStatus
        conn = DummyConn(rows=[("Threads_connected", "5")])
        status = getMysqlStatus(conn)
        assert any("Threads_connected" in s for s in status)

    def test_get_process_list(self):
        from conMysql import getMysqlProcessList
        conn = DummyConn(rows=[(1, "dheeru", "localhost", "pushdb", "Query", 0)])
        procs = getMysqlProcessList(conn)
        assert len(procs) == 1
        assert "dheeru" in procs[0]

    def test_get_current_database(self):
        from conMysql import getCurrentDatabase
        assert getCurrentDatabase(DummyConn(rows=[("pushdb",)])) == "pushdb"

    def test_get_version(self):
        from conMysql import getMysqlVersion
        assert getMysqlVersion(DummyConn(rows=[("10.6.19-MariaDB",)])) == "10.6.19-MariaDB"

    # ── Table schema ──────────────────────────────────────────────────────────

    def test_get_table_schema_with_database(self):
        from conMysql import getMysqlTableSchema
        col_rows = [
            ("id",   "int(11)", "NO",  None, "PRI", "auto_increment"),
            ("name", "varchar(255)", "YES", None, "", ""),
        ]
        schema = getMysqlTableSchema(DummyConn(rows=col_rows), "users",
                                     database="mydb")
        assert len(schema) == 2
        assert schema[0]["name"] == "id"
        assert schema[1]["nullable"] is True

    def test_get_table_schema_no_database(self):
        """When database is omitted, getCurrentDatabase is called first
        (requires cursor_sequence with the right pop order)."""
        from conMysql import getMysqlTableSchema
        col_rows = [("id", "int(11)", "NO", None, "PRI", "auto_increment")]
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=col_rows),        # cursor 0: info_schema query
            DummyCursor(rows=[("testdb",)]),   # cursor 1: getCurrentDatabase
        ])
        schema = getMysqlTableSchema(conn, "t1")
        assert len(schema) == 1
        assert schema[0]["name"] == "id"

    def test_get_table_schema_primary_key_annotation(self):
        from conMysql import getMysqlTableSchema
        col_rows = [("pk", "int(11)", "NO", None, "PRI", "auto_increment")]
        schema = getMysqlTableSchema(DummyConn(rows=col_rows), "t", database="db")
        assert "PRIMARY KEY" in schema[0]["type"]
        assert "AUTO_INCREMENT" in schema[0]["type"]

    # ── isRoot ────────────────────────────────────────────────────────────────

    def test_is_root_all_privileges(self):
        from conMysql import isRoot
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("root@localhost",)]),
            DummyCursor(rows=[("GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost`",)]),
        ])
        assert isRoot(conn) is True

    def test_is_root_super_privilege(self):
        from conMysql import isRoot
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("admin@localhost",)]),
            DummyCursor(rows=[("GRANT SUPER, SELECT ON *.* TO `admin`@`localhost`",)]),
        ])
        assert isRoot(conn) is True

    def test_is_root_false(self):
        from conMysql import isRoot
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("reader@localhost",)]),
            DummyCursor(rows=[("GRANT SELECT ON `mydb`.* TO `reader`@`localhost`",)]),
        ])
        assert isRoot(conn) is False


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 4 – config_loader
# ═════════════════════════════════════════════════════════════════════════════

class TestConfigLoader:

    @pytest.fixture
    def cfg(self, tmp_path):
        from common.config_loader import ConfigLoader
        ini = tmp_path / "test.ini"
        ini.write_text(
            "[section1]\n"
            "str_key   = hello\n"
            "int_key   = 42\n"
            "float_key = 3.14\n"
            "bool_true  = true\n"
            "bool_false = false\n"
            "list_key   = a, b, c\n"
            "path_key   = ~/mydir\n"
            "octal_key  = 0o600\n",
            encoding="utf-8",
        )
        return ConfigLoader(str(ini))

    def test_get_str(self, cfg):
        assert cfg.get("section1", "str_key") == "hello"

    def test_get_str_default(self, cfg):
        assert cfg.get("section1", "missing", default="fallback") == "fallback"

    def test_get_int(self, cfg):
        assert cfg.get_int("section1", "int_key") == 42

    def test_get_int_default(self, cfg):
        assert cfg.get_int("section1", "missing", default=99) == 99

    def test_get_int_invalid_returns_default(self, tmp_path):
        from common.config_loader import ConfigLoader
        ini = tmp_path / "bad.ini"
        ini.write_text("[s]\nkey = abc\n")
        cl = ConfigLoader(str(ini))
        assert cl.get_int("s", "key", default=7) == 7

    def test_get_float(self, cfg):
        assert abs(cfg.get_float("section1", "float_key") - 3.14) < 1e-6

    def test_get_float_default(self, cfg):
        assert cfg.get_float("section1", "missing", default=0.5) == 0.5

    def test_get_bool_true(self, cfg):
        assert cfg.get_bool("section1", "bool_true") is True

    def test_get_bool_false(self, cfg):
        assert cfg.get_bool("section1", "bool_false") is False

    def test_get_bool_default(self, cfg):
        assert cfg.get_bool("section1", "missing", default=True) is True

    @pytest.mark.parametrize("val,expected", [
        ("true", True),  ("yes", True),  ("1", True),  ("on", True),
        ("false", False), ("no", False), ("0", False), ("off", False),
    ])
    def test_get_bool_variants(self, tmp_path, val, expected):
        from common.config_loader import ConfigLoader
        ini = tmp_path / f"b_{val}.ini"
        ini.write_text(f"[s]\nkey = {val}\n")
        assert ConfigLoader(str(ini)).get_bool("s", "key") is expected

    def test_get_list(self, cfg):
        assert cfg.get_list("section1", "list_key") == ["a", "b", "c"]

    def test_get_list_default(self, cfg):
        assert cfg.get_list("section1", "missing", default=["x"]) == ["x"]

    def test_get_list_custom_delimiter(self, tmp_path):
        from common.config_loader import ConfigLoader
        ini = tmp_path / "delim.ini"
        ini.write_text("[s]\nkey = x|y|z\n")
        assert ConfigLoader(str(ini)).get_list("s", "key", delimiter="|") == ["x", "y", "z"]

    def test_get_path_tilde_expanded(self, cfg):
        p = cfg.get_path("section1", "path_key")
        assert not str(p).startswith("~")
        assert "mydir" in str(p)

    def test_get_path_or_none_blank(self, tmp_path):
        from common.config_loader import ConfigLoader

        ini = tmp_path / "blank_path.ini"
        ini.write_text("[paths]\noracle_client_path =\n")
        cl = ConfigLoader(str(ini))
        assert cl.get_path_or_none("paths", "oracle_client_path") is None

    def test_get_path_or_none_whitespace(self, tmp_path):
        from common.config_loader import ConfigLoader

        ini = tmp_path / "ws_path.ini"
        ini.write_text("[paths]\noracle_client_path =   \n")
        cl = ConfigLoader(str(ini))
        assert cl.get_path_or_none("paths", "oracle_client_path") is None

    def test_get_path_or_none_set(self, tmp_path):
        from common.config_loader import ConfigLoader

        ini = tmp_path / "set_path.ini"
        ini.write_text("[paths]\noracle_client_path = ~/instantclient\n")
        cl = ConfigLoader(str(ini))
        p = cl.get_path_or_none("paths", "oracle_client_path")
        assert p is not None
        assert "instantclient" in str(p)

    def test_get_octal(self, cfg):
        assert cfg.get_octal("section1", "octal_key") == 0o600

    def test_has_section_true(self, cfg):
        assert cfg.has_section("section1") is True

    def test_has_section_false(self, cfg):
        assert cfg.has_section("ghost") is False

    def test_has_option_true(self, cfg):
        assert cfg.has_option("section1", "str_key") is True

    def test_has_option_false(self, cfg):
        assert cfg.has_option("section1", "ghost") is False

    def test_get_all(self, cfg):
        d = cfg.get_all("section1")
        assert "str_key" in d
        assert d["int_key"] == "42"

    def test_get_all_missing_section(self, cfg):
        assert cfg.get_all("nonexistent") == {}

    def test_missing_file_uses_defaults(self, tmp_path):
        from common.config_loader import ConfigLoader
        cl = ConfigLoader(str(tmp_path / "nonexistent.ini"))
        assert cl.get("s", "k", default="D") == "D"
        assert cl.get_int("s", "k", default=5) == 5

    def test_get_db_port_mysql(self):
        from common.config_loader import get_db_port
        assert get_db_port("mysql") == 3306

    def test_get_db_port_oracle(self):
        from common.config_loader import get_db_port
        assert get_db_port("oracle") == 1521

    def test_get_db_port_postgresql(self):
        from common.config_loader import get_db_port
        assert get_db_port("postgresql") == 5432

    def test_reload_does_not_raise(self, cfg):
        cfg.reload()
        assert cfg.get("section1", "str_key") == "hello"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 5 – monitoring_utils
# ═════════════════════════════════════════════════════════════════════════════

class TestMonitoringUtils:

    def test_gt_fires_after_window(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_gt_w2"
        assert sustained_breach(k, 95, ">", 90, window=2) is False
        assert sustained_breach(k, 95, ">", 90, window=2) is True

    def test_gt_no_fire_when_below_threshold(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_gt_below"
        for _ in range(5):
            assert sustained_breach(k, 50, ">", 90, window=2) is False

    def test_gt_exact_threshold_not_breached(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_gt_exact"
        for _ in range(5):
            assert sustained_breach(k, 90, ">", 90, window=1) is False

    def test_gt_resets_when_value_drops(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_gt_reset"
        sustained_breach(k, 95, ">", 90, window=3)
        sustained_breach(k, 95, ">", 90, window=3)
        sustained_breach(k, 50, ">", 90, window=3)   # drops below → window slides
        assert sustained_breach(k, 95, ">", 90, window=3) is False

    def test_lt_fires_after_window(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_lt_w3"
        assert sustained_breach(k, 80, "<", 100, window=3) is False
        assert sustained_breach(k, 80, "<", 100, window=3) is False
        assert sustained_breach(k, 80, "<", 100, window=3) is True

    def test_lt_exact_threshold_not_breached(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_lt_exact"
        for _ in range(5):
            assert sustained_breach(k, 100, "<", 100, window=1) is False

    def test_lt_no_fire_when_above(self):
        from monitoring.monitoring_utils import sustained_breach
        k = "mu_lt_above"
        for _ in range(5):
            assert sustained_breach(k, 200, "<", 100, window=2) is False

    def test_different_keys_are_independent(self):
        from monitoring.monitoring_utils import sustained_breach
        k1, k2 = "mu_ind_k1", "mu_ind_k2"
        sustained_breach(k1, 95, ">", 90, window=2)
        sustained_breach(k1, 95, ">", 90, window=2)  # k1 fires
        assert sustained_breach(k2, 95, ">", 90, window=2) is False  # k2 still fresh

    def test_non_numeric_value_returns_false(self):
        from monitoring.monitoring_utils import sustained_breach
        assert sustained_breach("mu_nan", "N/A", ">", 90, window=1) is False

    def test_window_1_fires_immediately(self):
        from monitoring.monitoring_utils import sustained_breach
        assert sustained_breach("mu_w1", 95, ">", 90, window=1) is True


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 6 – threshold_checker
# ═════════════════════════════════════════════════════════════════════════════

class TestThresholdChecker:

    def test_minimum_rules_loaded(self, checker):
        assert len(checker.list_rules()) >= 40

    def test_get_rule_aws_cpu(self, checker):
        rule = checker.get_rule("aws", "CPUUtilization")
        assert rule is not None
        assert rule.critical == 90.0
        assert rule.operator  == ">"

    def test_get_rule_nonexistent_returns_none(self, checker):
        assert checker.get_rule("aws", "NoSuchMetric") is None

    def test_list_rules_all_sources_present(self, checker):
        sources = {r.source for r in checker.list_rules()}
        assert {"aws", "azure", "gcp", "os", "db"}.issubset(sources)

    def test_list_rules_filtered_by_source(self, checker):
        aws = checker.list_rules(source="aws")
        assert all(r.source == "aws" for r in aws)
        assert len(aws) >= 5

    def test_check_no_alert_before_window(self, checker):
        # window=3 → first two calls must return None
        assert checker.check("aws", "CPUUtilization", 95.0, instance_id="tc_h1") is None
        assert checker.check("aws", "CPUUtilization", 95.0, instance_id="tc_h1") is None

    def test_check_fires_at_window(self, checker):
        checker.check("aws", "CPUUtilization", 95.0, instance_id="tc_h2")
        checker.check("aws", "CPUUtilization", 95.0, instance_id="tc_h2")
        alert = checker.check("aws", "CPUUtilization", 95.0, instance_id="tc_h2")
        assert alert is not None
        assert alert.severity in ("WARNING", "CRITICAL", "INFO")
        assert "tc_h2" in alert.message
        assert "95" in alert.message

    def test_check_disabled_rule_never_fires(self, checker):
        # ReadThroughput / WriteThroughput have enabled=false
        for _ in range(10):
            assert checker.check("aws", "ReadThroughput", 9e15,
                                 instance_id="tc_h3") is None

    def test_check_less_than_operator_fires(self, checker):
        # FreeStorageSpace threshold < 5 GB (in bytes)
        for _ in range(3):
            result = checker.check("aws", "FreeStorageSpace", 1e9,
                                   instance_id="tc_h4")
        assert result is not None
        assert result.severity in ("WARNING", "CRITICAL", "INFO")

    def test_check_unknown_metric_returns_none(self, checker):
        assert checker.check("aws", "NoSuchMetric", 9999.0,
                             instance_id="tc_h5") is None

    def test_check_many_fires_for_breaching_metric(self, checker):
        metrics = {"CPUUtilization": 95.0, "DatabaseConnections": 10.0}
        alerts = []
        for _ in range(3):
            alerts = checker.check_many("aws", metrics, instance_id="tc_batch1")
        assert any("CPU" in a.message for a in alerts)
        # DatabaseConnections = 10 < 400 threshold → no alert for it
        assert not any("onnections" in a.message for a in alerts)

    def test_check_many_cloudwatch_dict_format(self, checker):
        """check_many accepts {'value': v, 'time': t} payloads."""
        metrics = {"CPUUtilization": {"value": 95.0, "time": "2026-01-01T00:00:00"}}
        for _ in range(3):
            alerts = checker.check_many("aws", metrics, instance_id="tc_batch2")
        assert isinstance(alerts, list)

    def test_check_os_cpu_fires(self, checker):
        for _ in range(3):
            result = checker.check("os", "cpu_utilization", 95.0,
                                   instance_id="tc_os1")
        assert result is not None

    def test_check_db_connections_fires(self, checker):
        for _ in range(3):
            result = checker.check("db", "active_connections", 500,
                                   instance_id="tc_db1")
        assert result is not None

    def test_check_gcp_cpu_fires(self, checker):
        for _ in range(3):
            result = checker.check(
                "gcp", "cpu_utilization", 0.95,
                instance_id="tc_gcp1",
                path=("cloudmonitoring", "cloudsql", "database"),
            )
        assert result is not None

    def test_check_azure_memory_fires(self, checker):
        for _ in range(3):
            result = checker.check("azure", "memory_percent", 95.0,
                                   instance_id="tc_az1")
        assert result is not None

    def test_reload_does_not_raise(self, checker):
        checker.reload()
        assert len(checker.list_rules()) >= 40

    def test_missing_config_raises_file_not_found(self):
        from monitoring.threshold_checker import ThresholdChecker
        with pytest.raises(FileNotFoundError):
            ThresholdChecker(config_path="/nonexistent/path/thresholds.ini")

    def test_instance_id_appears_in_alert(self, checker):
        iid = "my-special-rds-123"
        for _ in range(3):
            alert = checker.check("aws", "CPUUtilization", 95.0, instance_id=iid)
        assert iid in alert.message

    def test_check_below_threshold_never_fires(self, checker):
        for _ in range(10):
            assert checker.check("aws", "CPUUtilization", 50.0,
                                 instance_id="tc_safe") is None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 7 – connection_manager
# ═════════════════════════════════════════════════════════════════════════════

class TestConnectionManager:
    @staticmethod
    def _params(name, db_type="MySQL", host="h", port=3306,
                service_or_db="d", username="u", password="p", **over):
        from common.connection_params import ConnectionParams

        values = {
            "name": name,
            "db_type": db_type,
            "host": host,
            "port": port,
            "service_or_db": service_or_db,
            "username": username,
            "password": password,
        }
        values.update(over)
        return ConnectionParams.from_mapping(values)

    def test_add_connection_success(self, tmp_conn_manager):
        ok, msg = tmp_conn_manager.add_connection(
            self._params("dev-mysql", host="localhost", service_or_db="pushdb",
                         username="dheeru", password="dheeru",
                         save_password=True),
        )
        assert ok is True
        assert "success" in msg.lower()

    def test_add_duplicate_rejected(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(self._params("dup"))
        ok, msg = tmp_conn_manager.add_connection(self._params("dup"))
        assert ok is False
        assert "already exists" in msg.lower()

    def test_get_connection_found(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(
            self._params("prod", "PostgreSQL", "db.host", 5432, "mydb", "admin", "s"))
        conn = tmp_conn_manager.get_connection("prod")
        assert conn is not None
        assert conn["db_type"] == "PostgreSQL"
        assert conn["host"]    == "db.host"

    def test_get_connection_not_found(self, tmp_conn_manager):
        assert tmp_conn_manager.get_connection("ghost") is None

    def test_connection_exists_true(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(self._params("ex"))
        assert tmp_conn_manager.connection_exists("ex") is True

    def test_connection_exists_false(self, tmp_conn_manager):
        assert tmp_conn_manager.connection_exists("no_such") is False

    def test_update_connection_success(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(
            self._params("old", host="h1", service_or_db="d1", username="u1", password="p1"))
        ok, _ = tmp_conn_manager.update_connection(
            "old", self._params("new", host="h2", service_or_db="d2",
                                username="u2", password="p2")
        )
        assert ok is True
        assert tmp_conn_manager.get_connection("new") is not None
        assert tmp_conn_manager.get_connection("old") is None

    def test_update_connection_not_found(self, tmp_conn_manager):
        ok, _ = tmp_conn_manager.update_connection(
            "ghost", self._params("g2")
        )
        assert ok is False

    def test_delete_connection_success(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(self._params("del_me"))
        ok, _ = tmp_conn_manager.delete_connection("del_me")
        assert ok is True
        assert tmp_conn_manager.get_connection("del_me") is None

    def test_delete_connection_not_found(self, tmp_conn_manager):
        ok, _ = tmp_conn_manager.delete_connection("no_such")
        assert ok is False

    def test_get_all_connections(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(self._params("c1", host="h1"))
        tmp_conn_manager.add_connection(self._params("c2", "PostgreSQL", "h2", 5432))
        names = [c["name"] for c in tmp_conn_manager.get_all_connections()]
        assert "c1" in names
        assert "c2" in names

    def test_password_not_stored_when_save_false(self, tmp_conn_manager):
        tmp_conn_manager.add_connection(
            self._params("ns", password="mypassword", save_password=False))
        assert tmp_conn_manager.get_connection("ns")["password"] == ""

    def test_password_encrypted_on_disk(self, tmp_conn_manager, tmp_path):
        from common import paths as _paths

        tmp_conn_manager.add_connection(
            self._params("enc", password="supersecret", save_password=True))
        raw = json.loads(_paths.db_connections_path().read_text())
        stored_pw = raw[0]["password"]
        assert stored_pw != "supersecret"
        assert stored_pw is not None

    def test_password_decrypted_after_reload(self, tmp_conn_manager):
        """A second manager pointing at the same DBASSISTANT_HOME must decrypt."""
        import common.connection_manager as cm

        tmp_conn_manager.add_connection(
            self._params("dec", password="roundtrip", save_password=True))
        # DBASSISTANT_HOME is already set by ``tmp_conn_manager``; constructing
        # a fresh manager re-reads the same db.key + db.json pair.
        fresh = cm.ConnectionManager()
        assert fresh.get_connection("dec")["password"] == "roundtrip"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 8 – monitor_connection_manager
# ═════════════════════════════════════════════════════════════════════════════

class TestMonitorConnectionManager:

    def test_add_connection(self, tmp_monitor_manager):
        ok, _ = tmp_monitor_manager.add_connection("srv1", "10.0.0.1", "admin", "pass")
        assert ok is True

    def test_add_duplicate_rejected(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("dup", "1.2.3.4", "u", "p")
        ok, _ = tmp_monitor_manager.add_connection("dup", "1.2.3.4", "u", "p")
        assert ok is False

    def test_get_connection(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("mon1", "192.168.1.1", "monitor", "secret")
        conn = tmp_monitor_manager.get_connection("mon1")
        assert conn is not None
        assert conn["host"] == "192.168.1.1"

    def test_get_connection_not_found(self, tmp_monitor_manager):
        assert tmp_monitor_manager.get_connection("ghost") is None

    def test_update_connection(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("old", "1.1.1.1", "u", "p")
        ok, _ = tmp_monitor_manager.update_connection("old", "new", "2.2.2.2", "u2", "p2")
        assert ok is True
        assert tmp_monitor_manager.get_connection("new")["host"] == "2.2.2.2"
        assert tmp_monitor_manager.get_connection("old") is None

    def test_update_not_found(self, tmp_monitor_manager):
        ok, _ = tmp_monitor_manager.update_connection("ghost", "new", "h", "u", "p")
        assert ok is False

    def test_delete_connection(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("del_me", "5.5.5.5", "u", "p")
        ok, _ = tmp_monitor_manager.delete_connection("del_me")
        assert ok is True
        assert tmp_monitor_manager.get_connection("del_me") is None

    def test_delete_not_found(self, tmp_monitor_manager):
        ok, _ = tmp_monitor_manager.delete_connection("ghost")
        assert ok is False

    def test_connection_exists(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("chk", "h", "u", "p")
        assert tmp_monitor_manager.connection_exists("chk")      is True
        assert tmp_monitor_manager.connection_exists("no_chk")   is False

    def test_get_all_connections(self, tmp_monitor_manager):
        tmp_monitor_manager.add_connection("ma", "h1", "u", "p")
        tmp_monitor_manager.add_connection("mb", "h2", "u", "p")
        names = [c["name"] for c in tmp_monitor_manager.get_all_connections()]
        assert "ma" in names
        assert "mb" in names


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 9 – database_registry
# ═════════════════════════════════════════════════════════════════════════════

class TestDatabaseRegistry:

    @pytest.fixture(autouse=True)
    def _reset(self):
        """Force re-initialization for every test so they start clean."""
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry._initialized = False
        DatabaseRegistry._registry    = {}
        yield
        DatabaseRegistry._initialized = False
        DatabaseRegistry._registry    = {}

    def test_mysql_registered(self):
        from common.database_registry import DatabaseRegistry
        assert "MySQL" in DatabaseRegistry.get_all_types()

    def test_sqlite_registered(self):
        from common.database_registry import DatabaseRegistry
        assert "SQLite" in DatabaseRegistry.get_all_types()

    def test_get_config_has_module_and_ops(self):
        from common.database_registry import DatabaseRegistry
        cfg = DatabaseRegistry.get_config("MySQL")
        assert "module"     in cfg
        assert "operations" in cfg

    def test_get_config_unknown_returns_empty(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.get_config("NoSuchDB") == {}

    def test_get_operation_mysql_get_tables(self):
        from common.database_registry import DatabaseRegistry
        import conMysql
        op = DatabaseRegistry.get_operation("MySQL", "getTables")
        assert op is not None
        assert op.__name__ == conMysql.getMysqlTables.__name__

    def test_get_operation_nonexistent_returns_none(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.get_operation("MySQL", "getFoo") is None

    def test_get_operation_unknown_db_returns_none(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.get_operation("NoSuchDB", "getTables") is None

    def test_execute_operation_get_tables(self):
        from common.database_registry import DatabaseRegistry
        conn   = DummyConn(rows=[(b"t1",)])
        result = DatabaseRegistry.execute_operation("MySQL", "getTables", conn,
                                                    database="mydb")
        assert isinstance(result, list)
        assert "t1" in result

    def test_execute_operation_unknown_db_returns_none(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.execute_operation("Ghost", "getTables",
                                                  DummyConn()) is None

    def test_supports_operation_true(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.supports_operation("MySQL", "getTables") is True

    def test_supports_operation_false(self):
        from common.database_registry import DatabaseRegistry
        assert DatabaseRegistry.supports_operation("MySQL", "getFoo") is False

    def test_get_display_name_mysql(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()   # trigger lazy init
        assert DatabaseRegistry.get_display_name("MySQL") == "MySQL Database"

    def test_get_display_name_unknown_returns_key(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()
        assert DatabaseRegistry.get_display_name("Ghost") == "Ghost"

    def test_get_default_port_mysql(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()   # trigger lazy init
        assert DatabaseRegistry.get_default_port("MySQL") == 3306

    def test_get_default_port_unknown_is_zero(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()
        assert DatabaseRegistry.get_default_port("Ghost") == 0

    def test_get_connection_params_mysql(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()   # trigger lazy init
        params = DatabaseRegistry.get_connection_params("MySQL")
        assert "host" in params

    def test_available_operations_excludes_internal(self):
        from common.database_registry import DatabaseRegistry
        ops      = DatabaseRegistry.get_available_operations("MySQL")
        func_set = {fn for _, fn in ops}
        assert "connectMysql"    not in func_set
        assert "disconnectMysql" not in func_set

    def test_available_operations_includes_tables(self):
        from common.database_registry import DatabaseRegistry
        DatabaseRegistry.get_all_types()   # trigger lazy init
        display = {name for name, _ in
                   DatabaseRegistry.get_available_operations("MySQL")}
        assert "Tables" in display


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 10 – schema_converter: DataTypeMapper
# ═════════════════════════════════════════════════════════════════════════════

class TestDataTypeMapper:

    def test_mysql_varchar_to_oracle(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("VARCHAR(255)", "MySQL", "Oracle")
        assert "VARCHAR2" in r and "255" in r

    def test_mysql_int_to_oracle(self):
        from schema_converter import DataTypeMapper
        assert "NUMBER" in DataTypeMapper.convert_type("INT", "MySQL", "Oracle")

    def test_mysql_int_to_postgres(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("INT", "MySQL", "PostgreSQL").upper()
        assert "INT" in r

    def test_mysql_text_to_postgres(self):
        from schema_converter import DataTypeMapper
        assert "TEXT" in DataTypeMapper.convert_type(
            "TEXT", "MySQL", "PostgreSQL").upper()

    def test_mysql_datetime_to_postgres(self):
        from schema_converter import DataTypeMapper
        assert "TIMESTAMP" in DataTypeMapper.convert_type(
            "DATETIME", "MySQL", "PostgreSQL").upper()

    def test_mysql_blob_to_postgres(self):
        from schema_converter import DataTypeMapper
        assert "BYTEA" in DataTypeMapper.convert_type(
            "BLOB", "MySQL", "PostgreSQL").upper()

    def test_mysql_bigint_to_oracle(self):
        from schema_converter import DataTypeMapper
        assert "NUMBER" in DataTypeMapper.convert_type(
            "BIGINT", "MySQL", "Oracle").upper()

    def test_oracle_varchar2_to_mysql(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("VARCHAR2(100)", "Oracle", "MySQL")
        assert "VARCHAR" in r.upper() and "100" in r

    def test_oracle_clob_to_mysql(self):
        from schema_converter import DataTypeMapper
        assert "TEXT" in DataTypeMapper.convert_type(
            "CLOB", "Oracle", "MySQL").upper()

    def test_oracle_date_to_mysql(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("DATE", "Oracle", "MySQL")
        assert r is not None and len(r) > 0

    def test_postgres_boolean_to_mysql(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("BOOLEAN", "PostgreSQL", "MySQL").upper()
        assert "TINYINT" in r or "BOOL" in r

    def test_postgres_text_to_oracle(self):
        from schema_converter import DataTypeMapper
        assert "CLOB" in DataTypeMapper.convert_type(
            "TEXT", "PostgreSQL", "Oracle").upper()

    def test_varchar_size_preserved(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("VARCHAR(512)", "MySQL", "PostgreSQL")
        assert "512" in r

    def test_decimal_precision_preserved(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("DECIMAL(10,2)", "MySQL", "PostgreSQL")
        assert "10" in r and "2" in r

    def test_mysql_to_mysql_returns_original(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("VARCHAR(50)", "MySQL", "MySQL")
        assert "VARCHAR" in r.upper() and "50" in r

    def test_mariadb_treated_same_as_mysql(self):
        from schema_converter import DataTypeMapper
        r1 = DataTypeMapper.convert_type("INT", "MySQL",   "PostgreSQL")
        r2 = DataTypeMapper.convert_type("INT", "MariaDB", "PostgreSQL")
        assert r1 == r2

    def test_unknown_type_passthrough(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type("WEIRDTYPE", "MySQL", "PostgreSQL")
        assert r is not None

    def test_bytes_input_decoded(self):
        from schema_converter import DataTypeMapper
        r = DataTypeMapper.convert_type(b"VARCHAR(100)", "MySQL", "Oracle")
        assert r is not None and "VARCHAR2" in r


# ═════════════════════════════════════════════════════════════════════════════
# SECTION 11 – MySQL INTEGRATION tests  (local MariaDB/MySQL)
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestMySQLIntegration:
    """
    Runs against the local MariaDB instance.
    All tests share the module-scoped `mysql_conn` fixture (one connection
    for the whole module).  Tests that need isolated write operations open
    their own connection or use cursors on `mysql_conn`.
    """

    # ── Connection ────────────────────────────────────────────────────────────

    def test_connect_success(self):
        import conMysql
        conn = conMysql.connectMysql(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        )
        assert conn is not None
        assert conn.is_connected()
        conn.close()

    def test_connect_wrong_password_returns_none(self):
        import conMysql
        assert conMysql.connectMysql(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password="wrong_pw",
            port=MYSQL_PORT,
        ) is None

    def test_connect_wrong_host_returns_none(self):
        import conMysql
        assert conMysql.connectMysql(
            database=MYSQL_DB,
            host="255.255.255.255",
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        ) is None

    def test_validate_connection_live(self, mysql_conn):
        from conMysql import validate_connection
        assert validate_connection(mysql_conn) is True

    def test_connect_mysql_live_success(self):
        import conMysql
        conn = conMysql.connectMysql(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        )
        assert conn is not None
        assert conn.is_connected()
        conMysql.disconnectMysql(conn)

    def test_connect_mysql_bad_credentials(self):
        import conMysql
        assert conMysql.connectMysql(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password="bad_pw",
            port=MYSQL_PORT,
        ) is None

    def test_disconnect_separate_connection(self):
        import conMysql
        conn = conMysql.connectMysql(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        )
        assert conn.is_connected()
        conMysql.disconnectMysql(conn)
        assert not conn.is_connected()

    # ── Server metadata ───────────────────────────────────────────────────────

    def test_get_version(self, mysql_conn):
        from conMysql import getMysqlVersion
        v = getMysqlVersion(mysql_conn)
        assert v is not None
        assert "mariadb" in v.lower() or "mysql" in v.lower() or v[0].isdigit()

    def test_get_databases_includes_pushdb(self, mysql_conn):
        from conMysql import getMysqlDatabases
        dbs = getMysqlDatabases(mysql_conn)
        assert isinstance(dbs, list)
        assert MYSQL_DB in dbs

    def test_get_users_includes_dheeru(self, mysql_conn):
        from conMysql import getMysqlUsers
        users = getMysqlUsers(mysql_conn)
        assert any("dheeru" in u for u in users)

    def test_is_root(self, mysql_conn):
        from conMysql import isRoot
        # dheeru has GRANT ALL PRIVILEGES ON *.* so must be root
        assert isRoot(mysql_conn) is True

    def test_get_charsets(self, mysql_conn):
        from conMysql import getMysqlCharsets
        cs = getMysqlCharsets(mysql_conn)
        assert isinstance(cs, list)
        assert "utf8mb4" in cs or "utf8" in cs

    def test_get_collations(self, mysql_conn):
        from conMysql import getMysqlCollations
        c = getMysqlCollations(mysql_conn)
        assert isinstance(c, list) and len(c) > 0

    def test_get_engines_includes_innodb(self, mysql_conn):
        from conMysql import getMysqlEngines
        engines = getMysqlEngines(mysql_conn)
        assert any("InnoDB" in e for e in engines)

    def test_get_variables(self, mysql_conn):
        from conMysql import getMysqlVariables
        variables = getMysqlVariables(mysql_conn, pattern="max_connections")
        assert any("max_connections" in v for v in variables)

    def test_get_status(self, mysql_conn):
        from conMysql import getMysqlStatus
        status = getMysqlStatus(mysql_conn, pattern="Threads%")
        assert isinstance(status, list) and len(status) > 0

    def test_get_process_list(self, mysql_conn):
        from conMysql import getMysqlProcessList
        procs = getMysqlProcessList(mysql_conn)
        assert isinstance(procs, list) and len(procs) >= 1

    # ── Schema objects ────────────────────────────────────────────────────────

    def test_get_current_database(self, mysql_conn):
        from conMysql import getCurrentDatabase
        assert getCurrentDatabase(mysql_conn) == MYSQL_DB

    def test_get_tables_returns_list(self, mysql_conn):
        from conMysql import getMysqlTables
        tables = getMysqlTables(mysql_conn, database=MYSQL_DB)
        assert isinstance(tables, list) and len(tables) > 0

    def test_get_tables_includes_known_table(self, mysql_conn):
        from conMysql import getMysqlTables
        assert TEST_TABLE in getMysqlTables(mysql_conn, database=MYSQL_DB)

    def test_get_table_schema_push_device_info(self, mysql_conn):
        from conMysql import getMysqlTableSchema
        schema = getMysqlTableSchema(mysql_conn, TEST_TABLE,
                                     database=MYSQL_DB)
        assert isinstance(schema, list) and len(schema) > 0
        col_names = [c["name"] for c in schema]
        assert "acc_device_ctrl_no" in col_names
        assert "device_id"          in col_names

    def test_get_table_schema_has_required_keys(self, mysql_conn):
        from conMysql import getMysqlTableSchema
        schema = getMysqlTableSchema(mysql_conn, TEST_TABLE,
                                     database=MYSQL_DB)
        for col in schema:
            assert "name"     in col
            assert "type"     in col
            assert "nullable" in col

    def test_get_table_columns(self, mysql_conn):
        from conMysql import getMysqlTableColumns
        cols = getMysqlTableColumns(mysql_conn, TEST_TABLE,
                                    database=MYSQL_DB)
        assert "device_id" in cols

    def test_get_indexes_includes_primary(self, mysql_conn):
        from conMysql import getMysqlIndexes
        indexes = getMysqlIndexes(mysql_conn, TEST_TABLE, database=MYSQL_DB)
        assert isinstance(indexes, list)
        assert "PRIMARY" in indexes

    def test_get_all_indexes(self, mysql_conn):
        from conMysql import getMysqlAllIndexes
        indexes = getMysqlAllIndexes(mysql_conn, database=MYSQL_DB)
        assert isinstance(indexes, list) and len(indexes) > 0

    def test_get_views(self, mysql_conn):
        from conMysql import getMysqlViews
        assert isinstance(getMysqlViews(mysql_conn, database=MYSQL_DB), list)

    def test_get_procedures(self, mysql_conn):
        from conMysql import getMysqlProcedures
        assert isinstance(getMysqlProcedures(mysql_conn, database=MYSQL_DB), list)

    def test_get_functions(self, mysql_conn):
        from conMysql import getMysqlFunctions
        assert isinstance(getMysqlFunctions(mysql_conn, database=MYSQL_DB), list)

    def test_get_triggers(self, mysql_conn):
        from conMysql import getMysqlTriggers
        assert isinstance(getMysqlTriggers(mysql_conn, database=MYSQL_DB), list)

    def test_get_events(self, mysql_conn):
        from conMysql import getMysqlEvents
        assert isinstance(getMysqlEvents(mysql_conn, database=MYSQL_DB), list)

    def test_get_constraints(self, mysql_conn):
        from conMysql import getMysqlConstraints
        constraints = getMysqlConstraints(mysql_conn, database=MYSQL_DB)
        assert isinstance(constraints, list)

    # ── CRUD on a temporary table ─────────────────────────────────────────────

    def test_crud_create_insert_select_update_delete(self, mysql_conn):
        cur = mysql_conn.cursor(buffered=True)
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
            cur.execute(
                f"CREATE TABLE `{TEST_TABLE}` "
                f"(id INT PRIMARY KEY AUTO_INCREMENT, "
                f" name VARCHAR(100), value INT)"
            )
            mysql_conn.commit()

            # INSERT two rows
            cur.execute(f"INSERT INTO `{TEST_TABLE}` (name, value) VALUES ('alpha', 10)")
            cur.execute(f"INSERT INTO `{TEST_TABLE}` (name, value) VALUES ('beta',  20)")
            mysql_conn.commit()

            # SELECT count
            cur.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE}`")
            assert cur.fetchone()[0] == 2

            # SELECT specific value
            cur.execute(f"SELECT value FROM `{TEST_TABLE}` WHERE name = 'alpha'")
            assert cur.fetchone()[0] == 10

            # UPDATE
            cur.execute(f"UPDATE `{TEST_TABLE}` SET value = 99 WHERE name = 'alpha'")
            mysql_conn.commit()
            cur.execute(f"SELECT value FROM `{TEST_TABLE}` WHERE name = 'alpha'")
            assert cur.fetchone()[0] == 99

            # DELETE
            cur.execute(f"DELETE FROM `{TEST_TABLE}` WHERE name = 'beta'")
            mysql_conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE}`")
            assert cur.fetchone()[0] == 1

        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
            mysql_conn.commit()
            cur.close()

    def test_new_table_visible_in_get_tables(self, mysql_conn):
        from conMysql import getMysqlTables
        cur = mysql_conn.cursor(buffered=True)
        tbl = f"{TEST_TABLE}_vis"
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            cur.execute(f"CREATE TABLE `{tbl}` (id INT PRIMARY KEY)")
            mysql_conn.commit()
            assert tbl in getMysqlTables(mysql_conn, database=MYSQL_DB)
        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            mysql_conn.commit()
            cur.close()

    def test_table_schema_reflects_created_columns(self, mysql_conn):
        from conMysql import getMysqlTableSchema
        cur = mysql_conn.cursor(buffered=True)
        tbl = f"{TEST_TABLE}_schema"
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            cur.execute(
                f"CREATE TABLE `{tbl}` "
                f"(pk INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(64))"
            )
            mysql_conn.commit()
            schema    = getMysqlTableSchema(mysql_conn, tbl, database=MYSQL_DB)
            col_names = [c["name"] for c in schema]
            assert "pk"    in col_names
            assert "label" in col_names
        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            mysql_conn.commit()
            cur.close()

    def test_index_visible_after_creation(self, mysql_conn):
        from conMysql import getMysqlIndexes
        cur = mysql_conn.cursor(buffered=True)
        tbl = f"{TEST_TABLE}_idx"
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            cur.execute(
                f"CREATE TABLE `{tbl}` "
                f"(id INT PRIMARY KEY, val VARCHAR(50), INDEX idx_val (val))"
            )
            mysql_conn.commit()
            indexes = getMysqlIndexes(mysql_conn, tbl, database=MYSQL_DB)
            assert "idx_val" in indexes
        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            mysql_conn.commit()
            cur.close()
