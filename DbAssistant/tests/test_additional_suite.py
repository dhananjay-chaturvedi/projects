"""
tests/test_additional_suite.py
==============================
Additional test coverage complementing test_full_suite.py.

Sections
--------
  A.  conSQLite          – full coverage via real in-memory SQLite
  B.  conPostgres        – unit tests (mocked psycopg2-style connection)
  C.  conMariadb         – unit tests (mocked) + integration (local MariaDB)
  D.  send_notification  – setup_logger, send_alert
  E.  SchemaConverter    – _ensure_str, convert_schema, generate DDL, generate indexes
  F.  DataConverter      – _convert_row_data type coercions
  G.  ConversionValidator – validate_schema_conversion, validate_data_transfer
  H.  threshold_checker  – _format_value all branches
  I.  conMysql edge cases – missing DB fallback, connectMysql default port
  J.  ConnectionManager edge cases – corrupt JSON, empty file, encrypt/decrypt None
  K.  MariaDB INTEGRATION – real queries against local MariaDB via conMariadb

Local MariaDB: host=localhost  port=3306  user=dheeru  password=dheeru  db=pushdb

Run from project root:
    pytest tests/test_additional_suite.py -v
    pytest tests/test_additional_suite.py -v -m "not integration"
"""

import types
import urllib.request

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Credentials (same local MariaDB instance)
# ─────────────────────────────────────────────────────────────────────────────
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "dheeru"
MYSQL_PASS = "dheeru"
MYSQL_DB   = "test"
TEST_TABLE = "pytest_addl_tmp"


# ═════════════════════════════════════════════════════════════════════════════
# SHARED MOCKS  (duplicated from test_full_suite so this file is self-contained)
# ═════════════════════════════════════════════════════════════════════════════

class DummyCursor:
    def __init__(self, rows=None, query_map=None):
        self._default   = rows or []
        self._query_map = {k.lower(): v for k, v in (query_map or {}).items()}
        self._current   = self._default

    def execute(self, query, params=None):
        q = (query or "").lower()
        for pat, rows in self._query_map.items():
            if pat in q:
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
    """Mock for mysql.connector-style connection (MySQL / MariaDB)."""

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

    def cursor(self, buffered=True, dictionary=False):
        if self._seq:
            return self._seq.pop(0)
        return DummyCursor(rows=self._rows, query_map=self._query_map)

    def close(self):
        self._connected = False

    def commit(self):
        pass


class PgConn:
    """Mock for psycopg2-style connection (PostgreSQL)."""

    def __init__(self, closed=0, rows=None, query_map=None, cursor_sequence=None):
        self.closed   = closed          # 0 = open, nonzero = closed
        self._rows    = rows or []
        self._qmap    = query_map or {}
        self._seq     = list(cursor_sequence) if cursor_sequence else None

    def cursor(self):
        if self._seq:
            return self._seq.pop(0)
        return DummyCursor(rows=self._rows, query_map=self._qmap)

    def close(self):
        self.closed = 1


class MockManager:
    """Minimal db-manager stub for SchemaConverter / DataConverter."""

    def __init__(self, db_type, conn=None):
        self.db_type = db_type
        self.conn    = conn


# ═════════════════════════════════════════════════════════════════════════════
# SHARED FIXTURES
# ═════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def _clear_monitoring_store():
    import monitoring_utils
    monitoring_utils._store.clear()
    yield
    monitoring_utils._store.clear()


@pytest.fixture(scope="module")
def mariadb_conn():
    """Real MariaDB connection shared across all integration tests in this module."""
    import conMariadb
    conn = conMariadb.connectMariadb(
        database=MYSQL_DB,
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        port=MYSQL_PORT,
    )
    if conn is None:
        pytest.skip("Local MariaDB not reachable – skipping integration tests")
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
    for tbl in (TEST_TABLE, f"{TEST_TABLE}_vis", f"{TEST_TABLE}_schema"):
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


# ═════════════════════════════════════════════════════════════════════════════
# SECTION A – conSQLite  (real in-memory SQLite – no mocking required)
# ═════════════════════════════════════════════════════════════════════════════

class TestSQLite:

    @pytest.fixture
    def mem_conn(self):
        """Fresh in-memory SQLite connection."""
        import conSQLite
        conn = conSQLite.connectSQLite(":memory:")
        yield conn
        conSQLite.disconnectSQLite(conn)

    @pytest.fixture
    def populated_conn(self, mem_conn):
        """In-memory SQLite with a table, view, index and trigger."""
        cur = mem_conn.cursor()
        cur.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, age INTEGER)"
        )
        cur.execute("CREATE VIEW v_adult AS SELECT * FROM users WHERE age >= 18")
        cur.execute("CREATE INDEX idx_name ON users (name)")
        cur.execute(
            "CREATE TRIGGER trg_before_insert BEFORE INSERT ON users "
            "BEGIN SELECT RAISE(IGNORE); END"
        )
        cur.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)")
        cur.execute("INSERT INTO users (name, age) VALUES ('Bob', 15)")
        mem_conn.commit()
        cur.close()
        return mem_conn

    # ── connect / disconnect ──────────────────────────────────────────────────

    def test_connect_in_memory(self):
        import conSQLite
        conn = conSQLite.connectSQLite(":memory:")
        assert conn is not None
        conSQLite.disconnectSQLite(conn)

    def test_connect_creates_directory(self, tmp_path):
        import conSQLite
        db_path = str(tmp_path / "subdir" / "test.db")
        conn = conSQLite.connectSQLite(db_path)
        assert conn is not None
        conSQLite.disconnectSQLite(conn)
        assert (tmp_path / "subdir" / "test.db").exists()

    def test_disconnect_returns_true(self, mem_conn):
        import conSQLite
        assert conSQLite.disconnectSQLite(mem_conn) is True

    def test_disconnect_none_returns_true(self):
        import conSQLite
        assert conSQLite.disconnectSQLite(None) is True

    # ── metadata ─────────────────────────────────────────────────────────────

    def test_get_version(self, mem_conn):
        import conSQLite
        v = conSQLite.getSQLiteVersion(mem_conn)
        assert v is not None
        assert "SQLite" in v

    def test_is_root_always_true(self, mem_conn):
        import conSQLite
        assert conSQLite.isRoot(mem_conn) is True

    def test_get_current_database_is_main(self, mem_conn):
        import conSQLite
        assert conSQLite.getCurrentDatabase(mem_conn) == "main"

    # ── schema objects ────────────────────────────────────────────────────────

    def test_get_tables_empty(self, mem_conn):
        import conSQLite
        assert conSQLite.getSQLiteTables(mem_conn) == []

    def test_get_tables_after_create(self, populated_conn):
        import conSQLite
        tables = conSQLite.getSQLiteTables(populated_conn)
        assert "users" in tables

    def test_get_tables_excludes_sqlite_internal(self, mem_conn):
        import conSQLite
        tables = conSQLite.getSQLiteTables(mem_conn)
        assert not any("sqlite_" in t for t in tables)

    def test_get_views_empty(self, mem_conn):
        import conSQLite
        assert conSQLite.getSQLiteViews(mem_conn) == []

    def test_get_views_after_create(self, populated_conn):
        import conSQLite
        views = conSQLite.getSQLiteViews(populated_conn)
        assert "v_adult" in views

    def test_get_indexes_empty(self, mem_conn):
        import conSQLite
        assert conSQLite.getSQLiteIndexes(mem_conn) == []

    def test_get_indexes_after_create(self, populated_conn):
        import conSQLite
        indexes = conSQLite.getSQLiteIndexes(populated_conn)
        assert "idx_name" in indexes

    def test_get_indexes_excludes_sqlite_internal(self, populated_conn):
        import conSQLite
        indexes = conSQLite.getSQLiteIndexes(populated_conn)
        assert not any("sqlite_" in i for i in indexes)

    def test_get_triggers_empty(self, mem_conn):
        import conSQLite
        assert conSQLite.getSQLiteTriggers(mem_conn) == []

    def test_get_triggers_after_create(self, populated_conn):
        import conSQLite
        triggers = conSQLite.getSQLiteTriggers(populated_conn)
        assert "trg_before_insert" in triggers

    def test_get_schemas_includes_main(self, mem_conn):
        import conSQLite
        schemas = conSQLite.getSQLiteSchemas(mem_conn)
        assert "main" in schemas

    def test_get_table_info(self, populated_conn):
        import conSQLite
        info = conSQLite.getSQLiteTableInfo(populated_conn, "users")
        assert len(info) == 3   # id, name, age
        col_names = [row[1] for row in info]
        assert "id"   in col_names
        assert "name" in col_names
        assert "age"  in col_names

    def test_get_table_schema_primary_key(self, populated_conn):
        import conSQLite
        schema = conSQLite.getSQLiteTableSchema(populated_conn, "users")
        assert len(schema) == 3
        pk_col = next(c for c in schema if c["name"] == "id")
        assert "PRIMARY KEY" in pk_col["type"]

    def test_get_table_schema_nullable(self, populated_conn):
        import conSQLite
        schema = conSQLite.getSQLiteTableSchema(populated_conn, "users")
        age_col  = next(c for c in schema if c["name"] == "age")
        name_col = next(c for c in schema if c["name"] == "name")
        assert age_col["nullable"]  is True    # no NOT NULL constraint
        assert name_col["nullable"] is False   # NOT NULL

    def test_get_table_schema_has_keys(self, populated_conn):
        import conSQLite
        for col in conSQLite.getSQLiteTableSchema(populated_conn, "users"):
            assert "name"     in col
            assert "type"     in col
            assert "nullable" in col

    def test_get_table_schema_none_conn(self):
        import conSQLite
        assert conSQLite.getSQLiteTableSchema(None, "users") == []

    def test_get_table_schema_nonexistent_table(self, mem_conn):
        import conSQLite
        result = conSQLite.getSQLiteTableSchema(mem_conn, "no_such_table")
        assert result == []

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def test_crud_insert_select_update_delete(self, mem_conn):
        cur = mem_conn.cursor()
        cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        cur.execute("INSERT INTO t VALUES (1, 'hello')")
        mem_conn.commit()

        cur.execute("SELECT val FROM t WHERE id = 1")
        assert cur.fetchone()[0] == "hello"

        cur.execute("UPDATE t SET val = 'world' WHERE id = 1")
        mem_conn.commit()
        cur.execute("SELECT val FROM t WHERE id = 1")
        assert cur.fetchone()[0] == "world"

        cur.execute("DELETE FROM t WHERE id = 1")
        mem_conn.commit()
        cur.execute("SELECT COUNT(*) FROM t")
        assert cur.fetchone()[0] == 0
        cur.close()


# ═════════════════════════════════════════════════════════════════════════════
# SECTION B – conPostgres  (mocked)
# ═════════════════════════════════════════════════════════════════════════════

class TestConPostgresUnit:

    def test_connect_success(self, monkeypatch):
        import conPostgres
        dummy = PgConn()

        monkeypatch.setattr(
            conPostgres, "psycopg2",
            types.SimpleNamespace(
                connect=lambda **kw: dummy,
                Error=conPostgres.Error,
            ),
        )
        conn = conPostgres.connectPostgres(
            database="db", host="host", user="user", password="pw", port=5432
        )
        assert conn is dummy

    def test_connect_failure_returns_none(self, monkeypatch):
        import conPostgres

        def bad_connect(**kw):
            raise conPostgres.Error("bad creds")

        monkeypatch.setattr(
            conPostgres, "psycopg2",
            types.SimpleNamespace(connect=bad_connect, Error=conPostgres.Error),
        )
        assert conPostgres.connectPostgres(
            database="db", host="h", user="u", password="bad", port=5432
        ) is None

    def test_disconnect_returns_true(self):
        import conPostgres
        conn = PgConn()
        assert conPostgres.disconnectPostgres(conn) is True

    def test_disconnect_none_returns_true(self):
        import conPostgres
        assert conPostgres.disconnectPostgres(None) is True

    def test_get_version_parses_number(self):
        import conPostgres
        conn = PgConn(rows=[("PostgreSQL 14.5 on x86_64-pc-linux-gnu",)])
        assert conPostgres.getPostgresVersion(conn) == "14.5"

    def test_is_superuser_true(self):
        import conPostgres
        conn = PgConn(rows=[(True,)])
        assert conPostgres.isSuperuser(conn) is True

    def test_is_superuser_false(self):
        import conPostgres
        conn = PgConn(rows=[(False,)])
        assert conPostgres.isSuperuser(conn) is False

    def test_get_tables(self):
        import conPostgres
        conn = PgConn(rows=[("public", "users"), ("public", "orders")])
        tables = conPostgres.getPostgresTables(conn)
        assert "public.users"  in tables
        assert "public.orders" in tables

    def test_get_views(self):
        import conPostgres
        conn = PgConn(rows=[("public", "v_summary")])
        assert "public.v_summary" in conPostgres.getPostgresViews(conn)

    def test_get_functions(self):
        import conPostgres
        conn = PgConn(rows=[("public", "fn_calc")])
        assert "public.fn_calc" in conPostgres.getPostgresFunctions(conn)

    def test_get_procedures(self):
        import conPostgres
        conn = PgConn(rows=[("public", "sp_run")])
        assert "public.sp_run" in conPostgres.getPostgresProcedures(conn)

    def test_get_triggers(self):
        import conPostgres
        conn = PgConn(rows=[("public", "trg_audit")])
        assert "public.trg_audit" in conPostgres.getPostgresTriggers(conn)

    def test_get_indexes(self):
        import conPostgres
        conn = PgConn(rows=[("public", "idx_email")])
        assert "public.idx_email" in conPostgres.getPostgresIndexes(conn)

    def test_get_sequences(self):
        import conPostgres
        conn = PgConn(rows=[("public", "users_id_seq")])
        assert "public.users_id_seq" in conPostgres.getPostgresSequences(conn)

    def test_get_constraints(self):
        import conPostgres
        conn = PgConn(rows=[("public", "users", "pk_users", "PRIMARY KEY")])
        constraints = conPostgres.getPostgresConstraints(conn)
        assert any("pk_users" in c for c in constraints)
        assert any("PRIMARY KEY" in c for c in constraints)

    def test_get_schemas(self):
        import conPostgres
        conn = PgConn(rows=[("public",), ("app",)])
        schemas = conPostgres.getPostgresSchemas(conn)
        assert "public" in schemas

    def test_get_databases(self):
        import conPostgres
        conn = PgConn(rows=[("postgres",), ("myapp",)])
        dbs = conPostgres.getPostgresDatabases(conn)
        assert "myapp" in dbs

    def test_get_users(self):
        import conPostgres
        conn = PgConn(rows=[("postgres",), ("appuser",)])
        assert "appuser" in conPostgres.getPostgresUsers(conn)

    def test_get_roles(self):
        import conPostgres
        conn = PgConn(rows=[("readonly",), ("admin",)])
        assert "readonly" in conPostgres.getPostgresRoles(conn)

    def test_get_tablespaces(self):
        import conPostgres
        conn = PgConn(rows=[("pg_default",), ("fast_ssd",)])
        assert "fast_ssd" in conPostgres.getPostgresTablespaces(conn)

    def test_get_extensions(self):
        import conPostgres
        conn = PgConn(rows=[("pgcrypto", "1.3"), ("uuid-ossp", "1.1")])
        exts = conPostgres.getPostgresExtensions(conn)
        assert any("pgcrypto" in e for e in exts)

    def test_get_activity(self):
        import conPostgres
        conn = PgConn(rows=[(42, "appuser", "psql", "127.0.0.1", "idle")])
        activities = conPostgres.getPostgresActivity(conn)
        assert len(activities) == 1
        assert "appuser" in activities[0]

    def test_get_table_schema_closed_conn_returns_empty(self):
        import conPostgres
        conn = PgConn(closed=1)
        assert conPostgres.getPostgresTableSchema(conn, "users") == []

    def test_get_table_schema_none_conn_returns_empty(self):
        import conPostgres
        assert conPostgres.getPostgresTableSchema(None, "users") == []

    def test_get_table_schema_columns_returned(self):
        import conPostgres
        # Row format: (col_name, data_type, max_length, precision, scale,
        #              is_nullable, default_val, is_pk_count)  ← 8 elements
        col_rows = [
            ("id",   "integer",           None, 32,   0,    "NO",  "nextval('users_id_seq'::regclass)", 1),
            ("name", "character varying", 100,  None, None, "YES", None,                               0),
        ]
        conn = PgConn(rows=col_rows)
        schema = conPostgres.getPostgresTableSchema(conn, "users")
        assert len(schema) == 2
        assert schema[0]["name"] == "id"
        assert "PRIMARY KEY" in schema[0]["type"]   # is_pk=1 → annotated
        assert schema[0]["nullable"] is False
        assert schema[1]["nullable"] is True

    def test_get_table_schema_schema_dot_table(self):
        import conPostgres
        # Row: (col_name, data_type, max_len, precision, scale, is_nullable, default, is_pk)
        col_rows = [("col1", "text", None, None, None, "YES", None, 0)]
        conn = PgConn(rows=col_rows)
        schema = conPostgres.getPostgresTableSchema(conn, "myschema.mytable")
        assert len(schema) == 1


# ═════════════════════════════════════════════════════════════════════════════
# SECTION C – conMariadb  (unit tests, mocked)
# ═════════════════════════════════════════════════════════════════════════════

class TestConMariadbUnit:

    def _patch_connector(self, monkeypatch, dummy_conn=None, raise_exc=None):
        import conMariadb

        class FakeConnector:
            Error = conMariadb.Error

            @staticmethod
            def connect(**kwargs):
                if raise_exc:
                    raise raise_exc
                return dummy_conn

        monkeypatch.setattr(
            conMariadb, "mysql",
            types.SimpleNamespace(connector=FakeConnector),
        )

    def test_connect_success(self, monkeypatch):
        import conMariadb
        # connectMariadb executes SELECT DATABASE() internally after connecting
        dummy = DummyConn(
            connected=True,
            cursor_sequence=[DummyCursor(rows=[("pushdb",)])],
        )
        self._patch_connector(monkeypatch, dummy_conn=dummy)
        conn = conMariadb.connectMariadb(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        )
        assert conn is dummy

    def test_connect_failure_returns_none(self, monkeypatch):
        import conMariadb
        self._patch_connector(
            monkeypatch, raise_exc=conMariadb.Error("bad creds")
        )
        assert conMariadb.connectMariadb(
            database="db", host="h", user="u", password="bad", port=3306
        ) is None

    def test_disconnect_connected(self):
        import conMariadb
        conn = DummyConn(connected=True)
        assert conMariadb.disconnectMariadb(conn) is True

    def test_disconnect_none(self):
        import conMariadb
        assert conMariadb.disconnectMariadb(None) is True

    def test_decode_value_bytearray(self):
        import conMariadb
        assert conMariadb.decode_value(bytearray(b"hello")) == "hello"

    def test_decode_value_bytes(self):
        import conMariadb
        assert conMariadb.decode_value(b"world") == "world"

    def test_decode_value_str(self):
        import conMariadb
        assert conMariadb.decode_value("text") == "text"

    def test_get_version(self):
        import conMariadb
        conn = DummyConn(rows=[("10.6.19-MariaDB",)])
        assert conMariadb.getMariadbVersion(conn) == "10.6.19-MariaDB"

    def test_is_root_all_privileges(self):
        import conMariadb
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("root@localhost",)]),
            DummyCursor(rows=[("GRANT ALL PRIVILEGES ON *.* TO `root`@`localhost`",)]),
        ])
        assert conMariadb.isRoot(conn) is True

    def test_is_root_by_username(self):
        import conMariadb
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("root@127.0.0.1",)]),
            DummyCursor(rows=[("GRANT SELECT ON *.* TO `root`@`127.0.0.1`",)]),
        ])
        assert conMariadb.isRoot(conn) is True

    def test_is_root_false(self):
        import conMariadb
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("viewer@localhost",)]),
            DummyCursor(rows=[("GRANT SELECT ON `db`.* TO `viewer`@`localhost`",)]),
        ])
        assert conMariadb.isRoot(conn) is False

    def test_get_current_database(self):
        import conMariadb
        conn = DummyConn(rows=[("pushdb",)])
        assert conMariadb.getCurrentDatabase(conn) == "pushdb"

    def test_get_current_database_none(self):
        import conMariadb
        conn = DummyConn(rows=[(None,)])
        assert conMariadb.getCurrentDatabase(conn) is None

    def test_select_database_success(self):
        import conMariadb
        conn = DummyConn(rows=[])
        assert conMariadb.selectDatabase(conn, "newdb") is True

    def test_get_tables_with_db(self):
        import conMariadb
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[("pushdb",)]),                # getCurrentDatabase
            DummyCursor(rows=[(b"users",), (b"orders",)]), # SHOW TABLES
        ])
        tables = conMariadb.getMariadbTables(conn)
        assert "users"  in tables
        assert "orders" in tables

    def test_get_tables_no_db_returns_empty(self):
        import conMariadb
        conn = DummyConn(cursor_sequence=[
            DummyCursor(rows=[(None,)]),  # getCurrentDatabase → None
        ])
        assert conMariadb.getMariadbTables(conn) == []

    def test_get_views(self):
        import conMariadb
        conn = DummyConn(rows=[(b"v_summary",)])
        assert "v_summary" in conMariadb.getMariadbViews(conn)

    def test_get_procedures(self):
        import conMariadb
        conn = DummyConn(rows=[(b"sp_run",)])
        assert "sp_run" in conMariadb.getMariadbProcedures(conn)

    def test_get_functions(self):
        import conMariadb
        conn = DummyConn(rows=[(b"fn_calc",)])
        assert "fn_calc" in conMariadb.getMariadbFunctions(conn)

    def test_get_triggers(self):
        import conMariadb
        conn = DummyConn(rows=[(b"trg_ins",)])
        assert "trg_ins" in conMariadb.getMariadbTriggers(conn)

    def test_get_all_indexes(self):
        import conMariadb
        conn = DummyConn(rows=[("users", "idx_email")])
        indexes = conMariadb.getMariadbAllIndexes(conn)
        assert any("idx_email" in i for i in indexes)

    def test_get_constraints(self):
        import conMariadb
        conn = DummyConn(rows=[("orders", "fk_user", "FOREIGN KEY")])
        constraints = conMariadb.getMariadbConstraints(conn)
        assert any("fk_user" in c for c in constraints)

    def test_get_events(self):
        import conMariadb
        conn = DummyConn(rows=[(b"daily_job",)])
        assert "daily_job" in conMariadb.getMariadbEvents(conn)

    def test_get_databases(self):
        import conMariadb
        conn = DummyConn(rows=[(b"pushdb",), (b"information_schema",)])
        assert "pushdb" in conMariadb.getMariadbDatabases(conn)

    def test_get_users(self):
        import conMariadb
        conn = DummyConn(rows=[("dheeru", "localhost")])
        assert "dheeru@localhost" in conMariadb.getMariadbUsers(conn)

    def test_get_engines(self):
        import conMariadb
        conn = DummyConn(rows=[("InnoDB", "YES")])
        engines = conMariadb.getMariadbEngines(conn)
        assert any("InnoDB" in e for e in engines)

    def test_get_charsets(self):
        import conMariadb
        conn = DummyConn(rows=[("utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", 4)])
        charsets = conMariadb.getMariadbCharsets(conn)
        assert any("utf8mb4" in c for c in charsets)

    def test_get_process_list(self):
        import conMariadb
        conn = DummyConn(rows=[(1, "dheeru", "localhost", "pushdb", "Query", 0)])
        procs = conMariadb.getMariadbProcessList(conn)
        assert len(procs) == 1
        assert "dheeru" in procs[0]

    def test_get_sequences_returns_list(self):
        import conMariadb
        conn = DummyConn(rows=[(b"seq_order_id",)])
        seqs = conMariadb.getMariadbSequences(conn)
        assert isinstance(seqs, list)

    def test_get_table_schema_invalid_conn(self):
        import conMariadb
        conn = DummyConn(connected=False)
        assert conMariadb.getMariadbTableSchema(conn, "users") == []


# ═════════════════════════════════════════════════════════════════════════════
# SECTION D – send_notification
# ═════════════════════════════════════════════════════════════════════════════

class TestSendNotification:

    def test_setup_logger_returns_logger(self, tmp_path, monkeypatch):
        import logging
        import send_notification as sn
        monkeypatch.chdir(tmp_path)
        logger = sn.setup_logger("test_logger_A")
        assert logger is not None
        assert isinstance(logger, logging.Logger)

    def test_setup_logger_idempotent(self, tmp_path, monkeypatch):
        """Calling setup_logger twice for the same name returns the same logger
        without adding duplicate handlers."""
        import send_notification as sn
        monkeypatch.chdir(tmp_path)
        log1 = sn.setup_logger("test_logger_B")
        handler_count = len(log1.handlers)
        log2 = sn.setup_logger("test_logger_B")
        assert log1 is log2
        assert len(log2.handlers) == handler_count

    def test_setup_logger_has_file_and_stream_handler(self, tmp_path, monkeypatch):
        import logging
        import send_notification as sn
        monkeypatch.chdir(tmp_path)
        logger = sn.setup_logger("test_logger_C")
        handler_types = {type(h) for h in logger.handlers}
        assert logging.FileHandler    in handler_types
        assert logging.StreamHandler  in handler_types

    def test_send_alert_no_webhook_does_not_raise(self, monkeypatch, capsys):
        import send_notification as sn
        monkeypatch.delenv("ALERT_TEAMS_WEBHOOK_URL", raising=False)
        # Should not raise even if webhook is missing
        result = sn.send_alert("Test message")
        captured = capsys.readouterr()
        assert result["ok"] is False
        assert "not set" in result["message"]
        assert "not set" in captured.err

    def test_send_alert_with_webhook_http_error_caught(self, monkeypatch, capsys):
        import send_notification as sn
        monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "http://fake.local/webhook")

        def fake_urlopen(req):
            raise OSError("connection refused")

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
        result = sn.send_alert("Error event", max_attempts=1)  # must not raise
        captured = capsys.readouterr()
        assert result["ok"] is False
        assert "connection refused" in result["message"]
        assert "connection refused" in captured.err

    def test_send_alert_with_webhook_non_200_response(self, monkeypatch, capsys):
        import send_notification as sn
        monkeypatch.setenv("ALERT_TEAMS_WEBHOOK_URL", "http://fake.local/webhook")

        class FakeResp:
            status = 503
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        monkeypatch.setattr(urllib.request, "urlopen", lambda req: FakeResp())
        result = sn.send_alert("Overload alert", max_attempts=1)
        captured = capsys.readouterr()
        assert result["ok"] is False
        assert result["status"] == 503
        assert "503" in captured.err or "503" in captured.out


# ═════════════════════════════════════════════════════════════════════════════
# SECTION E – SchemaConverter
# ═════════════════════════════════════════════════════════════════════════════

class TestSchemaConverter:

    # ── _ensure_str ───────────────────────────────────────────────────────────

    def test_ensure_str_none(self):
        from schema_converter import SchemaConverter
        assert SchemaConverter._ensure_str(None) is None

    def test_ensure_str_bytes(self):
        from schema_converter import SchemaConverter
        assert SchemaConverter._ensure_str(b"hello") == "hello"

    def test_ensure_str_string(self):
        from schema_converter import SchemaConverter
        assert SchemaConverter._ensure_str("text") == "text"

    def test_ensure_str_int(self):
        from schema_converter import SchemaConverter
        assert SchemaConverter._ensure_str(42) == "42"

    # ── generate_create_table_ddl – MySQL target ──────────────────────────────

    def _make_schema(self, table="orders", auto_increment=True):
        return {
            "table_name": table,
            "columns": [
                {"name": "id",     "type": "INT",          "nullable": False,
                 "default": None,  "auto_increment": auto_increment},
                {"name": "amount", "type": "DECIMAL(10,2)", "nullable": False,
                 "default": "0.00"},
                {"name": "note",   "type": "VARCHAR(255)",  "nullable": True,
                 "default": None},
            ],
            "primary_key": ["id"],
            "indexes":     [],
            "foreign_keys": [],
        }

    def test_generate_mysql_ddl_contains_create_table(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "CREATE TABLE orders" in ddl

    def test_generate_mysql_ddl_has_innodb(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        schema = self._make_schema()
        schema["table_engine"] = "InnoDB"
        ddl = sc.generate_create_table_ddl(schema)
        assert "ENGINE=InnoDB" in ddl

    def test_generate_mysql_ddl_auto_increment(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "AUTO_INCREMENT" in ddl

    def test_generate_mysql_ddl_primary_key(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "PRIMARY KEY (id)" in ddl

    def test_generate_mysql_ddl_not_null(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "NOT NULL" in ddl

    def test_generate_mysql_ddl_default_value(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "DEFAULT 0.00" in ddl

    # ── generate_create_table_ddl – Oracle target ─────────────────────────────

    def test_generate_oracle_ddl_no_semicolon_in_table_def(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        # The CREATE TABLE itself must not end with a semicolon
        table_part = ddl.split("CREATE SEQUENCE")[0]
        assert not table_part.rstrip().endswith(";")

    def test_generate_oracle_ddl_pk_constraint_name(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "CONSTRAINT pk_orders" in ddl

    def test_generate_oracle_ddl_sequence_for_autoincrement(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        ddl = sc.generate_all_table_ddl(self._make_schema(auto_increment=True))
        combined = "\n".join(ddl)
        assert "CREATE SEQUENCE" in combined
        assert "CREATE OR REPLACE TRIGGER" in combined

    def test_generate_oracle_ddl_no_sequence_when_no_autoincrement(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        ddl = sc.generate_create_table_ddl(self._make_schema(auto_increment=False))
        assert "CREATE SEQUENCE" not in ddl

    # ── generate_create_table_ddl – PostgreSQL target ─────────────────────────

    def test_generate_postgres_ddl_serial_for_autoincrement(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("PostgreSQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "SERIAL" in ddl

    def test_generate_postgres_ddl_bigserial_for_bigint(self):
        from schema_converter import SchemaConverter
        schema = {
            "table_name": "t",
            "columns": [{"name": "id", "type": "BIGINT", "nullable": False,
                          "default": None, "auto_increment": True}],
            "primary_key": ["id"],
            "indexes": [], "foreign_keys": [],
        }
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("PostgreSQL"))
        ddl = sc.generate_create_table_ddl(schema)
        assert "BIGSERIAL" in ddl

    def test_generate_postgres_ddl_primary_key(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("PostgreSQL"))
        ddl = sc.generate_create_table_ddl(self._make_schema())
        assert "PRIMARY KEY (id)" in ddl

    def test_generate_unknown_target_returns_none(self):
        from schema_converter import SchemaConverter
        sc  = SchemaConverter(MockManager("MySQL"), MockManager("Cassandra"))
        assert sc.generate_create_table_ddl(self._make_schema()) is None

    # ── generate_indexes_ddl ──────────────────────────────────────────────────

    def test_generate_indexes_ddl_creates_index(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        schema = {
            "table_name": "orders",
            "indexes": [
                {"name": "idx_amount", "columns": ["amount", "status"]},
            ],
        }
        ddls = sc.generate_indexes_ddl(schema)
        assert len(ddls) == 1
        assert "CREATE INDEX idx_amount ON orders (amount, status)" in ddls[0]

    def test_generate_indexes_ddl_skips_empty_columns(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("MySQL"))
        schema = {
            "table_name": "t",
            "indexes": [
                {"name": "idx_full",  "columns": ["col1"]},
                {"name": "idx_empty", "columns": []},
            ],
        }
        ddls = sc.generate_indexes_ddl(schema)
        assert len(ddls) == 1
        assert "idx_full" in ddls[0]

    # ── convert_schema ────────────────────────────────────────────────────────

    def test_convert_schema_preserves_table_name(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        src = {
            "table_name": "employees",
            "columns": [{"name": "id", "type": "INT", "nullable": False,
                          "default": None, "extra": "auto_increment"}],
            "primary_key": ["id"],
            "indexes": [], "foreign_keys": [],
        }
        converted = sc.convert_schema(src)
        assert converted["table_name"] == "employees"

    def test_convert_schema_preserves_primary_key(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("PostgreSQL"))
        src = {
            "table_name": "t",
            "columns": [{"name": "id", "type": "INT", "nullable": False,
                          "default": None, "extra": ""}],
            "primary_key": ["id", "tenant"],
            "indexes": [], "foreign_keys": [],
        }
        assert sc.convert_schema(src)["primary_key"] == ["id", "tenant"]

    def test_convert_schema_type_mapped(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("Oracle"))
        src = {
            "table_name": "t",
            "columns": [{"name": "n", "type": "VARCHAR(50)", "nullable": True,
                          "default": None, "extra": ""}],
            "primary_key": [], "indexes": [], "foreign_keys": [],
        }
        converted = sc.convert_schema(src)
        assert "VARCHAR2" in converted["columns"][0]["type"]

    def test_convert_schema_auto_increment_flag(self):
        from schema_converter import SchemaConverter
        sc = SchemaConverter(MockManager("MySQL"), MockManager("PostgreSQL"))
        src = {
            "table_name": "t",
            "columns": [{"name": "id", "type": "INT", "nullable": False,
                          "default": None, "extra": "auto_increment"}],
            "primary_key": ["id"], "indexes": [], "foreign_keys": [],
        }
        converted = sc.convert_schema(src)
        assert converted["columns"][0].get("auto_increment") is True


# ═════════════════════════════════════════════════════════════════════════════
# SECTION F – DataConverter._convert_row_data
# ═════════════════════════════════════════════════════════════════════════════

class TestDataConverterRowData:

    @pytest.fixture
    def dc_mysql_to_mysql(self):
        from schema_converter import DataConverter
        return DataConverter(MockManager("MySQL"), MockManager("MySQL"))

    @pytest.fixture
    def dc_mysql_to_oracle(self):
        from schema_converter import DataConverter
        return DataConverter(MockManager("MySQL"), MockManager("Oracle"))

    def test_none_passes_through(self, dc_mysql_to_mysql):
        row = dc_mysql_to_mysql._convert_row_data((None, None))
        assert row == (None, None)

    def test_bytes_pass_through_when_flagged_binary(self, dc_mysql_to_mysql):
        # Real transfers supply binary_flags from the cursor description; a
        # column flagged binary must be preserved byte-for-byte.
        row = dc_mysql_to_mysql._convert_row_data(
            (b"\x00\x01",), binary_flags=[True]
        )
        assert row == (b"\x00\x01",)

    def test_bytearray_preserved_when_flagged_binary(self, dc_mysql_to_mysql):
        data = bytearray(b"bin")
        row = dc_mysql_to_mysql._convert_row_data((data,), binary_flags=[True])
        assert row[0] == b"bin"

    def test_unflagged_bytes_are_decoded_as_text(self, dc_mysql_to_mysql):
        # Without a binary flag, byte strings are treated as text and decoded
        # using the conversion charset (charset-aware transfer behaviour).
        row = dc_mysql_to_mysql._convert_row_data((b"hello",))
        assert row == ("hello",)

    def test_bool_true_to_oracle_becomes_1(self, dc_mysql_to_oracle):
        row = dc_mysql_to_oracle._convert_row_data((True,))
        assert row == (1,)

    def test_bool_false_to_oracle_becomes_0(self, dc_mysql_to_oracle):
        row = dc_mysql_to_oracle._convert_row_data((False,))
        assert row == (0,)

    def test_bool_true_non_oracle_unchanged(self, dc_mysql_to_mysql):
        row = dc_mysql_to_mysql._convert_row_data((True,))
        assert row == (True,)

    def test_string_passes_through(self, dc_mysql_to_mysql):
        row = dc_mysql_to_mysql._convert_row_data(("hello",))
        assert row == ("hello",)

    def test_int_passes_through(self, dc_mysql_to_mysql):
        row = dc_mysql_to_mysql._convert_row_data((42,))
        assert row == (42,)

    def test_mixed_row(self, dc_mysql_to_oracle):
        from datetime import datetime
        dt = datetime(2026, 1, 1)
        row = dc_mysql_to_oracle._convert_row_data((1, "name", None, True, dt))
        assert row[0] == 1
        assert row[1] == "name"
        assert row[2] is None
        assert row[3] == 1          # bool True → 1 for Oracle
        assert row[4] is dt

    def test_returns_tuple(self, dc_mysql_to_mysql):
        result = dc_mysql_to_mysql._convert_row_data([1, 2, 3])
        assert isinstance(result, tuple)


# ═════════════════════════════════════════════════════════════════════════════
# SECTION E2 – DefaultValueFormatter
# ═════════════════════════════════════════════════════════════════════════════

class TestDefaultValueFormatter:

    def test_mysql_zero_datetime_is_quoted(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        result = DVF.convert_default(
            "0000-00-00 00:00:00",
            "DATETIME",
            "MySQL",
            "MariaDB",
        )
        assert result == "'0000-00-00 00:00:00'"

    def test_mysql_zero_date_is_quoted(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        result = DVF.convert_default(
            "0000-00-00",
            "DATE",
            "MySQL",
            "MariaDB",
        )
        assert result == "'0000-00-00'"

    def test_numeric_default_unquoted(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        assert DVF.convert_default("0.00", "DECIMAL(10,2)", "MySQL", "MariaDB") == "0.00"
        assert DVF.convert_default("0", "INT", "MySQL", "MariaDB") == "0"

    def test_string_default_is_quoted(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        result = DVF.convert_default("active", "VARCHAR(20)", "MySQL", "MariaDB")
        assert result == "'active'"

    def test_current_timestamp_passthrough_mysql(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        result = DVF.convert_default(
            "CURRENT_TIMESTAMP",
            "TIMESTAMP",
            "MySQL",
            "MariaDB",
        )
        assert result == "CURRENT_TIMESTAMP"

    def test_oracle_sysdate_to_mysql(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        result = DVF.convert_default(
            "SYSDATE",
            "DATE",
            "Oracle",
            "MariaDB",
        )
        assert result == "CURRENT_TIMESTAMP"

    def test_postgres_nextval_skipped(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        assert (
            DVF.convert_default(
                "nextval('users_id_seq'::regclass)",
                "INTEGER",
                "PostgreSQL",
                "MariaDB",
                auto_increment=True,
            )
            is None
        )

    def test_convert_schema_quotes_zero_datetime_in_ddl(self):
        from schema_converter import SchemaConverter

        sc = SchemaConverter(MockManager("MySQL"), MockManager("MariaDB"))
        src = {
            "table_name": "ACC_SMS_SEND_INFO",
            "columns": [
                {
                    "name": "UPDATE_DATE",
                    "type": "datetime",
                    "nullable": True,
                    "default": "0000-00-00 00:00:00",
                    "extra": "",
                }
            ],
            "primary_key": ["SMS_SEND_NO"],
            "indexes": [],
            "foreign_keys": [],
        }
        converted = sc.convert_schema(src)
        ddl = sc.generate_create_table_ddl(converted)
        assert "DEFAULT '0000-00-00 00:00:00'" in ddl
        assert "DEFAULT 0000-00-00" not in ddl

    def test_parse_mysql_on_update_extra(self):
        from schema_converter.converter import DefaultValueFormatter as DVF

        parsed = DVF.parse_mysql_extra("on update CURRENT_TIMESTAMP")
        assert parsed["on_update"] == "CURRENT_TIMESTAMP"


# ═════════════════════════════════════════════════════════════════════════════
# SECTION G – ConversionValidator
# ═════════════════════════════════════════════════════════════════════════════

class TestConversionValidator:

    @pytest.fixture
    def base_schema(self):
        return {
            "table_name": "orders",
            "columns": [
                {"name": "id",   "type": "INT"},
                {"name": "name", "type": "VARCHAR(100)"},
            ],
            "primary_key": ["id"],
        }

    def test_validate_schema_identical_returns_no_issues(self, base_schema):
        from schema_converter import ConversionValidator
        issues = ConversionValidator.validate_schema_conversion(
            base_schema, base_schema
        )
        assert issues == []

    def test_validate_schema_column_count_mismatch(self, base_schema):
        from schema_converter import ConversionValidator
        short = {
            "table_name": "orders",
            "columns": [{"name": "id", "type": "INT"}],
            "primary_key": ["id"],
        }
        issues = ConversionValidator.validate_schema_conversion(base_schema, short)
        assert any("column" in i.lower() for i in issues)

    def test_validate_schema_primary_key_mismatch(self, base_schema):
        from schema_converter import ConversionValidator
        different_pk = dict(base_schema, primary_key=["name"])
        issues = ConversionValidator.validate_schema_conversion(
            base_schema, different_pk
        )
        assert any("primary key" in i.lower() for i in issues)

    def test_validate_schema_both_issues_reported(self):
        from schema_converter import ConversionValidator
        src = {"columns": [{"name": "a"}], "primary_key": ["a"]}
        dst = {"columns": [],              "primary_key": ["b"]}
        issues = ConversionValidator.validate_schema_conversion(src, dst)
        assert len(issues) == 2

    def test_validate_data_transfer_matching_counts_returns_none(self):
        from schema_converter import ConversionValidator
        assert ConversionValidator.validate_data_transfer(100, 100) is None

    def test_validate_data_transfer_mismatch_returns_string(self):
        from schema_converter import ConversionValidator
        result = ConversionValidator.validate_data_transfer(100, 95)
        assert result is not None
        assert "100" in result and "95" in result

    def test_validate_data_transfer_zero_both(self):
        from schema_converter import ConversionValidator
        assert ConversionValidator.validate_data_transfer(0, 0) is None

    def test_validate_data_transfer_mismatch_zero_target(self):
        from schema_converter import ConversionValidator
        result = ConversionValidator.validate_data_transfer(50, 0)
        assert result is not None


# ═════════════════════════════════════════════════════════════════════════════
# SECTION G2 – SchemaComparer / DataComparer
# ═════════════════════════════════════════════════════════════════════════════

class TestSchemaComparer:

    @pytest.fixture
    def matching_schema(self):
        return {
            "table_name": "users",
            "columns": [
                {"name": "id", "type": "INT", "nullable": False},
                {"name": "name", "type": "VARCHAR(100)", "nullable": True},
            ],
            "primary_key": ["id"],
            "indexes": [{"name": "idx_name", "columns": ["name"]}],
        }

    def test_compare_tables_match(self, matching_schema, monkeypatch):
        from schema_converter import SchemaComparer

        class FakeMgr:
            db_type = "MySQL"

        src = FakeMgr()
        tgt = FakeMgr()

        def fake_get(self, table):
            return dict(matching_schema, table_name=table)

        def fake_convert(self, schema):
            return schema

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            fake_get,
        )
        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.convert_schema",
            fake_convert,
        )

        result = SchemaComparer.compare_tables(src, tgt, "users", "users")
        assert result["match"] is True
        assert result["issues"] == []

    def test_compare_tables_column_mismatch(self, matching_schema, monkeypatch):
        from schema_converter import SchemaComparer

        class FakeMgr:
            db_type = "MySQL"

        src = FakeMgr()
        tgt = FakeMgr()
        target_schema = dict(matching_schema)
        target_schema["columns"] = [
            {"name": "id", "type": "INT", "nullable": False},
        ]

        call = {"n": 0}

        def fake_get(self, table):
            call["n"] += 1
            return matching_schema if call["n"] == 1 else target_schema

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            fake_get,
        )
        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.convert_schema",
            lambda self, s: s,
        )

        result = SchemaComparer.compare_tables(src, tgt, "users", "users")
        assert result["match"] is False
        assert any("missing" in i.lower() for i in result["issues"])

    def test_compare_tables_source_missing(self, monkeypatch):
        from schema_converter import SchemaComparer

        class FakeMgr:
            db_type = "MySQL"

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            lambda self, table: None,
        )

        result = SchemaComparer.compare_tables(FakeMgr(), FakeMgr(), "nope", "nope")
        assert result["match"] is False
        assert "not found" in result["error"].lower()

    def test_compare_tables_index_columns_as_dicts(self, matching_schema, monkeypatch):
        from schema_converter import SchemaComparer

        class FakeMgr:
            db_type = "MySQL"

        src = FakeMgr()
        tgt = FakeMgr()
        schema_with_dict_indexes = dict(matching_schema)
        schema_with_dict_indexes["indexes"] = [
            {
                "name": "idx_name",
                "columns": [{"name": "name", "order": "ASC"}],
            }
        ]

        def fake_get(self, table):
            return schema_with_dict_indexes

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            fake_get,
        )
        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.convert_schema",
            lambda self, s: s,
        )

        result = SchemaComparer.compare_tables(src, tgt, "users", "users")
        assert result["match"] is True


class TestDataComparer:

    def test_normalize_cell_handles_binary_and_datetime(self):
        from schema_converter.converter import _normalize_cell
        from datetime import datetime

        assert _normalize_cell(None) is None
        assert _normalize_cell(b"abc") == ("__binary__", 3)
        dt = datetime(2024, 1, 2, 3, 4, 5, 999)
        assert _normalize_cell(dt) == "2024-01-02 03:04:05"
        assert _normalize_cell(True) == 1
        assert _normalize_cell(False) == 0

    def test_compare_table_data_sample_match(self, monkeypatch):
        from schema_converter import DataComparer

        schema = {
            "table_name": "users",
            "columns": [
                {"name": "id", "type": "INT", "nullable": False},
                {"name": "name", "type": "VARCHAR(50)", "nullable": True},
            ],
            "primary_key": ["id"],
            "indexes": [],
        }

        class FakeMgr:
            db_type = "MySQL"
            conn = object()

        src = FakeMgr()
        tgt = FakeMgr()

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            lambda self, table: schema,
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataConverter.get_row_count",
            lambda self, table, is_source=True: 2,
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataComparer._fetch_rows",
            staticmethod(
                lambda manager, table, col_list, order_by, limit, offset: [
                    (1, "alice"),
                    (2, "bob"),
                ][:limit]
            ),
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataConverter._convert_row_data",
            lambda self, row: row,
        )

        comparer = DataComparer(src, tgt)
        result = comparer.compare_table_data("users", "users", mode="sample", sample_size=2)
        assert result["match"] is True
        assert result["rows_compared"] == 2

    def test_compare_table_data_detects_mismatch(self, monkeypatch):
        from schema_converter import DataComparer

        schema = {
            "table_name": "users",
            "columns": [{"name": "id", "type": "INT", "nullable": False}],
            "primary_key": ["id"],
            "indexes": [],
        }

        class FakeMgr:
            db_type = "MySQL"
            conn = object()

        rows_by_side = {"src": [(1,)], "tgt": [(2,)]}

        def fake_fetch(manager, table, col_list, order_by, limit, offset):
            key = "src" if manager is src else "tgt"
            return rows_by_side[key][:limit]

        src = FakeMgr()
        tgt = FakeMgr()

        monkeypatch.setattr(
            "schema_converter.converter.SchemaConverter.get_table_schema",
            lambda self, table: schema,
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataConverter.get_row_count",
            lambda self, table, is_source=True: 1,
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataComparer._fetch_rows",
            staticmethod(fake_fetch),
        )
        monkeypatch.setattr(
            "schema_converter.converter.DataConverter._convert_row_data",
            lambda self, row: row,
        )

        comparer = DataComparer(src, tgt)
        result = comparer.compare_table_data("users", "users", mode="sample", sample_size=1)
        assert result["match"] is False
        assert len(result["mismatched_rows"]) == 1


class TestSchemaCompareService:

    def test_get_compare_sample_size_reads_properties(self, monkeypatch, tmp_path):
        from common.config_loader import get_compare_sample_size, ConfigLoader

        ini = tmp_path / "properties.ini"
        ini.write_text(
            "[schema.conversion]\ncompare_sample_size = 25\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "common.config_loader.properties",
            ConfigLoader(str(ini)),
        )
        assert get_compare_sample_size() == 25

    def test_service_compare_schema_delegates(self, monkeypatch):
        from schema_converter.service import SchemaService

        class FakeMgr:
            db_type = "MySQL"

        svc = SchemaService(connect=lambda name: FakeMgr())
        monkeypatch.setattr(
            "schema_converter.service.SchemaComparer.compare_tables",
            lambda *a, **k: {"match": True, "error": None, "issues": []},
        )
        r = svc.compare_schema("a", "b", "users")
        assert r["match"] is True
        assert r["error"] is None

    def test_service_compare_data_delegates(self, monkeypatch):
        from schema_converter.service import SchemaService

        class FakeMgr:
            db_type = "MySQL"

        svc = SchemaService(connect=lambda name: FakeMgr())

        class FakeComparer:
            def __init__(self, *a):
                pass

            def compare_table_data(self, *a, **k):
                return {"match": False, "error": None, "rows_compared": 10}

        monkeypatch.setattr(
            "schema_converter.service.DataComparer",
            FakeComparer,
        )
        r = svc.compare_data("a", "b", "users", mode="sample")
        assert r["match"] is False
        assert r["rows_compared"] == 10


# ═════════════════════════════════════════════════════════════════════════════
# SECTION H – threshold_checker._format_value  (all branches)
# ═════════════════════════════════════════════════════════════════════════════

class TestFormatValue:

    def _fmt(self, value, unit):
        from monitoring.threshold_checker import _format_value
        return _format_value(value, unit)

    # ── bytes ─────────────────────────────────────────────────────────────────

    def test_bytes_above_1gb(self):
        r = self._fmt(5 * 1024 ** 3, "bytes")
        assert "GB" in r and "5.00" in r

    def test_bytes_between_1mb_and_1gb(self):
        r = self._fmt(512 * 1024 ** 2, "bytes")
        assert "MB" in r

    def test_bytes_below_1mb(self):
        r = self._fmt(500, "bytes")
        assert "B" in r and "500" in r

    # ── bytes/sec ─────────────────────────────────────────────────────────────

    def test_bytes_per_sec(self):
        r = self._fmt(10 * 1024 ** 2, "bytes/sec")
        assert "MB/s" in r

    # ── seconds ───────────────────────────────────────────────────────────────

    def test_seconds_below_1_shown_as_ms(self):
        r = self._fmt(0.1, "seconds")
        assert "ms" in r and "100" in r

    def test_seconds_above_1_shown_as_s(self):
        r = self._fmt(5.0, "seconds")
        assert "s" in r and "5" in r

    def test_second_singular_unit(self):
        r = self._fmt(0.05, "second")
        assert "ms" in r

    # ── percent ───────────────────────────────────────────────────────────────

    def test_percent(self):
        r = self._fmt(85.5, "percent")
        assert "%" in r and "85.5" in r

    # ── ratio ─────────────────────────────────────────────────────────────────

    def test_ratio_three_decimal_places(self):
        r = self._fmt(0.9, "ratio")
        assert "0.900" in r

    # ── MB / GB ───────────────────────────────────────────────────────────────

    def test_mb_unit(self):
        r = self._fmt(512, "MB")
        assert "MB" in r and "512" in r

    def test_gb_unit(self):
        r = self._fmt(2.5, "GB")
        assert "GB" in r and "2.50" in r

    # ── generic ───────────────────────────────────────────────────────────────

    def test_whole_number_no_decimal(self):
        r = self._fmt(400.0, "count")
        assert r == "400"

    def test_decimal_two_dp(self):
        r = self._fmt(12.34, "iops")
        assert "12.34" in r


# ═════════════════════════════════════════════════════════════════════════════
# SECTION I – conMysql edge cases
# ═════════════════════════════════════════════════════════════════════════════

class TestConMysqlEdgeCases:

    def test_get_tables_no_db_falls_to_information_schema(self):
        """When SELECT DATABASE() returns None, getMysqlTables falls back to
        querying information_schema and still returns a list."""
        from conMysql import getMysqlTables
        conn = DummyConn(query_map={
            "select database()":    [(None,)],
            "information_schema":   [(b"table_a",), (b"table_b",)],
        })
        tables = getMysqlTables(conn)
        assert isinstance(tables, list)

    def test_get_tables_db_error_returns_empty(self):
        """If the cursor raises a mysql Error, getMysqlTables returns []."""
        import conMysql
        from conMysql import getMysqlTables

        class ErrorConn(DummyConn):
            def cursor(self, buffered=True):
                raise conMysql.Error("DB down")

        assert getMysqlTables(ErrorConn()) == []

    def test_connect_uses_default_port_when_none(self, monkeypatch):
        """connectMysql reads the default port from config when port=None."""
        import conMysql
        called_with = {}

        class FakeConnector:
            Error = conMysql.Error
            @staticmethod
            def connect(**kwargs):
                called_with.update(kwargs)
                return DummyConn(connected=True)

        monkeypatch.setattr(
            conMysql, "mysql",
            types.SimpleNamespace(connector=FakeConnector),
        )
        conMysql.connectMysql(
            database="db", host="h", user="u", password="p", port=None
        )
        assert called_with.get("port") == 3306    # default from config

    def test_get_databases_error_returns_empty(self):
        import conMysql
        from conMysql import getMysqlDatabases

        class ErrorConn(DummyConn):
            def cursor(self, buffered=True):
                raise conMysql.Error("gone")

        assert getMysqlDatabases(ErrorConn()) == []

    def test_get_version_error_returns_none(self):
        import conMysql
        from conMysql import getMysqlVersion

        class ErrorConn(DummyConn):
            def cursor(self, buffered=True):
                raise conMysql.Error("gone")

        assert getMysqlVersion(ErrorConn()) is None

    def test_is_root_error_returns_false(self):
        import conMysql
        from conMysql import isRoot

        class ErrorConn(DummyConn):
            def cursor(self, buffered=True):
                raise conMysql.Error("gone")

        assert isRoot(ErrorConn()) is False

    def test_get_table_columns_error_returns_empty(self):
        import conMysql
        from conMysql import getMysqlTableColumns

        class ErrorConn(DummyConn):
            def cursor(self, buffered=True):
                raise conMysql.Error("gone")

        assert getMysqlTableColumns(ErrorConn(), "t") == []

    def test_disconnect_always_true_on_closed(self):
        from conMysql import disconnectMysql
        conn = DummyConn(connected=True)
        conn.close()
        assert disconnectMysql(conn) is True


# ═════════════════════════════════════════════════════════════════════════════
# SECTION J – ConnectionManager edge cases
# ═════════════════════════════════════════════════════════════════════════════

class TestConnectionManagerEdgeCases:

    @pytest.fixture
    def tmp_mgr(self, tmp_path, monkeypatch):
        import common.connection_manager as cm
        from common import paths as _paths

        monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
        _paths.reset_bootstrap_state_for_tests()
        _paths.ensure_layout()
        return cm.ConnectionManager()

    def test_load_from_corrupted_json_returns_empty(self, tmp_path, monkeypatch):
        import common.connection_manager as cm
        from common import paths as _paths

        monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path))
        _paths.reset_bootstrap_state_for_tests()
        _paths.ensure_layout()
        # Pollute the canonical db.json with invalid JSON before construction.
        _paths.db_connections_path().write_text("{{ not valid json")
        mgr = cm.ConnectionManager()
        assert mgr.load_connections() == []

    def test_encrypt_empty_password_returns_none(self, tmp_mgr):
        assert tmp_mgr._encrypt_password("") is None

    def test_encrypt_none_password_returns_none(self, tmp_mgr):
        assert tmp_mgr._encrypt_password(None) is None

    def test_decrypt_none_returns_none(self, tmp_mgr):
        assert tmp_mgr._decrypt_password(None) is None

    def test_encrypt_decrypt_roundtrip(self, tmp_mgr):
        encrypted = tmp_mgr._encrypt_password("my_secret")
        assert encrypted != "my_secret"
        assert tmp_mgr._decrypt_password(encrypted) == "my_secret"

    def test_get_all_empty_initially(self, tmp_mgr):
        assert tmp_mgr.get_all_connections() == []

    def test_add_multiple_connections_ordered(self, tmp_mgr):
        from common.connection_params import ConnectionParams

        def params(name):
            return ConnectionParams.from_mapping({
                "name": name, "db_type": "MySQL", "host": "h",
                "port": 3306, "service_or_db": "d",
                "username": "u", "password": "p",
            })

        tmp_mgr.add_connection(params("first"))
        tmp_mgr.add_connection(params("second"))
        tmp_mgr.add_connection(params("third"))
        names = [c["name"] for c in tmp_mgr.get_all_connections()]
        assert names == ["first", "second", "third"]


# ═════════════════════════════════════════════════════════════════════════════
# SECTION K – MariaDB INTEGRATION  (local MariaDB via conMariadb)
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestMariaDBIntegration:
    """
    Integration tests against the local MariaDB instance using conMariadb.py.
    All tests share the module-scoped `mariadb_conn` fixture.
    """

    def test_connect_success(self):
        import conMariadb
        conn = conMariadb.connectMariadb(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            port=MYSQL_PORT,
        )
        assert conn is not None
        assert conn.is_connected()
        conMariadb.disconnectMariadb(conn)

    def test_connect_wrong_password_returns_none(self):
        import conMariadb
        assert conMariadb.connectMariadb(
            database=MYSQL_DB,
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password="wrong_pw",
            port=MYSQL_PORT,
        ) is None

    def test_get_version(self, mariadb_conn):
        import conMariadb
        v = conMariadb.getMariadbVersion(mariadb_conn)
        assert v is not None
        assert "mariadb" in v.lower() or v[0].isdigit()

    def test_is_root(self, mariadb_conn):
        import conMariadb
        assert conMariadb.isRoot(mariadb_conn) is True

    def test_get_current_database(self, mariadb_conn):
        import conMariadb
        assert conMariadb.getCurrentDatabase(mariadb_conn) == MYSQL_DB

    def test_select_database(self, mariadb_conn):
        import conMariadb
        assert conMariadb.selectDatabase(mariadb_conn, MYSQL_DB) is True

    def test_get_databases_includes_pushdb(self, mariadb_conn):
        import conMariadb
        dbs = conMariadb.getMariadbDatabases(mariadb_conn)
        assert MYSQL_DB in dbs

    def test_get_tables_returns_list(self, mariadb_conn):
        import conMariadb
        tables = conMariadb.getMariadbTables(mariadb_conn)
        assert isinstance(tables, list) and len(tables) > 0

    def test_get_tables_includes_known_table(self, mariadb_conn):
        import conMariadb
        assert TEST_TABLE in conMariadb.getMariadbTables(mariadb_conn)

    def test_get_views(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbViews(mariadb_conn), list)

    def test_get_procedures(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbProcedures(mariadb_conn), list)

    def test_get_functions(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbFunctions(mariadb_conn), list)

    def test_get_triggers(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbTriggers(mariadb_conn), list)

    def test_get_all_indexes(self, mariadb_conn):
        import conMariadb
        indexes = conMariadb.getMariadbAllIndexes(mariadb_conn)
        assert isinstance(indexes, list) and len(indexes) > 0

    def test_get_constraints(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbConstraints(mariadb_conn), list)

    def test_get_events(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbEvents(mariadb_conn), list)

    def test_get_users(self, mariadb_conn):
        import conMariadb
        users = conMariadb.getMariadbUsers(mariadb_conn)
        assert any("dheeru" in u for u in users)

    def test_get_engines_includes_innodb(self, mariadb_conn):
        import conMariadb
        engines = conMariadb.getMariadbEngines(mariadb_conn)
        assert any("InnoDB" in e for e in engines)

    def test_get_charsets(self, mariadb_conn):
        import conMariadb
        charsets = conMariadb.getMariadbCharsets(mariadb_conn)
        assert isinstance(charsets, list) and len(charsets) > 0

    def test_get_process_list(self, mariadb_conn):
        import conMariadb
        procs = conMariadb.getMariadbProcessList(mariadb_conn)
        assert isinstance(procs, list) and len(procs) >= 1

    def test_get_sequences(self, mariadb_conn):
        import conMariadb
        assert isinstance(conMariadb.getMariadbSequences(mariadb_conn), list)

    def test_get_table_schema_push_device_info(self, mariadb_conn):
        import conMariadb
        schema = conMariadb.getMariadbTableSchema(
            mariadb_conn, TEST_TABLE, database=MYSQL_DB
        )
        assert isinstance(schema, list) and len(schema) > 0
        col_names = [c["name"] for c in schema]
        assert "acc_device_ctrl_no" in col_names
        assert "device_id"          in col_names

    def test_table_schema_has_required_keys(self, mariadb_conn):
        import conMariadb
        schema = conMariadb.getMariadbTableSchema(
            mariadb_conn, TEST_TABLE, database=MYSQL_DB
        )
        for col in schema:
            assert "name"     in col
            assert "type"     in col
            assert "nullable" in col

    def test_crud_on_temp_table(self, mariadb_conn):
        cur = mariadb_conn.cursor(buffered=True)
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
            cur.execute(
                f"CREATE TABLE `{TEST_TABLE}` "
                f"(id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(80))"
            )
            mariadb_conn.commit()

            cur.execute(f"INSERT INTO `{TEST_TABLE}` (label) VALUES ('alpha')")
            cur.execute(f"INSERT INTO `{TEST_TABLE}` (label) VALUES ('beta')")
            mariadb_conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE}`")
            assert cur.fetchone()[0] == 2

            cur.execute(f"UPDATE `{TEST_TABLE}` SET label = 'gamma' WHERE label = 'alpha'")
            mariadb_conn.commit()
            cur.execute(f"SELECT label FROM `{TEST_TABLE}` WHERE id = 1")
            assert cur.fetchone()[0] == "gamma"

            cur.execute(f"DELETE FROM `{TEST_TABLE}` WHERE label = 'beta'")
            mariadb_conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE}`")
            assert cur.fetchone()[0] == 1

        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{TEST_TABLE}`")
            mariadb_conn.commit()
            cur.close()

    def test_new_table_visible_via_get_tables(self, mariadb_conn):
        import conMariadb
        cur = mariadb_conn.cursor(buffered=True)
        tbl = f"{TEST_TABLE}_vis"
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            cur.execute(f"CREATE TABLE `{tbl}` (id INT PRIMARY KEY)")
            mariadb_conn.commit()
            assert tbl in conMariadb.getMariadbTables(mariadb_conn)
        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            mariadb_conn.commit()
            cur.close()

    def test_schema_reflects_created_columns(self, mariadb_conn):
        import conMariadb
        cur = mariadb_conn.cursor(buffered=True)
        tbl = f"{TEST_TABLE}_schema"
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            cur.execute(
                f"CREATE TABLE `{tbl}` "
                f"(pk INT PRIMARY KEY AUTO_INCREMENT, tag VARCHAR(64))"
            )
            mariadb_conn.commit()
            schema    = conMariadb.getMariadbTableSchema(
                mariadb_conn, tbl, database=MYSQL_DB
            )
            col_names = [c["name"] for c in schema]
            assert "pk"  in col_names
            assert "tag" in col_names
        finally:
            cur.execute(f"DROP TABLE IF EXISTS `{tbl}`")
            mariadb_conn.commit()
            cur.close()
