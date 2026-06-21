"""
db_manager.py
=============
Standalone DatabaseManager — no tkinter dependency.

Extracted from conDbUi.py so it can be imported in headless environments
(CLI, REST API, daemon) without requiring a display or python3-tk.

conDbUi.py imports this module and re-exports DatabaseManager unchanged,
so all existing GUI code continues to work without modification.
"""

import sys
import threading
import time

from common.autocommit import get_autocommit
from common.database_registry import DatabaseRegistry
from common.config_loader import properties, console_debug, console_print
from common.ssl_connect import ssl_connect_kwargs
from common.sql_splitter import (
    looks_like_procedural_block,
    split_sql_statements,
    strip_sql_comments,
)


class DatabaseManager:
    """Unified database manager for all database types using registry."""

    def __init__(self, db_type):
        self.db_type = db_type
        self.conn = None
        self._last_connect_params = None
        self._ssh_tunnel = None
        # Serializes statements/transaction ops on this physical session so a
        # single DatabaseManager shared between threads (e.g. a SQL Editor tab
        # and the Objects browser, or a migration) cannot interleave cursors and
        # corrupt the connection. Reentrant so execute_query -> reconnect ->
        # connect within one thread does not self-deadlock. NOTE: cancel_query
        # intentionally does NOT take this lock — it must run concurrently with
        # the in-flight statement it is cancelling.
        self._lock = threading.RLock()
        self.config = DatabaseRegistry.get_config(db_type)
        if not self.config:
            raise ValueError(f"Unsupported database type: {db_type}")

    @property
    def lock(self):
        """The per-session reentrant lock (held while a statement runs).

        External callers that reach into ``manager.conn`` directly (e.g. raw
        cursors in data migration) should acquire this to stay consistent with
        :meth:`execute_query`.
        """
        return self._lock

    @property
    def capabilities(self):
        return DatabaseRegistry.get_capabilities(self.db_type)

    # Default connect timeout in seconds. Can be overridden via kwargs or env.
    DEFAULT_CONNECT_TIMEOUT = 30

    def connect(self, **kwargs):
        """Connect to database using registry.

        When ``kwargs`` carries an ``ssh_tunnel`` block (see
        :mod:`common.ssh_tunnel`), an SSH local port-forward is opened first and
        the driver is pointed at ``127.0.0.1:<local_port>`` instead of the
        original host/port. The tunnel is owned by this manager and closed in
        :meth:`disconnect`.

        ``connect_timeout`` (seconds, default 30) limits how long the driver
        call blocks.  Pass 0 to disable the timeout (e.g. for local SQLite).
        """
        import concurrent.futures as _cf
        self._last_connect_params = dict(kwargs)
        connect_func = DatabaseRegistry.get_operation(self.db_type, "connect")
        if not connect_func:
            raise NotImplementedError(
                f"Connect operation not available for {self.db_type}"
            )

        if "connect_timeout" in kwargs:
            timeout = float(kwargs.pop("connect_timeout"))
        else:
            from common.config_loader import config
            timeout = config.get_float(
                "database.connection", "connection_timeout",
                default=self.DEFAULT_CONNECT_TIMEOUT)
        port = int(kwargs.get("port", self.config["default_port"]))
        host = kwargs.get("host")
        host, port = self._apply_ssh_tunnel(kwargs.get("ssh_tunnel"), host, port)
        ssl_kw = ssl_connect_kwargs(kwargs)

        # Integer timeout for drivers that accept it natively (psycopg2,
        # mysql.connector).  0 means "no timeout" in those drivers.
        _native_timeout = max(1, int(timeout)) if timeout > 0 else 0

        def _do_connect():
            if self.db_type == "Oracle":
                return connect_func(
                    db=kwargs.get("service"),
                    host=host,
                    user=kwargs.get("username"),
                    password=kwargs.get("password"),
                    port=port,
                    **ssl_kw,
                )
            elif self.db_type in ("MongoDB", "DocumentDB"):
                tls = kwargs.get("tls")
                if self.db_type == "DocumentDB":
                    tls = True
                return connect_func(
                    database=kwargs.get("database"),
                    host=host,
                    user=kwargs.get("username"),
                    password=kwargs.get("password"),
                    port=port,
                    tls=tls,
                    tls_ca_file=kwargs.get("tls_ca_file"),
                    auth_source=kwargs.get("auth_source"),
                )
            else:
                return connect_func(
                    database=kwargs.get("database"),
                    host=host,
                    user=kwargs.get("username"),
                    password=kwargs.get("password"),
                    port=port,
                    connect_timeout=_native_timeout,
                    **ssl_kw,
                )

        # SQLite connects to a local file — no network, no timeout needed.
        # Other in-process engines likewise don't need the thread wrapper.
        local_db = self.db_type in ("SQLite",)
        if timeout <= 0 or local_db:
            self.conn = _do_connect()
        else:
            # Do NOT use ThreadPoolExecutor as a context manager: its __exit__
            # calls shutdown(wait=True), which blocks until the worker thread
            # finishes — even after fut.result(timeout=…) raises TimeoutError.
            # For a hanging TCP connect that means another 60-90s wait while
            # the OS times out. Use shutdown(wait=False) so the caller gets
            # control back immediately; the worker thread is a daemon and will
            # be cleaned up when the process exits.
            pool = _cf.ThreadPoolExecutor(max_workers=1)
            fut = pool.submit(_do_connect)
            try:
                self.conn = fut.result(timeout=timeout)
            except _cf.TimeoutError:
                raise TimeoutError(
                    f"Connection to {self.db_type} timed out after {timeout}s."
                )
            finally:
                pool.shutdown(wait=False)
        return self.conn

    def _apply_ssh_tunnel(self, ssh_tunnel, host, port):
        """Open (or reuse) an SSH tunnel and return the effective host/port.

        Reuses an already-open tunnel so reconnect attempts don't spawn a new
        forward each time. Returns the original host/port unchanged when no
        tunnel is configured.
        """
        from common.ssh_tunnel import normalize_tunnel_config, open_tunnel_from_config

        clean = normalize_tunnel_config(ssh_tunnel)
        if clean is None:
            return host, port

        if self._ssh_tunnel is not None and self._ssh_tunnel.is_open:
            return self._ssh_tunnel.local_host, self._ssh_tunnel.local_port

        self._ssh_tunnel = open_tunnel_from_config(
            clean, host or "127.0.0.1", port
        )
        return self._ssh_tunnel.local_host, self._ssh_tunnel.local_port

    def _close_db_handle(self):
        """Close just the DB connection, leaving any SSH tunnel intact."""
        if self.conn:
            try:
                disconnect_func = DatabaseRegistry.get_operation(self.db_type, "disconnect")
                if disconnect_func:
                    disconnect_func(self.conn)
            finally:
                self.conn = None

    def disconnect(self):
        """Disconnect from database and tear down any SSH tunnel."""
        try:
            self._close_db_handle()
        finally:
            # Treat the handle as unusable even if the driver raised while
            # closing. Keeping a half-closed object around causes worse
            # follow-up failures.
            self.conn = None
            if self._ssh_tunnel is not None:
                try:
                    self._ssh_tunnel.close()
                finally:
                    self._ssh_tunnel = None

    def ping_or_reconnect(self) -> bool:
        """Validate the current connection, reconnecting once when needed."""
        ping_func = DatabaseRegistry.get_operation(self.db_type, "ping")
        if self.conn and ping_func:
            try:
                if ping_func(self.conn):
                    return True
            except Exception as ping_err:
                console_debug(f"{self.db_type} ping failed: {ping_err}")

        return self._reconnect_with_saved_params()

    def _reconnect_with_saved_params(self) -> bool:
        """Reconnect using the last successful connect() parameters."""
        if not self._last_connect_params:
            return False

        # Tunnel-backed connections keep the existing SSH forward alive; the
        # driver-specific reconnect helpers would point at the original
        # host/port, so reopen the DB handle through connect() (which reuses
        # the live tunnel and rewrites host/port to the local end).
        if self._ssh_tunnel is not None and self._ssh_tunnel.is_open:
            self._close_db_handle()
            return self.connect(**self._last_connect_params) is not None

        reconnect_func = DatabaseRegistry.get_operation(self.db_type, "reconnect")
        if reconnect_func:
            try:
                self.conn = self._call_reconnect_func(reconnect_func)
                return self.conn is not None
            except Exception as reconnect_err:
                console_debug(
                    f"{self.db_type} reconnect operation failed: {reconnect_err}"
                )

        try:
            self.disconnect()
        except Exception as exc:
            console_debug(f"{self.db_type} disconnect before reconnect failed: {exc}")
        return self.connect(**self._last_connect_params) is not None

    def _call_reconnect_func(self, reconnect_func):
        """Call a driver reconnect function using saved public params."""
        params = dict(self._last_connect_params or {})
        ssl_kw = ssl_connect_kwargs(params)
        if self.db_type == "Oracle":
            return reconnect_func(
                self.conn,
                db=params.get("service"),
                host=params.get("host"),
                user=params.get("username"),
                password=params.get("password"),
                port=params.get("port", self.config["default_port"]),
                **ssl_kw,
            )
        if self.db_type == "SQLite":
            return reconnect_func(
                self.conn,
                database=params.get("database"),
            )
        if self.db_type in ("MongoDB", "DocumentDB"):
            tls = params.get("tls")
            if self.db_type == "DocumentDB":
                tls = True
            return reconnect_func(
                self.conn,
                database=params.get("database"),
                host=params.get("host"),
                user=params.get("username"),
                password=params.get("password"),
                port=params.get("port", self.config["default_port"]),
                tls=tls,
                tls_ca_file=params.get("tls_ca_file"),
                auth_source=params.get("auth_source"),
            )
        return reconnect_func(
            self.conn,
            database=params.get("database"),
            host=params.get("host"),
            user=params.get("username"),
            password=params.get("password"),
            port=params.get("port", self.config["default_port"]),
            **ssl_kw,
        )

    def get_version(self):
        """Get database version using registry."""
        if not self.conn:
            return None
        with self._lock:
            return DatabaseRegistry.execute_operation(
                self.db_type, "getVersion", self.conn
            )

    def is_admin(self):
        """Check if user has admin privileges using registry."""
        if not self.conn:
            return False
        with self._lock:
            return (
                DatabaseRegistry.execute_operation(self.db_type, "isRoot", self.conn)
                or False
            )

    def execute_operation(self, operation_func_name):
        """Execute a database operation by function name."""
        if not self.conn:
            return []
        module = self.config.get("module")
        if not module:
            return []
        func = getattr(module, operation_func_name, None)
        if func:
            with self._lock:
                return func(self.conn)
        return []

    def execute_document_query(self, query_text: str):
        """Execute a JSON document query (MongoDB / DocumentDB)."""
        if not self.conn:
            return None, "Not connected to database"
        func = DatabaseRegistry.get_operation(self.db_type, "executeDocumentQuery")
        if not func:
            return None, f"Document queries are not supported for {self.db_type}"
        with self._lock:
            start = time.perf_counter()
            result, error = func(self.conn, query_text)
        if error:
            return None, error
        if result is not None:
            result["time"] = time.perf_counter() - start
        return result, None

    def execute_query(self, sql):
        """Execute a SQL or document query and return (result, error)."""
        caps = self.capabilities
        if caps.supports_document_query and caps.query_language == "document":
            return self.execute_document_query(sql)

        if not self.conn:
            return None, "Not connected to database"

        with self._lock:
            return self._execute_query_locked(sql, caps)

    def _execute_query_locked(self, sql, caps):
        try:
            if not isinstance(sql, str):
                return None, "SQL must be a string."
            sql = sql.strip()
            if not sql:
                return None, "SQL cannot be empty."
            statements = self._split_sql_statements(sql)
            if not statements:
                return None, "SQL cannot be empty."

            if not caps.supports_multi_statement and len(statements) > 1:
                return None, "Multiple statements are not supported for this database type."

            if len(statements) == 1:
                return self._execute_single_statement(statements[0])

            results_list = []
            start_time = time.perf_counter()

            for i, stmt in enumerate(statements, 1):
                stmt = stmt.strip()
                if not stmt:
                    continue
                result, error = self._execute_single_statement(stmt)
                if error:
                    stmt_preview = stmt[:300] + ("..." if len(stmt) > 300 else "")
                    return (
                        None,
                        f"Error in statement {i} of {len(statements)}:\n{error}\n\nFull statement:\n{stmt_preview}",
                    )
                if result:
                    result["statement_num"] = i
                    sql_preview_limit = properties.get_int(
                        "ui.limits", "sql_preview_limit", default=100
                    )
                    result["statement"] = stmt[:sql_preview_limit] + (
                        "..." if len(stmt) > sql_preview_limit else ""
                    )
                results_list.append(result)

            execution_time = time.perf_counter() - start_time
            return {
                "multiple_results": True,
                "results": results_list,
                "time": execution_time,
                "count": len(statements),
            }, None

        except Exception as e:
            return None, str(e)

    def _split_sql_statements(self, sql):
        """Split SQL by semicolons while respecting strings/comments/bodies."""
        return split_sql_statements(sql)

    def _looks_like_procedural_block(self, sql: str) -> bool:
        return looks_like_procedural_block(sql)

    def _strip_sql_comments(self, sql: str) -> str:
        """Remove SQL comments while preserving string literals."""
        return strip_sql_comments(sql)

    def _execute_single_statement(self, sql):
        """Execute a single SQL statement."""
        cursor = None
        try:
            sql = sql.strip()

            sql_without_comments = self._strip_sql_comments(sql).strip()

            if not sql_without_comments:
                return {"message": "Comment-only statement skipped.", "time": 0}, None

            if self.db_type == "Oracle":
                sql = sql.rstrip(";").strip()

            if not sql:
                return {"message": "Empty statement skipped.", "time": 0}, None

            if self.db_type in ["MySQL", "MariaDB"]:
                cursor = self.conn.cursor(buffered=True)
            else:
                cursor = self.conn.cursor()

            start_time = time.perf_counter()
            cursor.execute(sql)

            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows, truncated, max_rows = self._fetch_rows(cursor)
                execution_time = time.perf_counter() - start_time
                return {
                    "columns": columns,
                    "rows": rows,
                    "time": execution_time,
                    "rowcount": len(rows),
                    "truncated": truncated,
                    "max_rows": max_rows,
                }, None
            else:
                execution_time = time.perf_counter() - start_time
                rowcount = cursor.rowcount
                is_autocommit = get_autocommit(self.conn, self.db_type)
                commit_status = (
                    " (auto-committed)" if is_autocommit else " (use Commit button)"
                )
                return {
                    "message": f"Query executed successfully. {rowcount} row(s) affected{commit_status}.",
                    "time": execution_time,
                }, None

        except Exception as e:
            sql_error_limit = properties.get_int(
                "ui.limits", "sql_error_limit", default=500
            )
            sql_preview = sql[:sql_error_limit] + (
                "..." if len(sql) > sql_error_limit else ""
            )
            return None, f"{str(e)}\n\nSQL attempted:\n{sql_preview}"
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def _fetch_rows(self, cursor):
        """Fetch rows with a configurable cap to avoid unbounded memory use."""
        max_rows = properties.get_int("ui.limits", "query_result_max_rows", default=10000)
        if max_rows <= 0:
            return cursor.fetchall(), False, max_rows
        fetchmany = getattr(cursor, "fetchmany", None)
        rows = fetchmany(max_rows + 1) if callable(fetchmany) else cursor.fetchall()
        if not isinstance(rows, (list, tuple)):
            # Some unit-test mocks expose ``fetchmany`` dynamically but don't
            # configure it. Fall back to the older fetchall path in that case.
            rows = cursor.fetchall()
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
        return rows, truncated, max_rows

    def commit(self):
        """Commit transaction."""
        if self.conn and self.capabilities.supports_transactions:
            with self._lock:
                self.conn.commit()
            return True
        return False

    def rollback(self):
        """Rollback transaction."""
        if self.conn and self.capabilities.supports_transactions:
            with self._lock:
                self.conn.rollback()
            return True
        return False

    def cancel_query(self):
        """Cancel the currently executing query."""
        if not self.conn:
            return False
        try:
            if self.db_type == "Oracle":
                if hasattr(self.conn, "cancel"):
                    self.conn.cancel()
                    console_print("Oracle query cancellation requested")
                    return True

            elif self.db_type in ["MySQL", "MariaDB"]:
                cursor = None
                kill_cursor = None
                try:
                    cursor = self.conn.cursor(buffered=True)
                    cursor.execute("SELECT CONNECTION_ID()")
                    connection_id = int(cursor.fetchone()[0])
                    kill_cursor = self.conn.cursor(buffered=True)
                    kill_cursor.execute(f"KILL QUERY {connection_id}")
                    console_print(
                        f"{self.db_type} query cancellation requested (killed query on connection {connection_id})"
                    )
                    return True
                except Exception as e:
                    print(f"Error cancelling {self.db_type} query: {e}", file=sys.stderr)
                    return False
                finally:
                    for cur in (cursor, kill_cursor):
                        if cur:
                            try:
                                cur.close()
                            except Exception:
                                pass

            elif self.db_type == "PostgreSQL":
                cursor = None
                cancel_cursor = None
                try:
                    cursor = self.conn.cursor()
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid = int(cursor.fetchone()[0])
                    cancel_cursor = self.conn.cursor()
                    cancel_cursor.execute(f"SELECT pg_cancel_backend({backend_pid})")
                    console_print(
                        f"PostgreSQL query cancellation requested (cancelled backend PID {backend_pid})"
                    )
                    return True
                except Exception as e:
                    print(f"Error cancelling PostgreSQL query: {e}", file=sys.stderr)
                    return False
                finally:
                    for cur in (cursor, cancel_cursor):
                        if cur:
                            try:
                                cur.close()
                            except Exception:
                                pass

            elif self.db_type == "SQLite":
                if hasattr(self.conn, "interrupt"):
                    self.conn.interrupt()
                    console_print("SQLite query interruption requested")
                    return True
                else:
                    console_print("SQLite doesn't support query cancellation")
                    return False

            else:
                console_print(f"Query cancellation not implemented for {self.db_type}")
                return False

        except Exception as e:
            print(f"Error cancelling query: {e}", file=sys.stderr)
            return False
