"""
common/headless/db_service.py
=============================
Core database service — connections, query, objects, schema introspection.

No module-specific code (monitoring, AI, schema conversion). Ship this with
every module build alongside ``common/`` drivers and registry.
"""

from __future__ import annotations

import threading
import time
import re

from common.autocommit import get_autocommit as read_autocommit
from common.autocommit import set_autocommit as write_autocommit
from common.connection_params import ConnectionParams
from common.db_manager import DatabaseManager
from common.connection_manager import ConnectionManager
from common.database_registry import DatabaseRegistry
from common.headless.cloud_service import CloudServiceMixin
from common.sql_splitter import split_sql_statements


_ERR_CANNOT_CONNECT = (
    "Could not connect to '{name}' ({db_type}). "
    "Verify the server is reachable and the host/port/credentials "
    "are correct, then try again."
)


class CoreDBService(CloudServiceMixin):
    """Thread-safe core service: connections, SQL, object browser, cloud."""

    def __init__(self, connection_manager=None) -> None:
        # ``connection_manager`` is an injection point so callers can back the
        # service with an alternate store (e.g. the Monitor tab's isolated
        # ``monitor_db.json``) while reusing all connection/SQL logic unchanged.
        self._cm = connection_manager or ConnectionManager()
        self._active: dict[str, DatabaseManager] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def _lock(self, name: str) -> threading.Lock:
        with self._meta_lock:
            if name not in self._locks:
                self._locks[name] = threading.Lock()
            return self._locks[name]

    def _conn_kwargs(self, profile: dict) -> dict:
        """Map a connection profile dict to DatabaseManager.connect() kwargs."""
        kwargs = {
            "host":     profile.get("host", ""),
            "username": profile.get("username", profile.get("user", "")),
            "password": profile.get("password", ""),
            "port":     profile.get("port", ""),
            "database": profile.get("service_or_db", profile.get("database", "")),
            "service":  profile.get("service_or_db", profile.get("database", "")),
        }
        for key in (
            "ssl_mode", "ssl_ca", "ssl_cert", "ssl_key", "wallet_location",
            "tls", "tls_ca_file",
        ):
            if profile.get(key) not in (None, ""):
                kwargs[key] = profile.get(key)
        if profile.get("ssh_tunnel"):
            kwargs["ssh_tunnel"] = profile.get("ssh_tunnel")
        return kwargs

    def _merge_saved_with_form(self, name: str, form: dict) -> dict:
        """Merge a saved profile with inline form values (Tk Test/Connect flow).

        Form-supplied password always wins when non-empty; otherwise the stored
        password is kept so edit-without-retyping still works.
        """
        saved = dict(self._cm.get_connection(name) or {})
        merged = dict(saved)
        merged.update({
            "name": name,
            "db_type": form.get("db_type", saved.get("db_type", "")),
            "host": form.get("host", saved.get("host", "")),
            "port": str(form.get("port", saved.get("port", ""))),
            "username": form.get("user", saved.get("username", "")),
            "service_or_db": form.get("service") or form.get("database")
                               or saved.get("service_or_db", ""),
        })
        pw = form.get("password", "")
        if pw:
            merged["password"] = pw
        elif saved.get("password"):
            merged["password"] = saved["password"]
        if form.get("ssh_tunnel") is not None:
            merged["ssh_tunnel"] = form.get("ssh_tunnel")
        for key in (
            "ssl_mode", "ssl_ca", "ssl_cert", "ssl_key", "wallet_location",
            "tls", "tls_ca_file",
        ):
            if form.get(key) not in (None, ""):
                merged[key] = form.get(key)
        return merged

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def list_connections(self) -> list[dict]:
        """Return all saved connection profiles (passwords omitted)."""
        conns = self._cm.get_all_connections()
        safe = []
        for c in conns:
            row = dict(c)
            row.pop("password", None)
            safe.append(row)
        return safe

    def add_connection(
        self,
        params: ConnectionParams,
    ) -> dict:
        """Save a new connection profile. Returns {ok, message}.

        Pass ``ssh_tunnel`` (a dict with ``ssh_host``/``ssh_user`` and optional
        ``ssh_port``/``ssh_password``/``ssh_key_file``) to register a remote
        connection that reaches the database through an SSH tunnel.

        SSL/TLS parameters mirror the desktop connection form:
        ``ssl_mode``/``ssl_ca``/``ssl_cert``/``ssl_key`` for SQL engines,
        ``wallet_location`` for Oracle, and ``tls``/``tls_ca_file`` for
        MongoDB/DocumentDB.
        """
        save_pw = (
            bool(params.password)
            if params.save_password is None
            else bool(params.save_password)
        )
        params = ConnectionParams(
            **{**params.__dict__, "save_password": save_pw}
        )
        try:
            ok, message = self._cm.add_connection(
                params,
            )
            return {
                "ok": bool(ok),
                "message": message if message else f"Connection '{params.name}' saved.",
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def update_connection(
        self,
        old_name: str,
        params: ConnectionParams,
    ) -> dict:
        """Update an existing connection profile in place. Returns {ok, message}.

        Mirrors the desktop "Save" on an already-saved connection. If the saved
        profile has a stored password and *password* is left blank, the existing
        password is preserved so editing other fields does not wipe credentials.
        """
        existing = self._cm.get_connection(old_name)
        if not existing:
            return {"ok": False, "message": f"Connection '{old_name}' not found."}
        # Preserve a previously-saved password when the form leaves it blank.
        password = params.password
        if not password and existing.get("password"):
            password = existing.get("password", "")
        save_password = params.save_password
        if save_password is None:
            save_password = bool(existing.get("save_password", bool(password)))
        params = ConnectionParams(
            **{
                **params.__dict__,
                "password": password,
                "save_password": bool(save_password),
            }
        )
        # Drop any live session for the old name so the next open uses new params.
        self.disconnect(old_name)
        try:
            ok, message = self._cm.update_connection(
                old_name=old_name,
                params=params,
            )
            return {
                "ok": bool(ok),
                "message": message if message else f"Connection '{params.name}' updated.",
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def connection_metadata(self) -> dict:
        """Capability metadata for building connection forms (UI-agnostic).

        Returns ``{db_types: [...], engines: {db_type: {...}}}`` where each
        engine entry carries ``default_port``, ``service_label`` (Service name
        vs Database name), ``is_document`` (MongoDB/DocumentDB), and SSL
        capability flags (``supports_ssl``, ``ssl_mode_options``,
        ``ssl_fields``). This lets the web/TUI render the same capability-driven
        SSL/TLS fields as the desktop form.
        """
        from common.database_registry import DatabaseRegistry

        types = DatabaseRegistry.get_all_types()
        engines: dict[str, dict] = {}
        for db_type in types:
            try:
                default_port = DatabaseRegistry.get_default_port(db_type)
            except Exception:
                default_port = 0
            is_document = db_type in ("MongoDB", "DocumentDB")
            entry = {
                "default_port": default_port or "",
                "service_label": "Service name" if db_type == "Oracle"
                                 else "Database name",
                "is_document": is_document,
                "supports_ssl": False,
                "ssl_mode_options": [],
                "ssl_fields": [],
                "tls_default": db_type == "DocumentDB",
            }
            if is_document:
                entry["supports_tls"] = True
            else:
                try:
                    caps = DatabaseRegistry.get_capabilities(db_type)
                    entry["supports_ssl"] = bool(getattr(caps, "supports_ssl", False))
                    entry["ssl_mode_options"] = list(getattr(caps, "ssl_mode_options", ()) or ())
                    entry["ssl_fields"] = list(getattr(caps, "ssl_fields", ()) or ())
                except Exception:
                    pass
            engines[db_type] = entry
        return {"db_types": types, "engines": engines}

    def remove_connection(self, name: str) -> dict:
        """Remove a saved connection profile. Returns {ok, message}."""
        self.disconnect(name)
        try:
            ok, message = self._cm.delete_connection(name)
            return {"ok": ok, "message": message}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def test_connection(self, name: str) -> dict:
        """
        Open (or reuse) a connection, retrieve version, then leave it open.
        Returns {ok, latency_ms, version, message}.
        """
        profile = self._cm.get_connection(name)
        if not profile:
            return {"ok": False, "latency_ms": None, "version": None,
                    "message": f"Connection '{name}' not found."}
        return self._test_profile(profile, name=name)

    def test_connection_inline(
        self,
        params: ConnectionParams,
    ) -> dict:
        """Test using form parameters without requiring a saved password.

        Mirrors the Tk desktop ``Test Connection`` button: uses the values the
        user typed, disconnects when done, and does not require the profile to
        exist on disk.
        """
        return self._test_profile(params.to_profile(include_password=True), name=params.name, leave_open=False)

    def _test_profile(
        self, profile: dict, *, name: str, leave_open: bool = True,
    ) -> dict:
        db_type = profile.get("db_type", "")
        t0 = time.perf_counter()
        mgr: DatabaseManager | None = None
        try:
            mgr = DatabaseManager(db_type)
            mgr.connect(**self._conn_kwargs(profile))
            if getattr(mgr, "conn", None) is None:
                raise ConnectionError(
                    _ERR_CANNOT_CONNECT.format(name=name, db_type=db_type)
                )
            latency = round((time.perf_counter() - t0) * 1000, 1)
            version = mgr.get_version() or "unknown"
            if leave_open and name:
                with self._lock(name):
                    self._active[name] = mgr
                    mgr = None  # do not disconnect in finally
            return {"ok": True, "latency_ms": latency, "version": str(version),
                    "message": f"Connected to {db_type} in {latency} ms"}
        except Exception as exc:
            latency = round((time.perf_counter() - t0) * 1000, 1)
            return {"ok": False, "latency_ms": latency, "version": None,
                    "message": str(exc)}
        finally:
            if mgr is not None:
                try:
                    mgr.disconnect()
                except Exception:
                    pass

    def _get_or_connect(self, name: str, profile: dict | None = None) -> DatabaseManager:
        """Return cached DatabaseManager or create a fresh one.

        The network connect() call runs outside the lock to avoid holding it
        for an unbounded round-trip time (deadlock risk and serialisation).
        A double-check after the connect ensures only one manager wins.
        """
        with self._lock(name):
            if name in self._active and self._active[name].conn is not None:
                return self._active[name]
            if profile is None:
                profile = self._cm.get_connection(name)
            if not profile:
                raise ValueError(f"Connection '{name}' not found.")
            db_type = profile["db_type"]
            conn_kwargs = self._conn_kwargs(profile)

        # Connect outside the lock — network I/O must not block other threads.
        mgr = DatabaseManager(db_type)
        mgr.connect(**conn_kwargs)
        if getattr(mgr, "conn", None) is None:
            raise ConnectionError(
                _ERR_CANNOT_CONNECT.format(name=name, db_type=db_type)
            )

        with self._lock(name):
            # Another thread may have connected while we were outside the lock.
            if name in self._active and self._active[name].conn is not None:
                mgr.disconnect()  # discard our redundant connection
                return self._active[name]
            self._active[name] = mgr
            return mgr

    def get_manager(self, name: str, profile: dict | None = None) -> DatabaseManager:
        """Public accessor: return a connected DatabaseManager for *name*.

        Used by optional modules (e.g. schema converter) that need a live
        connection resolved through the shared core.
        """
        return self._get_or_connect(name, profile)

    def open_session(self, name: str, profile: dict | None = None) -> DatabaseManager:
        """Open a fresh, uncached DB session for concurrent worker use.

        Unlike :meth:`get_manager`, this does not store the manager in
        ``_active``. Callers own the returned session and must disconnect it.
        """
        if profile is None:
            profile = self._cm.get_connection(name)
        if not profile:
            raise ValueError(f"Connection '{name}' not found.")
        db_type = profile["db_type"]
        mgr = DatabaseManager(db_type)
        mgr.connect(**self._conn_kwargs(profile))
        if getattr(mgr, "conn", None) is None:
            raise ConnectionError(
                _ERR_CANNOT_CONNECT.format(name=name, db_type=db_type)
            )
        return mgr

    def get_connection_profile(self, name: str) -> dict | None:
        """Return a saved connection profile dict, or None."""
        return self._cm.get_connection(name)

    def connection_lock(self, name: str) -> threading.Lock:
        """Per-connection lock for serializing access from module services."""
        return self._lock(name)

    def disconnect(self, name: str):
        """Close and remove a cached connection."""
        with self._lock(name):
            mgr = self._active.pop(name, None)
            if mgr:
                try:
                    mgr.disconnect()
                except Exception:
                    pass

    def disconnect_all(self):
        """Close all cached connections."""
        for name in list(self._active.keys()):
            self.disconnect(name)

    # ------------------------------------------------------------------
    # Active-connection lifecycle (parity with the Connections tab)
    # ------------------------------------------------------------------

    def open_connection(self, name: str, form: dict | None = None) -> dict:
        """Open (or reuse) a cached connection and keep it warm.

        When *form* is supplied (Web/TUI Connect from the connection form),
        merge inline field values — especially password — with the saved
        profile so ``save_password=false`` still connects with what the user
        typed. Mirrors the Tk desktop Connect behaviour.
        """
        profile = self._cm.get_connection(name)
        if not profile:
            return {"ok": False, "message": f"Connection '{name}' not found.",
                    "db_type": "", "host": "", "version": None}
        if form:
            self.disconnect(name)
            profile = self._merge_saved_with_form(name, form)
        db_type = profile.get("db_type", "")
        host = profile.get("host", "")
        try:
            mgr = self._get_or_connect(name, profile)
            version = None
            try:
                version = mgr.get_version()
            except Exception:
                pass
            return {
                "ok": True,
                "message": f"Connection '{name}' is open.",
                "db_type": db_type,
                "host": host,
                "version": str(version) if version else None,
            }
        except Exception as exc:
            return {"ok": False, "message": str(exc), "db_type": db_type,
                    "host": host, "version": None}

    def close_connection(self, name: str) -> dict:
        """Close one cached connection. Returns ``{ok, message}``."""
        with self._lock(name):
            mgr = self._active.pop(name, None)
        if not mgr:
            return {"ok": False,
                    "message": f"Connection '{name}' is not currently active."}
        try:
            mgr.disconnect()
            return {"ok": True, "message": f"Connection '{name}' closed."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def close_all_connections(self) -> dict:
        """Close every cached connection. Returns ``{ok, closed, message}``."""
        names = list(self._active.keys())
        if not names:
            return {"ok": True, "closed": [],
                    "message": "No active connections."}
        errors: list[str] = []
        for name in names:
            try:
                self.disconnect(name)
            except Exception as exc:
                errors.append(f"{name}: {exc}")
        if errors:
            return {"ok": False, "closed": [n for n in names if n not in self._active],
                    "message": "Some connections failed to close: " + "; ".join(errors)}
        return {"ok": True, "closed": names,
                "message": f"Closed {len(names)} connection(s)."}

    def list_active_connections(self) -> list[dict]:
        """Return one row per currently-cached connection.

        Each row carries ``name``, ``db_type``, ``host``, ``port``,
        ``service_or_db``, ``username``, ``connected`` (the live state of
        the underlying driver connection).
        """
        out: list[dict] = []
        with self._meta_lock:
            active_items = list(self._active.items())
        # Pre-index saved profiles once (O(n)) so the per-connection lookup is O(1).
        profiles: dict[str, dict] = {
            c["name"]: c
            for c in self._cm.connections
            if isinstance(c, dict) and c.get("name")
        }
        for name, mgr in active_items:
            profile = profiles.get(name) or {}
            out.append({
                "name": name,
                "db_type": getattr(mgr, "db_type", "")
                           or profile.get("db_type", ""),
                "host": profile.get("host", ""),
                "port": profile.get("port", ""),
                "service_or_db": profile.get("service_or_db", ""),
                "username": profile.get("username", ""),
                "connected": getattr(mgr, "conn", None) is not None,
            })
        return out

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, name: str, sql: str) -> dict:
        """
        Execute SQL and return a normalized result dict:
          {columns, rows, rowcount, time_ms, message, error, multiple_results}
        """
        try:
            mgr = self._get_or_connect(name)
        except Exception as exc:
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0,
                    "time_ms": 0, "message": None}

        with self._lock(name):
            try:
                raw, error = mgr.execute_query(sql)
            except Exception as exc:
                return {
                    "error": str(exc),
                    "columns": [],
                    "rows": [],
                    "rowcount": 0,
                    "time_ms": 0,
                    "message": None,
                }

        if error:
            return {"error": error, "columns": [], "rows": [], "rowcount": 0,
                    "time_ms": 0, "message": None}

        if raw is None:
            return {"error": "No result", "columns": [], "rows": [], "rowcount": 0,
                    "time_ms": 0, "message": None}

        # Multiple statements
        if raw.get("multiple_results"):
            return {
                "error": None,
                "multiple_results": True,
                "results": raw.get("results", []),
                "time_ms": round(raw.get("time", 0) * 1000, 1),
                "count": raw.get("count", 0),
                "message": f"{raw.get('count',0)} statement(s) executed.",
                "columns": [],
                "rows": [],
                "rowcount": 0,
            }

        # SELECT
        if "columns" in raw:
            rows = []
            for row in (raw.get("rows") or []):
                rows.append([str(v) if v is not None else "" for v in row])
            return {
                "error": None,
                "columns": raw["columns"],
                "rows": rows,
                "rowcount": raw.get("rowcount", len(rows)),
                "time_ms": round(raw.get("time", 0) * 1000, 1),
                "message": f"{raw.get('rowcount', len(rows))} row(s) returned.",
                "truncated": bool(raw.get("truncated", False)),
                "max_rows": raw.get("max_rows"),
            }

        # DML / DDL
        return {
            "error": None,
            "columns": [],
            "rows": [],
            "rowcount": 0,
            "time_ms": round(raw.get("time", 0) * 1000, 1),
            "message": raw.get("message", "OK"),
        }

    # ------------------------------------------------------------------
    # Query power tools (transactions, autocommit, cancel, multi-statement)
    # ------------------------------------------------------------------

    @staticmethod
    def _split_sql_statements(sql: str) -> list[str]:
        """Split a multi-statement SQL blob on ``;`` safely."""
        return split_sql_statements(sql)

    def execute_multi(self, name: str, sql: str) -> dict:
        """Split *sql* on ``;`` and execute each statement serially.

        Returns ``{error, count, results: [{statement, result}]}`` where each
        ``result`` has the same shape as :meth:`execute` output. Stops at the
        first failure unless every statement succeeds; the partial ``results``
        list reflects what ran.
        """
        statements = self._split_sql_statements(sql or "")
        if not statements:
            return {"error": "No statements to execute.", "count": 0, "results": []}
        results: list[dict] = []
        for stmt in statements:
            res = self.execute(name, stmt)
            results.append({"statement": stmt, "result": res})
            if res.get("error"):
                return {"error": res["error"], "count": len(results),
                        "results": results}
        return {"error": None, "count": len(results), "results": results}

    def format_sql(self, sql: str) -> dict:
        """Pretty-print SQL (keyword upper-case, reindented).

        UI-agnostic equivalent of the desktop "Format SQL" action so the web
        and TUI editors format identically. Returns ``{ok, sql, message}``.
        """
        text = sql or ""
        try:
            import sqlparse
            formatted = sqlparse.format(
                text, reindent=True, keyword_case="upper",
                identifier_case=None, strip_comments=False,
            )
            return {"ok": True, "sql": formatted, "message": "Formatted."}
        except Exception as exc:
            return {"ok": False, "sql": text, "message": str(exc)}

    def cancel_query(self, name: str) -> dict:
        """Attempt to cancel the currently running query on *name*."""
        with self._lock(name):
            mgr = self._active.get(name)
            if not mgr:
                return {"ok": False, "message": f"Connection '{name}' is not active."}
            try:
                ok = bool(mgr.cancel_query())
                return {"ok": ok,
                        "message": "Cancel signal sent." if ok else "Could not cancel."}
            except Exception as exc:
                return {"ok": False, "message": str(exc)}

    def get_autocommit(self, name: str) -> dict:
        """Return ``{ok, autocommit, message}`` for the live connection."""
        with self._lock(name):
            mgr = self._active.get(name)
            if not mgr or not getattr(mgr, "conn", None):
                return {"ok": False, "autocommit": None,
                        "message": f"Connection '{name}' is not active."}
            try:
                ac = read_autocommit(mgr.conn, getattr(mgr, "db_type", ""))
                return {"ok": True, "autocommit": ac, "message": ""}
            except Exception as exc:
                return {"ok": False, "autocommit": None, "message": str(exc)}

    def set_autocommit(self, name: str, enabled: bool) -> dict:
        """Toggle autocommit on the live connection."""
        with self._lock(name):
            mgr = self._active.get(name)
            if not mgr or not getattr(mgr, "conn", None):
                return {"ok": False, "message": f"Connection '{name}' is not active."}
            try:
                db_type = getattr(mgr, "db_type", "")
                if not getattr(mgr.capabilities, "supports_transactions", True):
                    return {"ok": False,
                            "message": f"{db_type} does not support transactions."}
                write_autocommit(mgr.conn, db_type, enabled)
                return {"ok": True,
                        "message": f"Autocommit {'enabled' if enabled else 'disabled'}."}
            except Exception as exc:
                return {"ok": False, "message": str(exc)}

    def commit(self, name: str) -> dict:
        """Commit the current transaction on *name*."""
        with self._lock(name):
            mgr = self._active.get(name)
            if not mgr or not getattr(mgr, "conn", None):
                return {"ok": False, "message": f"Connection '{name}' is not active."}
            try:
                mgr.commit()
                return {"ok": True, "message": "Transaction committed."}
            except Exception as exc:
                return {"ok": False, "message": str(exc)}

    def rollback(self, name: str) -> dict:
        """Roll back the current transaction on *name*."""
        with self._lock(name):
            mgr = self._active.get(name)
            if not mgr or not getattr(mgr, "conn", None):
                return {"ok": False, "message": f"Connection '{name}' is not active."}
            try:
                mgr.rollback()
                return {"ok": True, "message": "Transaction rolled back."}
            except Exception as exc:
                return {"ok": False, "message": str(exc)}

    # ------------------------------------------------------------------
    # Database objects
    # ------------------------------------------------------------------

    # CLI/object-type alias -> registry operation KEY (not the driver function
    # name). DatabaseRegistry.execute_operation() resolves the key to the
    # engine-specific function and returns None when the engine does not
    # support it. Covers the full set of browsable object types the drivers
    # register (see database_registry.get_available_operations display map).
    _OBJ_OP_MAP = {
        "tables":             "getTables",
        "collections":        "getTables",
        "views":              "getViews",
        "procs":              "getProcedures",
        "procedures":         "getProcedures",
        "functions":          "getFunctions",
        "indexes":            "getIndexes",
        "triggers":           "getTriggers",
        "sequences":          "getSequences",
        "constraints":        "getConstraints",
        "events":             "getEvents",
        "databases":          "getDatabases",
        "users":              "getUsers",
        "schemas":            "getSchemas",
        "tablespaces":        "getTablespaces",
        "engines":            "getEngines",
        "charsets":           "getCharsets",
        "processlist":        "getProcessList",
        "roles":              "getRoles",
        "extensions":         "getExtensions",
        "synonyms":           "getSynonyms",
        "packages":           "getPackages",
        "types":              "getTypes",
        "materializedviews":  "getMaterializedViews",
        "databaselinks":      "getDatabaseLinks",
        "profiles":           "getProfiles",
        "sessions":           "getSessions",
        "activity":           "getActivity",
    }

    @classmethod
    def supported_object_types(cls) -> list[str]:
        """Return the sorted list of object-type aliases the CLI/API accept."""
        return sorted(cls._OBJ_OP_MAP)

    def get_objects(self, name: str, obj_type: str = "tables") -> list:
        """
        Return a list of database objects for *obj_type*.

        *obj_type* is one of the aliases in ``_OBJ_OP_MAP`` (tables, views,
        procs, functions, indexes, triggers, sequences, constraints, events,
        databases, users, schemas, tablespaces, engines, charsets,
        processlist, roles, extensions, synonyms, packages, types,
        materializedviews, databaselinks, profiles, sessions, activity).

        Rows are returned as plain strings for single-column results, or as
        tuples/lists for multi-column results (e.g. processlist, users).
        Returns ``[{"error": ...}]`` for unknown or unsupported types.
        """
        key = obj_type.lower().replace(" ", "").replace("_", "")
        op = self._OBJ_OP_MAP.get(key)
        if not op:
            return [{"error": f"Unknown object type '{obj_type}'. "
                              f"Supported: {', '.join(self.supported_object_types())}"}]
        try:
            mgr = self._get_or_connect(name)
            db_type = getattr(mgr, "db_type", "")
            if not DatabaseRegistry.supports_operation(db_type, op):
                return [{"error": f"'{obj_type}' is not supported for {db_type}."}]
            with self._lock(name):
                result = DatabaseRegistry.execute_operation(db_type, op, mgr.conn)
            if isinstance(result, (list, tuple)):
                # Flatten single-column tuples to plain strings
                flat = []
                for row in result:
                    flat.append(row[0] if isinstance(row, (list, tuple)) and len(row) == 1 else row)
                return flat
            return []
        except Exception as exc:
            return [{"error": str(exc)}]


    # ------------------------------------------------------------------
    # Table tooling (sample, count, export, import) — parity with the
    # Database Objects tab.
    # ------------------------------------------------------------------

    @staticmethod
    def _quote_table_for(db_type: str, table_name: str) -> str:
        """Engine-specific identifier quoting (mirrors the UI helper)."""
        parts = [p for p in str(table_name).split(".") if p]
        if db_type == "SQLite":
            return ".".join(f'"{p.replace(chr(34), chr(34) * 2)}"' for p in parts)
        if db_type == "SQLServer":
            return ".".join(f"[{p.replace(']', ']]')}]" for p in parts)
        if db_type in ("MySQL", "MariaDB"):
            return ".".join(f"`{p.replace('`', '``')}`" for p in parts)
        return ".".join(f'"{p.replace(chr(34), chr(34) * 2)}"' for p in parts)

    @staticmethod
    def _apply_row_limit(db_type: str, sql: str, limit: int) -> str:
        if db_type in ("MySQL", "MariaDB", "PostgreSQL", "SQLite"):
            return f"{sql} LIMIT {int(limit)}"
        if db_type == "SQLServer":
            return sql.replace("SELECT *", f"SELECT TOP {int(limit)} *", 1)
        if db_type == "Oracle":
            return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {int(limit)}"
        return sql

    def sample_table(self, name: str, table: str, limit: int | None = None) -> dict:
        """Return up to *limit* sample rows for *table*.

        Returns ``{error, table, columns, rows, rowcount}``. Limit is clamped
        to a reasonable maximum to avoid accidental full-table reads.
        """
        from common.config_loader import config
        default_rows = config.get_int(
            "database.performance", "sample_default_rows", default=5)
        max_rows = config.get_int(
            "database.performance", "sample_max_rows", default=1000)
        if limit is None:
            limit = default_rows
        try:
            limit = max(1, int(limit or 1))
        except Exception:
            limit = default_rows
        limit = min(limit, max_rows)
        try:
            mgr = self._get_or_connect(name)
        except Exception as exc:
            return {"error": str(exc), "table": table,
                    "columns": [], "rows": [], "rowcount": 0}
        db_type = getattr(mgr, "db_type", "")
        try:
            with self._lock(name):
                if db_type in ("MongoDB", "DocumentDB"):
                    import json as _json
                    query = _json.dumps({
                        "collection": table,
                        "operation": "find",
                        "filter": {},
                        "limit": limit,
                    })
                    raw, error = mgr.execute_document_query(query)
                else:
                    sql = self._apply_row_limit(
                        db_type,
                        f"SELECT * FROM {self._quote_table_for(db_type, table)}",
                        limit,
                    )
                    raw, error = mgr.execute_query(sql)
        except Exception as exc:
            return {"error": str(exc), "table": table,
                    "columns": [], "rows": [], "rowcount": 0}
        if error:
            return {"error": error, "table": table,
                    "columns": [], "rows": [], "rowcount": 0}
        raw = raw or {"columns": [], "rows": []}
        cols = raw.get("columns") or []
        rows = []
        for row in (raw.get("rows") or []):
            rows.append([str(v) if v is not None else "" for v in row])
        return {"error": None, "table": table, "columns": cols,
                "rows": rows, "rowcount": len(rows)}

    def count_table(self, name: str, table: str) -> dict:
        """Return ``{error, table, count}`` — row count for *table*."""
        try:
            mgr = self._get_or_connect(name)
        except Exception as exc:
            return {"error": str(exc), "table": table, "count": 0}
        db_type = getattr(mgr, "db_type", "")
        try:
            with self._lock(name):
                if db_type in ("MongoDB", "DocumentDB"):
                    import json as _json
                    query = _json.dumps({
                        "collection": table,
                        "operation": "count",
                        "filter": {},
                    })
                    raw, error = mgr.execute_document_query(query)
                    if error:
                        return {"error": error, "table": table, "count": 0}
                    # `count` ops typically return a single cell.
                    rows = (raw or {}).get("rows") or []
                    val = rows[0][0] if rows and rows[0] else 0
                    return {"error": None, "table": table, "count": int(val or 0)}
                sql = f"SELECT COUNT(*) FROM {self._quote_table_for(db_type, table)}"
                raw, error = mgr.execute_query(sql)
        except Exception as exc:
            return {"error": str(exc), "table": table, "count": 0}
        if error:
            return {"error": error, "table": table, "count": 0}
        rows = (raw or {}).get("rows") or []
        val = rows[0][0] if rows and rows[0] else 0
        try:
            count = int(val)
        except (TypeError, ValueError):
            count = 0
        return {"error": None, "table": table, "count": count}

    def export_table(
        self,
        name: str,
        table: str,
        output_path: str,
        fmt: str = "csv",
        limit: int | None = None,
    ) -> dict:
        """Dump *table* to *output_path* as CSV or JSON.

        ``limit=None`` exports rows up to the query safety cap configured in
        ``[ui.limits] query_result_max_rows``. Returns
        ``{ok, path, format, rowcount, message, truncated}``.
        """
        from pathlib import Path as _Path
        from common.io.export_utils import (
            export_result_to_csv,
            export_rows_to_json,
        )

        fmt_norm = (fmt or "csv").lower()
        if fmt_norm not in ("csv", "json"):
            return {"ok": False, "path": output_path, "format": fmt,
                    "rowcount": 0,
                    "message": f"Unsupported format '{fmt}'. Use csv|json."}

        from common import paths as _paths
        from common.security.paths import PathEscapeError, resolve_user_path

        try:
            out = resolve_user_path(_paths.exports_dir(), output_path)
        except PathEscapeError as exc:
            return {"ok": False, "path": output_path, "format": fmt_norm,
                    "rowcount": 0, "message": str(exc)}

        try:
            mgr = self._get_or_connect(name)
        except Exception as exc:
            return {"ok": False, "path": output_path, "format": fmt_norm,
                    "rowcount": 0, "message": str(exc)}
        db_type = getattr(mgr, "db_type", "")
        sql = f"SELECT * FROM {self._quote_table_for(db_type, table)}"
        if limit is not None and int(limit) > 0:
            sql = self._apply_row_limit(db_type, sql, int(limit))
        try:
            with self._lock(name):
                raw, error = mgr.execute_query(sql)
        except Exception as exc:
            return {"ok": False, "path": output_path, "format": fmt_norm,
                    "rowcount": 0, "message": str(exc)}
        if error:
            return {"ok": False, "path": output_path, "format": fmt_norm,
                    "rowcount": 0, "message": error}
        raw = raw or {"columns": [], "rows": []}
        cols = raw.get("columns") or []
        rows = raw.get("rows") or []
        truncated = bool(raw.get("truncated", False))
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            if fmt_norm == "csv":
                export_result_to_csv(str(out), {"columns": cols, "rows": rows})
            else:
                export_rows_to_json(str(out), rows, columns=cols)
        except Exception as exc:
            return {"ok": False, "path": str(out), "format": fmt_norm,
                    "rowcount": 0, "message": str(exc)}
        msg = f"Wrote {len(rows)} row(s) to {out}."
        if truncated:
            msg += " Output was truncated by query_result_max_rows safety cap."
        return {"ok": True, "path": str(out), "format": fmt_norm,
                "rowcount": len(rows),
                "truncated": truncated,
                "message": msg}

    def import_csv_to_table(
        self,
        name: str,
        file_path: str,
        table: str | None = None,
        create_table: bool = True,
        chunk_size: int | None = None,
    ) -> dict:
        """Bulk-import a CSV file into *table* on *name*.

        - ``table=None`` derives the table name from the CSV filename stem.
        - ``create_table=True`` issues ``CREATE TABLE IF NOT EXISTS`` first,
          inferring column types from the first ~50 rows (INT / FLOAT / DATE /
          DATETIME / VARCHAR(255) fallback). Header row becomes the column
          names.
        - ``chunk_size`` controls how many rows are inserted per ``executemany``
          batch.

        Returns ``{ok, table, rows_inserted, columns, message}``.
        """
        import csv as _csv
        import re
        from pathlib import Path as _Path

        from common import paths as _paths
        from common.security.paths import PathEscapeError, resolve_user_path

        if chunk_size is None:
            from common.config_loader import config
            chunk_size = config.get_int(
                "database.performance", "import_chunk_size", default=500)

        try:
            csv_path = resolve_user_path(_paths.exports_dir(), file_path)
        except PathEscapeError as exc:
            return {"ok": False, "table": table or "", "rows_inserted": 0,
                    "columns": [], "message": str(exc)}
        if not csv_path.exists():
            return {"ok": False, "table": table or "", "rows_inserted": 0,
                    "columns": [],
                    "message": f"CSV file not found: {csv_path}"}

        target_table = (table or csv_path.stem).strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", target_table):
            return {"ok": False, "table": target_table, "rows_inserted": 0,
                    "columns": [],
                    "message": (
                        f"Invalid table name '{target_table}'. Use "
                        "[A-Za-z_][A-Za-z0-9_]* characters only."
                    )}

        # Read header and a bounded sample for type inference.  Do not load
        # the whole CSV into memory; production import files can be large.
        try:
            with open(csv_path, "r", encoding="utf-8", newline="") as fh:
                reader = _csv.reader(fh)
                try:
                    header = next(reader)
                except StopIteration:
                    return {"ok": False, "table": target_table,
                            "rows_inserted": 0, "columns": [],
                            "message": "CSV file is empty."}
                header = [
                    (col or f"col_{i + 1}").strip() for i, col in enumerate(header)
                ]
                if not header:
                    return {"ok": False, "table": target_table,
                            "rows_inserted": 0, "columns": [],
                            "message": "CSV header is empty."}
                lowered = [h.lower() for h in header]
                if len(lowered) != len(set(lowered)):
                    return {"ok": False, "table": target_table,
                            "rows_inserted": 0, "columns": header,
                            "message": "CSV header contains duplicate column names."}
                sample = []
                for _ in range(50):
                    try:
                        sample.append(next(reader))
                    except StopIteration:
                        break
        except Exception as exc:
            return {"ok": False, "table": target_table, "rows_inserted": 0,
                    "columns": [], "message": f"Could not read CSV: {exc}"}

        try:
            mgr = self._get_or_connect(name)
        except Exception as exc:
            return {"ok": False, "table": target_table, "rows_inserted": 0,
                    "columns": header, "message": str(exc)}
        db_type = getattr(mgr, "db_type", "")

        def _infer(col_values: list[str]) -> str:
            kinds = {"int": True, "float": True, "date": True, "datetime": True}
            for v in col_values:
                if v is None or v == "":
                    continue
                if kinds["int"]:
                    try:
                        int(v)
                    except Exception:
                        kinds["int"] = False
                if kinds["float"]:
                    try:
                        float(v)
                    except Exception:
                        kinds["float"] = False
                if kinds["datetime"] and not re.match(
                    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?", v
                ):
                    kinds["datetime"] = False
                if kinds["date"] and not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
                    kinds["date"] = False
            if kinds["int"]:
                return "INTEGER"
            if kinds["float"]:
                return "FLOAT"
            if kinds["datetime"]:
                return "DATETIME"
            if kinds["date"]:
                return "DATE"
            return "VARCHAR(255)"

        col_types = []
        for i, _h in enumerate(header):
            values = [r[i] if i < len(r) else "" for r in sample]
            col_types.append(_infer(values))

        quoted_table = self._quote_table_for(db_type, target_table)

        if create_table:
            cols_sql = ", ".join(
                f"{self._quote_table_for(db_type, h)} {t}"
                for h, t in zip(header, col_types)
            )
            create_sql = f"CREATE TABLE IF NOT EXISTS {quoted_table} ({cols_sql})"
            try:
                with self._lock(name):
                    _raw, err = mgr.execute_query(create_sql)
                if err:
                    return {"ok": False, "table": target_table,
                            "rows_inserted": 0, "columns": header,
                            "message": f"CREATE TABLE failed: {err}"}
            except Exception as exc:
                return {"ok": False, "table": target_table,
                        "rows_inserted": 0, "columns": header,
                        "message": f"CREATE TABLE failed: {exc}"}

        # Driver-style placeholders.
        if db_type in ("PostgreSQL",):
            placeholder = "%s"
        elif db_type in ("Oracle",):
            placeholder = ":1"  # unused below; we build named placeholders
        else:
            placeholder = "%s" if db_type in ("MySQL", "MariaDB") else "?"

        if db_type == "Oracle":
            ph_list = ", ".join(f":{i + 1}" for i in range(len(header)))
        else:
            ph_list = ", ".join([placeholder] * len(header))

        insert_sql = (
            f"INSERT INTO {quoted_table} "
            f"({', '.join(self._quote_table_for(db_type, h) for h in header)}) "
            f"VALUES ({ph_list})"
        )

        def _normalise_row(r: list[str]) -> list:
            """Pad/truncate to header length and replace blanks with None."""
            if len(r) < len(header):
                r = list(r) + [""] * (len(header) - len(r))
            elif len(r) > len(header):
                r = r[:len(header)]
            return [(v if v != "" else None) for v in r]

        # Bulk insert in chunks via the driver cursor.
        inserted = 0
        try:
            with self._lock(name):
                conn = mgr.conn
                cur = conn.cursor()
                try:
                    batch_size = max(1, int(chunk_size))
                    with open(csv_path, "r", encoding="utf-8", newline="") as fh:
                        reader = _csv.reader(fh)
                        next(reader, None)  # header
                        batch: list[list] = []
                        for row in reader:
                            batch.append(_normalise_row(row))
                            if len(batch) >= batch_size:
                                cur.executemany(insert_sql, batch)
                                inserted += len(batch)
                                batch = []
                        if batch:
                            cur.executemany(insert_sql, batch)
                            inserted += len(batch)
                    if hasattr(conn, "commit"):
                        try:
                            conn.commit()
                        except Exception:
                            pass
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass
        except Exception as exc:
            return {"ok": False, "table": target_table,
                    "rows_inserted": inserted, "columns": header,
                    "message": f"INSERT failed after {inserted} row(s): {exc}"}

        return {"ok": True, "table": target_table,
                "rows_inserted": inserted, "columns": header,
                "message": f"Inserted {inserted} row(s) into {target_table}."}

    def get_table_schema(self, name: str, table: str) -> dict:
        """Return columns/types/keys/indexes for *table* in connection *name*."""
        try:
            mgr = self._get_or_connect(name)
            db_type = getattr(mgr, "db_type", "")
            if not DatabaseRegistry.supports_operation(db_type, "getTableSchema"):
                return {
                    "error": f"Schema retrieval not supported for {db_type}.",
                    "table": table,
                    "columns": [],
                    "indexes": [],
                }
            schema_name = None
            table_name = table
            if db_type in ("MySQL", "MariaDB") and "." in table:
                schema_name, table_name = table.split(".", 1)
                schema_name = schema_name.strip("`\" ")
                table_name = table_name.strip("`\" ")

            with self._lock(name):
                schema_kwargs = {"database": schema_name} if schema_name else {}
                schema = (
                    DatabaseRegistry.execute_operation(
                        db_type, "getTableSchema", mgr.conn, table_name, **schema_kwargs
                    )
                    or []
                )
                indexes: list = []
                if DatabaseRegistry.supports_operation(db_type, "getIndexes"):
                    all_indexes = (
                        DatabaseRegistry.execute_operation(
                            db_type, "getIndexes", mgr.conn
                        )
                        or []
                    )
                    prefix = f"{table}."
                    indexes = [
                        item
                        for item in all_indexes
                        if isinstance(item, str) and item.startswith(prefix)
                    ]
            return {
                "error": None,
                "table": table,
                "columns": schema,
                "indexes": indexes,
            }
        except Exception as exc:
            return {"error": str(exc), "table": table, "columns": [], "indexes": []}

    # ------------------------------------------------------------------
    # Database registry (supported engines + operations)
    # ------------------------------------------------------------------

    def list_db_types(self) -> list[dict]:
        """Return all engines registered in DatabaseRegistry."""
        out: list[dict] = []
        for t in DatabaseRegistry.get_all_types():
            caps = DatabaseRegistry.get_capabilities(t)
            out.append({
                "db_type": t,
                "display_name": DatabaseRegistry.get_display_name(t),
                "default_port": DatabaseRegistry.get_default_port(t),
                "capabilities": caps.to_dict(),
            })
        return out

    def list_db_ops(self, db_type: str) -> list[dict]:
        """Return [(display_name, op_name)] tuples available for *db_type*."""
        # Force registry initialisation through a public init-aware accessor.
        DatabaseRegistry.get_all_types()
        rows = DatabaseRegistry.get_available_operations(db_type) or []
        return [
            {"display_name": d, "operation": op}
            for d, op in (rows if isinstance(rows, list) else [])
        ]


    def show_config(self, section: str | None = None) -> dict:
        """Return current config values (no secrets — section/key/value only)."""
        from common.config_loader import get_config

        cfg = get_config()
        parser = getattr(cfg, "parser", None)
        if parser is None:
            return {"error": "Config parser unavailable", "sections": {}}
        out: dict[str, dict] = {}
        for sect in parser.sections():
            if section and sect != section:
                continue
            out[sect] = {
                key: ("***" if self._is_secret_config_key(key) else value)
                for key, value in parser.items(sect)
            }
        return {"error": None, "sections": out}

    @staticmethod
    def _is_secret_config_key(key: str) -> bool:
        key_l = (key or "").lower()
        return any(
            token in key_l
            for token in (
                "password",
                "secret",
                "token",
                "key",
                "credential",
                "client_secret",
                "webhook",
            )
        )

