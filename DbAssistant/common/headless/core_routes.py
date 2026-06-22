"""
Neutral HTTP-route glue over the headless service layer.

This module is the *bridge* between HTTP and the in-process service
(:class:`common.headless.db_service.CoreDBService` or a composite). It contains
the request models and the functions that register routes on any FastAPI app,
but it is NOT itself "the API product".

Two independent consumers use it:

* :mod:`common.headless.app_factory` — assembles the public REST API
  (adds docs, CORS, the API-key middleware, root redirect to ``/docs``).
* :mod:`common.ui.web` — builds the standalone Web UI's *own* server, reading
  the service directly and serving the SPA. The Web UI never imports
  ``app_factory``, so deleting the public API leaves the Web UI fully working.

Both register the exact same handlers here, so there is a single source of
truth for behaviour and the two surfaces cannot drift apart.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from common.core import modules as _modules
from common.connection_params import ConnectionParams


def _sample_defaults() -> tuple[int, int]:
    """(default rows, max rows) for the sample-table route, from config."""
    from common.config_loader import config
    return (
        config.get_int("database.performance", "sample_default_rows", default=5),
        config.get_int("database.performance", "sample_max_rows", default=1000),
    )


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class SSHTunnelSpec(BaseModel):
    ssh_host: str = Field(..., min_length=1, max_length=512, examples=["bastion.example.com"])
    ssh_user: str = Field("", max_length=256, examples=["ubuntu"])
    ssh_port: int = Field(22, ge=1, le=65535, examples=[22])
    ssh_password: str = Field("", max_length=4096)
    ssh_key_file: str = Field("", max_length=1024, examples=["/home/me/.ssh/id_rsa"])


class ConnectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, examples=["my_mysql"])
    db_type: str = Field(..., min_length=1, max_length=64, examples=["MySQL"])
    host: str = Field(..., min_length=1, max_length=512, examples=["localhost"])
    port: str = Field("", max_length=16, examples=["3306"])
    user: str = Field(..., min_length=1, max_length=256, examples=["root"])
    password: str = Field("", max_length=4096, examples=["secret"])
    database: str = Field("", max_length=512, examples=["mydb"])
    service: str = Field("", max_length=512, examples=[""])
    save_password: Optional[bool] = Field(None)
    # Capability-driven SSL/TLS (mirrors the desktop connection form).
    ssl_mode: str = Field("", max_length=64)
    ssl_ca: str = Field("", max_length=1024)
    ssl_cert: str = Field("", max_length=1024)
    ssl_key: str = Field("", max_length=1024)
    wallet_location: str = Field("", max_length=1024)
    tls: Optional[bool] = Field(None)
    tls_ca_file: str = Field("", max_length=1024)
    # Optional: reach the database through an SSH tunnel. host/port above are
    # the database endpoint as seen from the SSH host (often localhost).
    ssh_tunnel: Optional[SSHTunnelSpec] = Field(None)


class ConnectionUpdate(ConnectionCreate):
    """Update an existing profile. ``old_name`` is the current saved name; a
    blank password preserves the previously-stored one."""
    old_name: str = Field(..., min_length=1, max_length=128, examples=["my_mysql"])


class CloudProfile(BaseModel):
    """A cloud DB connection profile. Provider-specific auth fields (e.g.
    ``access_key_id``) are accepted as extra top-level keys; ``sql_connection``
    carries the DB login. ``old_name`` is set when renaming an edited profile."""

    model_config = ConfigDict(extra="allow")

    provider: str = Field(..., examples=["AWS"])
    display_name: str = Field(..., min_length=1, max_length=128, examples=["prod-mysql"])
    auth_mode: str = Field("keys", examples=["keys", "pwd", "sso", "env"])
    sql_connection: dict = Field(default_factory=dict)
    old_name: Optional[str] = Field(None, max_length=128)


class QueryRequest(BaseModel):
    connection: str = Field(..., min_length=1, max_length=128, examples=["my_mysql"])
    sql: str = Field(..., min_length=1, max_length=1_000_000, examples=["SELECT 1"])


class MultiQueryRequest(BaseModel):
    connection: str = Field(..., min_length=1, max_length=128, examples=["my_mysql"])
    sql: str = Field(
        ...,
        min_length=1,
        max_length=1_000_000,
        examples=["CREATE TABLE t(id INT); INSERT INTO t VALUES (1); SELECT * FROM t;"],
        description="Multi-statement SQL; split on top-level ';'",
    )


class FormatSqlRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=1_000_000, examples=["select 1"])


class AutocommitRequest(BaseModel):
    enabled: bool = Field(..., examples=[True])


class TableExportRequest(BaseModel):
    table: str = Field(..., min_length=1, max_length=512, examples=["customers"])
    output_path: str = Field(..., min_length=1, max_length=4096, examples=["/tmp/customers.csv"])
    format: str = Field("csv", max_length=16, examples=["csv", "json"])
    limit: int | None = Field(None, examples=[1000])


class CsvImportRequest(BaseModel):
    file_path: str = Field(..., min_length=1, max_length=4096, examples=["/tmp/customers.csv"])
    table: str | None = Field(None, max_length=512, examples=["customers"])
    create_table: bool = Field(True, examples=[True])
    chunk_size: int = Field(500, examples=[500])


class DashboardLayoutRequest(BaseModel):
    rows: list = Field(
        ...,
        description="2-column grid: list of rows, each a list of panel ids "
                    "or null. Unknown panel ids are rejected.",
        examples=[[["connections", "monitor"], ["ai", "schema"],
                   ["sql_editor", "objects"]]],
        min_length=1,
    )


class SettingsWriteRequest(BaseModel):
    values: dict[str, str] = Field(
        ...,
        description="Map of curated setting id -> new value. Validated as a "
                    "batch; nothing is written unless every value validates.",
        examples=[{"ui.theme": "dark", "sql.row_limit": "1000"}],
    )


class SettingsRestoreRequest(BaseModel):
    target: str = Field(
        "all", description="all | config | properties",
        examples=["all"],
    )


def _error(detail: str, status: int = 400):
    raise HTTPException(status_code=status, detail=detail)


def _connection_form_dict(body: ConnectionCreate) -> dict:
    """Flatten a ConnectionCreate model for inline test/connect."""
    data = body.model_dump()
    if body.ssh_tunnel:
        data["ssh_tunnel"] = body.ssh_tunnel.model_dump()
    return data


def _inline_connection_kwargs(body: ConnectionCreate) -> dict:
    return {
        "name": body.name,
        "db_type": body.db_type,
        "host": body.host,
        "port": body.port,
        "user": body.user,
        "password": body.password,
        "save_password": body.save_password,
        "database": body.database,
        "service": body.service,
        "ssh_tunnel": body.ssh_tunnel.model_dump() if body.ssh_tunnel else None,
        "ssl_mode": body.ssl_mode or None,
        "ssl_ca": body.ssl_ca or None,
        "ssl_cert": body.ssl_cert or None,
        "ssl_key": body.ssl_key or None,
        "wallet_location": body.wallet_location or None,
        "tls": body.tls,
        "tls_ca_file": body.tls_ca_file or None,
    }


def _connection_params(body: ConnectionCreate) -> ConnectionParams:
    return ConnectionParams.from_mapping(_inline_connection_kwargs(body))


# ---------------------------------------------------------------------------
# Route registration
# ---------------------------------------------------------------------------
def mount_core_routes(app: FastAPI, svc: Any, *, root_redirect: str = "/docs") -> None:
    """Register always-on core routes (connections, query, objects, config).

    ``root_redirect`` controls where ``GET /`` sends the browser. The public
    REST API uses ``/docs`` (Swagger); the standalone Web UI passes ``/ui/`` so
    the bare host opens the app instead of API docs.
    """

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse(url=root_redirect)

    @app.get("/api", tags=["Health"])
    def api_index():
        return {
            "service": app.title,
            "version": app.version,
            "docs": "/docs",
            "openapi": "/openapi.json",
            "health": "/api/health",
            "modules": "/api/modules",
        }

    @app.get("/api/health", tags=["Health"])
    def health():
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "service": app.title,
        }

    @app.get("/api/auth/whoami", tags=["Auth"])
    def auth_whoami(request: Request):
        key = getattr(request.state, "api_key", None)
        return {
            "authenticated": bool(key),
            "key_id": (key or {}).get("key_id", ""),
            "name": (key or {}).get("name", ""),
        }

    @app.get("/api/modules", tags=["Health"])
    def modules_status():
        return _modules.status()

    @app.get("/api/connections", tags=["Connections"])
    def list_connections():
        return svc.list_connections()

    @app.get("/api/connections/metadata", tags=["Connections"])
    def connection_metadata():
        """DB-type capability metadata for building connection forms."""
        if not hasattr(svc, "connection_metadata"):
            return {"db_types": [], "engines": {}}
        return svc.connection_metadata()

    @app.post("/api/connections", tags=["Connections"], status_code=201)
    def create_connection(body: ConnectionCreate):
        result = svc.add_connection(_connection_params(body))
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.put("/api/connections/{name}", tags=["Connections"])
    def update_connection(name: str, body: ConnectionUpdate):
        """Update the profile currently saved as ``{name}`` (rename allowed)."""
        if not hasattr(svc, "update_connection"):
            _error("update_connection not supported by this service.", 501)
        result = svc.update_connection(name, _connection_params(body))
        if not result["ok"]:
            _error(result["message"], 404 if "not found" in result["message"].lower() else 400)
        return result

    @app.delete("/api/connections/{name}", tags=["Connections"])
    def delete_connection(name: str):
        result = svc.remove_connection(name)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.post("/api/connections/{name}/test", tags=["Connections"])
    def test_connection(name: str):
        result = svc.test_connection(name)
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.post("/api/connections/test-inline", tags=["Connections"])
    def test_connection_inline(body: ConnectionCreate):
        """Test using form values (password need not be saved). Mirrors Tk Test."""
        if not hasattr(svc, "test_connection_inline"):
            _error("test_connection_inline not supported by this service.", 501)
        result = svc.test_connection_inline(_connection_params(body))
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.post("/api/connections/{name}/open-form", tags=["Connections"])
    def open_connection_with_form(name: str, body: ConnectionCreate):
        """Open using saved profile merged with inline form credentials."""
        if not hasattr(svc, "open_connection"):
            _error("open_connection not supported by this service.", 501)
        result = svc.open_connection(name, form=_connection_form_dict(body))
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.get("/api/connections/active", tags=["Connections"])
    def list_active_connections():
        """Currently-open (cached) connections — mirrors the UI's Active list."""
        if not hasattr(svc, "list_active_connections"):
            return []
        return svc.list_active_connections()

    @app.post("/api/connections/{name}/open", tags=["Connections"])
    def open_connection(name: str):
        """Open (or reuse) a cached connection so subsequent queries are warm."""
        if not hasattr(svc, "open_connection"):
            _error("open_connection not supported by this service.", 501)
        result = svc.open_connection(name)
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.post("/api/connections/{name}/close", tags=["Connections"])
    def close_connection(name: str):
        """Close one cached connection (does not delete the saved profile)."""
        if not hasattr(svc, "close_connection"):
            _error("close_connection not supported by this service.", 501)
        result = svc.close_connection(name)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.post("/api/connections/close-all", tags=["Connections"])
    def close_all_connections():
        """Close every cached connection."""
        if not hasattr(svc, "close_all_connections"):
            _error("close_all_connections not supported by this service.", 501)
        result = svc.close_all_connections()
        if not result["ok"]:
            _error(result["message"])
        return result

    # ------------------------------------------------------------------
    # Cloud DB connections (AWS / Azure / GCP / Other) — mirrors the
    # desktop Connections-tab cloud section.
    # ------------------------------------------------------------------
    def _need_cloud(method: str):
        if not hasattr(svc, method):
            _error("Cloud connections are not supported by this service.", 501)

    def _cloud_profile(body: CloudProfile) -> tuple[dict, Optional[str]]:
        data = body.model_dump()
        old_name = data.pop("old_name", None)
        return data, old_name

    @app.get("/api/cloud/schemas", tags=["Cloud"])
    def cloud_schemas():
        """Provider form schemas + shared SQL fields (build the cloud form)."""
        _need_cloud("cloud_db_provider_schemas")
        return svc.cloud_db_provider_schemas()

    @app.get("/api/cloud/connections", tags=["Cloud"])
    def cloud_list():
        _need_cloud("list_cloud_db_connections")
        return svc.list_cloud_db_connections()

    @app.get("/api/cloud/connections/{name}", tags=["Cloud"])
    def cloud_get(name: str):
        """Full profile for editing (secret values blanked)."""
        _need_cloud("get_cloud_db_connection")
        prof = svc.get_cloud_db_connection(name)
        if prof is None:
            _error(f"Cloud profile '{name}' not found.", 404)
        return prof

    @app.post("/api/cloud/connections", tags=["Cloud"], status_code=201)
    def cloud_create(body: CloudProfile):
        _need_cloud("save_cloud_db_connection")
        data, old_name = _cloud_profile(body)
        result = svc.save_cloud_db_connection(data, old_name=old_name)
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.put("/api/cloud/connections/{name}", tags=["Cloud"])
    def cloud_update(name: str, body: CloudProfile):
        _need_cloud("save_cloud_db_connection")
        data, old_name = _cloud_profile(body)
        result = svc.save_cloud_db_connection(data, old_name=old_name or name)
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.delete("/api/cloud/connections/{name}", tags=["Cloud"])
    def cloud_delete(name: str):
        _need_cloud("delete_cloud_db_connection")
        result = svc.delete_cloud_db_connection(name)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.post("/api/cloud/resolve", tags=["Cloud"])
    def cloud_resolve(body: CloudProfile):
        """Resolve an AWS RDS instance id to a SQL endpoint (host/port/db_type)."""
        _need_cloud("resolve_cloud_db_endpoint")
        data, _ = _cloud_profile(body)
        return svc.resolve_cloud_db_endpoint(data)

    @app.post("/api/cloud/test-login", tags=["Cloud"])
    def cloud_test_login(body: CloudProfile):
        """Best-effort cloud-provider auth check."""
        _need_cloud("cloud_db_test_login")
        data, _ = _cloud_profile(body)
        return svc.cloud_db_test_login(data)

    @app.post("/api/cloud/test-db", tags=["Cloud"])
    def cloud_test_db(body: CloudProfile):
        """Open a short-lived SQL connection using the profile's SQL params."""
        _need_cloud("test_cloud_db")
        data, _ = _cloud_profile(body)
        return svc.test_cloud_db(data)

    @app.post("/api/cloud/connect", tags=["Cloud"])
    def cloud_connect(body: CloudProfile):
        """Save+mirror the profile, then open it as an active connection."""
        _need_cloud("connect_cloud_db")
        data, old_name = _cloud_profile(body)
        result = svc.connect_cloud_db(data, old_name=old_name)
        if not result.get("ok"):
            _error(result.get("message", "Connect failed."))
        return result

    @app.post("/api/query", tags=["SQL"])
    def execute_query(body: QueryRequest):
        result = svc.execute(body.connection, body.sql)
        if result.get("error"):
            _error(result["error"])
        return result

    @app.post("/api/query/multi", tags=["SQL"])
    def execute_query_multi(body: MultiQueryRequest):
        """Split *sql* on ``;`` and execute each statement serially."""
        if not hasattr(svc, "execute_multi"):
            _error("execute_multi not supported by this service.", 501)
        result = svc.execute_multi(body.connection, body.sql)
        # Even on partial failure we return the full payload so callers can
        # inspect which statements succeeded before the failure.
        return result

    @app.post("/api/query/format", tags=["SQL"])
    def format_sql(body: FormatSqlRequest):
        """Pretty-print SQL (mirrors the desktop 'Format SQL' action)."""
        if not hasattr(svc, "format_sql"):
            _error("format_sql not supported by this service.", 501)
        return svc.format_sql(body.sql)

    @app.post("/api/query/{connection}/cancel", tags=["SQL"])
    def cancel_query(connection: str):
        """Send a cancel signal to a currently-running query on *connection*."""
        if not hasattr(svc, "cancel_query"):
            _error("cancel_query not supported by this service.", 501)
        result = svc.cancel_query(connection)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.get("/api/query/{connection}/autocommit", tags=["SQL"])
    def get_autocommit(connection: str):
        """Return the live autocommit state of *connection*."""
        if not hasattr(svc, "get_autocommit"):
            _error("get_autocommit not supported by this service.", 501)
        result = svc.get_autocommit(connection)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.put("/api/query/{connection}/autocommit", tags=["SQL"])
    def set_autocommit(connection: str, body: AutocommitRequest):
        """Toggle autocommit on *connection*."""
        if not hasattr(svc, "set_autocommit"):
            _error("set_autocommit not supported by this service.", 501)
        result = svc.set_autocommit(connection, body.enabled)
        if not result["ok"]:
            _error(result["message"], 400)
        return result

    @app.post("/api/query/{connection}/commit", tags=["SQL"])
    def commit_tx(connection: str):
        """Commit the current transaction on *connection*."""
        if not hasattr(svc, "commit"):
            _error("commit not supported by this service.", 501)
        result = svc.commit(connection)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.post("/api/query/{connection}/rollback", tags=["SQL"])
    def rollback_tx(connection: str):
        """Roll back the current transaction on *connection*."""
        if not hasattr(svc, "rollback"):
            _error("rollback not supported by this service.", 501)
        result = svc.rollback(connection)
        if not result["ok"]:
            _error(result["message"], 404)
        return result

    @app.get("/api/objects/{connection}", tags=["Objects"])
    def get_objects(
        connection: str,
        type: str = Query(
            "tables",
            description="Object type alias (tables, views, procs, … — engine-dependent)",
        ),
    ):
        items = svc.get_objects(connection, type)
        if items and isinstance(items[0], dict) and "error" in items[0]:
            _error(items[0]["error"])
        return {"connection": connection, "type": type, "items": items, "count": len(items)}

    _sample_def, _sample_max = _sample_defaults()

    @app.get("/api/objects/{connection}/sample", tags=["Objects"])
    def sample_table(
        connection: str,
        table: str = Query(..., description="Table or collection name"),
        limit: int = Query(_sample_def, ge=1, le=_sample_max),
    ):
        """Return up to *limit* sample rows for *table*."""
        if not hasattr(svc, "sample_table"):
            _error("sample_table not supported by this service.", 501)
        result = svc.sample_table(connection, table, limit)
        if result.get("error"):
            _error(result["error"])
        return result

    @app.get("/api/objects/{connection}/count", tags=["Objects"])
    def count_table(connection: str, table: str = Query(...)):
        """Return ``{table, count}`` for *table*."""
        if not hasattr(svc, "count_table"):
            _error("count_table not supported by this service.", 501)
        result = svc.count_table(connection, table)
        if result.get("error"):
            _error(result["error"])
        return result

    @app.get("/api/objects/{connection}/schema", tags=["Objects"])
    def table_schema(connection: str, table: str = Query(...)):
        """Return column schema (name/type/nullable/default) for *table*."""
        if not hasattr(svc, "get_table_schema"):
            _error("get_table_schema not supported by this service.", 501)
        result = svc.get_table_schema(connection, table)
        if result.get("error"):
            _error(result["error"])
        return result

    @app.post("/api/objects/{connection}/export", tags=["Objects"])
    def export_table(connection: str, body: TableExportRequest):
        """Dump *table* to *output_path* on the server filesystem as CSV/JSON."""
        if not hasattr(svc, "export_table"):
            _error("export_table not supported by this service.", 501)
        result = svc.export_table(
            connection, body.table, body.output_path,
            fmt=body.format, limit=body.limit,
        )
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.post("/api/objects/{connection}/import-csv", tags=["Objects"])
    def import_csv_to_table(connection: str, body: CsvImportRequest):
        """Bulk-load a CSV file into a target table (creates it if missing)."""
        if not hasattr(svc, "import_csv_to_table"):
            _error("import_csv_to_table not supported by this service.", 501)
        result = svc.import_csv_to_table(
            connection, body.file_path,
            table=body.table,
            create_table=body.create_table,
            chunk_size=body.chunk_size,
        )
        if not result["ok"]:
            _error(result["message"])
        return result

    @app.get("/api/config", tags=["Config"])
    def config_show(section: str = Query("", description="Limit to one section; blank = all")):
        r = svc.show_config(section=section or None)
        if r.get("error"):
            _error(r["error"])
        return r

    # ------------------------------------------------------------------
    # Curated settings. Secrets are ALWAYS redacted on read and never
    # returned. Writes mirror the desktop Settings UI (settings_service),
    # so the Web/Textual UIs can save settings like Tk.
    # ------------------------------------------------------------------
    @app.get("/api/config/settings", tags=["Config"])
    def config_settings(
        group: str = Query("", description="Filter by group name (blank = all)"),
        grouped: bool = Query(False, description="Return settings grouped by category"),
    ):
        """Curated, self-describing settings with current values (secrets redacted)."""
        from common.config import settings_service as _S

        if grouped:
            data = _S.grouped(redact=True)
            if group:
                data = {group: data.get(group, [])}
            return {"groups": data, "writable": True}
        rows = _S.describe_all(redact=True)
        if group:
            rows = [r for r in rows if r["group"].lower() == group.lower()]
        return {"settings": rows, "count": len(rows), "writable": True}

    @app.post("/api/config/settings", tags=["Config"])
    def config_settings_write(body: SettingsWriteRequest):
        """Validate and persist a batch of curated settings (like the Tk Save)."""
        from common.config import settings_service as _S

        r = _S.set_many(body.values)
        if not r.get("ok"):
            _error(r.get("message", "Settings save failed."))
        return r

    @app.post("/api/config/settings/restore", tags=["Config"])
    def config_settings_restore(body: SettingsRestoreRequest):
        """Restore config/properties from their *.ini.example defaults."""
        from common.config import settings_service as _S

        target = (body.target or "all").strip().lower()
        if target not in ("all", "config", "properties"):
            _error("target must be one of: all, config, properties.")
        r = _S.restore_defaults(target)
        if not r.get("ok"):
            _error(r.get("message", "Restore failed."))
        return r

    @app.get("/api/config/settings/{spec_id}", tags=["Config"])
    def config_setting(spec_id: str):
        """One setting's description + current value (secret redacted)."""
        from common.config import settings_service as _S

        spec = _S.find(spec_id)
        if spec is None:
            _error(f"Unknown setting '{spec_id}'.", 404)
        return _S.describe(spec, redact=True)

    @app.get("/api/databases/types", tags=["Databases"])
    def databases_types():
        return svc.list_db_types()

    @app.get("/api/databases/ops", tags=["Databases"])
    def databases_ops(type: str = Query(..., description="DB engine, e.g. MySQL")):
        return svc.list_db_ops(type)

    @app.get("/api/dashboard", tags=["Dashboard"])
    def dashboard_snapshot():
        from common.dashboard.service import (
            DashboardCapabilities,
            DashboardRuntime,
            DashboardService,
        )

        mod_status = _modules.status()
        active = getattr(svc, "_active", {})

        def _active_map():
            return active if isinstance(active, dict) else {}

        dash = DashboardService(
            DashboardRuntime(
                get_active_connections=_active_map,
                get_saved_connections=lambda: svc.list_connections(),
            ),
            DashboardCapabilities(
                has_schema=mod_status.get("migrator", {}).get("installed", False),
                has_ai=mod_status.get("ai", {}).get("installed", False),
                has_monitor=mod_status.get("monitor", {}).get("installed", False),
            ),
        )
        return dash.collect()

    # ------------------------------------------------------------------
    # Phase 7 — app-level: cache clearing, dashboard layout, shortcuts.
    # ------------------------------------------------------------------

    @app.post("/api/app/clear-caches", tags=["App"])
    def app_clear_caches():
        """Clear every in-process cache we know how to clear (AI schema/context, ...)."""
        from common.headless import app_service as appsvc

        return appsvc.clear_all_caches(svc)

    @app.get("/api/dashboard/layout", tags=["Dashboard"])
    def app_get_layout():
        """Current 2-column dashboard grid plus the default-for-reset reference."""
        from common.headless import app_service as appsvc

        return appsvc.get_dashboard_layout()

    @app.post("/api/dashboard/layout/reset", tags=["Dashboard"])
    def app_reset_layout():
        """Reset the dashboard grid to the default layout."""
        from common.headless import app_service as appsvc

        return appsvc.reset_dashboard_layout()

    @app.put("/api/dashboard/layout", tags=["Dashboard"])
    def app_save_layout(body: DashboardLayoutRequest):
        """Save a new dashboard grid (validated against the known panel ids)."""
        from common.headless import app_service as appsvc

        r = appsvc.save_dashboard_layout(body.rows)
        if not r["ok"]:
            _error(r["message"])
        return r

    @app.get("/api/app/shortcuts", tags=["App"])
    def app_shortcuts(section: str = Query("", description="Optional section filter")):
        """Curated keyboard-shortcut reference for the UI."""
        from common.headless import app_service as appsvc

        return appsvc.list_shortcuts(section or None)


def _composite_full_service(core: Any):
    """Build a composite service that exposes every installed module's bridge.

    Falls back to the bare *core* if no module bridges are discoverable. This
    is what backs the **full** surface (``module_key=None``) so endpoints like
    ``/api/migrator/convert`` resolve their service methods correctly instead of
    falling through to ``CoreDBService`` (which doesn't carry them).

    Idempotent — if *core* is already a CompositeService the original is
    returned, so callers can safely wrap a service twice without duplicating
    the module bridges.
    """
    from common.core.standalone_runner import _MODULE_SERVICE_FACTORIES, _import_factory
    from common.headless.composite import CompositeService, composite_service

    if isinstance(core, CompositeService):
        return core

    bridges: list[Any] = []
    for module_key, factory_path in _MODULE_SERVICE_FACTORIES.items():
        manifest = _modules.get(module_key)
        if manifest is None:
            continue
        try:
            built = _import_factory(factory_path)(core)
            # The module factory may return a composite itself; in that case
            # only keep its module-side layer so we don't shadow the live core.
            mods = getattr(built, "_modules", None)
            if mods:
                bridges.extend(mods)
            else:
                bridges.append(built)
        except Exception as exc:  # pragma: no cover
            import logging

            logging.getLogger(__name__).warning(
                "Could not build bridge for module '%s': %s", module_key, exc
            )
    if not bridges:
        return core
    return composite_service(core, *bridges)


def mount_module_routers(
    app: FastAPI,
    svc: Any,
    *,
    module_key: Optional[str] = None,
) -> list[str]:
    """Mount one module (standalone) or all installed modules (full tool)."""
    mounted: list[str] = []
    if module_key is not None:
        manifest = _modules.get(module_key)
        if manifest is None or manifest.build_router is None:
            return mounted
        try:
            app.include_router(manifest.build_router(svc))
            mounted.append(module_key)
        except Exception as exc:  # pragma: no cover
            import logging

            logging.getLogger(__name__).warning(
                "Could not mount module '%s' router: %s", module_key, exc
            )
        return mounted

    # Full app: build a single composite so every router sees one service that
    # carries the core + every module's methods.
    full_svc = _composite_full_service(svc)
    for command, manifest in _modules.discover().items():
        if manifest.build_router is None:
            continue
        try:
            app.include_router(manifest.build_router(full_svc))
            mounted.append(command)
        except Exception as exc:  # pragma: no cover
            import logging

            logging.getLogger(__name__).warning(
                "Could not mount module '%s' router: %s", command, exc
            )
    return mounted
