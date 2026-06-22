"""
common/headless/cloud_service.py
================================
Headless cloud DB connection service.

Framework-agnostic counterpart to the Tk ``CloudDBConnectionPanel``: provider
form schemas, encrypted profile CRUD (via :class:`CloudConnectionManager`),
SQL-endpoint resolution, and DB test/connect. Mixed into ``CoreDBService`` so
the public API and the standalone Web UI expose the SAME cloud operations the
desktop Connections tab has, keeping all three UIs in sync.

No UI imports here; this is pure service logic.
"""

from __future__ import annotations

from typing import Any

# Credential fields that must never be returned to a client; blanked on read
# and preserved-on-write when the incoming value is empty.
_SECRET_KEYS = frozenset({
    "access_key_id", "secret_access_key", "session_token", "client_secret",
    "password", "private_key", "api_key", "sso_client_secret", "sa_key_json",
    "oauth_token", "oauth_client_secret", "oauth_refresh_token", "bearer_token",
    "sql_password",
})


def _norm_fields(fields: Any) -> list[dict]:
    """Normalise schema field tuples ``(label, key, secret, help[, choices])``."""
    out: list[dict] = []
    for f in fields or ():
        item = {
            "label": f[0],
            "key": f[1],
            "secret": (len(f) > 2 and f[2] == "*"),
            "help": f[3] if len(f) > 3 else "",
        }
        if len(f) > 4 and isinstance(f[4], (list, tuple)):
            item["choices"] = list(f[4])
        out.append(item)
    return out


class CloudServiceMixin:
    """Cloud DB connection operations for the core service.

    Relies on the host class providing ``self._cm`` (a ConnectionManager) and
    ``open_connection(name)`` — both present on :class:`CoreDBService`.
    """

    # -- lazy singletons ------------------------------------------------- #
    def _cloud_manager(self):
        mgr = getattr(self, "_cloud_db_mgr", None)
        if mgr is None:
            from common.cloud import CloudConnectionManager

            mgr = CloudConnectionManager()
            self._cloud_db_mgr = mgr
        return mgr

    # -- schema (the "common object" every UI renders) ------------------- #
    def cloud_db_provider_schemas(self) -> dict:
        """Provider form schemas + shared SQL fields + MFA list (JSON-ready)."""
        from common.cloud.schemas import (
            CLOUD_DB_SQL_FIELDS,
            CLOUD_PROVIDER_SCHEMAS,
            MFA_TYPES,
        )

        providers: dict[str, dict] = {}
        for name, schema in CLOUD_PROVIDER_SCHEMAS.items():
            env = schema.get("env_auth", {}) or {}
            sso = schema.get("sso_auth", {}) or {}
            providers[name] = {
                "label": schema.get("label", name),
                "api": schema.get("api", ""),
                "mfaCommon": bool(schema.get("mfa_common", False)),
                "mfaHint": schema.get("mfa_hint", ""),
                "resource": _norm_fields(schema.get("resource", [])),
                "keysAuth": _norm_fields(schema.get("keys_auth", [])),
                "pwdAuth": _norm_fields(schema.get("pwd_auth", [])),
                "envAuth": {
                    "tabLabel": env.get("tab_label", "Environment / Instance Role"),
                    "help": env.get("help", ""),
                    "fields": _norm_fields(env.get("fields", [])),
                },
                "ssoAuth": {
                    "tabLabel": sso.get("tab_label", "SSO / OIDC"),
                    "fields": _norm_fields(sso.get("fields", [])),
                },
            }
        return {
            "providers": providers,
            "providerOrder": list(CLOUD_PROVIDER_SCHEMAS.keys()),
            "sqlFields": _norm_fields(CLOUD_DB_SQL_FIELDS),
            "mfaTypes": list(MFA_TYPES),
        }

    # -- profile CRUD ---------------------------------------------------- #
    def _summarise(self, name: str, profile: dict) -> dict:
        sql = profile.get("sql_connection") or {}
        return {
            "name": name,
            "display_name": profile.get("display_name", name),
            "provider": profile.get("provider", ""),
            "auth_mode": profile.get("auth_mode", "keys"),
            "region": profile.get("region", ""),
            "resource_name": profile.get("resource_name", ""),
            "db_type": sql.get("db_type", ""),
            "host": sql.get("host", ""),
            "port": sql.get("port", ""),
            "database": sql.get("service_or_db", "") or sql.get("database", ""),
            "username": sql.get("username", ""),
        }

    def list_cloud_db_connections(self) -> list[dict]:
        """Return one summary row per saved cloud profile (no secrets)."""
        data = self._cloud_manager().load_cloud_databases()
        return [self._summarise(name, prof) for name, prof in data.items()]

    def _blank_secrets(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {
                k: ("" if k in _SECRET_KEYS and v else self._blank_secrets(v))
                for k, v in obj.items()
            }
        return obj

    def get_cloud_db_connection(self, name: str) -> dict | None:
        """Return a full profile for editing, with secret values blanked."""
        data = self._cloud_manager().load_cloud_databases()
        prof = data.get(name)
        if prof is None:
            return None
        return self._blank_secrets(dict(prof))

    def _preserve_secrets(self, incoming: dict, existing: dict | None) -> dict:
        """Copy stored secrets over blank incoming ones (top + sql_connection)."""
        if not existing:
            return incoming
        for k in _SECRET_KEYS:
            if not (incoming.get(k) or "").strip() and existing.get(k):
                incoming[k] = existing[k]
        inc_sql = incoming.get("sql_connection") or {}
        ex_sql = existing.get("sql_connection") or {}
        for k in ("password",):
            if not (inc_sql.get(k) or "").strip() and ex_sql.get(k):
                inc_sql[k] = ex_sql[k]
        incoming["sql_connection"] = inc_sql
        return incoming

    def save_cloud_db_connection(
        self, profile: dict, *, old_name: str | None = None,
        resolve_remote: bool = False,
    ) -> dict:
        """Validate, persist (encrypted) and mirror a cloud DB profile.

        Returns ``{ok, message, name}``. A blank secret preserves the stored one.
        """
        from common.cloud import CLOUD_PROVIDER_SCHEMAS, validate_cloud_profile
        from common.cloud.profiles import PURPOSE_CONNECTIONS, TARGET_CLOUD_DB
        from common.cloud.sql_bridge import (
            enrich_sql_connection,
            sync_cloud_db_to_saved_connections,
        )

        profile = dict(profile or {})
        profile.setdefault("purpose", PURPOSE_CONNECTIONS)
        profile.setdefault("target_kind", TARGET_CLOUD_DB)
        provider = profile.get("provider", "")
        schema = CLOUD_PROVIDER_SCHEMAS.get(provider)
        if not schema:
            return {"ok": False, "message": "Select a cloud provider.", "name": ""}

        err = validate_cloud_profile(
            profile, provider, schema,
            require_db_identifier=True, target_kind=TARGET_CLOUD_DB,
        )
        if err:
            return {"ok": False, "message": err, "name": ""}
        sql = profile.get("sql_connection") or {}
        if not (sql.get("username") or "").strip():
            return {"ok": False, "message": "DB username is required.", "name": ""}
        if not (sql.get("host") or "").strip() and provider.upper() != "AWS":
            return {"ok": False, "message": "SQL host is required.", "name": ""}

        mgr = self._cloud_manager()
        databases = mgr.load_cloud_databases()
        name = (profile.get("display_name") or "").strip()
        existing = databases.get(old_name) or databases.get(name)
        profile = self._preserve_secrets(profile, existing)
        profile = enrich_sql_connection(profile, resolve_remote=resolve_remote)

        if old_name and old_name != name:
            databases.pop(old_name, None)
        databases[name] = profile
        if not mgr.save_cloud_databases(databases):
            return {"ok": False, "message": "Could not write cloud profile.", "name": ""}

        ok, msg = sync_cloud_db_to_saved_connections(
            profile, self._cm, resolve_remote=resolve_remote,
        )
        if hasattr(self._cm, "load_connections"):
            try:
                self._cm.connections = self._cm.load_connections()
            except Exception:
                pass
        return {"ok": True, "message": msg or f"Saved cloud profile '{name}'.", "name": name}

    def delete_cloud_db_connection(self, name: str) -> dict:
        """Remove a saved cloud profile. Returns ``{ok, message}``."""
        mgr = self._cloud_manager()
        databases = mgr.load_cloud_databases()
        if name not in databases:
            return {"ok": False, "message": f"Cloud profile '{name}' not found."}
        databases.pop(name, None)
        if not mgr.save_cloud_databases(databases):
            return {"ok": False, "message": "Could not write cloud profile."}
        return {"ok": True, "message": f"Deleted cloud profile '{name}'."}

    # -- SQL endpoint resolve / test / connect --------------------------- #
    def resolve_cloud_db_endpoint(self, profile: dict) -> dict:
        """Resolve an AWS RDS endpoint to {host, port, db_type}."""
        if (profile.get("provider") or "").upper() != "AWS":
            return {"ok": False, "message": "Auto-resolve is supported for AWS RDS only."}
        try:
            from common.cloud.sql_bridge import resolve_aws_rds_sql_endpoint

            resolved = resolve_aws_rds_sql_endpoint(profile)
        except Exception as exc:
            return {"ok": False, "message": f"Resolve failed: {exc}"}
        if not resolved:
            return {"ok": False, "message": "Could not resolve RDS endpoint."}
        return {"ok": True, "message": f"Resolved {resolved.get('host')}:{resolved.get('port')}",
                **resolved}

    def _sql_params(self, profile: dict) -> tuple[str, str, str, str, str, str]:
        from common.cloud.sql_bridge import enrich_sql_connection

        data = enrich_sql_connection(dict(profile), resolve_remote=False)
        sql = data.get("sql_connection") or {}
        return (
            (sql.get("db_type") or "").strip(),
            (sql.get("host") or "").strip(),
            str(sql.get("port") or "").strip(),
            (sql.get("username") or "").strip(),
            sql.get("password") or "",
            (sql.get("service_or_db") or sql.get("database") or "").strip(),
        )

    def test_cloud_db(self, profile: dict) -> dict:
        """Open a short-lived SQL connection using the profile's SQL params."""
        from common.db_manager import DatabaseManager

        db_type, host, port, user, password, database = self._sql_params(profile)
        if not host:
            return {"ok": False, "message": "SQL host is required (use Resolve for AWS)."}
        if not user:
            return {"ok": False, "message": "DB username is required."}
        try:
            mgr = DatabaseManager(db_type)
            conn = mgr.connect(host=host, port=int(port or 0), username=user,
                               password=password, database=database, service=database)
            ok = conn is not None
            if ok:
                mgr.disconnect()
            return {"ok": ok,
                    "message": f"DB login OK at {host}:{port}" if ok else "DB login failed."}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}

    def connect_cloud_db(self, profile: dict, *, old_name: str | None = None) -> dict:
        """Save+mirror the profile, then open it as an active connection."""
        saved = self.save_cloud_db_connection(profile, old_name=old_name)
        if not saved["ok"]:
            return saved
        name = saved["name"]
        result = self.open_connection(name)
        if result.get("ok"):
            result["message"] = f"Cloud DB '{name}' connected — added to active connections."
        return result

    def cloud_db_test_login(self, profile: dict) -> dict:
        """Best-effort cloud-provider auth check (AWS STS where available)."""
        provider = (profile.get("provider") or "").upper()
        if provider == "AWS":
            try:
                from common.cloud.sql_bridge import _aws_boto3_session

                session = _aws_boto3_session(profile)
                ident = session.client("sts").get_caller_identity()
                return {"ok": True,
                        "message": f"AWS login OK — account {ident.get('Account', '?')}."}
            except Exception as exc:
                return {"ok": False, "message": f"AWS login failed: {exc}"}
        return {
            "ok": True,
            "message": (f"Saved. Live login test for {provider or 'this provider'} "
                        "requires its SDK/CLI; profile fields validated."),
        }
