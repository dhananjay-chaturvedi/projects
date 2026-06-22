"""
Bridge cloud DB profiles (cloud_connections.json) to SQL saved connections.

When a cloud database is registered from the Connections tab, resolve the SQL
endpoint where possible and mirror the profile into ConnectionManager so it
appears under Load Saved / active connection pickers.
"""

from __future__ import annotations

from typing import Any, Optional

from common.cloud.profiles import PURPOSE_CONNECTIONS, TARGET_CLOUD_DB


def engine_to_db_type(engine: str) -> str:
    e = (engine or "").lower()
    if "maria" in e:
        return "MariaDB"
    if "postgres" in e:
        return "PostgreSQL"
    if "oracle" in e:
        return "Oracle"
    if "sqlserver" in e or "mssql" in e:
        return "SQLServer"
    return "MySQL"


def _aws_boto3_session(profile: dict):
    import boto3

    region = (profile.get("region", "") or "").strip() or None
    access_key = (profile.get("access_key_id") or "").strip()
    secret_key = (profile.get("secret_access_key") or "").strip()
    session_tok = (profile.get("session_token") or "").strip() or None
    profile_name = (profile.get("sso_profile") or "").strip() or None
    if access_key and secret_key:
        return boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_tok,
            region_name=region,
        )
    return boto3.Session(profile_name=profile_name, region_name=region)


def resolve_aws_rds_sql_endpoint(profile: dict) -> Optional[dict[str, str]]:
    """Return {host, port, db_type} for an AWS RDS instance id, or None."""
    instance_id = (profile.get("resource_name") or "").strip()
    if not instance_id:
        return None
    try:
        session = _aws_boto3_session(profile)
        rds = session.client("rds", region_name=session.region_name or None)
        resp = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
        instances = resp.get("DBInstances") or []
        if not instances:
            return None
        inst = instances[0]
        endpoint = inst.get("Endpoint") or {}
        engine = inst.get("Engine") or profile.get("db_engine") or ""
        return {
            "host": endpoint.get("Address") or "",
            "port": str(endpoint.get("Port") or 3306),
            "db_type": engine_to_db_type(engine),
        }
    except Exception:
        return None


def enrich_sql_connection(profile: dict, *, resolve_remote: bool = True) -> dict:
    """Fill missing sql_connection fields from the cloud API where possible."""
    sql = dict(profile.get("sql_connection") or {})
    provider = (profile.get("provider") or "").upper()

    if resolve_remote and not sql.get("host") and provider == "AWS":
        resolved = resolve_aws_rds_sql_endpoint(profile)
        if resolved:
            for key, val in resolved.items():
                if val and not sql.get(key):
                    sql[key] = val

    if not sql.get("db_type"):
        sql["db_type"] = engine_to_db_type(profile.get("db_engine", ""))

    if not sql.get("port"):
        sql["port"] = "3306"

    profile = dict(profile)
    profile["sql_connection"] = sql
    return profile


def sync_cloud_db_to_saved_connections(
    profile: dict,
    connection_manager: Any,
    *,
    resolve_remote: bool = True,
    persist: bool = True,
) -> tuple[bool, str]:
    """
    Mirror a cloud DB profile into ConnectionManager (saved_connections.json).

    Returns (success, user-facing message).
    """
    if profile.get("purpose") != PURPOSE_CONNECTIONS:
        return True, ""
    if profile.get("target_kind", TARGET_CLOUD_DB) != TARGET_CLOUD_DB:
        return True, ""

    profile = enrich_sql_connection(profile, resolve_remote=resolve_remote)
    sql = profile.get("sql_connection") or {}
    name = (profile.get("display_name") or "").strip()
    host = (sql.get("host") or "").strip()

    if not name:
        return False, "Cloud profile saved but display name is missing."

    if not host:
        return (
            False,
            f"Cloud profile '{name}' saved. SQL endpoint could not be resolved — "
            "enter SQL host/username in the wizard SQL section and save again, "
            "or add a manual connection with the RDS endpoint hostname.",
        )

    db_type = sql.get("db_type") or "MySQL"
    port = str(sql.get("port") or "3306")
    service_or_db = (sql.get("service_or_db") or sql.get("database") or "").strip()
    username = (sql.get("username") or "").strip()
    password = sql.get("password") or ""
    save_password = bool(password)
    from common.connection_params import ConnectionParams

    params = ConnectionParams(
        name=name,
        db_type=db_type,
        host=host,
        port=port,
        service_or_db=service_or_db,
        username=username,
        password=password,
        save_password=save_password,
    )

    if connection_manager.connection_exists(name):
        ok, msg = connection_manager.update_connection(
            name,
            params,
            persist=persist,
        )
    else:
        ok, msg = connection_manager.add_connection(
            params,
            persist=persist,
        )

    if ok:
        return (
            True,
            f"Cloud DB '{name}' saved — available under Load Saved "
            f"({host}:{port}). Enter DB username/password if needed, then Connect.",
        )
    return False, f"Cloud profile saved but SQL connection mirror failed: {msg}"


def sync_all_cloud_dbs_to_saved_connections(
    profiles: dict[str, dict],
    connection_manager: Any,
    *,
    resolve_remote: bool = True,
) -> bool:
    """
    Mirror many cloud DB profiles into ConnectionManager with one disk write.

    Returns True when mirrored data changed.
    """
    def _signature(conns):
        return {
            c["name"]: (
                c.get("db_type"),
                c.get("host"),
                str(c.get("port")),
                c.get("service_or_db"),
                c.get("username"),
            )
            for c in conns
        }

    before = _signature(connection_manager.get_all_connections())
    for profile in profiles.values():
        sync_cloud_db_to_saved_connections(
            profile,
            connection_manager,
            resolve_remote=resolve_remote,
            persist=False,
        )
    after = _signature(connection_manager.get_all_connections())
    if after != before:
        connection_manager.save_connections()
        return True
    return False
