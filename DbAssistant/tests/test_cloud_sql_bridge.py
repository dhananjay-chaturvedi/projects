"""Tests for cloud profile → SQL saved connection bridge."""

from __future__ import annotations

from unittest.mock import MagicMock

from common.cloud.profiles import PURPOSE_CONNECTIONS, TARGET_CLOUD_DB
from common.cloud.sql_bridge import (
    engine_to_db_type,
    enrich_sql_connection,
    sync_all_cloud_dbs_to_saved_connections,
    sync_cloud_db_to_saved_connections,
)


def test_engine_to_db_type():
    assert engine_to_db_type("mariadb") == "MariaDB"
    assert engine_to_db_type("aurora-postgresql") == "PostgreSQL"
    assert engine_to_db_type("mysql") == "MySQL"


def test_enrich_sql_connection_aws(monkeypatch):
    profile = {
        "provider": "AWS",
        "resource_name": "my-rds",
        "region": "ap-northeast-1",
        "purpose": PURPOSE_CONNECTIONS,
        "sql_connection": {"username": "dbuser"},
    }
    monkeypatch.setattr(
        "common.cloud.sql_bridge.resolve_aws_rds_sql_endpoint",
        lambda p: {"host": "my-rds.x.rds.amazonaws.com", "port": "3306", "db_type": "MariaDB"},
    )
    out = enrich_sql_connection(profile)
    assert out["sql_connection"]["host"] == "my-rds.x.rds.amazonaws.com"
    assert out["sql_connection"]["username"] == "dbuser"


def test_enrich_sql_connection_skips_remote_when_disabled(monkeypatch):
    profile = {
        "provider": "AWS",
        "resource_name": "my-rds",
        "region": "ap-northeast-1",
        "purpose": PURPOSE_CONNECTIONS,
        "sql_connection": {"username": "dbuser"},
    }

    def _fail(_profile):
        raise AssertionError("resolve_aws_rds_sql_endpoint should not run")

    monkeypatch.setattr(
        "common.cloud.sql_bridge.resolve_aws_rds_sql_endpoint",
        _fail,
    )
    out = enrich_sql_connection(profile, resolve_remote=False)
    assert out["sql_connection"].get("host") in (None, "")


def test_sync_all_cloud_dbs_to_saved_connections_batches_save():
    cm = MagicMock()
    cm.get_all_connections.side_effect = [
        [],
        [
            {
                "name": "aws-pushdb-dev",
                "db_type": "MariaDB",
                "host": "dev-rds.example.com",
                "port": "3306",
                "service_or_db": "pushdb",
                "username": "app",
            }
        ],
    ]
    cm.connection_exists.return_value = False
    cm.add_connection.return_value = (True, "ok")

    profile = {
        "display_name": "aws-pushdb-dev",
        "purpose": PURPOSE_CONNECTIONS,
        "target_kind": TARGET_CLOUD_DB,
        "provider": "AWS",
        "sql_connection": {
            "db_type": "MariaDB",
            "host": "dev-rds.example.com",
            "port": "3306",
            "service_or_db": "pushdb",
            "username": "app",
            "password": "secret",
        },
    }
    changed = sync_all_cloud_dbs_to_saved_connections(
        {"aws-pushdb-dev": profile}, cm, resolve_remote=False
    )
    assert changed is True
    cm.add_connection.assert_called_once()
    assert cm.add_connection.call_args.kwargs["persist"] is False
    cm.save_connections.assert_called_once()


def test_sync_cloud_db_to_saved_connections_adds_entry():
    cm = MagicMock()
    cm.connection_exists.return_value = False
    cm.add_connection.return_value = (True, "ok")
    cm.load_connections.return_value = []

    profile = {
        "display_name": "aws-pushdb-dev",
        "purpose": PURPOSE_CONNECTIONS,
        "target_kind": TARGET_CLOUD_DB,
        "provider": "AWS",
        "sql_connection": {
            "db_type": "MariaDB",
            "host": "dev-rds.example.com",
            "port": "3306",
            "service_or_db": "pushdb",
            "username": "app",
            "password": "secret",
        },
    }
    ok, msg = sync_cloud_db_to_saved_connections(profile, cm)
    assert ok is True
    assert "Load Saved" in msg
    cm.add_connection.assert_called_once()
    params = cm.add_connection.call_args[0][0]
    assert params.name == "aws-pushdb-dev"
    assert params.host == "dev-rds.example.com"


def test_sync_skips_monitor_profiles():
    cm = MagicMock()
    profile = {"display_name": "x", "purpose": "monitor", "target_kind": TARGET_CLOUD_DB}
    ok, msg = sync_cloud_db_to_saved_connections(profile, cm)
    assert ok is True
    assert msg == ""
    cm.add_connection.assert_not_called()
