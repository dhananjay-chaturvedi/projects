"""Unit tests for unified SSL/TLS connection helpers."""

from __future__ import annotations

from common.drivers.ssl_support import (
    mysql_ssl_connect_kwargs,
    oracle_ssl_connect_kwargs,
    postgres_ssl_connect_kwargs,
    sqlserver_encryption_value,
    sqlserver_ssl_connect_kwargs,
    ssl_enabled,
)
from common.ssl_connect import ssl_connect_kwargs


def test_ssl_disabled_by_default():
    assert ssl_enabled(None, None) is False
    assert mysql_ssl_connect_kwargs(None, None) == {}
    assert postgres_ssl_connect_kwargs("disable", None) == {}


def test_mysql_verify_ca():
    kwargs = mysql_ssl_connect_kwargs("verify_ca", "/tmp/ca.pem")
    assert kwargs["ssl_ca"] == "/tmp/ca.pem"
    assert kwargs["ssl_verify_cert"] is True


def test_postgres_require_with_rootcert():
    kwargs = postgres_ssl_connect_kwargs("require", "/tmp/root.pem")
    assert kwargs["sslmode"] == "require"
    assert kwargs["sslrootcert"] == "/tmp/root.pem"


def test_sqlserver_encryption_mapping():
    assert sqlserver_encryption_value("require") == "require"
    assert sqlserver_encryption_value("disable") == "off"
    assert sqlserver_ssl_connect_kwargs(ssl_mode="require") == {"encryption": "require"}


def test_oracle_tcps_dsn_when_ssl_required():
    extra = oracle_ssl_connect_kwargs(
        host="db.example.com",
        port=1521,
        service_name="ORCLPDB1",
        ssl_mode="require",
    )
    assert "TCPS" in extra["dsn"]
    assert "ORCLPDB1" in extra["dsn"]


def test_ssl_connect_kwargs_from_profile():
    profile = {
        "host": "h",
        "ssl_mode": "require",
        "ssl_ca": "/tmp/ca.pem",
        "password": "x",
    }
    out = ssl_connect_kwargs(profile)
    assert out == {"ssl_mode": "require", "ssl_ca": "/tmp/ca.pem"}
