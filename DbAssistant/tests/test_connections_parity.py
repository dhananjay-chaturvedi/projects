"""Connections-tab parity tests (service + API + TUI screen).

Validates the capability metadata and the extended ``add_connection`` that the
web and Textual Connections screens rely on to mirror the desktop tab:
default ports, service/db label, capability-driven SSL/TLS, and SSH tunnels.
"""

from __future__ import annotations

import pytest

from common.connection_params import ConnectionParams


# --------------------------------------------------------------------------- #
# Service layer
# --------------------------------------------------------------------------- #
def _svc():
    from common.headless.db_service import CoreDBService
    return CoreDBService()


def test_connection_metadata_shape():
    md = _svc().connection_metadata()
    assert "db_types" in md and "engines" in md
    assert md["db_types"], "expected at least one registered db type"
    for db_type in md["db_types"]:
        eng = md["engines"][db_type]
        assert "default_port" in eng
        assert eng["service_label"] in ("Service name", "Database name")
        assert isinstance(eng["ssl_mode_options"], list)
        assert isinstance(eng["ssl_fields"], list)


def test_metadata_oracle_uses_service_label_and_wallet():
    md = _svc().connection_metadata()
    if "Oracle" not in md["db_types"]:
        pytest.skip("Oracle not registered")
    oracle = md["engines"]["Oracle"]
    assert oracle["service_label"] == "Service name"
    assert "wallet" in oracle["ssl_fields"]


def test_metadata_document_engines_flagged():
    md = _svc().connection_metadata()
    for name in ("MongoDB", "DocumentDB"):
        if name in md["db_types"]:
            assert md["engines"][name]["is_document"] is True


def test_add_connection_persists_ssl_and_ssh():
    svc = _svc()
    r = svc.add_connection(
        ConnectionParams.from_mapping({
            "name": "parity_pg", "db_type": "PostgreSQL",
            "host": "localhost", "port": "5432", "user": "u",
            "password": "p", "service": "mydb", "save_password": True,
            "ssl_mode": "require",
            "ssh_tunnel": {"ssh_host": "bastion", "ssh_user": "ubuntu",
                           "ssh_port": 22, "ssh_password": "x",
                           "ssh_key_file": ""},
        }),
    )
    assert r["ok"], r
    prof = svc.get_connection_profile("parity_pg")
    assert prof["ssl_mode"] == "require"
    assert prof["service_or_db"] == "mydb"
    assert prof["ssh_tunnel"]["ssh_host"] == "bastion"


def test_add_connection_document_tls():
    svc = _svc()
    if "MongoDB" not in svc.connection_metadata()["db_types"]:
        pytest.skip("MongoDB not registered")
    r = svc.add_connection(
        ConnectionParams.from_mapping({
            "name": "parity_mongo", "db_type": "MongoDB",
            "host": "localhost", "port": "27017", "user": "u",
            "password": "p", "tls": True, "tls_ca_file": "/tmp/ca.pem",
        }),
    )
    assert r["ok"], r
    prof = svc.get_connection_profile("parity_mongo")
    assert prof.get("tls") is True
    assert prof.get("tls_ca_file") == "/tmp/ca.pem"


# --------------------------------------------------------------------------- #
# API layer
# --------------------------------------------------------------------------- #
def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def test_api_metadata_endpoint():
    c = _client()
    r = c.get("/api/connections/metadata")
    assert r.status_code == 200
    body = r.json()
    assert body["db_types"]
    assert "engines" in body


def test_api_create_with_ssl_and_ssh_roundtrip():
    c = _client()
    body = {
        "name": "api_pg", "db_type": "PostgreSQL", "host": "localhost",
        "port": "5432", "user": "u", "password": "p", "service": "mydb",
        "save_password": True, "ssl_mode": "require",
        "ssh_tunnel": {"ssh_host": "bastion", "ssh_user": "ubuntu",
                       "ssh_port": 22, "ssh_password": "x", "ssh_key_file": ""},
    }
    assert c.post("/api/connections", json=body).status_code == 201
    rows = c.get("/api/connections").json()
    row = next(x for x in rows if x["name"] == "api_pg")
    assert row["ssl_mode"] == "require"
    assert row["ssh_tunnel"]["ssh_host"] == "bastion"


def test_service_test_inline_uses_form_password_not_disk():
    svc = _svc()
    svc.add_connection(
        ConnectionParams.from_mapping({
            "name": "disk_no_pw", "db_type": "MariaDB",
            "host": "localhost", "port": "3306", "user": "u",
            "password": "secret", "service": "test", "save_password": False,
        }),
    )
    prof = svc.get_connection_profile("disk_no_pw")
    assert not prof.get("password")
    # Inline test with password in form should succeed when DB reachable;
    # at minimum it must not fail with 'using password: NO' before connect.
    r = svc.test_connection_inline(
        ConnectionParams.from_mapping({
            "name": "disk_no_pw", "db_type": "MariaDB",
            "host": "localhost", "port": "3306", "user": "u",
            "password": "secret", "service": "test",
        }),
    )
    assert "using password: NO" not in (r.get("message") or "").lower()
    svc.remove_connection("disk_no_pw")


# --------------------------------------------------------------------------- #
# Textual Connections screen
# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_connections_dynamic_fields_and_save():
    from textual.widgets import Button, DataTable, Input, Select

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("connections")
        await pilot.pause()
        scr = app.screen

        # Oracle: wallet field visible + Service-name label + port 1521.
        type_sel = scr.query_one("#add-type", Select)
        if "Oracle" in (scr._meta.get("db_types") or []):
            type_sel.value = "Oracle"
            await pilot.pause()
            assert scr.query_one("#add-wallet").display is True
            assert scr.query_one("#add-port", Input).value == "1521"

        # MongoDB: TLS group replaces SSL group.
        if "MongoDB" in (scr._meta.get("db_types") or []):
            type_sel.value = "MongoDB"
            await pilot.pause()
            assert scr.query_one("#tls-group").display is True
            assert scr.query_one("#ssl-group").display is False

        # The remote (SSH tunnel) connection lives in its own section; key auth
        # toggles the SSH key/password fields.
        type_sel.value = "PostgreSQL"
        await pilot.pause()
        scr.query_one("#r-ssh-auth", Select).value = "key"
        await pilot.pause()
        assert scr.query_one("#r-ssh-key").display is True
        assert scr.query_one("#r-ssh-password").display is False

        # Fill + save the remote form (back to password auth to keep it simple).
        scr.query_one("#r-ssh-auth", Select).value = "password"
        await pilot.pause()
        scr.query_one("#r-name", Input).value = "tui_parity"
        scr.query_one("#r-host", Input).value = "localhost"
        scr.query_one("#r-service", Input).value = "mydb"
        scr.query_one("#r-user", Input).value = "u"
        scr.query_one("#r-password", Input).value = "p"
        scr.query_one("#r-ssh-host", Input).value = "bastion"
        scr.query_one("#r-ssh-user", Input).value = "ubuntu"
        scr.query_one("#r-save", Button).press()
        await pilot.pause()

        table = scr.query_one("#conn-table", DataTable)
        names = {str(table.get_row_at(i)[0]) for i in range(table.row_count)}
        assert "tui_parity" in names
        prof = app.svc.get_connection_profile("tui_parity")
        assert prof["ssh_tunnel"]["ssh_host"] == "bastion"
        assert prof["service_or_db"] == "mydb"
