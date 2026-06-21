"""Phase 0 parity tests — migration target-DB qualification and monitor-DB SSH
tunnel must work through the CLI/API/service layers, not just the UI.

These use fakes/mocks so they run headless without real DB or SSH access.
"""

from __future__ import annotations

import pytest

from schema_converter.table_naming import base_table_name, qualify_target_table


# --------------------------------------------------------------------------- #
# Shared naming helper (used identically by UI, CLI, API)
# --------------------------------------------------------------------------- #
class TestTableNaming:
    def test_base_strips_schema_and_quotes(self):
        assert base_table_name("public.orders") == "orders"
        assert base_table_name('"public"."orders"') == "orders"
        assert base_table_name("`db`.`t`") == "t"
        assert base_table_name("[dbo].[t]") == "t"
        assert base_table_name("orders") == "orders"
        assert base_table_name("") == ""

    def test_qualify_with_target_db(self):
        assert qualify_target_table("public.orders", "test") == "test.orders"

    def test_qualify_with_prefix_suffix(self):
        assert (
            qualify_target_table("public.orders", "test", "mig_", "_copy")
            == "test.mig_orders_copy"
        )

    def test_qualify_without_target_db_is_bare(self):
        assert qualify_target_table("public.orders") == "orders"
        assert qualify_target_table("public.orders", "", "p_", "_s") == "p_orders_s"


# --------------------------------------------------------------------------- #
# Bridge: convert + transfer must honour target_db/prefix/suffix
# --------------------------------------------------------------------------- #
class _CaptureBridge:
    """A SchemaBridge whose convert/transfer record the qualified target."""


def test_bridge_convert_multi_builds_qualified_name_map(monkeypatch):
    from schema_converter.bridge import SchemaBridge
    from schema_converter.table_naming import TargetNaming

    bridge = SchemaBridge(core=object())
    seen = []

    def fake_convert(source_conn, target_db_type, table, **kw):
        target_db = kw["naming"].target_db
        seen.append((table, target_db, kw.get("table_name_map")))
        return {"error": None, "ddl": "", "indexes_ddl": [], "all_ddl": [],
                "issues": [], "target_table": f"{target_db}.{table.split('.')[-1]}"}

    monkeypatch.setattr(bridge, "convert_schema", fake_convert)
    r = bridge.convert_schema_multi(
        "src", "MariaDB", ["public.orders", "public.items"],
        naming=TargetNaming(target_db="test"),
    )
    # Every table converted with the shared map and target_db.
    assert all(db == "test" for _, db, _ in seen)
    shared_map = seen[0][2]
    assert shared_map == {"public.orders": "test.orders",
                          "public.items": "test.items"}
    # Reported per-table target_table is qualified.
    targets = {row["table"]: row["target_table"] for row in r["tables"]}
    assert targets["public.orders"] == "test.orders"


def test_bridge_transfer_data_auto_qualifies_target(monkeypatch):
    from schema_converter.bridge import SchemaBridge

    captured = {}

    class FakeMgr:
        db_type = "PostgreSQL"

    class FakeCore:
        def get_manager(self, name):
            return FakeMgr()

    def fake_transfer(source_manager, target_manager, source_name, target_name, **kw):
        captured["table"] = source_name
        captured["target_table"] = target_name
        return 5

    monkeypatch.setattr(
        "schema_converter.adapters.transfer_object", fake_transfer
    )
    bridge = SchemaBridge(core=FakeCore())
    from schema_converter.table_naming import TargetNaming
    from schema_converter.transfer_options import TransferRequest

    r = bridge.transfer_data(
        TransferRequest(
            "src", "tgt", "public.orders", naming=TargetNaming(target_db="test")
        ),
    )
    assert r["ok"] is True
    assert captured["target_table"] == "test.orders"
    assert r["target_table"] == "test.orders"


def test_bridge_transfer_data_explicit_target_overrides_db(monkeypatch):
    from schema_converter.bridge import SchemaBridge

    captured = {}

    class FakeMgr:
        db_type = "PostgreSQL"

    class FakeCore:
        def get_manager(self, name):
            return FakeMgr()

    def fake_transfer(source_manager, target_manager, source_name, target_name, **kw):
        captured["target_table"] = target_name
        return 1

    monkeypatch.setattr(
        "schema_converter.adapters.transfer_object", fake_transfer
    )
    bridge = SchemaBridge(core=FakeCore())
    from schema_converter.table_naming import TargetNaming
    from schema_converter.transfer_options import TransferRequest

    bridge.transfer_data(
        TransferRequest(
            "src", "tgt", "public.orders",
            target_table="other.explicit", naming=TargetNaming(target_db="test"),
        ),
    )
    assert captured["target_table"] == "other.explicit"


# --------------------------------------------------------------------------- #
# Migrator API wiring: target_db must reach the service
# --------------------------------------------------------------------------- #
def _migrator_client(fake_svc):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from schema_converter.api import build_router

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=fake_svc))
    return TestClient(app)


def test_migrator_api_convert_forwards_target_db():
    seen = {}

    class FakeSvc:
        def convert_schema(self, source_conn, target_type, table, **kw):
            seen.update(kw)
            seen["table"] = table
            return {"error": None, "ddl": "CREATE TABLE test.users(...)",
                    "indexes_ddl": [], "all_ddl": [], "issues": [],
                    "target_table": "test.users"}

    client = _migrator_client(FakeSvc())
    resp = client.post("/api/migrator/convert", json={
        "source_conn": "src", "target_type": "MariaDB", "table": "public.users",
        "target_db": "test", "prefix": "", "suffix": "",
    })
    assert resp.status_code == 200, resp.text
    assert seen["naming"].target_db == "test"
    assert resp.json()["target_table"] == "test.users"


def test_migrator_api_transfer_forwards_target_db():
    seen = {}

    class FakeSvc:
        def transfer_data(self, request, options):
            seen["request"] = request
            seen["options"] = options
            return {"ok": True, "rows_transferred": 3, "source_table": request.table,
                    "target_table": "test.users", "message": "ok"}

    client = _migrator_client(FakeSvc())
    resp = client.post("/api/migrator/transfer-data", json={
        "source_conn": "src", "target_conn": "tgt", "table": "public.users",
        "target_db": "test",
    })
    assert resp.status_code == 200, resp.text
    assert seen["request"].naming.target_db == "test"


# --------------------------------------------------------------------------- #
# Monitoring: monitor-DB add must forward ssh_tunnel through service + API
# --------------------------------------------------------------------------- #
def test_monitor_service_add_db_forwards_ssh_tunnel():
    from monitoring.service import MonitorService

    captured = {}

    class FakeCore:
        def add_connection(self, params):
            captured.update(params.to_profile(include_password=True))
            return {"ok": True, "message": "saved"}

    svc = MonitorService.__new__(MonitorService)
    svc._monitor_db_core = lambda: FakeCore()  # type: ignore[attr-defined]
    tunnel = {"ssh_host": "bastion", "ssh_user": "ubuntu", "ssh_port": 22,
              "ssh_password": "", "ssh_key_file": "/k"}
    r = svc.add_monitor_db_connection(
        name="m", db_type="MariaDB", host="localhost", port="3306",
        user="root", ssh_tunnel=tunnel,
    )
    assert r["ok"] is True
    assert captured["ssh_tunnel"] == tunnel


def test_monitor_api_add_db_forwards_ssh_tunnel():
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from monitoring.api import build_router

    seen = {}

    class FakeSvc:
        def add_monitor_db_connection(self, params):
            seen["params"] = params
            return {"ok": True, "message": "saved"}

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=FakeSvc()))
    client = TestClient(app)
    resp = client.post("/api/monitor/db-connections", json={
        "name": "m", "db_type": "MariaDB", "host": "localhost", "port": "3306",
        "username": "root", "password": "",
        "ssh_tunnel": {"ssh_host": "bastion", "ssh_user": "ubuntu",
                       "ssh_port": 22, "ssh_password": "", "ssh_key_file": ""},
    })
    assert resp.status_code == 201, resp.text
    assert seen["params"].ssh_tunnel["ssh_host"] == "bastion"
