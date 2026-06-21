"""Phase 2 tests for configurable parallel data transfer."""

from __future__ import annotations

import argparse
import threading
import time

import pytest


class FakeManager:
    def __init__(self, name="mgr"):
        self.name = name
        self.db_type = "PostgreSQL"
        self.conn = object()
        self.disconnect_calls = 0

    def disconnect(self):
        self.disconnect_calls += 1
        self.conn = None


def test_parallel_runner_uses_bounded_workers_and_disconnects(monkeypatch):
    import schema_converter.parallel_transfer as pt

    active = {"count": 0, "max": 0}
    active_lock = threading.Lock()
    managers: list[FakeManager] = []

    def fake_transfer(
        source_manager,
        target_manager,
        source_table,
        target_table,
        runtime=None,
    ):
        with active_lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        time.sleep(0.03)
        if runtime and runtime.progress_callback:
            runtime.progress_callback(2, 2)
        with active_lock:
            active["count"] -= 1
        return 2

    def factory(name):
        mgr = FakeManager(name)
        managers.append(mgr)
        return mgr

    monkeypatch.setattr("schema_converter.adapters.transfer_object", fake_transfer)
    monkeypatch.setattr(
        "schema_converter.converter.DataConverter.get_row_count",
        lambda self, table, is_source=True: 2,
    )
    specs = pt.build_transfer_specs(["s.t1", "s.t2", "s.t3"], lambda t: f"test.{t.split('.')[-1]}")
    from schema_converter.transfer_options import ParallelTransferContext

    context = ParallelTransferContext(
        source_conn="src",
        target_conn="tgt",
        source_manager_factory=factory,
        target_manager_factory=factory,
        batch_size=100,
        workers=2,
    )
    result = pt.run_parallel_transfer(
        specs,
        context,
    )

    assert result["ok"] is True
    assert result["workers"] == 2
    assert result["successful"] == 3
    assert result["total_rows"] == 6
    assert active["max"] <= 2
    assert len(managers) == 6  # source + target per table
    assert all(m.disconnect_calls == 1 for m in managers)


def test_bridge_transfer_data_multi_uses_open_session(monkeypatch):
    from schema_converter.bridge import SchemaBridge

    opened = []

    class FakeCore:
        def open_session(self, name):
            mgr = FakeManager(name)
            opened.append((name, mgr))
            return mgr

    def fake_transfer(
        source_manager,
        target_manager,
        source_table,
        target_table,
        runtime=None,
    ):
        return 1

    monkeypatch.setattr("schema_converter.adapters.transfer_object", fake_transfer)
    monkeypatch.setattr(
        "schema_converter.converter.DataConverter.get_row_count",
        lambda self, table, is_source=True: 1,
    )
    bridge = SchemaBridge(FakeCore())
    from schema_converter.table_naming import TargetNaming
    from schema_converter.transfer_options import TransferMultiRequest

    result = bridge.transfer_data_multi(
        TransferMultiRequest(
            "src_conn",
            "tgt_conn",
            ["public.a", "public.b"],
            naming=TargetNaming(target_db="test"),
            parallel=True,
            workers=2,
        ),
    )

    assert result["ok"] is True
    assert result["parallel"] is True
    assert [row["target_table"] for row in result["tables"]] == ["test.a", "test.b"]
    assert [name for name, _ in opened].count("src_conn") == 2
    assert [name for name, _ in opened].count("tgt_conn") == 2
    assert all(mgr.disconnect_calls == 1 for _, mgr in opened)


def test_migrator_api_transfer_data_multi_forwards_parallel_options():
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from schema_converter.api import build_router

    seen = {}

    class FakeSvc:
        def transfer_data_multi(self, request, options):
            seen["request"] = request
            seen["options"] = options
            return {
                "ok": True,
                "error": None,
                "tables": [{"source_table": request.tables[0], "target_table": "test.a", "ok": True}],
                "successful": 1,
                "failed": 0,
                "total_rows": 1,
                "workers": request.workers,
            }

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=FakeSvc()))
    client = TestClient(app)

    resp = client.post("/api/migrator/transfer-data-multi", json={
        "source_conn": "src",
        "target_conn": "tgt",
        "tables": ["public.a"],
        "target_db": "test",
        "parallel": True,
        "workers": 3,
        "batch_size": 500,
    })

    assert resp.status_code == 200, resp.text
    assert seen["request"].tables == ["public.a"]
    assert seen["request"].naming.target_db == "test"
    assert seen["request"].parallel is True
    assert seen["request"].workers == 3
    assert seen["request"].batch_size == 500


def test_cli_transfer_data_accepts_tables_parallel_workers():
    import schema_converter.cli as cli

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)

    args = parser.parse_args([
        "migrator",
        "transfer-data",
        "--source-conn", "src",
        "--target-conn", "tgt",
        "--tables", "public.a,public.b",
        "--target-db", "test",
        "--parallel",
        "--workers", "4",
    ])

    assert args.schema_action == "transfer-data"
    assert args.tables == "public.a,public.b"
    assert args.table == ""
    assert args.target_db == "test"
    assert args.parallel is True
    assert args.workers == 4


def test_parallel_workers_setting_is_exposed():
    from schema_converter import module_config

    assert module_config.DEFAULTS["schema.conversion"]["parallel_workers"] == "1"
    assert module_config.get_int("schema.conversion", "parallel_workers", default=99) >= 1
