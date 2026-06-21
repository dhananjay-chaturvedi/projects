"""UI/CLI/API parity tests for the G1-G10 transfer options + dry-run."""

from __future__ import annotations

import argparse

import pytest


class FakeManager:
    def __init__(self, db_type="PostgreSQL"):
        self.db_type = db_type
        self.conn = object()


class FakeCore:
    def get_manager(self, name):
        return FakeManager()


# --------------------------------------------------------------------------- #
# Bridge wiring (drives the API/CLI services)
# --------------------------------------------------------------------------- #
def test_bridge_transfer_data_threads_options_and_writes_report(monkeypatch, tmp_path):
    from schema_converter.bridge import SchemaBridge
    from schema_converter.transfer_options import TransferRequest, options_from_mapping

    captured = {}

    def fake_transfer(src, tgt, table, target_table, **kw):
        runtime = kw.get("runtime")
        captured["options"] = runtime.options
        stats = runtime.stats_out
        if stats is not None:
            stats.update({"skipped": 3, "error_count": 1, "duration_seconds": 0.5,
                          "errors": [{"type": "row", "message": "x"}]})
        return 7

    monkeypatch.setattr("schema_converter.adapters.transfer_object", fake_transfer)
    report_file = tmp_path / "r.json"
    bridge = SchemaBridge(FakeCore())
    request = TransferRequest("src", "tgt", "users", target_table="test.users")
    options = options_from_mapping({
        "where": "status = 'active'",
        "limit": 100,
        "columns": "id,name",
        "column_map": "name:full_name",
        "continue_on_error": True,
        "overflow_policy": "truncate",
        "null_policy": "empty_to_null",
        "bool_policy": "int",
        "timezone_policy": "utc",
        "reset_sequences": True,
        "report_path": str(report_file),
    })
    result = bridge.transfer_data(
        request,
        options,
    )

    opts = captured["options"]
    assert opts.where == "status = 'active'"
    assert opts.limit == 100
    assert opts.columns == ("id", "name")
    assert opts.column_map == {"name": "full_name"}
    assert opts.continue_on_error is True
    assert opts.overflow_policy == "truncate"
    assert opts.null_policy == "empty_to_null"
    assert opts.bool_policy == "int"
    assert opts.timezone_policy == "utc"
    assert opts.reset_sequences is True

    assert result["ok"] is True
    assert result["rows_transferred"] == 7
    assert result["skipped"] == 3
    assert report_file.exists()
    assert result["report_path"] == str(report_file)


def test_bridge_validate_migration(monkeypatch):
    from schema_converter.bridge import SchemaBridge

    monkeypatch.setattr(
        "schema_converter.migration_validation.validate_migration",
        lambda src, tgt, pairs, **kw: {
            "ok": True,
            "tables": [{"source_table": p[0], "target_table": p[1]} for p in pairs],
            "summary": {"tables": len(pairs), "errors": 0, "warnings": 0},
        },
    )
    bridge = SchemaBridge(FakeCore())
    from schema_converter.table_naming import TargetNaming

    r = bridge.validate_migration(
        "src", "tgt", ["users", "orders"], naming=TargetNaming(target_db="test")
    )
    assert r["ok"] is True
    assert r["source_conn"] == "src"
    assert [t["target_table"] for t in r["tables"]] == ["test.users", "test.orders"]


# --------------------------------------------------------------------------- #
# CLI argument parsing
# --------------------------------------------------------------------------- #
def test_cli_transfer_data_parses_gap_flags():
    import schema_converter.cli as cli

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)

    args = parser.parse_args([
        "migrator", "transfer-data",
        "--source-conn", "src", "--target-conn", "tgt",
        "--table", "users",
        "--where", "id > 10",
        "--limit", "50",
        "--columns", "id,name",
        "--column-map", "name:full_name",
        "--continue-on-error",
        "--overflow-policy", "truncate",
        "--null-policy", "empty_to_null",
        "--bool-policy", "int",
        "--timezone-policy", "utc",
        "--reset-sequences",
        "--checkpoint",
        "--report", "/tmp/r.json",
    ])
    assert args.where == "id > 10"
    assert args.limit == 50
    assert args.columns == "id,name"
    assert args.column_map == "name:full_name"
    assert args.continue_on_error is True
    assert args.overflow_policy == "truncate"
    assert args.null_policy == "empty_to_null"
    assert args.bool_policy == "int"
    assert args.timezone_policy == "utc"
    assert args.reset_sequences is True
    assert args.checkpoint is True
    assert args.report_path == "/tmp/r.json"


def test_cli_validate_subcommand_parses():
    import schema_converter.cli as cli

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)
    args = parser.parse_args([
        "migrator", "validate",
        "--source-conn", "src", "--target-conn", "tgt",
        "--tables", "users,orders",
        "--type-map", "varchar2:text",
    ])
    assert args.schema_action == "validate"
    assert args.tables == "users,orders"
    assert args.type_map == "varchar2:text"


# --------------------------------------------------------------------------- #
# API request models + routes
# --------------------------------------------------------------------------- #
def test_api_transfer_data_forwards_gap_options():
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from schema_converter.api import build_router

    seen = {}

    class FakeSvc:
        def transfer_data(self, request, options):
            seen["request"] = request
            seen["options"] = options
            return {"ok": True, "rows_transferred": 1, "skipped": 0,
                    "source_table": request.table, "target_table": "test.users",
                    "message": "ok"}

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=FakeSvc()))
    client = TestClient(app)
    resp = client.post("/api/migrator/transfer-data", json={
        "source_conn": "src", "target_conn": "tgt", "table": "users",
        "where": "x=1", "limit": 10, "columns": "id", "overflow_policy": "skip",
        "continue_on_error": True, "timezone_policy": "naive",
    })
    assert resp.status_code == 200, resp.text
    assert seen["options"].where == "x=1"
    assert seen["options"].limit == 10
    assert seen["options"].overflow_policy == "skip"
    assert seen["options"].continue_on_error is True
    assert seen["options"].timezone_policy == "naive"


def test_api_validate_route():
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from schema_converter.api import build_router

    class FakeSvc:
        def validate_migration(self, source_conn, target_conn, tables, **kw):
            return {"ok": True, "tables": [], "summary": {"tables": len(tables),
                    "errors": 0, "warnings": 0}}

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=FakeSvc()))
    client = TestClient(app)
    resp = client.post("/api/migrator/validate", json={
        "source_conn": "src", "target_conn": "tgt", "tables": ["users"],
        "target_db": "test",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is True


def test_api_routes_convert_unexpected_service_exception_to_http_error():
    fastapi = pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from schema_converter.api import build_router

    class FakeSvc:
        def compare_data(self, *args, **kwargs):
            raise RuntimeError("database session disappeared")

    app = fastapi.FastAPI()
    app.include_router(build_router(svc=FakeSvc()))
    client = TestClient(app)
    resp = client.post("/api/migrator/compare-data", json={
        "source_conn": "src", "target_conn": "tgt", "table": "users",
    })
    assert resp.status_code == 500
    assert resp.json()["detail"] == "database session disappeared"


# --------------------------------------------------------------------------- #
# Single-table-only options + per-table limit (UI/CLI/API/config parity)
# --------------------------------------------------------------------------- #
def test_options_from_config_reads_moved_settings(monkeypatch):
    from schema_converter import module_config, transfer_options

    monkeypatch.setattr(
        module_config, "get_bool",
        lambda section, key, default=False: True if key in (
            "continue_on_error", "reset_sequences") else default,
    )
    opts = transfer_options.options_from_config()
    assert opts.continue_on_error is True
    assert opts.reset_sequences is True


def test_bridge_multi_applies_limit_per_table(monkeypatch):
    from schema_converter.bridge import SchemaBridge
    from schema_converter.transfer_options import (
        TransferMultiRequest,
        options_from_mapping,
    )

    captured = {}

    def fake_run(specs, context, options=None):
        captured["options"] = options
        captured["workers"] = context.workers
        return {"ok": True, "tables": [], "successful": 0, "failed": 0,
                "total_rows": 0, "workers": context.workers}

    monkeypatch.setattr(
        "schema_converter.parallel_transfer.run_parallel_transfer", fake_run
    )
    bridge = SchemaBridge(FakeCore())
    bridge.transfer_data_multi(
        TransferMultiRequest("src", "tgt", ["a", "b"]),
        options_from_mapping({"limit": 250, "column_map": "name:full_name"}),
    )
    assert captured["options"].limit == 250
    # Column rename applies to every selected table.
    assert captured["options"].column_map == {"name": "full_name"}
    # WHERE/columns are single-table only -> never set for multi.
    assert captured["options"].where == ""
    assert captured["options"].columns == ()


def test_transfer_data_multi_signature_allows_column_map_not_where():
    import inspect

    from schema_converter.bridge import SchemaBridge

    params = inspect.signature(SchemaBridge.transfer_data_multi).parameters
    assert list(params) == ["self", "request", "options"]


def test_api_multi_model_has_limit_and_column_map_not_single_table_fields():
    from schema_converter.api import DataTransferMultiRequest

    fields = DataTransferMultiRequest.model_fields
    assert "limit" in fields
    assert "column_map" in fields
    assert "where" not in fields
    assert "columns" not in fields


def test_cli_multi_rejects_single_table_only_flags(monkeypatch):
    import schema_converter.cli as cli

    class FakeSvc:
        def transfer_data(self, *a, **k):
            return {"ok": True, "message": "ok"}

        def transfer_data_multi(self, *a, **k):
            return {"ok": True, "tables": [], "successful": 0, "failed": 0,
                    "total_rows": 0, "workers": 1}

    monkeypatch.setattr(cli, "_schema_service", lambda: FakeSvc())

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)
    args = parser.parse_args([
        "migrator", "transfer-data",
        "--source-conn", "src", "--target-conn", "tgt",
        "--tables", "a,b", "--where", "id > 1",
    ])
    assert cli._transfer_data(args) == 1


def test_cli_multi_forwards_limit(monkeypatch):
    import schema_converter.cli as cli

    seen = {}

    class FakeSvc:
        def transfer_data(self, *a, **k):
            return {"ok": True, "message": "ok"}

        def transfer_data_multi(self, request, options):
            seen["request"] = request
            seen["options"] = options
            return {"ok": True, "tables": [], "successful": 0, "failed": 0,
                    "total_rows": 0, "workers": 1}

    monkeypatch.setattr(cli, "_schema_service", lambda: FakeSvc())

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)
    args = parser.parse_args([
        "migrator", "transfer-data",
        "--source-conn", "src", "--target-conn", "tgt",
        "--tables", "a,b", "--limit", "500",
    ])
    assert cli._transfer_data(args) == 0
    assert seen["options"].limit == 500


def test_cli_multi_forwards_column_map(monkeypatch):
    import schema_converter.cli as cli

    seen = {}

    class FakeSvc:
        def transfer_data(self, *a, **k):
            return {"ok": True, "message": "ok"}

        def transfer_data_multi(self, request, options):
            seen["request"] = request
            seen["options"] = options
            return {"ok": True, "tables": [], "successful": 0, "failed": 0,
                    "total_rows": 0, "workers": 1}

    monkeypatch.setattr(cli, "_schema_service", lambda: FakeSvc())

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    cli.register_cli(sub)
    args = parser.parse_args([
        "migrator", "transfer-data",
        "--source-conn", "src", "--target-conn", "tgt",
        "--tables", "a,b", "--column-map", "name:full_name",
    ])
    # Column rename is allowed for multi-table and forwarded to the service.
    assert cli._transfer_data(args) == 0
    assert seen["options"].column_map == {"name": "full_name"}
