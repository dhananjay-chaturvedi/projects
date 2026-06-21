"""Monitor-tab-only DB connections — isolation + CRUD + live metrics.

Verifies that DB connections added from the Monitor tab live in their own
store (``monitor_db.json``) and are invisible to the core Connections store
(and therefore to the SQL Editor / Data Migration / AI Query tabs), while the
Monitor module can still manage and monitor them via service/CLI/API.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

MYSQL_HOST = os.environ.get("MYSQL_TEST_HOST", "localhost")
MYSQL_PORT = os.environ.get("MYSQL_TEST_PORT", "3306")
MYSQL_USER = os.environ.get("MYSQL_TEST_USER", "dheeru")
MYSQL_PASS = os.environ.get("MYSQL_TEST_PASS", "dheeru")
MYSQL_DB = os.environ.get("MYSQL_TEST_DB", "test")
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def isolated_home(tmp_path, monkeypatch):
    """Point the dbassistant home at a throwaway dir for each test."""
    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    from common import paths as _paths

    if hasattr(_paths, "reset_bootstrap_state_for_tests"):
        _paths.reset_bootstrap_state_for_tests()
    _paths.bootstrap(force=True)
    return tmp_path


def _seed_core_and_monitor(name_core="core_only", name_mon="mon_only"):
    from common.connection_params import ConnectionParams
    from common.connection_manager import ConnectionManager
    from monitoring.monitor_db_connection_manager import MonitorDBConnectionManager

    core = ConnectionManager()
    core.add_connection(
        ConnectionParams.from_mapping({
            "name": name_core, "db_type": "MariaDB", "host": "localhost",
            "port": "3306", "service_or_db": "test",
            "username": "dheeru", "password": "dheeru",
            "save_password": True,
        }),
    )
    mon = MonitorDBConnectionManager()
    mon.add_connection(
        ConnectionParams.from_mapping({
            "name": name_mon, "db_type": "MariaDB", "host": "localhost",
            "port": "3306", "service_or_db": "test",
            "username": "dheeru", "password": "dheeru",
            "save_password": True,
        }),
    )
    return core, mon


def test_store_files_are_separate(isolated_home):
    _seed_core_and_monitor()
    from common import paths as _paths

    assert _paths.db_connections_path().exists()
    assert _paths.monitor_db_connections_path().exists()
    assert _paths.db_connections_path() != _paths.monitor_db_connections_path()


def test_monitor_conn_not_visible_to_core_store(isolated_home):
    """The core store (used by SQL Editor / Migration / AI Query) must never
    surface a Monitor-tab connection."""
    from common.connection_manager import ConnectionManager

    _seed_core_and_monitor()

    core_names = [c["name"] for c in ConnectionManager().get_all_connections()]
    assert core_names == ["core_only"]
    assert "mon_only" not in core_names

    # CoreDBService (backs SQL Editor / Migration / AI Query) is equally blind.
    from common.headless.db_service import CoreDBService

    svc_names = [c["name"] for c in CoreDBService().list_connections()]
    assert "mon_only" not in svc_names


def test_core_conn_still_visible_to_monitor(isolated_home):
    """Monitoring can still read core Connections-tab profiles (unchanged)."""
    _seed_core_and_monitor()
    from monitoring.service import make_service

    svc = make_service()
    all_rows = [r for r in svc.list_all_connections("all") if not r.get("error")]
    by_src = {(r["source"], r["name"]) for r in all_rows}
    assert ("db", "core_only") in by_src
    assert ("monitor-db", "mon_only") in by_src


def test_service_crud_and_source_resolution(isolated_home):
    from monitoring.service import make_service

    svc = make_service()
    r = svc.add_monitor_db_connection(
        name="svc_db", db_type="MariaDB", host="localhost", port="3306",
        user="dheeru", password="dheeru", database="test",
    )
    assert r["ok"], r

    names = [c["name"] for c in svc.list_monitor_db_connections()]
    assert names == ["svc_db"]
    assert svc.resolve_connection_source("svc_db") == "monitor-db"

    # Not present in the core list.
    core_names = [c["name"] for c in svc.list_connections()]
    assert "svc_db" not in core_names

    rm = svc.remove_monitor_db_connection("svc_db")
    assert rm["ok"], rm
    assert svc.list_monitor_db_connections() == []


def test_monitoring_core_connections_add_routes_to_monitor_store(isolated_home):
    """Regression: adding through the Monitoring CLI must not populate db.json."""
    env = os.environ.copy()
    cmd = [
        sys.executable, "-m", "monitoring", "connections", "add",
        "--name", "cli_compat", "--type", "MariaDB", "--host", "localhost",
        "--port", "3306", "--db", "test", "--user", "dheeru",
        "--password", "dheeru",
    ]
    p = subprocess.run(
        cmd, cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=30,
    )
    assert p.returncode == 0, p.stdout + p.stderr

    from common.connection_manager import ConnectionManager
    from monitoring.monitor_db_connection_manager import MonitorDBConnectionManager

    assert [c["name"] for c in ConnectionManager().get_all_connections()] == []
    assert [
        c["name"] for c in MonitorDBConnectionManager().get_all_connections()
    ] == ["cli_compat"]

    listed = subprocess.run(
        [sys.executable, "-m", "monitoring", "connections", "list"],
        cwd=str(ROOT), env=env, text=True, capture_output=True, timeout=30,
    )
    assert listed.returncode == 0, listed.stdout + listed.stderr
    assert "monitor-db" in listed.stdout
    assert "cli_compat" in listed.stdout


@pytest.mark.integration
def test_live_monitor_db_metrics(isolated_home, mysql_available):
    """End-to-end against the local MariaDB (dheeru/dheeru)."""
    from monitoring.service import make_service

    svc = make_service()
    add = svc.add_monitor_db_connection(
        name="live_db", db_type="MariaDB", host=MYSQL_HOST, port=str(MYSQL_PORT),
        user=MYSQL_USER, password=MYSQL_PASS, database=MYSQL_DB,
    )
    assert add["ok"], add

    test = svc.test_monitor_db_connection("live_db")
    assert test["ok"], test

    metrics = svc.get_metrics_monitor_db("live_db")
    assert metrics.get("error") is None, metrics
    assert metrics.get("sections"), "expected metric sections"
    section_titles = {t for t, _ in metrics.get("sections", [])}
    assert "Host / OS" not in section_titles, "OS metrics must be SSH-only"
    assert metrics.get("os_note") == ""

    any_r = svc.monitor_any("live_db")
    assert any_r.get("source") == "monitor-db"
    assert any_r.get("error") is None
    assert not any(a.get("source") == "os" for a in any_r.get("alerts", []))
