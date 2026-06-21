"""
Shared pytest fixtures for DbManagementTool test suite.
"""

from __future__ import annotations

import os
import sys
import tempfile
import types
from pathlib import Path

import pytest

# Project root on sys.path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _register_legacy_module_shims() -> None:
    """Map legacy flat module names used by older tests to current packages."""
    import importlib

    _ALIASES = {
        "conMysql": "common.drivers.conMysql",
        "conMariadb": "common.drivers.conMariadb",
        "conPostgres": "common.drivers.conPostgres",
        "conSQLite": "common.drivers.conSQLite",
        "conOracle": "common.drivers.conOracle",
        "conSqlServer": "common.drivers.conSqlServer",
        "conMongo": "common.drivers.conMongo",
        "monitoring_utils": "monitoring.monitoring_utils",
        "db_metric_config": "monitoring.db_metric_config",
        "db_os_collector": "monitoring.db_os_collector",
        "send_notification": "monitoring.send_notification",
    }
    for alias, target in _ALIASES.items():
        if alias not in sys.modules:
            try:
                sys.modules[alias] = importlib.import_module(target)
            except ImportError:
                pass


_register_legacy_module_shims()


@pytest.fixture(autouse=True)
def isolated_dbassistant_home(tmp_path, monkeypatch):
    """Keep tests away from the developer's real ~/.dbassistant store.

    Tests or live smoke runs can opt into a specific store by setting
    DBASSISTANT_HOME before pytest starts. Otherwise each test gets a fresh
    temp home that subprocess CLI/API calls inherit from the environment.
    """
    if os.environ.get("DBASSISTANT_HOME"):
        yield
        return

    home = tmp_path / "dbassistant-home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DBASSISTANT_HOME", str(home))

    from common import paths as _paths

    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()
    try:
        yield
    finally:
        _paths.reset_bootstrap_state_for_tests()

# ── Standard MySQL integration credentials ───────────────────────────────────
MYSQL_HOST = os.environ.get("MYSQL_TEST_HOST", "localhost")
MYSQL_PORT = int(os.environ.get("MYSQL_TEST_PORT", "3306"))
MYSQL_USER = os.environ.get("MYSQL_TEST_USER", "dheeru")
MYSQL_PASS = os.environ.get("MYSQL_TEST_PASS", "dheeru")
MYSQL_DB = os.environ.get("MYSQL_TEST_DB", "test")


def pytest_configure(config):
    if not os.environ.get("DBASSISTANT_HOME"):
        home = Path(tempfile.mkdtemp(prefix="dbassistant-pytest-"))
        os.environ["DBASSISTANT_HOME"] = str(home)
    config.addinivalue_line(
        "markers", "integration: tests requiring live external services"
    )


# ── MySQL availability ───────────────────────────────────────────────────────

def _mysql_reachable() -> bool:
    try:
        import mysql.connector

        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            connection_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def mysql_available():
    if not _mysql_reachable():
        pytest.skip(
            f"MySQL not reachable at {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        )


@pytest.fixture
def mysql_connect_kwargs(mysql_available):
    return {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASS,
        "database": MYSQL_DB,
    }


# ── AWS availability ───────────────────────────────────────────────────────────

def _aws_reachable() -> bool:
    try:
        import boto3

        boto3.client("sts").get_caller_identity()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def aws_available():
    if not _aws_reachable():
        pytest.skip("AWS credentials not available (~/.aws or env)")


# ── GCP ADC availability ───────────────────────────────────────────────────────

def _gcp_adc_available() -> bool:
    adc = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    return adc.is_file()


@pytest.fixture(scope="session")
def gcp_adc_available():
    if not _gcp_adc_available():
        pytest.skip("GCP application_default_credentials.json not found")


# ── Minimal ServerMonitorUI liveness helper host (no Tk) ────────────────────────

@pytest.fixture
def liveness_ui():
    """Object with bound ServerMonitorUI liveness helper methods, no Tk init."""
    from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI

    ui = types.SimpleNamespace(
        refresh_interval=5000,
        _db_metric_skip_ping_if_used_within=0,
        _cloud_health_skip_if_used_within=0,
        _ssh_keepalive_skip_if_used_within=0,
        _cloud_force_refresh_interval=1800,
        _db_last_metric_at={},
        _ssh_last_cmd_ok_at={},
        _cloud_last_ok_at={},
        _cloud_consecutive_failures={},
        _cloud_needs_refresh={},
        _db_sections_cache={},
        _db_os_note_cache={},
        _threshold_checker=None,
    )
    cls = ServerMonitorUI
    for name in (
        "_liveness_window_seconds",
        "_should_skip_liveness",
        "_seconds_until_expiry",
        "_cloud_should_refresh_keepalive",
        "_clear_cloud_liveness_state",
    ):
        meth = getattr(cls, name)
        setattr(ui, name, types.MethodType(meth, ui))
    return ui


@pytest.fixture
def tmp_config_dir(tmp_path, monkeypatch):
    """Isolated storage root for the DBAssistant tool.

    Sets ``DBASSISTANT_HOME`` to a per-test temp directory and resets the
    in-process bootstrap state so :func:`common.paths.bootstrap` runs
    fresh against the override. The returned path points at the
    *connections* subdir (used by older tests that built filenames
    underneath it directly).
    """
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DBASSISTANT_HOME", str(home))

    from common import paths as _paths

    _paths.reset_bootstrap_state_for_tests()
    _paths.ensure_layout()
    return _paths.connections_dir()


@pytest.fixture
def dbassistant_home(tmp_path, monkeypatch):
    """Isolated storage root for tests that need the bare home directory."""
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DBASSISTANT_HOME", str(home))

    from common import paths as _paths

    _paths.reset_bootstrap_state_for_tests()
    return home


# ── Comprehensive integration fixtures ───────────────────────────────────────

@pytest.fixture(scope="session")
def saved_db_connection_names():
    """Names from ~/.dbmanager or DBTOOL_TEST_CONNS."""
    from tests.integration_helpers import load_saved_db_connection_names

    return load_saved_db_connection_names()


@pytest.fixture(scope="session")
def saved_cloud_connection_names():
    """Cloud profile names from config or DBTOOL_TEST_CLOUD_CONNS."""
    from tests.integration_helpers import load_saved_cloud_connection_names

    return load_saved_cloud_connection_names()


@pytest.fixture
def mysql_svc(tmp_config_dir, mysql_available, mysql_connect_kwargs):
    """CoreDBService with ephemeral MySQL connection in isolated config dir."""
    from common.connection_params import ConnectionParams
    from common.headless.db_service import CoreDBService

    svc = CoreDBService()
    kw = mysql_connect_kwargs
    name = "pytest_mysql_ephemeral"
    result = svc.add_connection(
        ConnectionParams.from_mapping({
            "name": name,
            "db_type": "MySQL",
            "host": kw["host"],
            "port": kw["port"],
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }),
    )
    assert result["ok"], result.get("message")
    yield svc, name
    try:
        svc.disconnect_all()
    except Exception:
        pass


@pytest.fixture
def mysql_raw_conn(mysql_available, mysql_connect_kwargs):
    """Direct mysql.connector connection for DDL setup."""
    import mysql.connector

    kw = mysql_connect_kwargs
    conn = mysql.connector.connect(**kw, connection_timeout=10)
    yield conn
    try:
        conn.close()
    except Exception:
        pass


@pytest.fixture
def schema_test_tables(mysql_raw_conn):
    """Create paired source/target tables for schema/data compare tests."""
    from tests.integration_helpers import (
        SCHEMA_MIRROR_TABLE,
        SCHEMA_TEST_TABLE,
        setup_mysql_table,
        teardown_mysql_table,
    )

    src = setup_mysql_table(mysql_raw_conn, SCHEMA_TEST_TABLE)
    tgt = setup_mysql_table(mysql_raw_conn, SCHEMA_MIRROR_TABLE)
    yield src, tgt
    teardown_mysql_table(mysql_raw_conn, src)
    teardown_mysql_table(mysql_raw_conn, tgt)


@pytest.fixture
def api_client(monkeypatch):
    """FastAPI TestClient for full master API.

    Hermetic: clears ``DBTOOL_API_KEY`` so tests don't 401 against the
    dev's real ``.env`` setting. The module-level ``app`` reads the env
    once at import time, so by the time the fixture runs the app may
    already have captured a key. Rebuild a fresh app per test instead.
    """
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    from common.headless.db_service import CoreDBService

    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    svc = CoreDBService()
    return TestClient(create_app(svc=svc))


@pytest.fixture
def core_api_client(tmp_config_dir, mysql_available, mysql_connect_kwargs, monkeypatch):
    """TestClient backed by composite service with ephemeral MySQL.

    Hermetic: clear ``DBTOOL_API_KEY`` so the API doesn't 401 against the
    dev's local ``.env`` setting.
    """
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.connection_params import ConnectionParams
    from common.headless.app_factory import create_app
    from common.headless.db_service import CoreDBService
    from schema_converter.bridge import make_service

    monkeypatch.delenv("DBTOOL_API_KEY", raising=False)
    core = CoreDBService()
    kw = mysql_connect_kwargs
    core.add_connection(
        ConnectionParams.from_mapping({
            "name": "api_mysql",
            "db_type": "MySQL",
            "host": kw["host"],
            "port": kw["port"],
            "user": kw["user"],
            "password": kw["password"],
            "database": kw["database"],
        }),
    )
    svc = make_service(core)
    app = create_app(svc=svc)
    client = TestClient(app)
    yield client, svc, "api_mysql"
    try:
        svc.disconnect_all()
    except Exception:
        pass
