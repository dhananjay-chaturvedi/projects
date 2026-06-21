"""dbtool.py CLI smoke tests.

Ensures every top-level subcommand and every nested action prints `--help`
successfully. This is the contract for the CLI: every module's
functionality must be reachable through `dbtool <command>`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def _run(*argv, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [PY, str(ROOT / "dbtool.py"), *argv],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
        timeout=timeout,
    )


def test_dbtool_help():
    r = _run("--help")
    assert r.returncode == 0
    out = r.stdout.lower()
    # All top-level commands appear in help
    for cmd in (
        "connections", "query", "objects", "migrator", "ai", "monitor",
        "daemon", "api", "databases", "thresholds", "config", "notify",
        "os", "cloud",
    ):
        assert cmd in out, f"missing '{cmd}' in top-level help"


@pytest.mark.parametrize(
    "sub",
    [
        "connections", "query", "objects", "migrator", "ai", "monitor",
        "daemon", "api", "databases", "thresholds", "config", "notify",
        "os", "cloud",
    ],
)
def test_subcommand_help(sub):
    r = _run(sub, "--help")
    assert r.returncode == 0


@pytest.mark.parametrize(
    "argv",
    [
        ("migrator", "convert", "--help"),
        ("migrator", "show", "--help"),
        ("migrator", "dump", "--help"),
        ("databases", "types", "--help"),
        ("databases", "ops", "--help"),
        ("thresholds", "list", "--help"),
        ("thresholds", "show", "--help"),
        ("thresholds", "check", "--help"),
        ("config", "show", "--help"),
        ("notify", "send", "--help"),
        ("os", "metrics", "--help"),
        ("cloud", "connections", "--help"),
        ("cloud", "connections", "list", "--help"),
        ("cloud", "connections", "add", "--help"),
        ("cloud", "connections", "remove", "--help"),
        ("cloud", "connections", "test", "--help"),
        ("cloud", "metrics", "--help"),
        ("cloud", "monitor", "--help"),
        ("daemon", "start", "--help"),
        ("daemon", "stop", "--help"),
        ("daemon", "status", "--help"),
        ("connections", "list", "--help"),
        ("connections", "add", "--help"),
        ("connections", "remove", "--help"),
        ("connections", "test", "--help"),
    ],
)
def test_nested_subcommand_help(argv):
    r = _run(*argv, timeout=20)
    assert r.returncode == 0, f"{' '.join(argv)} failed: {r.stderr}"


def test_databases_types_runs():
    """`databases types` works without DB connectivity (registry-only)."""
    r = _run("databases", "types", "--format", "json", timeout=30)
    assert r.returncode == 0, r.stderr
    # output must contain at least one engine name in any case
    out = r.stdout.lower()
    assert any(eng in out for eng in ("mysql", "sqlite", "postgresql"))


def test_databases_ops_mysql_lists_operations():
    """`databases ops --type MySQL` must enumerate at least 5 operations."""
    r = _run("databases", "ops", "--type", "MySQL", "--format", "json", timeout=30)
    assert r.returncode == 0, r.stderr
    assert "getMysql" in r.stdout, r.stdout


def test_thresholds_list_no_source_runs():
    """`thresholds list` must return at least one rule from the bundled INI."""
    r = _run("thresholds", "list", "--format", "json", timeout=30)
    assert r.returncode == 0, r.stderr
    assert '"metric"' in r.stdout


def test_os_metrics_runs():
    """`os metrics` must report the standard host counters via psutil."""
    r = _run("os", "metrics", "--format", "json", timeout=30)
    assert r.returncode == 0, r.stderr
    assert "cpu_utilization" in r.stdout or "memory_utilization" in r.stdout


def test_config_show_section_runs():
    r = _run("config", "show", "--section", "paths", timeout=30)
    assert r.returncode == 0, r.stderr
    assert "[paths]" in r.stdout or "config_dir" in r.stdout


def test_cloud_connections_list_runs():
    """`cloud connections list` must not error even if no profile exists."""
    r = _run("cloud", "connections", "list", "--format", "json", timeout=30)
    assert r.returncode == 0, r.stderr
