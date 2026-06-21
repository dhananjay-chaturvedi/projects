"""
Shared helpers, datasets, and assertions for comprehensive integration tests.

Uses live services when available (local MySQL, saved ~/.dbmanager profiles,
AWS/GCP credentials). Never logs or asserts on password/secret fields.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Saved connection names used in prior live smoke runs (override via DBTOOL_TEST_CONNS).
DEFAULT_SAVED_DB_CONNS = (
    "local_mariadb",
    "ntfd_push_notification",
    "aws_stg_pushdb",
    "rw-dev-histvm",
)
DEFAULT_SAVED_CLOUD_CONNS = ("aws-pushdb-dev",)
TUNNEL_CONNS = frozenset(
    {"aws_stg_pushdb", "ntfd_push_notification", "rw-dev-histvm"}
)

# Set DBTOOL_INCLUDE_TUNNEL=1 to exercise SSH-tunnel connections (slow if tunnel down).
INCLUDE_TUNNEL = os.environ.get("DBTOOL_INCLUDE_TUNNEL", "").strip() in ("1", "true", "yes")


def filter_test_connection_names(names: list[str]) -> list[str]:
    if INCLUDE_TUNNEL:
        return names
    return [n for n in names if n not in TUNNEL_CONNS]

# Object types meaningful on MySQL/MariaDB (unsupported types assert error dict).
MYSQL_OBJECT_TYPES = (
    "tables",
    "views",
    "indexes",
    "triggers",
    "procs",
    "functions",
    "events",
    "databases",
    "users",
    "charsets",
    "engines",
    "processlist",
    "constraints",
)

# Schema conversion target engines to exercise mapper permutations.
SCHEMA_TARGET_TYPES = (
    "MySQL",
    "MariaDB",
    "PostgreSQL",
    "SQLite",
    "Oracle",
    "SQL Server",
)

# Data compare modes and sample sizes (properties.ini default is also tested).
DATA_COMPARE_MODES = ("sample", "full")
DATA_COMPARE_SAMPLE_SIZES = (1, 5, 10, 50)


@dataclass(frozen=True)
class SqlCase:
    """Single SQL execution case with expected shape."""

    label: str
    sql: str
    expect_columns: list[str] | None = None
    min_rows: int = 1
    row_check: Callable[[list[list[str]]], bool] | None = None
    max_ms: float = 30_000.0
    expect_error: bool = False


@dataclass
class TableFixture:
    """Ephemeral table created for schema/data compare tests."""

    name: str
    columns_sql: str
    seed_sql: list[str] = field(default_factory=list)


# ── Parametrized SQL datasets ────────────────────────────────────────────────

SELECT_SQL_CASES: tuple[SqlCase, ...] = (
    SqlCase("literal_int", "SELECT 1 AS one", ["one"], row_check=lambda r: r[0][0] == "1"),
    SqlCase("arithmetic", "SELECT 2 + 3 AS sum_val", ["sum_val"], row_check=lambda r: r[0][0] == "5"),
    SqlCase(
        "string_concat",
        "SELECT CONCAT('a', 'b') AS ab",
        ["ab"],
        row_check=lambda r: r[0][0] == "ab",
    ),
    SqlCase(
        "null_handling",
        "SELECT NULL AS n, COALESCE(NULL, 'x') AS cx",
        ["n", "cx"],
        row_check=lambda r: r[0][0] == "" and r[0][1] == "x",
    ),
    SqlCase("current_database", "SELECT DATABASE() AS db", ["db"], min_rows=1),
    SqlCase("version", "SELECT VERSION() AS v", ["v"], min_rows=1),
    SqlCase(
        "information_schema_count",
        "SELECT COUNT(*) AS cnt FROM information_schema.tables "
        "WHERE table_schema = DATABASE()",
        ["cnt"],
        row_check=lambda r: int(r[0][0]) >= 0,
    ),
    SqlCase(
        "multi_column",
        "SELECT 'hello' AS msg, 42 AS num, UPPER('abc') AS up",
        ["msg", "num", "up"],
        row_check=lambda r: r[0] == ["hello", "42", "ABC"],
    ),
    SqlCase(
        "subquery_scalar",
        "SELECT (SELECT 99) AS nested",
        ["nested"],
        row_check=lambda r: r[0][0] == "99",
    ),
    SqlCase(
        "union_all",
        "SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3",
        ["n"],
        min_rows=3,
        row_check=lambda r: sorted(x[0] for x in r) == ["1", "2", "3"],
    ),
)

PORTABLE_SQL_CASES = tuple(
    c for c in SELECT_SQL_CASES
    if c.label
    in ("literal_int", "arithmetic", "string_concat", "null_handling", "multi_column", "subquery_scalar")
)

MYSQL_ONLY_SQL_LABELS = frozenset(
    {"current_database", "information_schema_count", "union_all", "version"}
)

INVALID_SQL_CASES: tuple[SqlCase, ...] = (
    SqlCase("syntax_error", "SELEC 1", expect_error=True),
    SqlCase("bad_table", "SELECT * FROM __pytest_nonexistent_xyz__", expect_error=True),
)

DML_SQL_SEQUENCE = (
    "CREATE TABLE IF NOT EXISTS `{table}` ("
    "id INT PRIMARY KEY AUTO_INCREMENT, "
    "tag VARCHAR(32) NOT NULL, "
    "amount DECIMAL(10,2) DEFAULT 0, "
    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ")",
    "DELETE FROM `{table}` WHERE tag LIKE 'pytest_%'",
    "INSERT INTO `{table}` (tag, amount) VALUES ('pytest_a', 10.5)",
    "INSERT INTO `{table}` (tag, amount) VALUES ('pytest_b', 20.0)",
    "UPDATE `{table}` SET amount = 99.9 WHERE tag = 'pytest_a'",
    "SELECT tag, amount FROM `{table}` WHERE tag IN ('pytest_a','pytest_b') ORDER BY tag",
)

SCHEMA_TEST_TABLE = TableFixture(
    name="pytest_comp_schema_src",
    columns_sql=(
        "id INT PRIMARY KEY AUTO_INCREMENT, "
        "code VARCHAR(16) NOT NULL, "
        "payload JSON NULL, "
        "score DECIMAL(8,2) DEFAULT 0"
    ),
    seed_sql=(
        "INSERT INTO `{name}` (code, score) VALUES ('A', 1.1)",
        "INSERT INTO `{name}` (code, score) VALUES ('B', 2.2)",
    ),
)

SCHEMA_MIRROR_TABLE = TableFixture(
    name="pytest_comp_schema_tgt",
    columns_sql=(
        "id INT PRIMARY KEY AUTO_INCREMENT, "
        "code VARCHAR(16) NOT NULL, "
        "payload JSON NULL, "
        "score DECIMAL(8,2) DEFAULT 0"
    ),
    seed_sql=(
        "INSERT INTO `{name}` (code, score) VALUES ('A', 1.1)",
        "INSERT INTO `{name}` (code, score) VALUES ('B', 2.2)",
    ),
)


def project_python() -> Path:
    venv = ROOT / ".venv" / "bin" / "python"
    return venv if venv.is_file() else Path(sys.executable)


def load_saved_db_connection_names() -> list[str]:
    explicit = os.environ.get("DBTOOL_TEST_CONNS", "").strip()
    if explicit:
        return filter_test_connection_names(
            [n.strip() for n in explicit.split(",") if n.strip()]
        )
    try:
        from common.connection_manager import ConnectionManager

        cm = ConnectionManager()
        names = [c.get("name", "") for c in cm.get_all_connections() if c.get("name")]
        if names:
            return filter_test_connection_names(names)
    except Exception:
        pass
    if os.environ.get("DBASSISTANT_HOME"):
        return []
    return filter_test_connection_names(list(DEFAULT_SAVED_DB_CONNS))


def load_saved_cloud_connection_names() -> list[str]:
    explicit = os.environ.get("DBTOOL_TEST_CLOUD_CONNS", "").strip()
    if explicit:
        return [n.strip() for n in explicit.split(",") if n.strip()]
    try:
        from common.cloud.connection_manager import CloudConnectionManager

        cm = CloudConnectionManager()
        data = cm.load_cloud_databases()
        names = list(data.keys())
        if names:
            return names
    except Exception:
        pass
    if os.environ.get("DBASSISTANT_HOME"):
        return []
    return list(DEFAULT_SAVED_CLOUD_CONNS)


def tunnel_unreachable(text: str) -> bool:
    low = (text or "").lower()
    return "can't connect" in low or "could not connect" in low or "connection refused" in low


def skip_if_tunnel(conn_name: str, err_text: str) -> None:
    if conn_name in TUNNEL_CONNS and tunnel_unreachable(err_text):
        pytest.skip(f"SSH tunnel not up for {conn_name}")


def first_table_name(items: list) -> str | None:
    if not items:
        return None
    row = items[0]
    if isinstance(row, str):
        return row
    if isinstance(row, dict):
        for key in ("name", "table_name", "tables", "table"):
            if row.get(key):
                return str(row[key])
    if isinstance(row, (list, tuple)) and row:
        return str(row[0])
    return None


def assert_query_result(case: SqlCase, result: dict) -> None:
    if case.expect_error:
        assert result.get("error"), f"{case.label}: expected error, got {result}"
        return
    assert not result.get("error"), f"{case.label}: {result.get('error')}"
    if case.expect_columns is not None:
        assert result.get("columns") == case.expect_columns, (
            f"{case.label}: columns {result.get('columns')} != {case.expect_columns}"
        )
    rows = result.get("rows") or []
    assert len(rows) >= case.min_rows, f"{case.label}: expected >= {case.min_rows} rows"
    if case.row_check is not None:
        assert case.row_check(rows), f"{case.label}: row_check failed for {rows!r}"
    elapsed = float(result.get("time_ms") or 0)
    assert elapsed <= case.max_ms, f"{case.label}: slow query {elapsed}ms > {case.max_ms}ms"


def run_cli(module: str, *args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    cmd = [str(project_python()), "-m", module, *args]
    return subprocess.run(
        cmd,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def parse_json_stdout(text: str) -> Any:
    text = (text or "").strip()
    # CLI may print log lines before JSON — scan for first [ or { and decode.
    for start_char, end_char in (("[", "]"), ("{", "}")):
        start = text.find(start_char)
        if start < 0:
            continue
        depth = 0
        for i in range(start, len(text)):
            ch = text[i]
            if ch == start_char:
                depth += 1
            elif ch == end_char:
                depth -= 1
                if depth == 0:
                    return json.loads(text[start : i + 1])
    raise ValueError("no JSON in stdout")


def setup_mysql_table(conn, fixture: TableFixture) -> str:
    """Create and seed table; return table name."""
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{fixture.name}`")
        cur.execute(f"CREATE TABLE `{fixture.name}` ({fixture.columns_sql})")
        for stmt in fixture.seed_sql:
            cur.execute(stmt.format(name=fixture.name))
        conn.commit()
    finally:
        cur.close()
    return fixture.name


def teardown_mysql_table(conn, table_name: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        conn.commit()
    finally:
        cur.close()


def ddl_contains_create_table(ddl: str, table: str) -> bool:
    if not ddl:
        return False
    pat = re.compile(rf"create\s+table\s+[`\"]?{re.escape(table)}[`\"]?", re.I)
    return bool(pat.search(ddl))
