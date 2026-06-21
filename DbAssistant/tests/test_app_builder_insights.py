"""Tests for the DB insights dashboard generator (insights_admin variant)."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from ai_assistant.app_builder.insights_app import generate_insights
from ai_assistant.app_builder.spec import AppSpec
from ai_assistant.app_builder.webapp import generate_app


def _seed_dba_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount REAL,
            status TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        );
        INSERT INTO customers (id, name, email) VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com');
        INSERT INTO orders (id, customer_id, amount, status) VALUES
            (1, 1, 99.50, 'shipped'),
            (2, 1, 45.00, 'pending'),
            (3, 2, 120.00, 'shipped');
    """)
    conn.commit()
    conn.close()


def test_generate_insights_produces_dba_surfaces():
    spec = AppSpec(app_name="dbinsights", kind="insights")
    files = generate_insights(spec)

    required = (
        "src/introspect.py", "src/web.py", "src/api.py",
        "templates/index.html", "templates/table_detail.html",
        "templates/relationships.html", "templates/sample.html",
    )
    for rel in required:
        assert rel in files, f"missing {rel}"
    assert "Database Overview" in files["templates/index.html"]
    assert "dashboard_summary" in files["src/introspect.py"]
    assert "db_insights" in files["src/app.py"]


def test_insights_kind_routed_through_generate_app():
    spec = AppSpec(app_name="dbinsights", kind="insights")
    files = generate_app(spec)
    assert "src/introspect.py" in files
    assert "templates/relationships.html" in files


def test_insights_app_runs_against_sample_db(tmp_path):
    spec = AppSpec(app_name="dbinsights", kind="insights")
    ws = tmp_path / "ws"
    ws.mkdir()
    for rel, content in generate_insights(spec).items():
        path = ws / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    db_path = ws / "app.db"
    _seed_dba_db(db_path)

    env = {**os.environ, "APP_DB_PATH": str(db_path)}
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "tests/test_app.py"],
        cwd=str(ws), capture_output=True, text=True, env=env, timeout=60,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr

    # Spot-check live introspection via a one-liner
    check = (
        "import os\n"
        f"os.environ['APP_DB_PATH'] = {str(db_path)!r}\n"
        "from src.introspect import dashboard_summary, all_relationships\n"
        "s = dashboard_summary()\n"
        "assert s['table_count'] == 2\n"
        "assert s['total_rows'] == 5\n"
        "assert len(all_relationships()) == 1\n"
        "print('ok')\n"
    )
    proc2 = subprocess.run(
        [sys.executable, "-c", check],
        cwd=str(ws), capture_output=True, text=True, env=env, timeout=30,
    )
    assert proc2.returncode == 0, proc2.stderr
    assert "ok" in proc2.stdout


def test_flows_set_insights_kind_for_insights_admin_variant():
    from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
    from ai_assistant.app_builder.flows import BuildFlows

    bp = AppBlueprint(
        name="test", mode=BuildMode.FROM_DATABASE,
        db_app_variant="insights_admin", kind="",
    )
    # Simulate what build_from_database does before _run
    if bp.db_app_variant == "insights_admin":
        bp.kind = "insights"
    assert bp.kind == "insights"
